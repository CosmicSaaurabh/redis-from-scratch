// Package testutil starts real servers and speaks real RESP to them.
//
// The end-to-end tests deliberately go over a TCP socket rather than calling
// the command table directly. Everything interesting about a server lives in
// the parts a direct call skips: framing, pipelining, reply batching, timeouts
// and connection teardown. A test that bypasses them proves the command
// handlers work and nothing else.
package testutil

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/clock"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/config"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/persist"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/resp"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/server"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/stats"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store/memory"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/wal"
)

// Server is a running server under test.
type Server struct {
	Addr   string
	Config *config.Config
	Store  store.Store
	Clock  *clock.Mock
	Stats  *stats.Stats

	srv   *server.Server
	close func()
}

// Options tune the server a test starts.
type Options struct {
	// Dir is the data directory. Empty means an ephemeral one.
	Dir string
	// Durable enables the write-ahead log and snapshots.
	Durable bool
	// SyncPolicy selects the fsync discipline when Durable.
	SyncPolicy wal.SyncPolicy
	// RequirePass enables AUTH.
	RequirePass string
	// MaxClients bounds connections.
	MaxClients int
	// Configure runs last and can change anything.
	Configure func(*config.Config)
}

// Start brings up a server on a free port and registers its shutdown.
func Start(t *testing.T, opt Options) *Server {
	t.Helper()

	dir := opt.Dir
	if dir == "" {
		dir = t.TempDir()
	}
	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.MetricsAddr = ""
	cfg.Dir = dir
	cfg.RequirePass = opt.RequirePass
	cfg.AppendOnly = opt.Durable
	cfg.AppendFsync = opt.SyncPolicy
	cfg.SavePoints = nil
	cfg.ShutdownGrace = 2 * time.Second
	if opt.MaxClients > 0 {
		cfg.MaxClients = opt.MaxClients
	}
	if opt.Configure != nil {
		opt.Configure(cfg)
	}

	// A silent logger by default: a passing test should print nothing, and a
	// failing one is easier to read without a server's info lines in it.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if testing.Verbose() {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}

	clk := clock.NewMock(time.UnixMilli(1_700_000_000_000))
	st := stats.New()

	var (
		engine store.Store
		saver  server.Saver
		closer = func() error { return nil }
	)
	if opt.Durable {
		e, _, err := persist.Open(persist.Options{
			Dir: dir, SyncPolicy: cfg.AppendFsync, Clock: clk, Logger: logger,
		})
		if err != nil {
			t.Fatalf("open durable engine: %v", err)
		}
		engine, saver, closer = e, e, e.Close
	} else {
		m := memory.New(clk, nil)
		engine, closer = m, m.Close
	}

	srv, err := server.New(server.Options{
		Config: cfg, Store: engine, Clock: clk, Logger: logger, Stats: st,
		Saver: saver, RunID: "test-run-id", Version: "test",
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}

	ready := make(chan error, 1)
	go func() { ready <- srv.Serve() }()

	// Serve binds inside the goroutine, so the address is not known until it
	// has. Polling for it beats an arbitrary sleep, which is the usual source
	// of a test that passes locally and flakes in CI.
	deadline := time.Now().Add(5 * time.Second)
	for srv.Addr() == "" {
		select {
		case err := <-ready:
			t.Fatalf("server exited before it was ready: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("server did not bind within 5s")
		}
		time.Sleep(time.Millisecond)
	}

	s := &Server{Addr: srv.Addr(), Config: cfg, Store: engine, Clock: clk, Stats: st, srv: srv}
	s.close = func() {
		_ = srv.Close()
		_ = closer()
	}
	t.Cleanup(s.close)
	return s
}

// Stop shuts the server down early.
func (s *Server) Stop() { s.close() }

// Client is a RESP client for tests.
type Client struct {
	t    *testing.T
	conn net.Conn
	w    *resp.Writer
	r    *resp.ReplyReader
}

// Connect opens a client to the server.
func (s *Server) Connect(t *testing.T) *Client {
	t.Helper()
	conn, err := net.DialTimeout("tcp", s.Addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", s.Addr, err)
	}
	c := &Client{
		t:    t,
		conn: conn,
		w:    resp.NewWriter(conn, 64<<10),
		r:    resp.NewReplyReader(conn, resp.DefaultLimits()),
	}
	t.Cleanup(func() { _ = conn.Close() })
	return c
}

// Conn exposes the raw socket for tests that need to send malformed bytes.
func (c *Client) Conn() net.Conn { return c.conn }

// Send writes a command without reading the reply.
func (c *Client) Send(args ...string) {
	c.t.Helper()
	c.w.WriteCommandStrings(args...)
	if err := c.w.Flush(); err != nil {
		c.t.Fatalf("send %v: %v", args, err)
	}
}

// SendRaw writes arbitrary bytes.
func (c *Client) SendRaw(b []byte) {
	c.t.Helper()
	if _, err := c.conn.Write(b); err != nil {
		c.t.Fatalf("send raw: %v", err)
	}
}

// Read reads one reply.
func (c *Client) Read() resp.Reply {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	rep, err := c.r.Read()
	if err != nil {
		c.t.Fatalf("read reply: %v", err)
	}
	return rep
}

// Do sends a command and returns its reply.
func (c *Client) Do(args ...string) resp.Reply {
	c.t.Helper()
	c.Send(args...)
	return c.Read()
}

// Str runs a command and asserts a non-error reply, returning it as text.
func (c *Client) Str(args ...string) string {
	c.t.Helper()
	rep := c.Do(args...)
	if rep.IsError() {
		c.t.Fatalf("%v: unexpected error reply %s", args, rep.Str)
	}
	if rep.Null {
		return "<nil>"
	}
	return rep.String()
}

// Int runs a command and asserts an integer reply.
func (c *Client) Int(args ...string) int64 {
	c.t.Helper()
	rep := c.Do(args...)
	if rep.IsError() {
		c.t.Fatalf("%v: unexpected error reply %s", args, rep.Str)
	}
	if rep.Kind != resp.TypeInteger {
		c.t.Fatalf("%v: expected an integer reply, got %s", args, rep.String())
	}
	return rep.Int
}

// ErrMsg runs a command and asserts an error reply, returning its text.
func (c *Client) ErrMsg(args ...string) string {
	c.t.Helper()
	rep := c.Do(args...)
	if !rep.IsError() {
		c.t.Fatalf("%v: expected an error reply, got %s", args, rep.String())
	}
	return string(rep.Str)
}

// Pipeline sends several commands and reads all their replies.
func (c *Client) Pipeline(cmds ...[]string) []resp.Reply {
	c.t.Helper()
	for _, cmd := range cmds {
		c.w.WriteCommandStrings(cmd...)
	}
	if err := c.w.Flush(); err != nil {
		c.t.Fatalf("pipeline flush: %v", err)
	}
	out := make([]resp.Reply, 0, len(cmds))
	for range cmds {
		out = append(out, c.Read())
	}
	return out
}

// Close hangs up.
func (c *Client) Close() { _ = c.conn.Close() }

// ReadClosed asserts the server closed the connection.
func (c *Client) ReadClosed() {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	_, err := c.conn.Read(buf)
	if err == nil {
		c.t.Fatal("expected the connection to be closed, but it is still readable")
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			c.t.Fatalf("connection was not closed: read timed out")
		}
	}
}

// Eventually retries until fn returns nil or the deadline passes.
//
// It exists so tests can wait on a background flush or an expiry cycle without
// a fixed sleep, which is what makes a suite either slow or flaky depending on
// how the sleep was guessed.
func Eventually(t *testing.T, within time.Duration, what string, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last error
	for time.Now().Before(deadline) {
		last = fn()
		if last == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s did not happen within %s: %v", what, within, last)
}

// FreePort returns a port nothing is listening on.
func FreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatalf("release the reserved port: %v", err)
	}
	return port
}

// Errorf formats a comparison failure consistently.
func Errorf(format string, args ...any) error { return fmt.Errorf(format, args...) }
