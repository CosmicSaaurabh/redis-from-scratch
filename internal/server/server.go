// Package server owns the network side: accepting connections, framing
// commands, batching replies and shutting down cleanly.
//
// Concurrency shape is one goroutine per connection. That is the right model
// here rather than an event loop, because Go's scheduler already multiplexes
// blocked network reads onto a small number of OS threads, and a goroutine per
// connection costs a few kilobytes of stack instead of a hand-written state
// machine. What matters is that the number of them is bounded, every one of
// them can be interrupted, and none of them can outlive shutdown.
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/clock"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/command"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/config"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/resp"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/stats"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
)

// maxBatch bounds how many pipelined commands are served before replies are
// flushed. Without a cap, a client that pipelines a million commands would
// make the server buffer a million replies before writing any of them.
const maxBatch = 1024

// flushThreshold forces a flush once buffered replies reach this size, so that
// a batch of commands with large values does not accumulate unbounded memory.
const flushThreshold = 256 << 10

// walFlusher is implemented by storage engines whose durability depends on the
// server pushing log bytes to the kernel before it answers the client.
type walFlusher interface {
	FlushWAL() error
}

// Server is a RESP server.
type Server struct {
	cfg   *config.Config
	store store.Store
	table *command.Table
	stat  *stats.Stats
	clk   clock.Clock
	log   *slog.Logger

	runID     string
	version   string
	startedAt time.Time

	ln       net.Listener
	flushWAL walFlusher

	// slots bounds concurrent connections. A buffered channel is used rather
	// than a counter because it makes the bound impossible to leak past: a
	// connection that cannot take a slot is refused at accept time instead of
	// being queued into memory the server does not have.
	slots chan struct{}

	mu    sync.RWMutex
	conns map[uint64]*connection

	nextID atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	stopOnce     sync.Once
	shutdownSave atomic.Bool
	shutdownReq  chan bool

	bgsaveActive atomic.Bool

	// commandTimeout is only armed for an out-of-process engine. Wrapping every
	// command in a context.WithTimeout costs an allocation and a runtime timer,
	// and against an in-process map there is no remote call that could hang, so
	// the deadline would only ever add overhead.
	commandTimeout time.Duration

	// hooks the process supplies for persistence-aware commands.
	saver Saver
}

// Saver is the persistence surface SAVE, BGSAVE and INFO need. It is optional:
// a cache-only server passes nil and those commands report that persistence is
// disabled rather than pretending to succeed.
type Saver interface {
	Snapshot(ctx context.Context) (string, error)
	LastSnapshot() (int64, bool)
	Sync(ctx context.Context) error
}

// Options configures a Server.
type Options struct {
	Config  *config.Config
	Store   store.Store
	Clock   clock.Clock
	Logger  *slog.Logger
	Stats   *stats.Stats
	Saver   Saver
	RunID   string
	Version string
}

// New builds a server. It does not listen; call Serve for that.
func New(opt Options) (*Server, error) {
	if opt.Config == nil || opt.Store == nil {
		return nil, errors.New("server: Config and Store are required")
	}
	if err := opt.Config.Validate(); err != nil {
		return nil, err
	}
	if opt.Clock == nil {
		opt.Clock = clock.System{}
	}
	if opt.Logger == nil {
		opt.Logger = slog.Default()
	}
	if opt.Stats == nil {
		opt.Stats = stats.New()
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		cfg:         opt.Config,
		store:       opt.Store,
		table:       command.NewTable(),
		stat:        opt.Stats,
		clk:         opt.Clock,
		log:         opt.Logger,
		runID:       opt.RunID,
		version:     opt.Version,
		startedAt:   opt.Clock.Now(),
		slots:       make(chan struct{}, opt.Config.MaxClients),
		conns:       make(map[uint64]*connection),
		ctx:         ctx,
		cancel:      cancel,
		shutdownReq: make(chan bool, 1),
		saver:       opt.Saver,
	}
	if fw, ok := opt.Store.(walFlusher); ok {
		s.flushWAL = fw
	}
	if opt.Config.Engine == config.EngineLSM {
		s.commandTimeout = opt.Config.EngineTimeout
	}
	return s, nil
}

// Addr returns the address the server is listening on, or empty before Serve.
func (s *Server) Addr() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// ShutdownRequests yields true when a client asked for a shutdown with a final
// save, false without. The process wires this to its own stop sequence rather
// than letting a command call os.Exit from inside a handler.
func (s *Server) ShutdownRequests() <-chan bool { return s.shutdownReq }

// Serve listens and accepts until the context is cancelled or Close is called.
func (s *Server) Serve() error {
	lc := net.ListenConfig{KeepAlive: s.cfg.TCPKeepAlive}
	ln, err := lc.Listen(s.ctx, "tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("server: listen on %s: %w", s.cfg.Addr, err)
	}
	s.ln = ln
	s.log.Info("accepting connections",
		"addr", ln.Addr().String(), "engine", s.store.Name(), "maxclients", s.cfg.MaxClients)

	if s.cfg.ActiveExpireEnabled {
		s.wg.Add(1)
		go s.activeExpireLoop()
	}

	for {
		nc, err := ln.Accept()
		if err != nil {
			if s.ctx.Err() != nil {
				return nil // orderly shutdown
			}
			// A transient accept failure - a fd limit, a RST between the SYN
			// and the accept - must not kill the listener. Back off briefly so
			// the loop cannot spin at full speed against a persistent one.
			s.log.Warn("accept failed", "err", err)
			select {
			case <-time.After(20 * time.Millisecond):
				continue
			case <-s.ctx.Done():
				return nil
			}
		}
		s.stat.ConnectionsReceived.Add(1)

		select {
		case s.slots <- struct{}{}:
		default:
			// At the connection limit. Refuse with a protocol-level error so
			// the client sees a reason rather than an unexplained reset.
			s.stat.ConnectionsRejected.Add(1)
			s.refuse(nc)
			continue
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() { <-s.slots }()
			s.handle(nc)
		}()
	}
}

// refuse rejects a connection over the limit, best effort and time bounded so
// a hostile client cannot make the accept loop wait on it.
func (s *Server) refuse(nc net.Conn) {
	_ = nc.SetWriteDeadline(time.Now().Add(time.Second))
	_, _ = nc.Write([]byte("-ERR max number of clients reached\r\n"))
	_ = nc.Close()
}

// Close stops accepting, interrupts in-flight connections and waits for them.
func (s *Server) Close() error {
	var err error
	s.stopOnce.Do(func() {
		s.cancel()
		if s.ln != nil {
			err = s.ln.Close()
		}

		// Cancelling the context is not enough on its own: a goroutine blocked
		// in a socket read does not observe a context. Every connection is
		// closed explicitly, which is what actually unblocks those reads.
		s.mu.Lock()
		for _, c := range s.conns {
			c.close()
		}
		s.mu.Unlock()

		done := make(chan struct{})
		go func() { s.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(s.cfg.ShutdownGrace):
			s.log.Warn("shutdown grace expired with connections still active",
				"grace", s.cfg.ShutdownGrace, "active", s.stat.ConnectionsActive.Load())
		}
	})
	return err
}

// connection couples the socket with the command layer's view of a client.
type connection struct {
	nc    net.Conn
	state *command.Conn
	// cctx is reused across every command on this connection. Commands are
	// strictly sequential per connection, so one instance is enough.
	cctx *command.Context
	// closeOnce keeps a CLIENT KILL racing with the connection's own exit from
	// closing the same socket twice.
	closeOnce sync.Once
}

func (c *connection) close() {
	c.closeOnce.Do(func() { _ = c.nc.Close() })
}

func (s *Server) handle(nc net.Conn) {
	id := s.nextID.Add(1)
	now := s.clk.Now()
	conn := &connection{
		nc: nc,
		state: command.NewConn(id, nc.RemoteAddr().String(), nc.LocalAddr().String(),
			s.cfg.RequirePass == "", now),
	}

	s.mu.Lock()
	s.conns[id] = conn
	s.mu.Unlock()
	s.stat.ConnectionsActive.Add(1)

	defer func() {
		// A panic in a command handler must take down one connection, not the
		// process. The stack is logged with the client's identity so the bug is
		// actionable rather than just survived.
		if r := recover(); r != nil {
			s.log.Error("panic serving client", "client", id, "addr", conn.state.RemoteAddr, "panic", r)
		}
		s.mu.Lock()
		delete(s.conns, id)
		s.mu.Unlock()
		s.stat.ConnectionsActive.Add(-1)
		conn.close()
	}()

	r := resp.NewReader(nc, s.cfg.Limits)
	w := resp.NewWriter(nc, s.cfg.ReplyBufferSize)
	conn.cctx = &command.Context{Ctx: s.ctx, W: w, Conn: conn.state, Env: (*env)(s)}
	s.serveConn(conn, r, w)
}

// serveConn is the per-connection command loop.
func (s *Server) serveConn(conn *connection, r *resp.Reader, w *resp.Writer) {
	idleDeadline := s.deadlineFor(s.cfg.IdleTimeout)

	for {
		if s.ctx.Err() != nil {
			return
		}

		// The blocking read happens with no replies pending, because the batch
		// below always flushes before looping back here. That is what makes it
		// safe to block: the client is never waiting on a reply the server is
		// holding.
		if err := nc(conn).SetReadDeadline(s.deadlineFor(s.cfg.ReadTimeout)); err != nil {
			return
		}
		args, err := r.ReadCommand()
		if err != nil {
			if s.handleReadError(conn, w, r, err, &idleDeadline) {
				continue
			}
			return
		}
		if !idleDeadline.IsZero() {
			idleDeadline = s.deadlineFor(s.cfg.IdleTimeout)
		}

		if args != nil && !s.dispatch(conn, w, args) {
			s.finish(conn, w)
			return
		}

		// Serve the rest of the pipeline, but only while a whole command is
		// already buffered, so this loop can never block.
		batched := 1
		for batched < maxBatch && w.Buffered() < flushThreshold && r.HasCompleteCommand() {
			args, err := r.ReadCommand()
			if err != nil {
				if !s.handleReadError(conn, w, r, err, &idleDeadline) {
					s.finish(conn, w)
					return
				}
				break
			}
			if args == nil {
				continue
			}
			if !s.dispatch(conn, w, args) {
				s.finish(conn, w)
				return
			}
			batched++
		}
		if batched > 1 {
			s.stat.PipelineBatches.Add(1)
		}

		if !s.finish(conn, w) {
			return
		}
		if conn.state.ShouldClose() {
			return
		}
	}
}

// finish makes the batch's writes durable and then visible, in that order.
//
// The write-ahead log is pushed to the kernel before a single reply byte
// reaches the socket. Reversing these two lines would mean a client could see
// an acknowledgement for a write that exists only in this process's memory,
// and killing the process would lose it - the exact guarantee the everysec
// policy is supposed to provide.
func (s *Server) finish(conn *connection, w *resp.Writer) bool {
	if s.flushWAL != nil {
		if err := s.flushWAL.FlushWAL(); err != nil {
			s.log.Error("write-ahead log flush failed, dropping client rather than acknowledging",
				"client", conn.state.ID, "err", err)
			return false
		}
	}
	pending := w.Buffered()
	if pending == 0 {
		return true
	}
	if err := nc(conn).SetWriteDeadline(s.deadlineFor(s.cfg.WriteTimeout)); err != nil {
		return false
	}
	if err := w.Flush(); err != nil {
		if !isBenignNetErr(err) {
			s.log.Debug("reply flush failed", "client", conn.state.ID, "err", err)
		}
		return false
	}
	s.stat.NetOutputBytes.Add(uint64(pending))
	return true
}

// handleReadError decides whether the connection survives. It returns true to
// continue the loop.
func (s *Server) handleReadError(conn *connection, w *resp.Writer, r *resp.Reader, err error, idleDeadline *time.Time) bool {
	switch {
	case errors.Is(err, io.EOF):
		return false

	case resp.IsProtocolError(err):
		// Framing is lost, so there is no way to resynchronise with the
		// client's byte stream. Report the reason and hang up: continuing
		// would misinterpret every subsequent byte.
		s.stat.ProtocolErrors.Add(1)
		w.WriteError("ERR " + err.Error())
		conn.state.CloseAfterReply()
		s.finish(conn, w)
		return false

	case isTimeout(err):
		if r.InFlight() {
			// A partial command that stopped arriving. Holding the goroutine
			// and its buffers open for this is exactly the slowloris the
			// deadline exists to stop.
			s.stat.TimeoutDisconnects.Add(1)
			s.log.Debug("dropping client that stalled mid-command", "client", conn.state.ID)
			return false
		}
		if !idleDeadline.IsZero() && time.Now().After(*idleDeadline) {
			s.stat.TimeoutDisconnects.Add(1)
			return false
		}
		// A healthy connection that simply had nothing to say.
		return true

	default:
		if !isBenignNetErr(err) {
			s.log.Debug("read failed", "client", conn.state.ID, "err", err)
		}
		return false
	}
}

// dispatch runs one command. It returns false when the connection must end.
func (s *Server) dispatch(conn *connection, w *resp.Writer, args [][]byte) bool {
	if conn.state.Killed() {
		return false
	}
	var inBytes uint64
	for _, a := range args {
		inBytes += uint64(len(a))
	}
	s.stat.NetInputBytes.Add(inBytes)

	cctx := conn.cctx
	cctx.Args = args
	if s.commandTimeout > 0 {
		ctx, cancel := context.WithTimeout(s.ctx, s.commandTimeout)
		cctx.Ctx = ctx
		defer func() {
			cancel()
			cctx.Ctx = s.ctx
		}()
	}

	if err := s.table.Dispatch(cctx); err != nil {
		s.log.Error("command failed", "client", conn.state.ID, "cmd", cctx.Name(), "err", err)
		return false
	}
	return !conn.state.ShouldClose()
}

// activeExpireLoop reclaims expired keys in the background.
//
// Lazy expiry alone is not enough: a key that is written with a TTL and never
// read again would occupy memory forever, so a workload of write-once keys
// would look like a slow memory leak. The cycle samples volatile keys and, if
// a large fraction of the sample was already expired, runs again immediately,
// which lets it drain a mass expiry quickly without a fixed rate that would be
// either too slow or permanently wasteful.
func (s *Server) activeExpireLoop() {
	defer s.wg.Done()
	tick := time.NewTicker(s.cfg.ActiveExpireInterval)
	defer tick.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-tick.C:
		}
		for round := 0; round < 16; round++ {
			sampled, expired := s.expireCycle()
			if sampled == 0 || float64(expired)/float64(sampled) < s.cfg.ActiveExpireThreshold {
				break
			}
			if s.ctx.Err() != nil {
				return
			}
		}
	}
}

func (s *Server) expireCycle() (sampled, expired int) {
	keys, err := s.store.SampleVolatile(s.ctx, s.cfg.ActiveExpireSample)
	if err != nil || len(keys) == 0 {
		return 0, 0
	}
	now := s.clk.NowMs()
	for _, k := range keys {
		sampled++
		// Get already performs lazy expiry, so reading a key that is past its
		// deadline is what reclaims it. Deleting it explicitly here would
		// journal a redundant record on every cycle.
		rec, ok, err := s.store.Get(s.ctx, k)
		if err != nil {
			return sampled, expired
		}
		if !ok || rec.Expired(now) {
			expired++
			s.stat.ExpiredKeys.Add(1)
		}
	}
	return sampled, expired
}

// deadlineFor computes a socket deadline.
//
// It reads the wall clock directly rather than the server's injected clock.
// The injected clock exists so that TTL semantics can be tested without
// sleeping, and it can legitimately be frozen or set to any instant; handing
// such a value to SetReadDeadline would arm the kernel with a deadline that is
// already in the past and every read would fail instantly. Expiry is a
// semantic decision and belongs to the injected clock; a socket deadline is an
// instruction to the operating system and belongs to real time.
func (s *Server) deadlineFor(d time.Duration) time.Time {
	if d <= 0 {
		return time.Time{}
	}
	return time.Now().Add(d)
}

func nc(c *connection) net.Conn { return c.nc }

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// isBenignNetErr reports errors that mean "the client went away", which is
// normal traffic rather than something an operator should see in the log.
func isBenignNetErr(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
