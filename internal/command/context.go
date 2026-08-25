// Package command implements the Redis command surface on top of the storage
// interface.
//
// A command handler receives a Context, writes exactly one reply, and returns.
// It never touches the socket, never flushes, and never decides when the
// connection closes; the server owns all three so that pipelining, timeouts
// and shutdown stay in one place.
package command

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/clock"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/resp"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/stats"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
)

// Env is what a command handler is allowed to reach in the server.
//
// It is an interface rather than a concrete type so that the command package
// does not import the server package, which would be an import cycle, and so
// that tests can drive handlers without opening a socket.
type Env interface {
	// Store is the storage engine.
	Store() store.Store
	// Clock supplies time for expiry arithmetic.
	Clock() clock.Clock
	// Stats are the server counters.
	Stats() *stats.Stats
	// ConfigGet reads a runtime parameter.
	ConfigGet(name string) (string, bool)
	// ConfigAll lists every runtime parameter.
	ConfigAll() map[string]string
	// ConfigSet changes a runtime parameter.
	ConfigSet(name, value string) error
	// RequirePass returns the configured password, empty if AUTH is disabled.
	RequirePass() string
	// Info renders the INFO reply for the requested sections.
	Info(sections []string) string
	// Save writes a snapshot synchronously and returns its path.
	Save(ctx context.Context) (string, error)
	// BackgroundSave starts a snapshot and returns once it is under way.
	BackgroundSave(ctx context.Context) error
	// LastSave reports the Unix time of the last successful snapshot.
	LastSave() int64
	// SyncStore forces acknowledged writes to stable storage.
	SyncStore(ctx context.Context) error
	// Shutdown asks the server to stop. save requests a final snapshot.
	Shutdown(save bool)
	// Clients lists connected clients for CLIENT LIST and CLIENT KILL.
	Clients() []*Conn
	// KillClient closes a client by id and reports whether it was found.
	KillClient(id uint64) bool
	// RunID is the random identifier this process was started with.
	RunID() string
	// Version is the server version string.
	Version() string
}

// Conn is the per-connection state a command may read or change.
//
// Fields written by the connection's own goroutine and read by others - the
// CLIENT LIST of a different connection, for instance - are atomics. Fields
// only ever touched by the owning goroutine are plain.
type Conn struct {
	// ID is the monotonically assigned client identifier.
	ID uint64
	// RemoteAddr is the client's address.
	RemoteAddr string
	// LocalAddr is the server address the client reached.
	LocalAddr string
	// CreatedAt is when the connection was accepted.
	CreatedAt time.Time

	// Proto is the negotiated protocol version.
	Proto resp.Version
	// Authenticated is true once AUTH has succeeded, or if AUTH is disabled.
	Authenticated bool
	// Name is the client-supplied label from CLIENT SETNAME.
	name atomic.Pointer[string]
	// LastCommand is the most recent command name, for CLIENT LIST.
	lastCommand atomic.Pointer[string]
	// LastInteraction is the Unix time of the last command.
	lastInteraction atomic.Int64
	// CommandCount is how many commands this connection has run.
	commandCount atomic.Uint64

	// closeAfterReply asks the server to hang up once the pending reply has
	// been flushed. QUIT and a fatal protocol error both use it, which is what
	// lets the client actually see the reply before the socket goes away.
	closeAfterReply atomic.Bool
	// killed is set when another connection issued CLIENT KILL against this
	// one.
	killed atomic.Bool
}

// NewConn returns connection state for a freshly accepted client.
func NewConn(id uint64, remote, local string, authenticated bool, now time.Time) *Conn {
	c := &Conn{
		ID:            id,
		RemoteAddr:    remote,
		LocalAddr:     local,
		CreatedAt:     now,
		Proto:         resp.RESP2,
		Authenticated: authenticated,
	}
	empty := ""
	c.name.Store(&empty)
	c.lastCommand.Store(&empty)
	c.lastInteraction.Store(now.Unix())
	return c
}

// Name returns the client's label.
func (c *Conn) Name() string {
	if p := c.name.Load(); p != nil {
		return *p
	}
	return ""
}

// SetName sets the client's label.
func (c *Conn) SetName(s string) { c.name.Store(&s) }

// LastCommand returns the most recent command name.
func (c *Conn) LastCommand() string {
	if p := c.lastCommand.Load(); p != nil {
		return *p
	}
	return ""
}

// Touch records that a command ran on this connection.
//
// name is a pointer into the command table rather than a string, so that
// storing it does not force a heap allocation on every command.
func (c *Conn) Touch(name *string, nowUnix int64) {
	c.lastCommand.Store(name)
	c.lastInteraction.Store(nowUnix)
	c.commandCount.Add(1)
}

// IdleSeconds reports how long since the last command.
func (c *Conn) IdleSeconds(now time.Time) int64 {
	return now.Unix() - c.lastInteraction.Load()
}

// CommandCount reports how many commands this connection has run.
func (c *Conn) CommandCount() uint64 { return c.commandCount.Load() }

// CloseAfterReply asks the server to hang up once the reply is flushed.
func (c *Conn) CloseAfterReply() { c.closeAfterReply.Store(true) }

// ShouldClose reports whether the server should hang up after flushing.
func (c *Conn) ShouldClose() bool { return c.closeAfterReply.Load() || c.killed.Load() }

// Kill marks the connection for termination by another client's request.
func (c *Conn) Kill() { c.killed.Store(true) }

// Killed reports whether CLIENT KILL targeted this connection.
func (c *Conn) Killed() bool { return c.killed.Load() }

// Context carries everything one command invocation needs.
//
// The server allocates exactly one per connection and rewrites Args before
// each dispatch. Commands on a connection are strictly sequential, so reuse is
// safe, and it removes an allocation from the hot path.
type Context struct {
	// Ctx is cancelled when the server is shutting down or the connection dies.
	Ctx context.Context
	// Args holds the command name at index zero followed by its arguments.
	// The slices alias the connection's read buffer and are only valid for the
	// duration of the call, so anything stored must be copied.
	Args [][]byte
	// W writes the reply.
	W *resp.Writer
	// Conn is the calling connection's state.
	Conn *Conn
	// Env reaches the server.
	Env Env

	// name is the lowercased command name, resolved once by the dispatcher.
	name string
	// table is the registry that dispatched this command, so that COMMAND can
	// describe the very table it is running in rather than a second copy that
	// could drift out of sync.
	table *Table
}

// Table returns the registry that dispatched this command.
func (c *Context) Table() *Table { return c.table }

// Name returns the lowercased command name.
func (c *Context) Name() string { return c.name }

// Argc returns the number of arguments including the command name.
func (c *Context) Argc() int { return len(c.Args) }

// Arg returns argument i, or nil if it is absent.
func (c *Context) Arg(i int) []byte {
	if i < 0 || i >= len(c.Args) {
		return nil
	}
	return c.Args[i]
}

// ArgString returns argument i as a string.
func (c *Context) ArgString(i int) string { return string(c.Arg(i)) }

// ArgLower returns argument i lowercased, for keyword matching.
func (c *Context) ArgLower(i int) string { return strings.ToLower(string(c.Arg(i))) }

// Store is a shorthand for the storage engine.
func (c *Context) Store() store.Store { return c.Env.Store() }

// NowMs is the current time in Unix milliseconds.
func (c *Context) NowMs() int64 { return c.Env.Clock().NowMs() }

// Hit records a keyspace hit.
func (c *Context) Hit() { c.Env.Stats().KeyspaceHits.Add(1) }

// Miss records a keyspace miss.
func (c *Context) Miss() { c.Env.Stats().KeyspaceMisses.Add(1) }
