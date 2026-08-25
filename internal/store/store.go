// Package store defines the pluggable storage boundary.
//
// Everything above this line - the RESP codec, the command table, the server -
// is storage agnostic. Everything below it is an engine: an in-process sharded
// hash map, or the Rust LSM-tree reached over gRPC.
//
// The interface is deliberately narrow. It carries no Redis semantics at all:
// no INCR, no APPEND, no NX. Those live in the command layer and are expressed
// through exactly one primitive, Update, which performs an atomic
// read-modify-write. Pushing atomicity down here rather than layering a lock
// table on top means each backend can use the cheapest correct mechanism it
// has: the memory engine takes one shard lock, the LSM client takes one stripe
// lock, and neither pays for a second lock on the hot path.
package store

import (
	"context"
	"errors"
)

// ErrClosed is returned by every method once Close has been called.
var ErrClosed = errors.New("store: closed")

// Record is the unit of storage: an opaque value plus an optional absolute
// expiry.
//
// ExpireAt is an absolute Unix time in milliseconds, and zero means "never".
// Absolute rather than relative is a durability requirement, not a style
// choice: a relative TTL replayed from a write-ahead log hours after it was
// written would silently extend every key's life by the length of the outage.
type Record struct {
	Value    []byte
	ExpireAt int64
}

// Expired reports whether the record is logically gone as of nowMs.
func (r Record) Expired(nowMs int64) bool {
	return r.ExpireAt != 0 && r.ExpireAt <= nowMs
}

// Volatile reports whether the record carries an expiry.
func (r Record) Volatile() bool { return r.ExpireAt != 0 }

// Clone returns a deep copy. Callers that hand a Record to a store must not
// retain the value slice, and stores must not retain a caller's slice; Clone
// is how both sides honour that.
func (r Record) Clone() Record {
	if r.Value == nil {
		return Record{ExpireAt: r.ExpireAt}
	}
	v := make([]byte, len(r.Value))
	copy(v, r.Value)
	return Record{Value: v, ExpireAt: r.ExpireAt}
}

// Action is what an UpdateFunc asks the store to do with a key.
type Action uint8

const (
	// ActionNone leaves the key untouched. Used when a conditional command
	// such as SET NX decides not to write.
	ActionNone Action = iota
	// ActionPut writes the returned Record.
	ActionPut
	// ActionDelete removes the key.
	ActionDelete
)

// UpdateFunc is called with the current state of a key while the store holds
// that key exclusively. It returns what should happen next.
//
// The cur Record and its Value are owned by the store and are only valid for
// the duration of the call. The function must not block on I/O: it runs under
// a lock that serialises every other operation on the same key.
type UpdateFunc func(cur Record, found bool) (next Record, act Action, err error)

// Mutation is one write in a batch.
type Mutation struct {
	Key    []byte
	Record Record
	Delete bool
}

// Store is the persistence engine contract.
//
// All methods are safe for concurrent use. Key and value slices passed in are
// borrowed for the duration of the call only; implementations copy what they
// retain. Slices returned from Get and Scan are owned by the caller and are
// safe to hold, because the cost of one copy on the read path is far cheaper
// than the class of aliasing bug the alternative invites.
type Store interface {
	// Name identifies the engine for INFO and metrics.
	Name() string

	// Get returns the record for key. The bool reports presence. Expired
	// records are reported as absent; whether they are physically reclaimed is
	// up to the engine.
	Get(ctx context.Context, key []byte) (Record, bool, error)

	// Put writes a record unconditionally.
	Put(ctx context.Context, key []byte, rec Record) error

	// Delete removes a key and reports whether it existed.
	Delete(ctx context.Context, key []byte) (bool, error)

	// Update performs an atomic read-modify-write. This is the only primitive
	// that gives command handlers compare-and-set semantics.
	Update(ctx context.Context, key []byte, fn UpdateFunc) error

	// MultiWrite applies a batch. It is atomic with respect to concurrent
	// single-key operations on the same engine, which is what MSET requires.
	MultiWrite(ctx context.Context, muts []Mutation) error

	// Scan walks the keyspace incrementally. It calls fn for each live record
	// and stops early if fn returns false. The returned cursor is fed to the
	// next call; a cursor of zero starts a new walk and a returned zero ends
	// it.
	//
	// Guarantee: a key present for the whole walk is visited at least once.
	// Keys added or removed mid-walk may or may not be visited. This is the
	// same contract Redis SCAN offers.
	Scan(ctx context.Context, cursor uint64, count int, fn func(key []byte, rec Record) bool) (uint64, error)

	// Len returns the number of live keys.
	Len(ctx context.Context) (int64, error)

	// FlushAll removes every key.
	FlushAll(ctx context.Context) error

	// SampleVolatile returns up to n keys that carry an expiry, chosen
	// pseudo-randomly. The active expiry cycle uses it. Engines that reclaim
	// expired data by other means, such as during LSM compaction, may return
	// nothing.
	SampleVolatile(ctx context.Context, n int) ([][]byte, error)

	// Stats reports engine counters for INFO.
	Stats(ctx context.Context) (Stats, error)

	// Close releases resources. It must be idempotent.
	Close() error
}

// Stats are engine-reported counters surfaced through INFO.
type Stats struct {
	Keys         int64
	VolatileKeys int64
	MemoryBytes  int64
	DiskBytes    int64
	Extra        map[string]string
}

// Journal is the write-ahead log as the storage engine sees it.
//
// The engine calls Log* while it still holds the key exclusively, before the
// mutation becomes visible to any other goroutine, and calls Await after
// releasing the key.
//
// That ordering is not a detail. If the log append happened outside the key
// lock, two writers to the same key could be assigned log sequence numbers in
// one order and apply to memory in the other: SET k=A takes LSN 5, INCR k
// takes the lock, reads the old value, takes LSN 6, applies, and only then does
// the SET apply. Memory ends at A, the log replays to old+1, and the database
// silently returns a different answer after every restart. Journalling under
// the key makes the two orders the same order by construction.
type Journal interface {
	// LogPut records a key write and returns its log sequence number.
	LogPut(key []byte, rec Record) (uint64, error)
	// LogDelete records a key removal.
	LogDelete(key []byte) (uint64, error)
	// LogFlushAll records a keyspace wipe.
	LogFlushAll() (uint64, error)
	// LogBatch records mutations that must replay atomically.
	LogBatch(muts []Mutation) (uint64, error)
	// Await blocks until lsn is durable under the configured policy. It is
	// called after the key lock is released so that an fsync never stalls
	// other keys.
	Await(lsn uint64) error
}

// Flusher is implemented by engines that can force durable state to disk on
// demand, which is what SAVE and a graceful shutdown need.
type Flusher interface {
	// Sync blocks until every write acknowledged before the call is durable.
	Sync(ctx context.Context) error
}

// Snapshotter is implemented by engines that can produce a point-in-time
// on-disk image, which is what BGSAVE exposes.
type Snapshotter interface {
	// Snapshot writes a consistent image and returns the path written.
	Snapshot(ctx context.Context) (string, error)
	// LastSnapshot reports the Unix time of the last successful snapshot and
	// whether one has ever been taken.
	LastSnapshot() (int64, bool)
}
