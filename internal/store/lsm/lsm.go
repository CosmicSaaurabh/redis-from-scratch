// Package lsm is the client for the Rust LSM storage engine.
//
// The engine runs as a separate process and speaks gRPC. Keeping it out of
// process rather than linking it through cgo is deliberate: cgo would make
// every call cross the Go scheduler's boundary, complicate the build, and put
// a segfault in the storage engine inside the same address space as the
// server. A local gRPC hop costs tens of microseconds and buys a hard fault
// boundary, an independently profileable process and a clean seam for the
// remote-engine deployment a later phase wants.
package lsm

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
)

// Options configures the engine client.
type Options struct {
	// Addr is the engine's gRPC address.
	Addr string
	// Timeout bounds a single call.
	Timeout time.Duration
	// Logger receives connection events.
	Logger *slog.Logger
}

// errNotBuilt is returned until the gRPC transport is generated and wired.
var errNotBuilt = errors.New("lsm: the Rust storage engine client is not built into this binary; run make proto")

// Store is a store.Store backed by the out-of-process LSM engine.
type Store struct {
	opt Options
}

// Open connects to the engine.
func Open(opt Options) (*Store, error) { return nil, errNotBuilt }

// Name identifies the engine.
func (s *Store) Name() string { return "lsm" }

// Get reads a key.
func (s *Store) Get(ctx context.Context, key []byte) (store.Record, bool, error) {
	return store.Record{}, false, errNotBuilt
}

// Put writes a key.
func (s *Store) Put(ctx context.Context, key []byte, rec store.Record) error { return errNotBuilt }

// Delete removes a key.
func (s *Store) Delete(ctx context.Context, key []byte) (bool, error) { return false, errNotBuilt }

// Update performs an atomic read-modify-write.
func (s *Store) Update(ctx context.Context, key []byte, fn store.UpdateFunc) error {
	return errNotBuilt
}

// MultiWrite applies a batch.
func (s *Store) MultiWrite(ctx context.Context, muts []store.Mutation) error { return errNotBuilt }

// Scan walks the keyspace.
func (s *Store) Scan(ctx context.Context, cursor uint64, count int, fn func([]byte, store.Record) bool) (uint64, error) {
	return 0, errNotBuilt
}

// Len counts keys.
func (s *Store) Len(ctx context.Context) (int64, error) { return 0, errNotBuilt }

// FlushAll empties the keyspace.
func (s *Store) FlushAll(ctx context.Context) error { return errNotBuilt }

// SampleVolatile returns keys with a TTL. The LSM engine reclaims expired
// records during compaction, so it returns nothing and the server's active
// expiry cycle correctly becomes a no-op against it.
func (s *Store) SampleVolatile(ctx context.Context, n int) ([][]byte, error) { return nil, nil }

// Stats reports engine counters.
func (s *Store) Stats(ctx context.Context) (store.Stats, error) {
	return store.Stats{}, errNotBuilt
}

// Snapshot asks the engine to flush its memtable to an SSTable.
func (s *Store) Snapshot(ctx context.Context) (string, error) { return "", errNotBuilt }

// LastSnapshot reports the engine's last flush.
func (s *Store) LastSnapshot() (int64, bool) { return 0, false }

// Sync forces the engine's write-ahead log to stable storage.
func (s *Store) Sync(ctx context.Context) error { return errNotBuilt }

// Close releases the connection.
func (s *Store) Close() error { return nil }

var _ store.Store = (*Store)(nil)
