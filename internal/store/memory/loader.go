package memory

import (
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
)

// Loader applies recovered state directly into the engine.
//
// It deliberately bypasses the journal. Recovery is replaying the journal, so
// journalling the replay would append every recovered record back into the log
// on every restart, and the log would grow without bound across restarts even
// on an idle server.
//
// It also bypasses the expiry check: a record that was already expired when the
// process died is loaded as-is and reclaimed lazily on first touch, exactly as
// it would have been had the process never restarted. Dropping it here instead
// would make recovery's result depend on how long the outage lasted.
type Loader struct{ s *Store }

// Loader returns an applier for write-ahead log replay and snapshot loading.
// It is not safe to use concurrently with serving traffic.
func (s *Store) Loader() *Loader { return &Loader{s: s} }

// Put installs a key's record verbatim.
func (l *Loader) Put(key []byte, rec store.Record) error {
	sh := l.s.shardFor(key)
	sh.mu.Lock()
	sh.putLocked(string(key), rec.Clone())
	sh.mu.Unlock()
	return nil
}

// Delete removes a key.
func (l *Loader) Delete(key []byte) error {
	sh := l.s.shardFor(key)
	sh.mu.Lock()
	if cur, ok := sh.m[string(key)]; ok {
		sh.evictLocked(string(key), cur)
	}
	sh.mu.Unlock()
	return nil
}

// FlushAll empties the keyspace.
func (l *Loader) FlushAll() error {
	for i := range l.s.shards {
		sh := &l.s.shards[i]
		sh.mu.Lock()
		sh.m = make(map[string]store.Record)
		sh.volatile = make(map[string]struct{})
		sh.bytes = 0
		sh.mu.Unlock()
	}
	return nil
}

// Batch applies a group of mutations.
func (l *Loader) Batch(muts []store.Mutation) error {
	for _, m := range muts {
		if m.Delete {
			if err := l.Delete(m.Key); err != nil {
				return err
			}
			continue
		}
		if err := l.Put(m.Key, m.Record); err != nil {
			return err
		}
	}
	return nil
}

// ForEach visits every record currently in the engine, including expired ones
// that have not yet been reclaimed.
//
// The walk is shard by shard and takes only one shard lock at a time, so it
// does not stall the whole server, and consequently it does not observe a
// single instant: writes landing in a shard already visited are missed and
// writes landing in a shard not yet visited are included. Snapshotting relies
// on that being safe, and it is, because a snapshot is paired with the log
// sequence number taken before the walk began. Every mutation the walk raced
// with is still in the log after that point and gets replayed on top.
func (s *Store) ForEach(fn func(key []byte, rec store.Record) bool) {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		batch := make([]scanned, 0, len(sh.m))
		for k, rec := range sh.m {
			batch = append(batch, scanned{key: []byte(k), rec: rec.Clone()})
		}
		sh.mu.RUnlock()
		for _, e := range batch {
			if !fn(e.key, e.rec) {
				return
			}
		}
	}
}
