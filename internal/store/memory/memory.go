// Package memory implements store.Store as a sharded in-process hash map.
//
// It is the engine behind the "memory" storage mode, where durability comes
// from the write-ahead log and snapshots layered above it, and it is also the
// reference implementation the LSM engine is differentially tested against.
package memory

import (
	"context"
	"hash/maphash"
	"math/rand/v2"
	"sync"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/clock"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
)

// shardCount is the number of independently locked partitions.
//
// It is also the granularity of the SCAN cursor, which is why it is this
// large: a cursor that names a shard rather than a hash-bucket position gives
// SCAN its "present throughout, returned at least once" guarantee for free,
// but only if a single shard is small enough to return in one reply. At 1024
// shards a one-million-key database yields roughly a thousand keys per SCAN
// call, which is a reasonable reply size.
const shardCount = 1024

const shardMask = shardCount - 1

// entryOverhead is a rough per-key accounting constant covering the map
// bucket, the string header and the Record struct. INFO reports an estimate,
// not a measurement, and says so.
const entryOverhead = 64

type shard struct {
	mu sync.RWMutex
	m  map[string]store.Record
	// volatile indexes the subset of m that carries an expiry. The active
	// expiry cycle samples from here instead of from m, so the cost of a cycle
	// scales with the number of keys that actually have a TTL rather than with
	// the size of the whole keyspace.
	volatile map[string]struct{}
	bytes    int64
	// pad keeps adjacent shards off the same cache line. Without it, two
	// goroutines locking different shards can still serialise on the coherence
	// protocol.
	_ [40]byte
}

// Store is a sharded in-memory key-value store.
type Store struct {
	shards [shardCount]shard
	seed   maphash.Seed
	clk    clock.Clock
	// jrn is the write-ahead log, or nil when the engine runs without
	// durability. Every mutation journals through it while the shard lock is
	// held; see store.Journal for why that placement is load-bearing.
	jrn store.Journal

	closed sync.Once
	dead   sync.RWMutex
	isDead bool
}

// New returns an empty store using clk for expiry decisions and jrn for
// durability. A nil jrn makes the engine non-durable, which is what the pure
// cache mode and most unit tests want.
func New(clk clock.Clock, jrn store.Journal) *Store {
	if clk == nil {
		clk = clock.System{}
	}
	s := &Store{seed: maphash.MakeSeed(), clk: clk, jrn: jrn}
	for i := range s.shards {
		s.shards[i].m = make(map[string]store.Record)
		s.shards[i].volatile = make(map[string]struct{})
	}
	return s
}

// Name identifies the engine.
func (s *Store) Name() string { return "memory" }

func (s *Store) shardFor(key []byte) *shard {
	h := maphash.Bytes(s.seed, key)
	return &s.shards[h&shardMask]
}

func (s *Store) alive() error {
	s.dead.RLock()
	defer s.dead.RUnlock()
	if s.isDead {
		return store.ErrClosed
	}
	return nil
}

// Get returns the record for key, treating an expired record as absent.
//
// When it finds an expired record it reclaims it under a write lock. That
// reclaim is deliberately not journalled: the record's absolute ExpireAt is
// already in the log, so a replay reconstructs the same logical state whether
// or not the physical delete was recorded.
func (s *Store) Get(ctx context.Context, key []byte) (store.Record, bool, error) {
	if err := s.alive(); err != nil {
		return store.Record{}, false, err
	}
	now := s.clk.NowMs()
	sh := s.shardFor(key)

	sh.mu.RLock()
	rec, ok := sh.m[string(key)]
	sh.mu.RUnlock()
	if ok && !rec.Expired(now) {
		// Returned without copying. Stored values are immutable: putLocked
		// installs a fresh copy and nothing ever writes through an installed
		// slice, so handing the caller a read-only alias is safe and takes a
		// per-GET allocation off the hot path.
		return rec, true, nil
	}
	if !ok {
		return store.Record{}, false, nil
	}

	sh.mu.Lock()
	if cur, still := sh.m[string(key)]; still && cur.Expired(s.clk.NowMs()) {
		sh.evictLocked(string(key), cur)
	}
	sh.mu.Unlock()
	return store.Record{}, false, nil
}

// Put writes a record unconditionally.
func (s *Store) Put(ctx context.Context, key []byte, rec store.Record) error {
	if err := s.alive(); err != nil {
		return err
	}
	sh := s.shardFor(key)
	sh.mu.Lock()
	lsn, err := s.logPut(key, rec)
	if err != nil {
		sh.mu.Unlock()
		return err
	}
	sh.putLocked(string(key), rec.Clone())
	sh.mu.Unlock()
	return s.await(lsn)
}

// Delete removes a key and reports whether a live record was present.
func (s *Store) Delete(ctx context.Context, key []byte) (bool, error) {
	if err := s.alive(); err != nil {
		return false, err
	}
	now := s.clk.NowMs()
	sh := s.shardFor(key)
	locked := true
	sh.mu.Lock()
	defer func() {
		if locked {
			sh.mu.Unlock()
		}
	}()

	rec, ok := sh.m[string(key)]
	if !ok {
		return false, nil
	}
	lsn, err := s.logDelete(key)
	if err != nil {
		return false, err
	}
	sh.evictLocked(string(key), rec)
	existed := !rec.Expired(now)
	sh.mu.Unlock()
	locked = false
	return existed, s.await(lsn)
}

// Update runs fn with the key held exclusively.
func (s *Store) Update(ctx context.Context, key []byte, fn store.UpdateFunc) error {
	if err := s.alive(); err != nil {
		return err
	}
	now := s.clk.NowMs()
	sh := s.shardFor(key)
	locked := true
	sh.mu.Lock()
	defer func() {
		if locked {
			sh.mu.Unlock()
		}
	}()

	sk := string(key)
	cur, ok := sh.m[sk]
	if ok && cur.Expired(now) {
		sh.evictLocked(sk, cur)
		cur, ok = store.Record{}, false
	}

	next, act, err := fn(cur, ok)
	if err != nil {
		return err
	}
	var lsn uint64
	switch act {
	case store.ActionPut:
		if lsn, err = s.logPut(key, next); err != nil {
			return err
		}
		sh.putLocked(sk, next.Clone())
	case store.ActionDelete:
		if !ok {
			return nil
		}
		if lsn, err = s.logDelete(key); err != nil {
			return err
		}
		sh.evictLocked(sk, cur)
	case store.ActionNone:
		return nil
	}
	sh.mu.Unlock()
	locked = false
	return s.await(lsn)
}

// MultiWrite applies muts. Shard locks are taken in ascending shard order so
// that two concurrent batches touching the same pair of shards can never
// deadlock by grabbing them in opposite orders.
func (s *Store) MultiWrite(ctx context.Context, muts []store.Mutation) error {
	if err := s.alive(); err != nil {
		return err
	}
	if len(muts) == 0 {
		return nil
	}

	idx := make(map[int][]int, len(muts))
	for i, m := range muts {
		h := maphash.Bytes(s.seed, m.Key) & shardMask
		idx[int(h)] = append(idx[int(h)], i)
	}
	order := make([]int, 0, len(idx))
	for k := range idx {
		order = append(order, k)
	}
	sortInts(order)

	// Lock every shard the batch touches before journalling, so that the LSN
	// is assigned and the memory apply happens without any other writer to
	// these keys slipping in between. Ascending shard order makes the
	// multi-lock acquisition deadlock-free.
	for _, si := range order {
		s.shards[si].mu.Lock()
	}
	lsn, err := s.logBatch(muts)
	if err != nil {
		for i := len(order) - 1; i >= 0; i-- {
			s.shards[order[i]].mu.Unlock()
		}
		return err
	}

	for _, si := range order {
		sh := &s.shards[si]
		for _, mi := range idx[si] {
			m := muts[mi]
			sk := string(m.Key)
			if m.Delete {
				if cur, ok := sh.m[sk]; ok {
					sh.evictLocked(sk, cur)
				}
				continue
			}
			sh.putLocked(sk, m.Record.Clone())
		}
	}
	for i := len(order) - 1; i >= 0; i-- {
		s.shards[order[i]].mu.Unlock()
	}
	return s.await(lsn)
}

// Scan walks whole shards at a time. The cursor is the index of the next shard
// to visit, biased by one so that zero can mean both "start" and "finished".
func (s *Store) Scan(ctx context.Context, cursor uint64, count int, fn func(key []byte, rec store.Record) bool) (uint64, error) {
	if err := s.alive(); err != nil {
		return 0, err
	}
	if count <= 0 {
		count = 10
	}
	now := s.clk.NowMs()
	next := int(cursor)
	if next < 0 || next > shardCount {
		return 0, nil
	}

	emitted := 0
	for ; next < shardCount; next++ {
		sh := &s.shards[next]

		// Copy the shard's live records out under the read lock rather than
		// invoking fn while holding it: fn belongs to the command layer and
		// may touch the store again, which would deadlock on a non-reentrant
		// RWMutex.
		sh.mu.RLock()
		batch := make([]scanned, 0, len(sh.m))
		for k, rec := range sh.m {
			if rec.Expired(now) {
				continue
			}
			// The key must be copied because it is a map key string, but the
			// value is immutable and can be aliased.
			batch = append(batch, scanned{key: []byte(k), rec: rec})
		}
		sh.mu.RUnlock()

		for _, e := range batch {
			if !fn(e.key, e.rec) {
				return uint64(next + 1), nil
			}
		}
		emitted += len(batch)
		if emitted >= count {
			next++
			break
		}
	}
	if next >= shardCount {
		return 0, nil
	}
	return uint64(next), nil
}

type scanned struct {
	key []byte
	rec store.Record
}

// Len counts live keys.
func (s *Store) Len(ctx context.Context) (int64, error) {
	if err := s.alive(); err != nil {
		return 0, err
	}
	now := s.clk.NowMs()
	var n int64
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		if len(sh.volatile) == 0 {
			n += int64(len(sh.m))
		} else {
			for _, rec := range sh.m {
				if !rec.Expired(now) {
					n++
				}
			}
		}
		sh.mu.RUnlock()
	}
	return n, nil
}

// FlushAll drops every key.
func (s *Store) FlushAll(ctx context.Context) error {
	if err := s.alive(); err != nil {
		return err
	}
	// Journal before touching any shard. A FLUSHALL that half-applied and then
	// failed to journal would leave memory and the log describing different
	// databases, so the log entry is written first and the wipe follows.
	lsn, err := s.logFlushAll()
	if err != nil {
		return err
	}
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		sh.m = make(map[string]store.Record)
		sh.volatile = make(map[string]struct{})
		sh.bytes = 0
		sh.mu.Unlock()
	}
	return s.await(lsn)
}

// SampleVolatile returns up to n keys carrying an expiry, drawn from randomly
// chosen shards.
func (s *Store) SampleVolatile(ctx context.Context, n int) ([][]byte, error) {
	if err := s.alive(); err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	out := make([][]byte, 0, n)
	// Probe a bounded number of shards so an empty volatile set cannot turn
	// the expiry cycle into a full sweep of all 1024 shards.
	for probes := 0; probes < 32 && len(out) < n; probes++ {
		sh := &s.shards[rand.IntN(shardCount)]
		sh.mu.RLock()
		for k := range sh.volatile {
			out = append(out, []byte(k))
			if len(out) >= n {
				break
			}
		}
		sh.mu.RUnlock()
	}
	return out, nil
}

// Stats reports approximate counters for INFO.
func (s *Store) Stats(ctx context.Context) (store.Stats, error) {
	if err := s.alive(); err != nil {
		return store.Stats{}, err
	}
	var st store.Stats
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		st.Keys += int64(len(sh.m))
		st.VolatileKeys += int64(len(sh.volatile))
		st.MemoryBytes += sh.bytes
		sh.mu.RUnlock()
	}
	st.Extra = map[string]string{"shards": itoa(shardCount)}
	return st, nil
}

// Close makes every subsequent call return store.ErrClosed and releases the
// shard maps.
func (s *Store) Close() error {
	s.closed.Do(func() {
		s.dead.Lock()
		s.isDead = true
		s.dead.Unlock()
		for i := range s.shards {
			sh := &s.shards[i]
			sh.mu.Lock()
			sh.m = nil
			sh.volatile = nil
			sh.bytes = 0
			sh.mu.Unlock()
		}
	})
	return nil
}

func (s *Store) logPut(key []byte, rec store.Record) (uint64, error) {
	if s.jrn == nil {
		return 0, nil
	}
	return s.jrn.LogPut(key, rec)
}

func (s *Store) logDelete(key []byte) (uint64, error) {
	if s.jrn == nil {
		return 0, nil
	}
	return s.jrn.LogDelete(key)
}

func (s *Store) logBatch(muts []store.Mutation) (uint64, error) {
	if s.jrn == nil {
		return 0, nil
	}
	return s.jrn.LogBatch(muts)
}

func (s *Store) logFlushAll() (uint64, error) {
	if s.jrn == nil {
		return 0, nil
	}
	return s.jrn.LogFlushAll()
}

// await blocks until lsn is durable. It runs with no shard lock held, which is
// what lets one fsync cover many concurrent writers instead of serialising
// them behind a held lock.
func (s *Store) await(lsn uint64) error {
	if s.jrn == nil || lsn == 0 {
		return nil
	}
	return s.jrn.Await(lsn)
}

func (sh *shard) putLocked(key string, rec store.Record) {
	if old, ok := sh.m[key]; ok {
		sh.bytes -= int64(len(old.Value))
	} else {
		sh.bytes += int64(len(key)) + entryOverhead
	}
	sh.bytes += int64(len(rec.Value))
	sh.m[key] = rec
	if rec.Volatile() {
		sh.volatile[key] = struct{}{}
	} else {
		delete(sh.volatile, key)
	}
}

func (sh *shard) evictLocked(key string, old store.Record) {
	delete(sh.m, key)
	delete(sh.volatile, key)
	sh.bytes -= int64(len(key)) + entryOverhead + int64(len(old.Value))
	if sh.bytes < 0 {
		sh.bytes = 0
	}
}

func sortInts(a []int) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

var _ store.Store = (*Store)(nil)
