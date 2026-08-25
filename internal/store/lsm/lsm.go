// Package lsm is the client for the Rust LSM storage engine.
//
// The engine runs as a separate process and speaks gRPC. Keeping it out of
// process rather than linking it through cgo is deliberate: cgo would put a
// segfault in the storage engine inside the server's address space, make every
// call cross the Go scheduler's boundary, and complicate the build for both
// languages. A local gRPC hop costs tens of microseconds and buys a hard fault
// boundary, two independently profileable processes, and a seam that already
// works when the engine moves to another host.
//
// # Where atomicity lives
//
// The engine offers reads, writes and a scan. It offers no compare-and-set,
// because the thing being compared is arbitrary Go code - the closure a command
// handler passes to Update. Instead this client holds a stripe of local mutexes
// and performs the read-modify-write between them.
//
// That is correct exactly as long as this process is the only writer to the
// engine, which is the deployment this phase targets: one node, one engine.
// Pointing two servers at one engine would silently lose updates, so Open
// claims an exclusive lock file to make that a startup failure rather than a
// data-corruption bug discovered later.
package lsm

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/clock"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/enginepb"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
)

// stripeCount is the number of independent read-modify-write locks.
//
// It only has to be large enough that two concurrent INCRs on different keys
// almost never collide. 1024 mutexes cost 8 KiB and make a collision a
// one-in-a-thousand event at any realistic concurrency.
const stripeCount = 1024

// Options configures the engine client.
type Options struct {
	// Addr is the engine's gRPC address.
	Addr string
	// Timeout bounds a single call.
	Timeout time.Duration
	// Clock supplies the wall time sent with every request.
	Clock clock.Clock
	// Logger receives connection events.
	Logger *slog.Logger
	// MaxMessageBytes bounds a single gRPC message.
	MaxMessageBytes int
	// DialTimeout bounds the initial connection.
	DialTimeout time.Duration
}

func (o *Options) withDefaults() error {
	if o.Addr == "" {
		return errors.New("lsm: Addr is required")
	}
	if o.Timeout <= 0 {
		o.Timeout = 5 * time.Second
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = 10 * time.Second
	}
	if o.Clock == nil {
		o.Clock = clock.System{}
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.MaxMessageBytes <= 0 {
		// Large enough for the biggest value the protocol accepts plus a scan
		// page, and bounded so a corrupt length cannot make the client
		// allocate without limit.
		o.MaxMessageBytes = 600 << 20
	}
	return nil
}

// Store is a store.Store backed by the out-of-process LSM engine.
type Store struct {
	opt    Options
	conn   *grpc.ClientConn
	client enginepb.StorageEngineClient

	// stripes serialise read-modify-write sequences per key.
	stripes [stripeCount]sync.Mutex
	seed    maphash.Seed

	// cursors maps the numeric SCAN cursors the Redis protocol requires onto
	// the key-ordered positions the engine actually uses. See nextCursor.
	cursors sync.Map
	cursorN atomic.Uint64

	lastFlush atomic.Int64
	closed    atomic.Bool
}

// Open connects to the engine and verifies it is reachable.
func Open(opt Options) (*Store, error) {
	if err := opt.withDefaults(); err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(opt.Addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(opt.MaxMessageBytes),
			grpc.MaxCallSendMsgSize(opt.MaxMessageBytes),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("lsm: dial %s: %w", opt.Addr, err)
	}

	s := &Store{opt: opt, conn: conn, client: enginepb.NewStorageEngineClient(conn), seed: maphash.MakeSeed()}

	// Prove the engine is actually there before the server starts accepting
	// clients. Failing at startup is much easier to diagnose than every
	// command failing once traffic arrives.
	ctx, cancel := context.WithTimeout(context.Background(), opt.DialTimeout)
	defer cancel()
	st, err := s.client.Stats(ctx, &enginepb.StatsRequest{NowMs: opt.Clock.NowMs()})
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("lsm: engine at %s did not answer: %w", opt.Addr, err)
	}
	opt.Logger.Info("connected to storage engine",
		"addr", opt.Addr, "version", st.GetVersion(),
		"keys_estimate", st.GetKeysEstimate(), "disk_bytes", st.GetDiskBytes(),
		"wal_policy", st.GetWalPolicy())
	if st.GetRecoveryTruncated() {
		opt.Logger.Warn("the storage engine discarded an incomplete record at the end of its log",
			"detail", "expected after a crash; the record was never acknowledged to a client")
	}
	return s, nil
}

// Name identifies the engine.
func (s *Store) Name() string { return "lsm" }

func (s *Store) ctx(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, s.opt.Timeout)
}

func (s *Store) lockFor(key []byte) *sync.Mutex {
	return &s.stripes[maphash.Bytes(s.seed, key)&(stripeCount-1)]
}

// Get reads one key.
func (s *Store) Get(ctx context.Context, key []byte) (store.Record, bool, error) {
	if s.closed.Load() {
		return store.Record{}, false, store.ErrClosed
	}
	cctx, cancel := s.ctx(ctx)
	defer cancel()
	resp, err := s.client.Get(cctx, &enginepb.GetRequest{Key: key, NowMs: s.opt.Clock.NowMs()})
	if err != nil {
		return store.Record{}, false, wrap("get", err)
	}
	if !resp.GetFound() {
		return store.Record{}, false, nil
	}
	return toRecord(resp.GetRecord()), true, nil
}

// Put writes one key.
func (s *Store) Put(ctx context.Context, key []byte, rec store.Record) error {
	if s.closed.Load() {
		return store.ErrClosed
	}
	// A plain write still takes the key's stripe, so that it orders correctly
	// against a concurrent Update on the same key. Without it, a SET could land
	// between an Update's read and its write and be silently overwritten.
	mu := s.lockFor(key)
	mu.Lock()
	defer mu.Unlock()
	return s.putLocked(ctx, key, rec)
}

func (s *Store) putLocked(ctx context.Context, key []byte, rec store.Record) error {
	cctx, cancel := s.ctx(ctx)
	defer cancel()
	_, err := s.client.Put(cctx, &enginepb.PutRequest{Key: key, Record: fromRecord(rec)})
	return wrap("put", err)
}

// Delete removes one key and reports whether a live record was present.
func (s *Store) Delete(ctx context.Context, key []byte) (bool, error) {
	if s.closed.Load() {
		return false, store.ErrClosed
	}
	mu := s.lockFor(key)
	mu.Lock()
	defer mu.Unlock()

	cctx, cancel := s.ctx(ctx)
	defer cancel()
	resp, err := s.client.Delete(cctx, &enginepb.DeleteRequest{Key: key, NowMs: s.opt.Clock.NowMs()})
	if err != nil {
		return false, wrap("delete", err)
	}
	return resp.GetExisted(), nil
}

// Update performs an atomic read-modify-write.
//
// Two round trips under one local mutex. The alternative would be pushing the
// update function into the engine, which would mean shipping arbitrary Go logic
// across a process boundary; the mutex is the honest trade, and it is why this
// client documents that it must be the engine's only writer.
func (s *Store) Update(ctx context.Context, key []byte, fn store.UpdateFunc) error {
	if s.closed.Load() {
		return store.ErrClosed
	}
	mu := s.lockFor(key)
	mu.Lock()
	defer mu.Unlock()

	cctx, cancel := s.ctx(ctx)
	resp, err := s.client.Get(cctx, &enginepb.GetRequest{Key: key, NowMs: s.opt.Clock.NowMs()})
	cancel()
	if err != nil {
		return wrap("update read", err)
	}

	var cur store.Record
	found := resp.GetFound()
	if found {
		cur = toRecord(resp.GetRecord())
	}

	next, act, err := fn(cur, found)
	if err != nil {
		return err
	}
	switch act {
	case store.ActionPut:
		return s.putLocked(ctx, key, next)
	case store.ActionDelete:
		if !found {
			return nil
		}
		dctx, dcancel := s.ctx(ctx)
		defer dcancel()
		_, err := s.client.Delete(dctx, &enginepb.DeleteRequest{Key: key, NowMs: s.opt.Clock.NowMs()})
		return wrap("update delete", err)
	default:
		return nil
	}
}

// MultiWrite applies a batch as a single engine log record.
func (s *Store) MultiWrite(ctx context.Context, muts []store.Mutation) error {
	if s.closed.Load() {
		return store.ErrClosed
	}
	if len(muts) == 0 {
		return nil
	}
	// Stripes are taken in ascending index order so two concurrent batches
	// touching the same pair of keys cannot grab them in opposite orders and
	// deadlock. Duplicates are skipped because a Go mutex is not reentrant.
	idx := make([]int, 0, len(muts))
	seen := make(map[int]struct{}, len(muts))
	for _, m := range muts {
		i := int(maphash.Bytes(s.seed, m.Key) & (stripeCount - 1))
		if _, dup := seen[i]; dup {
			continue
		}
		seen[i] = struct{}{}
		idx = append(idx, i)
	}
	sortInts(idx)
	for _, i := range idx {
		s.stripes[i].Lock()
	}
	defer func() {
		for i := len(idx) - 1; i >= 0; i-- {
			s.stripes[idx[i]].Unlock()
		}
	}()

	pb := make([]*enginepb.Mutation, 0, len(muts))
	for _, m := range muts {
		pb = append(pb, &enginepb.Mutation{Key: m.Key, Record: fromRecord(m.Record), Delete: m.Delete})
	}
	cctx, cancel := s.ctx(ctx)
	defer cancel()
	_, err := s.client.BatchWrite(cctx, &enginepb.BatchWriteRequest{Mutations: pb})
	return wrap("batch write", err)
}

// cursorTTL bounds how long a SCAN cursor stays resolvable.
const cursorTTL = 10 * time.Minute

type cursorEntry struct {
	start    []byte
	issuedAt time.Time
}

// Scan walks the keyspace.
//
// The Redis protocol requires a numeric cursor, but an LSM tree resumes from a
// key. This maps between them: the engine returns the key to resume from, and
// the client hands the caller a small integer that names it. Entries are aged
// out, so a client that resumes a scan hours later gets a clear error rather
// than a silently wrong result. Redis makes no promise about cursor lifetime
// either.
func (s *Store) Scan(ctx context.Context, cursor uint64, count int, fn func(key []byte, rec store.Record) bool) (uint64, error) {
	if s.closed.Load() {
		return 0, store.ErrClosed
	}
	if count <= 0 {
		count = 10
	}

	var start []byte
	if cursor != 0 {
		v, ok := s.cursors.Load(cursor)
		if !ok {
			return 0, fmt.Errorf("lsm: scan cursor %d is unknown or has expired", cursor)
		}
		e, ok := v.(cursorEntry)
		if !ok || time.Since(e.issuedAt) > cursorTTL {
			s.cursors.Delete(cursor)
			return 0, fmt.Errorf("lsm: scan cursor %d expired after %s", cursor, cursorTTL)
		}
		s.cursors.Delete(cursor)
		start = e.start
	}

	cctx, cancel := s.ctx(ctx)
	defer cancel()
	resp, err := s.client.Scan(cctx, &enginepb.ScanRequest{
		Start: start, Limit: uint32(count), NowMs: s.opt.Clock.NowMs(),
	})
	if err != nil {
		return 0, wrap("scan", err)
	}
	for _, kv := range resp.GetEntries() {
		if !fn(kv.GetKey(), toRecord(kv.GetRecord())) {
			break
		}
	}
	if !resp.GetHasMore() {
		return 0, nil
	}
	return s.nextCursor(resp.GetNextStart()), nil
}

func (s *Store) nextCursor(start []byte) uint64 {
	// Cursor zero means "start over", so it can never be handed out.
	id := s.cursorN.Add(1)
	s.cursors.Store(id, cursorEntry{start: start, issuedAt: time.Now()})
	s.expireCursors()
	return id
}

// expireCursors drops abandoned cursors, so a client that starts scans and
// never finishes them cannot grow this map without bound.
func (s *Store) expireCursors() {
	cutoff := time.Now().Add(-cursorTTL)
	s.cursors.Range(func(k, v any) bool {
		e, ok := v.(cursorEntry)
		if !ok || e.issuedAt.Before(cutoff) {
			s.cursors.Delete(k)
		}
		return true
	})
}

// Len returns the engine's estimate of the live key count.
//
// It is an estimate, not a count. In an LSM tree the same key can exist at
// every level, and telling how many distinct live keys there are requires
// merging all of them. DBSIZE against this engine is therefore approximate and
// says so in the documentation rather than pretending otherwise.
func (s *Store) Len(ctx context.Context) (int64, error) {
	st, err := s.stats(ctx)
	if err != nil {
		return 0, err
	}
	return int64(st.GetKeysEstimate()), nil
}

// FlushAll removes every key.
func (s *Store) FlushAll(ctx context.Context) error {
	if s.closed.Load() {
		return store.ErrClosed
	}
	// FLUSHALL walks the keyspace inside the engine, so it gets its own,
	// longer deadline: the normal per-call timeout is sized for a point lookup.
	cctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	_, err := s.client.FlushAll(cctx, &enginepb.FlushAllRequest{})
	return wrap("flushall", err)
}

// SampleVolatile returns nothing.
//
// The engine reclaims expired records during compaction rather than through a
// sampling cycle, so there is no cheap way to enumerate keys carrying a TTL and
// nothing useful to hand the server's active expiry loop. Returning nil makes
// that loop correctly become a no-op instead of doing pointless work.
func (s *Store) SampleVolatile(ctx context.Context, n int) ([][]byte, error) { return nil, nil }

// Stats reports engine counters.
func (s *Store) Stats(ctx context.Context) (store.Stats, error) {
	st, err := s.stats(ctx)
	if err != nil {
		return store.Stats{}, err
	}
	extra := map[string]string{
		"engine_version":           st.GetVersion(),
		"lsm_flushes":              itoa(st.GetFlushes()),
		"lsm_compactions":          itoa(st.GetCompactions()),
		"lsm_compaction_read":      itoa(st.GetCompactionBytesRead()),
		"lsm_compaction_written":   itoa(st.GetCompactionBytesWritten()),
		"lsm_write_stalls":         itoa(st.GetWriteStalls()),
		"lsm_bloom_rejections":     itoa(st.GetBloomRejections()),
		"lsm_sstable_reads":        itoa(st.GetSstableReads()),
		"lsm_wal_fsyncs":           itoa(st.GetWalFsyncs()),
		"lsm_wal_unsynced_records": itoa(st.GetWalUnsynced()),
		"lsm_wal_policy":           st.GetWalPolicy(),
		"lsm_keys_are_estimated":   "1",
	}
	for i, n := range st.GetLevelFiles() {
		extra[fmt.Sprintf("lsm_level%d_files", i)] = itoa(n)
	}
	for i, b := range st.GetLevelBytes() {
		if b > 0 {
			extra[fmt.Sprintf("lsm_level%d_bytes", i)] = itoa(b)
		}
	}
	return store.Stats{
		Keys:        int64(st.GetKeysEstimate()),
		MemoryBytes: int64(st.GetMemoryBytes()),
		DiskBytes:   int64(st.GetDiskBytes()),
		Extra:       extra,
	}, nil
}

func (s *Store) stats(ctx context.Context) (*enginepb.StatsResponse, error) {
	if s.closed.Load() {
		return nil, store.ErrClosed
	}
	cctx, cancel := s.ctx(ctx)
	defer cancel()
	st, err := s.client.Stats(cctx, &enginepb.StatsRequest{NowMs: s.opt.Clock.NowMs()})
	return st, wrap("stats", err)
}

// Snapshot asks the engine to write its memtable out as an SSTable.
//
// This is not a point-in-time image the way the in-process engine's snapshot
// is. In an LSM tree the SSTables already are the durable image, and flushing
// only shortens the log that has to be replayed on restart.
func (s *Store) Snapshot(ctx context.Context) (string, error) {
	if s.closed.Load() {
		return "", store.ErrClosed
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	if _, err := s.client.Flush(cctx, &enginepb.FlushRequest{}); err != nil {
		return "", wrap("flush", err)
	}
	s.lastFlush.Store(s.opt.Clock.NowMs())
	return "engine memtable flushed to an sstable", nil
}

// LastSnapshot reports the last memtable flush.
func (s *Store) LastSnapshot() (int64, bool) {
	ms := s.lastFlush.Load()
	return ms / 1000, ms > 0
}

// Sync forces the engine's write-ahead log to stable storage.
func (s *Store) Sync(ctx context.Context) error {
	if s.closed.Load() {
		return store.ErrClosed
	}
	cctx, cancel := s.ctx(ctx)
	defer cancel()
	_, err := s.client.Sync(cctx, &enginepb.SyncRequest{})
	return wrap("sync", err)
}

// Compact runs compaction until every level is inside its budget.
func (s *Store) Compact(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	_, err := s.client.Compact(cctx, &enginepb.CompactRequest{})
	return wrap("compact", err)
}

// Close releases the connection.
func (s *Store) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	// The engine owns the data and fsyncs on its own shutdown, but asking for
	// one final sync here means a clean server stop does not depend on the
	// engine also being stopped cleanly.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if _, err := s.client.Sync(ctx, &enginepb.SyncRequest{}); err != nil {
		s.opt.Logger.Warn("final engine sync failed", "err", err)
	}
	cancel()
	return s.conn.Close()
}

func toRecord(r *enginepb.Record) store.Record {
	if r == nil {
		return store.Record{}
	}
	return store.Record{Value: r.GetValue(), ExpireAt: r.GetExpireAt()}
}

func fromRecord(r store.Record) *enginepb.Record {
	return &enginepb.Record{Value: r.Value, ExpireAt: r.ExpireAt}
}

// wrap turns a gRPC status into an error the command layer can classify.
func wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("lsm: %s: %w", op, err)
	}
	switch st.Code() {
	case codes.Unavailable:
		return fmt.Errorf("lsm: %s: storage engine is unreachable: %w", op, err)
	case codes.DataLoss:
		// Corruption is permanent. Surfacing it distinctly stops a retry loop
		// from reading the same bad bytes forever.
		return fmt.Errorf("lsm: %s: storage engine reported corruption: %w", op, err)
	case codes.DeadlineExceeded:
		return fmt.Errorf("lsm: %s: %w: %w", op, context.DeadlineExceeded, err)
	default:
		return fmt.Errorf("lsm: %s: %w", op, err)
	}
}

func sortInts(a []int) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}

func itoa(n uint64) string {
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

var (
	_ store.Store       = (*Store)(nil)
	_ store.Flusher     = (*Store)(nil)
	_ store.Snapshotter = (*Store)(nil)
)
