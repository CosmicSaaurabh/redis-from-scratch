package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/clock"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
)

// SyncPolicy decides when log bytes are forced to stable storage.
type SyncPolicy int

const (
	// SyncAlways fsyncs before a write is acknowledged. A power cut loses
	// nothing that was acknowledged. This is the only policy that can honestly
	// be called durable.
	SyncAlways SyncPolicy = iota
	// SyncEverySec hands bytes to the kernel before acknowledging and fsyncs
	// once a second. Killing the process loses nothing, because the data is
	// already in the page cache. Losing power loses up to one second.
	SyncEverySec
	// SyncNever never fsyncs explicitly and leaves the decision to the kernel.
	SyncNever
)

// ParseSyncPolicy maps a config string to a policy.
func ParseSyncPolicy(s string) (SyncPolicy, error) {
	switch s {
	case "always":
		return SyncAlways, nil
	case "everysec", "":
		return SyncEverySec, nil
	case "no", "never":
		return SyncNever, nil
	default:
		return 0, fmt.Errorf("wal: unknown sync policy %q (want always, everysec or no)", s)
	}
}

// String renders the policy as its config spelling.
func (p SyncPolicy) String() string {
	switch p {
	case SyncAlways:
		return "always"
	case SyncEverySec:
		return "everysec"
	default:
		return "no"
	}
}

const (
	segMagic       = "RFSWAL\x01\x00" // 8 bytes; keep in sync with segHeaderSize
	segHeaderSize  = 8 + 8 + 8 + 4    // magic, firstLSN, createdAtMs, crc
	segSuffix      = ".wal"
	defaultSegSize = 64 << 20
	defaultBufSize = 1 << 20
	bgFlushEvery   = 100 * time.Millisecond
)

// Options configures a Log.
type Options struct {
	// Dir is the directory holding segment files. It is created if absent.
	Dir string
	// SegmentSize is the byte threshold at which a new segment is started.
	SegmentSize int64
	// Policy selects the fsync discipline.
	Policy SyncPolicy
	// MaxBufferBytes forces a flush once this many unwritten bytes accumulate,
	// bounding the memory a burst of pipelined writes can pin.
	MaxBufferBytes int
	// Clock supplies timestamps for segment headers.
	Clock clock.Clock
	// Logger receives background I/O failures. Foreground failures are
	// returned to the caller instead.
	Logger *slog.Logger
}

func (o *Options) withDefaults() {
	if o.SegmentSize <= 0 {
		o.SegmentSize = defaultSegSize
	}
	if o.MaxBufferBytes <= 0 {
		o.MaxBufferBytes = defaultBufSize
	}
	if o.Clock == nil {
		o.Clock = clock.System{}
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Log is an append-only, segmented, checksummed write-ahead log.
//
// Concurrency shape. Two locks, never held at the same time in a way that can
// invert:
//
//	mu      guards the in-memory append buffer and the LSN counter. Held for
//	        the duration of a memcpy and nothing else, so it is uncontended in
//	        practice even under heavy pipelining.
//	flushMu serialises everything that touches the file. It is also the
//	        group-commit gate: writers pile up behind it, and the one that gets
//	        in flushes every record buffered so far, so N concurrent writers
//	        cost one fsync rather than N.
type Log struct {
	opt Options

	mu      sync.Mutex
	buf     []byte
	spare   []byte
	nextLSN uint64

	flushMu sync.Mutex
	f       *os.File
	segSeq  uint64
	segLen  int64

	// lastAppended mirrors nextLSN-1 outside the mutex so that Flush and Sync
	// can answer "there is nothing to do" without touching a lock at all.
	// Read-only workloads call Flush once per pipeline batch, and making 50
	// connections queue on a mutex to each discover an empty buffer was
	// measurably more expensive than the write it was trying to batch.
	lastAppended atomic.Uint64
	writtenLSN   atomic.Uint64
	syncedLSN    atomic.Uint64

	appends  atomic.Uint64
	fsyncs   atomic.Uint64
	writes   atomic.Uint64
	bytesOut atomic.Uint64

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	closed atomic.Bool
	// fatal latches the first background I/O failure. Once the log cannot be
	// written, continuing to accept writes would mean acknowledging data that
	// is guaranteed to be lost. Every subsequent append fails instead, which
	// turns a silent durability hole into a loud outage.
	fatal atomic.Pointer[error]
}

// Fatal returns the latched I/O failure, if the log has taken one.
func (l *Log) Fatal() error {
	if p := l.fatal.Load(); p != nil {
		return *p
	}
	return nil
}

func (l *Log) latch(err error) error {
	if err == nil {
		return nil
	}
	l.fatal.CompareAndSwap(nil, &err)
	return err
}

// Open prepares a log for appending in opt.Dir, continuing after startLSN.
//
// It always begins a fresh segment rather than reopening the last one. That is
// a deliberate simplification with a safety payoff: recovery may have
// truncated a torn tail off the previous segment, and never appending to a
// file that was just rewritten removes an entire class of "wrote past the
// truncation point" bug.
func Open(opt Options, startLSN uint64) (*Log, error) {
	opt.withDefaults()
	if opt.Dir == "" {
		return nil, fmt.Errorf("wal: Dir is required")
	}
	if err := os.MkdirAll(opt.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: create dir: %w", err)
	}

	segs, err := listSegments(opt.Dir)
	if err != nil {
		return nil, err
	}
	nextSeq := uint64(1)
	if n := len(segs); n > 0 {
		nextSeq = segs[n-1].seq + 1
	}

	l := &Log{
		opt:     opt,
		buf:     make([]byte, 0, opt.MaxBufferBytes),
		spare:   make([]byte, 0, opt.MaxBufferBytes),
		nextLSN: startLSN + 1,
		segSeq:  nextSeq,
		stop:    make(chan struct{}),
	}
	l.lastAppended.Store(startLSN)
	l.writtenLSN.Store(startLSN)
	l.syncedLSN.Store(startLSN)

	if err := l.openSegment(nextSeq, l.nextLSN); err != nil {
		return nil, err
	}

	l.wg.Add(1)
	go l.background()
	return l, nil
}

// Policy reports the configured fsync discipline.
func (l *Log) Policy() SyncPolicy { return l.opt.Policy }

// LastLSN returns the highest LSN handed out.
func (l *Log) LastLSN() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nextLSN - 1
}

// SyncedLSN returns the highest LSN known to be on stable storage.
func (l *Log) SyncedLSN() uint64 { return l.syncedLSN.Load() }

// AppendPut records a key write and returns its LSN.
func (l *Log) AppendPut(key []byte, rec store.Record) (uint64, error) {
	return l.append(RecPut, func(dst []byte) []byte {
		return appendPutPayload(dst, key, rec)
	})
}

// AppendDelete records a key removal and returns its LSN.
func (l *Log) AppendDelete(key []byte) (uint64, error) {
	return l.append(RecDelete, func(dst []byte) []byte {
		return appendDeletePayload(dst, key)
	})
}

// AppendFlushAll records a full keyspace wipe and returns its LSN.
func (l *Log) AppendFlushAll() (uint64, error) {
	return l.append(RecFlushAll, func(dst []byte) []byte { return dst })
}

// AppendBatch records mutations that must replay atomically.
func (l *Log) AppendBatch(muts []store.Mutation) (uint64, error) {
	return l.append(RecBatch, func(dst []byte) []byte {
		return appendBatchPayload(dst, muts)
	})
}

// scratchPool amortises the temporary payload buffer across appends. The
// payload has to be materialised before framing because the record header
// carries its length.
var scratchPool = sync.Pool{New: func() any { b := make([]byte, 0, 512); return &b }}

func (l *Log) append(typ RecordType, encode func([]byte) []byte) (uint64, error) {
	if l.closed.Load() {
		return 0, store.ErrClosed
	}
	if err := l.Fatal(); err != nil {
		return 0, err
	}
	sp, ok := scratchPool.Get().(*[]byte)
	if !ok {
		// Cannot happen: the pool's New returns exactly this type. Falling back
		// rather than asserting keeps a pool misconfiguration from panicking on
		// the write path.
		buf := make([]byte, 0, 512)
		sp = &buf
	}
	payload := encode((*sp)[:0])

	l.mu.Lock()
	lsn := l.nextLSN
	l.nextLSN++
	l.buf = appendRecord(l.buf, typ, lsn, payload)
	overflow := len(l.buf) >= l.opt.MaxBufferBytes
	l.lastAppended.Store(lsn)
	l.mu.Unlock()

	*sp = payload[:0]
	scratchPool.Put(sp)
	l.appends.Add(1)

	if overflow {
		if err := l.Flush(); err != nil {
			return lsn, err
		}
	}
	return lsn, nil
}

// Flush hands every buffered record to the kernel. It does not fsync.
//
// The server calls this once per pipeline batch, before it flushes replies to
// the socket. That ordering is the guarantee behind the everysec policy: a
// client never sees an acknowledgement whose log bytes are still sitting in
// this process's memory, so killing the process cannot lose acknowledged
// writes even though a power cut still can.
func (l *Log) Flush() error {
	if l.closed.Load() {
		return store.ErrClosed
	}
	if l.lastAppended.Load() <= l.writtenLSN.Load() {
		return nil
	}
	l.flushMu.Lock()
	defer l.flushMu.Unlock()
	// Re-check under the gate. Between the fast path above and acquiring the
	// lock another goroutine may already have written these records, and this
	// is where that turns into a skipped syscall rather than an empty one.
	if l.lastAppended.Load() <= l.writtenLSN.Load() {
		return nil
	}
	_, err := l.drainLocked()
	return err
}

// Sync blocks until every record up to and including lsn is on stable storage.
//
// Under SyncAlways this is the group commit point. Writers queue on flushMu;
// whichever one gets in flushes and fsyncs everything buffered so far, and the
// writers behind it find syncedLSN already past their own LSN and return
// without issuing a second fsync.
func (l *Log) Sync(lsn uint64) error {
	if l.closed.Load() {
		return store.ErrClosed
	}
	if l.syncedLSN.Load() >= lsn {
		return nil
	}
	l.flushMu.Lock()
	defer l.flushMu.Unlock()
	if l.syncedLSN.Load() >= lsn {
		return nil
	}
	return l.syncLocked()
}

// SyncAll forces everything buffered so far to stable storage.
func (l *Log) SyncAll() error {
	if l.closed.Load() {
		return store.ErrClosed
	}
	l.flushMu.Lock()
	defer l.flushMu.Unlock()
	return l.syncLocked()
}

// syncLocked flushes then fsyncs. Caller holds flushMu.
func (l *Log) syncLocked() error {
	upto, err := l.drainLocked()
	if err != nil {
		return err
	}
	if upto == 0 {
		upto = l.writtenLSN.Load()
	}
	if l.syncedLSN.Load() >= upto {
		return nil
	}
	if err := l.f.Sync(); err != nil {
		return l.latch(fmt.Errorf("wal: fsync: %w", err))
	}
	l.fsyncs.Add(1)
	l.syncedLSN.Store(upto)
	return nil
}

// drainLocked writes the pending buffer to the current segment, rotating if it
// has grown past the segment size. It returns the highest LSN now in the file.
// Caller holds flushMu.
func (l *Log) drainLocked() (uint64, error) {
	l.mu.Lock()
	if len(l.buf) == 0 {
		l.mu.Unlock()
		return l.writtenLSN.Load(), nil
	}
	pending := l.buf
	upto := l.nextLSN - 1
	l.buf = l.spare[:0]
	l.spare = pending
	l.mu.Unlock()

	if _, err := l.f.Write(pending); err != nil {
		// The buffer is gone but the bytes may be partially written. Recovery
		// handles that: the torn record fails its checksum and is truncated.
		// Reporting the error is what stops the caller acknowledging the write.
		return 0, l.latch(fmt.Errorf("wal: write segment %d: %w", l.segSeq, err))
	}
	l.writes.Add(1)
	l.bytesOut.Add(uint64(len(pending)))
	l.segLen += int64(len(pending))
	l.writtenLSN.Store(upto)

	// Rotation is evaluated per drain, not per record, so a segment can
	// overshoot SegmentSize by up to one buffer. With the defaults that is at
	// most 1 MiB on a 64 MiB segment, which is not worth splitting a write for.
	if l.segLen >= l.opt.SegmentSize {
		if err := l.rotateLocked(upto + 1); err != nil {
			return upto, err
		}
	}
	return upto, nil
}

// rotateLocked closes the current segment durably and starts a new one.
func (l *Log) rotateLocked(firstLSN uint64) error {
	// fsync before closing regardless of policy. Rotation is the one moment
	// the old segment stops receiving writes, so it is the cheapest possible
	// place to make it durable, and leaving a sealed segment unsynced would
	// mean a crash could lose data the log has already moved past.
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("wal: fsync before rotate: %w", err)
	}
	l.fsyncs.Add(1)
	l.syncedLSN.Store(l.writtenLSN.Load())
	if err := l.f.Close(); err != nil {
		return fmt.Errorf("wal: close segment %d: %w", l.segSeq, err)
	}
	return l.openSegment(l.segSeq+1, firstLSN)
}

// openSegment creates segment seq and writes its header.
func (l *Log) openSegment(seq, firstLSN uint64) error {
	path := segmentPath(l.opt.Dir, seq)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("wal: create segment %s: %w", path, err)
	}
	hdr := encodeSegHeader(firstLSN, l.opt.Clock.NowMs())
	if _, err := f.Write(hdr); err != nil {
		_ = f.Close()
		return fmt.Errorf("wal: write segment header: %w", err)
	}
	// The header must be durable and the directory entry must be durable
	// before any record lands in this file. Otherwise a crash can leave a
	// segment whose records exist but whose name does not, and recovery would
	// silently skip them.
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("wal: fsync segment header: %w", err)
	}
	if err := syncDir(l.opt.Dir); err != nil {
		_ = f.Close()
		return err
	}
	l.f = f
	l.segSeq = seq
	l.segLen = int64(len(hdr))
	return nil
}

// background flushes periodically so that an idle-but-not-empty buffer cannot
// sit in user space indefinitely, and fsyncs once a second under everysec.
func (l *Log) background() {
	defer l.wg.Done()
	flushTick := time.NewTicker(bgFlushEvery)
	defer flushTick.Stop()
	syncTick := time.NewTicker(time.Second)
	defer syncTick.Stop()

	for {
		select {
		case <-l.stop:
			return
		case <-flushTick.C:
			if err := l.Flush(); err != nil {
				l.opt.Logger.Error("wal background flush failed", "err", err)
			}
		case <-syncTick.C:
			if l.opt.Policy == SyncEverySec {
				if err := l.SyncAll(); err != nil {
					l.opt.Logger.Error("wal background fsync failed", "err", err)
				}
			}
		}
	}
}

// Stats reports counters for INFO.
type Stats struct {
	Appends    uint64
	Writes     uint64
	Fsyncs     uint64
	BytesOut   uint64
	LastLSN    uint64
	SyncedLSN  uint64
	SegmentSeq uint64
	Policy     string
}

// Stats snapshots the log's counters.
func (l *Log) Stats() Stats {
	return Stats{
		Appends:    l.appends.Load(),
		Writes:     l.writes.Load(),
		Fsyncs:     l.fsyncs.Load(),
		BytesOut:   l.bytesOut.Load(),
		LastLSN:    l.LastLSN(),
		SyncedLSN:  l.syncedLSN.Load(),
		SegmentSeq: l.segSeq,
		Policy:     l.opt.Policy.String(),
	}
}

// Close stops the background worker, flushes, fsyncs and closes the segment.
// It is idempotent.
func (l *Log) Close() error {
	if !l.closed.CompareAndSwap(false, true) {
		return nil
	}
	l.stopOnce.Do(func() { close(l.stop) })
	l.wg.Wait()

	l.flushMu.Lock()
	defer l.flushMu.Unlock()
	// closed is already true, so re-enter the internals directly rather than
	// through the guarded public methods.
	if _, err := l.drainLocked(); err != nil {
		_ = l.f.Close()
		return err
	}
	if err := l.f.Sync(); err != nil {
		_ = l.f.Close()
		return fmt.Errorf("wal: fsync on close: %w", err)
	}
	l.syncedLSN.Store(l.writtenLSN.Load())
	return l.f.Close()
}

// TrimTo removes segments entirely superseded by a snapshot covering safeLSN.
//
// A segment is only removable when the segment after it starts at or before
// safeLSN+1, which proves every record in the candidate is already captured.
// The caller must have made the snapshot durable first; deleting log segments
// before the snapshot they rely on is fsynced is the classic way to build a
// database that loses data on exactly one reboot in a thousand.
func (l *Log) TrimTo(safeLSN uint64) (removed int, err error) {
	l.flushMu.Lock()
	defer l.flushMu.Unlock()

	segs, err := listSegments(l.opt.Dir)
	if err != nil {
		return 0, err
	}
	for i := 0; i+1 < len(segs); i++ {
		if segs[i].seq == l.segSeq {
			break
		}
		if segs[i+1].firstLSN > safeLSN+1 {
			break
		}
		if err := os.Remove(segs[i].path); err != nil && !os.IsNotExist(err) {
			return removed, fmt.Errorf("wal: remove segment %s: %w", segs[i].path, err)
		}
		removed++
	}
	if removed > 0 {
		if err := syncDir(l.opt.Dir); err != nil {
			return removed, err
		}
	}
	return removed, nil
}

type segmentInfo struct {
	seq      uint64
	firstLSN uint64
	path     string
	size     int64
}

func segmentPath(dir string, seq uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%012d%s", seq, segSuffix))
}

// listSegments returns the segments in dir ordered by sequence number.
func listSegments(dir string) ([]segmentInfo, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("wal: read dir: %w", err)
	}
	var out []segmentInfo
	for _, e := range ents {
		if e.IsDir() || filepath.Ext(e.Name()) != segSuffix {
			continue
		}
		var seq uint64
		if _, err := fmt.Sscanf(e.Name(), "%012d"+segSuffix, &seq); err != nil {
			continue
		}
		path := filepath.Join(dir, e.Name())
		info, err := e.Info()
		if err != nil {
			return nil, fmt.Errorf("wal: stat %s: %w", path, err)
		}
		first, err := readSegHeader(path)
		if err != nil {
			return nil, err
		}
		out = append(out, segmentInfo{seq: seq, firstLSN: first, path: path, size: info.Size()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].seq < out[j].seq })
	return out, nil
}

func encodeSegHeader(firstLSN uint64, createdMs int64) []byte {
	b := make([]byte, 0, segHeaderSize)
	b = append(b, segMagic...)
	b = binary.LittleEndian.AppendUint64(b, firstLSN)
	b = binary.LittleEndian.AppendUint64(b, uint64(createdMs))
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

func readSegHeader(path string) (firstLSN uint64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("wal: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	var hdr [segHeaderSize]byte
	if _, err := f.ReadAt(hdr[:], 0); err != nil {
		return 0, fmt.Errorf("wal: read header of %s: %w", path, err)
	}
	return parseSegHeader(hdr[:], path)
}

func parseSegHeader(hdr []byte, path string) (uint64, error) {
	if len(hdr) < segHeaderSize || string(hdr[:len(segMagic)]) != segMagic {
		return 0, fmt.Errorf("%w: %s has bad magic", ErrCorrupt, path)
	}
	want := binary.LittleEndian.Uint32(hdr[segHeaderSize-4:])
	if got := crc32.Checksum(hdr[:segHeaderSize-4], castagnoli); got != want {
		return 0, fmt.Errorf("%w: %s header checksum %08x != %08x", ErrCorrupt, path, got, want)
	}
	return binary.LittleEndian.Uint64(hdr[8:16]), nil
}

// syncDir fsyncs a directory so that file creations and removals inside it are
// durable. Creating a file and fsyncing the file itself is not enough: the
// directory entry that names it lives in a different block.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("wal: open dir for fsync: %w", err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("wal: fsync dir %s: %w", dir, err)
	}
	return nil
}
