// Package persist composes the in-memory engine, the write-ahead log and
// snapshots into one durable store.
//
// Recovery order is the whole design in three lines: load the snapshot, learn
// the log sequence number it was safe from, then replay every log record after
// that. Snapshots bound how much log has to be replayed; the log bounds how
// much a snapshot is allowed to miss. Neither is sufficient alone.
package persist

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/clock"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/snapshot"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store/memory"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/wal"
)

// SavePoint triggers a background snapshot once at least Changes mutations
// have accumulated over at least After. Several save points can be configured;
// the first one satisfied wins, which is how Redis expresses "snapshot often
// when busy, rarely when idle" with one knob.
type SavePoint struct {
	After   time.Duration
	Changes uint64
}

// Options configures the durable engine.
type Options struct {
	// Dir is the data directory. The log lives in Dir/wal and snapshots in
	// Dir/snapshot.
	Dir string
	// SyncPolicy is one of "always", "everysec" or "no".
	SyncPolicy wal.SyncPolicy
	// SegmentSize is the write-ahead log segment rotation threshold.
	SegmentSize int64
	// SavePoints schedule background snapshots. Empty disables them.
	SavePoints []SavePoint
	// Clock supplies time; tests inject a mock.
	Clock clock.Clock
	// Logger receives recovery and checkpoint events.
	Logger *slog.Logger
}

// Recovery reports what happened at startup. It is surfaced through INFO and
// logged, because "we silently lost the tail of your log" must never be
// something an operator has to infer.
type Recovery struct {
	SnapshotLoaded bool
	SnapshotKeys   uint64
	SnapshotLSN    uint64
	SnapshotAgeMs  int64
	LogApplied     int
	LogSkipped     int
	LogSegments    int
	LogBytes       int64
	StartLSN       uint64
	Truncated      bool
	TruncatedPath  string
	TruncatedAt    int64
	Elapsed        time.Duration
}

// Engine is a durable store.Store.
type Engine struct {
	*memory.Store

	opt     Options
	dir     string
	walDir  string
	snapDir string
	clk     clock.Clock
	log     atomic.Pointer[wal.Log]

	dirty       atomic.Uint64
	lastSaveMs  atomic.Int64
	lastSaveOK  atomic.Bool
	savesTotal  atomic.Uint64
	savesFailed atomic.Uint64

	// snapMu makes checkpointing single-flight. Two concurrent snapshots would
	// race on the same temporary filename and could trim the log against the
	// older of the two LSNs.
	snapMu sync.Mutex

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
	closed   atomic.Bool

	recovery Recovery
}

// Open recovers the data directory and returns a ready engine.
func Open(opt Options) (*Engine, Recovery, error) {
	if opt.Dir == "" {
		return nil, Recovery{}, errors.New("persist: Dir is required")
	}
	if opt.Clock == nil {
		opt.Clock = clock.System{}
	}
	if opt.Logger == nil {
		opt.Logger = slog.Default()
	}
	started := time.Now()

	e := &Engine{
		opt:     opt,
		dir:     opt.Dir,
		walDir:  filepath.Join(opt.Dir, "wal"),
		snapDir: filepath.Join(opt.Dir, "snapshot"),
		clk:     opt.Clock,
		stop:    make(chan struct{}),
	}
	// The engine is its own journal. The memory store is constructed pointing
	// at it before the log exists; journalling is a no-op until Open installs
	// the log, which is safe because recovery writes through Loader and
	// deliberately does not journal.
	e.Store = memory.New(opt.Clock, e)

	var rec Recovery
	loader := e.Store.Loader()

	info, err := snapshot.Load(e.snapDir, loader)
	switch {
	case err == nil:
		rec.SnapshotLoaded = true
		rec.SnapshotKeys = info.Keys
		rec.SnapshotLSN = info.LSN
		rec.SnapshotAgeMs = opt.Clock.NowMs() - info.CreatedAtMs
	case errors.Is(err, snapshot.ErrNotFound):
		// First boot, or snapshots have never been taken. Not an error.
	default:
		return nil, rec, fmt.Errorf("persist: load snapshot: %w", err)
	}

	res, err := wal.Replay(e.walDir, rec.SnapshotLSN, loader)
	if err != nil {
		return nil, rec, fmt.Errorf("persist: replay log: %w", err)
	}
	rec.LogApplied = res.Applied
	rec.LogSkipped = res.Skipped
	rec.LogSegments = res.Segments
	rec.LogBytes = res.BytesRead
	rec.Truncated = res.Truncated
	rec.TruncatedPath = res.TruncatedPath
	rec.TruncatedAt = res.TruncatedAt
	rec.StartLSN = res.LastLSN

	lg, err := wal.Open(wal.Options{
		Dir:         e.walDir,
		SegmentSize: opt.SegmentSize,
		Policy:      opt.SyncPolicy,
		Clock:       opt.Clock,
		Logger:      opt.Logger,
	}, res.LastLSN)
	if err != nil {
		return nil, rec, fmt.Errorf("persist: open log: %w", err)
	}
	e.log.Store(lg)
	e.lastSaveMs.Store(opt.Clock.NowMs())
	e.lastSaveOK.Store(true)

	rec.Elapsed = time.Since(started)
	e.recovery = rec

	if len(opt.SavePoints) > 0 {
		e.wg.Add(1)
		go e.checkpointLoop()
	}
	return e, rec, nil
}

// Name identifies the engine for INFO.
func (e *Engine) Name() string { return "memory+wal" }

// Recovery reports what startup found.
func (e *Engine) Recovery() Recovery { return e.recovery }

// LogPut implements store.Journal.
func (e *Engine) LogPut(key []byte, rec store.Record) (uint64, error) {
	lg := e.log.Load()
	if lg == nil {
		return 0, nil
	}
	e.dirty.Add(1)
	return lg.AppendPut(key, rec)
}

// LogDelete implements store.Journal.
func (e *Engine) LogDelete(key []byte) (uint64, error) {
	lg := e.log.Load()
	if lg == nil {
		return 0, nil
	}
	e.dirty.Add(1)
	return lg.AppendDelete(key)
}

// LogFlushAll implements store.Journal.
func (e *Engine) LogFlushAll() (uint64, error) {
	lg := e.log.Load()
	if lg == nil {
		return 0, nil
	}
	e.dirty.Add(1)
	return lg.AppendFlushAll()
}

// LogBatch implements store.Journal.
func (e *Engine) LogBatch(muts []store.Mutation) (uint64, error) {
	lg := e.log.Load()
	if lg == nil {
		return 0, nil
	}
	e.dirty.Add(uint64(len(muts)))
	return lg.AppendBatch(muts)
}

// Await implements store.Journal. Under any policy other than always it
// returns immediately, and durability is provided by FlushWAL plus the log's
// own one-second fsync.
func (e *Engine) Await(lsn uint64) error {
	lg := e.log.Load()
	if lg == nil || lg.Policy() != wal.SyncAlways {
		return nil
	}
	return lg.Sync(lsn)
}

// FlushWAL pushes buffered log records to the kernel without fsyncing.
//
// The server calls this once per pipeline batch, immediately before it flushes
// replies to the socket. That is what makes the everysec policy survive a
// kill -9: the acknowledged writes are already in the page cache, which
// outlives the process even though it does not outlive a power cut.
func (e *Engine) FlushWAL() error {
	lg := e.log.Load()
	if lg == nil {
		return nil
	}
	return lg.Flush()
}

// Sync forces everything written so far to stable storage.
func (e *Engine) Sync(ctx context.Context) error {
	lg := e.log.Load()
	if lg == nil {
		return nil
	}
	return lg.SyncAll()
}

// Snapshot writes a point-in-time image and trims the log it supersedes.
func (e *Engine) Snapshot(ctx context.Context) (string, error) {
	e.snapMu.Lock()
	defer e.snapMu.Unlock()

	lg := e.log.Load()
	var lsn uint64
	if lg != nil {
		// Read the LSN before walking the keyspace. Reading it afterwards
		// would claim coverage of writes the walk may have raced past, and
		// recovery would then skip the very log records that repair the gap.
		lsn = lg.LastLSN()
	}
	dirtyAtStart := e.dirty.Load()

	info, err := snapshot.Write(e.snapDir, lsn, e.clk.NowMs(), func(emit snapshot.Emit) error {
		var ferr error
		e.Store.ForEach(func(key []byte, rec store.Record) bool {
			if ferr = emit(key, rec); ferr != nil {
				return false
			}
			return true
		})
		return ferr
	})
	if err != nil {
		e.savesFailed.Add(1)
		e.lastSaveOK.Store(false)
		return "", err
	}

	// Trim only after the snapshot is fsynced and renamed. Reversing these two
	// steps is the classic way to build a database that loses everything on
	// exactly one unlucky reboot.
	if lg != nil {
		removed, err := lg.TrimTo(lsn)
		if err != nil {
			// The image is safe on disk, so this is not data loss - it is disk
			// that will not be reclaimed until the next successful checkpoint.
			e.opt.Logger.Warn("snapshot written but log trim failed", "err", err, "lsn", lsn)
		} else if removed > 0 {
			e.opt.Logger.Info("trimmed superseded log segments", "segments", removed, "lsn", lsn)
		}
	}

	e.dirty.Add(^(dirtyAtStart - 1)) // subtract dirtyAtStart
	e.lastSaveMs.Store(e.clk.NowMs())
	e.lastSaveOK.Store(true)
	e.savesTotal.Add(1)
	e.opt.Logger.Info("snapshot complete",
		"keys", info.Keys, "bytes", info.Bytes, "lsn", info.LSN, "path", info.Path)
	return info.Path, nil
}

// LastSnapshot reports when the last successful snapshot finished.
func (e *Engine) LastSnapshot() (int64, bool) {
	return e.lastSaveMs.Load() / 1000, e.savesTotal.Load() > 0
}

// Dirty reports mutations accumulated since the last snapshot.
func (e *Engine) Dirty() uint64 { return e.dirty.Load() }

// WALStats exposes log counters for INFO.
func (e *Engine) WALStats() (wal.Stats, bool) {
	lg := e.log.Load()
	if lg == nil {
		return wal.Stats{}, false
	}
	return lg.Stats(), true
}

// PersistenceStats exposes checkpoint counters for INFO.
type PersistenceStats struct {
	Saves        uint64
	SavesFailed  uint64
	LastSaveMs   int64
	LastSaveOK   bool
	DirtyChanges uint64
}

// PersistenceStats snapshots checkpoint counters.
func (e *Engine) PersistenceStats() PersistenceStats {
	return PersistenceStats{
		Saves:        e.savesTotal.Load(),
		SavesFailed:  e.savesFailed.Load(),
		LastSaveMs:   e.lastSaveMs.Load(),
		LastSaveOK:   e.lastSaveOK.Load(),
		DirtyChanges: e.dirty.Load(),
	}
}

// checkpointLoop evaluates the configured save points once a second.
func (e *Engine) checkpointLoop() {
	defer e.wg.Done()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-e.stop:
			return
		case <-tick.C:
			if !e.shouldSave() {
				continue
			}
			if _, err := e.Snapshot(context.Background()); err != nil {
				e.opt.Logger.Error("background snapshot failed", "err", err)
			}
		}
	}
}

func (e *Engine) shouldSave() bool {
	dirty := e.dirty.Load()
	if dirty == 0 {
		return false
	}
	elapsed := time.Duration(e.clk.NowMs()-e.lastSaveMs.Load()) * time.Millisecond
	for _, sp := range e.opt.SavePoints {
		if elapsed >= sp.After && dirty >= sp.Changes {
			return true
		}
	}
	return false
}

// Close stops background work, takes a final snapshot when anything is dirty,
// and closes the log durably.
//
// The final snapshot is not required for correctness - the log alone recovers
// the same state - but it makes the next start fast, and a clean shutdown is
// exactly when that is free.
func (e *Engine) Close() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	e.stopOnce.Do(func() { close(e.stop) })
	e.wg.Wait()

	var firstErr error
	if e.dirty.Load() > 0 && len(e.opt.SavePoints) > 0 {
		if _, err := e.Snapshot(context.Background()); err != nil {
			e.opt.Logger.Error("final snapshot failed, recovery will replay the log instead", "err", err)
			firstErr = err
		}
	}
	if lg := e.log.Load(); lg != nil {
		if err := lg.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := e.Store.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

var (
	_ store.Store       = (*Engine)(nil)
	_ store.Journal     = (*Engine)(nil)
	_ store.Flusher     = (*Engine)(nil)
	_ store.Snapshotter = (*Engine)(nil)
)
