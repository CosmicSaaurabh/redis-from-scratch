package persist

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/clock"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/wal"
)

func open(t *testing.T, dir string, policy wal.SyncPolicy, saves ...SavePoint) (*Engine, Recovery) {
	t.Helper()
	e, rec, err := Open(Options{
		Dir:         dir,
		SyncPolicy:  policy,
		SegmentSize: 1 << 16,
		SavePoints:  saves,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return e, rec
}

func mustGet(t *testing.T, e *Engine, key string) (string, bool) {
	t.Helper()
	rec, ok, err := e.Get(context.Background(), []byte(key))
	if err != nil {
		t.Fatalf("Get(%s): %v", key, err)
	}
	return string(rec.Value), ok
}

func mustPut(t *testing.T, e *Engine, key, val string) {
	t.Helper()
	if err := e.Put(context.Background(), []byte(key), store.Record{Value: []byte(val)}); err != nil {
		t.Fatalf("Put(%s): %v", key, err)
	}
}

// TestRestartRecoversFromLogAlone is the base durability claim: with no
// snapshot involved, everything acknowledged is still there after a restart.
func TestRestartRecoversFromLogAlone(t *testing.T) {
	dir := t.TempDir()
	e, _ := open(t, dir, wal.SyncAlways)
	for i := 0; i < 500; i++ {
		mustPut(t, e, fmt.Sprintf("k%03d", i), fmt.Sprintf("v%03d", i))
	}
	if _, err := e.Delete(context.Background(), []byte("k010")); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, rec := open(t, dir, wal.SyncAlways)
	defer func() { _ = e2.Close() }()
	if rec.SnapshotLoaded {
		t.Error("no snapshot was ever taken, but one was loaded")
	}
	if rec.LogApplied != 501 {
		t.Errorf("replayed %d records, want 501", rec.LogApplied)
	}
	for i := 0; i < 500; i++ {
		k := fmt.Sprintf("k%03d", i)
		v, ok := mustGet(t, e2, k)
		if i == 10 {
			if ok {
				t.Errorf("%s was deleted before shutdown but came back", k)
			}
			continue
		}
		if !ok || v != fmt.Sprintf("v%03d", i) {
			t.Errorf("%s = %q,%v after restart", k, v, ok)
		}
	}
}

// TestSnapshotThenMoreWritesRecovers exercises the composition: recovery must
// take the image and then the log tail after it, not one or the other.
func TestSnapshotThenMoreWritesRecovers(t *testing.T) {
	dir := t.TempDir()
	e, _ := open(t, dir, wal.SyncAlways)
	for i := 0; i < 100; i++ {
		mustPut(t, e, fmt.Sprintf("pre%03d", i), "before")
	}
	if _, err := e.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		mustPut(t, e, fmt.Sprintf("post%03d", i), "after")
	}
	// Overwrite a key captured in the snapshot; the log tail must win.
	mustPut(t, e, "pre050", "overwritten")
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, rec := open(t, dir, wal.SyncAlways)
	defer func() { _ = e2.Close() }()
	if !rec.SnapshotLoaded {
		t.Fatal("snapshot was not loaded")
	}
	if rec.SnapshotKeys != 100 {
		t.Errorf("snapshot held %d keys, want 100", rec.SnapshotKeys)
	}
	if v, ok := mustGet(t, e2, "pre050"); !ok || v != "overwritten" {
		t.Errorf("pre050 = %q,%v; the log tail did not win over the snapshot", v, ok)
	}
	for i := 0; i < 100; i++ {
		if _, ok := mustGet(t, e2, fmt.Sprintf("pre%03d", i)); !ok {
			t.Fatalf("pre%03d lost", i)
		}
		if _, ok := mustGet(t, e2, fmt.Sprintf("post%03d", i)); !ok {
			t.Fatalf("post%03d lost", i)
		}
	}
}

// TestFuzzySnapshotUnderConcurrentWrites is the load-bearing test for the
// snapshot design. The image is taken without stopping writes, so it is
// guaranteed to be internally inconsistent. Recovery must still land on
// exactly the set of acknowledged writes, because the LSN was captured before
// the walk and the log after it repairs every race.
func TestFuzzySnapshotUnderConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	e, _ := open(t, dir, wal.SyncAlways)

	const writers, each = 8, 400
	var acked atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("w%d:%04d", w, i)
				if err := e.Put(context.Background(), []byte(key), store.Record{Value: []byte(key)}); err != nil {
					t.Errorf("put: %v", err)
					return
				}
				acked.Add(1)
			}
		}(w)
	}

	// Snapshot repeatedly while the writers run, which is the worst case for a
	// fuzzy walk.
	snapDone := make(chan struct{})
	go func() {
		defer close(snapDone)
		for i := 0; i < 6; i++ {
			if _, err := e.Snapshot(context.Background()); err != nil {
				t.Errorf("snapshot: %v", err)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(stop)
	<-snapDone
	total := acked.Load()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, _ := open(t, dir, wal.SyncAlways)
	defer func() { _ = e2.Close() }()
	n, err := e2.Len(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != total {
		t.Fatalf("recovered %d keys, acknowledged %d", n, total)
	}
	for w := 0; w < writers; w++ {
		for i := 0; i < each; i++ {
			key := fmt.Sprintf("w%d:%04d", w, i)
			if v, ok := mustGet(t, e2, key); !ok || v != key {
				t.Fatalf("%s = %q,%v after fuzzy snapshot recovery", key, v, ok)
			}
		}
	}
}

// TestAbandonedEngineRecovers stands in for kill -9: the engine is never
// closed, so nothing runs on the way out. Under SyncAlways every acknowledged
// write is already fsynced, so all of them must come back.
func TestAbandonedEngineRecovers(t *testing.T) {
	dir := t.TempDir()
	e, _, err := Open(Options{Dir: dir, SyncPolicy: wal.SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		mustPut(t, e, fmt.Sprintf("k%03d", i), "durable")
	}
	// Deliberately no Close: simulate the process disappearing.

	e2, rec := open(t, dir, wal.SyncAlways)
	defer func() { _ = e2.Close() }()
	if rec.LogApplied != 200 {
		t.Fatalf("recovered %d of 200 acknowledged writes", rec.LogApplied)
	}
	n, _ := e2.Len(context.Background())
	if n != 200 {
		t.Fatalf("Len = %d want 200", n)
	}
}

func TestExpirySurvivesRestartAsAbsoluteTime(t *testing.T) {
	dir := t.TempDir()
	clk := clock.NewMock(time.UnixMilli(1_700_000_000_000))
	e, _, err := Open(Options{Dir: dir, SyncPolicy: wal.SyncAlways, Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := e.Put(ctx, []byte("soon"), store.Record{Value: []byte("v"), ExpireAt: clk.NowMs() + 1000}); err != nil {
		t.Fatal(err)
	}
	if err := e.Put(ctx, []byte("later"), store.Record{Value: []byte("v"), ExpireAt: clk.NowMs() + 3_600_000}); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// The "outage" lasts longer than the short TTL. An absolute expiry means
	// the key is gone on restart; a relative one would silently resurrect it
	// for another second.
	clk.Advance(10 * time.Second)
	e2, _, err := Open(Options{Dir: dir, SyncPolicy: wal.SyncAlways, Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e2.Close() }()

	if _, ok := mustGet(t, e2, "soon"); ok {
		t.Error("a key whose TTL elapsed during the outage came back alive")
	}
	if _, ok := mustGet(t, e2, "later"); !ok {
		t.Error("a key with time left on its TTL was lost")
	}
}

func TestFlushAllIsJournalled(t *testing.T) {
	dir := t.TempDir()
	e, _ := open(t, dir, wal.SyncAlways)
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		mustPut(t, e, fmt.Sprintf("k%02d", i), "v")
	}
	if err := e.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	mustPut(t, e, "after", "flush")
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, _ := open(t, dir, wal.SyncAlways)
	defer func() { _ = e2.Close() }()
	n, _ := e2.Len(ctx)
	if n != 1 {
		t.Fatalf("Len = %d after FLUSHALL then one write, want 1", n)
	}
	if v, ok := mustGet(t, e2, "after"); !ok || v != "flush" {
		t.Fatalf("after = %q,%v", v, ok)
	}
}

func TestMultiWriteReplaysAtomically(t *testing.T) {
	dir := t.TempDir()
	e, _ := open(t, dir, wal.SyncAlways)
	ctx := context.Background()
	muts := []store.Mutation{
		{Key: []byte("a"), Record: store.Record{Value: []byte("1")}},
		{Key: []byte("b"), Record: store.Record{Value: []byte("2")}},
		{Key: []byte("c"), Record: store.Record{Value: []byte("3")}},
	}
	if err := e.MultiWrite(ctx, muts); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, rec := open(t, dir, wal.SyncAlways)
	defer func() { _ = e2.Close() }()
	if rec.LogApplied != 1 {
		t.Errorf("batch replayed as %d records, want a single atomic record", rec.LogApplied)
	}
	for _, k := range []string{"a", "b", "c"} {
		if _, ok := mustGet(t, e2, k); !ok {
			t.Errorf("%s missing after batch replay", k)
		}
	}
}

// TestSnapshotTrimsLogAndStillRecovers proves trimming is safe: after old
// segments are deleted, the snapshot plus what remains still reconstructs
// everything.
func TestSnapshotTrimsLogAndStillRecovers(t *testing.T) {
	dir := t.TempDir()
	e, _, err := Open(Options{Dir: dir, SyncPolicy: wal.SyncAlways, SegmentSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2000; i++ {
		mustPut(t, e, fmt.Sprintf("k%05d", i), "payload-payload-payload")
	}
	if _, err := e.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 2000; i < 2100; i++ {
		mustPut(t, e, fmt.Sprintf("k%05d", i), "tail")
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, rec := open(t, dir, wal.SyncAlways)
	defer func() { _ = e2.Close() }()
	if !rec.SnapshotLoaded {
		t.Fatal("snapshot not loaded")
	}
	n, _ := e2.Len(context.Background())
	if n != 2100 {
		t.Fatalf("Len = %d want 2100", n)
	}
	if v, ok := mustGet(t, e2, "k02099"); !ok || v != "tail" {
		t.Fatalf("post-snapshot key lost: %q %v", v, ok)
	}
	if v, ok := mustGet(t, e2, "k00000"); !ok || v != "payload-payload-payload" {
		t.Fatalf("pre-snapshot key lost after trim: %q %v", v, ok)
	}
}

func TestSavePointTriggersBackgroundSnapshot(t *testing.T) {
	dir := t.TempDir()
	e, _, err := Open(Options{
		Dir:        dir,
		SyncPolicy: wal.SyncEverySec,
		SavePoints: []SavePoint{{After: 0, Changes: 10}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()

	for i := 0; i < 50; i++ {
		mustPut(t, e, fmt.Sprintf("k%02d", i), "v")
	}
	deadline := time.After(5 * time.Second)
	for e.PersistenceStats().Saves == 0 {
		select {
		case <-deadline:
			t.Fatal("no background snapshot after 5s with a satisfied save point")
		case <-time.After(50 * time.Millisecond):
		}
	}
	if e.Dirty() != 0 {
		t.Errorf("dirty counter = %d after a snapshot, want 0", e.Dirty())
	}
}

func TestUpdateIsAtomicAcrossGoroutines(t *testing.T) {
	dir := t.TempDir()
	e, _ := open(t, dir, wal.SyncEverySec)
	ctx := context.Background()

	const goroutines, incs = 16, 500
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < incs; i++ {
				err := e.Update(ctx, []byte("counter"), func(cur store.Record, found bool) (store.Record, store.Action, error) {
					n := int64(0)
					if found {
						v, ok := parseI(cur.Value)
						if !ok {
							return store.Record{}, store.ActionNone, fmt.Errorf("bad counter %q", cur.Value)
						}
						n = v
					}
					return store.Record{Value: fmtI(n + 1)}, store.ActionPut, nil
				})
				if err != nil {
					t.Errorf("update: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	v, ok := mustGet(t, e, "counter")
	if !ok {
		t.Fatal("counter missing")
	}
	if v != string(fmtI(goroutines*incs)) {
		t.Fatalf("counter = %s, want %d: lost updates", v, goroutines*incs)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, _ := open(t, dir, wal.SyncEverySec)
	defer func() { _ = e2.Close() }()
	if v2, _ := mustGet(t, e2, "counter"); v2 != v {
		t.Fatalf("counter after restart = %s, want %s: the log and memory diverged", v2, v)
	}
}

func parseI(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	var v int64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		v = v*10 + int64(c-'0')
	}
	return v, true
}

func fmtI(n int64) []byte {
	if n == 0 {
		return []byte("0")
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return append([]byte(nil), b[i:]...)
}
