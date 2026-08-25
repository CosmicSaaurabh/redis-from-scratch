package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
)

type collector struct {
	ops []string
}

func (c *collector) Put(key []byte, rec store.Record) error {
	c.ops = append(c.ops, fmt.Sprintf("put %s=%s@%d", key, rec.Value, rec.ExpireAt))
	return nil
}
func (c *collector) Delete(key []byte) error {
	c.ops = append(c.ops, fmt.Sprintf("del %s", key))
	return nil
}
func (c *collector) FlushAll() error {
	c.ops = append(c.ops, "flushall")
	return nil
}
func (c *collector) Batch(muts []store.Mutation) error {
	for _, m := range muts {
		if m.Delete {
			c.ops = append(c.ops, fmt.Sprintf("bdel %s", m.Key))
			continue
		}
		c.ops = append(c.ops, fmt.Sprintf("bput %s=%s", m.Key, m.Record.Value))
	}
	return nil
}

func openLog(t *testing.T, dir string, policy SyncPolicy, segSize int64) *Log {
	t.Helper()
	l, err := Open(Options{Dir: dir, Policy: policy, SegmentSize: segSize}, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, SyncAlways, 0)

	if _, err := l.AppendPut([]byte("a"), store.Record{Value: []byte("1"), ExpireAt: 99}); err != nil {
		t.Fatal(err)
	}
	if _, err := l.AppendDelete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if _, err := l.AppendBatch([]store.Mutation{
		{Key: []byte("c"), Record: store.Record{Value: []byte("3")}},
		{Key: []byte("d"), Delete: true},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := l.AppendFlushAll(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	var c collector
	res, err := Replay(dir, 0, &c)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	want := []string{"put a=1@99", "del b", "bput c=3", "bdel d", "flushall"}
	if len(c.ops) != len(want) {
		t.Fatalf("ops = %v want %v", c.ops, want)
	}
	for i := range want {
		if c.ops[i] != want[i] {
			t.Errorf("op %d = %q want %q", i, c.ops[i], want[i])
		}
	}
	if res.LastLSN != 4 {
		t.Errorf("LastLSN = %d want 4", res.LastLSN)
	}
	if res.Truncated {
		t.Error("clean log reported as truncated")
	}
}

func TestReplaySkipsRecordsCoveredBySnapshot(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, SyncAlways, 0)
	for i := 0; i < 5; i++ {
		if _, err := l.AppendPut([]byte{byte('a' + i)}, store.Record{Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	var c collector
	res, err := Replay(dir, 3, &c)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.ops) != 2 {
		t.Fatalf("applied %d records, want 2: %v", len(c.ops), c.ops)
	}
	if res.Skipped != 3 {
		t.Errorf("Skipped = %d want 3", res.Skipped)
	}
	if res.LastLSN != 5 {
		t.Errorf("LastLSN = %d want 5", res.LastLSN)
	}
}

func TestSegmentRotationAndReplayAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	// A tiny segment size forces a rotation every few records.
	l := openLog(t, dir, SyncAlways, 512)
	const n = 200
	for i := 0; i < n; i++ {
		if _, err := l.AppendPut([]byte(fmt.Sprintf("key%04d", i)), store.Record{Value: []byte("value")}); err != nil {
			t.Fatal(err)
		}
		// Flush the way the server does, once per batch. Rotation is evaluated
		// when bytes reach the file, so a log that never flushes never rotates.
		if err := l.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	segs, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 5 {
		t.Fatalf("expected several segments, got %d", len(segs))
	}

	var c collector
	res, err := Replay(dir, 0, &c)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.ops) != n {
		t.Fatalf("replayed %d of %d records", len(c.ops), n)
	}
	if res.Segments != len(segs) {
		t.Errorf("read %d segments, listed %d", res.Segments, len(segs))
	}
}

// TestTornTailIsTruncated is the crash-recovery contract: an interrupted write
// at the very end of the log is discarded, and everything before it survives.
func TestTornTailIsTruncated(t *testing.T) {
	for _, cut := range []int{1, 5, 12, 20} {
		t.Run(fmt.Sprintf("cut%d", cut), func(t *testing.T) {
			dir := t.TempDir()
			l := openLog(t, dir, SyncAlways, 0)
			for i := 0; i < 10; i++ {
				if _, err := l.AppendPut([]byte{byte('a' + i)}, store.Record{Value: []byte("value")}); err != nil {
					t.Fatal(err)
				}
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}

			segs, err := listSegments(dir)
			if err != nil {
				t.Fatal(err)
			}
			path := segs[len(segs)-1].path
			st, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Truncate(path, st.Size()-int64(cut)); err != nil {
				t.Fatal(err)
			}

			var c collector
			res, err := Replay(dir, 0, &c)
			if err != nil {
				t.Fatalf("Replay after tear: %v", err)
			}
			if !res.Truncated {
				t.Fatal("expected the torn tail to be reported")
			}
			if len(c.ops) != 9 {
				t.Fatalf("recovered %d records, want the 9 complete ones", len(c.ops))
			}
			if res.LastLSN != 9 {
				t.Errorf("LastLSN = %d want 9", res.LastLSN)
			}

			// Truncation must be persistent and idempotent: replaying again
			// finds a clean log.
			var c2 collector
			res2, err := Replay(dir, 0, &c2)
			if err != nil {
				t.Fatalf("second Replay: %v", err)
			}
			if res2.Truncated {
				t.Error("second replay still saw a torn tail, truncation was not persisted")
			}
			if len(c2.ops) != 9 {
				t.Fatalf("second replay recovered %d records", len(c2.ops))
			}
		})
	}
}

// TestBitFlipInTailIsCaught proves the checksum, not just the length, is doing
// work: the file is the right size but one byte is wrong.
func TestBitFlipInTailIsCaught(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, SyncAlways, 0)
	for i := 0; i < 4; i++ {
		if _, err := l.AppendPut([]byte{byte('a' + i)}, store.Record{Value: []byte("payload")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	segs, _ := listSegments(dir)
	path := segs[0].path
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-3] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	var c collector
	res, err := Replay(dir, 0, &c)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if !res.Truncated {
		t.Fatal("a flipped byte in the last record was not detected")
	}
	if len(c.ops) != 3 {
		t.Fatalf("recovered %d records, want 3", len(c.ops))
	}
}

// TestCorruptionMidLogIsFatal is the other half of the contract: damage that a
// crash could not have caused must not be silently repaired.
func TestCorruptionMidLogIsFatal(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, SyncAlways, 512)
	for i := 0; i < 100; i++ {
		if _, err := l.AppendPut([]byte(fmt.Sprintf("k%03d", i)), store.Record{Value: []byte("payload")}); err != nil {
			t.Fatal(err)
		}
		if err := l.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	segs, _ := listSegments(dir)
	if len(segs) < 3 {
		t.Skipf("need several segments, got %d", len(segs))
	}
	victim := segs[0].path
	data, err := os.ReadFile(victim)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-4] ^= 0xFF
	if err := os.WriteFile(victim, data, 0o644); err != nil {
		t.Fatal(err)
	}

	var c collector
	if _, err := Replay(dir, 0, &c); !errors.Is(err, ErrTornMidLog) {
		t.Fatalf("mid-log corruption returned %v, want ErrTornMidLog", err)
	}
}

func TestTrimToRemovesOnlySupersededSegments(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, SyncAlways, 512)
	for i := 0; i < 100; i++ {
		if _, err := l.AppendPut([]byte(fmt.Sprintf("k%03d", i)), store.Record{Value: []byte("payload")}); err != nil {
			t.Fatal(err)
		}
		if err := l.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	before, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Trimming against LSN 0 must remove nothing: no snapshot covers anything.
	if removed, err := l.TrimTo(0); err != nil || removed != 0 {
		t.Fatalf("TrimTo(0) removed %d, err %v", removed, err)
	}

	removed, err := l.TrimTo(50)
	if err != nil {
		t.Fatal(err)
	}
	if removed == 0 {
		t.Fatal("TrimTo(50) removed nothing")
	}
	after, _ := listSegments(dir)
	if len(after) != len(before)-removed {
		t.Fatalf("segments %d -> %d but reported %d removed", len(before), len(after), removed)
	}

	// Everything after the trim point must still replay.
	var c collector
	if _, err := Replay(dir, 50, &c); err != nil {
		t.Fatalf("replay after trim: %v", err)
	}
	if len(c.ops) != 50 {
		t.Fatalf("replayed %d records after trim, want 50", len(c.ops))
	}
}

func TestConcurrentAppendsAreOrderedAndComplete(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, SyncAlways, 1<<20)

	const writers, each = 16, 200
	var wg sync.WaitGroup
	seen := make([][]uint64, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				lsn, err := l.AppendPut([]byte(fmt.Sprintf("w%02d-%03d", w, i)), store.Record{Value: []byte("v")})
				if err != nil {
					t.Errorf("append: %v", err)
					return
				}
				if err := l.Sync(lsn); err != nil {
					t.Errorf("sync: %v", err)
					return
				}
				seen[w] = append(seen[w], lsn)
			}
		}(w)
	}
	wg.Wait()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// Every LSN handed out must be unique and every record must be on disk.
	all := map[uint64]bool{}
	for _, s := range seen {
		for _, lsn := range s {
			if all[lsn] {
				t.Fatalf("LSN %d handed out twice", lsn)
			}
			all[lsn] = true
		}
	}
	var c collector
	res, err := Replay(dir, 0, &c)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.ops) != writers*each {
		t.Fatalf("replayed %d of %d records", len(c.ops), writers*each)
	}
	if res.Truncated {
		t.Error("clean concurrent log reported a torn tail")
	}
}

// TestGroupCommitAmortisesFsync proves the group-commit gate actually batches:
// many concurrent synced writers must cost far fewer fsyncs than writers.
func TestGroupCommitAmortisesFsync(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, SyncAlways, 1<<20)

	const writers = 64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			lsn, err := l.AppendPut([]byte(fmt.Sprintf("k%03d", w)), store.Record{Value: []byte("v")})
			if err != nil {
				t.Errorf("append: %v", err)
				return
			}
			if err := l.Sync(lsn); err != nil {
				t.Errorf("sync: %v", err)
			}
		}(w)
	}
	close(start)
	wg.Wait()

	if got := l.Stats().Fsyncs; got >= writers {
		t.Fatalf("%d fsyncs for %d concurrent writers: group commit is not batching", got, writers)
	}
	if l.SyncedLSN() < uint64(writers) {
		t.Fatalf("SyncedLSN = %d, expected all %d writes durable", l.SyncedLSN(), writers)
	}
}

func TestPolicyEverysecDoesNotFsyncPerWrite(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, SyncEverySec, 1<<20)
	for i := 0; i < 100; i++ {
		lsn, err := l.AppendPut([]byte{byte(i)}, store.Record{Value: []byte("v")})
		if err != nil {
			t.Fatal(err)
		}
		if lsn != uint64(i+1) {
			t.Fatalf("lsn = %d want %d", lsn, i+1)
		}
	}
	if got := l.Stats().Fsyncs; got > 1 {
		t.Fatalf("everysec issued %d fsyncs for 100 writes", got)
	}
	// Flush must still get the bytes to the kernel, which is what makes a
	// process kill survivable under this policy.
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	if l.Stats().Writes == 0 {
		t.Fatal("Flush did not write to the file")
	}
}

func TestSyncPolicyParsing(t *testing.T) {
	for in, want := range map[string]SyncPolicy{
		"always": SyncAlways, "everysec": SyncEverySec, "": SyncEverySec, "no": SyncNever, "never": SyncNever,
	} {
		got, err := ParseSyncPolicy(in)
		if err != nil || got != want {
			t.Errorf("ParseSyncPolicy(%q) = %v,%v want %v", in, got, err, want)
		}
	}
	if _, err := ParseSyncPolicy("sometimes"); err == nil {
		t.Error("expected an error for an unknown policy")
	}
}

func TestSegmentHeaderCorruptionIsReported(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, SyncAlways, 0)
	if _, err := l.AppendPut([]byte("a"), store.Record{Value: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	segs, _ := listSegments(dir)
	data, _ := os.ReadFile(segs[0].path)
	data[2] ^= 0xFF // corrupt the magic
	if err := os.WriteFile(segs[0].path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	var c collector
	if _, err := Replay(dir, 0, &c); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
}

func TestOpenStartsFreshSegment(t *testing.T) {
	dir := t.TempDir()
	l1, err := Open(Options{Dir: dir, Policy: SyncAlways}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l1.AppendPut([]byte("a"), store.Record{Value: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if err := l1.Close(); err != nil {
		t.Fatal(err)
	}

	l2, err := Open(Options{Dir: dir, Policy: SyncAlways}, 1)
	if err != nil {
		t.Fatal(err)
	}
	lsn, err := l2.AppendPut([]byte("b"), store.Record{Value: []byte("2")})
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 2 {
		t.Fatalf("second run started at LSN %d, want 2", lsn)
	}
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}

	segs, _ := listSegments(dir)
	if len(segs) != 2 {
		t.Fatalf("expected a fresh segment per run, got %d", len(segs))
	}

	var c collector
	if _, err := Replay(dir, 0, &c); err != nil {
		t.Fatal(err)
	}
	if len(c.ops) != 2 {
		t.Fatalf("replayed %v across restarts", c.ops)
	}
}

func TestEmptyDirReplaysNothing(t *testing.T) {
	var c collector
	res, err := Replay(filepath.Join(t.TempDir(), "missing"), 0, &c)
	if err != nil {
		t.Fatalf("replaying a missing dir should be benign, got %v", err)
	}
	if res.Segments != 0 || len(c.ops) != 0 {
		t.Fatalf("unexpected result %+v", res)
	}
}

func BenchmarkAppendEverysec(b *testing.B) {
	dir := b.TempDir()
	l, err := Open(Options{Dir: dir, Policy: SyncEverySec, SegmentSize: 1 << 30}, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	rec := store.Record{Value: make([]byte, 64)}
	key := []byte("benchmark-key")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := l.AppendPut(key, rec); err != nil {
			b.Fatal(err)
		}
	}
}
