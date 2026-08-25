package snapshot

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
)

type memApplier struct{ m map[string]store.Record }

func newApplier() *memApplier { return &memApplier{m: map[string]store.Record{}} }

func (a *memApplier) Put(key []byte, rec store.Record) error {
	a.m[string(key)] = rec.Clone()
	return nil
}

func writeSet(t *testing.T, dir string, lsn uint64, pairs map[string]store.Record) Info {
	t.Helper()
	keys := make([]string, 0, len(pairs))
	for k := range pairs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	info, err := Write(dir, lsn, 1_700_000_000_000, func(emit Emit) error {
		for _, k := range keys {
			if err := emit([]byte(k), pairs[k]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	return info
}

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	want := map[string]store.Record{
		"alpha":  {Value: []byte("one")},
		"beta":   {Value: []byte("two"), ExpireAt: 1_700_000_050_000},
		"empty":  {Value: []byte{}},
		"binary": {Value: []byte{0x00, 0xff, 0x0d, 0x0a}},
		"negexp": {Value: []byte("x"), ExpireAt: -5},
		"bigval": {Value: make([]byte, 100_000)},
	}
	info := writeSet(t, dir, 42, want)
	if info.Keys != uint64(len(want)) {
		t.Fatalf("wrote %d keys, want %d", info.Keys, len(want))
	}

	got := newApplier()
	loaded, err := Load(dir, got)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.LSN != 42 {
		t.Errorf("LSN = %d want 42", loaded.LSN)
	}
	if len(got.m) != len(want) {
		t.Fatalf("loaded %d keys, want %d", len(got.m), len(want))
	}
	for k, w := range want {
		g, ok := got.m[k]
		if !ok {
			t.Errorf("key %q missing", k)
			continue
		}
		if string(g.Value) != string(w.Value) || g.ExpireAt != w.ExpireAt {
			t.Errorf("key %q = %v want %v", k, g, w)
		}
	}
}

func TestEmptySnapshot(t *testing.T) {
	dir := t.TempDir()
	writeSet(t, dir, 7, nil)
	got := newApplier()
	info, err := Load(dir, got)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if info.LSN != 7 || len(got.m) != 0 {
		t.Fatalf("got %+v with %d keys", info, len(got.m))
	}
}

func TestLoadMissingIsNotFound(t *testing.T) {
	if _, err := Load(t.TempDir(), newApplier()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestPeekDoesNotLoadBody(t *testing.T) {
	dir := t.TempDir()
	writeSet(t, dir, 99, map[string]store.Record{"k": {Value: []byte("v")}})
	info, err := Peek(dir)
	if err != nil {
		t.Fatal(err)
	}
	if info.LSN != 99 || info.CreatedAtMs != 1_700_000_000_000 {
		t.Fatalf("Peek = %+v", info)
	}
}

// TestTruncatedFileIsRejected models a crash during the snapshot write. The
// rename is what makes this impossible in practice, but the loader must still
// refuse the file rather than load a prefix of it.
func TestTruncatedFileIsRejected(t *testing.T) {
	dir := t.TempDir()
	writeSet(t, dir, 1, map[string]store.Record{
		"a": {Value: []byte("aaaa")}, "b": {Value: []byte("bbbb")}, "c": {Value: []byte("cccc")},
	})
	path := filepath.Join(dir, Name)
	st, _ := os.Stat(path)
	for _, cut := range []int64{1, 10, 30} {
		if err := os.Truncate(path, st.Size()-cut); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(dir, newApplier()); err == nil {
			t.Fatalf("truncating %d bytes was accepted", cut)
		}
	}
}

func TestBodyCorruptionIsCaught(t *testing.T) {
	dir := t.TempDir()
	writeSet(t, dir, 1, map[string]store.Record{
		"key1": {Value: []byte("value-one")},
		"key2": {Value: []byte("value-two")},
	})
	path := filepath.Join(dir, Name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte inside a value, keeping every length field intact so only
	// the checksum can catch it.
	data[headerSize+8] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir, newApplier()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
}

func TestHeaderCorruptionIsCaught(t *testing.T) {
	dir := t.TempDir()
	writeSet(t, dir, 1, map[string]store.Record{"k": {Value: []byte("v")}})
	path := filepath.Join(dir, Name)
	data, _ := os.ReadFile(path)
	data[1] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir, newApplier()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
}

// TestWriteIsAtomic checks that a failed write leaves the previous snapshot
// intact and drops its temporary file, so a repeatedly failing checkpoint
// cannot fill the disk or destroy the last good image.
func TestWriteIsAtomic(t *testing.T) {
	dir := t.TempDir()
	writeSet(t, dir, 1, map[string]store.Record{"keep": {Value: []byte("original")}})

	boom := errors.New("iterator exploded")
	_, err := Write(dir, 2, 0, func(emit Emit) error {
		if err := emit([]byte("partial"), store.Record{Value: []byte("x")}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("got %v, want the iterator error", err)
	}

	got := newApplier()
	info, err := Load(dir, got)
	if err != nil {
		t.Fatalf("previous snapshot was damaged: %v", err)
	}
	if info.LSN != 1 || string(got.m["keep"].Value) != "original" {
		t.Fatalf("previous snapshot changed: %+v %v", info, got.m)
	}
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		if e.Name() != Name {
			t.Errorf("failed write left %s behind", e.Name())
		}
	}
}

func TestLargeSnapshotRoundTrips(t *testing.T) {
	dir := t.TempDir()
	const n = 20_000
	_, err := Write(dir, 5, 0, func(emit Emit) error {
		for i := 0; i < n; i++ {
			k := fmt.Sprintf("key:%06d", i)
			v := fmt.Sprintf("value-%d-%s", i, k)
			if err := emit([]byte(k), store.Record{Value: []byte(v), ExpireAt: int64(i)}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got := newApplier()
	if _, err := Load(dir, got); err != nil {
		t.Fatal(err)
	}
	if len(got.m) != n {
		t.Fatalf("loaded %d of %d", len(got.m), n)
	}
	probe := "key:012345"
	if string(got.m[probe].Value) != "value-12345-"+probe {
		t.Fatalf("%s = %q", probe, got.m[probe].Value)
	}
	if got.m[probe].ExpireAt != 12345 {
		t.Fatalf("expiry not preserved: %d", got.m[probe].ExpireAt)
	}
}
