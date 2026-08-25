// Package snapshot writes and loads point-in-time images of the keyspace.
//
// A snapshot on its own is not a recovery mechanism; a snapshot plus the
// write-ahead log after it is. The image is taken fuzzily, without stopping
// writes, and is paired with the log sequence number observed before the walk
// started. Recovery loads the image and then replays every log record after
// that LSN, which repairs anything the walk raced with. The property that makes
// this sound is that log records are idempotent physical mutations: replaying
// "key k holds exactly these bytes" over a value the snapshot already captured
// is a no-op, not a double-apply.
package snapshot

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

const (
	fileMagic   = "RFSSNAP\x01"
	footerMagic = "RFSSEND\x01"

	// FormatVersion is bumped whenever the on-disk layout changes
	// incompatibly. Loading refuses anything it does not recognise rather than
	// misinterpreting old bytes.
	FormatVersion uint32 = 1

	headerSize = 8 + 4 + 8 + 8 + 4 // magic, version, createdAtMs, lsn, crc
	footerSize = 8 + 8 + 4 + 8     // count, bodyLen, bodyCRC, magic

	// Name is the filename of the current snapshot inside the data directory.
	Name = "dump.rfs"
)

// ErrCorrupt reports a snapshot that failed structural or checksum validation.
var ErrCorrupt = errors.New("snapshot: corrupt")

// ErrNotFound reports that no snapshot exists.
var ErrNotFound = errors.New("snapshot: not found")

// Info describes a snapshot.
type Info struct {
	// LSN is the log sequence number the image is safe from. Recovery replays
	// log records strictly greater than this.
	LSN uint64
	// CreatedAtMs is when the write began, in Unix milliseconds.
	CreatedAtMs int64
	// Keys is the number of records in the image.
	Keys uint64
	// Bytes is the size of the file on disk.
	Bytes int64
	// Path is where the image lives.
	Path string
}

// Emit writes one record into a snapshot in progress.
type Emit func(key []byte, rec store.Record) error

// Write produces a snapshot at dir/Name.
//
// lsn must be read before iteration begins, never after. Taking it afterwards
// would claim the image covers writes it may have missed, and recovery would
// skip exactly the log records needed to fill the gap.
//
// The write is atomic against a crash: the image lands in a temporary file, is
// fsynced, and only then is renamed into place. A crash at any point leaves
// either the previous snapshot or the new one, never a half-written file
// wearing the real name.
func Write(dir string, lsn uint64, createdAtMs int64, iter func(Emit) error) (Info, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Info{}, fmt.Errorf("snapshot: create dir: %w", err)
	}
	final := filepath.Join(dir, Name)
	tmp := fmt.Sprintf("%s.tmp.%d", final, os.Getpid())

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return Info{}, fmt.Errorf("snapshot: create %s: %w", tmp, err)
	}
	// From here on every failure path must remove the temporary file, or a
	// crashed snapshot leaks disk on every attempt.
	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(tmp)
	}

	bw := bufio.NewWriterSize(f, 1<<20)
	if _, err := bw.Write(encodeHeader(lsn, createdAtMs)); err != nil {
		cleanup()
		return Info{}, fmt.Errorf("snapshot: write header: %w", err)
	}

	var (
		count   uint64
		bodyLen uint64
		sum     uint32
		buf     = make([]byte, 0, 4096)
	)
	emit := func(key []byte, rec store.Record) error {
		buf = buf[:0]
		buf = binary.AppendUvarint(buf, uint64(len(key)))
		buf = append(buf, key...)
		buf = binary.AppendVarint(buf, rec.ExpireAt)
		buf = binary.AppendUvarint(buf, uint64(len(rec.Value)))
		buf = append(buf, rec.Value...)
		if _, err := bw.Write(buf); err != nil {
			return fmt.Errorf("snapshot: write entry: %w", err)
		}
		sum = crc32.Update(sum, castagnoli, buf)
		bodyLen += uint64(len(buf))
		count++
		return nil
	}

	if err := iter(emit); err != nil {
		cleanup()
		return Info{}, err
	}

	footer := make([]byte, 0, footerSize)
	footer = binary.LittleEndian.AppendUint64(footer, count)
	footer = binary.LittleEndian.AppendUint64(footer, bodyLen)
	footer = binary.LittleEndian.AppendUint32(footer, sum)
	footer = append(footer, footerMagic...)
	if _, err := bw.Write(footer); err != nil {
		cleanup()
		return Info{}, fmt.Errorf("snapshot: write footer: %w", err)
	}
	if err := bw.Flush(); err != nil {
		cleanup()
		return Info{}, fmt.Errorf("snapshot: flush: %w", err)
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return Info{}, fmt.Errorf("snapshot: fsync: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		cleanup()
		return Info{}, fmt.Errorf("snapshot: stat: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return Info{}, fmt.Errorf("snapshot: close: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return Info{}, fmt.Errorf("snapshot: rename into place: %w", err)
	}
	// The rename is a directory operation. Without fsyncing the directory the
	// new name can be lost on a power cut even though the file's contents were
	// already durable, and recovery would silently fall back to the old image.
	if err := syncDir(dir); err != nil {
		return Info{}, err
	}

	return Info{LSN: lsn, CreatedAtMs: createdAtMs, Keys: count, Bytes: st.Size(), Path: final}, nil
}

// Applier receives records from a snapshot being loaded. The slices it is
// handed alias the read buffer and must be copied if retained.
type Applier interface {
	Put(key []byte, rec store.Record) error
}

// Load reads the snapshot in dir into ap. It returns ErrNotFound when no
// snapshot exists, which a first boot treats as an empty database.
func Load(dir string, ap Applier) (Info, error) {
	path := filepath.Join(dir, Name)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Info{}, ErrNotFound
		}
		return Info{}, fmt.Errorf("snapshot: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	st, err := f.Stat()
	if err != nil {
		return Info{}, fmt.Errorf("snapshot: stat: %w", err)
	}
	size := st.Size()
	if size < headerSize+footerSize {
		return Info{}, fmt.Errorf("%w: %s is %d bytes, shorter than an empty snapshot", ErrCorrupt, path, size)
	}

	var hdr [headerSize]byte
	if _, err := f.ReadAt(hdr[:], 0); err != nil {
		return Info{}, fmt.Errorf("snapshot: read header: %w", err)
	}
	lsn, createdMs, err := decodeHeader(hdr[:], path)
	if err != nil {
		return Info{}, err
	}

	// Read the footer first. It carries the body length, which is what makes
	// the body unambiguously delimited instead of "read until it looks like a
	// footer".
	var ftr [footerSize]byte
	if _, err := f.ReadAt(ftr[:], size-footerSize); err != nil {
		return Info{}, fmt.Errorf("snapshot: read footer: %w", err)
	}
	if string(ftr[20:]) != footerMagic {
		return Info{}, fmt.Errorf("%w: %s has no footer magic, the write was interrupted", ErrCorrupt, path)
	}
	count := binary.LittleEndian.Uint64(ftr[0:8])
	bodyLen := binary.LittleEndian.Uint64(ftr[8:16])
	wantCRC := binary.LittleEndian.Uint32(ftr[16:20])

	if int64(bodyLen) != size-headerSize-footerSize {
		return Info{}, fmt.Errorf("%w: %s body length %d does not match file size %d", ErrCorrupt, path, bodyLen, size)
	}

	if _, err := f.Seek(headerSize, io.SeekStart); err != nil {
		return Info{}, fmt.Errorf("snapshot: seek to body: %w", err)
	}
	br := bufio.NewReaderSize(io.LimitReader(f, int64(bodyLen)), 1<<20)

	var (
		sum     uint32
		loaded  uint64
		scratch = make([]byte, 0, 4096)
	)
	for loaded < count {
		key, val, exp, raw, err := readEntry(br, &scratch)
		if err != nil {
			return Info{}, fmt.Errorf("%w: %s entry %d: %v", ErrCorrupt, path, loaded, err)
		}
		sum = crc32.Update(sum, castagnoli, raw)
		if err := ap.Put(key, store.Record{Value: val, ExpireAt: exp}); err != nil {
			return Info{}, fmt.Errorf("snapshot: apply entry %d: %w", loaded, err)
		}
		loaded++
	}
	if sum != wantCRC {
		return Info{}, fmt.Errorf("%w: %s body checksum %08x != %08x", ErrCorrupt, path, sum, wantCRC)
	}
	if n, _ := io.Copy(io.Discard, br); n != 0 {
		return Info{}, fmt.Errorf("%w: %s has %d bytes after the last entry", ErrCorrupt, path, n)
	}

	return Info{LSN: lsn, CreatedAtMs: createdMs, Keys: count, Bytes: size, Path: path}, nil
}

// Peek reads a snapshot's header without loading it.
func Peek(dir string) (Info, error) {
	path := filepath.Join(dir, Name)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Info{}, ErrNotFound
		}
		return Info{}, fmt.Errorf("snapshot: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	var hdr [headerSize]byte
	if _, err := f.ReadAt(hdr[:], 0); err != nil {
		return Info{}, fmt.Errorf("snapshot: read header: %w", err)
	}
	lsn, createdMs, err := decodeHeader(hdr[:], path)
	if err != nil {
		return Info{}, err
	}
	st, err := f.Stat()
	if err != nil {
		return Info{}, fmt.Errorf("snapshot: stat: %w", err)
	}
	return Info{LSN: lsn, CreatedAtMs: createdMs, Bytes: st.Size(), Path: path}, nil
}

// readEntry decodes one record and also returns its exact on-disk bytes so the
// caller can checksum precisely what was written.
func readEntry(br *bufio.Reader, scratch *[]byte) (key, val []byte, expireAt int64, raw []byte, err error) {
	buf := (*scratch)[:0]

	klen, buf, err := readUvarint(br, buf)
	if err != nil {
		return nil, nil, 0, nil, err
	}
	keyStart := len(buf)
	buf, err = readN(br, buf, int(klen))
	if err != nil {
		return nil, nil, 0, nil, err
	}
	keyEnd := len(buf)

	expireAt, buf, err = readVarint(br, buf)
	if err != nil {
		return nil, nil, 0, nil, err
	}

	vlen, buf, err := readUvarint(br, buf)
	if err != nil {
		return nil, nil, 0, nil, err
	}
	valStart := len(buf)
	buf, err = readN(br, buf, int(vlen))
	if err != nil {
		return nil, nil, 0, nil, err
	}

	*scratch = buf
	return buf[keyStart:keyEnd], buf[valStart:], expireAt, buf, nil
}

func readUvarint(br *bufio.Reader, buf []byte) (uint64, []byte, error) {
	var v uint64
	var shift uint
	for i := 0; i < binary.MaxVarintLen64; i++ {
		b, err := br.ReadByte()
		if err != nil {
			return 0, buf, err
		}
		buf = append(buf, b)
		if b < 0x80 {
			return v | uint64(b)<<shift, buf, nil
		}
		v |= uint64(b&0x7f) << shift
		shift += 7
	}
	return 0, buf, errors.New("uvarint overflows 64 bits")
}

func readVarint(br *bufio.Reader, buf []byte) (int64, []byte, error) {
	u, buf, err := readUvarint(br, buf)
	if err != nil {
		return 0, buf, err
	}
	// Undo protobuf zig-zag, matching binary.AppendVarint.
	x := int64(u >> 1)
	if u&1 != 0 {
		x = ^x
	}
	return x, buf, nil
}

func readN(br *bufio.Reader, buf []byte, n int) ([]byte, error) {
	if n < 0 || n > 1<<30 {
		return buf, fmt.Errorf("implausible length %d", n)
	}
	start := len(buf)
	for cap(buf)-len(buf) < n {
		grown := make([]byte, len(buf), max(cap(buf)*2, len(buf)+n))
		copy(grown, buf)
		buf = grown
	}
	buf = buf[:start+n]
	if _, err := io.ReadFull(br, buf[start:]); err != nil {
		return buf, err
	}
	return buf, nil
}

func encodeHeader(lsn uint64, createdAtMs int64) []byte {
	b := make([]byte, 0, headerSize)
	b = append(b, fileMagic...)
	b = binary.LittleEndian.AppendUint32(b, FormatVersion)
	b = binary.LittleEndian.AppendUint64(b, uint64(createdAtMs))
	b = binary.LittleEndian.AppendUint64(b, lsn)
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

func decodeHeader(hdr []byte, path string) (lsn uint64, createdAtMs int64, err error) {
	if string(hdr[:8]) != fileMagic {
		return 0, 0, fmt.Errorf("%w: %s has bad magic", ErrCorrupt, path)
	}
	want := binary.LittleEndian.Uint32(hdr[headerSize-4:])
	if got := crc32.Checksum(hdr[:headerSize-4], castagnoli); got != want {
		return 0, 0, fmt.Errorf("%w: %s header checksum %08x != %08x", ErrCorrupt, path, got, want)
	}
	if v := binary.LittleEndian.Uint32(hdr[8:12]); v != FormatVersion {
		return 0, 0, fmt.Errorf("snapshot: %s is format version %d, this build reads %d", path, v, FormatVersion)
	}
	createdAtMs = int64(binary.LittleEndian.Uint64(hdr[12:20]))
	lsn = binary.LittleEndian.Uint64(hdr[20:28])
	return lsn, createdAtMs, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("snapshot: open dir for fsync: %w", err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("snapshot: fsync dir %s: %w", dir, err)
	}
	return nil
}
