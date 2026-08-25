package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
)

// Applier receives replayed mutations in log order. The byte slices handed to
// it alias the read buffer and must be copied if retained.
type Applier interface {
	Put(key []byte, rec store.Record) error
	Delete(key []byte) error
	FlushAll() error
	Batch(muts []store.Mutation) error
}

// ReplayResult describes what recovery found.
type ReplayResult struct {
	// LastLSN is the highest LSN successfully recovered.
	LastLSN uint64
	// Applied counts records handed to the Applier.
	Applied int
	// Skipped counts records already covered by a snapshot.
	Skipped int
	// Segments counts segment files read.
	Segments int
	// BytesRead is the total bytes consumed.
	BytesRead int64
	// Truncated reports whether a torn tail was discarded.
	Truncated bool
	// TruncatedPath and TruncatedAt locate the discarded tail.
	TruncatedPath string
	TruncatedAt   int64
}

// ErrTornMidLog reports damage somewhere other than the very end of the log,
// which cannot be explained by an interrupted write and therefore is not
// automatically repairable.
var ErrTornMidLog = errors.New("wal: corruption before the end of the log")

// Replay reconstructs state from the segments in dir, applying only records
// with an LSN greater than afterLSN.
//
// Recovery rests on one asymmetry. A crash can only ever damage the very end
// of the log, because the log is append-only and the kernel writes forward. So
// a checksum failure in the final segment's final record is an interrupted
// write: expected, benign, and repaired by truncating it away. A checksum
// failure anywhere earlier is real corruption - bad hardware, a bad filesystem,
// someone editing files by hand - and replay refuses to guess, because
// silently skipping a bad record in the middle would resurrect whatever the
// following records overwrote.
func Replay(dir string, afterLSN uint64, ap Applier) (ReplayResult, error) {
	var res ReplayResult
	res.LastLSN = afterLSN

	segs, err := listSegments(dir)
	if err != nil {
		return res, err
	}
	if len(segs) == 0 {
		return res, nil
	}

	expectLSN := uint64(0)
	for i, seg := range segs {
		last := i == len(segs)-1
		n, off, damaged, err := replaySegment(seg, afterLSN, &expectLSN, ap, &res)
		res.Segments++
		res.BytesRead += n

		if damaged != nil {
			if !last {
				return res, fmt.Errorf("%w: %s at offset %d: %v", ErrTornMidLog, seg.path, off, damaged)
			}
			if err := truncateAt(seg.path, off); err != nil {
				return res, err
			}
			if err := syncDir(dir); err != nil {
				return res, err
			}
			res.Truncated = true
			res.TruncatedPath = seg.path
			res.TruncatedAt = off
			return res, nil
		}
		if err != nil {
			return res, err
		}
	}
	return res, nil
}

// replaySegment reads one segment. A returned non-nil damaged value means the
// segment ends in an unreadable record starting at the returned offset.
func replaySegment(seg segmentInfo, afterLSN uint64, expectLSN *uint64, ap Applier, res *ReplayResult) (read int64, badOff int64, damaged error, err error) {
	f, err := os.Open(seg.path)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("wal: open %s: %w", seg.path, err)
	}
	defer func() { _ = f.Close() }()

	br := bufio.NewReaderSize(f, 1<<20)
	hdr := make([]byte, segHeaderSize)
	if _, err := io.ReadFull(br, hdr); err != nil {
		return 0, 0, nil, fmt.Errorf("wal: read header of %s: %w", seg.path, err)
	}
	if _, err := parseSegHeader(hdr, seg.path); err != nil {
		return 0, 0, nil, err
	}

	off := int64(segHeaderSize)
	var rh [headerSize]byte
	payload := make([]byte, 0, 4096)

	for {
		nh, err := io.ReadFull(br, rh[:])
		if errors.Is(err, io.EOF) && nh == 0 {
			return off, off, nil, nil // clean end of segment
		}
		if err != nil {
			// A partial record header is a torn write.
			return off, off, fmt.Errorf("short record header (%d bytes): %w", nh, err), nil
		}

		want := binary.LittleEndian.Uint32(rh[0:4])
		plen := binary.LittleEndian.Uint32(rh[4:8])
		typ := RecordType(rh[8])
		lsn := binary.LittleEndian.Uint64(rh[9:17])

		if plen > maxRecordSize {
			return off, off, fmt.Errorf("record length %d exceeds maximum", plen), nil
		}
		if cap(payload) < int(plen) {
			payload = make([]byte, plen)
		}
		payload = payload[:plen]
		if _, err := io.ReadFull(br, payload); err != nil {
			return off, off, fmt.Errorf("short record payload: %w", err), nil
		}

		sum := crc32.Update(crc32.Checksum(rh[4:], castagnoli), castagnoli, payload)
		if sum != want {
			return off, off, fmt.Errorf("checksum %08x != %08x", sum, want), nil
		}

		if *expectLSN != 0 && lsn != *expectLSN {
			// A gap or a repeat is not something an interrupted write can
			// produce, so treat it as corruption wherever it appears.
			return off, off, fmt.Errorf("LSN %d out of order, expected %d", lsn, *expectLSN), nil
		}
		*expectLSN = lsn + 1
		off += headerSize + int64(plen)

		if lsn <= afterLSN {
			res.Skipped++
			res.LastLSN = lsn
			continue
		}
		if err := applyRecord(typ, payload, ap); err != nil {
			return off, off, nil, fmt.Errorf("wal: apply record lsn=%d type=%s: %w", lsn, typ, err)
		}
		res.Applied++
		res.LastLSN = lsn
	}
}

func applyRecord(typ RecordType, payload []byte, ap Applier) error {
	switch typ {
	case RecPut:
		key, rec, err := decodePut(payload)
		if err != nil {
			return err
		}
		return ap.Put(key, rec)
	case RecDelete:
		key, err := decodeDelete(payload)
		if err != nil {
			return err
		}
		return ap.Delete(key)
	case RecFlushAll:
		if len(payload) != 0 {
			return fmt.Errorf("%w: flushall carries %d bytes", ErrCorrupt, len(payload))
		}
		return ap.FlushAll()
	case RecBatch:
		muts, err := decodeBatch(payload)
		if err != nil {
			return err
		}
		return ap.Batch(muts)
	default:
		return fmt.Errorf("%w: unknown record type %d", ErrCorrupt, uint8(typ))
	}
}

// truncateAt discards everything at or after off and makes the truncation
// itself durable, so that a crash during recovery does not leave the log in a
// third, previously unseen state.
func truncateAt(path string, off int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("wal: open %s for truncate: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	if err := f.Truncate(off); err != nil {
		return fmt.Errorf("wal: truncate %s to %d: %w", path, off, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("wal: fsync after truncate: %w", err)
	}
	return nil
}
