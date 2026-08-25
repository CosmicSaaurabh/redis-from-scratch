// Package wal implements a segmented, checksummed write-ahead log.
//
// The log records physical mutations, not the commands that produced them.
// That distinction matters: replaying "INCRBYFLOAT k 0.1" or "EXPIRE k 100"
// reproduces a different state than the original execution did, because one is
// floating point and the other is relative to wall time. Replaying "key k now
// holds these exact bytes and expires at this exact instant" cannot drift.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
)

// castagnoli is the CRC-32C polynomial. It is chosen over IEEE because both
// arm64 and x86-64 implement it as a single instruction, which keeps
// checksumming off the critical path of every write.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// RecordType tags the payload of a log record.
type RecordType uint8

const (
	// RecPut stores a key's full record: value plus absolute expiry.
	RecPut RecordType = 1
	// RecDelete removes a key.
	RecDelete RecordType = 2
	// RecFlushAll removes every key.
	RecFlushAll RecordType = 3
	// RecBatch groups several mutations that must be replayed atomically, so
	// that a crash cannot leave an MSET half applied.
	RecBatch RecordType = 4
)

func (t RecordType) String() string {
	switch t {
	case RecPut:
		return "put"
	case RecDelete:
		return "delete"
	case RecFlushAll:
		return "flushall"
	case RecBatch:
		return "batch"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(t))
	}
}

// headerSize is crc32c(4) + payload length(4) + type(1) + lsn(8).
const headerSize = 17

// maxRecordSize bounds a single log record. A length field read out of a
// corrupt tail could otherwise ask the reader to allocate gigabytes before it
// gets a chance to notice the checksum is wrong.
const maxRecordSize = 1 << 30

// ErrCorrupt reports a record that failed its checksum or was structurally
// invalid. Whether that is fatal depends on where in the log it occurs; see
// Replay.
var ErrCorrupt = errors.New("wal: corrupt record")

// appendRecord frames payload into dst and returns the extended slice.
func appendRecord(dst []byte, typ RecordType, lsn uint64, payload []byte) []byte {
	start := len(dst)
	dst = append(dst, 0, 0, 0, 0) // crc placeholder
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(payload)))
	dst = append(dst, byte(typ))
	dst = binary.LittleEndian.AppendUint64(dst, lsn)
	dst = append(dst, payload...)

	// The checksum covers everything after itself: length, type, LSN and
	// payload. Covering the length field is the point - a torn write that
	// corrupts only the length would otherwise be undetectable and would
	// desynchronise every record after it.
	sum := crc32.Checksum(dst[start+4:], castagnoli)
	binary.LittleEndian.PutUint32(dst[start:start+4], sum)
	return dst
}

// appendPutPayload encodes a key and its record.
func appendPutPayload(dst []byte, key []byte, rec store.Record) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(key)))
	dst = append(dst, key...)
	dst = binary.AppendVarint(dst, rec.ExpireAt)
	dst = binary.AppendUvarint(dst, uint64(len(rec.Value)))
	return append(dst, rec.Value...)
}

// appendDeletePayload encodes a key removal.
func appendDeletePayload(dst []byte, key []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(key)))
	return append(dst, key...)
}

// appendBatchPayload encodes an atomic group of mutations.
func appendBatchPayload(dst []byte, muts []store.Mutation) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(muts)))
	for _, m := range muts {
		if m.Delete {
			dst = append(dst, byte(RecDelete))
			dst = appendDeletePayload(dst, m.Key)
			continue
		}
		dst = append(dst, byte(RecPut))
		dst = appendPutPayload(dst, m.Key, m.Record)
	}
	return dst
}

// decodePut parses a put payload. The returned slices alias p.
func decodePut(p []byte) (key []byte, rec store.Record, err error) {
	key, p, err = takeBytes(p)
	if err != nil {
		return nil, rec, err
	}
	exp, n := binary.Varint(p)
	if n <= 0 {
		return nil, rec, ErrCorrupt
	}
	p = p[n:]
	val, p, err := takeBytes(p)
	if err != nil {
		return nil, rec, err
	}
	if len(p) != 0 {
		return nil, rec, fmt.Errorf("%w: %d trailing bytes in put", ErrCorrupt, len(p))
	}
	return key, store.Record{Value: val, ExpireAt: exp}, nil
}

// decodeDelete parses a delete payload. The returned slice aliases p.
func decodeDelete(p []byte) ([]byte, error) {
	key, rest, err := takeBytes(p)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes in delete", ErrCorrupt, len(rest))
	}
	return key, nil
}

// decodeBatch parses a batch payload into mutations aliasing p.
func decodeBatch(p []byte) ([]store.Mutation, error) {
	n, adv := binary.Uvarint(p)
	if adv <= 0 || n > maxRecordSize {
		return nil, ErrCorrupt
	}
	p = p[adv:]
	muts := make([]store.Mutation, 0, n)
	for i := uint64(0); i < n; i++ {
		if len(p) == 0 {
			return nil, ErrCorrupt
		}
		sub := RecordType(p[0])
		p = p[1:]
		key, rest, err := takeBytes(p)
		if err != nil {
			return nil, err
		}
		switch sub {
		case RecDelete:
			muts = append(muts, store.Mutation{Key: key, Delete: true})
			p = rest
		case RecPut:
			exp, adv := binary.Varint(rest)
			if adv <= 0 {
				return nil, ErrCorrupt
			}
			rest = rest[adv:]
			val, next, err := takeBytes(rest)
			if err != nil {
				return nil, err
			}
			muts = append(muts, store.Mutation{Key: key, Record: store.Record{Value: val, ExpireAt: exp}})
			p = next
		default:
			return nil, fmt.Errorf("%w: bad batch sub-type %d", ErrCorrupt, sub)
		}
	}
	if len(p) != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes in batch", ErrCorrupt, len(p))
	}
	return muts, nil
}

// takeBytes reads a uvarint-prefixed byte string, returning it and the rest.
func takeBytes(p []byte) (val, rest []byte, err error) {
	n, adv := binary.Uvarint(p)
	if adv <= 0 {
		return nil, nil, ErrCorrupt
	}
	p = p[adv:]
	if n > uint64(len(p)) {
		return nil, nil, fmt.Errorf("%w: length %d exceeds %d remaining", ErrCorrupt, n, len(p))
	}
	return p[:n], p[n:], nil
}
