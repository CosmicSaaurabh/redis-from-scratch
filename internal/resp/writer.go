package resp

import (
	"bufio"
	"io"
	"math"
	"strconv"
)

// Writer serialises replies. It is protocol-version aware: the RESP3-only
// types (map, set, double, boolean, null, push) are emitted natively on a
// RESP3 connection and degraded to their RESP2 equivalents otherwise, so a
// command handler never has to branch on the version itself.
//
// Writer is not safe for concurrent use. Each connection owns exactly one.
type Writer struct {
	bw      *bufio.Writer
	ver     Version
	scratch []byte
}

// NewWriter wraps w with a buffer of size bufSize. Replies accumulate in the
// buffer and only reach the socket on Flush, which is what makes pipelining
// cheap: N pipelined commands cost one write syscall, not N.
func NewWriter(w io.Writer, bufSize int) *Writer {
	if bufSize <= 0 {
		bufSize = 16 << 10
	}
	return &Writer{
		bw:      bufio.NewWriterSize(w, bufSize),
		ver:     RESP2,
		scratch: make([]byte, 0, 32),
	}
}

// Version reports the dialect currently in use.
func (w *Writer) Version() Version { return w.ver }

// SetVersion switches the dialect. Only HELLO should call this.
func (w *Writer) SetVersion(v Version) { w.ver = v }

// Buffered reports how many reply bytes are pending.
func (w *Writer) Buffered() int { return w.bw.Buffered() }

// Flush pushes buffered replies to the underlying writer.
func (w *Writer) Flush() error { return w.bw.Flush() }

func (w *Writer) writeHeader(t byte, n int64) {
	w.scratch = w.scratch[:0]
	w.scratch = append(w.scratch, t)
	w.scratch = strconv.AppendInt(w.scratch, n, 10)
	w.scratch = append(w.scratch, '\r', '\n')
	_, _ = w.bw.Write(w.scratch)
}

// WriteSimpleString emits `+s\r\n`. s must not contain CR or LF; callers pass
// only server-controlled constants here.
func (w *Writer) WriteSimpleString(s string) {
	_ = w.bw.WriteByte(TypeSimpleString)
	_, _ = w.bw.WriteString(s)
	_, _ = w.bw.Write(crlf)
}

// WriteError emits `-msg\r\n`. msg is expected to already carry a Redis-style
// error code prefix such as "ERR " or "WRONGTYPE ".
func (w *Writer) WriteError(msg string) {
	_ = w.bw.WriteByte(TypeError)
	_, _ = w.bw.WriteString(sanitizeLine(msg))
	_, _ = w.bw.Write(crlf)
}

// WriteInt emits `:n\r\n`.
func (w *Writer) WriteInt(n int64) { w.writeHeader(TypeInteger, n) }

// WriteBulk emits a bulk string. A nil b is a null reply, matching the way
// Redis distinguishes "key missing" from "key holds an empty string".
func (w *Writer) WriteBulk(b []byte) {
	if b == nil {
		w.WriteNull()
		return
	}
	w.writeHeader(TypeBulkString, int64(len(b)))
	_, _ = w.bw.Write(b)
	_, _ = w.bw.Write(crlf)
}

// WriteBulkString is WriteBulk for a string, avoiding a []byte conversion.
func (w *Writer) WriteBulkString(s string) {
	w.writeHeader(TypeBulkString, int64(len(s)))
	_, _ = w.bw.WriteString(s)
	_, _ = w.bw.Write(crlf)
}

// WriteNull emits the null reply: `_\r\n` on RESP3, `$-1\r\n` on RESP2.
func (w *Writer) WriteNull() {
	if w.ver >= RESP3 {
		_, _ = w.bw.WriteString("_\r\n")
		return
	}
	_, _ = w.bw.WriteString("$-1\r\n")
}

// WriteNullArray emits a null where an array was expected. RESP2 spells this
// `*-1`, which is a different byte sequence from a null bulk string and some
// older clients depend on the distinction.
func (w *Writer) WriteNullArray() {
	if w.ver >= RESP3 {
		_, _ = w.bw.WriteString("_\r\n")
		return
	}
	_, _ = w.bw.WriteString("*-1\r\n")
}

// WriteArrayHeader announces n following elements.
func (w *Writer) WriteArrayHeader(n int) { w.writeHeader(TypeArray, int64(n)) }

// WriteMapHeader announces n following key/value pairs, i.e. 2n elements. On
// RESP2 it degrades to a flat array of 2n items, which is exactly how RESP2
// clients have always received maps.
func (w *Writer) WriteMapHeader(n int) {
	if w.ver >= RESP3 {
		w.writeHeader(TypeMap, int64(n))
		return
	}
	w.writeHeader(TypeArray, int64(n*2))
}

// WriteSetHeader announces n following elements of an unordered set. RESP2
// degrades to an array.
func (w *Writer) WriteSetHeader(n int) {
	if w.ver >= RESP3 {
		w.writeHeader(TypeSet, int64(n))
		return
	}
	w.writeHeader(TypeArray, int64(n))
}

// WritePushHeader announces an out-of-band push message. RESP2 has no push
// type; the caller is responsible for only pushing on connections that asked
// for it.
func (w *Writer) WritePushHeader(n int) {
	if w.ver >= RESP3 {
		w.writeHeader(TypePush, int64(n))
		return
	}
	w.writeHeader(TypeArray, int64(n))
}

// WriteBool emits a boolean: `#t`/`#f` on RESP3, `:1`/`:0` on RESP2.
func (w *Writer) WriteBool(b bool) {
	if w.ver >= RESP3 {
		if b {
			_, _ = w.bw.WriteString("#t\r\n")
		} else {
			_, _ = w.bw.WriteString("#f\r\n")
		}
		return
	}
	if b {
		w.WriteInt(1)
	} else {
		w.WriteInt(0)
	}
}

// WriteDouble emits a floating point number: a native double on RESP3, a bulk
// string on RESP2. Both use the same textual form so a client that upgrades
// sees no change in the value, only in its type tag.
func (w *Writer) WriteDouble(f float64) {
	s := FormatDouble(f)
	if w.ver >= RESP3 {
		_ = w.bw.WriteByte(TypeDouble)
		_, _ = w.bw.WriteString(s)
		_, _ = w.bw.Write(crlf)
		return
	}
	w.WriteBulkString(s)
}

// WriteVerbatim emits a verbatim string with a 3-character format hint such as
// "txt" or "mkd". RESP2 degrades to a plain bulk string.
func (w *Writer) WriteVerbatim(format, s string) {
	if w.ver >= RESP3 && len(format) == 3 {
		w.writeHeader(TypeVerbatim, int64(len(s)+4))
		_, _ = w.bw.WriteString(format)
		_ = w.bw.WriteByte(':')
		_, _ = w.bw.WriteString(s)
		_, _ = w.bw.Write(crlf)
		return
	}
	w.WriteBulkString(s)
}

// WriteBigNumber emits an arbitrary-precision integer as text. RESP2 degrades
// to a bulk string.
func (w *Writer) WriteBigNumber(s string) {
	if w.ver >= RESP3 {
		_ = w.bw.WriteByte(TypeBigNumber)
		_, _ = w.bw.WriteString(s)
		_, _ = w.bw.Write(crlf)
		return
	}
	w.WriteBulkString(s)
}

// WriteRaw appends pre-encoded protocol bytes. Used for hot constant replies
// that are assembled once at startup.
func (w *Writer) WriteRaw(b []byte) { _, _ = w.bw.Write(b) }

// FormatDouble renders a float the way Redis does: integers without a decimal
// point, infinities as inf/-inf, and everything else with the shortest
// representation that round-trips.
func FormatDouble(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case math.IsNaN(f):
		return "nan"
	case f == math.Trunc(f) && math.Abs(f) < 1e17:
		return strconv.FormatFloat(f, 'f', 0, 64)
	default:
		return strconv.FormatFloat(f, 'g', 17, 64)
	}
}

// sanitizeLine strips CR and LF so a caller cannot inject extra protocol
// frames through an error message that echoes user input.
func sanitizeLine(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] == '\r' || s[i] == '\n' {
			b := []byte(s)
			for j := i; j < len(b); j++ {
				if b[j] == '\r' || b[j] == '\n' {
					b[j] = ' '
				}
			}
			return string(b)
		}
	}
	return s
}
