package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

// ProtocolError is returned when the client sends bytes that are not valid
// RESP. It is fatal for the connection: the server replies with the error and
// then closes, because once framing is lost there is no way to resynchronise
// with the client's byte stream.
type ProtocolError struct{ Msg string }

func (e *ProtocolError) Error() string { return "Protocol error: " + e.Msg }

func protoErrf(format string, args ...any) error {
	return &ProtocolError{Msg: fmt.Sprintf(format, args...)}
}

// IsProtocolError reports whether err is a framing violation as opposed to an
// I/O failure. The server uses this to decide whether to send an error reply
// before hanging up.
func IsProtocolError(err error) bool {
	var pe *ProtocolError
	return errors.As(err, &pe)
}

// Limits bound how much memory a single client can force the server to
// allocate. Every one of these is explicit: an unbounded read here is a
// one-packet denial of service.
type Limits struct {
	// MaxBulkSize is the largest single argument accepted, in bytes.
	MaxBulkSize int64
	// MaxMultiBulkLen is the largest number of arguments in one command.
	MaxMultiBulkLen int
	// MaxInlineSize is the largest legacy inline command line, in bytes.
	MaxInlineSize int
	// ReadBufferSize is the size of the per-connection bufio buffer.
	ReadBufferSize int
}

// DefaultLimits mirrors the real Redis defaults: a 512 MiB proto-max-bulk-len
// and a 1M element multibulk cap.
func DefaultLimits() Limits {
	return Limits{
		MaxBulkSize:     512 << 20,
		MaxMultiBulkLen: 1 << 20,
		MaxInlineSize:   64 << 10,
		ReadBufferSize:  16 << 10,
	}
}

// arenaBlock is the allocation granularity for argument bytes. Arguments
// smaller than this are packed into a shared block so that a command of N
// small arguments costs at most one allocation instead of N.
const arenaBlock = 16 << 10

// Reader parses client commands off a connection.
//
// Lifetime contract: the [][]byte returned by ReadCommand aliases an internal
// buffer and is only valid until the next call to ReadCommand on the same
// Reader. Anything that outlives the command - a key or value being stored -
// must be copied by the caller. This is what keeps the hot path free of
// per-argument garbage.
type Reader struct {
	br    *bufio.Reader
	lim   Limits
	args  [][]byte
	block []byte
	// inFlight is true between consuming a command's first byte and returning
	// it, so a caller can distinguish an idle timeout from a partial frame.
	inFlight bool
}

// NewReader wraps rd. A zero-valued lim is replaced by DefaultLimits.
func NewReader(rd io.Reader, lim Limits) *Reader {
	if lim.MaxBulkSize <= 0 {
		lim = DefaultLimits()
	}
	if lim.ReadBufferSize <= 0 {
		lim.ReadBufferSize = DefaultLimits().ReadBufferSize
	}
	return &Reader{
		br:    bufio.NewReaderSize(rd, lim.ReadBufferSize),
		lim:   lim,
		args:  make([][]byte, 0, 8),
		block: make([]byte, 0, arenaBlock),
	}
}

// Buffered reports how many bytes are already in the read buffer. The server
// uses this to decide whether it can serve another pipelined command without
// touching the socket, which is what lets it batch replies.
func (r *Reader) Buffered() int { return r.br.Buffered() }

// ReadCommand reads one command. It blocks until a full command is available.
// It returns a nil slice with a nil error for protocol-level no-ops such as an
// empty multibulk, which the caller should skip.
func (r *Reader) ReadCommand() ([][]byte, error) {
	r.args = r.args[:0]
	r.block = r.block[:0]
	r.inFlight = false

	prefix, err := r.br.Peek(1)
	if err != nil {
		return nil, err
	}
	r.inFlight = true
	if prefix[0] != TypeArray {
		args, err := r.readInline()
		if err == nil {
			r.inFlight = false
		}
		return args, err
	}
	if _, err := r.br.Discard(1); err != nil {
		return nil, err
	}

	n, err := r.readCount("multibulk")
	if err != nil {
		return nil, err
	}
	switch {
	case n < 0:
		// A null multibulk from a client is not meaningful; treat it as a no-op
		// rather than dropping the connection.
		r.inFlight = false
		return nil, nil
	case n == 0:
		r.inFlight = false
		return nil, nil
	case n > r.lim.MaxMultiBulkLen:
		return nil, protoErrf("invalid multibulk length")
	}

	if cap(r.args) < n {
		r.args = make([][]byte, 0, n)
	}
	for i := 0; i < n; i++ {
		arg, err := r.readBulk()
		if err != nil {
			return nil, err
		}
		r.args = append(r.args, arg)
	}
	r.inFlight = false
	return r.args, nil
}

// readBulk reads one `$<len>\r\n<bytes>\r\n` argument into the arena.
func (r *Reader) readBulk() ([]byte, error) {
	t, err := r.br.ReadByte()
	if err != nil {
		return nil, err
	}
	if t != TypeBulkString {
		return nil, protoErrf("expected '$', got '%c'", t)
	}
	n, err := r.readCount("bulk")
	if err != nil {
		return nil, err
	}
	if n < 0 || int64(n) > r.lim.MaxBulkSize {
		return nil, protoErrf("invalid bulk length")
	}

	buf := r.alloc(n)
	if _, err := io.ReadFull(r.br, buf); err != nil {
		return nil, ioOrUnexpected(err)
	}
	// Consume and validate the trailing CRLF. A client that lies about the
	// length would otherwise desynchronise the stream silently.
	var tail [2]byte
	if _, err := io.ReadFull(r.br, tail[:]); err != nil {
		return nil, ioOrUnexpected(err)
	}
	if tail[0] != '\r' || tail[1] != '\n' {
		return nil, protoErrf("bulk string not terminated by CRLF")
	}
	return buf, nil
}

// alloc hands out n bytes of argument storage.
//
// The arena is a chain of blocks rather than one growable slice on purpose: a
// growable slice would reallocate mid-command and silently invalidate the
// argument slices already handed out for that same command. Allocating a fresh
// block instead leaves earlier slices pointing at still-live memory.
func (r *Reader) alloc(n int) []byte {
	if n > arenaBlock {
		return make([]byte, n)
	}
	if cap(r.block)-len(r.block) < n {
		r.block = make([]byte, 0, arenaBlock)
	}
	start := len(r.block)
	r.block = r.block[:start+n]
	return r.block[start : start+n : start+n]
}

// readCount reads a decimal integer terminated by CRLF.
func (r *Reader) readCount(what string) (int, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}
	v, ok := parseInt(line)
	if !ok {
		return 0, protoErrf("invalid %s length", what)
	}
	if v > int64(^uint(0)>>1) || v < -int64(^uint(0)>>1) {
		return 0, protoErrf("invalid %s length", what)
	}
	return int(v), nil
}

// readLine returns one CRLF-terminated line without the terminator. The result
// aliases the bufio buffer and is only valid until the next read.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return nil, protoErrf("too big inline request")
		}
		return nil, ioOrUnexpected(err)
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, protoErrf("unbalanced line terminator")
	}
	return line[:len(line)-2], nil
}

// readInline parses the legacy inline protocol, where a bare line of
// whitespace-separated words is a command. telnet and health probes use it.
func (r *Reader) readInline() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) > r.lim.MaxInlineSize {
		return nil, protoErrf("too big inline request")
	}
	buf := r.alloc(len(line))
	copy(buf, line)

	for i := 0; i < len(buf); {
		for i < len(buf) && isSpace(buf[i]) {
			i++
		}
		if i == len(buf) {
			break
		}
		start := i
		for i < len(buf) && !isSpace(buf[i]) {
			i++
		}
		r.args = append(r.args, buf[start:i:i])
	}
	if len(r.args) == 0 {
		return nil, nil
	}
	return r.args, nil
}

func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n' || b == '\v' || b == '\f'
}

// ioOrUnexpected normalises a mid-value EOF. io.EOF between commands is a
// clean disconnect; io.EOF inside a value means the client vanished mid-frame,
// which callers must not mistake for an orderly close.
func ioOrUnexpected(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

// parseInt parses a base-10 signed integer without allocating.
//
// The accumulator is unsigned so that the most negative int64 is
// representable. Accumulating in int64 and negating at the end cannot parse
// -9223372036854775808, because its magnitude is one past the largest positive
// int64, and a parser that rejects a value the type can hold makes INCR and
// DECR behave differently at the two ends of the range.
func parseInt(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	neg := false
	i := 0
	if b[0] == '-' || b[0] == '+' {
		neg = b[0] == '-'
		i++
		if i == len(b) {
			return 0, false
		}
	}
	limit := uint64(1<<63 - 1)
	if neg {
		limit = 1 << 63
	}
	var v uint64
	for ; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		d := uint64(c - '0')
		if v > (limit-d)/10 {
			return 0, false
		}
		v = v*10 + d
	}
	if neg {
		// Negating through uint64 keeps the minimum value from overflowing on
		// the way back into int64.
		return -int64(v-1) - 1, true
	}
	return int64(v), true
}

// ParseInt exposes the allocation-free integer parser to command handlers,
// which need the same "strict decimal, no leading zeros tolerance" semantics
// Redis applies to arguments like EXPIRE seconds.
func ParseInt(b []byte) (int64, bool) { return parseInt(b) }

// InFlight reports whether the reader stopped part-way through a command.
//
// The server uses it to tell two very different timeouts apart. A read
// deadline that fires with nothing in flight is a healthy idle client and the
// connection should survive. One that fires mid-command is a client that sent
// a partial frame and stopped, which is a slowloris: it costs a goroutine and
// a buffer for as long as it is tolerated, so the connection is dropped.
func (r *Reader) InFlight() bool { return r.inFlight }

// HasCompleteCommand reports whether a whole command is already buffered.
//
// This is what makes reply batching safe. A server that keeps reading while
// any bytes are buffered can block mid-frame while holding replies the client
// is waiting for, and the two sides deadlock until a timeout breaks it. By
// only continuing the batch when a complete command is already in memory, the
// server can never block with an unflushed reply.
//
// It parses framing without consuming anything and never allocates.
func (r *Reader) HasCompleteCommand() bool {
	n := r.br.Buffered()
	if n == 0 {
		return false
	}
	buf, err := r.br.Peek(n)
	if err != nil {
		return false
	}
	if buf[0] != TypeArray {
		// An inline command is complete once a newline is present.
		for _, b := range buf {
			if b == '\n' {
				return true
			}
		}
		return false
	}

	count, rest, ok := peekCount(buf[1:])
	if !ok {
		return false
	}
	if count <= 0 {
		return true
	}
	if count > r.lim.MaxMultiBulkLen {
		// Over the limit, so the next real read will reject it. Reporting the
		// command as complete lets that rejection happen promptly instead of
		// stalling until a timeout.
		return true
	}
	for i := 0; i < count; i++ {
		if len(rest) == 0 || rest[0] != TypeBulkString {
			return len(rest) > 0 // malformed: let the real reader produce the error
		}
		size, tail, ok := peekCount(rest[1:])
		if !ok {
			return false
		}
		if size < 0 || int64(size) > r.lim.MaxBulkSize {
			return true
		}
		if len(tail) < size+2 {
			return false
		}
		rest = tail[size+2:]
	}
	return true
}

// peekCount reads a CRLF-terminated integer from buf without consuming it.
func peekCount(buf []byte) (n int, rest []byte, ok bool) {
	for i := 0; i+1 < len(buf); i++ {
		if buf[i] != '\r' || buf[i+1] != '\n' {
			continue
		}
		v, valid := parseInt(buf[:i])
		if !valid {
			// Malformed, but fully framed: the real reader will reject it.
			return 0, buf[i+2:], true
		}
		if v > int64(^uint(0)>>1) || v < -int64(^uint(0)>>1) {
			return 0, buf[i+2:], true
		}
		return int(v), buf[i+2:], true
	}
	return 0, nil, false
}
