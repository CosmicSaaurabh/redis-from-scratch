package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// ReplyReader parses server replies. It is the mirror of Reader: clients only
// ever send commands, and servers send the full type surface, so the two
// directions get separate parsers instead of one that is bad at both.
//
// It is used by the benchmark harness and the end-to-end tests, which need to
// speak to the server the way a real client library does.
type ReplyReader struct {
	br  *bufio.Reader
	lim Limits
}

// NewReplyReader wraps rd.
func NewReplyReader(rd io.Reader, lim Limits) *ReplyReader {
	if lim.MaxBulkSize <= 0 {
		lim = DefaultLimits()
	}
	if lim.ReadBufferSize <= 0 {
		lim.ReadBufferSize = DefaultLimits().ReadBufferSize
	}
	return &ReplyReader{br: bufio.NewReaderSize(rd, lim.ReadBufferSize), lim: lim}
}

// Buffered reports how many reply bytes are already in memory.
func (r *ReplyReader) Buffered() int { return r.br.Buffered() }

// Reply is one decoded server reply.
type Reply struct {
	// Kind is the RESP type byte.
	Kind byte
	// Str carries simple strings, bulk strings, errors, verbatim strings and
	// big numbers.
	Str []byte
	// Int carries integers.
	Int int64
	// Double carries RESP3 doubles.
	Double float64
	// Bool carries RESP3 booleans.
	Bool bool
	// Elems carries arrays, sets, pushes and flattened maps. A map's elements
	// alternate key, value.
	Elems []Reply
	// Null is true for the null reply in any of its spellings.
	Null bool
}

// IsError reports whether the reply is an error.
func (r Reply) IsError() bool { return r.Kind == TypeError || r.Kind == TypeBlobError }

// String renders the reply for test failure messages.
func (r Reply) String() string {
	switch r.Kind {
	case TypeArray, TypeSet, TypePush, TypeMap:
		s := "["
		for i, e := range r.Elems {
			if i > 0 {
				s += " "
			}
			s += e.String()
		}
		return s + "]"
	case TypeInteger:
		return strconv.FormatInt(r.Int, 10)
	case TypeBoolean:
		return strconv.FormatBool(r.Bool)
	case TypeDouble:
		return FormatDouble(r.Double)
	case TypeNull:
		return "(nil)"
	}
	if r.Null {
		return "(nil)"
	}
	return string(r.Str)
}

// ErrTooDeep guards against a reply that nests far enough to exhaust the
// stack. A server would not send one, but a client must not be trivially
// crashable by whatever it happens to be pointed at.
var ErrTooDeep = errors.New("resp: reply nesting too deep")

const maxReplyDepth = 64

// Read decodes one reply.
func (r *ReplyReader) Read() (Reply, error) { return r.read(0) }

func (r *ReplyReader) read(depth int) (Reply, error) {
	if depth > maxReplyDepth {
		return Reply{}, ErrTooDeep
	}
	t, err := r.br.ReadByte()
	if err != nil {
		return Reply{}, err
	}
	line, err := r.readLine()
	if err != nil {
		return Reply{}, err
	}
	rep := Reply{Kind: t}

	switch t {
	case TypeSimpleString, TypeError, TypeBigNumber:
		rep.Str = append([]byte(nil), line...)
		return rep, nil

	case TypeInteger:
		v, ok := parseInt(line)
		if !ok {
			return rep, protoErrf("bad integer reply %q", line)
		}
		rep.Int = v
		return rep, nil

	case TypeNull:
		rep.Null = true
		return rep, nil

	case TypeBoolean:
		rep.Bool = len(line) == 1 && line[0] == 't'
		return rep, nil

	case TypeDouble:
		f, err := parseDouble(line)
		if err != nil {
			return rep, err
		}
		rep.Double = f
		return rep, nil

	case TypeBulkString, TypeVerbatim, TypeBlobError:
		n, ok := parseInt(line)
		if !ok {
			return rep, protoErrf("bad bulk length %q", line)
		}
		if n < 0 {
			rep.Null = true
			return rep, nil
		}
		if n > r.lim.MaxBulkSize {
			return rep, protoErrf("bulk reply of %d bytes exceeds the limit", n)
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r.br, buf); err != nil {
			return rep, ioOrUnexpected(err)
		}
		rep.Str = buf[:n]
		return rep, nil

	case TypeArray, TypeSet, TypePush, TypeMap:
		n, ok := parseInt(line)
		if !ok {
			return rep, protoErrf("bad aggregate length %q", line)
		}
		if n < 0 {
			rep.Null = true
			return rep, nil
		}
		if t == TypeMap {
			n *= 2
		}
		if n > int64(r.lim.MaxMultiBulkLen) {
			return rep, protoErrf("aggregate reply of %d elements exceeds the limit", n)
		}
		rep.Elems = make([]Reply, 0, n)
		for i := int64(0); i < n; i++ {
			e, err := r.read(depth + 1)
			if err != nil {
				return rep, err
			}
			rep.Elems = append(rep.Elems, e)
		}
		return rep, nil

	case TypeAttribute:
		// Attributes are metadata attached to the next reply. Skip the
		// attribute map and return whatever follows it, which is what a client
		// that does not use attributes must do.
		n, ok := parseInt(line)
		if !ok {
			return rep, protoErrf("bad attribute length %q", line)
		}
		for i := int64(0); i < n*2; i++ {
			if _, err := r.read(depth + 1); err != nil {
				return rep, err
			}
		}
		return r.read(depth)

	default:
		return rep, protoErrf("unknown reply type '%c'", t)
	}
}

// Discard consumes one reply without building it.
//
// The benchmark harness must read every reply to keep the connection framed,
// but allocating a value tree per reply would make the load generator's own
// garbage collector part of what is being measured.
func (r *ReplyReader) Discard() error { return r.discard(0) }

func (r *ReplyReader) discard(depth int) error {
	if depth > maxReplyDepth {
		return ErrTooDeep
	}
	t, err := r.br.ReadByte()
	if err != nil {
		return err
	}
	line, err := r.readLine()
	if err != nil {
		return err
	}
	switch t {
	case TypeSimpleString, TypeError, TypeInteger, TypeNull, TypeBoolean, TypeDouble, TypeBigNumber:
		return nil

	case TypeBulkString, TypeVerbatim, TypeBlobError:
		n, ok := parseInt(line)
		if !ok {
			return protoErrf("bad bulk length %q", line)
		}
		if n < 0 {
			return nil
		}
		if n > r.lim.MaxBulkSize {
			return protoErrf("bulk reply of %d bytes exceeds the limit", n)
		}
		if _, err := r.br.Discard(int(n) + 2); err != nil {
			return ioOrUnexpected(err)
		}
		return nil

	case TypeArray, TypeSet, TypePush, TypeMap, TypeAttribute:
		n, ok := parseInt(line)
		if !ok {
			return protoErrf("bad aggregate length %q", line)
		}
		if n < 0 {
			return nil
		}
		if t == TypeMap || t == TypeAttribute {
			n *= 2
		}
		for i := int64(0); i < n; i++ {
			if err := r.discard(depth + 1); err != nil {
				return err
			}
		}
		if t == TypeAttribute {
			return r.discard(depth)
		}
		return nil

	default:
		return protoErrf("unknown reply type '%c'", t)
	}
}

func (r *ReplyReader) readLine() ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return nil, protoErrf("reply line exceeds the read buffer")
		}
		return nil, ioOrUnexpected(err)
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, protoErrf("reply line not terminated by CRLF")
	}
	return line[:len(line)-2], nil
}

func parseDouble(b []byte) (float64, error) {
	switch string(b) {
	case "inf", "+inf":
		return posInf, nil
	case "-inf":
		return negInf, nil
	case "nan":
		return nan, nil
	}
	f, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return 0, fmt.Errorf("resp: bad double reply %q: %w", b, err)
	}
	return f, nil
}
