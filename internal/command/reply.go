package command

import (
	"fmt"
	"strconv"
)

// Redis error replies are a two-part convention: an uppercase code followed by
// a human sentence. Clients switch on the code, so the codes below are fixed
// strings rather than formatted text.
const (
	codeErr       = "ERR"
	codeWrongType = "WRONGTYPE"
	codeNoAuth    = "NOAUTH"
	codeNoPerm    = "NOPERM"
	codeExecAbort = "EXECABORT"
	codeMisconf   = "MISCONF"
)

// Common error sentences, matching real Redis wording so that existing client
// libraries which string-match on them keep working.
const (
	msgWrongArgs      = "wrong number of arguments for '%s' command"
	msgNotInteger     = "value is not an integer or out of range"
	msgNotFloat       = "value is not a valid float"
	msgSyntax         = "syntax error"
	msgIncrOverflow   = "increment or decrement would overflow"
	msgInvalidExpire  = "invalid expire time in '%s' command"
	msgUnknownCommand = "unknown command '%s', with args beginning with:"
	msgNoAuth         = "Authentication required."
	msgAuthNotSet     = "Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?"
	msgWrongPass      = "invalid username-password pair or user is disabled."
	msgOffsetRange    = "offset is out of range"
	msgStringTooLong  = "string exceeds maximum allowed size (proto-max-bulk-len)"
	msgDBIndexRange   = "DB index is out of range"
	msgSingleDB       = "this server implements a single database; SELECT accepts only index 0"
)

// Err writes a generic error reply.
func (c *Context) Err(format string, args ...any) error {
	c.Env.Stats().CommandsFailed.Add(1)
	c.W.WriteError(codeErr + " " + fmt.Sprintf(format, args...))
	return nil
}

// ErrCode writes an error reply with an explicit code.
func (c *Context) ErrCode(code, format string, args ...any) error {
	c.Env.Stats().CommandsFailed.Add(1)
	c.W.WriteError(code + " " + fmt.Sprintf(format, args...))
	return nil
}

// ErrWrongArgs reports an arity violation.
func (c *Context) ErrWrongArgs() error { return c.Err(msgWrongArgs, c.name) }

// ErrSyntax reports an unparseable option combination.
func (c *Context) ErrSyntax() error { return c.Err(msgSyntax) }

// ErrNotInteger reports an argument that should have been an integer.
func (c *Context) ErrNotInteger() error { return c.Err(msgNotInteger) }

// OK writes the +OK reply.
func (c *Context) OK() error {
	c.W.WriteSimpleString("OK")
	return nil
}

// Int writes an integer reply.
func (c *Context) Int(n int64) error {
	c.W.WriteInt(n)
	return nil
}

// Bulk writes a bulk string reply, or null when b is nil.
func (c *Context) Bulk(b []byte) error {
	c.W.WriteBulk(b)
	return nil
}

// BulkString writes a bulk string reply.
func (c *Context) BulkString(s string) error {
	c.W.WriteBulkString(s)
	return nil
}

// Null writes a null reply.
func (c *Context) Null() error {
	c.W.WriteNull()
	return nil
}

// Bool writes a boolean, which RESP2 renders as 1 or 0.
func (c *Context) Bool(b bool) error {
	c.W.WriteBool(b)
	return nil
}

// SimpleString writes a status reply.
func (c *Context) SimpleString(s string) error {
	c.W.WriteSimpleString(s)
	return nil
}

// intArg parses argument i as an integer, replying with an error if it is not.
func (c *Context) intArg(i int) (int64, bool) {
	v, err := strconv.ParseInt(string(c.Arg(i)), 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// floatArg parses argument i as a float, rejecting NaN because Redis treats it
// as an error rather than a value.
func (c *Context) floatArg(i int) (float64, bool) {
	s := string(c.Arg(i))
	switch s {
	case "inf", "+inf", "infinity", "+infinity":
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v != v {
		return 0, false
	}
	return v, true
}
