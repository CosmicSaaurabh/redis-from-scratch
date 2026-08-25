package command

import (
	"context"
	"errors"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
)

// Sentinel errors returned from inside an UpdateFunc.
//
// A handler cannot write its reply from inside the callback, because the
// engine is holding the key and the reply would then be produced under a lock
// it knows nothing about. Instead the callback returns one of these, the
// engine unwinds cleanly without applying anything, and storeErr turns it into
// the right RESP error on the way out.
var (
	errNotInteger    = errors.New("value is not an integer or out of range")
	errNotFloat      = errors.New("value is not a valid float")
	errIncrOverflow  = errors.New("increment or decrement would overflow")
	errNaNOrInf      = errors.New("increment would produce NaN or Infinity")
	errStringTooLong = errors.New("string exceeds maximum allowed size")
)

// storeErr converts an engine or callback error into a client reply.
//
// Returning the error up to the connection loop instead would be wrong for
// almost all of these: they are the client's fault, the connection is
// perfectly healthy, and RESP requires exactly one reply per command. Only
// failures that make the connection unusable are propagated.
func storeErr(c *Context, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, errNotInteger):
		return c.Err(msgNotInteger)
	case errors.Is(err, errNotFloat):
		return c.Err(msgNotFloat)
	case errors.Is(err, errIncrOverflow):
		return c.Err(msgIncrOverflow)
	case errors.Is(err, errNaNOrInf):
		return c.Err("increment would produce NaN or Infinity")
	case errors.Is(err, errStringTooLong):
		return c.Err(msgStringTooLong)
	case errors.Is(err, store.ErrClosed):
		return c.Err("server is shutting down")
	case errors.Is(err, context.Canceled):
		return c.Err("request cancelled")
	case errors.Is(err, context.DeadlineExceeded):
		return c.Err("storage engine timed out")
	default:
		// An unexpected engine failure is reported to the client as a generic
		// error and returned upward so the server can log it with full
		// context. The client sees a reply either way, so its framing survives.
		c.Env.Stats().CommandsFailed.Add(1)
		c.W.WriteError(codeErr + " internal error: " + err.Error())
		return nil
	}
}
