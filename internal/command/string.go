package command

import (
	"math"
	"strconv"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/resp"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
)

func registerString(t *Table) {
	t.add(&Command{Name: "get", Arity: 2, Flags: FlagReadonly | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Return the string value of a key", Since: "1.0.0", Handler: cmdGet})
	t.add(&Command{Name: "set", Arity: -3, Flags: FlagWrite, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Set the string value of a key", Since: "1.0.0", Handler: cmdSet})
	t.add(&Command{Name: "setnx", Arity: 3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Set the value of a key only when it does not exist", Since: "1.0.0", Handler: cmdSetNX})
	t.add(&Command{Name: "setex", Arity: 4, Flags: FlagWrite, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Set the value and expiration of a key in seconds", Since: "2.0.0", Handler: cmdSetEX})
	t.add(&Command{Name: "psetex", Arity: 4, Flags: FlagWrite, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Set the value and expiration of a key in milliseconds", Since: "2.6.0", Handler: cmdPSetEX})
	t.add(&Command{Name: "getset", Arity: 3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Set a key and return its old value", Since: "1.0.0", Handler: cmdGetSet})
	t.add(&Command{Name: "getdel", Arity: 2, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Return the value of a key and delete it", Since: "6.2.0", Handler: cmdGetDel})
	t.add(&Command{Name: "getex", Arity: -2, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Return the value of a key and change its expiration", Since: "6.2.0", Handler: cmdGetEx})
	t.add(&Command{Name: "mget", Arity: -2, Flags: FlagReadonly | FlagFast, FirstKey: 1, LastKey: -1, KeyStep: 1,
		Summary: "Return the values of several keys", Since: "1.0.0", Handler: cmdMGet})
	t.add(&Command{Name: "mset", Arity: -3, Flags: FlagWrite, FirstKey: 1, LastKey: -1, KeyStep: 2,
		Summary: "Set several keys", Since: "1.0.1", Handler: cmdMSet})
	t.add(&Command{Name: "msetnx", Arity: -3, Flags: FlagWrite, FirstKey: 1, LastKey: -1, KeyStep: 2,
		Summary: "Set several keys only when none of them exist", Since: "1.0.1", Handler: cmdMSetNX})
	t.add(&Command{Name: "append", Arity: 3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Append a value to a key", Since: "2.0.0", Handler: cmdAppend})
	t.add(&Command{Name: "strlen", Arity: 2, Flags: FlagReadonly | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Return the length of the value stored at a key", Since: "2.2.0", Handler: cmdStrlen})
	t.add(&Command{Name: "incr", Arity: 2, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Increment the integer value of a key by one", Since: "1.0.0", Handler: cmdIncr})
	t.add(&Command{Name: "decr", Arity: 2, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Decrement the integer value of a key by one", Since: "1.0.0", Handler: cmdDecr})
	t.add(&Command{Name: "incrby", Arity: 3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Increment the integer value of a key", Since: "1.0.0", Handler: cmdIncrBy})
	t.add(&Command{Name: "decrby", Arity: 3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Decrement the integer value of a key", Since: "1.0.0", Handler: cmdDecrBy})
	t.add(&Command{Name: "incrbyfloat", Arity: 3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Increment the float value of a key", Since: "2.6.0", Handler: cmdIncrByFloat})
	t.add(&Command{Name: "getrange", Arity: 4, Flags: FlagReadonly, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Return a substring of the value at a key", Since: "2.4.0", Handler: cmdGetRange})
	t.add(&Command{Name: "substr", Arity: 4, Flags: FlagReadonly, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Deprecated alias for GETRANGE", Since: "1.0.0", Handler: cmdGetRange})
	t.add(&Command{Name: "setrange", Arity: 4, Flags: FlagWrite, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Overwrite part of the value at a key", Since: "2.2.0", Handler: cmdSetRange})
}

func cmdGet(c *Context) error {
	rec, ok, err := c.Store().Get(c.Ctx, c.Arg(1))
	if err != nil {
		return storeErr(c, err)
	}
	if !ok {
		c.Miss()
		return c.Null()
	}
	c.Hit()
	return c.Bulk(rec.Value)
}

// setFlags captures the option soup SET accepts.
type setFlags struct {
	nx, xx   bool
	get      bool
	keepTTL  bool
	hasExp   bool
	expireAt int64
}

// parseSetOptions reads SET's trailing options starting at argument from.
func parseSetOptions(c *Context, from int) (setFlags, bool) {
	var f setFlags
	now := c.NowMs()
	for i := from; i < c.Argc(); i++ {
		switch c.ArgLower(i) {
		case "nx":
			if f.xx {
				return f, false
			}
			f.nx = true
		case "xx":
			if f.nx {
				return f, false
			}
			f.xx = true
		case "get":
			f.get = true
		case "keepttl":
			if f.hasExp {
				return f, false
			}
			f.keepTTL = true
		case "ex", "px", "exat", "pxat":
			if f.hasExp || f.keepTTL || i+1 >= c.Argc() {
				return f, false
			}
			unit := c.ArgLower(i)
			n, ok := c.intArg(i + 1)
			if !ok {
				return f, false
			}
			at, ok := absoluteExpiry(unit, n, now)
			if !ok {
				return f, false
			}
			f.hasExp, f.expireAt = true, at
			i++
		default:
			return f, false
		}
	}
	return f, true
}

// absoluteExpiry converts a relative or absolute TTL argument into an absolute
// Unix millisecond deadline.
//
// Everything below the storage layer speaks absolute time. Converting here, at
// the edge, means a TTL is pinned to a wall-clock instant the moment the
// command is accepted, so replaying it from the log or loading it from a
// snapshot cannot quietly extend it by the length of the outage.
func absoluteExpiry(unit string, n, nowMs int64) (int64, bool) {
	switch unit {
	case "ex":
		if n > math.MaxInt64/1000 || n < math.MinInt64/1000 {
			return 0, false
		}
		return addClamped(nowMs, n*1000), true
	case "px":
		return addClamped(nowMs, n), true
	case "exat":
		if n > math.MaxInt64/1000 || n < math.MinInt64/1000 {
			return 0, false
		}
		return n * 1000, true
	case "pxat":
		return n, true
	}
	return 0, false
}

// addClamped adds without wrapping. A TTL of nine quintillion seconds is
// nonsense, but wrapping it into the past and instantly deleting the key would
// be worse than saturating.
func addClamped(a, b int64) int64 {
	if b > 0 && a > math.MaxInt64-b {
		return math.MaxInt64
	}
	if b < 0 && a < math.MinInt64-b {
		return math.MinInt64
	}
	return a + b
}

func cmdSet(c *Context) error {
	key, val := c.Arg(1), c.Arg(2)
	f, ok := parseSetOptions(c, 3)
	if !ok {
		return c.ErrSyntax()
	}
	// An expiry that has already passed is legal and means "set then
	// immediately gone", but a zero or negative EX/PX is a client bug and
	// Redis rejects it rather than guessing.
	if f.hasExp {
		if n, _ := c.intArg(indexOfExpiryValue(c)); n <= 0 && isRelativeExpiry(c) {
			return c.Err(msgInvalidExpire, "set")
		}
	}

	var (
		prev    []byte
		prevSet bool
		wrote   bool
	)
	err := c.Store().Update(c.Ctx, key, func(cur store.Record, found bool) (store.Record, store.Action, error) {
		if f.get && found {
			// cur.Value belongs to the engine and dies with this callback, so
			// the reply value has to be copied out now.
			prev = append([]byte(nil), cur.Value...)
			prevSet = true
		}
		if (f.nx && found) || (f.xx && !found) {
			return store.Record{}, store.ActionNone, nil
		}
		next := store.Record{Value: val}
		switch {
		case f.hasExp:
			next.ExpireAt = f.expireAt
		case f.keepTTL && found:
			next.ExpireAt = cur.ExpireAt
		}
		wrote = true
		return next, store.ActionPut, nil
	})
	if err != nil {
		return storeErr(c, err)
	}

	if f.get {
		if !prevSet {
			return c.Null()
		}
		return c.Bulk(prev)
	}
	if !wrote {
		// A conditional SET that did not fire replies with a null, and RESP2
		// spells that as a null bulk string rather than a null array.
		return c.Null()
	}
	return c.OK()
}

// indexOfExpiryValue locates the numeric argument of the EX/PX/EXAT/PXAT
// option, used only to produce Redis's specific error for a non-positive TTL.
func indexOfExpiryValue(c *Context) int {
	for i := 3; i < c.Argc()-1; i++ {
		switch c.ArgLower(i) {
		case "ex", "px", "exat", "pxat":
			return i + 1
		}
	}
	return 0
}

func isRelativeExpiry(c *Context) bool {
	for i := 3; i < c.Argc()-1; i++ {
		switch c.ArgLower(i) {
		case "ex", "px":
			return true
		case "exat", "pxat":
			return false
		}
	}
	return false
}

func cmdSetNX(c *Context) error {
	var wrote bool
	err := c.Store().Update(c.Ctx, c.Arg(1), func(cur store.Record, found bool) (store.Record, store.Action, error) {
		if found {
			return store.Record{}, store.ActionNone, nil
		}
		wrote = true
		return store.Record{Value: c.Arg(2)}, store.ActionPut, nil
	})
	if err != nil {
		return storeErr(c, err)
	}
	return c.Bool(wrote)
}

func cmdSetEX(c *Context) error  { return setWithTTL(c, 1000) }
func cmdPSetEX(c *Context) error { return setWithTTL(c, 1) }

func setWithTTL(c *Context, unitMs int64) error {
	n, ok := c.intArg(2)
	if !ok {
		return c.ErrNotInteger()
	}
	if n <= 0 {
		return c.Err(msgInvalidExpire, c.name)
	}
	if n > math.MaxInt64/unitMs {
		return c.Err(msgInvalidExpire, c.name)
	}
	rec := store.Record{Value: c.Arg(3), ExpireAt: addClamped(c.NowMs(), n*unitMs)}
	if err := c.Store().Put(c.Ctx, c.Arg(1), rec); err != nil {
		return storeErr(c, err)
	}
	return c.OK()
}

func cmdGetSet(c *Context) error {
	var prev []byte
	var found bool
	err := c.Store().Update(c.Ctx, c.Arg(1), func(cur store.Record, ok bool) (store.Record, store.Action, error) {
		if ok {
			prev = append([]byte(nil), cur.Value...)
			found = true
		}
		// GETSET clears any TTL, which is the historical behaviour SET without
		// KEEPTTL also has.
		return store.Record{Value: c.Arg(2)}, store.ActionPut, nil
	})
	if err != nil {
		return storeErr(c, err)
	}
	if !found {
		return c.Null()
	}
	return c.Bulk(prev)
}

func cmdGetDel(c *Context) error {
	var prev []byte
	var found bool
	err := c.Store().Update(c.Ctx, c.Arg(1), func(cur store.Record, ok bool) (store.Record, store.Action, error) {
		if !ok {
			return store.Record{}, store.ActionNone, nil
		}
		prev = append([]byte(nil), cur.Value...)
		found = true
		return store.Record{}, store.ActionDelete, nil
	})
	if err != nil {
		return storeErr(c, err)
	}
	if !found {
		c.Miss()
		return c.Null()
	}
	c.Hit()
	return c.Bulk(prev)
}

func cmdGetEx(c *Context) error {
	var (
		hasExp, persist bool
		expireAt        int64
	)
	now := c.NowMs()
	for i := 2; i < c.Argc(); i++ {
		switch c.ArgLower(i) {
		case "persist":
			if hasExp {
				return c.ErrSyntax()
			}
			persist = true
		case "ex", "px", "exat", "pxat":
			if persist || hasExp || i+1 >= c.Argc() {
				return c.ErrSyntax()
			}
			unit := c.ArgLower(i)
			n, ok := c.intArg(i + 1)
			if !ok {
				return c.ErrNotInteger()
			}
			if (unit == "ex" || unit == "px") && n <= 0 {
				return c.Err(msgInvalidExpire, "getex")
			}
			at, ok := absoluteExpiry(unit, n, now)
			if !ok {
				return c.ErrSyntax()
			}
			hasExp, expireAt = true, at
			i++
		default:
			return c.ErrSyntax()
		}
	}

	var val []byte
	var found bool
	err := c.Store().Update(c.Ctx, c.Arg(1), func(cur store.Record, ok bool) (store.Record, store.Action, error) {
		if !ok {
			return store.Record{}, store.ActionNone, nil
		}
		val = append([]byte(nil), cur.Value...)
		found = true
		switch {
		case persist && cur.ExpireAt != 0:
			return store.Record{Value: cur.Value}, store.ActionPut, nil
		case hasExp:
			return store.Record{Value: cur.Value, ExpireAt: expireAt}, store.ActionPut, nil
		default:
			// GETEX with no options is a pure read and must not journal a
			// write, or a read-heavy workload would fill the log.
			return store.Record{}, store.ActionNone, nil
		}
	})
	if err != nil {
		return storeErr(c, err)
	}
	if !found {
		c.Miss()
		return c.Null()
	}
	c.Hit()
	return c.Bulk(val)
}

func cmdMGet(c *Context) error {
	c.W.WriteArrayHeader(c.Argc() - 1)
	for i := 1; i < c.Argc(); i++ {
		rec, ok, err := c.Store().Get(c.Ctx, c.Arg(i))
		if err != nil {
			// The array header is already on the wire, so the reply must be
			// completed with the right element count. Filling the remainder
			// with nulls keeps the client's framing valid; the error surfaces
			// through logs and metrics instead.
			for ; i < c.Argc(); i++ {
				c.W.WriteNull()
			}
			return err
		}
		if !ok {
			c.Miss()
			c.W.WriteNull()
			continue
		}
		c.Hit()
		c.W.WriteBulk(rec.Value)
	}
	return nil
}

func cmdMSet(c *Context) error {
	if c.Argc()%2 != 1 {
		return c.ErrWrongArgs()
	}
	muts := make([]store.Mutation, 0, (c.Argc()-1)/2)
	for i := 1; i < c.Argc(); i += 2 {
		muts = append(muts, store.Mutation{Key: c.Arg(i), Record: store.Record{Value: c.Arg(i + 1)}})
	}
	if err := c.Store().MultiWrite(c.Ctx, muts); err != nil {
		return storeErr(c, err)
	}
	return c.OK()
}

func cmdMSetNX(c *Context) error {
	if c.Argc()%2 != 1 {
		return c.ErrWrongArgs()
	}
	// MSETNX is all-or-nothing, so existence has to be checked for every key
	// before any of them is written. Without a cross-key lock this check and
	// the write are not one atomic step, so a concurrent SET between them can
	// make MSETNX overwrite a key that appeared in the gap. That window is
	// documented in docs/adr/ADR-005 and closes when the command log lands in
	// the consensus phase.
	for i := 1; i < c.Argc(); i += 2 {
		_, ok, err := c.Store().Get(c.Ctx, c.Arg(i))
		if err != nil {
			return storeErr(c, err)
		}
		if ok {
			return c.Int(0)
		}
	}
	muts := make([]store.Mutation, 0, (c.Argc()-1)/2)
	for i := 1; i < c.Argc(); i += 2 {
		muts = append(muts, store.Mutation{Key: c.Arg(i), Record: store.Record{Value: c.Arg(i + 1)}})
	}
	if err := c.Store().MultiWrite(c.Ctx, muts); err != nil {
		return storeErr(c, err)
	}
	return c.Int(1)
}

func cmdAppend(c *Context) error {
	suffix := c.Arg(2)
	maxLen := c.maxValueLen()
	var newLen int64
	err := c.Store().Update(c.Ctx, c.Arg(1), func(cur store.Record, found bool) (store.Record, store.Action, error) {
		if int64(len(cur.Value))+int64(len(suffix)) > maxLen {
			return store.Record{}, store.ActionNone, errStringTooLong
		}
		next := make([]byte, 0, len(cur.Value)+len(suffix))
		next = append(next, cur.Value...)
		next = append(next, suffix...)
		newLen = int64(len(next))
		return store.Record{Value: next, ExpireAt: cur.ExpireAt}, store.ActionPut, nil
	})
	if err != nil {
		return storeErr(c, err)
	}
	return c.Int(newLen)
}

func cmdStrlen(c *Context) error {
	rec, ok, err := c.Store().Get(c.Ctx, c.Arg(1))
	if err != nil {
		return storeErr(c, err)
	}
	if !ok {
		c.Miss()
		return c.Int(0)
	}
	c.Hit()
	return c.Int(int64(len(rec.Value)))
}

func cmdIncr(c *Context) error { return incrBy(c, 1) }
func cmdDecr(c *Context) error { return incrBy(c, -1) }

func cmdIncrBy(c *Context) error {
	n, ok := c.intArg(2)
	if !ok {
		return c.ErrNotInteger()
	}
	return incrBy(c, n)
}

func cmdDecrBy(c *Context) error {
	n, ok := c.intArg(2)
	if !ok {
		return c.ErrNotInteger()
	}
	// DECRBY of the most negative int64 cannot be expressed as a positive
	// increment, so it overflows rather than silently wrapping to itself.
	if n == math.MinInt64 {
		return c.Err(msgIncrOverflow)
	}
	return incrBy(c, -n)
}

func incrBy(c *Context, delta int64) error {
	var result int64
	err := c.Store().Update(c.Ctx, c.Arg(1), func(cur store.Record, found bool) (store.Record, store.Action, error) {
		var base int64
		if found {
			v, ok := parseStrictInt(cur.Value)
			if !ok {
				return store.Record{}, store.ActionNone, errNotInteger
			}
			base = v
		}
		if (delta > 0 && base > math.MaxInt64-delta) || (delta < 0 && base < math.MinInt64-delta) {
			return store.Record{}, store.ActionNone, errIncrOverflow
		}
		result = base + delta
		return store.Record{Value: strconv.AppendInt(nil, result, 10), ExpireAt: cur.ExpireAt}, store.ActionPut, nil
	})
	if err != nil {
		return storeErr(c, err)
	}
	return c.Int(result)
}

func cmdIncrByFloat(c *Context) error {
	delta, ok := c.floatArg(2)
	if !ok {
		return c.Err(msgNotFloat)
	}
	var result string
	err := c.Store().Update(c.Ctx, c.Arg(1), func(cur store.Record, found bool) (store.Record, store.Action, error) {
		base := 0.0
		if found {
			v, err := strconv.ParseFloat(string(cur.Value), 64)
			if err != nil {
				return store.Record{}, store.ActionNone, errNotFloat
			}
			base = v
		}
		sum := base + delta
		if math.IsNaN(sum) || math.IsInf(sum, 0) {
			return store.Record{}, store.ActionNone, errNaNOrInf
		}
		result = formatFloat(sum)
		return store.Record{Value: []byte(result), ExpireAt: cur.ExpireAt}, store.ActionPut, nil
	})
	if err != nil {
		return storeErr(c, err)
	}
	return c.BulkString(result)
}

// formatFloat renders an INCRBYFLOAT result the way Redis does: the shortest
// decimal that round-trips, with no exponent and no trailing zeros.
func formatFloat(f float64) string {
	s := strconv.FormatFloat(f, 'f', -1, 64)
	if s == "-0" {
		return "0"
	}
	return s
}

func cmdGetRange(c *Context) error {
	start, ok1 := c.intArg(2)
	end, ok2 := c.intArg(3)
	if !ok1 || !ok2 {
		return c.ErrNotInteger()
	}
	rec, ok, err := c.Store().Get(c.Ctx, c.Arg(1))
	if err != nil {
		return storeErr(c, err)
	}
	if !ok {
		c.Miss()
		return c.BulkString("")
	}
	c.Hit()
	lo, hi, empty := resolveRange(start, end, int64(len(rec.Value)))
	if empty {
		return c.BulkString("")
	}
	return c.Bulk(rec.Value[lo : hi+1])
}

// resolveRange applies Redis's inclusive, negative-from-the-end index rules.
func resolveRange(start, end, length int64) (lo, hi int64, empty bool) {
	if length == 0 {
		return 0, 0, true
	}
	if start < 0 {
		start += length
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end += length
		if end < 0 {
			// A negative end that is still out of range after adjustment
			// selects nothing, rather than clamping to the first byte.
			return 0, 0, true
		}
	}
	if end >= length {
		end = length - 1
	}
	if start > end || start >= length {
		return 0, 0, true
	}
	return start, end, false
}

func cmdSetRange(c *Context) error {
	offset, ok := c.intArg(2)
	if !ok {
		return c.ErrNotInteger()
	}
	if offset < 0 {
		return c.Err(msgOffsetRange)
	}
	patch := c.Arg(3)
	maxLen := c.maxValueLen()
	if offset+int64(len(patch)) > maxLen {
		return c.Err(msgStringTooLong)
	}

	var newLen int64
	err := c.Store().Update(c.Ctx, c.Arg(1), func(cur store.Record, found bool) (store.Record, store.Action, error) {
		if len(patch) == 0 {
			// SETRANGE with an empty patch never creates a key and never
			// changes one; it only reports the current length.
			newLen = int64(len(cur.Value))
			return store.Record{}, store.ActionNone, nil
		}
		size := max(int64(len(cur.Value)), offset+int64(len(patch)))
		// The gap between the old end and the offset is filled with zero
		// bytes, which is why the buffer is allocated zeroed rather than
		// appended to.
		next := make([]byte, size)
		copy(next, cur.Value)
		copy(next[offset:], patch)
		newLen = size
		return store.Record{Value: next, ExpireAt: cur.ExpireAt}, store.ActionPut, nil
	})
	if err != nil {
		return storeErr(c, err)
	}
	return c.Int(newLen)
}

// maxValueLen is the largest value the server will hold, taken from the same
// limit that bounds an inbound bulk string so a value can always be read back.
func (c *Context) maxValueLen() int64 {
	if v, ok := c.Env.ConfigGet("proto-max-bulk-len"); ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return resp.DefaultLimits().MaxBulkSize
}

// parseStrictInt applies Redis's integer rules: no leading or trailing space,
// no leading zeros, no plus sign. "007" is a string, not the number seven, and
// INCR must refuse it rather than quietly rewriting the value.
func parseStrictInt(b []byte) (int64, bool) {
	if len(b) == 0 || len(b) > 20 {
		return 0, false
	}
	i := 0
	neg := false
	if b[0] == '-' {
		neg = true
		i = 1
		if len(b) == 1 {
			return 0, false
		}
	}
	if b[i] == '0' && len(b)-i > 1 {
		return 0, false
	}
	v, ok := resp.ParseInt(b)
	if !ok {
		return 0, false
	}
	if neg && v > 0 {
		return 0, false
	}
	return v, true
}
