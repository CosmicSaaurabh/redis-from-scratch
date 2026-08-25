package command

import (
	"math/rand/v2"
	"strconv"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
)

func registerGeneric(t *Table) {
	t.add(&Command{Name: "del", Arity: -2, Flags: FlagWrite, FirstKey: 1, LastKey: -1, KeyStep: 1,
		Summary: "Delete one or more keys", Since: "1.0.0", Handler: cmdDel})
	t.add(&Command{Name: "unlink", Arity: -2, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: -1, KeyStep: 1,
		Summary: "Delete one or more keys, reclaiming memory lazily", Since: "4.0.0", Handler: cmdDel})
	t.add(&Command{Name: "exists", Arity: -2, Flags: FlagReadonly | FlagFast, FirstKey: 1, LastKey: -1, KeyStep: 1,
		Summary: "Count how many of the given keys exist", Since: "1.0.0", Handler: cmdExists})
	t.add(&Command{Name: "touch", Arity: -2, Flags: FlagReadonly | FlagFast, FirstKey: 1, LastKey: -1, KeyStep: 1,
		Summary: "Count how many of the given keys exist, updating their access time", Since: "3.2.1", Handler: cmdExists})
	t.add(&Command{Name: "type", Arity: 2, Flags: FlagReadonly | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Return the type of the value stored at a key", Since: "1.0.0", Handler: cmdType})

	t.add(&Command{Name: "expire", Arity: -3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Set a key's time to live in seconds", Since: "1.0.0", Handler: expireCmd("ex")})
	t.add(&Command{Name: "pexpire", Arity: -3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Set a key's time to live in milliseconds", Since: "2.6.0", Handler: expireCmd("px")})
	t.add(&Command{Name: "expireat", Arity: -3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Set the expiration of a key as a Unix timestamp in seconds", Since: "1.2.0", Handler: expireCmd("exat")})
	t.add(&Command{Name: "pexpireat", Arity: -3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Set the expiration of a key as a Unix timestamp in milliseconds", Since: "2.6.0", Handler: expireCmd("pxat")})

	t.add(&Command{Name: "ttl", Arity: 2, Flags: FlagReadonly | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Return a key's remaining time to live in seconds", Since: "1.0.0", Handler: ttlCmd(1000)})
	t.add(&Command{Name: "pttl", Arity: 2, Flags: FlagReadonly | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Return a key's remaining time to live in milliseconds", Since: "2.6.0", Handler: ttlCmd(1)})
	t.add(&Command{Name: "expiretime", Arity: 2, Flags: FlagReadonly | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Return the absolute expiry of a key in seconds", Since: "7.0.0", Handler: expireTimeCmd(1000)})
	t.add(&Command{Name: "pexpiretime", Arity: 2, Flags: FlagReadonly | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Return the absolute expiry of a key in milliseconds", Since: "7.0.0", Handler: expireTimeCmd(1)})
	t.add(&Command{Name: "persist", Arity: 2, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, KeyStep: 1,
		Summary: "Remove a key's expiration", Since: "2.2.0", Handler: cmdPersist})

	t.add(&Command{Name: "keys", Arity: 2, Flags: FlagReadonly, Summary: "Return all keys matching a pattern",
		Since: "1.0.0", Handler: cmdKeys})
	t.add(&Command{Name: "scan", Arity: -2, Flags: FlagReadonly, Summary: "Incrementally iterate the keyspace",
		Since: "2.8.0", Handler: cmdScan})
	t.add(&Command{Name: "randomkey", Arity: 1, Flags: FlagReadonly, Summary: "Return a random key",
		Since: "1.0.0", Handler: cmdRandomKey})
	t.add(&Command{Name: "dbsize", Arity: 1, Flags: FlagReadonly | FlagFast, Summary: "Return the number of keys",
		Since: "1.0.0", Handler: cmdDBSize})

	t.add(&Command{Name: "rename", Arity: 3, Flags: FlagWrite, FirstKey: 1, LastKey: 2, KeyStep: 1,
		Summary: "Rename a key", Since: "1.0.0", Handler: cmdRename})
	t.add(&Command{Name: "renamenx", Arity: 3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 2, KeyStep: 1,
		Summary: "Rename a key only when the destination does not exist", Since: "1.0.0", Handler: cmdRenameNX})
	t.add(&Command{Name: "copy", Arity: -3, Flags: FlagWrite, FirstKey: 1, LastKey: 2, KeyStep: 1,
		Summary: "Copy a key", Since: "6.2.0", Handler: cmdCopy})

	t.add(&Command{Name: "flushdb", Arity: -1, Flags: FlagWrite, Summary: "Remove every key",
		Since: "1.0.0", Handler: cmdFlush})
	t.add(&Command{Name: "flushall", Arity: -1, Flags: FlagWrite, Summary: "Remove every key",
		Since: "1.0.0", Handler: cmdFlush})
}

func cmdDel(c *Context) error {
	var n int64
	for i := 1; i < c.Argc(); i++ {
		removed, err := c.Store().Delete(c.Ctx, c.Arg(i))
		if err != nil {
			return storeErr(c, err)
		}
		if removed {
			n++
		}
	}
	return c.Int(n)
}

func cmdExists(c *Context) error {
	var n int64
	for i := 1; i < c.Argc(); i++ {
		// EXISTS counts repeats: "EXISTS k k" on a live key returns 2. That is
		// deliberate in Redis and clients rely on it.
		_, ok, err := c.Store().Get(c.Ctx, c.Arg(i))
		if err != nil {
			return storeErr(c, err)
		}
		if ok {
			c.Hit()
			n++
		} else {
			c.Miss()
		}
	}
	return c.Int(n)
}

func cmdType(c *Context) error {
	_, ok, err := c.Store().Get(c.Ctx, c.Arg(1))
	if err != nil {
		return storeErr(c, err)
	}
	if !ok {
		return c.SimpleString("none")
	}
	// This server stores strings only; see docs/adr/ADR-003 for why the other
	// container types were left out of the MVP rather than half-implemented.
	return c.SimpleString("string")
}

// expireCmd builds a handler for the four EXPIRE spellings, which differ only
// in the unit and whether the argument is relative or absolute.
func expireCmd(unit string) func(*Context) error {
	return func(c *Context) error {
		n, ok := c.intArg(2)
		if !ok {
			return c.ErrNotInteger()
		}
		at, ok := absoluteExpiry(unit, n, c.NowMs())
		if !ok {
			return c.Err(msgInvalidExpire, c.name)
		}

		var nx, xx, gt, lt bool
		for i := 3; i < c.Argc(); i++ {
			switch c.ArgLower(i) {
			case "nx":
				nx = true
			case "xx":
				xx = true
			case "gt":
				gt = true
			case "lt":
				lt = true
			default:
				return c.ErrSyntax()
			}
		}
		// NX is exclusive with everything else, and GT with LT, because the
		// combinations are contradictory rather than merely redundant.
		if (nx && (xx || gt || lt)) || (gt && lt) {
			return c.Err("NX and XX, GT or LT options at the same time are not compatible")
		}

		now := c.NowMs()
		var applied bool
		err := c.Store().Update(c.Ctx, c.Arg(1), func(cur store.Record, found bool) (store.Record, store.Action, error) {
			if !found {
				return store.Record{}, store.ActionNone, nil
			}
			switch {
			case nx && cur.ExpireAt != 0:
				return store.Record{}, store.ActionNone, nil
			case xx && cur.ExpireAt == 0:
				return store.Record{}, store.ActionNone, nil
			case gt && (cur.ExpireAt == 0 || at <= cur.ExpireAt):
				// A key with no TTL is treated as infinitely far away, so GT
				// can never shorten it.
				return store.Record{}, store.ActionNone, nil
			case lt && cur.ExpireAt != 0 && at >= cur.ExpireAt:
				return store.Record{}, store.ActionNone, nil
			}
			applied = true
			if at <= now {
				// An expiry in the past deletes immediately, and journalling it
				// as a delete rather than a doomed write keeps recovery from
				// depending on when it happens to run.
				return store.Record{}, store.ActionDelete, nil
			}
			return store.Record{Value: cur.Value, ExpireAt: at}, store.ActionPut, nil
		})
		if err != nil {
			return storeErr(c, err)
		}
		return c.Bool(applied)
	}
}

// ttlCmd builds TTL and PTTL. Redis distinguishes three outcomes with two
// negative sentinels: -2 for a missing key and -1 for a key with no expiry.
func ttlCmd(divisor int64) func(*Context) error {
	return func(c *Context) error {
		rec, ok, err := c.Store().Get(c.Ctx, c.Arg(1))
		if err != nil {
			return storeErr(c, err)
		}
		if !ok {
			c.Miss()
			return c.Int(-2)
		}
		c.Hit()
		if rec.ExpireAt == 0 {
			return c.Int(-1)
		}
		remaining := rec.ExpireAt - c.NowMs()
		if remaining < 0 {
			remaining = 0
		}
		if divisor == 1000 {
			// Round to the nearest second so that a 1000 ms TTL reports 1
			// rather than 0 the instant after it is set.
			return c.Int((remaining + 999) / 1000)
		}
		return c.Int(remaining)
	}
}

func expireTimeCmd(divisor int64) func(*Context) error {
	return func(c *Context) error {
		rec, ok, err := c.Store().Get(c.Ctx, c.Arg(1))
		if err != nil {
			return storeErr(c, err)
		}
		if !ok {
			return c.Int(-2)
		}
		if rec.ExpireAt == 0 {
			return c.Int(-1)
		}
		return c.Int(rec.ExpireAt / divisor)
	}
}

func cmdPersist(c *Context) error {
	var cleared bool
	err := c.Store().Update(c.Ctx, c.Arg(1), func(cur store.Record, found bool) (store.Record, store.Action, error) {
		if !found || cur.ExpireAt == 0 {
			return store.Record{}, store.ActionNone, nil
		}
		cleared = true
		return store.Record{Value: cur.Value}, store.ActionPut, nil
	})
	if err != nil {
		return storeErr(c, err)
	}
	return c.Bool(cleared)
}

// keysScanBudget bounds how many keys KEYS will collect.
//
// KEYS is O(N) and blocks the caller, so an unbounded one on a large keyspace
// is a self-inflicted outage. Redis ships it unbounded and tells operators not
// to use it; this server bounds it and says so in the error, which is the same
// advice delivered at a moment the operator is actually reading.
const keysScanBudget = 1_000_000

func cmdKeys(c *Context) error {
	pattern := c.Arg(1)
	trivial := isTrivialPattern(pattern)

	var keys [][]byte
	var cursor uint64
	over := false
	for {
		next, err := c.Store().Scan(c.Ctx, cursor, 1000, func(key []byte, _ store.Record) bool {
			if !trivial && !matchPattern(pattern, key, false) {
				return true
			}
			if len(keys) >= keysScanBudget {
				over = true
				return false
			}
			keys = append(keys, key)
			return true
		})
		if err != nil {
			return storeErr(c, err)
		}
		if over {
			return c.Err("KEYS matched more than %d keys; use SCAN instead", keysScanBudget)
		}
		if next == 0 {
			break
		}
		cursor = next
	}

	c.W.WriteArrayHeader(len(keys))
	for _, k := range keys {
		c.W.WriteBulk(k)
	}
	return nil
}

func cmdScan(c *Context) error {
	cursor, err := strconv.ParseUint(c.ArgString(1), 10, 64)
	if err != nil {
		return c.Err("invalid cursor")
	}
	var (
		pattern  []byte
		count    = 10
		wantType string
	)
	for i := 2; i < c.Argc(); i++ {
		switch c.ArgLower(i) {
		case "match":
			if i+1 >= c.Argc() {
				return c.ErrSyntax()
			}
			pattern = c.Arg(i + 1)
			i++
		case "count":
			if i+1 >= c.Argc() {
				return c.ErrSyntax()
			}
			n, ok := c.intArg(i + 1)
			if !ok || n < 1 {
				return c.ErrSyntax()
			}
			count = int(min(n, 100_000))
			i++
		case "type":
			if i+1 >= c.Argc() {
				return c.ErrSyntax()
			}
			wantType = c.ArgLower(i + 1)
			i++
		default:
			return c.ErrSyntax()
		}
	}
	// The only type this server stores is string, so any other TYPE filter is
	// answered honestly with an empty page rather than an error.
	if wantType != "" && wantType != "string" {
		c.W.WriteArrayHeader(2)
		c.W.WriteBulkString("0")
		c.W.WriteArrayHeader(0)
		return nil
	}

	trivial := pattern == nil || isTrivialPattern(pattern)
	var keys [][]byte
	next, err := c.Store().Scan(c.Ctx, cursor, count, func(key []byte, _ store.Record) bool {
		if trivial || matchPattern(pattern, key, false) {
			keys = append(keys, key)
		}
		return true
	})
	if err != nil {
		return storeErr(c, err)
	}

	c.W.WriteArrayHeader(2)
	c.W.WriteBulkString(strconv.FormatUint(next, 10))
	c.W.WriteArrayHeader(len(keys))
	for _, k := range keys {
		c.W.WriteBulk(k)
	}
	return nil
}

func cmdRandomKey(c *Context) error {
	// Start from a random cursor position so that repeated calls do not always
	// return a key from the same corner of the keyspace, then wrap once so an
	// unlucky start near the end still finds something.
	start := rand.Uint64() % 1024
	for _, cursor := range [2]uint64{start, 0} {
		var found []byte
		_, err := c.Store().Scan(c.Ctx, cursor, 1, func(key []byte, _ store.Record) bool {
			found = key
			return false
		})
		if err != nil {
			return storeErr(c, err)
		}
		if found != nil {
			return c.Bulk(found)
		}
	}
	return c.Null()
}

func cmdDBSize(c *Context) error {
	n, err := c.Store().Len(c.Ctx)
	if err != nil {
		return storeErr(c, err)
	}
	return c.Int(n)
}

func cmdRename(c *Context) error {
	moved, existed, err := renameKey(c, false)
	if err != nil {
		return storeErr(c, err)
	}
	if !existed {
		return c.Err("no such key")
	}
	_ = moved
	return c.OK()
}

func cmdRenameNX(c *Context) error {
	moved, existed, err := renameKey(c, true)
	if err != nil {
		return storeErr(c, err)
	}
	if !existed {
		return c.Err("no such key")
	}
	return c.Bool(moved)
}

// renameKey moves src to dst.
//
// This is a two-key operation built from single-key primitives, so it is not
// atomic: a reader can observe the destination written before the source is
// removed. Making it atomic needs a cross-key ordering point, which arrives
// with the replicated command log in the consensus phase. The window is
// recorded in docs/adr/ADR-005 rather than papered over.
func renameKey(c *Context, nx bool) (moved, existed bool, err error) {
	src, dst := c.Arg(1), c.Arg(2)

	rec, ok, err := c.Store().Get(c.Ctx, src)
	if err != nil {
		return false, false, err
	}
	if !ok {
		return false, false, nil
	}
	if string(src) == string(dst) {
		// Renaming a key onto itself is a no-op that must still report the key
		// existed, and must not delete it.
		return !nx, true, nil
	}

	if nx {
		var wrote bool
		err = c.Store().Update(c.Ctx, dst, func(_ store.Record, found bool) (store.Record, store.Action, error) {
			if found {
				return store.Record{}, store.ActionNone, nil
			}
			wrote = true
			return rec, store.ActionPut, nil
		})
		if err != nil || !wrote {
			return false, true, err
		}
	} else if err := c.Store().Put(c.Ctx, dst, rec); err != nil {
		return false, true, err
	}

	if _, err := c.Store().Delete(c.Ctx, src); err != nil {
		return true, true, err
	}
	return true, true, nil
}

func cmdCopy(c *Context) error {
	src, dst := c.Arg(1), c.Arg(2)
	replace := false
	for i := 3; i < c.Argc(); i++ {
		switch c.ArgLower(i) {
		case "replace":
			replace = true
		case "db":
			if i+1 >= c.Argc() {
				return c.ErrSyntax()
			}
			if c.ArgString(i+1) != "0" {
				return c.Err(msgSingleDB)
			}
			i++
		default:
			return c.ErrSyntax()
		}
	}
	if string(src) == string(dst) {
		return c.Err("source and destination objects are the same")
	}

	rec, ok, err := c.Store().Get(c.Ctx, src)
	if err != nil {
		return storeErr(c, err)
	}
	if !ok {
		return c.Int(0)
	}

	var wrote bool
	err = c.Store().Update(c.Ctx, dst, func(_ store.Record, found bool) (store.Record, store.Action, error) {
		if found && !replace {
			return store.Record{}, store.ActionNone, nil
		}
		wrote = true
		return rec, store.ActionPut, nil
	})
	if err != nil {
		return storeErr(c, err)
	}
	return c.Bool(wrote)
}

func cmdFlush(c *Context) error {
	// ASYNC and SYNC are accepted for compatibility. This server always wipes
	// synchronously, which is honest: pretending a wipe is asynchronous when it
	// is not would make FLUSHALL ASYNC look faster than it is.
	for i := 1; i < c.Argc(); i++ {
		switch c.ArgLower(i) {
		case "async", "sync":
		default:
			return c.ErrSyntax()
		}
	}
	if err := c.Store().FlushAll(c.Ctx); err != nil {
		return storeErr(c, err)
	}
	return c.OK()
}
