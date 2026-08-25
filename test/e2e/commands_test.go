package e2e

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/resp"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/testutil"
)

func TestStringCommands(t *testing.T) {
	s := testutil.Start(t, testutil.Options{})
	c := s.Connect(t)

	t.Run("set and get", func(t *testing.T) {
		if got := c.Str("SET", "k", "v"); got != "OK" {
			t.Fatalf("SET = %q", got)
		}
		if got := c.Str("GET", "k"); got != "v" {
			t.Fatalf("GET = %q", got)
		}
		if rep := c.Do("GET", "missing"); !rep.Null {
			t.Fatalf("GET on a missing key = %s, want a null reply", rep.String())
		}
	})

	t.Run("empty and binary values round trip", func(t *testing.T) {
		c.Str("SET", "empty", "")
		if got := c.Do("GET", "empty"); got.Null || len(got.Str) != 0 {
			t.Fatalf("an empty string came back as %s; it must be distinct from a missing key", got.String())
		}
		binary := string([]byte{0x00, 0xff, '\r', '\n', 0x7f})
		c.Str("SET", "bin", binary)
		if got := c.Str("GET", "bin"); got != binary {
			t.Fatalf("binary value corrupted: %q", got)
		}
	})

	t.Run("append and strlen", func(t *testing.T) {
		c.Str("DEL", "a")
		if n := c.Int("APPEND", "a", "hello"); n != 5 {
			t.Fatalf("APPEND on a missing key = %d, want 5", n)
		}
		if n := c.Int("APPEND", "a", " world"); n != 11 {
			t.Fatalf("APPEND = %d", n)
		}
		if got := c.Str("GET", "a"); got != "hello world" {
			t.Fatalf("GET = %q", got)
		}
		if n := c.Int("STRLEN", "a"); n != 11 {
			t.Fatalf("STRLEN = %d", n)
		}
		if n := c.Int("STRLEN", "no-such-key"); n != 0 {
			t.Fatalf("STRLEN on a missing key = %d, want 0", n)
		}
	})

	t.Run("counters", func(t *testing.T) {
		c.Str("DEL", "n")
		if v := c.Int("INCR", "n"); v != 1 {
			t.Fatalf("INCR on a missing key = %d, want 1", v)
		}
		if v := c.Int("INCRBY", "n", "41"); v != 42 {
			t.Fatalf("INCRBY = %d", v)
		}
		if v := c.Int("DECRBY", "n", "2"); v != 40 {
			t.Fatalf("DECRBY = %d", v)
		}
		if v := c.Int("DECR", "n"); v != 39 {
			t.Fatalf("DECR = %d", v)
		}
		c.Str("SET", "s", "notanumber")
		if msg := c.ErrMsg("INCR", "s"); !strings.Contains(msg, "not an integer") {
			t.Fatalf("INCR on a string = %q", msg)
		}
		// Redis refuses leading zeros: "007" is a string, not seven, and
		// incrementing it would silently rewrite the value.
		c.Str("SET", "z", "007")
		if msg := c.ErrMsg("INCR", "z"); !strings.Contains(msg, "not an integer") {
			t.Fatalf("INCR on \"007\" = %q, want a refusal", msg)
		}
	})

	t.Run("counter overflow is refused", func(t *testing.T) {
		c.Str("SET", "big", "9223372036854775807")
		if msg := c.ErrMsg("INCR", "big"); !strings.Contains(msg, "overflow") {
			t.Fatalf("INCR at int64 max = %q, want an overflow error", msg)
		}
		c.Str("SET", "small", "-9223372036854775808")
		if msg := c.ErrMsg("DECR", "small"); !strings.Contains(msg, "overflow") {
			t.Fatalf("DECR at int64 min = %q", msg)
		}
	})

	t.Run("incrbyfloat", func(t *testing.T) {
		c.Str("DEL", "f")
		if got := c.Str("INCRBYFLOAT", "f", "10.5"); got != "10.5" {
			t.Fatalf("INCRBYFLOAT = %q", got)
		}
		if got := c.Str("INCRBYFLOAT", "f", "0.1"); got != "10.6" {
			t.Fatalf("INCRBYFLOAT = %q, want 10.6", got)
		}
		if got := c.Str("INCRBYFLOAT", "f", "-0.6"); got != "10" {
			t.Fatalf("INCRBYFLOAT = %q, want a trimmed 10", got)
		}
		if msg := c.ErrMsg("INCRBYFLOAT", "f", "nope"); !strings.Contains(msg, "not a valid float") {
			t.Fatalf("%q", msg)
		}
	})

	t.Run("getrange and setrange", func(t *testing.T) {
		c.Str("SET", "r", "This is a string")
		for _, tc := range []struct{ start, end, want string }{
			{"0", "3", "This"},
			{"-3", "-1", "ing"},
			{"0", "-1", "This is a string"},
			{"10", "100", "string"},
			{"-1", "-3", ""},
			{"100", "200", ""},
		} {
			if got := c.Str("GETRANGE", "r", tc.start, tc.end); got != tc.want && !(tc.want == "" && got == "") {
				t.Errorf("GETRANGE %s %s = %q want %q", tc.start, tc.end, got, tc.want)
			}
		}

		c.Str("DEL", "sr")
		if n := c.Int("SETRANGE", "sr", "5", "world"); n != 10 {
			t.Fatalf("SETRANGE = %d, want 10", n)
		}
		// The gap must be zero bytes, not spaces.
		if got := c.Str("GET", "sr"); got != "\x00\x00\x00\x00\x00world" {
			t.Fatalf("SETRANGE padded with %q", got)
		}
		if msg := c.ErrMsg("SETRANGE", "sr", "-1", "x"); !strings.Contains(msg, "out of range") {
			t.Fatalf("%q", msg)
		}
	})

	t.Run("getset getdel", func(t *testing.T) {
		c.Str("SET", "g", "old")
		if got := c.Str("GETSET", "g", "new"); got != "old" {
			t.Fatalf("GETSET = %q", got)
		}
		if got := c.Str("GET", "g"); got != "new" {
			t.Fatalf("GET after GETSET = %q", got)
		}
		if got := c.Str("GETDEL", "g"); got != "new" {
			t.Fatalf("GETDEL = %q", got)
		}
		if n := c.Int("EXISTS", "g"); n != 0 {
			t.Fatalf("GETDEL left the key behind")
		}
		if rep := c.Do("GETDEL", "never-existed"); !rep.Null {
			t.Fatalf("GETDEL on a missing key = %s", rep.String())
		}
	})

	t.Run("mget mset msetnx", func(t *testing.T) {
		c.Str("MSET", "m1", "1", "m2", "2", "m3", "3")
		rep := c.Do("MGET", "m1", "m2", "m3", "missing")
		if len(rep.Elems) != 4 {
			t.Fatalf("MGET returned %d elements", len(rep.Elems))
		}
		if !rep.Elems[3].Null {
			t.Fatalf("MGET did not return a null for the missing key")
		}
		if string(rep.Elems[1].Str) != "2" {
			t.Fatalf("MGET[1] = %q", rep.Elems[1].Str)
		}

		if n := c.Int("MSETNX", "m1", "x", "brand-new", "y"); n != 0 {
			t.Fatalf("MSETNX = %d; it must be all or nothing", n)
		}
		if n := c.Int("EXISTS", "brand-new"); n != 0 {
			t.Fatal("a failed MSETNX wrote one of its keys anyway")
		}
		if n := c.Int("MSETNX", "fresh1", "a", "fresh2", "b"); n != 1 {
			t.Fatalf("MSETNX on fresh keys = %d", n)
		}
		if msg := c.ErrMsg("MSET", "odd"); !strings.Contains(msg, "wrong number") {
			t.Fatalf("%q", msg)
		}
	})
}

func TestSetOptions(t *testing.T) {
	s := testutil.Start(t, testutil.Options{})
	c := s.Connect(t)

	t.Run("nx and xx", func(t *testing.T) {
		c.Str("DEL", "k")
		if rep := c.Do("SET", "k", "v1", "XX"); !rep.Null {
			t.Fatalf("SET XX on a missing key = %s, want a null reply", rep.String())
		}
		if got := c.Str("SET", "k", "v1", "NX"); got != "OK" {
			t.Fatalf("SET NX on a missing key = %q", got)
		}
		if rep := c.Do("SET", "k", "v2", "NX"); !rep.Null {
			t.Fatalf("SET NX on an existing key = %s", rep.String())
		}
		if got := c.Str("GET", "k"); got != "v1" {
			t.Fatalf("a rejected SET NX changed the value: %q", got)
		}
		if got := c.Str("SET", "k", "v2", "XX"); got != "OK" {
			t.Fatalf("SET XX on an existing key = %q", got)
		}
		if msg := c.ErrMsg("SET", "k", "v", "NX", "XX"); !strings.Contains(msg, "syntax") {
			t.Fatalf("NX and XX together = %q", msg)
		}
	})

	t.Run("get option returns the old value", func(t *testing.T) {
		c.Str("SET", "g", "before")
		if got := c.Str("SET", "g", "after", "GET"); got != "before" {
			t.Fatalf("SET GET = %q", got)
		}
		c.Str("DEL", "g")
		if rep := c.Do("SET", "g", "x", "GET"); !rep.Null {
			t.Fatalf("SET GET on a missing key = %s", rep.String())
		}
	})

	t.Run("expiry options", func(t *testing.T) {
		c.Str("SET", "e", "v", "EX", "100")
		if ttl := c.Int("TTL", "e"); ttl != 100 {
			t.Fatalf("TTL = %d", ttl)
		}
		c.Str("SET", "e", "v2", "KEEPTTL")
		if ttl := c.Int("TTL", "e"); ttl != 100 {
			t.Fatalf("KEEPTTL dropped the expiry: TTL = %d", ttl)
		}
		// A plain SET clears the TTL, which is the behaviour KEEPTTL exists to
		// opt out of.
		c.Str("SET", "e", "v3")
		if ttl := c.Int("TTL", "e"); ttl != -1 {
			t.Fatalf("a plain SET left a TTL of %d", ttl)
		}
		for _, bad := range [][]string{
			{"SET", "e", "v", "EX", "0"},
			{"SET", "e", "v", "EX", "-1"},
			{"SET", "e", "v", "PX", "0"},
		} {
			if msg := c.ErrMsg(bad...); !strings.Contains(msg, "invalid expire") {
				t.Errorf("%v = %q, want an invalid-expire error", bad, msg)
			}
		}
		if msg := c.ErrMsg("SET", "e", "v", "EX", "10", "PX", "10"); !strings.Contains(msg, "syntax") {
			t.Fatalf("two expiry options = %q", msg)
		}
	})
}

func TestExpiry(t *testing.T) {
	s := testutil.Start(t, testutil.Options{})
	c := s.Connect(t)

	t.Run("ttl sentinels", func(t *testing.T) {
		if ttl := c.Int("TTL", "no-such-key"); ttl != -2 {
			t.Fatalf("TTL on a missing key = %d, want -2", ttl)
		}
		c.Str("SET", "forever", "v")
		if ttl := c.Int("TTL", "forever"); ttl != -1 {
			t.Fatalf("TTL on a key with no expiry = %d, want -1", ttl)
		}
	})

	t.Run("expire and persist", func(t *testing.T) {
		c.Str("SET", "k", "v")
		if n := c.Int("EXPIRE", "k", "100"); n != 1 {
			t.Fatalf("EXPIRE = %d", n)
		}
		if ttl := c.Int("TTL", "k"); ttl != 100 {
			t.Fatalf("TTL = %d", ttl)
		}
		if n := c.Int("PERSIST", "k"); n != 1 {
			t.Fatalf("PERSIST = %d", n)
		}
		if ttl := c.Int("TTL", "k"); ttl != -1 {
			t.Fatalf("TTL after PERSIST = %d", ttl)
		}
		if n := c.Int("PERSIST", "k"); n != 0 {
			t.Fatalf("PERSIST on a key with no TTL = %d, want 0", n)
		}
		if n := c.Int("EXPIRE", "missing", "10"); n != 0 {
			t.Fatalf("EXPIRE on a missing key = %d", n)
		}
	})

	t.Run("key disappears when its ttl elapses", func(t *testing.T) {
		c.Str("SET", "fleeting", "v", "PX", "500")
		if n := c.Int("EXISTS", "fleeting"); n != 1 {
			t.Fatal("the key was not there to begin with")
		}
		// A mock clock rather than a sleep: the test is about the expiry rule,
		// not about how fast the machine running it happens to be.
		s.Clock.Advance(400 * time.Millisecond)
		if n := c.Int("EXISTS", "fleeting"); n != 1 {
			t.Fatal("the key expired early")
		}
		s.Clock.Advance(200 * time.Millisecond)
		if n := c.Int("EXISTS", "fleeting"); n != 0 {
			t.Fatal("the key outlived its TTL")
		}
		if rep := c.Do("GET", "fleeting"); !rep.Null {
			t.Fatalf("GET on an expired key = %s", rep.String())
		}
		if ttl := c.Int("TTL", "fleeting"); ttl != -2 {
			t.Fatalf("TTL on an expired key = %d, want -2", ttl)
		}
	})

	t.Run("expire options", func(t *testing.T) {
		c.Str("SET", "o", "v", "EX", "100")
		if n := c.Int("EXPIRE", "o", "50", "NX"); n != 0 {
			t.Fatal("EXPIRE NX applied to a key that already had a TTL")
		}
		if n := c.Int("EXPIRE", "o", "50", "XX"); n != 1 {
			t.Fatal("EXPIRE XX did not apply to a key that had a TTL")
		}
		if n := c.Int("EXPIRE", "o", "10", "GT"); n != 0 {
			t.Fatal("EXPIRE GT shortened a TTL")
		}
		if n := c.Int("EXPIRE", "o", "500", "GT"); n != 1 {
			t.Fatal("EXPIRE GT did not extend a TTL")
		}
		if n := c.Int("EXPIRE", "o", "10", "LT"); n != 1 {
			t.Fatal("EXPIRE LT did not shorten a TTL")
		}
		// A key with no TTL is infinitely far away, so GT can never shorten it.
		c.Str("SET", "p", "v")
		if n := c.Int("EXPIRE", "p", "100", "GT"); n != 0 {
			t.Fatal("EXPIRE GT applied to a key with no TTL")
		}
		if msg := c.ErrMsg("EXPIRE", "o", "10", "NX", "XX"); !strings.Contains(msg, "not compatible") {
			t.Fatalf("%q", msg)
		}
	})

	t.Run("expire in the past deletes immediately", func(t *testing.T) {
		c.Str("SET", "gone", "v")
		if n := c.Int("EXPIREAT", "gone", "1"); n != 1 {
			t.Fatalf("EXPIREAT = %d", n)
		}
		if n := c.Int("EXISTS", "gone"); n != 0 {
			t.Fatal("a key with an expiry in the past is still present")
		}
	})

	t.Run("getex changes expiry without changing the value", func(t *testing.T) {
		c.Str("SET", "gx", "v", "EX", "100")
		if got := c.Str("GETEX", "gx", "PERSIST"); got != "v" {
			t.Fatalf("GETEX = %q", got)
		}
		if ttl := c.Int("TTL", "gx"); ttl != -1 {
			t.Fatalf("GETEX PERSIST left a TTL of %d", ttl)
		}
		if got := c.Str("GETEX", "gx", "EX", "200"); got != "v" {
			t.Fatalf("GETEX = %q", got)
		}
		if ttl := c.Int("TTL", "gx"); ttl != 200 {
			t.Fatalf("TTL = %d", ttl)
		}
	})
}

func TestKeyspaceCommands(t *testing.T) {
	s := testutil.Start(t, testutil.Options{})
	c := s.Connect(t)

	t.Run("del exists type", func(t *testing.T) {
		c.Str("MSET", "d1", "1", "d2", "2", "d3", "3")
		if n := c.Int("DEL", "d1", "d2", "nope"); n != 2 {
			t.Fatalf("DEL = %d, want 2", n)
		}
		// EXISTS counts repeats, which Redis does deliberately.
		if n := c.Int("EXISTS", "d3", "d3", "nope"); n != 2 {
			t.Fatalf("EXISTS with a repeated key = %d, want 2", n)
		}
		if got := c.Str("TYPE", "d3"); got != "string" {
			t.Fatalf("TYPE = %q", got)
		}
		if got := c.Str("TYPE", "nope"); got != "none" {
			t.Fatalf("TYPE on a missing key = %q", got)
		}
	})

	t.Run("rename", func(t *testing.T) {
		c.Str("SET", "old", "v")
		c.Str("DEL", "new")
		c.Str("RENAME", "old", "new")
		if n := c.Int("EXISTS", "old"); n != 0 {
			t.Fatal("RENAME left the source behind")
		}
		if got := c.Str("GET", "new"); got != "v" {
			t.Fatalf("GET after RENAME = %q", got)
		}
		if msg := c.ErrMsg("RENAME", "still-missing", "x"); !strings.Contains(msg, "no such key") {
			t.Fatalf("%q", msg)
		}
		// Renaming a key onto itself must not delete it.
		c.Str("RENAME", "new", "new")
		if got := c.Str("GET", "new"); got != "v" {
			t.Fatalf("renaming a key onto itself destroyed it")
		}

		c.Str("SET", "src", "s")
		c.Str("SET", "dst", "d")
		if n := c.Int("RENAMENX", "src", "dst"); n != 0 {
			t.Fatalf("RENAMENX onto an existing key = %d", n)
		}
		if got := c.Str("GET", "dst"); got != "d" {
			t.Fatal("a refused RENAMENX overwrote the destination")
		}
	})

	t.Run("copy", func(t *testing.T) {
		c.Str("SET", "cs", "value", "EX", "100")
		c.Str("DEL", "cd")
		if n := c.Int("COPY", "cs", "cd"); n != 1 {
			t.Fatalf("COPY = %d", n)
		}
		if got := c.Str("GET", "cd"); got != "value" {
			t.Fatalf("COPY produced %q", got)
		}
		if ttl := c.Int("TTL", "cd"); ttl != 100 {
			t.Fatalf("COPY did not carry the TTL: %d", ttl)
		}
		if n := c.Int("COPY", "cs", "cd"); n != 0 {
			t.Fatal("COPY overwrote an existing destination without REPLACE")
		}
		if n := c.Int("COPY", "cs", "cd", "REPLACE"); n != 1 {
			t.Fatal("COPY REPLACE did not overwrite")
		}
	})

	t.Run("keys and scan agree", func(t *testing.T) {
		c.Str("FLUSHALL")
		want := map[string]bool{}
		for i := 0; i < 500; i++ {
			k := fmt.Sprintf("scan:%03d", i)
			c.Str("SET", k, "v")
			want[k] = true
		}
		for i := 0; i < 50; i++ {
			c.Str("SET", fmt.Sprintf("other:%d", i), "v")
		}

		keys := c.Do("KEYS", "scan:*")
		if len(keys.Elems) != len(want) {
			t.Fatalf("KEYS returned %d keys, want %d", len(keys.Elems), len(want))
		}

		seen := map[string]bool{}
		cursor := "0"
		iterations := 0
		for {
			iterations++
			if iterations > 5000 {
				t.Fatal("SCAN did not terminate")
			}
			rep := c.Do("SCAN", cursor, "MATCH", "scan:*", "COUNT", "20")
			cursor = string(rep.Elems[0].Str)
			for _, e := range rep.Elems[1].Elems {
				seen[string(e.Str)] = true
			}
			if cursor == "0" {
				break
			}
		}
		if len(seen) != len(want) {
			t.Fatalf("SCAN found %d of %d keys", len(seen), len(want))
		}
		for k := range want {
			if !seen[k] {
				t.Fatalf("SCAN missed %s", k)
			}
		}
	})

	t.Run("dbsize and flushall", func(t *testing.T) {
		c.Str("FLUSHALL")
		if n := c.Int("DBSIZE"); n != 0 {
			t.Fatalf("DBSIZE after FLUSHALL = %d", n)
		}
		for i := 0; i < 10; i++ {
			c.Str("SET", fmt.Sprintf("f%d", i), "v")
		}
		if n := c.Int("DBSIZE"); n != 10 {
			t.Fatalf("DBSIZE = %d", n)
		}
		c.Str("FLUSHALL")
		if n := c.Int("DBSIZE"); n != 0 {
			t.Fatalf("DBSIZE after FLUSHALL = %d", n)
		}
	})

	t.Run("randomkey", func(t *testing.T) {
		c.Str("FLUSHALL")
		if rep := c.Do("RANDOMKEY"); !rep.Null {
			t.Fatalf("RANDOMKEY on an empty database = %s", rep.String())
		}
		c.Str("SET", "only", "v")
		if got := c.Str("RANDOMKEY"); got != "only" {
			t.Fatalf("RANDOMKEY = %q", got)
		}
	})
}

func TestGlobPatterns(t *testing.T) {
	s := testutil.Start(t, testutil.Options{})
	c := s.Connect(t)
	c.Str("FLUSHALL")
	for _, k := range []string{"hello", "hallo", "hxllo", "heeello", "hillo", "user:1:name", "user:2:name"} {
		c.Str("SET", k, "v")
	}
	for _, tc := range []struct {
		pattern string
		want    int
	}{
		{"h?llo", 4},
		{"h*llo", 5},
		{"h[ae]llo", 2},
		{"h[^e]llo", 3},
		{"h[a-b]llo", 1},
		{"user:*:name", 2},
		{"*", 7},
		{"nomatch*", 0},
	} {
		rep := c.Do("KEYS", tc.pattern)
		if len(rep.Elems) != tc.want {
			t.Errorf("KEYS %q matched %d keys, want %d", tc.pattern, len(rep.Elems), tc.want)
		}
	}
}

func TestPipelining(t *testing.T) {
	s := testutil.Start(t, testutil.Options{})
	c := s.Connect(t)

	const n = 2000
	cmds := make([][]string, 0, n*2)
	for i := 0; i < n; i++ {
		cmds = append(cmds, []string{"SET", fmt.Sprintf("p%d", i), fmt.Sprintf("v%d", i)})
	}
	for i := 0; i < n; i++ {
		cmds = append(cmds, []string{"GET", fmt.Sprintf("p%d", i)})
	}
	replies := c.Pipeline(cmds...)

	if len(replies) != len(cmds) {
		t.Fatalf("got %d replies for %d commands", len(replies), len(cmds))
	}
	// The reply order must match the command order exactly. A server that
	// skipped or reordered one reply would leave every later reply attributed
	// to the wrong command, which is the failure pipelining makes silent.
	for i := 0; i < n; i++ {
		if replies[i].String() != "OK" {
			t.Fatalf("reply %d = %s, want OK", i, replies[i].String())
		}
	}
	for i := 0; i < n; i++ {
		want := fmt.Sprintf("v%d", i)
		if got := replies[n+i].String(); got != want {
			t.Fatalf("reply %d = %q, want %q", n+i, got, want)
		}
	}
}

func TestPipelineWithAnErrorInTheMiddle(t *testing.T) {
	s := testutil.Start(t, testutil.Options{})
	c := s.Connect(t)
	c.Str("SET", "str", "notanumber")

	replies := c.Pipeline(
		[]string{"SET", "a", "1"},
		[]string{"INCR", "str"},
		[]string{"NOSUCHCOMMAND"},
		[]string{"GET", "a"},
	)
	if len(replies) != 4 {
		t.Fatalf("got %d replies, want 4", len(replies))
	}
	// Every command gets exactly one reply, errors included. Skipping the
	// reply for a failed command would desynchronise everything after it.
	if replies[0].String() != "OK" {
		t.Errorf("reply 0 = %s", replies[0].String())
	}
	if !replies[1].IsError() {
		t.Errorf("reply 1 should be an error")
	}
	if !replies[2].IsError() {
		t.Errorf("reply 2 should be an error")
	}
	if replies[3].String() != "1" {
		t.Errorf("reply 3 = %s, want the connection to still be usable", replies[3].String())
	}
}

func TestRESP3(t *testing.T) {
	s := testutil.Start(t, testutil.Options{})
	c := s.Connect(t)

	rep := c.Do("HELLO", "3")
	if rep.IsError() {
		t.Fatalf("HELLO 3 = %s", rep.Str)
	}
	if rep.Kind != resp.TypeMap {
		t.Fatalf("HELLO 3 replied with type %c, want a native RESP3 map", rep.Kind)
	}
	fields := map[string]string{}
	for i := 0; i+1 < len(rep.Elems); i += 2 {
		fields[string(rep.Elems[i].Str)] = rep.Elems[i+1].String()
	}
	if fields["proto"] != "3" {
		t.Fatalf("HELLO reported proto=%q", fields["proto"])
	}

	// Once upgraded, a missing key is the RESP3 null type rather than a null
	// bulk string.
	c.Str("DEL", "missing")
	if rep := c.Do("GET", "missing"); rep.Kind != resp.TypeNull {
		t.Fatalf("RESP3 null = type %c, want '_'", rep.Kind)
	}
	if rep := c.Do("CONFIG", "GET", "maxclients"); rep.Kind != resp.TypeMap {
		t.Fatalf("CONFIG GET on RESP3 = type %c, want a map", rep.Kind)
	}

	if msg := c.ErrMsg("HELLO", "4"); !strings.Contains(msg, "NOPROTO") {
		t.Fatalf("HELLO 4 = %q", msg)
	}
	// The connection stays on RESP3 after a rejected upgrade.
	if rep := c.Do("GET", "missing"); rep.Kind != resp.TypeNull {
		t.Fatal("a rejected HELLO changed the protocol version")
	}
}

func TestConnectionCommands(t *testing.T) {
	s := testutil.Start(t, testutil.Options{})
	c := s.Connect(t)

	if got := c.Str("PING"); got != "PONG" {
		t.Fatalf("PING = %q", got)
	}
	if got := c.Str("PING", "hello"); got != "hello" {
		t.Fatalf("PING with a payload = %q", got)
	}
	if got := c.Str("ECHO", "round trip"); got != "round trip" {
		t.Fatalf("ECHO = %q", got)
	}
	if got := c.Str("SELECT", "0"); got != "OK" {
		t.Fatalf("SELECT 0 = %q", got)
	}
	if msg := c.ErrMsg("SELECT", "1"); !strings.Contains(msg, "single database") {
		t.Fatalf("SELECT 1 = %q", msg)
	}

	// CLIENT SETNAME is per connection, so it must be visible on the same one.
	c.Str("CLIENT", "SETNAME", "tester")
	if got := c.Str("CLIENT", "GETNAME"); got != "tester" {
		t.Fatalf("CLIENT GETNAME = %q", got)
	}
	if !strings.Contains(c.Str("CLIENT", "INFO"), "name=tester") {
		t.Fatal("CLIENT INFO does not show the name")
	}
	if msg := c.ErrMsg("CLIENT", "SETNAME", "has space"); !strings.Contains(msg, "spaces") {
		t.Fatalf("%q", msg)
	}
}

func TestQuitFlushesItsReply(t *testing.T) {
	s := testutil.Start(t, testutil.Options{})
	c := s.Connect(t)
	// The client must actually receive +OK before the socket goes away. A
	// server that closed first would leave the client seeing a reset.
	if got := c.Str("QUIT"); got != "OK" {
		t.Fatalf("QUIT = %q", got)
	}
	c.ReadClosed()
}

func TestServerCommands(t *testing.T) {
	s := testutil.Start(t, testutil.Options{})
	c := s.Connect(t)

	info := c.Str("INFO")
	for _, section := range []string{"# Server", "# Clients", "# Memory", "# Stats", "# Replication"} {
		if !strings.Contains(info, section) {
			t.Errorf("INFO is missing the %s section", section)
		}
	}
	if !strings.Contains(c.Str("INFO", "stats"), "total_commands_processed") {
		t.Error("INFO stats is missing its counters")
	}
	if strings.Contains(c.Str("INFO", "stats"), "# Server") {
		t.Error("INFO stats returned sections that were not asked for")
	}

	if n := c.Int("COMMAND", "COUNT"); n < 50 {
		t.Errorf("COMMAND COUNT = %d, which is fewer commands than are registered", n)
	}
	rep := c.Do("COMMAND", "INFO", "get")
	if len(rep.Elems) != 1 || string(rep.Elems[0].Elems[0].Str) != "get" {
		t.Errorf("COMMAND INFO get = %s", rep.String())
	}
	if rep := c.Do("COMMAND", "GETKEYS", "SET", "mykey", "myvalue"); len(rep.Elems) != 1 ||
		string(rep.Elems[0].Str) != "mykey" {
		t.Errorf("COMMAND GETKEYS = %s", rep.String())
	}
	if rep := c.Do("COMMAND", "GETKEYS", "MSET", "k1", "v1", "k2", "v2"); len(rep.Elems) != 2 {
		t.Errorf("COMMAND GETKEYS on MSET = %s", rep.String())
	}

	if got := c.Str("CONFIG", "SET", "maxclients", "5000"); got != "OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	rep = c.Do("CONFIG", "GET", "maxclients")
	if len(rep.Elems) != 2 || string(rep.Elems[1].Str) != "5000" {
		t.Fatalf("CONFIG GET after SET = %s", rep.String())
	}
	if msg := c.ErrMsg("CONFIG", "SET", "not-a-real-option", "x"); !strings.Contains(msg, "Unknown option") {
		t.Fatalf("%q", msg)
	}
	// A parameter that exists but cannot change at runtime must be refused
	// rather than accepted and ignored.
	if msg := c.ErrMsg("CONFIG", "SET", "dir", "/tmp"); !strings.Contains(msg, "not modifiable") {
		t.Fatalf("CONFIG SET dir = %q", msg)
	}

	if rep := c.Do("TIME"); len(rep.Elems) != 2 {
		t.Errorf("TIME = %s", rep.String())
	}
	c.Str("SET", "obj", "12345")
	if got := c.Str("OBJECT", "ENCODING", "obj"); got != "int" {
		t.Errorf("OBJECT ENCODING on a number = %q", got)
	}
	c.Str("SET", "obj", "not a number")
	if got := c.Str("OBJECT", "ENCODING", "obj"); got != "raw" {
		t.Errorf("OBJECT ENCODING on a string = %q", got)
	}
	// WAIT must report the truth: there are no replicas.
	if n := c.Int("WAIT", "1", "0"); n != 0 {
		t.Errorf("WAIT = %d; a standalone server has no replicas to acknowledge", n)
	}
}

func TestErrorHandling(t *testing.T) {
	s := testutil.Start(t, testutil.Options{})
	c := s.Connect(t)

	for _, tc := range []struct {
		args     []string
		contains string
	}{
		{[]string{"GET"}, "wrong number of arguments"},
		{[]string{"GET", "a", "b"}, "wrong number of arguments"},
		{[]string{"SET", "k"}, "wrong number of arguments"},
		{[]string{"NOPE"}, "unknown command"},
		{[]string{"EXPIRE", "k", "abc"}, "not an integer"},
		{[]string{"SCAN", "notanumber"}, "invalid cursor"},
		{[]string{"SET", "k", "v", "BOGUS"}, "syntax error"},
	} {
		if msg := c.ErrMsg(tc.args...); !strings.Contains(msg, tc.contains) {
			t.Errorf("%v = %q, want it to mention %q", tc.args, msg, tc.contains)
		}
	}
	// The connection survives every one of those.
	if got := c.Str("PING"); got != "PONG" {
		t.Fatal("the connection did not survive a run of error replies")
	}
}

func TestProtocolErrorClosesTheConnection(t *testing.T) {
	s := testutil.Start(t, testutil.Options{})

	t.Run("bad multibulk header", func(t *testing.T) {
		c := s.Connect(t)
		c.SendRaw([]byte("*abc\r\n"))
		rep := c.Read()
		if !rep.IsError() || !strings.Contains(string(rep.Str), "Protocol error") {
			t.Fatalf("got %s, want a protocol error", rep.String())
		}
		// Framing is lost, so there is no way to resynchronise. The server must
		// hang up rather than misinterpret every following byte.
		c.ReadClosed()
	})

	t.Run("element that is not a bulk string", func(t *testing.T) {
		c := s.Connect(t)
		c.SendRaw([]byte("*1\r\n+OK\r\n"))
		if rep := c.Read(); !rep.IsError() {
			t.Fatalf("got %s", rep.String())
		}
		c.ReadClosed()
	})

	t.Run("oversized bulk", func(t *testing.T) {
		c := s.Connect(t)
		c.SendRaw([]byte("*1\r\n$999999999999\r\n"))
		if rep := c.Read(); !rep.IsError() {
			t.Fatalf("got %s", rep.String())
		}
		c.ReadClosed()
	})
}

func TestInlineCommands(t *testing.T) {
	s := testutil.Start(t, testutil.Options{})
	c := s.Connect(t)
	// The legacy inline protocol: what telnet and simple health probes speak.
	c.SendRaw([]byte("PING\r\n"))
	if got := c.Read().String(); got != "PONG" {
		t.Fatalf("inline PING = %q", got)
	}
	c.SendRaw([]byte("SET  inline\tvalue\r\n"))
	if got := c.Read().String(); got != "OK" {
		t.Fatalf("inline SET = %q", got)
	}
	if got := c.Str("GET", "inline"); got != "value" {
		t.Fatalf("GET = %q", got)
	}
	// Blank lines are skipped, not treated as empty commands.
	c.SendRaw([]byte("\r\n\r\nPING\r\n"))
	if got := c.Read().String(); got != "PONG" {
		t.Fatalf("after blank lines = %q", got)
	}
}

func TestAuth(t *testing.T) {
	s := testutil.Start(t, testutil.Options{RequirePass: "hunter2"})
	c := s.Connect(t)

	if msg := c.ErrMsg("GET", "k"); !strings.Contains(msg, "NOAUTH") {
		t.Fatalf("an unauthenticated command = %q", msg)
	}
	// PING is not exempt: it reveals the server is alive, which is fine, but
	// the keyspace must stay closed until AUTH succeeds.
	if msg := c.ErrMsg("PING"); !strings.Contains(msg, "NOAUTH") {
		t.Fatalf("unauthenticated PING = %q", msg)
	}
	if msg := c.ErrMsg("AUTH", "wrong"); !strings.Contains(msg, "WRONGPASS") {
		t.Fatalf("a wrong password = %q", msg)
	}
	if got := c.Str("AUTH", "hunter2"); got != "OK" {
		t.Fatalf("AUTH = %q", got)
	}
	if got := c.Str("SET", "k", "v"); got != "OK" {
		t.Fatalf("after AUTH = %q", got)
	}

	// HELLO can carry the credentials so a client completes its handshake in
	// one round trip.
	c2 := s.Connect(t)
	if rep := c2.Do("HELLO", "3", "AUTH", "default", "hunter2"); rep.IsError() {
		t.Fatalf("HELLO AUTH = %s", rep.Str)
	}
	if got := c2.Str("GET", "k"); got != "v" {
		t.Fatalf("after HELLO AUTH = %q", got)
	}
}

func TestMaxClientsIsEnforced(t *testing.T) {
	s := testutil.Start(t, testutil.Options{MaxClients: 3})

	held := make([]*testutil.Client, 0, 3)
	for i := 0; i < 3; i++ {
		c := s.Connect(t)
		if got := c.Str("PING"); got != "PONG" {
			t.Fatalf("connection %d = %q", i, got)
		}
		held = append(held, c)
	}
	_ = held

	// The fourth is refused with a reason rather than an unexplained reset, so
	// the client can tell a full server from a crashed one.
	over := s.Connect(t)
	rep := over.Read()
	if !rep.IsError() || !strings.Contains(string(rep.Str), "max number of clients") {
		t.Fatalf("the connection over the limit got %s", rep.String())
	}
}

func TestConcurrentClients(t *testing.T) {
	s := testutil.Start(t, testutil.Options{})

	const clients, each = 24, 400
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := s.Connect(t)
			for j := 0; j < each; j++ {
				key := fmt.Sprintf("c%d:%d", id, j)
				if got := c.Str("SET", key, key); got != "OK" {
					errs <- testutil.Errorf("client %d SET = %q", id, got)
					return
				}
				if got := c.Str("GET", key); got != key {
					errs <- testutil.Errorf("client %d read back %q, want %q", id, got, key)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	c := s.Connect(t)
	if n := c.Int("DBSIZE"); n != clients*each {
		t.Fatalf("DBSIZE = %d, want %d", n, clients*each)
	}
}

func TestConcurrentIncrementsDoNotLoseUpdates(t *testing.T) {
	s := testutil.Start(t, testutil.Options{})

	const clients, each = 16, 500
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := s.Connect(t)
			for j := 0; j < each; j++ {
				c.Int("INCR", "shared-counter")
			}
		}()
	}
	wg.Wait()

	c := s.Connect(t)
	// GET replies with a bulk string, not an integer, even when the value
	// happens to be numeric.
	got := c.Str("GET", "shared-counter")
	want := fmt.Sprint(clients * each)
	if got != want {
		t.Fatalf("counter = %s, want %s: increments were lost", got, want)
	}
}

func TestClientKill(t *testing.T) {
	s := testutil.Start(t, testutil.Options{})
	victim := s.Connect(t)
	victim.Str("CLIENT", "SETNAME", "victim")
	victimID := victim.Int("CLIENT", "ID")

	killer := s.Connect(t)
	if n := killer.Int("CLIENT", "KILL", "ID", fmt.Sprint(victimID)); n != 1 {
		t.Fatalf("CLIENT KILL = %d", n)
	}
	victim.ReadClosed()

	// The killer must not have killed itself: skipme defaults to yes.
	if got := killer.Str("PING"); got != "PONG" {
		t.Fatal("CLIENT KILL closed the connection that issued it")
	}
}
