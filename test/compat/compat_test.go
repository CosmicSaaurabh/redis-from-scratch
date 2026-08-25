// Package compat runs the same commands against this server and against a real
// redis-server and compares the replies byte for byte.
//
// Hand-written expectations encode what the author *believes* Redis does. This
// suite encodes what it actually does, which is the only version that matters
// when an existing client library is on the other end. Where the two servers
// legitimately differ - version strings, a single database, an LSM engine's
// estimated DBSIZE - the difference is stated here rather than hidden by a
// loose assertion.
package compat

import (
	"fmt"
	"math"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/resp"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/testutil"
)

// redisServer locates a real redis-server, or reports that there is none.
func redisServer(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server is not installed; skipping the differential comparison")
	}
	return path
}

// startRedis runs a real redis-server with persistence off and returns its
// address.
func startRedis(t *testing.T) string {
	t.Helper()
	bin := redisServer(t)
	port := testutil.FreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	cmd := exec.Command(bin,
		"--port", fmt.Sprint(port),
		"--bind", "127.0.0.1",
		"--save", "",
		"--appendonly", "no",
		"--daemonize", "no",
		"--dir", t.TempDir(),
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start redis-server: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	// Poll rather than sleep: redis binds in well under a second locally but
	// not reliably so on a loaded CI machine.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if out, err := exec.Command("redis-cli", "-p", fmt.Sprint(port), "PING").Output(); err == nil &&
			strings.TrimSpace(string(out)) == "PONG" {
			return addr
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("redis-server on %s did not become ready", addr)
	return ""
}

// pair holds one client against each server.
type pair struct {
	t     *testing.T
	ours  *testutil.Client
	redis *rawClient
}

func newPair(t *testing.T) *pair {
	t.Helper()
	redisAddr := startRedis(t)
	s := testutil.Start(t, testutil.Options{})
	p := &pair{t: t, ours: s.Connect(t), redis: dialRaw(t, redisAddr)}
	// Both start from an empty keyspace so that a leftover key cannot make a
	// difference look like an incompatibility.
	p.ours.Do("FLUSHALL")
	p.redis.do("FLUSHALL")
	return p
}

// same asserts both servers reply identically.
func (p *pair) same(args ...string) {
	p.t.Helper()
	got := render(p.ours.Do(args...))
	want := render(p.redis.do(args...))
	if got != want {
		p.t.Errorf("%s\n  ours:  %s\n  redis: %s", strings.Join(args, " "), got, want)
	}
}

// sameErrorCode asserts both reply with an error carrying the same code, while
// allowing the human sentence after it to differ.
func (p *pair) sameErrorCode(args ...string) {
	p.t.Helper()
	ours := p.ours.Do(args...)
	theirs := p.redis.do(args...)
	if !ours.IsError() || !theirs.IsError() {
		p.t.Errorf("%s: expected both to error\n  ours:  %s\n  redis: %s",
			strings.Join(args, " "), render(ours), render(theirs))
		return
	}
	if code(ours) != code(theirs) {
		p.t.Errorf("%s: error codes differ\n  ours:  %s\n  redis: %s",
			strings.Join(args, " "), ours.Str, theirs.Str)
	}
}

// sameFloat asserts both replies parse to the same number, within the slack
// that different float widths make unavoidable.
func (p *pair) sameFloat(args ...string) {
	p.t.Helper()
	ours := p.ours.Do(args...)
	theirs := p.redis.do(args...)
	if ours.IsError() || theirs.IsError() {
		p.t.Errorf("%s\n  ours:  %s\n  redis: %s", strings.Join(args, " "), render(ours), render(theirs))
		return
	}
	a, err1 := strconv.ParseFloat(string(ours.Str), 64)
	b, err2 := strconv.ParseFloat(string(theirs.Str), 64)
	if err1 != nil || err2 != nil {
		p.t.Errorf("%s: not numeric\n  ours:  %q\n  redis: %q", strings.Join(args, " "), ours.Str, theirs.Str)
		return
	}
	if math.Abs(a-b) > 1e-9 {
		p.t.Errorf("%s: %v vs redis %v", strings.Join(args, " "), a, b)
	}
}

func code(r resp.Reply) string {
	s := string(r.Str)
	if i := strings.IndexByte(s, ' '); i > 0 {
		return s[:i]
	}
	return s
}

// render turns a reply into a comparable string that keeps the distinctions
// that matter: type, nullness and exact bytes.
func render(r resp.Reply) string {
	if r.IsError() {
		return "ERROR(" + string(r.Str) + ")"
	}
	if r.Null {
		return "NULL"
	}
	switch r.Kind {
	case resp.TypeInteger:
		return fmt.Sprintf("INT(%d)", r.Int)
	case resp.TypeSimpleString:
		return fmt.Sprintf("STATUS(%s)", r.Str)
	case resp.TypeBulkString:
		return fmt.Sprintf("BULK(%q)", r.Str)
	case resp.TypeArray, resp.TypeSet, resp.TypeMap, resp.TypePush:
		parts := make([]string, 0, len(r.Elems))
		for _, e := range r.Elems {
			parts = append(parts, render(e))
		}
		return "ARRAY[" + strings.Join(parts, ",") + "]"
	default:
		return fmt.Sprintf("TYPE%c(%s)", r.Kind, r.String())
	}
}

func TestStringCommandsMatchRedis(t *testing.T) {
	p := newPair(t)

	p.same("SET", "k", "v")
	p.same("GET", "k")
	p.same("GET", "missing")
	p.same("SET", "k", "")
	p.same("GET", "k")
	p.same("STRLEN", "k")
	p.same("STRLEN", "missing")
	p.same("APPEND", "k", "abc")
	p.same("APPEND", "k", "def")
	p.same("GET", "k")
	p.same("GETSET", "k", "replaced")
	p.same("GETSET", "brand-new", "x")
	p.same("GETDEL", "brand-new")
	p.same("GETDEL", "brand-new")
	p.same("SETNX", "nx", "1")
	p.same("SETNX", "nx", "2")
	p.same("GET", "nx")
	p.same("MSET", "a", "1", "b", "2", "c", "3")
	p.same("MGET", "a", "b", "c", "missing")
	p.same("MSETNX", "a", "x", "d", "y")
	p.same("EXISTS", "d")
	p.same("MSETNX", "e", "1", "f", "2")
}

func TestCounterCommandsMatchRedis(t *testing.T) {
	p := newPair(t)

	p.same("INCR", "n")
	p.same("INCRBY", "n", "41")
	p.same("GET", "n")
	p.same("DECR", "n")
	p.same("DECRBY", "n", "10")
	p.same("SET", "s", "notanumber")
	p.sameErrorCode("INCR", "s")
	// Leading zeros are a string, not a number, and both servers must refuse.
	p.same("SET", "z", "007")
	p.sameErrorCode("INCR", "z")
	// Both ends of the int64 range.
	p.same("SET", "max", "9223372036854775807")
	p.sameErrorCode("INCR", "max")
	p.same("SET", "min", "-9223372036854775808")
	p.sameErrorCode("DECR", "min")
	p.same("INCR", "min")
	p.same("GET", "min")

	// INCRBYFLOAT is compared numerically rather than byte for byte.
	//
	// Redis accumulates in C `long double` and prints with %.17Lf, so its exact
	// output depends on what long double means on the platform: 80 bits on
	// x86-64 Linux, where 10.5 + 0.1 prints as "10.6", and 64 bits on arm64
	// macOS, where the same expression prints as "10.59999999999999964". Go has
	// no 80-bit float, so byte-exact agreement is not achievable on every
	// platform at once. This server uses float64 with the shortest
	// representation that round-trips, which matches Redis on Linux and is
	// documented in docs/adr/ADR-006.
	p.sameFloat("INCRBYFLOAT", "f", "10.5")
	p.sameFloat("INCRBYFLOAT", "f", "0.1")
	p.sameFloat("INCRBYFLOAT", "f", "-0.6")
	p.sameFloat("GET", "f")
	p.sameErrorCode("INCRBYFLOAT", "f", "nope")
}

func TestRangeCommandsMatchRedis(t *testing.T) {
	p := newPair(t)
	p.same("SET", "r", "This is a string")
	for _, se := range [][2]string{
		{"0", "3"}, {"-3", "-1"}, {"0", "-1"}, {"10", "100"},
		{"-1", "-3"}, {"100", "200"}, {"-100", "-90"}, {"5", "5"}, {"0", "0"},
	} {
		p.same("GETRANGE", "r", se[0], se[1])
	}
	p.same("GETRANGE", "missing", "0", "-1")

	p.same("SETRANGE", "sr", "5", "world")
	p.same("GET", "sr")
	p.same("SETRANGE", "sr", "0", "HELLO")
	p.same("GET", "sr")
	p.same("SETRANGE", "empty", "0", "")
	p.same("EXISTS", "empty")
	p.sameErrorCode("SETRANGE", "sr", "-1", "x")
}

func TestSetOptionsMatchRedis(t *testing.T) {
	p := newPair(t)

	p.same("SET", "k", "v1", "NX")
	p.same("SET", "k", "v2", "NX")
	p.same("GET", "k")
	p.same("SET", "k", "v3", "XX")
	p.same("SET", "absent", "v", "XX")
	p.same("SET", "k", "v4", "GET")
	p.same("SET", "fresh", "v", "GET")
	p.sameErrorCode("SET", "k", "v", "NX", "XX")
	p.sameErrorCode("SET", "k", "v", "EX", "0")
	p.sameErrorCode("SET", "k", "v", "EX", "-1")
	p.sameErrorCode("SET", "k", "v", "EX", "abc")
	p.sameErrorCode("SET", "k", "v", "BOGUS")
	p.sameErrorCode("SET", "k", "v", "EX", "10", "PX", "10")
}

func TestExpiryCommandsMatchRedis(t *testing.T) {
	p := newPair(t)

	p.same("TTL", "missing")
	p.same("PTTL", "missing")
	p.same("SET", "k", "v")
	p.same("TTL", "k")
	p.same("EXPIRE", "k", "100")
	p.same("TTL", "k")
	p.same("PERSIST", "k")
	p.same("TTL", "k")
	p.same("PERSIST", "k")
	p.same("EXPIRE", "missing", "100")

	p.same("SET", "o", "v", "EX", "100")
	p.same("EXPIRE", "o", "50", "NX")
	p.same("EXPIRE", "o", "50", "XX")
	p.same("EXPIRE", "o", "10", "GT")
	p.same("EXPIRE", "o", "500", "GT")
	p.same("EXPIRE", "o", "10", "LT")
	p.same("SET", "p", "v")
	p.same("EXPIRE", "p", "100", "GT")
	p.sameErrorCode("EXPIRE", "o", "10", "NX", "XX")

	// An expiry already in the past deletes the key on both.
	p.same("SET", "gone", "v")
	p.same("EXPIREAT", "gone", "1")
	p.same("EXISTS", "gone")

	p.same("SET", "gx", "v", "EX", "100")
	p.same("GETEX", "gx", "PERSIST")
	p.same("TTL", "gx")
	p.same("GETEX", "gx", "EX", "200")
	p.same("TTL", "gx")
	p.same("GETEX", "missing")
}

func TestKeyspaceCommandsMatchRedis(t *testing.T) {
	p := newPair(t)

	p.same("MSET", "d1", "1", "d2", "2", "d3", "3")
	p.same("DEL", "d1", "d2", "nope")
	p.same("EXISTS", "d3", "d3", "nope")
	p.same("TYPE", "d3")
	p.same("TYPE", "nope")
	p.same("DBSIZE")

	p.same("SET", "old", "v")
	p.same("RENAME", "old", "new")
	p.same("EXISTS", "old")
	p.same("GET", "new")
	p.sameErrorCode("RENAME", "still-missing", "x")
	p.same("SET", "src", "s")
	p.same("SET", "dst", "d")
	p.same("RENAMENX", "src", "dst")
	p.same("GET", "dst")
	p.same("RENAMENX", "src", "fresh-dst")
	p.same("EXISTS", "src")

	p.same("SET", "cs", "value", "EX", "100")
	p.same("COPY", "cs", "cd")
	p.same("GET", "cd")
	p.same("TTL", "cd")
	p.same("COPY", "cs", "cd")
	p.same("COPY", "cs", "cd", "REPLACE")
	p.sameErrorCode("COPY", "cs", "cs")
	p.same("UNLINK", "cd")
	p.same("TOUCH", "cs", "missing")
}

func TestGlobPatternsMatchRedis(t *testing.T) {
	p := newPair(t)
	for _, k := range []string{
		"hello", "hallo", "hxllo", "heeello", "hillo", "hbllo",
		"user:1:name", "user:2:name", "[", "[abc", "a-b",
	} {
		p.same("SET", k, "v")
	}
	// KEYS has no defined order, so the comparison is on the count. The
	// matcher's exact behaviour is pinned by the unit tests; what this checks
	// is that the two agree on which keys match at all.
	for _, pattern := range []string{
		"h?llo", "h*llo", "h[ae]llo", "h[^e]llo", "h[a-c]llo", "h[c-a]llo",
		"user:*:name", "*", "nomatch*", "[", "[abc", `a\-b`, "?", "h[ae", "h[]llo",
	} {
		ours := p.ours.Do("KEYS", pattern)
		theirs := p.redis.do("KEYS", pattern)
		if len(ours.Elems) != len(theirs.Elems) {
			t.Errorf("KEYS %q matched %d keys, redis matched %d", pattern, len(ours.Elems), len(theirs.Elems))
		}
	}
}

func TestArityAndUnknownCommandsMatchRedis(t *testing.T) {
	p := newPair(t)
	for _, args := range [][]string{
		{"GET"},
		{"GET", "a", "b"},
		{"SET", "k"},
		{"SET"},
		{"INCR"},
		{"INCR", "a", "b"},
		{"EXPIRE", "k"},
		{"MSET", "odd"},
		{"MGET"},
		{"TOTALLYUNKNOWN"},
		{"TOTALLYUNKNOWN", "with", "args"},
	} {
		p.sameErrorCode(args...)
	}
}

func TestConnectionCommandsMatchRedis(t *testing.T) {
	p := newPair(t)
	p.same("PING")
	p.same("PING", "payload")
	p.same("ECHO", "round trip")
	p.same("SELECT", "0")
	// SELECT on any other index differs: this server implements one database
	// and says so, while Redis has sixteen. The error code is the same, which
	// is what a client library switches on.
	p.sameErrorCode("SELECT", "99")
}

func TestScanAgreesWithRedis(t *testing.T) {
	p := newPair(t)
	for i := 0; i < 300; i++ {
		p.ours.Do("SET", fmt.Sprintf("s:%03d", i), "v")
		p.redis.do("SET", fmt.Sprintf("s:%03d", i), "v")
	}

	// The cursor values differ by construction - Redis uses reverse binary
	// iteration over its hash table, this server uses shard indices - but the
	// contract both must honour is that a full walk visits every key exactly
	// once when nothing changes underneath it.
	collect := func(scan func(cursor string) (string, []string)) map[string]int {
		seen := map[string]int{}
		cursor := "0"
		for i := 0; i < 10_000; i++ {
			next, keys := scan(cursor)
			for _, k := range keys {
				seen[k]++
			}
			cursor = next
			if cursor == "0" {
				return seen
			}
		}
		t.Fatal("SCAN did not terminate")
		return nil
	}

	ourKeys := collect(func(cursor string) (string, []string) {
		rep := p.ours.Do("SCAN", cursor, "COUNT", "13")
		return string(rep.Elems[0].Str), elemsToStrings(rep.Elems[1])
	})
	redisKeys := collect(func(cursor string) (string, []string) {
		rep := p.redis.do("SCAN", cursor, "COUNT", "13")
		return string(rep.Elems[0].Str), elemsToStrings(rep.Elems[1])
	})

	if len(ourKeys) != len(redisKeys) {
		t.Fatalf("a full SCAN found %d keys, redis found %d", len(ourKeys), len(redisKeys))
	}
	for k := range redisKeys {
		if ourKeys[k] == 0 {
			t.Errorf("SCAN missed %s", k)
		}
		if ourKeys[k] > 1 {
			t.Errorf("SCAN returned %s %d times on a keyspace that never changed", k, ourKeys[k])
		}
	}
}

func elemsToStrings(r resp.Reply) []string {
	out := make([]string, 0, len(r.Elems))
	for _, e := range r.Elems {
		out = append(out, string(e.Str))
	}
	return out
}

func TestBinaryAndEdgeCaseValuesMatchRedis(t *testing.T) {
	p := newPair(t)
	values := []string{
		"",
		"a",
		string([]byte{0x00}),
		string([]byte{0x00, 0xff, '\r', '\n'}),
		strings.Repeat("x", 100_000),
		"value with spaces and\ttabs",
	}
	for i, v := range values {
		k := fmt.Sprintf("bin:%d", i)
		p.same("SET", k, v)
		p.same("GET", k)
		p.same("STRLEN", k)
	}
	// An empty key name is legal in Redis and must round trip.
	p.same("SET", "", "empty key name")
	p.same("GET", "")
	p.same("EXISTS", "")
}
