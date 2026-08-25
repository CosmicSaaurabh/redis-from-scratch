package command

import "testing"

func TestMatchPattern(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "", true},
		{"*", "anything", true},
		{"**", "anything", true},
		{"a*", "abc", true},
		{"a*", "bbc", false},
		{"*c", "abc", true},
		{"a*c", "abc", true},
		{"a*c", "abbbbc", true},
		{"a*c", "abd", false},
		{"?", "a", true},
		{"?", "ab", false},
		{"a?c", "abc", true},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hello", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true},
		{"h[^e]llo", "hello", false},
		{"h[a-c]llo", "hbllo", true},
		{"h[a-c]llo", "hdllo", false},
		{"h[c-a]llo", "hbllo", true},
		{`\*`, "*", true},
		{`\*`, "x", false},
		{`a\?c`, "a?c", true},
		{`a\?c`, "abc", false},
		{"user:*:session", "user:42:session", true},
		{"user:*:session", "user:42:token", false},
		// An unterminated class matches nothing when it is empty and behaves as
		// a normal class when it has members. Verified against redis-server
		// 8.4: KEYS "[" returns nothing, KEYS "[abc" returns the key "a".
		{"[", "[", false},
		{"[", "", false},
		{"[abc", "a", true},
		{"[abc", "d", false},
		{"[a", "a", true},
		{"", "", true},
		{"", "a", false},
		{`\`, `\`, true},
	}
	for _, c := range cases {
		if got := matchPattern([]byte(c.pattern), []byte(c.s), false); got != c.want {
			t.Errorf("matchPattern(%q, %q) = %v want %v", c.pattern, c.s, got, c.want)
		}
	}
}

func TestMatchPatternCaseInsensitive(t *testing.T) {
	if !matchPattern([]byte("HELLO"), []byte("hello"), true) {
		t.Error("case-insensitive literal did not match")
	}
	if !matchPattern([]byte("h[A-C]llo"), []byte("hbllo"), true) {
		t.Error("case-insensitive range did not match")
	}
	if matchPattern([]byte("HELLO"), []byte("hello"), false) {
		t.Error("case-sensitive match should have failed")
	}
}

func TestIsTrivialPattern(t *testing.T) {
	for p, want := range map[string]bool{"*": true, "**": true, "": false, "a*": false, "?": false} {
		if got := isTrivialPattern([]byte(p)); got != want {
			t.Errorf("isTrivialPattern(%q) = %v want %v", p, got, want)
		}
	}
}

// FuzzMatchPattern asserts the matcher terminates and never panics on
// adversarial patterns, which is the property that matters when the pattern
// comes straight off the wire.
func FuzzMatchPattern(f *testing.F) {
	f.Add("*", "abc")
	f.Add("[a-", "a")
	f.Add(`\`, `\`)
	f.Add("*?*?*?*", "aaaaaaaa")
	f.Fuzz(func(t *testing.T, pattern, s string) {
		if len(pattern) > 64 || len(s) > 64 {
			t.Skip()
		}
		_ = matchPattern([]byte(pattern), []byte(s), false)
		_ = matchPattern([]byte(pattern), []byte(s), true)
	})
}

func TestResolveRangeMatchesRedis(t *testing.T) {
	// The expectations below were taken from redis-server 8.4 rather than from
	// the documentation, which does not describe the clamping behaviour.
	cases := []struct {
		start, end, length int64
		wantLo, wantHi     int64
		wantEmpty          bool
	}{
		{0, 3, 16, 0, 3, false},
		{-3, -1, 16, 13, 15, false},
		{0, -1, 16, 0, 15, false},
		{10, 100, 16, 10, 15, false},
		{-1, -3, 16, 0, 0, true},
		{100, 200, 16, 0, 0, true},
		// An end still negative after adjustment clamps to the first byte
		// rather than selecting nothing.
		{-100, -90, 16, 0, 0, false},
		{-100, -1, 16, 0, 15, false},
		{5, 5, 16, 5, 5, false},
		{0, 0, 0, 0, 0, true},
	}
	for _, c := range cases {
		lo, hi, empty := resolveRange(c.start, c.end, c.length)
		if empty != c.wantEmpty || (!empty && (lo != c.wantLo || hi != c.wantHi)) {
			t.Errorf("resolveRange(%d, %d, %d) = %d,%d,%v want %d,%d,%v",
				c.start, c.end, c.length, lo, hi, empty, c.wantLo, c.wantHi, c.wantEmpty)
		}
	}
}
