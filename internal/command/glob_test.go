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
