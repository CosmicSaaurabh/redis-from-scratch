package command

// matchPattern implements Redis glob matching for KEYS and SCAN MATCH.
//
// The grammar is '*' for any run, '?' for one byte, '[...]' for a class with
// optional '^' negation and 'a-b' ranges, and '\' to escape the next byte.
//
// The implementation is the recursive one Redis uses rather than a compiled
// automaton. That is a deliberate trade: patterns here are supplied per call
// and used once, so compiling would cost more than it saves, and the recursion
// only ever descends on '*', which is bounded by the pattern length.
func matchPattern(pattern, s []byte, caseInsensitive bool) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case '*':
			// Collapse runs of '*' so that "a**b" does not recurse twice per
			// position.
			for len(pattern) > 1 && pattern[1] == '*' {
				pattern = pattern[1:]
			}
			if len(pattern) == 1 {
				return true
			}
			for i := 0; i <= len(s); i++ {
				if matchPattern(pattern[1:], s[i:], caseInsensitive) {
					return true
				}
			}
			return false

		case '?':
			if len(s) == 0 {
				return false
			}
			s = s[1:]
			pattern = pattern[1:]

		case '[':
			if len(s) == 0 {
				return false
			}
			rest, ok := matchClass(pattern, s[0], caseInsensitive)
			if !ok {
				return false
			}
			pattern = rest
			s = s[1:]

		case '\\':
			// A trailing backslash matches a literal backslash rather than
			// running off the end of the pattern.
			if len(pattern) >= 2 {
				pattern = pattern[1:]
			}
			fallthrough

		default:
			if len(s) == 0 || !byteEq(pattern[0], s[0], caseInsensitive) {
				return false
			}
			s = s[1:]
			pattern = pattern[1:]
		}
	}
	return len(s) == 0
}

// matchClass evaluates a bracket expression against c and returns the pattern
// remaining after the closing bracket.
func matchClass(pattern []byte, c byte, ci bool) (rest []byte, matched bool) {
	p := pattern[1:] // skip '['
	negate := false
	if len(p) > 0 && p[0] == '^' {
		negate = true
		p = p[1:]
	}

	found := false
	for len(p) > 0 && p[0] != ']' {
		switch {
		case p[0] == '\\' && len(p) >= 2:
			if byteEq(p[1], c, ci) {
				found = true
			}
			p = p[2:]

		case len(p) >= 3 && p[1] == '-' && p[2] != ']':
			lo, hi := p[0], p[2]
			if lo > hi {
				lo, hi = hi, lo
			}
			v := c
			if ci {
				v, lo, hi = lower(c), lower(lo), lower(hi)
			}
			if v >= lo && v <= hi {
				found = true
			}
			p = p[3:]

		default:
			if byteEq(p[0], c, ci) {
				found = true
			}
			p = p[1:]
		}
	}
	// An unterminated class consumes the rest of the pattern, which is what
	// Redis does; there is no error reply for a malformed pattern.
	if len(p) > 0 {
		p = p[1:]
	}
	return p, found != negate
}

func byteEq(a, b byte, ci bool) bool {
	if a == b {
		return true
	}
	return ci && lower(a) == lower(b)
}

func lower(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + 32
	}
	return b
}

// isTrivialPattern reports whether the pattern matches everything, letting
// KEYS and SCAN skip the matcher entirely for the overwhelmingly common "*".
func isTrivialPattern(p []byte) bool {
	for _, b := range p {
		if b != '*' {
			return false
		}
	}
	return len(p) > 0
}
