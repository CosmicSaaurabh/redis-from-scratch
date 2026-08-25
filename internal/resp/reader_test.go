package resp

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func readAll(t *testing.T, in string) [][]string {
	t.Helper()
	r := NewReader(strings.NewReader(in), DefaultLimits())
	var out [][]string
	for {
		args, err := r.ReadCommand()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("ReadCommand(%q): %v", in, err)
		}
		if args == nil {
			continue
		}
		cmd := make([]string, len(args))
		for i, a := range args {
			cmd[i] = string(a)
		}
		out = append(out, cmd)
	}
}

func TestReadCommandMultibulk(t *testing.T) {
	got := readAll(t, "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
	want := [][]string{{"GET", "foo"}}
	if len(got) != 1 || got[0][0] != want[0][0] || got[0][1] != want[0][1] {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestReadCommandPipelined(t *testing.T) {
	in := "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\nc\r\n"
	got := readAll(t, in)
	if len(got) != 3 {
		t.Fatalf("expected 3 commands, got %d: %v", len(got), got)
	}
	if got[2][0] != "SET" || got[2][2] != "c" {
		t.Fatalf("third command wrong: %v", got[2])
	}
}

func TestReadCommandEmptyAndNullMultibulkAreNoOps(t *testing.T) {
	got := readAll(t, "*0\r\n*-1\r\n*1\r\n$4\r\nPING\r\n")
	if len(got) != 1 || got[0][0] != "PING" {
		t.Fatalf("expected only PING, got %v", got)
	}
}

func TestReadCommandEmptyBulkArgument(t *testing.T) {
	got := readAll(t, "*2\r\n$3\r\nSET\r\n$0\r\n\r\n")
	if len(got) != 1 || len(got[0]) != 2 || got[0][1] != "" {
		t.Fatalf("expected empty second arg, got %#v", got)
	}
}

func TestReadCommandInline(t *testing.T) {
	got := readAll(t, "PING\r\nSET  foo\tbar\r\n")
	if len(got) != 2 {
		t.Fatalf("expected 2, got %v", got)
	}
	if got[1][0] != "SET" || got[1][1] != "foo" || got[1][2] != "bar" {
		t.Fatalf("inline split wrong: %v", got[1])
	}
}

func TestReadCommandInlineBlankLineIsNoOp(t *testing.T) {
	got := readAll(t, "\r\n\r\nPING\r\n")
	if len(got) != 1 || got[0][0] != "PING" {
		t.Fatalf("got %v", got)
	}
}

func TestReadCommandLargeArgumentCrossesArenaBlock(t *testing.T) {
	// One argument larger than the arena block, followed by a small one. The
	// small argument must not corrupt the large one, which is the bug a naive
	// growable arena would introduce.
	big := strings.Repeat("x", arenaBlock*2+7)
	in := "*3\r\n$3\r\nSET\r\n$" + itoa(len(big)) + "\r\n" + big + "\r\n$5\r\nsmall\r\n"
	r := NewReader(strings.NewReader(in), DefaultLimits())
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 3 {
		t.Fatalf("want 3 args, got %d", len(args))
	}
	if string(args[1]) != big {
		t.Fatalf("large argument corrupted: len %d want %d", len(args[1]), len(big))
	}
	if string(args[2]) != "small" {
		t.Fatalf("trailing argument corrupted: %q", args[2])
	}
}

func TestReadCommandManySmallArgsShareOneBlock(t *testing.T) {
	// 200 single-byte args must all survive; if the arena reallocated in place
	// the earlier slices would now alias freed-and-reused memory.
	var sb strings.Builder
	const n = 200
	sb.WriteString("*" + itoa(n) + "\r\n")
	for i := 0; i < n; i++ {
		sb.WriteString("$1\r\n" + string(rune('a'+i%26)) + "\r\n")
	}
	r := NewReader(strings.NewReader(sb.String()), DefaultLimits())
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		want := byte('a' + i%26)
		if len(args[i]) != 1 || args[i][0] != want {
			t.Fatalf("arg %d = %q, want %q", i, args[i], string(want))
		}
	}
}

func TestReadCommandProtocolErrors(t *testing.T) {
	cases := map[string]string{
		"non-bulk element":     "*1\r\n+OK\r\n",
		"bad multibulk count":  "*abc\r\n",
		"bad bulk count":       "*1\r\n$abc\r\n",
		"negative bulk":        "*1\r\n$-5\r\n",
		"missing CRLF on bulk": "*1\r\n$2\r\nabxx",
		"bare LF terminator":   "*1\n",
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			r := NewReader(strings.NewReader(in), DefaultLimits())
			_, err := r.ReadCommand()
			if err == nil {
				t.Fatalf("expected error")
			}
			if !IsProtocolError(err) && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("expected protocol error, got %T %v", err, err)
			}
		})
	}
}

func TestReadCommandEnforcesLimits(t *testing.T) {
	lim := DefaultLimits()
	lim.MaxBulkSize = 4
	lim.MaxMultiBulkLen = 2

	r := NewReader(strings.NewReader("*1\r\n$5\r\nhello\r\n"), lim)
	if _, err := r.ReadCommand(); !IsProtocolError(err) {
		t.Fatalf("oversized bulk accepted: %v", err)
	}

	r = NewReader(strings.NewReader("*3\r\n"), lim)
	if _, err := r.ReadCommand(); !IsProtocolError(err) {
		t.Fatalf("oversized multibulk accepted: %v", err)
	}
}

func TestReadCommandTruncatedStreamIsUnexpectedEOF(t *testing.T) {
	r := NewReader(strings.NewReader("*2\r\n$3\r\nGET\r\n$10\r\nabc"), DefaultLimits())
	_, err := r.ReadCommand()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("want ErrUnexpectedEOF, got %v", err)
	}
}

func TestReadCommandCleanEOF(t *testing.T) {
	r := NewReader(strings.NewReader(""), DefaultLimits())
	if _, err := r.ReadCommand(); !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF, got %v", err)
	}
}

func TestParseInt(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		ok   bool
	}{
		{"0", 0, true},
		{"-1", -1, true},
		{"+7", 7, true},
		{"9223372036854775807", 1<<63 - 1, true},
		{"9223372036854775808", 0, false},
		{"", 0, false},
		{"-", 0, false},
		{"1a", 0, false},
		{" 1", 0, false},
	}
	for _, c := range cases {
		got, ok := ParseInt([]byte(c.in))
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("ParseInt(%q) = %d,%v want %d,%v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// FuzzReadCommand asserts the parser never panics and never reports success on
// input it did not fully consume in a well-framed way.
func FuzzReadCommand(f *testing.F) {
	f.Add([]byte("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"))
	f.Add([]byte("PING\r\n"))
	f.Add([]byte("*-1\r\n"))
	f.Add([]byte("*1\r\n$0\r\n\r\n"))
	f.Add([]byte("$\r\n"))
	f.Add([]byte("*99999999999999999999\r\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		lim := DefaultLimits()
		lim.MaxBulkSize = 1 << 16
		lim.MaxMultiBulkLen = 1024
		r := NewReader(bytes.NewReader(data), lim)
		for i := 0; i < 64; i++ {
			args, err := r.ReadCommand()
			if err != nil {
				return
			}
			for _, a := range args {
				_ = len(a)
			}
		}
	})
}
