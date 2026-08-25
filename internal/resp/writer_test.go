package resp

import (
	"bytes"
	"math"
	"testing"
)

func encode(t *testing.T, v Version, fn func(w *Writer)) string {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, 0)
	w.SetVersion(v)
	fn(w)
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func TestWriterRESP2Degradation(t *testing.T) {
	cases := []struct {
		name  string
		fn    func(w *Writer)
		resp2 string
		resp3 string
	}{
		{"null", func(w *Writer) { w.WriteNull() }, "$-1\r\n", "_\r\n"},
		{"null array", func(w *Writer) { w.WriteNullArray() }, "*-1\r\n", "_\r\n"},
		{"bool true", func(w *Writer) { w.WriteBool(true) }, ":1\r\n", "#t\r\n"},
		{"bool false", func(w *Writer) { w.WriteBool(false) }, ":0\r\n", "#f\r\n"},
		{"map header", func(w *Writer) { w.WriteMapHeader(2) }, "*4\r\n", "%2\r\n"},
		{"set header", func(w *Writer) { w.WriteSetHeader(3) }, "*3\r\n", "~3\r\n"},
		{"double", func(w *Writer) { w.WriteDouble(1.5) }, "$3\r\n1.5\r\n", ",1.5\r\n"},
		{"double int", func(w *Writer) { w.WriteDouble(3) }, "$1\r\n3\r\n", ",3\r\n"},
		{"inf", func(w *Writer) { w.WriteDouble(math.Inf(1)) }, "$3\r\ninf\r\n", ",inf\r\n"},
		{"verbatim", func(w *Writer) { w.WriteVerbatim("txt", "hi") }, "$2\r\nhi\r\n", "=6\r\ntxt:hi\r\n"},
		{"bignum", func(w *Writer) { w.WriteBigNumber("12") }, "$2\r\n12\r\n", "(12\r\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := encode(t, RESP2, c.fn); got != c.resp2 {
				t.Errorf("RESP2 = %q want %q", got, c.resp2)
			}
			if got := encode(t, RESP3, c.fn); got != c.resp3 {
				t.Errorf("RESP3 = %q want %q", got, c.resp3)
			}
		})
	}
}

func TestWriterBasicTypes(t *testing.T) {
	got := encode(t, RESP2, func(w *Writer) {
		w.WriteSimpleString("OK")
		w.WriteInt(-42)
		w.WriteBulk([]byte("abc"))
		w.WriteBulk(nil)
		w.WriteArrayHeader(0)
		w.WriteError("ERR nope")
	})
	want := "+OK\r\n:-42\r\n$3\r\nabc\r\n$-1\r\n*0\r\n-ERR nope\r\n"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestWriteErrorStripsNewlines(t *testing.T) {
	got := encode(t, RESP2, func(w *Writer) {
		w.WriteError("ERR bad\r\n+INJECTED")
	})
	want := "-ERR bad  +INJECTED\r\n"
	if got != want {
		t.Fatalf("newline injection not neutralised: %q", got)
	}
}

func TestFormatDouble(t *testing.T) {
	cases := []struct {
		in   float64
		want string
	}{
		{0, "0"},
		{1, "1"},
		{1.5, "1.5"},
		{-2.25, "-2.25"},
		{math.Inf(-1), "-inf"},
		{1e18, "1e+18"},
		{0.1, "0.10000000000000001"},
	}
	for _, c := range cases {
		if got := FormatDouble(c.in); got != c.want {
			t.Errorf("FormatDouble(%v) = %q want %q", c.in, got, c.want)
		}
	}
	if got := FormatDouble(math.NaN()); got != "nan" {
		t.Errorf("NaN = %q", got)
	}
}

func BenchmarkWriteBulk(b *testing.B) {
	w := NewWriter(discard{}, 64<<10)
	val := bytes.Repeat([]byte("v"), 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WriteBulk(val)
	}
	_ = w.Flush()
}

func BenchmarkReadCommand(b *testing.B) {
	one := []byte("*3\r\n$3\r\nSET\r\n$6\r\nkey123\r\n$8\r\nvalue123\r\n")
	stream := bytes.Repeat(one, 1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(one)))
	b.ResetTimer()
	for i := 0; i < b.N; i += 1024 {
		r := NewReader(bytes.NewReader(stream), DefaultLimits())
		for j := 0; j < 1024; j++ {
			if _, err := r.ReadCommand(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
