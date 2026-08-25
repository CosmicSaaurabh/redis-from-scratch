package bench

import (
	"math"
	"math/rand/v2"
	"sort"
	"testing"
	"time"
)

func TestIndexValueRoundTrip(t *testing.T) {
	// Every value must land in a bucket whose lower bound is at most the value
	// and within the promised relative error.
	for _, v := range []uint64{0, 1, 255, 256, 257, 511, 512, 1000, 1 << 20, 1<<63 - 1} {
		i := indexOf(v)
		lo := valueOf(i)
		if lo > v {
			t.Fatalf("value %d landed in bucket starting at %d", v, lo)
		}
		if v >= linearLimit {
			if rel := float64(v-lo) / float64(v); rel > 1.0/subBuckets {
				t.Fatalf("value %d bucket %d has relative error %.4f", v, lo, rel)
			}
		} else if lo != v {
			t.Fatalf("value %d below the linear limit was not exact, got %d", v, lo)
		}
	}
}

func TestBucketsAreMonotonic(t *testing.T) {
	prev := indexOf(0)
	for v := uint64(1); v < 1<<22; v = v + 1 + v/64 {
		i := indexOf(v)
		if i < prev {
			t.Fatalf("index went backwards at %d", v)
		}
		prev = i
	}
}

func TestQuantilesMatchASortedSample(t *testing.T) {
	h := NewHistogram()
	rng := rand.New(rand.NewPCG(1, 2))
	const n = 200_000
	raw := make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		// A realistic latency shape: a tight body with a heavy tail.
		v := uint64(rng.ExpFloat64() * 50_000)
		if i%1000 == 0 {
			v += uint64(rng.Uint64N(50_000_000))
		}
		h.Record(v)
		raw = append(raw, v)
	}
	sort.Slice(raw, func(i, j int) bool { return raw[i] < raw[j] })

	for _, q := range []float64{0.5, 0.9, 0.99, 0.999} {
		want := raw[int(math.Ceil(q*float64(n)))-1]
		got := h.Quantile(q)
		if got > want {
			t.Errorf("q%.3f = %d overstates the true value %d", q, got, want)
		}
		if want > 0 {
			if rel := float64(want-got) / float64(want); rel > 0.02 {
				t.Errorf("q%.3f = %d, true %d, relative error %.4f exceeds the bucket width", q, got, want, rel)
			}
		}
	}
	if h.Count() != n {
		t.Errorf("Count = %d want %d", h.Count(), n)
	}
}

func TestQuantilesBatchMatchesIndividual(t *testing.T) {
	h := NewHistogram()
	for i := 0; i < 10_000; i++ {
		h.Record(uint64(i * 37 % 9973))
	}
	qs := []float64{0.5, 0.95, 0.99, 0.999}
	batch := h.Quantiles(qs...)
	for _, q := range qs {
		if got, want := batch[q], h.Quantile(q); got != want {
			t.Errorf("q%.3f batch=%d individual=%d", q, got, want)
		}
	}
}

func TestMergePreservesTotals(t *testing.T) {
	a, b := NewHistogram(), NewHistogram()
	for i := 1; i <= 1000; i++ {
		a.RecordDuration(time.Duration(i) * time.Microsecond)
	}
	for i := 1001; i <= 2000; i++ {
		b.RecordDuration(time.Duration(i) * time.Microsecond)
	}
	a.Merge(b)
	if a.Count() != 2000 {
		t.Fatalf("Count = %d want 2000", a.Count())
	}
	if a.Min() != uint64(time.Microsecond) {
		t.Errorf("Min = %d", a.Min())
	}
	if a.Max() != uint64(2000*time.Microsecond) {
		t.Errorf("Max = %d", a.Max())
	}
	// The mean of 1..2000 microseconds is 1000.5 us.
	if mean := a.Mean() / 1000; math.Abs(mean-1000.5) > 1 {
		t.Errorf("Mean = %.2f us, want about 1000.5", mean)
	}
}

func TestEmptyHistogram(t *testing.T) {
	h := NewHistogram()
	if h.Count() != 0 || h.Mean() != 0 || h.Quantile(0.99) != 0 || h.Min() != 0 {
		t.Fatal("an empty histogram must report zeroes rather than sentinel values")
	}
}

func BenchmarkRecord(b *testing.B) {
	h := NewHistogram()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h.Record(uint64(i&0xFFFF) + 1000)
	}
}
