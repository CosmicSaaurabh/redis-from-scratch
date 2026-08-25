// Package bench is the load generator and latency recorder used to produce
// this project's published throughput numbers.
//
// It exists instead of relying only on redis-benchmark for two reasons. The
// first is coordinated omission: a closed-loop client that waits for a reply
// before sending the next request cannot observe a stall, because during the
// stall it stops sending. Every latency number it reports is therefore
// measured from a moment the server was already ready, and a server that
// freezes for a second looks fine. This harness supports an open-loop mode
// that timestamps each request when it was *due* to be sent, which is the only
// way to see that stall. The second is that a benchmark whose methodology is
// not written down is a marketing number, and the report this package emits
// records the configuration that produced it.
package bench

import (
	"math"
	"math/bits"
	"sort"
	"time"
)

// Histogram records latencies in log-linear buckets.
//
// It is the HdrHistogram idea in miniature: values below a threshold are
// counted exactly, and above it each octave is divided into a fixed number of
// linear sub-buckets. That bounds the relative error at roughly 0.8% no matter
// how large the value gets, using a fixed 59 KB of memory, so a worker can
// record every single sample instead of sampling. Sampling is what makes a
// p99.9 meaningless.
type Histogram struct {
	counts [bucketCount]uint64
	total  uint64
	sum    uint64
	min    uint64
	max    uint64
}

const (
	// linearLimit is the value below which counting is exact.
	linearLimit = 256
	// subBuckets is the number of linear divisions per octave above the limit.
	subBuckets = 128
	// bucketCount covers every uint64 magnitude.
	bucketCount = linearLimit + (64-8)*subBuckets
)

// NewHistogram returns an empty histogram.
func NewHistogram() *Histogram { return &Histogram{min: math.MaxUint64} }

// Record adds one observation, in nanoseconds.
func (h *Histogram) Record(ns uint64) {
	h.counts[indexOf(ns)]++
	h.total++
	h.sum += ns
	if ns < h.min {
		h.min = ns
	}
	if ns > h.max {
		h.max = ns
	}
}

// RecordDuration adds one observation.
func (h *Histogram) RecordDuration(d time.Duration) {
	if d < 0 {
		d = 0
	}
	h.Record(uint64(d))
}

// Merge folds other into h. Workers keep private histograms to avoid sharing a
// cache line on the hot path and merge once at the end.
func (h *Histogram) Merge(other *Histogram) {
	for i := range other.counts {
		h.counts[i] += other.counts[i]
	}
	h.total += other.total
	h.sum += other.sum
	if other.total > 0 {
		if other.min < h.min {
			h.min = other.min
		}
		if other.max > h.max {
			h.max = other.max
		}
	}
}

// Count returns the number of observations.
func (h *Histogram) Count() uint64 { return h.total }

// Mean returns the arithmetic mean in nanoseconds.
func (h *Histogram) Mean() float64 {
	if h.total == 0 {
		return 0
	}
	return float64(h.sum) / float64(h.total)
}

// Min returns the smallest observation.
func (h *Histogram) Min() uint64 {
	if h.total == 0 {
		return 0
	}
	return h.min
}

// Max returns the largest observation.
func (h *Histogram) Max() uint64 { return h.max }

// Quantile returns the value at q, where q is in [0,1].
//
// The reported value is the lower bound of the bucket the quantile falls in,
// so it never overstates how fast the server was.
func (h *Histogram) Quantile(q float64) uint64 {
	if h.total == 0 {
		return 0
	}
	if q <= 0 {
		return h.min
	}
	if q >= 1 {
		return h.max
	}
	want := uint64(math.Ceil(q * float64(h.total)))
	var seen uint64
	for i, c := range h.counts {
		if c == 0 {
			continue
		}
		seen += c
		if seen >= want {
			return valueOf(i)
		}
	}
	return h.max
}

// Quantiles returns several quantiles in one pass over the buckets.
func (h *Histogram) Quantiles(qs ...float64) map[float64]uint64 {
	out := make(map[float64]uint64, len(qs))
	if h.total == 0 {
		for _, q := range qs {
			out[q] = 0
		}
		return out
	}
	sorted := append([]float64(nil), qs...)
	sort.Float64s(sorted)

	targets := make([]uint64, len(sorted))
	for i, q := range sorted {
		targets[i] = uint64(math.Ceil(q * float64(h.total)))
		if targets[i] == 0 {
			targets[i] = 1
		}
	}

	var seen uint64
	next := 0
	for i, c := range h.counts {
		if c == 0 {
			continue
		}
		seen += c
		for next < len(sorted) && seen >= targets[next] {
			out[sorted[next]] = valueOf(i)
			next++
		}
		if next == len(sorted) {
			break
		}
	}
	for ; next < len(sorted); next++ {
		out[sorted[next]] = h.max
	}
	return out
}

// indexOf maps a value to its bucket.
func indexOf(v uint64) int {
	if v < linearLimit {
		return int(v)
	}
	msb := bits.Len64(v) - 1
	shift := uint(msb - 7)
	sub := int((v >> shift) & (subBuckets - 1))
	return linearLimit + (msb-8)*subBuckets + sub
}

// valueOf returns the lower bound of a bucket.
func valueOf(i int) uint64 {
	if i < linearLimit {
		return uint64(i)
	}
	rel := i - linearLimit
	msb := rel/subBuckets + 8
	sub := rel % subBuckets
	shift := uint(msb - 7)
	return uint64(subBuckets+sub) << shift
}
