package bench

import (
	"fmt"
	"math/rand/v2"
	"strconv"
)

// Distribution selects which keys a worker touches.
type Distribution string

const (
	// Uniform spreads requests evenly. It is the worst case for any cache and
	// the best case for measuring the storage engine itself, because every
	// shard and every CPU cache line is hit equally.
	Uniform Distribution = "uniform"
	// Zipf concentrates requests on a small hot set, which is what real
	// key-value traffic looks like and what makes a benchmark flatter than it
	// should be if you forget to say you used it.
	Zipf Distribution = "zipf"
	// Sequential walks the keyspace in order, which is the friendliest
	// possible pattern for an LSM tree and is reported separately for exactly
	// that reason.
	Sequential Distribution = "sequential"
)

// ParseDistribution validates a distribution name.
func ParseDistribution(s string) (Distribution, error) {
	switch Distribution(s) {
	case Uniform, Zipf, Sequential:
		return Distribution(s), nil
	default:
		return "", fmt.Errorf("bench: unknown distribution %q (want uniform, zipf or sequential)", s)
	}
}

// Workload names the command mix.
type Workload string

const (
	// WorkloadGet is read only.
	WorkloadGet Workload = "get"
	// WorkloadSet is write only.
	WorkloadSet Workload = "set"
	// WorkloadMixed interleaves reads and writes at the configured ratio.
	WorkloadMixed Workload = "mixed"
	// WorkloadIncr is a read-modify-write mix that exercises the atomic Update
	// path rather than the plain write path.
	WorkloadIncr Workload = "incr"
	// WorkloadPing measures protocol and network overhead with no storage work
	// at all, which is the ceiling every other number is measured against.
	WorkloadPing Workload = "ping"
)

// ParseWorkload validates a workload name.
func ParseWorkload(s string) (Workload, error) {
	switch Workload(s) {
	case WorkloadGet, WorkloadSet, WorkloadMixed, WorkloadIncr, WorkloadPing:
		return Workload(s), nil
	default:
		return "", fmt.Errorf("bench: unknown workload %q (want get, set, mixed, incr or ping)", s)
	}
}

// keyPrefix namespaces benchmark keys so a run can be cleaned up without
// touching anything else in the database.
const keyPrefix = "rfs:bench:"

// generator produces the next command for one worker.
//
// Each worker owns one, so the random source is not shared and no lock is
// taken on the hot path. Buffers are reused, so generating a request allocates
// nothing and the load generator's own garbage collection stays out of the
// measurement.
type generator struct {
	workload  Workload
	dist      Distribution
	keySpace  uint64
	readRatio float64

	rng  *rand.Rand
	zipf *rand.Zipf
	seq  uint64

	keyBuf []byte
	value  []byte
}

func newGenerator(cfg Config, seed uint64) *generator {
	rng := rand.New(rand.NewPCG(seed, seed*2862933555777941757+3037000493))
	g := &generator{
		workload:  cfg.Workload,
		dist:      cfg.Distribution,
		keySpace:  uint64(cfg.KeySpace),
		readRatio: cfg.ReadRatio,
		rng:       rng,
		keyBuf:    make([]byte, 0, len(keyPrefix)+24),
		value:     makeValue(cfg.ValueSize, seed),
	}
	if cfg.Distribution == Zipf {
		// s=1.1 and v=1 give the roughly 80/20 skew that production key-value
		// traffic tends to show.
		g.zipf = rand.NewZipf(rng, 1.1, 1, g.keySpace-1)
	}
	return g
}

// makeValue builds a value of the requested size with non-repeating content,
// so that a server or filesystem that happens to compress cannot flatter the
// numbers.
func makeValue(size int, seed uint64) []byte {
	if size <= 0 {
		size = 1
	}
	b := make([]byte, size)
	rng := rand.New(rand.NewPCG(seed+99, 0x9E3779B97F4A7C15))
	for i := range b {
		b[i] = byte('a' + rng.UintN(26))
	}
	return b
}

// nextKey renders the next key into the reusable buffer.
func (g *generator) nextKey() []byte {
	var n uint64
	switch g.dist {
	case Sequential:
		n = g.seq % g.keySpace
		g.seq++
	case Zipf:
		n = g.zipf.Uint64()
	default:
		n = g.rng.Uint64N(g.keySpace)
	}
	g.keyBuf = g.keyBuf[:0]
	g.keyBuf = append(g.keyBuf, keyPrefix...)
	g.keyBuf = strconv.AppendUint(g.keyBuf, n, 10)
	return g.keyBuf
}

// isRead decides whether the next operation reads, for a mixed workload.
func (g *generator) isRead() bool {
	switch g.workload {
	case WorkloadGet:
		return true
	case WorkloadSet, WorkloadIncr:
		return false
	case WorkloadPing:
		return true
	default:
		return g.rng.Float64() < g.readRatio
	}
}
