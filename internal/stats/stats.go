// Package stats holds the server's runtime counters.
//
// Counters are plain atomics rather than Prometheus metrics so that the hot
// path never touches a label lookup or a mutex. The Prometheus exporter reads
// these on scrape, which moves the cost from once per command to once per
// fifteen seconds.
package stats

import (
	"sync/atomic"
	"time"
)

// Stats is the server-wide counter set.
type Stats struct {
	ConnectionsReceived atomic.Uint64
	ConnectionsRejected atomic.Uint64
	ConnectionsActive   atomic.Int64
	CommandsProcessed   atomic.Uint64
	CommandsFailed      atomic.Uint64
	CommandsRejected    atomic.Uint64
	NetInputBytes       atomic.Uint64
	NetOutputBytes      atomic.Uint64
	KeyspaceHits        atomic.Uint64
	KeyspaceMisses      atomic.Uint64
	ExpiredKeys         atomic.Uint64
	ProtocolErrors      atomic.Uint64
	TimeoutDisconnects  atomic.Uint64
	PipelineBatches     atomic.Uint64

	// latency accumulates command service time. Sum plus count gives a mean;
	// the histogram in the metrics package gives the percentiles that actually
	// matter. Both are kept because INFO has no place to render a histogram.
	LatencyNanosTotal atomic.Uint64
	LatencyCount      atomic.Uint64
	LatencyMaxNanos   atomic.Uint64

	startedAt time.Time
}

// New returns a counter set stamped with the process start time.
func New() *Stats { return &Stats{startedAt: time.Now()} }

// StartedAt reports when the counters were created.
func (s *Stats) StartedAt() time.Time { return s.startedAt }

// UptimeSeconds reports how long the server has been running.
func (s *Stats) UptimeSeconds() int64 { return int64(time.Since(s.startedAt).Seconds()) }

// ObserveLatency records one command's service time.
func (s *Stats) ObserveLatency(d time.Duration) {
	n := uint64(d.Nanoseconds())
	s.LatencyNanosTotal.Add(n)
	s.LatencyCount.Add(1)
	for {
		cur := s.LatencyMaxNanos.Load()
		if n <= cur || s.LatencyMaxNanos.CompareAndSwap(cur, n) {
			return
		}
	}
}

// MeanLatency returns the mean command service time, or zero if none.
func (s *Stats) MeanLatency() time.Duration {
	c := s.LatencyCount.Load()
	if c == 0 {
		return 0
	}
	return time.Duration(s.LatencyNanosTotal.Load() / c)
}

// ResetStat zeroes the counters that CONFIG RESETSTAT clears. Connection and
// uptime gauges are deliberately left alone: they describe current state, not
// accumulated history, and zeroing them would misreport reality.
func (s *Stats) ResetStat() {
	s.CommandsProcessed.Store(0)
	s.CommandsFailed.Store(0)
	s.CommandsRejected.Store(0)
	s.ConnectionsReceived.Store(0)
	s.ConnectionsRejected.Store(0)
	s.NetInputBytes.Store(0)
	s.NetOutputBytes.Store(0)
	s.KeyspaceHits.Store(0)
	s.KeyspaceMisses.Store(0)
	s.ExpiredKeys.Store(0)
	s.ProtocolErrors.Store(0)
	s.TimeoutDisconnects.Store(0)
	s.PipelineBatches.Store(0)
	s.LatencyNanosTotal.Store(0)
	s.LatencyCount.Store(0)
	s.LatencyMaxNanos.Store(0)
}
