package bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/resp"
)

// Config describes one benchmark run.
type Config struct {
	// Addr is the server to load.
	Addr string
	// Password authenticates if the server requires it.
	Password string
	// Connections is the number of concurrent client connections.
	Connections int
	// Pipeline is how many commands are in flight per connection before
	// replies are read. One means a strict request-response round trip.
	Pipeline int
	// Duration is how long the measured phase runs. Ignored when Requests is
	// set.
	Duration time.Duration
	// Requests is a fixed operation count for the measured phase.
	Requests int
	// Warmup runs before measurement begins and is discarded. Without it the
	// first numbers include connection setup, the server's cold maps and Go's
	// JIT-free but still cold branch predictors.
	Warmup time.Duration
	// Rate is the target operations per second across all connections. Zero
	// means closed loop: send, wait for the reply, send again.
	Rate int
	// KeySpace is how many distinct keys the workload touches.
	KeySpace int
	// ValueSize is the value length in bytes for writes.
	ValueSize int
	// ReadRatio is the fraction of reads in a mixed workload.
	ReadRatio float64
	// Workload names the command mix.
	Workload Workload
	// Distribution selects the key access pattern.
	Distribution Distribution
	// Preload writes the whole keyspace before measuring, so that a read
	// workload measures hits rather than misses.
	Preload bool
	// Label names the run in the report.
	Label string
}

// withDefaults fills in anything the caller left blank.
func (c *Config) withDefaults() error {
	if c.Addr == "" {
		return errors.New("bench: Addr is required")
	}
	if c.Connections <= 0 {
		c.Connections = 50
	}
	if c.Pipeline <= 0 {
		c.Pipeline = 1
	}
	if c.KeySpace <= 0 {
		c.KeySpace = 100_000
	}
	if c.ValueSize <= 0 {
		c.ValueSize = 64
	}
	if c.Workload == "" {
		c.Workload = WorkloadMixed
	}
	if c.Distribution == "" {
		c.Distribution = Uniform
	}
	if c.Requests <= 0 && c.Duration <= 0 {
		c.Duration = 10 * time.Second
	}
	if c.ReadRatio < 0 || c.ReadRatio > 1 {
		return fmt.Errorf("bench: ReadRatio %.2f is outside [0,1]", c.ReadRatio)
	}
	if c.Label == "" {
		c.Label = string(c.Workload)
	}
	return nil
}

// Result is one run's outcome.
type Result struct {
	Config    Config
	StartedAt time.Time
	Elapsed   time.Duration
	Ops       uint64
	Errors    uint64
	Reads     uint64
	Writes    uint64
	Hist      *Histogram
	// AchievedRate is operations per second over the measured window.
	AchievedRate float64
	// ServerInfo is the INFO reply captured after the run.
	ServerInfo map[string]string
	// Notes record anything that would change how the numbers should be read.
	Notes []string
}

// Run executes one benchmark.
func Run(ctx context.Context, cfg Config) (*Result, error) {
	if err := cfg.withDefaults(); err != nil {
		return nil, err
	}

	conns := make([]*conn, 0, cfg.Connections)
	defer func() {
		for _, c := range conns {
			c.close()
		}
	}()
	for i := 0; i < cfg.Connections; i++ {
		c, err := dial(cfg)
		if err != nil {
			return nil, fmt.Errorf("bench: connection %d: %w", i, err)
		}
		conns = append(conns, c)
	}

	res := &Result{Config: cfg, Hist: NewHistogram()}

	if cfg.Preload {
		if err := preload(ctx, conns[0], cfg); err != nil {
			return nil, fmt.Errorf("bench: preload: %w", err)
		}
	}
	if cfg.Warmup > 0 {
		warm := cfg
		warm.Duration = cfg.Warmup
		warm.Requests = 0
		warm.Preload = false
		if _, err := drive(ctx, conns, warm, true); err != nil {
			return nil, fmt.Errorf("bench: warmup: %w", err)
		}
	}

	res.StartedAt = time.Now()
	m, err := drive(ctx, conns, cfg, false)
	if err != nil {
		return nil, err
	}
	res.Elapsed = m.elapsed
	res.Ops = m.ops
	res.Errors = m.errs
	res.Reads = m.reads
	res.Writes = m.writes
	res.Hist = m.hist
	if res.Elapsed > 0 {
		res.AchievedRate = float64(res.Ops) / res.Elapsed.Seconds()
	}

	if info, err := fetchInfo(conns[0]); err == nil {
		res.ServerInfo = info
	}
	if cfg.Rate > 0 {
		want := float64(cfg.Rate)
		if res.AchievedRate < want*0.98 {
			// An open-loop run that could not keep up did not measure what it
			// was asked to measure, and its latency figures describe a
			// saturated system rather than one at the target rate. Saying so
			// in the report is the difference between a benchmark and a
			// plausible-looking chart.
			res.Notes = append(res.Notes, fmt.Sprintf(
				"open-loop target of %d ops/sec was not met: achieved %.0f, so latencies reflect a saturated server",
				cfg.Rate, res.AchievedRate))
		}
	}
	return res, nil
}

type measurement struct {
	elapsed       time.Duration
	ops, errs     uint64
	reads, writes uint64
	hist          *Histogram
}

// drive runs the load across all connections.
func drive(ctx context.Context, conns []*conn, cfg Config, warmup bool) (measurement, error) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if cfg.Duration > 0 {
		var stop context.CancelFunc
		runCtx, stop = context.WithTimeout(runCtx, cfg.Duration)
		defer stop()
	}

	var (
		ops, errs, reads, writes atomic.Uint64
		budget                   atomic.Int64
		wg                       sync.WaitGroup
		firstErr                 atomic.Pointer[error]
	)
	if cfg.Requests > 0 {
		budget.Store(int64(cfg.Requests))
	}

	// Each connection gets its own share of the target rate, so no shared
	// limiter has to be locked on the hot path.
	var perConnInterval time.Duration
	if cfg.Rate > 0 {
		perConnInterval = time.Duration(float64(time.Second) / (float64(cfg.Rate) / float64(len(conns))))
	}

	hists := make([]*Histogram, len(conns))
	start := time.Now()

	for i, c := range conns {
		hists[i] = NewHistogram()
		wg.Add(1)
		go func(i int, c *conn) {
			defer wg.Done()
			g := newGenerator(cfg, uint64(i)+1)
			w := worker{
				conn: c, cfg: cfg, gen: g, hist: hists[i],
				ops: &ops, errs: &errs, reads: &reads, writes: &writes,
				budget: &budget, interval: perConnInterval, start: start,
			}
			if err := w.run(runCtx); err != nil && !isExpectedStop(err) {
				e := err
				firstErr.CompareAndSwap(nil, &e)
				cancel()
			}
		}(i, c)
	}
	wg.Wait()
	elapsed := time.Since(start)

	if p := firstErr.Load(); p != nil {
		return measurement{}, *p
	}

	merged := NewHistogram()
	for _, h := range hists {
		merged.Merge(h)
	}
	return measurement{
		elapsed: elapsed,
		ops:     ops.Load(), errs: errs.Load(),
		reads: reads.Load(), writes: writes.Load(),
		hist: merged,
	}, nil
}

// worker drives one connection.
type worker struct {
	conn *conn
	cfg  Config
	gen  *generator
	hist *Histogram

	ops, errs, reads, writes *atomic.Uint64
	budget                   *atomic.Int64

	// interval is the per-connection send period in open-loop mode.
	interval time.Duration
	start    time.Time
}

func (w *worker) run(ctx context.Context) error {
	depth := w.cfg.Pipeline
	sent := 0

	for {
		if ctx.Err() != nil {
			return nil
		}
		if w.budget != nil && w.cfg.Requests > 0 {
			if w.budget.Add(int64(-depth)) < 0 {
				return nil
			}
		}

		// In open-loop mode the clock, not the server, decides when a request
		// is due. The latency recorded runs from that due time, so a server
		// that stalls shows up as latency instead of quietly disappearing from
		// the sample - which is exactly the coordinated omission that makes
		// closed-loop latency numbers optimistic.
		var due time.Time
		if w.interval > 0 {
			due = w.start.Add(time.Duration(sent) * w.interval)
			if wait := time.Until(due); wait > 0 {
				select {
				case <-time.After(wait):
				case <-ctx.Done():
					return nil
				}
			}
		}

		issued := time.Now()
		if due.IsZero() {
			due = issued
		}

		var batchReads, batchWrites int
		for i := 0; i < depth; i++ {
			// encode reports what it chose. Asking the generator twice would
			// draw two random numbers per command and make the achieved
			// read/write split disagree with the configured ratio.
			if w.encode(w.conn.w) {
				batchReads++
			} else {
				batchWrites++
			}
		}
		if err := w.conn.w.Flush(); err != nil {
			return err
		}

		var batchErrs int
		for i := 0; i < depth; i++ {
			rep, err := w.conn.r.Read()
			if err != nil {
				return err
			}
			if rep.IsError() {
				batchErrs++
			}
		}

		// The whole batch shares one latency sample per command. With a
		// pipeline depth above one this is batch latency, not per-command
		// latency, and the report says so rather than dividing and pretending.
		lat := time.Since(due)
		for i := 0; i < depth; i++ {
			w.hist.RecordDuration(lat)
		}

		w.ops.Add(uint64(depth))
		w.reads.Add(uint64(batchReads))
		w.writes.Add(uint64(batchWrites))
		if batchErrs > 0 {
			w.errs.Add(uint64(batchErrs))
		}
		sent += depth
	}
}

// encode writes one command onto the connection's buffer and reports whether
// it was a read.
func (w *worker) encode(out *resp.Writer) bool {
	switch w.cfg.Workload {
	case WorkloadPing:
		out.WriteCommandStrings("PING")
		return true
	case WorkloadIncr:
		out.WriteCommand([]byte("INCR"), w.gen.nextKey())
		return false
	case WorkloadGet:
		out.WriteCommand([]byte("GET"), w.gen.nextKey())
		return true
	case WorkloadSet:
		out.WriteCommand([]byte("SET"), w.gen.nextKey(), w.gen.value)
		return false
	default:
		if w.gen.isRead() {
			out.WriteCommand([]byte("GET"), w.gen.nextKey())
			return true
		}
		out.WriteCommand([]byte("SET"), w.gen.nextKey(), w.gen.value)
		return false
	}
}

// conn is one benchmark client connection.
type conn struct {
	nc net.Conn
	r  *resp.ReplyReader
	w  *resp.Writer
}

func dial(cfg Config) (*conn, error) {
	nc, err := net.DialTimeout("tcp", cfg.Addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		// Nagle would coalesce small requests and turn a latency measurement
		// into a measurement of Nagle.
		_ = tc.SetNoDelay(true)
	}
	lim := resp.DefaultLimits()
	c := &conn{
		nc: nc,
		r:  resp.NewReplyReader(nc, lim),
		w:  resp.NewWriter(nc, 256<<10),
	}
	if cfg.Password != "" {
		c.w.WriteCommandStrings("AUTH", cfg.Password)
		if err := c.w.Flush(); err != nil {
			c.close()
			return nil, err
		}
		rep, err := c.r.Read()
		if err != nil {
			c.close()
			return nil, err
		}
		if rep.IsError() {
			c.close()
			return nil, fmt.Errorf("AUTH failed: %s", rep.Str)
		}
	}
	return c, nil
}

func (c *conn) close() {
	if c != nil && c.nc != nil {
		_ = c.nc.Close()
	}
}

// preload fills the keyspace so that a read benchmark measures hits.
func preload(ctx context.Context, c *conn, cfg Config) error {
	value := makeValue(cfg.ValueSize, 7)
	const batch = 1000
	key := make([]byte, 0, len(keyPrefix)+24)
	for i := 0; i < cfg.KeySpace; i += batch {
		n := min(batch, cfg.KeySpace-i)
		for j := 0; j < n; j++ {
			key = key[:0]
			key = append(key, keyPrefix...)
			key = strconv.AppendInt(key, int64(i+j), 10)
			c.w.WriteCommand([]byte("SET"), key, value)
		}
		if err := c.w.Flush(); err != nil {
			return err
		}
		for j := 0; j < n; j++ {
			if err := c.r.Discard(); err != nil {
				return err
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return nil
}

// fetchInfo reads the server's INFO so the report can record what the server
// thought was happening, not just what the client saw.
func fetchInfo(c *conn) (map[string]string, error) {
	c.w.WriteCommandStrings("INFO")
	if err := c.w.Flush(); err != nil {
		return nil, err
	}
	rep, err := c.r.Read()
	if err != nil {
		return nil, err
	}
	if rep.IsError() {
		return nil, fmt.Errorf("INFO: %s", rep.Str)
	}
	return parseInfo(string(rep.Str)), nil
}

func parseInfo(s string) map[string]string {
	out := make(map[string]string, 64)
	for len(s) > 0 {
		line := s
		if i := indexByte(s, '\n'); i >= 0 {
			line, s = s[:i], s[i+1:]
		} else {
			s = ""
		}
		line = trimCR(line)
		if line == "" || line[0] == '#' {
			continue
		}
		if i := indexByte(line, ':'); i > 0 {
			out[line[:i]] = line[i+1:]
		}
	}
	return out
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

func trimCR(s string) string {
	if len(s) > 0 && s[len(s)-1] == '\r' {
		return s[:len(s)-1]
	}
	return s
}

// isExpectedStop reports errors that are the normal consequence of the run
// ending rather than a failure worth reporting.
func isExpectedStop(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed)
}
