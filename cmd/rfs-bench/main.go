// Command rfs-bench generates load against a RESP server and reports
// throughput and latency with the methodology attached.
//
// It works against this server or against real Redis, which is the point: a
// number with no baseline beside it is not evidence of anything.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/bench"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "rfs-bench: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		addr      = flag.String("addr", "127.0.0.1:6379", "server address")
		password  = flag.String("password", "", "AUTH password")
		conns     = flag.Int("c", 50, "concurrent connections")
		pipeline  = flag.Int("P", 1, "pipeline depth")
		duration  = flag.Duration("d", 10*time.Second, "measured duration")
		requests  = flag.Int("n", 0, "fixed operation count, overrides -d")
		warmup    = flag.Duration("warmup", 2*time.Second, "warmup duration, discarded")
		rate      = flag.Int("rate", 0, "open-loop target ops/sec, 0 for closed loop")
		keySpace  = flag.Int("keys", 100_000, "distinct keys")
		valueSize = flag.Int("value", 64, "value size in bytes")
		readRatio = flag.Float64("read-ratio", 0.5, "fraction of reads in the mixed workload")
		workload  = flag.String("workload", "mixed", "get, set, mixed, incr or ping")
		dist      = flag.String("dist", "uniform", "uniform, zipf or sequential")
		preload   = flag.Bool("preload", true, "write the keyspace before measuring")
		label     = flag.String("label", "", "name for this run in the report")
		suite     = flag.String("suite", "", "run a named suite instead of a single run: standard or durability")
		jsonOut   = flag.String("json", "", "write the summary as JSON to this path")
		mdOut     = flag.String("markdown", "", "write a Markdown report to this path")
		title     = flag.String("title", "Benchmark", "report title")
	)
	flag.Parse()

	wl, err := bench.ParseWorkload(*workload)
	if err != nil {
		return err
	}
	d, err := bench.ParseDistribution(*dist)
	if err != nil {
		return err
	}

	base := bench.Config{
		Addr: *addr, Password: *password,
		Connections: *conns, Pipeline: *pipeline,
		Duration: *duration, Requests: *requests, Warmup: *warmup, Rate: *rate,
		KeySpace: *keySpace, ValueSize: *valueSize, ReadRatio: *readRatio,
		Workload: wl, Distribution: d, Preload: *preload, Label: *label,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runs, err := plan(*suite, base)
	if err != nil {
		return err
	}

	sums := make([]bench.Summary, 0, len(runs))
	for i, cfg := range runs {
		if ctx.Err() != nil {
			break
		}
		fmt.Fprintf(os.Stderr, "[%d/%d] %s: %d conns, pipeline %d ... ",
			i+1, len(runs), cfg.Label, cfg.Connections, cfg.Pipeline)
		res, err := bench.Run(ctx, cfg)
		if err != nil {
			return fmt.Errorf("run %q: %w", cfg.Label, err)
		}
		s := res.Summarise()
		sums = append(sums, s)
		fmt.Fprintf(os.Stderr, "%.0f ops/sec, p50 %.0fus, p99 %.0fus\n",
			s.OpsPerSec, s.LatencyUsec["p50"], s.LatencyUsec["p99"])
	}

	if *jsonOut != "" {
		f, err := os.Create(*jsonOut)
		if err != nil {
			return err
		}
		if err := bench.WriteJSON(f, sums); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	if *mdOut != "" {
		f, err := os.Create(*mdOut)
		if err != nil {
			return err
		}
		if err := bench.WriteMarkdown(f, *title, sums); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "wrote %s\n", *mdOut)
	}
	if *jsonOut == "" && *mdOut == "" {
		return bench.WriteMarkdown(os.Stdout, *title, sums)
	}
	return nil
}

// plan expands a suite name into the list of runs it covers.
func plan(suite string, base bench.Config) ([]bench.Config, error) {
	switch suite {
	case "":
		if base.Label == "" {
			base.Label = fmt.Sprintf("%s c=%d P=%d", base.Workload, base.Connections, base.Pipeline)
		}
		return []bench.Config{base}, nil

	case "standard":
		// A sweep rather than a single number: one configuration proves
		// nothing, and the shape across concurrency and pipeline depth is what
		// actually says whether a server scales.
		var out []bench.Config
		for _, wl := range []bench.Workload{bench.WorkloadGet, bench.WorkloadSet, bench.WorkloadMixed} {
			for _, p := range []int{1, 8, 32} {
				c := base
				c.Workload = wl
				c.Pipeline = p
				c.Preload = wl != bench.WorkloadSet
				c.Label = fmt.Sprintf("%s P=%d", wl, p)
				out = append(out, c)
			}
		}
		for _, conns := range []int{10, 50, 200, 500} {
			c := base
			c.Workload = bench.WorkloadMixed
			c.Pipeline = 1
			c.Connections = conns
			c.Label = fmt.Sprintf("mixed c=%d", conns)
			out = append(out, c)
		}
		return out, nil

	case "latency":
		// Open loop at fixed rates. This is the only mode whose latency
		// figures survive contact with a stall.
		var out []bench.Config
		for _, r := range []int{10_000, 50_000, 100_000, 200_000} {
			c := base
			c.Workload = bench.WorkloadMixed
			c.Pipeline = 1
			c.Rate = r
			c.Label = fmt.Sprintf("open-loop %s ops/sec", thousandsInt(r))
			out = append(out, c)
		}
		return out, nil

	default:
		return nil, fmt.Errorf("unknown suite %q (want standard or latency)", suite)
	}
}

func thousandsInt(n int) string {
	s := strconv.Itoa(n)
	var parts []string
	for len(s) > 3 {
		parts = append([]string{s[len(s)-3:]}, parts...)
		s = s[:len(s)-3]
	}
	return strings.Join(append([]string{s}, parts...), ",")
}
