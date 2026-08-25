package bench

import (
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"sort"
	"strings"
	"time"
)

// reportedQuantiles are the latency points every report carries. p99.9 is
// included because it is where a queueing problem shows up first, and a report
// that stops at p99 hides the tail that actually pages people.
var reportedQuantiles = []float64{0.5, 0.90, 0.95, 0.99, 0.999}

// Summary is the machine-readable form of a result.
type Summary struct {
	Label        string             `json:"label"`
	Workload     string             `json:"workload"`
	Distribution string             `json:"distribution"`
	Addr         string             `json:"addr"`
	Connections  int                `json:"connections"`
	Pipeline     int                `json:"pipeline"`
	KeySpace     int                `json:"key_space"`
	ValueBytes   int                `json:"value_bytes"`
	ReadRatio    float64            `json:"read_ratio,omitempty"`
	TargetRate   int                `json:"target_rate,omitempty"`
	StartedAt    time.Time          `json:"started_at"`
	ElapsedSec   float64            `json:"elapsed_seconds"`
	Ops          uint64             `json:"ops"`
	Reads        uint64             `json:"reads"`
	Writes       uint64             `json:"writes"`
	Errors       uint64             `json:"errors"`
	OpsPerSec    float64            `json:"ops_per_sec"`
	LatencyUsec  map[string]float64 `json:"latency_usec"`
	Server       map[string]string  `json:"server,omitempty"`
	Client       ClientEnv          `json:"client"`
	Notes        []string           `json:"notes,omitempty"`
}

// ClientEnv records what generated the load, because a benchmark run on a
// laptop that was also running the server is a different measurement from one
// run across a network, and the report should not let a reader forget which
// they are looking at.
type ClientEnv struct {
	GoVersion string `json:"go_version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	NumCPU    int    `json:"num_cpu"`
	Colocated bool   `json:"colocated_with_server"`
}

// Summarise converts a result into its reportable form.
func (r *Result) Summarise() Summary {
	q := r.Hist.Quantiles(reportedQuantiles...)
	lat := map[string]float64{
		"min":  float64(r.Hist.Min()) / 1000,
		"mean": r.Hist.Mean() / 1000,
		"max":  float64(r.Hist.Max()) / 1000,
	}
	for _, p := range reportedQuantiles {
		lat[quantileName(p)] = float64(q[p]) / 1000
	}

	interesting := map[string]bool{
		"rfs_version": true, "storage_engine": true, "aof_enabled": true,
		"aof_fsync_policy": true, "aof_fsyncs": true, "aof_writes": true,
		"aof_appends": true, "wal_unsynced_records": true,
		"used_memory_human": true, "connected_clients": true,
		"total_commands_processed": true, "keyspace_hits": true,
		"keyspace_misses": true, "expired_keys": true, "db0": true,
		"redis_version": true, "pipeline_batches": true,
	}
	server := make(map[string]string)
	for k, v := range r.ServerInfo {
		if interesting[k] {
			server[k] = v
		}
	}

	return Summary{
		Label:        r.Config.Label,
		Workload:     string(r.Config.Workload),
		Distribution: string(r.Config.Distribution),
		Addr:         r.Config.Addr,
		Connections:  r.Config.Connections,
		Pipeline:     r.Config.Pipeline,
		KeySpace:     r.Config.KeySpace,
		ValueBytes:   r.Config.ValueSize,
		ReadRatio:    r.Config.ReadRatio,
		TargetRate:   r.Config.Rate,
		StartedAt:    r.StartedAt,
		ElapsedSec:   r.Elapsed.Seconds(),
		Ops:          r.Ops,
		Reads:        r.Reads,
		Writes:       r.Writes,
		Errors:       r.Errors,
		OpsPerSec:    r.AchievedRate,
		LatencyUsec:  lat,
		Server:       server,
		Client: ClientEnv{
			GoVersion: runtime.Version(),
			OS:        runtime.GOOS,
			Arch:      runtime.GOARCH,
			NumCPU:    runtime.NumCPU(),
			Colocated: isLoopback(r.Config.Addr),
		},
		Notes: r.Notes,
	}
}

func quantileName(q float64) string {
	switch q {
	case 0.5:
		return "p50"
	case 0.90:
		return "p90"
	case 0.95:
		return "p95"
	case 0.99:
		return "p99"
	case 0.999:
		return "p99.9"
	default:
		return fmt.Sprintf("p%g", q*100)
	}
}

func isLoopback(addr string) bool {
	return strings.HasPrefix(addr, "127.") || strings.HasPrefix(addr, "localhost") ||
		strings.HasPrefix(addr, "[::1]") || strings.HasPrefix(addr, ":")
}

// WriteJSON emits summaries as JSON.
func WriteJSON(w io.Writer, sums []Summary) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(sums)
}

// WriteMarkdown emits a report table plus the methodology that produced it.
func WriteMarkdown(w io.Writer, title string, sums []Summary) error {
	var b strings.Builder
	b.WriteString("# " + title + "\n\n")

	if len(sums) > 0 {
		c := sums[0].Client
		fmt.Fprintf(&b, "Generated %s on %s/%s, %d logical CPUs, %s.\n\n",
			time.Now().UTC().Format(time.RFC3339), c.OS, c.Arch, c.NumCPU, c.GoVersion)
		if c.Colocated {
			// This is the single most important caveat on any localhost
			// benchmark and it belongs above the numbers, not in a footnote.
			b.WriteString("> **The load generator ran on the same machine as the server.** " +
				"Client and server therefore competed for the same CPUs, and there was no network. " +
				"Treat these as a relative comparison between configurations, not as an absolute " +
				"capacity figure for a deployed node.\n\n")
		}
	}

	b.WriteString("| run | conns | pipe | ops/sec | p50 us | p99 us | p99.9 us | max us | errors |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, s := range sums {
		fmt.Fprintf(&b, "| %s | %d | %d | %s | %.0f | %.0f | %.0f | %.0f | %d |\n",
			s.Label, s.Connections, s.Pipeline, thousands(s.OpsPerSec),
			s.LatencyUsec["p50"], s.LatencyUsec["p99"], s.LatencyUsec["p99.9"],
			s.LatencyUsec["max"], s.Errors)
	}

	b.WriteString("\n## How to read this\n\n")
	b.WriteString("- Latency is measured client side, from the moment a request was due to be sent " +
		"until its reply was fully read.\n")
	b.WriteString("- Runs with a pipeline depth above 1 report **batch** latency, attributed to each " +
		"command in the batch. A depth of 16 at 800 us does not mean each command took 800 us; " +
		"it means a batch of 16 did.\n")
	b.WriteString("- Runs with a target rate are open loop: latency is measured from the intended send " +
		"time, so a stall shows up instead of being hidden by the client pausing with the server.\n")
	b.WriteString("- Percentiles come from a log-linear histogram of every sample, not a reservoir, " +
		"so the tail is real and not a sampling artefact. Bucket width bounds the error at 0.8%.\n")

	b.WriteString("\n## Run details\n\n")
	for _, s := range sums {
		fmt.Fprintf(&b, "### %s\n\n", s.Label)
		fmt.Fprintf(&b, "- workload `%s`, key distribution `%s`, %s keys, %d byte values\n",
			s.Workload, s.Distribution, thousands(float64(s.KeySpace)), s.ValueBytes)
		fmt.Fprintf(&b, "- %d connections, pipeline depth %d, %.1fs measured, %s operations\n",
			s.Connections, s.Pipeline, s.ElapsedSec, thousands(float64(s.Ops)))
		if s.TargetRate > 0 {
			fmt.Fprintf(&b, "- open loop, target %s ops/sec\n", thousands(float64(s.TargetRate)))
		} else {
			b.WriteString("- closed loop: each connection waits for its replies before sending again\n")
		}
		if s.Reads > 0 && s.Writes > 0 {
			fmt.Fprintf(&b, "- %s reads, %s writes\n", thousands(float64(s.Reads)), thousands(float64(s.Writes)))
		}
		if len(s.Server) > 0 {
			keys := make([]string, 0, len(s.Server))
			for k := range s.Server {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			b.WriteString("- server reported: ")
			for i, k := range keys {
				if i > 0 {
					b.WriteString(", ")
				}
				fmt.Fprintf(&b, "`%s=%s`", k, s.Server[k])
			}
			b.WriteString("\n")
		}
		for _, n := range s.Notes {
			fmt.Fprintf(&b, "- **note:** %s\n", n)
		}
		b.WriteString("\n")
	}

	_, err := io.WriteString(w, b.String())
	return err
}

// thousands renders a number with separators so a reader can tell 900000 from
// 9000000 at a glance.
func thousands(f float64) string {
	n := int64(f + 0.5)
	neg := n < 0
	if neg {
		n = -n
	}
	s := fmt.Sprintf("%d", n)
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}
