// Package metrics exposes the server's counters over Prometheus.
//
// Nothing here is touched on the command path. The server keeps plain atomics
// in the stats package, and this package reads them when a scrape arrives,
// which moves the cost of observability from once per command to once every
// scrape interval. The Prometheus collector interface is designed for exactly
// this and calling it "pull-based instrumentation" is the whole reason it is a
// good fit for a hot path.
package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/persist"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/stats"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
)

// Sources are the objects the collector reads on each scrape.
type Sources struct {
	Stats  *stats.Stats
	Store  store.Store
	Engine any
	Logger *slog.Logger
}

// Serve starts an HTTP listener exposing /metrics, /health and pprof.
// It returns a shutdown function.
func Serve(addr string, src Sources) (func(context.Context) error, error) {
	if src.Logger == nil {
		src.Logger = slog.Default()
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		&collector{src: src},
	)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	}))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		// Liveness only. It deliberately does not touch the store: a readiness
		// probe that fails during a slow snapshot would restart a healthy node
		// in the middle of the one operation that must not be interrupted.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			src.Logger.Error("metrics server stopped", "err", err)
		}
	}()
	return srv.Shutdown, nil
}

// collector renders the server's state as Prometheus metrics on demand.
type collector struct{ src Sources }

var (
	descCommands = prometheus.NewDesc("rfs_commands_total",
		"Commands processed, by outcome.", []string{"outcome"}, nil)
	descConnections = prometheus.NewDesc("rfs_connections_total",
		"Connections, by outcome.", []string{"outcome"}, nil)
	descConnectionsActive = prometheus.NewDesc("rfs_connections_active",
		"Currently open client connections.", nil, nil)
	descNetBytes = prometheus.NewDesc("rfs_net_bytes_total",
		"Bytes over the client protocol, by direction.", []string{"direction"}, nil)
	descKeyspace = prometheus.NewDesc("rfs_keyspace_lookups_total",
		"Keyspace lookups, by result.", []string{"result"}, nil)
	descExpired = prometheus.NewDesc("rfs_expired_keys_total",
		"Keys reclaimed because their TTL elapsed.", nil, nil)
	descProtocolErrors = prometheus.NewDesc("rfs_protocol_errors_total",
		"Connections dropped because framing was lost.", nil, nil)
	descTimeouts = prometheus.NewDesc("rfs_timeout_disconnects_total",
		"Connections dropped for stalling.", nil, nil)
	descPipelines = prometheus.NewDesc("rfs_pipeline_batches_total",
		"Reply flushes that served more than one command.", nil, nil)
	descLatencyMean = prometheus.NewDesc("rfs_command_latency_mean_seconds",
		"Mean command service time since the last CONFIG RESETSTAT.", nil, nil)
	descLatencyMax = prometheus.NewDesc("rfs_command_latency_max_seconds",
		"Slowest command since the last CONFIG RESETSTAT.", nil, nil)
	descUptime = prometheus.NewDesc("rfs_uptime_seconds", "Process uptime.", nil, nil)

	descKeys = prometheus.NewDesc("rfs_keys",
		"Keys in the keyspace.", nil, nil)
	descVolatileKeys = prometheus.NewDesc("rfs_keys_volatile",
		"Keys carrying a TTL.", nil, nil)
	descDatasetBytes = prometheus.NewDesc("rfs_dataset_bytes_estimate",
		"Estimated dataset size. Derived from key and value lengths, not measured from the heap.", nil, nil)

	descWALRecords = prometheus.NewDesc("rfs_wal_records_total",
		"Write-ahead log records appended.", nil, nil)
	descWALWrites = prometheus.NewDesc("rfs_wal_writes_total",
		"Write syscalls issued to the write-ahead log.", nil, nil)
	descWALFsyncs = prometheus.NewDesc("rfs_wal_fsyncs_total",
		"fsync calls issued to the write-ahead log.", nil, nil)
	descWALBytes = prometheus.NewDesc("rfs_wal_bytes_total",
		"Bytes written to the write-ahead log.", nil, nil)
	descWALUnsynced = prometheus.NewDesc("rfs_wal_unsynced_records",
		"Acknowledged records not yet on stable storage. This is the exact size of the window a power cut would lose.",
		nil, nil)
	descSaves = prometheus.NewDesc("rfs_snapshots_total",
		"Snapshots, by outcome.", []string{"outcome"}, nil)
	descDirty = prometheus.NewDesc("rfs_changes_since_snapshot",
		"Mutations accumulated since the last snapshot.", nil, nil)
	descLastSave = prometheus.NewDesc("rfs_last_snapshot_timestamp_seconds",
		"When the last successful snapshot finished.", nil, nil)
	descTornTail = prometheus.NewDesc("rfs_recovery_truncated_torn_tail",
		"1 when the last recovery discarded an incomplete record from the end of the log.", nil, nil)
)

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, ch)
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	s := c.src.Stats
	counter := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v, labels...)
	}
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}

	if s != nil {
		counter(descCommands, float64(s.CommandsProcessed.Load()), "ok")
		counter(descCommands, float64(s.CommandsFailed.Load()), "error")
		counter(descCommands, float64(s.CommandsRejected.Load()), "rejected")
		counter(descConnections, float64(s.ConnectionsReceived.Load()), "accepted")
		counter(descConnections, float64(s.ConnectionsRejected.Load()), "rejected")
		gauge(descConnectionsActive, float64(s.ConnectionsActive.Load()))
		counter(descNetBytes, float64(s.NetInputBytes.Load()), "in")
		counter(descNetBytes, float64(s.NetOutputBytes.Load()), "out")
		counter(descKeyspace, float64(s.KeyspaceHits.Load()), "hit")
		counter(descKeyspace, float64(s.KeyspaceMisses.Load()), "miss")
		counter(descExpired, float64(s.ExpiredKeys.Load()))
		counter(descProtocolErrors, float64(s.ProtocolErrors.Load()))
		counter(descTimeouts, float64(s.TimeoutDisconnects.Load()))
		counter(descPipelines, float64(s.PipelineBatches.Load()))
		gauge(descLatencyMean, s.MeanLatency().Seconds())
		gauge(descLatencyMax, float64(s.LatencyMaxNanos.Load())/1e9)
		gauge(descUptime, float64(s.UptimeSeconds()))
	}

	if c.src.Store != nil {
		// A scrape must never be able to wedge on a slow engine, so the store
		// lookup carries its own short deadline rather than inheriting the
		// HTTP request's.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		st, err := c.src.Store.Stats(ctx)
		cancel()
		if err == nil {
			gauge(descKeys, float64(st.Keys))
			gauge(descVolatileKeys, float64(st.VolatileKeys))
			gauge(descDatasetBytes, float64(st.MemoryBytes))
		}
	}

	if pe, ok := c.src.Engine.(*persist.Engine); ok && pe != nil {
		if ws, ok := pe.WALStats(); ok {
			counter(descWALRecords, float64(ws.Appends))
			counter(descWALWrites, float64(ws.Writes))
			counter(descWALFsyncs, float64(ws.Fsyncs))
			counter(descWALBytes, float64(ws.BytesOut))
			gauge(descWALUnsynced, float64(ws.LastLSN-ws.SyncedLSN))
		}
		ps := pe.PersistenceStats()
		counter(descSaves, float64(ps.Saves), "ok")
		counter(descSaves, float64(ps.SavesFailed), "error")
		gauge(descDirty, float64(ps.DirtyChanges))
		gauge(descLastSave, float64(ps.LastSaveMs)/1000)
		gauge(descTornTail, boolToFloat(pe.Recovery().Truncated))
	}
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
