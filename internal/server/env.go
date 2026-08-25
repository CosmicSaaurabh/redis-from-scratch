package server

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"time"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/clock"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/command"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/persist"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/stats"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
)

// env adapts the Server to the narrow surface command handlers are allowed to
// reach. It is a defined type over Server rather than a wrapper struct so the
// adaptation costs nothing at runtime and cannot drift out of sync with the
// server's own state.
type env Server

func (e *env) srv() *Server { return (*Server)(e) }

func (e *env) Store() store.Store  { return e.store }
func (e *env) Clock() clock.Clock  { return e.clk }
func (e *env) Stats() *stats.Stats { return e.stat }
func (e *env) RunID() string       { return e.runID }
func (e *env) Version() string     { return e.version }
func (e *env) RequirePass() string { return e.cfg.RequirePass }

func (e *env) ConfigGet(name string) (string, bool) { return e.cfg.Get(name) }
func (e *env) ConfigAll() map[string]string         { return e.cfg.All() }
func (e *env) ConfigSet(name, value string) error   { return e.cfg.Set(name, value) }

// errNoPersistence is returned by the save commands when the server is running
// as a pure cache. Saying so is better than replying OK and writing nothing.
var errNoPersistence = errors.New("persistence is disabled on this server")

func (e *env) Save(ctx context.Context) (string, error) {
	if e.saver == nil {
		return "", errNoPersistence
	}
	return e.saver.Snapshot(ctx)
}

// BackgroundSave starts a snapshot without blocking the caller's connection.
//
// Only one may run at a time. A second concurrent snapshot would double the
// I/O for no benefit and race on the same temporary file, so the flag is
// claimed with a compare-and-swap rather than a check followed by a set.
func (e *env) BackgroundSave(ctx context.Context) error {
	if e.saver == nil {
		return errNoPersistence
	}
	if !e.bgsaveActive.CompareAndSwap(false, true) {
		return errors.New("background save already in progress")
	}
	s := e.srv()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.bgsaveActive.Store(false)
		// The server's own context is used, not the client connection's: a
		// client that disconnects mid-BGSAVE must not abort a snapshot the
		// server has already started writing.
		if _, err := s.saver.Snapshot(s.ctx); err != nil {
			s.log.Error("background save failed", "err", err)
		}
	}()
	return nil
}

func (e *env) LastSave() int64 {
	if e.saver == nil {
		return 0
	}
	ts, _ := e.saver.LastSnapshot()
	return ts
}

func (e *env) SyncStore(ctx context.Context) error {
	if e.saver == nil {
		return nil
	}
	return e.saver.Sync(ctx)
}

func (e *env) Shutdown(save bool) {
	e.shutdownSave.Store(save)
	select {
	case e.shutdownReq <- save:
	default:
		// A shutdown is already queued. Dropping the duplicate is correct;
		// blocking here would stall the handler inside a command.
	}
}

func (e *env) Clients() []*command.Conn {
	s := e.srv()
	s.mu.RLock()
	out := make([]*command.Conn, 0, len(s.conns))
	for _, c := range s.conns {
		out = append(out, c.state)
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// KillClient marks a connection dead and closes its socket.
//
// Closing the socket is what actually stops it: the target is almost certainly
// parked in a blocking read, where it will never notice a flag. The flag is
// still set first so that a target which is mid-command finishes without
// writing a reply into a socket that is about to disappear.
func (e *env) KillClient(id uint64) bool {
	s := e.srv()
	s.mu.RLock()
	c, ok := s.conns[id]
	s.mu.RUnlock()
	if !ok {
		return false
	}
	c.state.Kill()
	c.close()
	return true
}

func (e *env) Info(sections []string) string {
	s := e.srv()
	b := command.NewInfoBuilder(sections)
	now := e.clk.Now()

	if b.Section("server") {
		b.Field("redis_version", "7.2.0-compatible")
		b.Field("rfs_version", e.version)
		b.Field("redis_mode", "standalone")
		b.Field("os", runtime.GOOS+" "+runtime.GOARCH)
		b.Field("go_version", runtime.Version())
		b.Field("process_id", strconv.Itoa(processID()))
		b.Field("run_id", e.runID)
		b.Field("tcp_port", portOf(s.Addr()))
		b.Int("uptime_in_seconds", int64(now.Sub(e.startedAt).Seconds()))
		b.Int("uptime_in_days", int64(now.Sub(e.startedAt).Hours()/24))
		b.Field("executable", executablePath())
		b.Field("storage_engine", e.store.Name())
	}

	if b.Section("clients") {
		b.Int("connected_clients", e.stat.ConnectionsActive.Load())
		b.Int("maxclients", int64(e.cfg.MaxClients))
		b.Uint("rejected_connections", e.stat.ConnectionsRejected.Load())
		b.Int("blocked_clients", 0)
	}

	if b.Section("memory") {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		st, _ := e.store.Stats(context.Background())
		b.Uint("used_memory", ms.Alloc)
		b.Field("used_memory_human", humanBytes(int64(ms.Alloc)))
		b.Uint("used_memory_rss", ms.Sys)
		b.Int("used_memory_dataset", st.MemoryBytes)
		b.Field("used_memory_dataset_human", humanBytes(st.MemoryBytes))
		b.Uint("mem_fragmentation_ratio_numerator", ms.Sys)
		b.Field("mem_allocator", "go-runtime")
		b.Uint("gc_pause_total_ns", ms.PauseTotalNs)
		b.Uint("gc_cycles", uint64(ms.NumGC))
		// Reported as an estimate on purpose: it is derived from key and value
		// lengths plus a fixed per-entry allowance, not measured from the heap.
		b.Field("used_memory_dataset_is_estimate", "1")
	}

	if b.Section("persistence") {
		e.infoPersistence(b)
	}

	if b.Section("stats") {
		st := e.stat
		b.Uint("total_connections_received", st.ConnectionsReceived.Load())
		b.Uint("total_commands_processed", st.CommandsProcessed.Load())
		b.Uint("total_commands_failed", st.CommandsFailed.Load())
		b.Uint("total_commands_rejected", st.CommandsRejected.Load())
		b.Uint("total_net_input_bytes", st.NetInputBytes.Load())
		b.Uint("total_net_output_bytes", st.NetOutputBytes.Load())
		b.Uint("keyspace_hits", st.KeyspaceHits.Load())
		b.Uint("keyspace_misses", st.KeyspaceMisses.Load())
		b.Float("keyspace_hit_ratio", hitRatio(st))
		b.Uint("expired_keys", st.ExpiredKeys.Load())
		b.Uint("protocol_errors", st.ProtocolErrors.Load())
		b.Uint("timeout_disconnects", st.TimeoutDisconnects.Load())
		b.Uint("pipeline_batches", st.PipelineBatches.Load())
		b.Float("mean_command_latency_usec", float64(st.MeanLatency().Nanoseconds())/1000)
		b.Float("max_command_latency_usec", float64(st.LatencyMaxNanos.Load())/1000)
		b.Int("instantaneous_ops_per_sec", e.opsPerSec())
	}

	if b.Section("replication") {
		// Replication arrives with the consensus phase. Reporting the truth
		// here matters: a client library that sees role:master with zero
		// replicas behaves correctly, whereas an invented replica count would
		// make WAIT and failover logic misbehave.
		b.Field("role", "master")
		b.Int("connected_slaves", 0)
		b.Field("master_replid", e.runID)
		b.Int("master_repl_offset", 0)
	}

	if b.Section("cpu") {
		b.Int("num_goroutines", int64(runtime.NumGoroutine()))
		b.Int("num_cpu", int64(runtime.NumCPU()))
		b.Int("gomaxprocs", int64(runtime.GOMAXPROCS(0)))
	}

	if b.Section("commandstats") {
		b.Field("note", "per-command breakdown is exported via Prometheus at "+e.cfg.MetricsAddr)
	}

	if b.Section("keyspace") {
		n, err := e.store.Len(context.Background())
		if err == nil && n > 0 {
			st, _ := e.store.Stats(context.Background())
			b.Field("db0", fmt.Sprintf("keys=%d,expires=%d,avg_ttl=0", n, st.VolatileKeys))
		}
	}

	return b.String()
}

func (e *env) infoPersistence(b *command.InfoBuilder) {
	b.Bool("loading", false)
	if e.saver == nil {
		b.Bool("persistence_enabled", false)
		b.Field("note", "running as a cache: data is lost on restart")
		return
	}
	b.Bool("persistence_enabled", true)
	b.Bool("rdb_bgsave_in_progress", e.bgsaveActive.Load())

	pe, ok := e.saver.(*persist.Engine)
	if !ok {
		return
	}
	ps := pe.PersistenceStats()
	b.Int("rdb_last_save_time", ps.LastSaveMs/1000)
	b.Field("rdb_last_bgsave_status", statusOf(ps.LastSaveOK))
	b.Uint("rdb_changes_since_last_save", ps.DirtyChanges)
	b.Uint("rdb_saves_total", ps.Saves)
	b.Uint("rdb_saves_failed", ps.SavesFailed)

	if ws, ok := pe.WALStats(); ok {
		b.Bool("aof_enabled", true)
		b.Field("aof_fsync_policy", ws.Policy)
		b.Uint("aof_appends", ws.Appends)
		b.Uint("aof_writes", ws.Writes)
		b.Uint("aof_fsyncs", ws.Fsyncs)
		b.Uint("aof_bytes_written", ws.BytesOut)
		b.Uint("wal_last_lsn", ws.LastLSN)
		b.Uint("wal_synced_lsn", ws.SyncedLSN)
		b.Uint("wal_segment", ws.SegmentSeq)
		// The gap between the last and the synced LSN is how many
		// acknowledged writes are not yet on stable storage. Under the always
		// policy it is zero by construction; under everysec it is the size of
		// the window a power cut would take.
		b.Uint("wal_unsynced_records", ws.LastLSN-ws.SyncedLSN)
	}

	rec := pe.Recovery()
	b.Bool("recovery_snapshot_loaded", rec.SnapshotLoaded)
	b.Uint("recovery_snapshot_keys", rec.SnapshotKeys)
	b.Int("recovery_log_records_applied", int64(rec.LogApplied))
	b.Int("recovery_log_segments_read", int64(rec.LogSegments))
	b.Field("recovery_duration", rec.Elapsed.String())
	// A truncated tail is surfaced loudly and permanently. An operator must
	// never have to infer that recovery discarded part of the log.
	b.Bool("recovery_truncated_torn_tail", rec.Truncated)
	if rec.Truncated {
		b.Field("recovery_truncated_at", fmt.Sprintf("%s:%d", rec.TruncatedPath, rec.TruncatedAt))
	}
}

// opsPerSec is a coarse rate derived from the lifetime command count. It is
// labelled instantaneous for INFO compatibility, but the honest number to use
// for capacity work is the Prometheus rate over a real window.
func (e *env) opsPerSec() int64 {
	elapsed := time.Since(e.startedAt).Seconds()
	if elapsed < 1 {
		return 0
	}
	return int64(float64(e.stat.CommandsProcessed.Load()) / elapsed)
}

func hitRatio(st *stats.Stats) float64 {
	h, m := st.KeyspaceHits.Load(), st.KeyspaceMisses.Load()
	if h+m == 0 {
		return 0
	}
	return float64(h) / float64(h+m)
}

func statusOf(ok bool) string {
	if ok {
		return "ok"
	}
	return "err"
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return strconv.FormatInt(n, 10) + "B"
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit && exp < 4; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f%c", float64(n)/float64(div), "KMGTP"[exp])
}

func portOf(addr string) string {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[i+1:]
		}
	}
	return addr
}

var _ command.Env = (*env)(nil)
