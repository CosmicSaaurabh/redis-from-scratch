// Command server runs the redis-from-scratch node.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/clock"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/config"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/metrics"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/persist"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/server"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/stats"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store/lsm"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/store/memory"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/version"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/wal"
)

func main() {
	if err := run(); err != nil {
		// The logger may not exist yet, so the failure goes to stderr directly.
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, showVersion, err := loadConfig(os.Args[1:])
	if err != nil {
		return err
	}
	if showVersion {
		fmt.Println("redis-from-scratch", version.String())
		return nil
	}

	log := newLogger(cfg)
	slog.SetDefault(log)
	log.Info("starting",
		"version", version.String(), "addr", cfg.Addr,
		"engine", cfg.Engine, "dir", cfg.Dir,
		"appendonly", cfg.AppendOnly, "appendfsync", cfg.AppendFsync.String())

	clk := clock.System{}
	st := stats.New()

	engine, saver, closeEngine, err := openEngine(cfg, clk, log)
	if err != nil {
		return err
	}
	defer func() {
		if err := closeEngine(); err != nil {
			log.Error("closing storage engine failed", "err", err)
		}
	}()

	srv, err := server.New(server.Options{
		Config:  cfg,
		Store:   engine,
		Clock:   clk,
		Logger:  log,
		Stats:   st,
		Saver:   saver,
		RunID:   version.NewRunID(),
		Version: version.String(),
	})
	if err != nil {
		return err
	}

	var stopMetrics func(context.Context) error
	if cfg.MetricsAddr != "" {
		stopMetrics, err = metrics.Serve(cfg.MetricsAddr, metrics.Sources{
			Stats:  st,
			Store:  engine,
			Engine: saver,
			Logger: log,
		})
		if err != nil {
			return fmt.Errorf("metrics listener: %w", err)
		}
		log.Info("metrics listening", "addr", cfg.MetricsAddr, "path", "/metrics")
	}

	// Signals and the SHUTDOWN command reach the same stop path, so there is
	// exactly one shutdown sequence to reason about rather than two that can
	// drift apart.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()

	// Give the listener a moment to bind so the log line reports the real port
	// when the configured one was zero.
	select {
	case err := <-serveErr:
		if err != nil {
			return err
		}
		return nil
	case <-time.After(50 * time.Millisecond):
	}

	saveOnExit := true
	select {
	case sig := <-signals:
		log.Info("shutdown signal received", "signal", sig.String())
	case save := <-srv.ShutdownRequests():
		saveOnExit = save
		log.Info("shutdown requested by client", "save", save)
	case err := <-serveErr:
		if err != nil {
			return err
		}
		return nil
	}

	shutdownStart := time.Now()
	if err := srv.Close(); err != nil {
		log.Warn("closing listener failed", "err", err)
	}
	if stopMetrics != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := stopMetrics(ctx); err != nil {
			log.Warn("stopping metrics listener failed", "err", err)
		}
		cancel()
	}
	if !saveOnExit {
		log.Info("SHUTDOWN NOSAVE: skipping the final snapshot")
	}
	log.Info("stopped", "took", time.Since(shutdownStart).String())
	return nil
}

// openEngine builds the configured storage backend and returns it along with
// its persistence surface and a close function.
func openEngine(cfg *config.Config, clk clock.Clock, log *slog.Logger) (store.Store, server.Saver, func() error, error) {
	switch cfg.Engine {
	case config.EngineLSM:
		eng, err := lsm.Open(lsm.Options{
			Addr:    cfg.EngineAddr,
			Timeout: cfg.EngineTimeout,
			Logger:  log,
		})
		if err != nil {
			return nil, nil, nil, fmt.Errorf("connect to storage engine at %s: %w", cfg.EngineAddr, err)
		}
		return eng, eng, eng.Close, nil

	case config.EngineMemory:
		if !cfg.AppendOnly && len(cfg.SavePoints) == 0 {
			// A pure cache. This is a legitimate configuration, but it is
			// stated loudly because the difference between it and the durable
			// one is invisible until the first restart.
			log.Warn("persistence is disabled: all data will be lost on restart")
			mem := memory.New(clk, nil)
			return mem, nil, mem.Close, nil
		}
		policy := cfg.AppendFsync
		if !cfg.AppendOnly {
			policy = wal.SyncNever
		}
		eng, rec, err := persist.Open(persist.Options{
			Dir:         cfg.Dir,
			SyncPolicy:  policy,
			SegmentSize: cfg.SegmentSize,
			SavePoints:  cfg.SavePoints,
			Clock:       clk,
			Logger:      log,
		})
		if err != nil {
			return nil, nil, nil, fmt.Errorf("open data directory %s: %w", cfg.Dir, err)
		}
		logRecovery(log, rec)
		return eng, eng, eng.Close, nil

	default:
		return nil, nil, nil, fmt.Errorf("unknown storage engine %q", cfg.Engine)
	}
}

func logRecovery(log *slog.Logger, rec persist.Recovery) {
	log.Info("recovery complete",
		"snapshot_loaded", rec.SnapshotLoaded,
		"snapshot_keys", rec.SnapshotKeys,
		"log_records_applied", rec.LogApplied,
		"log_segments", rec.LogSegments,
		"log_bytes", rec.LogBytes,
		"took", rec.Elapsed.String())
	if rec.Truncated {
		// This is the single most important line the server ever logs: it
		// means the previous process died mid-write and recovery discarded an
		// incomplete record. Expected after a crash, alarming otherwise.
		log.Warn("discarded an incomplete record at the end of the write-ahead log",
			"path", rec.TruncatedPath, "offset", rec.TruncatedAt,
			"detail", "this is normal after a crash; the record was never acknowledged to a client")
	}
}

func loadConfig(argv []string) (cfg *config.Config, showVersion bool, err error) {
	cfg = config.Default()

	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	var (
		configFile = fs.String("config", "", "path to a Redis-style config file")
		addr       = fs.String("addr", "", "RESP listen address (default :6379)")
		dir        = fs.String("dir", "", "data directory")
		engine     = fs.String("engine", "", "storage engine: memory or lsm")
		engineAddr = fs.String("engine-addr", "", "gRPC address of the Rust LSM engine")
		fsyncMode  = fs.String("appendfsync", "", "write-ahead log fsync policy: always, everysec or no")
		appendOnly = fs.String("appendonly", "", "enable the write-ahead log: yes or no")
		metricsTo  = fs.String("metrics-addr", "", "Prometheus listen address, empty to disable")
		logLevel   = fs.String("loglevel", "", "debug, info, warn or error")
		logFormat  = fs.String("logformat", "", "text or json")
		maxClients = fs.Int("maxclients", 0, "maximum concurrent connections")
		noSave     = fs.Bool("no-save", false, "disable scheduled snapshots")
		ver        = fs.Bool("version", false, "print the version and exit")
	)
	if err := fs.Parse(argv); err != nil {
		return nil, false, err
	}
	if *ver {
		return cfg, true, nil
	}

	// The file is applied first so that flags override it, which is the order
	// operators expect when they add a flag to debug a running configuration.
	if *configFile != "" {
		if err := cfg.LoadFile(*configFile); err != nil {
			return nil, false, err
		}
	}
	if err := applyEnv(cfg); err != nil {
		return nil, false, err
	}

	if *addr != "" {
		cfg.Addr = *addr
	}
	if *dir != "" {
		cfg.Dir = *dir
	}
	if *engine != "" {
		cfg.Engine = config.EngineKind(*engine)
	}
	if *engineAddr != "" {
		cfg.EngineAddr = *engineAddr
	}
	if *fsyncMode != "" {
		p, err := wal.ParseSyncPolicy(*fsyncMode)
		if err != nil {
			return nil, false, err
		}
		cfg.AppendFsync = p
	}
	if *appendOnly != "" {
		cfg.AppendOnly = isYes(*appendOnly)
	}
	if *metricsTo != "" {
		cfg.MetricsAddr = strings.TrimSpace(*metricsTo)
		if cfg.MetricsAddr == "off" || cfg.MetricsAddr == "none" {
			cfg.MetricsAddr = ""
		}
	}
	if *logLevel != "" {
		cfg.LogLevel = *logLevel
	}
	if *logFormat != "" {
		cfg.LogFormat = *logFormat
	}
	if *maxClients > 0 {
		cfg.MaxClients = *maxClients
	}
	if *noSave {
		cfg.SavePoints = nil
	}

	if err := cfg.Validate(); err != nil {
		return nil, false, err
	}
	return cfg, false, nil
}

// applyEnv reads RFS_-prefixed environment variables, which is how the
// container image is configured without baking a config file into it.
func applyEnv(cfg *config.Config) error {
	for env, apply := range map[string]func(string) error{
		"RFS_ADDR":         func(v string) error { cfg.Addr = v; return nil },
		"RFS_DIR":          func(v string) error { cfg.Dir = v; return nil },
		"RFS_ENGINE":       func(v string) error { cfg.Engine = config.EngineKind(v); return nil },
		"RFS_ENGINE_ADDR":  func(v string) error { cfg.EngineAddr = v; return nil },
		"RFS_METRICS_ADDR": func(v string) error { cfg.MetricsAddr = v; return nil },
		"RFS_LOGLEVEL":     func(v string) error { cfg.LogLevel = v; return nil },
		"RFS_LOGFORMAT":    func(v string) error { cfg.LogFormat = v; return nil },
		"RFS_REQUIREPASS":  func(v string) error { cfg.RequirePass = v; return nil },
		"RFS_APPENDONLY":   func(v string) error { cfg.AppendOnly = isYes(v); return nil },
		"RFS_APPENDFSYNC": func(v string) error {
			p, err := wal.ParseSyncPolicy(v)
			if err != nil {
				return err
			}
			cfg.AppendFsync = p
			return nil
		},
	} {
		if v, ok := os.LookupEnv(env); ok {
			if err := apply(v); err != nil {
				return fmt.Errorf("%s: %w", env, err)
			}
		}
	}
	return nil
}

func newLogger(cfg *config.Config) *slog.Logger {
	level := slog.LevelInfo
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	opts := &slog.HandlerOptions{Level: level}
	if cfg.LogFormat == "json" {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}

func isYes(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "yes", "y", "true", "1", "on":
		return true
	}
	return false
}

var _ = errors.Is
