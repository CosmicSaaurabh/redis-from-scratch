// Package config holds server configuration.
//
// Values arrive from three places, later ones overriding earlier: a Redis-style
// config file, environment variables prefixed RFS_, and command line flags.
// A subset is mutable at runtime through CONFIG SET and is therefore read
// under a lock rather than copied at startup.
package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/persist"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/resp"
	"github.com/CosmicSaaurabh/redis-from-scratch/internal/wal"
)

// EngineKind selects the storage backend.
type EngineKind string

const (
	// EngineMemory keeps data in process, durable through the Go write-ahead
	// log and snapshots.
	EngineMemory EngineKind = "memory"
	// EngineLSM delegates storage to the Rust LSM engine over gRPC, which owns
	// its own log, memtable and SSTables.
	EngineLSM EngineKind = "lsm"
)

// Config is the full server configuration.
type Config struct {
	// Addr is the RESP listen address.
	Addr string
	// MetricsAddr serves Prometheus metrics and pprof. Empty disables it.
	MetricsAddr string
	// MaxClients bounds concurrent connections. Beyond it, new connections are
	// rejected with an error rather than queued, because an unbounded accept
	// queue turns a load spike into an out-of-memory kill.
	MaxClients int
	// ReadTimeout bounds how long a connection may stall mid-command. It is
	// not an idle timeout: it is armed only once a command has started
	// arriving, so an idle-but-healthy client is never disconnected.
	ReadTimeout time.Duration
	// WriteTimeout bounds a blocked reply write, which is what stops a client
	// that stops reading from pinning a goroutine and its buffers forever.
	WriteTimeout time.Duration
	// IdleTimeout closes connections with no traffic at all. Zero disables it.
	IdleTimeout time.Duration
	// ShutdownGrace is how long a graceful stop waits for clients to finish.
	ShutdownGrace time.Duration
	// TCPKeepAlive sets the keepalive period on accepted connections.
	TCPKeepAlive time.Duration
	// ReplyBufferSize is the per-connection reply buffer.
	ReplyBufferSize int

	// RequirePass enables AUTH. Empty means no authentication.
	RequirePass string

	// Engine selects the storage backend.
	Engine EngineKind
	// Dir is the data directory.
	Dir string
	// EngineAddr is the Rust LSM engine's gRPC address, used when Engine is
	// EngineLSM.
	EngineAddr string
	// EngineTimeout bounds a single gRPC call to the storage engine.
	EngineTimeout time.Duration

	// AppendOnly enables the write-ahead log. Disabling it makes the server a
	// pure cache: fast, and honest about losing everything on restart.
	AppendOnly bool
	// AppendFsync is the log's fsync discipline.
	AppendFsync wal.SyncPolicy
	// SegmentSize is the log rotation threshold in bytes.
	SegmentSize int64
	// SavePoints schedule background snapshots.
	SavePoints []persist.SavePoint

	// ActiveExpireEnabled controls the background expiry cycle.
	ActiveExpireEnabled bool
	// ActiveExpireInterval is how often the cycle runs.
	ActiveExpireInterval time.Duration
	// ActiveExpireSample is how many volatile keys each cycle inspects.
	ActiveExpireSample int
	// ActiveExpireThreshold is the fraction of a sample that must be expired
	// for the cycle to immediately run again, which is what lets it drain a
	// large batch of simultaneous expiries without a fixed rate limit.
	ActiveExpireThreshold float64

	// Limits bound what a single client can make the server allocate.
	Limits resp.Limits

	// LogLevel is one of debug, info, warn, error.
	LogLevel string
	// LogFormat is text or json.
	LogFormat string

	// mu guards the runtime-mutable subset reached through CONFIG SET.
	mu      sync.RWMutex
	mutable map[string]string
}

// Default returns the built-in configuration.
func Default() *Config {
	return &Config{
		Addr:            ":6379",
		MetricsAddr:     ":9121",
		MaxClients:      10000,
		ReadTimeout:     30 * time.Second,
		WriteTimeout:    30 * time.Second,
		IdleTimeout:     0,
		ShutdownGrace:   10 * time.Second,
		TCPKeepAlive:    5 * time.Minute,
		ReplyBufferSize: 16 << 10,

		Engine:        EngineMemory,
		Dir:           "./data",
		EngineAddr:    "127.0.0.1:50051",
		EngineTimeout: 5 * time.Second,

		AppendOnly:  true,
		AppendFsync: wal.SyncEverySec,
		SegmentSize: 64 << 20,
		SavePoints: []persist.SavePoint{
			{After: 900 * time.Second, Changes: 1},
			{After: 300 * time.Second, Changes: 10},
			{After: 60 * time.Second, Changes: 10000},
		},

		ActiveExpireEnabled:   true,
		ActiveExpireInterval:  100 * time.Millisecond,
		ActiveExpireSample:    20,
		ActiveExpireThreshold: 0.25,

		Limits:    resp.DefaultLimits(),
		LogLevel:  "info",
		LogFormat: "text",
		mutable:   map[string]string{},
	}
}

// Get returns a runtime-visible parameter and whether it is known.
func (c *Config) Get(name string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if v, ok := c.mutable[strings.ToLower(name)]; ok {
		return v, true
	}
	v, ok := c.snapshotLocked()[strings.ToLower(name)]
	return v, ok
}

// All returns every runtime-visible parameter.
func (c *Config) All() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := c.snapshotLocked()
	for k, v := range c.mutable {
		out[k] = v
	}
	return out
}

// ErrNotMutable reports a parameter that exists but cannot change at runtime.
var ErrNotMutable = errors.New("parameter is not modifiable at runtime")

// mutableParams is the allowlist for CONFIG SET. Everything outside it is
// rejected rather than silently accepted and ignored, which is the failure
// mode that makes operators distrust a CONFIG SET that returns OK.
var mutableParams = map[string]func(c *Config, v string) error{
	"appendfsync": func(c *Config, v string) error {
		p, err := wal.ParseSyncPolicy(v)
		if err != nil {
			return err
		}
		c.AppendFsync = p
		return nil
	},
	"maxclients": func(c *Config, v string) error {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return fmt.Errorf("maxclients must be a positive integer")
		}
		c.MaxClients = n
		return nil
	},
	"requirepass": func(c *Config, v string) error {
		c.RequirePass = v
		return nil
	},
	"proto-max-bulk-len": func(c *Config, v string) error {
		n, err := parseBytes(v)
		if err != nil {
			return err
		}
		c.Limits.MaxBulkSize = n
		return nil
	},
	"activeexpire": func(c *Config, v string) error {
		c.ActiveExpireEnabled = v == "yes" || v == "1" || v == "true"
		return nil
	},
	"loglevel": func(c *Config, v string) error {
		switch v {
		case "debug", "info", "warn", "error":
			c.LogLevel = v
			return nil
		}
		return fmt.Errorf("loglevel must be debug, info, warn or error")
	},
}

// Set applies a runtime configuration change.
func (c *Config) Set(name, value string) error {
	name = strings.ToLower(name)
	c.mu.Lock()
	defer c.mu.Unlock()
	apply, ok := mutableParams[name]
	if !ok {
		if _, known := c.snapshotLocked()[name]; known {
			return fmt.Errorf("%w: %s", ErrNotMutable, name)
		}
		return fmt.Errorf("unknown parameter %q", name)
	}
	if err := apply(c, value); err != nil {
		return err
	}
	c.mutable[name] = value
	return nil
}

// snapshotLocked renders the configuration as CONFIG GET sees it.
func (c *Config) snapshotLocked() map[string]string {
	yn := func(b bool) string {
		if b {
			return "yes"
		}
		return "no"
	}
	return map[string]string{
		"bind":                   c.Addr,
		"port":                   portOf(c.Addr),
		"maxclients":             strconv.Itoa(c.MaxClients),
		"timeout":                strconv.Itoa(int(c.IdleTimeout.Seconds())),
		"tcp-keepalive":          strconv.Itoa(int(c.TCPKeepAlive.Seconds())),
		"dir":                    c.Dir,
		"appendonly":             yn(c.AppendOnly),
		"appendfsync":            c.AppendFsync.String(),
		"save":                   formatSavePoints(c.SavePoints),
		"requirepass":            c.RequirePass,
		"proto-max-bulk-len":     strconv.FormatInt(c.Limits.MaxBulkSize, 10),
		"databases":              "1",
		"storage-engine":         string(c.Engine),
		"wal-segment-size":       strconv.FormatInt(c.SegmentSize, 10),
		"activeexpire":           yn(c.ActiveExpireEnabled),
		"loglevel":               c.LogLevel,
		"maxmemory":              "0",
		"maxmemory-policy":       "noeviction",
		"appendfilename":         "wal",
		"dbfilename":             "dump.rfs",
		"list-max-listpack-size": "128",
	}
}

func portOf(addr string) string {
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		return addr[i+1:]
	}
	return addr
}

func formatSavePoints(sps []persist.SavePoint) string {
	parts := make([]string, 0, len(sps))
	for _, sp := range sps {
		parts = append(parts, fmt.Sprintf("%d %d", int(sp.After.Seconds()), sp.Changes))
	}
	return strings.Join(parts, " ")
}

// LoadFile applies a Redis-style config file: one "directive value..." per
// line, # for comments.
func (c *Config) LoadFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("config: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)
	line := 0
	var saves []persist.SavePoint
	sawSave := false
	for sc.Scan() {
		line++
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		fields := strings.Fields(text)
		key := strings.ToLower(fields[0])
		args := fields[1:]
		if key == "save" {
			sawSave = true
			if len(args) == 2 {
				after, err1 := strconv.Atoi(args[0])
				changes, err2 := strconv.ParseUint(args[1], 10, 64)
				if err1 != nil || err2 != nil {
					return fmt.Errorf("config: %s:%d: save wants two integers", path, line)
				}
				saves = append(saves, persist.SavePoint{After: time.Duration(after) * time.Second, Changes: changes})
			}
			// "save" with no arguments disables snapshotting, matching Redis.
			continue
		}
		if err := c.applyDirective(key, args); err != nil {
			return fmt.Errorf("config: %s:%d: %w", path, line, err)
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("config: read %s: %w", path, err)
	}
	if sawSave {
		c.SavePoints = saves
	}
	return nil
}

func (c *Config) applyDirective(key string, args []string) error {
	arg := func() string {
		if len(args) == 0 {
			return ""
		}
		return args[0]
	}
	switch key {
	case "bind", "addr":
		c.Addr = arg()
	case "port":
		c.Addr = ":" + arg()
	case "dir":
		c.Dir = arg()
	case "maxclients":
		n, err := strconv.Atoi(arg())
		if err != nil {
			return fmt.Errorf("maxclients: %w", err)
		}
		c.MaxClients = n
	case "timeout":
		n, err := strconv.Atoi(arg())
		if err != nil {
			return fmt.Errorf("timeout: %w", err)
		}
		c.IdleTimeout = time.Duration(n) * time.Second
	case "tcp-keepalive":
		n, err := strconv.Atoi(arg())
		if err != nil {
			return fmt.Errorf("tcp-keepalive: %w", err)
		}
		c.TCPKeepAlive = time.Duration(n) * time.Second
	case "requirepass":
		c.RequirePass = arg()
	case "appendonly":
		c.AppendOnly = isYes(arg())
	case "appendfsync":
		p, err := wal.ParseSyncPolicy(arg())
		if err != nil {
			return err
		}
		c.AppendFsync = p
	case "storage-engine":
		switch EngineKind(arg()) {
		case EngineMemory:
			c.Engine = EngineMemory
		case EngineLSM:
			c.Engine = EngineLSM
		default:
			return fmt.Errorf("storage-engine must be memory or lsm")
		}
	case "engine-addr":
		c.EngineAddr = arg()
	case "wal-segment-size":
		n, err := parseBytes(arg())
		if err != nil {
			return fmt.Errorf("wal-segment-size: %w", err)
		}
		c.SegmentSize = n
	case "proto-max-bulk-len":
		n, err := parseBytes(arg())
		if err != nil {
			return fmt.Errorf("proto-max-bulk-len: %w", err)
		}
		c.Limits.MaxBulkSize = n
	case "metrics-addr":
		c.MetricsAddr = arg()
	case "loglevel":
		c.LogLevel = arg()
	case "logformat":
		c.LogFormat = arg()
	case "activeexpire":
		c.ActiveExpireEnabled = isYes(arg())
	case "databases":
		if arg() != "1" {
			return fmt.Errorf("this server implements a single database; see docs/adr/ADR-004")
		}
	default:
		return fmt.Errorf("unknown directive %q", key)
	}
	return nil
}

func isYes(s string) bool {
	switch strings.ToLower(s) {
	case "yes", "y", "true", "1", "on":
		return true
	}
	return false
}

// parseBytes accepts a plain integer or a size with a kb/mb/gb suffix.
func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "kb"):
		mult, s = 1<<10, strings.TrimSuffix(s, "kb")
	case strings.HasSuffix(s, "mb"):
		mult, s = 1<<20, strings.TrimSuffix(s, "mb")
	case strings.HasSuffix(s, "gb"):
		mult, s = 1<<30, strings.TrimSuffix(s, "gb")
	case strings.HasSuffix(s, "k"):
		mult, s = 1000, strings.TrimSuffix(s, "k")
	case strings.HasSuffix(s, "m"):
		mult, s = 1000_000, strings.TrimSuffix(s, "m")
	case strings.HasSuffix(s, "g"):
		mult, s = 1000_000_000, strings.TrimSuffix(s, "g")
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("not a byte size: %q", s)
	}
	if n < 0 {
		return 0, fmt.Errorf("byte size must not be negative")
	}
	return n * mult, nil
}

// Validate rejects configurations that would fail confusingly later.
func (c *Config) Validate() error {
	if c.Addr == "" {
		return errors.New("config: listen address is empty")
	}
	if c.MaxClients <= 0 {
		return errors.New("config: maxclients must be positive")
	}
	if c.Engine != EngineMemory && c.Engine != EngineLSM {
		return fmt.Errorf("config: unknown storage engine %q", c.Engine)
	}
	if c.Engine == EngineLSM && c.EngineAddr == "" {
		return errors.New("config: storage-engine lsm requires engine-addr")
	}
	if c.Limits.MaxBulkSize <= 0 || c.Limits.MaxMultiBulkLen <= 0 {
		return errors.New("config: protocol limits must be positive")
	}
	if c.ActiveExpireSample <= 0 {
		return errors.New("config: activeexpire sample size must be positive")
	}
	return nil
}
