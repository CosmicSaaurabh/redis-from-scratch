# Redis From Scratch

A Redis-compatible key-value server: RESP2 and RESP3 on the wire, Go for the
network and command layers, and a Rust LSM-tree storage engine behind a
pluggable interface.

It survives `kill -9` without losing acknowledged writes, and its replies are
verified byte for byte against a real `redis-server`.

```sh
make build && make run
redis-cli -p 6379 SET hello world
redis-cli -p 6379 GET hello
```

## Where it stands

| phase | | |
|---|---|---|
| 1. Protocol and concurrency | done | RESP2/RESP3, 67 commands, goroutine per connection |
| 2. Persistence | done | write-ahead log, snapshots, crash recovery proven under SIGKILL |
| 3. Storage engine | done | Rust LSM tree over gRPC: memtable, SSTables, Bloom filters, leveled compaction |
| 4. Replication and consensus | not started | Raft |
| 5. Sharding | not started | |
| 6. Observability | partial | Prometheus, `INFO`, pprof; OpenTelemetry tracing pending |
| 7. Testing and chaos | partial | five test suites and a benchmark harness; chaos harness pending |

## Measured, not asserted

Full methodology and per-configuration reports in
[docs/benchmarks](docs/benchmarks/README.md).
Measured on a 14-core laptop with the load generator co-located, against a real
`redis-server` running with persistence disabled, using the same harness for
both.

| workload | this server | redis-server | |
|---|---:|---:|---|
| GET, unpipelined | 145,352/sec | 140,584/sec | 1.03x |
| GET, pipeline 32 | 3,487,965/sec | 1,825,344/sec | **1.91x** |
| mixed, 500 connections | 155,910/sec | 139,286/sec | **1.12x** |
| SET, unpipelined | 119,338/sec | 139,515/sec | 0.86x |

Reads and high concurrency win because Redis is single-threaded and this server
is not. Durable writes lose because they are doing work the baseline was
configured not to do:

| write configuration | SET/sec | what a crash costs |
|---|---:|---|
| persistence off | 137,881 | everything |
| WAL, fsync every second | 119,338 | up to 1s to power loss, nothing to a process kill |
| WAL, fsync before acknowledge | 3,714 | nothing that was acknowledged |

That last row is the price of a physical disk confirming a write before a client
is told it happened. It is published rather than optimised away.

## Durability, proven

`make test-crash` spawns the real binary, kills it with `SIGKILL`, restarts it
and checks what survived. Ten scenarios, including:

- 2000 acknowledged writes under `fsync always`, zero lost
- 2000 under `everysec`, zero lost, because the page cache outlives the process
- 832 acknowledged writes across eight writers killed mid-traffic, zero lost
- a torn log tail truncated with everything before it intact
- five crash-restart cycles converging on the right key count
- a snapshot plus its log tail composing correctly, including a post-snapshot
  overwrite and delete
- TTLs pinned to wall-clock instants across an outage

`scripts/smoke-lsm.sh` does the same to the Rust engine while the Go server
stays up: 500 of 500 acknowledged writes recovered.

## Compatibility, verified

`test/compat` runs the same commands against this server and a real
`redis-server` and compares replies byte for byte, error messages included.
Hand-written expectations encode what the author believes Redis does; this
encodes what it actually does.

It found four real bugs, including `resp.ParseInt` being unable to represent
`-9223372036854775808` and `GETRANGE` mishandling a doubly-negative range.

Three behaviours differ on purpose, each with an ADR:
[one database](docs/adr/ADR-004-single-database.md),
[strings only](docs/adr/ADR-003-strings-only-datatypes.md), and
[`INCRBYFLOAT` precision](docs/adr/ADR-006-incrbyfloat-precision.md), where Redis
does not agree with itself across platforms.

## Architecture

```
   Redis clients
        |  RESP2 / RESP3 over TCP
        v
  +--------------------------------------+
  |  Go node                             |
  |  accept loop -> goroutine per conn   |
  |  -> command table -> store.Store     |
  +--------------------------------------+
        |                        |
        v                        v
  in-process engine      Rust engine process
  sharded maps           gRPC -> memtable
  + write-ahead log             -> WAL
  + fuzzy snapshots             -> SSTables + compaction
```

The command layer talks to one narrow interface and does not know which engine
is behind it. That interface carries no Redis semantics at all: no `INCR`, no
`SET NX`, only reads, writes, an atomic read-modify-write and an ordered scan.

Choosing between the engines is a question about dataset size. The in-process
engine holds every key and value in memory and is three times faster per
operation; the LSM engine keeps only the memtable, index blocks and Bloom
filters resident and leaves the data on disk.

## Running it

```sh
make run                      # in-process engine, port 6379
make run-lsm                  # Go server plus the Rust engine
docker compose up             # single node
docker compose --profile lsm up   # server plus engine, two containers
```

Configuration comes from a Redis-style file, `RFS_`-prefixed environment
variables, then flags, in that order.

```sh
bin/rfs-server -dir ./data -appendfsync always -metrics-addr :9121
```

Metrics at `/metrics`, liveness at `/health`, pprof at `/debug/pprof/`.

## Testing

```sh
make test          # unit and integration, race detector on
make test-e2e      # over a real socket
make test-compat   # differential against redis-server
make test-crash    # SIGKILL the real binary
make test-rust     # 75 Rust tests
make fuzz          # protocol parser and glob matcher
make lint          # vet, golangci-lint, clippy with -D warnings
```

Every Go test that touches concurrency runs with `-race`, and a finding fails
the build: a data race that has not crashed yet is a bug that has not been
noticed yet.

## Documentation

- [Benchmarks](docs/benchmarks/README.md) with full methodology
- [HLD-001](docs/high-level-design/HLD-001-single-node-architecture.md): architecture, NFRs, failure modes, capacity
- [LLD-001](docs/low-level-design/LLD-001-storage-and-durability.md): on-disk formats, lock ordering, recovery procedures
- Architecture decisions:
  [durability model](docs/adr/ADR-001-durability-model.md) ·
  [storage boundary](docs/adr/ADR-002-pluggable-storage-boundary.md) ·
  [strings only](docs/adr/ADR-003-strings-only-datatypes.md) ·
  [single database](docs/adr/ADR-004-single-database.md) ·
  [multi-key atomicity](docs/adr/ADR-005-multi-key-atomicity-window.md) ·
  [INCRBYFLOAT precision](docs/adr/ADR-006-incrbyfloat-precision.md)
- [Learning log](docs/learning-log.md)

## Known limitations

- No replication, no consensus, no sharding. Phases 4 and 5.
- `RENAME`, `COPY` and `MSETNX` are not atomic across keys under concurrent
  writes ([ADR-005](docs/adr/ADR-005-multi-key-atomicity-window.md)).
  Single-key compare-and-set is atomic, so `SET NX` remains a correct lock.
- `DBSIZE` against the LSM engine is an estimate, and says so in `INFO`.
- No `maxmemory` policy and no eviction. The server will use all of it.
- The LSM engine's open-table cache is unbounded.
- No `MULTI`/`EXEC`. It needs a cross-key ordering point, which arrives with the
  replicated log rather than before it.

## Repository layout

```
cmd/server        the node
cmd/rfs-bench     load generator
internal/         resp, command, server, store, wal, snapshot, persist, bench
engine/           the Rust LSM crate and its gRPC service
proto/            the contract between the two languages
test/             e2e, compat, crash
docs/             benchmarks, HLD, LLD, ADRs, learning log
scripts/          benchmark suite, full-stack smoke test
```
