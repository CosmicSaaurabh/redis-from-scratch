# Redis From Scratch

A Redis-compatible key-value server, built from the socket up.

RESP2 and RESP3 on the wire. Go for networking and the command layer. A Rust
LSM-tree storage engine behind a pluggable interface. It survives `kill -9`
without losing acknowledged writes, and its replies are checked byte for byte
against a real `redis-server`.

```sh
make build && make run
redis-cli -p 6379 SET hello world
redis-cli -p 6379 GET hello
```

Any Redis client works unmodified, including `redis-cli` and `redis-benchmark`.

---

## What is actually built

| phase | status | |
|---|---|---|
| 1. Protocol and concurrency | **done** | RESP2/RESP3, 67 commands, goroutine per connection |
| 2. Persistence | **done** | write-ahead log, fuzzy snapshots, crash recovery |
| 3. Storage engine | **done** | Rust LSM tree over gRPC: memtable, SSTables, Bloom filters, leveled compaction |
| 4. Replication and consensus | not started | Raft |
| 5. Sharding | not started | |
| 6. Observability | partial | Prometheus, `INFO`, pprof; OpenTelemetry pending |
| 7. Testing and chaos | partial | 5 test suites, benchmark harness; chaos harness pending |

Roughly 17,000 lines of Go and 6,000 of Rust, with 104 Go test functions and 75
Rust tests.

---

# The proofs

Three claims, each with the command that checks it. Nothing below is asserted
in a comment.

## 1. Throughput

Measured by `scripts/bench-suite.sh` on a 14-core laptop, load generator
co-located, against a real `redis-server` running with **persistence disabled**
(the fastest it can be configured), using the same harness for both.

| workload | this server | redis-server | |
|---|---:|---:|---|
| GET, unpipelined | 145,352/sec | 140,584/sec | 1.03x |
| GET, pipeline 8 | 1,195,258/sec | 861,118/sec | 1.39x |
| **GET, pipeline 32** | **3,487,965/sec** | 1,825,344/sec | **1.91x** |
| mixed, 50 connections | 134,008/sec | 134,240/sec | 1.00x |
| **mixed, 500 connections** | **155,910/sec** | 139,286/sec | **1.12x** |
| SET, unpipelined | 119,338/sec | 139,515/sec | 0.86x |

Reads and high concurrency win because Redis is single-threaded and this server
runs a goroutine per connection across every core. Once pipelining removes the
per-command round trip, that is the whole difference.

Durable writes lose, because they are doing work the baseline was configured not
to do. The size of that payment is the point:

| write configuration | SET/sec | what a crash costs |
|---|---:|---|
| persistence off | 137,881 | everything |
| WAL, fsync every second | 119,338 | ≤1s to power loss, **nothing** to a process kill |
| WAL, fsync before acknowledge | 3,714 | **nothing** that was acknowledged |

Fsyncing before every acknowledgement costs about 32x on this machine. That is
not a tuning problem to optimise away; it is the cost of a physical disk
confirming a write before a client is told it happened, and the group-commit
path already amortises one fsync across every writer waiting at that moment.
The number is published so the trade can be made deliberately.

```sh
make bench          # regenerates everything in docs/benchmarks/
```

Full methodology, latency percentiles and per-configuration reports:
**[docs/benchmarks](docs/benchmarks/README.md)**.

## 2. Durability

`make test-crash` spawns the **real binary**, kills it with `SIGKILL`, restarts
it and checks what survived. An in-process test can only kill what the Go
runtime agrees to kill; SIGKILL against a separate process removes every chance
to clean up, which is what a crash actually does.

| scenario | result |
|---|---|
| 2000 writes, `fsync always` | 0 lost |
| 2000 writes, `everysec` | 0 lost — the page cache outlives the process |
| pipelined writes, killed mid-flight | 0 lost |
| 832 writes, 8 concurrent writers, killed mid-traffic | 0 lost |
| torn log tail | truncated; everything before it intact |
| five crash-restart cycles | converges on the exact key count |
| snapshot + log tail, with post-snapshot overwrite and delete | both applied correctly |
| TTLs across an outage longer than the TTL | expired, not resurrected |
| clean `SIGTERM` | no torn record left behind |
| cache mode | honestly loses what it promised not to keep |

The Rust engine gets the same treatment while the Go server stays up:

```sh
./scripts/smoke-lsm.sh   # 500 of 500 acknowledged writes recovered
make test-crash          # the ten scenarios above
```

## 3. Compatibility

`test/compat` runs identical commands against this server and a real
`redis-server` and compares replies **byte for byte**, error messages included.

Hand-written expectations encode what the author *believes* Redis does. This
encodes what it actually does — which turned out to matter:

```
$ redis-cli SET k -9223372036854775808 ; redis-cli DECR k
OK
ERR increment or decrement would overflow      # both servers, identical
```

```sh
make test-compat    # skips cleanly if redis-server is not installed
```

Three behaviours differ **on purpose**, each with a written decision record:
[one database](docs/adr/ADR-004-single-database.md) ·
[strings only](docs/adr/ADR-003-strings-only-datatypes.md) ·
[`INCRBYFLOAT` precision](docs/adr/ADR-006-incrbyfloat-precision.md).

That last one is the interesting find: **Redis does not agree with itself across
platforms.** It accumulates in C `long double`, which is 80 bits on x86-64 Linux
and 64 on arm64 macOS, so `INCRBYFLOAT f 10.5` then `0.1` prints `10.6` on one
and `10.59999999999999964` on the other. Byte-exact agreement with "Redis" is
not a well-defined goal, so that one command is compared numerically and the
reason is written at the assertion.

---

## What the tests caught

Eleven real bugs, including three that would have caused **silent data loss** —
no error, no crash, just wrong answers later:

| bug | why it mattered |
|---|---|
| `FLUSHALL` scanned at `i64::MAX`, meaning "see everything" | it means the opposite: every key with a TTL looked expired, was skipped, never got a tombstone, and **came back alive** on the next read |
| the LSM scan emitted past a truncated source's boundary | **lost 367 of 1500 keys** with no error at all |
| `everysec` never pushed log bytes to the kernel | acknowledged writes sat in process memory, where a plain kill destroyed them |
| log append sat outside the key lock | `SET` and `INCR` could take log sequence numbers in one order and apply in another, so the database returned a **different answer after every restart** |
| data race between `Server.Addr` and `Serve` | a production race, not a test artifact: it is exactly what a supervisor polling for readiness hits |
| `resp.ParseInt` could not represent `-9223372036854775808` | `INCR` and `DECR` behaved differently at the two ends of the int64 range |
| `GETRANGE` with a doubly-negative range | returned empty where Redis returns the first byte |
| socket deadlines read from the injected clock | armed the kernel with a deadline in the past |
| the paged scan was O(n²) | collected whole files per page |

The first two were found by tests asserting the **complete** expected set after
a full walk. Spot-checking a few keys would have passed both.

---

## Architecture

```
   Redis clients
        │  RESP2 / RESP3 over TCP
        ▼
  ┌──────────────────────────────────────┐
  │  Go node                             │
  │  accept loop → goroutine per conn    │
  │  → command table → store.Store       │
  └──────────────────────────────────────┘
        │                        │
        ▼                        ▼
  in-process engine      Rust engine process
  1024 sharded maps      gRPC → memtable
  + write-ahead log            → write-ahead log
  + fuzzy snapshots            → SSTables + compaction
```

The command layer talks to one narrow interface and does not know which engine
is behind it. That interface carries **no Redis semantics at all** — no `INCR`,
no `SET NX` — only reads, writes, an atomic read-modify-write and an ordered
scan. Pushing atomicity down to the engine rather than layering a lock table on
top means each backend uses the cheapest correct mechanism it has.

The Rust engine runs **out of process, over gRPC, not cgo**. cgo would put a
segfault in the storage engine inside the server's address space. A local gRPC
hop costs tens of microseconds and buys a hard fault boundary, two
independently profileable processes, and a seam that already works when the
engine moves to another host. ([ADR-002](docs/adr/ADR-002-pluggable-storage-boundary.md))

Choosing between the engines is a question about **dataset size**, not about
which benchmark is larger. The in-process engine holds every key and value in
memory and is three times faster per operation. The LSM engine keeps only the
memtable, index blocks and Bloom filters resident and leaves the data on disk.

### The idea the durability model rests on

A crash can only ever damage the **very end** of an append-only log, because the
kernel writes forward. So a checksum failure in the final record is an
interrupted write: expected, benign, truncated away with a warning naming the
file and offset. A checksum failure anywhere earlier is something a crash could
not have caused, and recovery **refuses to start** rather than guessing —
silently skipping a bad record in the middle would resurrect whatever the
records after it overwrote. ([ADR-001](docs/adr/ADR-001-durability-model.md))

---

## Running it

```sh
make run                          # in-process engine, port 6379
make run-lsm                      # Go server plus the Rust engine
docker compose up                 # single node
docker compose --profile lsm up   # server plus engine, two containers
```

Configuration comes from a Redis-style file, then `RFS_`-prefixed environment
variables, then flags.

```sh
bin/rfs-server -dir ./data -appendfsync always -metrics-addr :9121
```

Metrics at `/metrics`, liveness at `/health`, pprof at `/debug/pprof/`.
`INFO` reports recovery state, including a permanent flag when the last restart
discarded a torn log tail — an operator should never have to infer that.

---

## Testing

```sh
make test          # unit and integration, race detector on
make test-e2e      # over a real socket: framing, pipelining, timeouts, teardown
make test-compat   # differential against a real redis-server
make test-crash    # SIGKILL the real binary and check what survived
make test-rust     # 75 Rust tests
make fuzz          # protocol parser and glob matcher
make lint          # vet, golangci-lint, clippy with -D warnings
```

Every Go test that touches concurrency runs with `-race`, and a finding fails
the build: a data race that has not crashed yet is a bug that has not been
noticed yet. That policy is what caught the `Server.Addr` race above, on the
final verification pass.

The end-to-end suite deliberately goes over a TCP socket rather than calling the
command table directly, because everything interesting about a server —
framing, pipelining, reply batching, timeouts, connection teardown — lives in
the parts a direct call skips.

CI runs seven jobs: Go build and race tests, end-to-end, crash recovery,
differential compatibility, the Rust suite with clippy denying warnings, a check
that the committed protobuf bindings still match the contract, and a smoke test
that drives the Go server against the Rust engine and kills the engine mid-run.

---

## Documentation

- **[Benchmarks](docs/benchmarks/README.md)** — every number, with the methodology that produced it
- **[HLD-001](docs/high-level-design/HLD-001-single-node-architecture.md)** — architecture, NFRs with measurements, eight failure modes, capacity
- **[LLD-001](docs/low-level-design/LLD-001-storage-and-durability.md)** — on-disk formats byte by byte, lock ordering, recovery procedures
- **[Learning log](docs/learning-log.md)** — what was genuinely surprising
- Architecture decisions:
  [durability model](docs/adr/ADR-001-durability-model.md) ·
  [storage boundary](docs/adr/ADR-002-pluggable-storage-boundary.md) ·
  [strings only](docs/adr/ADR-003-strings-only-datatypes.md) ·
  [single database](docs/adr/ADR-004-single-database.md) ·
  [multi-key atomicity](docs/adr/ADR-005-multi-key-atomicity-window.md) ·
  [INCRBYFLOAT precision](docs/adr/ADR-006-incrbyfloat-precision.md)

---

## Known limitations

Stated plainly, because a limitation you find in the README is a design
decision and one you find in production is a bug.

- **No replication, consensus or sharding.** Phases 4 and 5.
- **`RENAME`, `COPY` and `MSETNX` are not atomic across keys** under concurrent
  writes ([ADR-005](docs/adr/ADR-005-multi-key-atomicity-window.md)). Single-key
  compare-and-set *is* atomic — 8000 concurrent `INCR`s against one key lose
  nothing — so `SET NX` remains a correct distributed lock. The fix is not a
  global lock, which would give back the entire concurrency advantage; it is the
  replicated command log, which provides a total order as a side effect.
- **`DBSIZE` against the LSM engine is an estimate**, and says so in `INFO`. In
  an LSM tree the same key can exist at every level, and an exact count is a
  full merge.
- **No `maxmemory` policy and no eviction.** The server will use all of it.
- **The LSM engine's open-table cache is unbounded.** Fine at these file counts;
  it needs an eviction policy before it is not.
- **No `MULTI`/`EXEC`.** It needs a cross-key ordering point, which arrives with
  the replicated log rather than before it.
- **Strings only.** No lists, hashes, sets or sorted sets
  ([ADR-003](docs/adr/ADR-003-strings-only-datatypes.md)).

---

## Repository layout

```
cmd/server        the node
cmd/rfs-bench     load generator with a log-linear latency histogram
internal/
  resp/           RESP2/RESP3 codec, specialised per direction
  command/        67 commands, dispatch table, glob matching
  server/         accept loop, connection lifecycle, deadlines, batching
  store/          the storage interface; memory/ and lsm/ implementations
  wal/            segmented write-ahead log and recovery
  snapshot/       point-in-time images
  persist/        composes memory + wal + snapshot
engine/           the Rust LSM crate and its gRPC service
proto/            the contract between the two languages
test/             e2e, compat, crash
docs/             benchmarks, HLD, LLD, ADRs, learning log
scripts/          benchmark suite, full-stack smoke test
```
