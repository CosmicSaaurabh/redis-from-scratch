# Redis From Scratch MVP Task Breakdown

Phased, bite-sized tasks in learning order.
Each task lists a Definition of Done (DoD) and the architectural edge cases to watch while writing the code.
Every phase follows the gate order: HLD issue, then LLD issue, then feature issues.
Each phase must be demoable and tested before the next begins.

Legend: `[HLD]` design discussion, `[LLD]` detailed design, `[FEAT]` implementation.

---

## Phase 0: Foundation

### T0.1 [FEAT] Project scaffolding

Set up the repo skeleton: Go module layout (`cmd/`, `internal/`), Rust workspace stub for the future storage crate, `proto/` directory, Docker Compose for a single node, golangci-lint and clippy wired in, `.gitignore` (IDE files, binaries), and GitHub Actions CI running build, lint, and tests for both languages with `-race` on Go tests.
The existing `main.go`/`RESP.go` prototype is the starting point and will be reworked in Phase 1.

DoD:
- `go build ./...`, `go test -race ./...`, `cargo build`, and `cargo test` pass locally and in CI from a clean clone.
- Lint runs in CI for both languages and a violation fails the build.
- README documents the one-command local setup.
- `.idea/` and build artifacts are gitignored and removed from tracking.

Edge cases to watch:
- CI toolchain versions pinned; a Go or Rust version drift between local and CI invalidates race and miri-style findings later.
- The Rust crate must build in CI from day one even while empty, or Phase 3 starts with a broken pipeline.
- Cross-language CI is slow if naive; cache Go modules and Cargo registry now.

---

## Phase 1: Protocol & Concurrency (the front door)

### T1.1 [HLD] HLD-001: Single-node server architecture

Interview-style design of the single-node server: components (listener, connection handler, parser, command dispatch, keyspace), the concurrency model (goroutine-per-connection vs event loop) with rejected alternative, data flow for one command, and throughput/latency targets.

DoD:
- `docs/high-level-design/HLD-001-single-node-architecture.md` exists with component diagram, sequence flow, trade-offs, rejected alternatives, and at least two failure modes.
- Explicit decision on the concurrency model with the reasoning Redis itself used (single-threaded event loop) contrasted against Go's goroutine model.
- Numbers: target ops/sec single node, p99 latency budget, max concurrent connections.

Edge cases to watch:
- Where backpressure lives when a client writes faster than the server drains.
- What one shared keyspace under concurrent access means for the locking design before Phase 3 replaces the store.

### T1.2 [LLD] LLD-001: RESP parser and command dispatch

Detailed design of the RESP2/RESP3 parser (state machine over a buffered reader), the command table, request/response lifecycle, keyspace locking strategy (single mutex vs sharded map, with rejected alternative), and the error-reply contract.

DoD:
- `docs/low-level-design/LLD-001-resp-and-dispatch.md` with parser state machine, package layout, locking strategy, and goroutine ownership diagram (which goroutine reads, executes, writes).
- RESP edge-case matrix documented: inline commands, nested arrays, null bulk strings, big bulk lengths, protocol errors.
- Benchmark plan for the parser (allocations per command as a tracked number).

Edge cases to watch:
- Partial reads: a command frame can arrive one byte at a time; the parser must never block holding a lock or buffer unboundedly.
- Malicious lengths: a declared bulk length of 512 MB must be rejected before allocation, not after.
- Pipelining: many commands in one TCP segment must all execute and reply in order.

### T1.3 [FEAT] RESP2/RESP3 parser

Rework the prototype parser into a spec-complete RESP2 parser with RESP3 upgrades (`HELLO`), fuzz-tested, with zero allocations on the hot path where feasible.

DoD:
- Parser passes a table-driven test suite covering the documented edge-case matrix.
- A Go fuzz test runs clean for an agreed corpus time; crashes found become regression tests.
- `redis-cli` connects and round-trips `PING`/`HELLO` against the server.

Edge cases to watch:
- `\r` and `\n` split across reads; off-by-one on CRLF scanning is the classic bug here.
- Reply for unknown commands and wrong arity must match Redis error formats, or client libraries misbehave.
- RESP3 double, boolean, and map types have no RESP2 equivalent; the negotiated protocol version must gate reply encoding.

### T1.4 [FEAT] Concurrent server and core command set

Implement the accepted concurrency model, the in-memory keyspace, and `GET`, `SET` (`NX`/`XX`/`EX`/`PX`), `DEL`, `EXISTS`, `INCR`/`DECR` with correct type errors.

DoD:
- 100+ concurrent `redis-cli` and client-library connections operate correctly; a stress test with `-race` shows zero races.
- A deliberately slow client does not degrade other clients (proven by a test, not asserted).
- `INCR` on a non-integer value returns the exact Redis error string.

Edge cases to watch:
- Connection teardown mid-command: half-written replies, `EPIPE`, and goroutine leak on abandoned connections (assert goroutine count in tests).
- `SET` with `NX` plus `EX` combines a conditional write and TTL atomically; two clients racing on `NX` must yield exactly one winner.
- Integer overflow on `INCR` at int64 bounds must error, not wrap.

### T1.5 [FEAT] Expiration: EXPIRE, TTL, and reclamation

Implement `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, lazy expiration on access, and an active expiration cycle.

DoD:
- TTL semantics match Redis: `TTL` on missing key is -2, on key without TTL is -1; overwriting `SET` clears TTL.
- Active expiry reclaims a large batch of expired keys without a latency spike visible in the benchmark (numbers in the PR).
- Time is monotonic-clock based for expiry math; a wall-clock jump test proves keys do not mass-expire.

Edge cases to watch:
- Expiry racing a concurrent `GET`: the read must see either the live value or a miss, never a torn state.
- The active-expiry scan holding the keyspace lock too long is a self-inflicted latency spike; bound the per-cycle work.
- Phase 4 note: expiration is a write (state change) and will later need to flow through Raft, not fire independently on replicas; design the seam now.

Phase 1 demo: `redis-benchmark` runs against the server; publish the first ops/sec and p99 numbers to compare against after Phase 3.

---

## Phase 2: Persistence (survive kill -9)

### T2.1 [HLD] HLD-002: Durability model

Design the durability architecture: WAL-first write path, fsync policy options (`always`, `everysec`, `no`) with their exact loss windows, snapshot plus log-truncation lifecycle, and the recovery procedure as a state machine.

DoD:
- `docs/high-level-design/HLD-002-durability-model.md` with the write path diagram, fsync policy table with loss windows, recovery flow, rejected alternatives (e.g. snapshot-only like RDB, logical command log like AOF vs binary record log), and two failure modes.
- Explicit statement of what "acknowledged" means at each fsync policy.
- Crash-test matrix defined: kill points at every lifecycle stage (mid-append, post-append pre-fsync, mid-snapshot, mid-truncation).

Edge cases to watch:
- fsync of the file is not enough on first create/rename; the directory entry needs fsync too.
- A snapshot taken while writes continue must be a consistent point-in-time view; decide the mechanism (lock, copy-on-write iteration) now.

### T2.2 [LLD] LLD-002: WAL format and recovery

Detailed design of the on-disk WAL record format (length, CRC32C checksum, type, payload), segment file rotation, snapshot file format, manifest/pointer file telling recovery where to start, and the exact replay algorithm including torn-tail handling.

DoD:
- `docs/low-level-design/LLD-002-wal-format-and-recovery.md` with byte-level record layout, segment naming and rotation rules, and the recovery algorithm in pseudocode.
- Torn-write policy documented: a record failing its checksum at the tail truncates the log there; a checksum failure mid-log is corruption and refuses startup.
- Rejected alternatives recorded (e.g. one big file vs segments, text AOF vs binary records).

Edge cases to watch:
- A record split across a segment boundary either must be impossible by design or handled by replay; pick one and document it.
- Recovery must be idempotent: crashing during recovery and re-running it lands in the same state.
- Group-commit batching interacts with the `everysec` policy; the ack path must not lie about durability.

### T2.3 [FEAT] Write-ahead log

Implement WAL append on every write command before keyspace mutation, with configurable fsync policy and segment rotation.

DoD:
- Write path is WAL-append, then apply, then ack, in that order, verified by a test that fails the fsync (fault injection) and asserts the write is not acknowledged.
- Throughput with `always` vs `everysec` measured and documented in the PR.
- Segment rotation happens under load without dropping or reordering records.

Edge cases to watch:
- `O_APPEND` plus a single writer goroutine vs mutex-guarded writes; two goroutines appending interleave records and corrupt the log.
- Disk-full mid-append must surface as a command error and stop accepting writes, not silently drop the tail.
- fsync latency spikes (ext4/APFS journal flush) stall the ack path; group commit is the mitigation, measure it.

### T2.4 [FEAT] Snapshotting, log truncation, and crash-recovery harness

Implement point-in-time snapshots, WAL truncation after a durable snapshot, startup recovery (load snapshot, replay WAL tail), and the automated crash-recovery test harness that `kill -9`s the server at every point in the crash-test matrix under write load and verifies zero acknowledged loss.

DoD:
- The crash harness runs the full kill-point matrix in CI; every acknowledged write (recorded client-side) is present after recovery, every unacknowledged write is either present or absent but never corrupt.
- Snapshot runs under sustained write load without blocking writes beyond the documented pause budget.
- Truncation never removes WAL records newer than the snapshot's consistent point (proven by a kill between snapshot completion and truncation).

Edge cases to watch:
- The snapshot-then-truncate ordering has a crash window between the two; recovery must handle both files claiming authority via the manifest.
- Atomic snapshot publication: write to temp file, fsync, rename, fsync directory; a kill mid-rename must leave the old snapshot valid.
- Clock-independent recovery: replaying `EXPIRE` records must not resurrect or prematurely kill keys based on replay-time wall clock.

Phase 2 demo: `kill -9` during a live `redis-benchmark` run, restart, and a verifier script confirms zero acknowledged-write loss.

---

## Phase 3: Rust Storage Engine (the LSM-tree)

### T3.1 [HLD] HLD-003: Storage engine architecture and the Go-Rust boundary

Design the LSM engine (memtable, SSTables, compaction, bloom filters, manifest) and decide the Go-to-Rust boundary: gRPC over UDS vs cgo FFI, decided with a measured round-trip latency comparison and recorded as an ADR.
Define the pluggable storage interface in Go that both the Phase 1 in-memory store and the Rust engine implement.

DoD:
- `docs/high-level-design/HLD-003-storage-engine.md` with the LSM read/write path diagrams, compaction strategy choice (leveled vs size-tiered) with rejected alternative, and two failure modes.
- `ADR-<nnn>-go-rust-boundary.md` with the latency measurement table backing the decision.
- The Go storage interface is small (get, set, delete, iterate, snapshot hooks) and documented; ownership of durability (Phase 2 WAL vs engine WAL) is explicitly resolved.

Edge cases to watch:
- If the engine owns its own WAL, the Phase 2 Go WAL becomes the Raft log only; double-journaling every write is a real performance bug to design out now.
- Crossing the boundary per-operation vs batching amortization; the hot path budget decides.

### T3.2 [LLD] LLD-003: Memtable, SSTable format, and compaction

Byte-level design of the SSTable (data blocks, index block, bloom filter, footer), memtable structure (skiplist or ordered map) and flush threshold, manifest file recording live SSTables per level, tombstone semantics, and the compaction algorithm with its trigger conditions.

DoD:
- `docs/low-level-design/LLD-003-lsm-internals.md` with SSTable byte layout, manifest update protocol (atomic via write-temp-rename), compaction I/O complexity analysis, and bloom filter sizing math (bits per key vs false-positive rate).
- Read path documented: memtable, immutable memtables, then SSTables newest-first per level, with bloom filter short-circuit.
- Rejected alternatives recorded (e.g. B-tree engine, hash index per SSTable vs sparse block index).

Edge cases to watch:
- Tombstones must survive compaction until they are provably older than every SSTable that could contain the key, or deleted keys resurrect.
- Manifest corruption is engine death; its update must be as crash-safe as the data itself.
- Bloom filters are per-SSTable memory; sizing math at 10M keys decides whether the design fits in RAM.

### T3.3 [FEAT] Memtable and SSTable flush

Implement the memtable, freeze-and-flush to SSTable on threshold, and the manifest.

DoD:
- Writes land in the memtable and flush produces spec-conformant SSTables readable by a standalone dump tool (build the dump tool; it is the debugger for every later phase).
- Flush runs on a background thread while new writes continue into a fresh memtable.
- Crash during flush recovers cleanly: the half-written SSTable is ignored, data replays from the engine WAL.

Edge cases to watch:
- The freeze window (swap active memtable for a new one) is a race hot spot; reads must see a consistent view across active plus immutable memtables.
- Flush backpressure: if flush is slower than ingest, immutable memtables pile up; bound them and stall writes explicitly.
- Rust ownership design for the shared memtable (Arc plus lock scope) decides whether readers block writers.

### T3.4 [FEAT] Read path with bloom filters and block index

Implement point reads across memtable and SSTables with bloom filter short-circuiting and sparse block index binary search.

DoD:
- A read-heavy benchmark shows bloom filters skipping SSTable reads (measured filter hit/skip counters, published in the PR).
- Correctness test: keys overwritten and deleted across multiple flushed SSTables always resolve to the newest version.
- Measured false-positive rate matches the sizing math within tolerance.

Edge cases to watch:
- Newest-version-wins requires a strict SSTable ordering; a bug here returns stale values silently, so test overwrite chains deliberately.
- mmap vs pread for SSTable access changes page-cache behavior and error handling (SIGBUS on truncated file vs error return); pick one and know why.

### T3.5 [FEAT] Background compaction

Implement the chosen compaction strategy with explicit trigger conditions, atomic manifest swap, and obsolete-file deletion.

DoD:
- Sustained overwrite-heavy load keeps space amplification bounded (measured) and read amplification within the design budget.
- Compaction runs concurrently with reads and writes; a chaos test kills the engine mid-compaction and recovery discards the partial output via the manifest.
- Compaction throughput and its interference with foreground p99 latency are measured and published.

Edge cases to watch:
- Deleting an SSTable still held open by an in-flight read; file lifetime must be tied to reader reference counts.
- Compaction starving foreground I/O; rate-limit it and prove the p99 impact.
- Write stall policy when L0 files pile up: explicit slowdown/stop thresholds, mirroring what RocksDB learned the hard way.

### T3.6 [FEAT] Wire the engine into the Go server

Implement the boundary transport chosen in the ADR and swap the Rust engine in behind the pluggable storage interface, keeping the in-memory engine selectable by config.

DoD:
- The full Phase 1 command suite and Phase 2 crash harness pass against the Rust engine.
- Before/after benchmark against Phase 1 numbers published: throughput, p99, and allocation profile of the Go side.
- Engine process/lifecycle management is explicit: startup handshake, health check, and clean shutdown draining in-flight operations.

Edge cases to watch:
- Error taxonomy across the boundary: a storage error, a transport error, and a not-found must be distinguishable in Go, not collapsed into one string.
- Backpressure across the boundary when the engine stalls (compaction, disk); the Go side must fail fast, not queue unboundedly.

Phase 3 demo: same `redis-benchmark` run as Phase 1, now hitting the LSM engine, with the comparison table committed.

---

## Phase 4: Replication & Consensus (Raft)

### T4.1 [HLD] HLD-004: Replication architecture

Design the Raft-based replication: node roles, the replicated log as the source of truth, where the log lives relative to the storage engine, the apply pipeline, cluster bootstrap, and the two consistency modes.
Decide build-from-paper vs library (the learning goal mandates from-paper; record it as an ADR with the honest trade-off).

DoD:
- `docs/high-level-design/HLD-004-replication-architecture.md` with the full write path (client, leader, log replication, majority commit, apply, ack), the read paths for both consistency modes, and two failure modes.
- Explicit safety argument sketch: why committed entries survive any minority failure.
- Failover budget decomposed: election timeout ranges that make sub-2s failover achievable.

Edge cases to watch:
- The Raft log vs engine WAL relationship: replicated entries must not be double-fsynced wastefully, but Raft's persistence rules (term, vote, log before responding) are non-negotiable.
- Client redirect protocol when a follower receives a write (`-MOVED`-style error vs proxying); pick one now, it shapes Phase 5.

### T4.2 [LLD] LLD-004: Raft internals

Detailed design of the Raft implementation: state per role, persistent vs volatile state, `RequestVote` and `AppendEntries` proto contracts, the election timer with randomization, log storage format, commit/apply pipeline, and the goroutine/channel topology of the node.

DoD:
- `docs/low-level-design/LLD-004-raft-internals.md` with the role state machine diagram, RPC contracts, persistence points (exactly what is fsynced before which RPC reply), and the goroutine ownership map.
- Every timer documented with its range and the invariant it protects (election timeout >> broadcast time >> RPC latency).
- Rejected alternatives recorded (e.g. pre-vote now vs later, batched vs per-entry AppendEntries).

Edge cases to watch:
- Replying to `RequestVote` before persisting the vote is the classic safety bug; the persistence points table exists to prevent it.
- A single goroutine owning all Raft state (event-loop style) vs fine-grained locking; the former is dramatically easier to reason about and test.
- Log index/term arithmetic off-by-ones around entry 0/1 and snapshot boundaries; deterministic unit tests per rule in the paper.

### T4.3 [FEAT] Leader election

Implement node startup, election timers, `RequestVote`, and role transitions across a Docker Compose cluster of 3-5 nodes.

DoD:
- A cluster elects exactly one leader per term; killing the leader elects a new one within the timeout budget, repeatedly, in an automated test.
- Persistent state (term, votedFor) survives restart; a restarted node rejoins without disrupting a stable leader.
- Election under partition: the minority side never elects; term inflation on heal is observed and documented (motivating pre-vote as a recorded future improvement).

Edge cases to watch:
- Split vote livelock if timers are not randomized per-election, not just per-node.
- Two nodes with the same timer seed in containers started simultaneously; seed carefully.
- A node with a stale clock is irrelevant to Raft correctness (timers are relative), but a paused container (SIGSTOP) is the zombie-leader generator; test with it.

### T4.4 [FEAT] Log replication and the apply pipeline

Implement `AppendEntries`, majority commit, and the apply pipeline feeding committed entries into the storage engine; writes now flow client, leader, Raft, apply, ack.

DoD:
- A 5-node cluster serves the full command suite with writes acknowledged only after majority commit.
- Follower log divergence after partitions heals per the paper's consistency check (conflict rollback), covered by a deterministic test.
- The crash harness now runs cluster-wide: kill any node (including the leader) under load, zero acknowledged-write loss.

Edge cases to watch:
- Apply must be exactly-once into the engine even across restarts: persist lastApplied atomically with engine effects or make apply idempotent; decide and prove.
- The leader acking before its own local fsync of the entry violates durability even with majority commit if the majority counted itself.
- Expiration and `INCR` are non-deterministic if evaluated at apply time on each node with local clocks or local state; command entries must carry the resolved outcome (deterministic apply), the classic replicated-state-machine lesson.

### T4.5 [FEAT] Raft snapshots and log compaction

Implement snapshot creation tied to the engine's snapshot mechanism, `InstallSnapshot` for lagging followers, and Raft log truncation.

DoD:
- A follower down for an extended period catches up via `InstallSnapshot` and converges to identical state (state hash comparison in test).
- Log truncation keeps the Raft log bounded under sustained load (measured disk usage over time).
- Kill during snapshot install leaves the follower recoverable.

Edge cases to watch:
- Snapshot index/term bookkeeping: the entry-before-snapshot problem breaks AppendEntries consistency checks if the boundary math is off.
- A snapshot transferring while the leader truncates the log it is streaming from; hold a reference or restart the transfer.

### T4.6 [FEAT] Consistency modes: read-index and stale reads

Implement linearizable reads via Raft read-index (leader confirms leadership with a heartbeat round before serving) and eventual reads served locally by followers, selectable per request.

DoD:
- A linearizability checker (e.g. Porcupine-style) validates recorded histories of concurrent reads/writes under leader kills for the linearizable mode; violations fail CI.
- Stale-read mode measurably serves from followers (traffic distribution in metrics) and documents its staleness bound honestly (unbounded during partition).
- The zombie-leader test: a SIGSTOP-ed old leader resumed after a new election never serves a stale linearizable read.

Edge cases to watch:
- Read-index without the heartbeat confirmation round is the textbook stale-read bug; the zombie test exists to catch exactly this.
- Lease-based reads are the tempting optimization that imports clock assumptions; record it as a rejected-for-now alternative with the reason.

Phase 4 demo: kill the leader during `redis-benchmark`, cluster recovers writes in under 2 seconds, linearizability checker passes; publish the failover distribution.

---

## Phase 5: Sharding

### T5.1 [HLD] HLD-005: Partitioning and routing

Design the sharding scheme: consistent hashing vs range partitioning vs fixed hash slots (Redis Cluster's choice), one Raft group per shard, the shard map and its source of truth, and the routing model (client-side redirects vs proxy) with rejected alternatives.

DoD:
- `docs/high-level-design/HLD-005-sharding.md` with the shard map design, request routing flow, cross-shard implications for multi-key commands (declare them unsupported cross-shard, like Redis), and two failure modes.
- Explicit decision on shard-map distribution and staleness handling (a node with an old map must redirect, not serve wrong data).
- Capacity math: shards per node, memory and connection budgets.

Edge cases to watch:
- The shard map itself needs consistency; deciding who owns it (static config for MVP vs a metadata Raft group) bounds this phase's scope.
- Multi-key operations (`DEL k1 k2`) spanning shards; Redis's hash-tag answer is worth studying before inventing one.

### T5.2 [LLD] LLD-005: Shard map and request routing

Detailed design of slot-to-shard mapping, the redirect error contract, connection management per shard in the router, and how `CLUSTER STATE` reports topology.

DoD:
- `docs/low-level-design/LLD-005-shard-routing.md` with the slot mapping function, redirect protocol, and router connection lifecycle.
- Startup and topology-change flows documented (MVP: static topology, changes require restart; dynamic membership is the recorded stretch goal).

Edge cases to watch:
- Hashing must be stable across versions and languages (the Go router and any tooling must agree); pick and pin the hash function.
- A shard whose Raft group has no leader (mid-election) needs a distinct error from "wrong shard"; clients back off differently.

### T5.3 [FEAT] Sharded cluster

Implement slot mapping, per-shard Raft groups, request routing with redirects, and `CLUSTER STATE`.

DoD:
- A multi-shard cluster (2-3 shards, 3 nodes each or overlapping) serves the command suite with keys verifiably distributed (distribution histogram in test).
- A request to the wrong node redirects and the reference client pattern (follow redirect once) succeeds.
- Chaos: killing one shard's leader affects only that shard's keys; other shards' latency is unaffected (measured).

Edge cases to watch:
- Uneven key distribution from a poor hash or hot key skews one shard; publish the distribution and know the mitigation story.
- Cross-shard `redis-benchmark` behavior with redirects; document how benchmarking accounts for them.

---

## Phase 6: Observability

### T6.1 [LLD] LLD-006: Metrics, tracing, and logging design

Design the observability surface: metric inventory with bounded labels, trace span topology per command (parse, route, raft-replicate, engine-apply, respond), log schema, and `INFO` sections.
Mirrors the Sentinel Engine observability approach for cross-project consistency.

DoD:
- `docs/low-level-design/LLD-006-observability.md` with the full metric list (name, type, labels, cardinality bound), span topology diagram, and structured log schema.
- Explicit sampling decision for traces at target load.

Edge cases to watch:
- Key or command-argument values must never be labels or logged payloads (cardinality and privacy); command name and shard id are the bounded labels.
- Rust engine metrics need an export path across the boundary; piggyback on the transport rather than a second scrape endpoint, or justify otherwise.

### T6.2 [FEAT] Metrics, traces, structured logs, and admin commands

Implement Prometheus metrics, OpenTelemetry tracing across Go and the Rust engine boundary, structured JSON logs with trace ids, `INFO`, and a Grafana dashboard.

DoD:
- One trace shows a `SET`'s full life across router, leader, Raft commit, and engine apply.
- Grafana dashboard JSON committed under `infra/`: command rates, latency histograms, Raft term/commit-index lag, compaction activity, WAL fsync latency.
- `INFO` reports role, term, shard ownership, keyspace stats, and engine stats.
- Every log line during a command carries the trace id and shard id.

Edge cases to watch:
- Trace context propagation across the Go-Rust boundary and across Raft RPCs needs explicit metadata plumbing; context does not flow magically.
- Histogram bucket boundaries chosen now determine whether the sub-2s failover and p99 claims are even measurable; pick buckets against the NFR targets.

---

## Phase 7: Testing, Chaos & Benchmarks

### T7.1 [LLD] LLD-007: Chaos harness and benchmark methodology

Design the reusable chaos harness (node kill, SIGSTOP pause, network partition and latency injection between containers, clock skew) and the benchmark methodology (workloads, warm-up, duration, percentile reporting, hardware disclosure).

DoD:
- `docs/low-level-design/LLD-007-chaos-and-bench.md` with the fault injection catalogue, how each fault is applied and healed, and the benchmark report template.
- Linearizability-check integration into chaos runs specified.

Edge cases to watch:
- Partitions via container network manipulation must be symmetric or deliberately asymmetric; accidental asymmetry produces confusing, non-reproducible results.
- Benchmarking through the chaos harness's own overhead lies; measure the harness tax first.

### T7.2 [FEAT] Chaos harness

Implement the harness and the standing chaos suite: repeated leader kills, partition and heal cycles, paused-process zombies, disk-full injection on one node, all under load with the linearizability checker on recorded histories.

DoD:
- The chaos suite runs in CI (bounded profile) and a longer local profile; any acknowledged-write loss or linearizability violation fails the run.
- Each PRD failure mode (zombie leader, fsync failure, compaction stall) has a corresponding chaos scenario.
- Flakes in the chaos suite are triaged as real bugs or harness bugs, never retried into silence.

Edge cases to watch:
- The history recorder must not itself drop records under load or it fabricates linearizability violations; make the recorder's durability stronger than the system under test.
- Time-based assertions (sub-2s failover) on loaded CI runners need generous CI budgets with the strict numbers measured locally and published.

### T7.3 [FEAT] Benchmark harness and published numbers

Implement the benchmarking tool per the methodology and produce the committed numbers: sustained writes/sec, read/write latency percentiles per consistency mode, failover time distribution over N kills, and recovery time from crash.

DoD:
- `docs/benchmarks/` contains the report: methodology, hardware, workload mix, and results with percentiles; every resume claim traces to a committed run.
- The 8,000+ writes/sec and sub-2s failover targets are met or the gap is analyzed honestly with the bottleneck identified.
- A one-command script reproduces the benchmark on a fresh machine.

Edge cases to watch:
- Coordinated omission in latency measurement (the classic benchmarking lie); the load generator must issue at a fixed rate independent of response arrival.
- Comparing against real Redis is tempting and fine, but only with identical durability settings, or the comparison is meaningless.

---

## Stretch Flavors (post-Phase 7, pick two or three at most)

Each stretch feature enters through the same HLD, LLD, FEAT gate cycle.

- S1: Vector similarity search: `VADD`/`VSEARCH` backed by an HNSW index in the Rust engine; highest priority, extends the Couchbase Vector Search experience into an owned implementation.
- S2: Dynamic cluster membership: add/remove Raft nodes without downtime via joint consensus; the "most from-scratch Rafts hardcode membership" differentiator.
- S3: Pluggable storage engines: a B-tree engine behind the Phase 3 interface, proving the abstraction.
- S4: Multi-tenancy: logical databases with per-tenant quotas.

---

## Learning Checkpoints

At the end of each phase, update `docs/learning-log.md` with:
- The three most surprising things learned.
- One production incident this phase's work would have prevented.
- One thing that would be designed differently at 100x scale.
