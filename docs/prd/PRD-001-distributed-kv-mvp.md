# PRD-001: Redis From Scratch MVP

- Status: Accepted
- Author: Saurabh Mishra
- Reviewer: Principal Architect (Claude)
- Date: 2026-07-21

## 1. Problem Statement

Every serious backend system leans on a distributed key-value store, yet the hard problems inside one - durability across crashes, consensus-based replication, storage engine internals, and recovery under real faults - stay hidden behind the client library.
Tutorial-grade "build your own Redis" projects stop at a parser and a map, and teach none of those problems.

Redis From Scratch is a distributed, Redis-compatible key-value store built entirely by hand.
It speaks RESP so existing Redis clients work against it, persists through `kill -9`, replicates via Raft across a 3-5 node cluster with automatic failover, offers per-request linearizable or eventual consistency, and shards horizontally.
The goal is to personally implement, and be able to defend in an interview, each of those mechanisms.

## 2. Goals

- RESP2/RESP3 compatibility: standard Redis clients (`redis-cli`, `redis-benchmark`, client libraries) work unmodified for the supported command set.
- A real command surface: `GET`, `SET` (with `NX`/`XX`/`EX`/`PX`), `DEL`, `EXISTS`, `EXPIRE`, `TTL`, `INCR`/`DECR`, plus admin commands `INFO` and `CLUSTER STATE`.
- Durability: acknowledged writes survive `kill -9` of the process at any instant, proven by an automated crash-recovery test.
- A Rust storage engine (WAL, memtable, SSTables, LSM compaction, bloom filters) behind a pluggable storage interface, keeping GC off the hot write path.
- Raft replication across 3-5 nodes: leader election, log replication, snapshot-based log compaction, automatic failover.
- Configurable consistency per request: linearizable via Raft read-index, or eventual via stale follower reads.
- Horizontal sharding with deterministic key routing.
- First-class observability: Prometheus metrics, OpenTelemetry traces per command, structured logs.
- A chaos harness and benchmarking tool that produce the published resilience and performance numbers.

## 3. Non-Goals (MVP)

- No full Redis command coverage: no pub/sub, streams, Lua scripting, transactions (`MULTI`/`EXEC`), or ACLs.
- No Redis Cluster wire-protocol compatibility; cluster topology and routing are our own design.
- No multi-region or cross-datacenter story.
- No client SDKs; RESP compatibility with existing clients is the interface.
- Stretch flavors (vector search, dynamic membership, pluggable engines beyond the interface seam, multi-tenancy) are explicitly post-MVP.

## 4. Users and Use Cases

- Backend developer: points an existing Redis client at the cluster, issues commands, chooses per-request consistency, observes latency via metrics.
- Platform operator: runs the 3-5 node cluster via Docker Compose, kills nodes and watches failover, reads `INFO`/`CLUSTER STATE`, gets metrics in Prometheus.
- The engineer-as-learner: every subsystem is small enough to hold in one head and defend at a whiteboard.

Representative flow: a client `SET`s a key on the leader, the write replicates through Raft and lands in the Rust LSM engine, the leader is `kill -9`ed, a new leader is elected in under 2 seconds, and a linearizable `GET` returns the value.

## 5. Functional Requirements

- FR1: The server accepts RESP2/RESP3 connections and serves the supported command set to unmodified Redis clients.
- FR2: Concurrent clients are served without interference; a slow client never blocks other clients.
- FR3: Keys support expiration (`EXPIRE`, `TTL`, `SET ... EX/PX`) with both lazy and active reclamation.
- FR4: An acknowledged write is durable: it survives immediate `kill -9` and is present after restart.
- FR5: The store recovers to a consistent state from any crash point, replaying the WAL and discarding torn tail records.
- FR6: Writes replicate via Raft; a write is acknowledged only after commit on a majority.
- FR7: Loss of the leader triggers automatic election; the cluster resumes accepting writes without operator action.
- FR8: A read may request linearizable consistency (served via read-index) or eventual consistency (servable by any follower), per request.
- FR9: Keys are partitioned across shards; any node routes or redirects a request for a key it does not own.
- FR10: `INFO` and `CLUSTER STATE` report node role, term, shard ownership, and storage statistics at any time.

## 6. Non-Functional Requirements

- NFR1 (consistency): the system chooses consistency over availability for writes (CP); a minority partition refuses writes rather than diverging.
- NFR2 (throughput target): 8,000+ sustained writes/sec on a single shard with replication enabled, on developer hardware, measured by the project benchmark harness.
- NFR3 (failover target): automatic leader failover completes in under 2 seconds, measured from leader kill to first successful write on the new leader.
- NFR4 (durability): zero acknowledged-write loss across the crash-test matrix (`kill -9` at every WAL/snapshot lifecycle stage).
- NFR5 (write-path latency): the storage engine keeps the write hot path free of GC pauses; engine-side p99 write latency is measured and published per release.
- NFR6 (observability): every command carries a trace spanning parse, route, replicate, apply, and respond.

## 7. Success Criteria

- `redis-cli` and `redis-benchmark` run unmodified against the server for the supported command set.
- Crash-recovery test: `kill -9` during a sustained write load, restart, and verify zero acknowledged-write loss and no corruption, automated in CI.
- Chaos test: leader kill under load recovers writes in under 2 seconds; a network partition never yields divergent acknowledged writes, checked by a linearizability checker over recorded histories.
- Benchmark: 8,000+ writes/sec sustained with documented methodology, hardware, and percentile latencies committed to the repo.
- Grafana dashboard shows command rates, latency histograms, Raft term/commit-index, and compaction activity.

## 8. Open Questions

- Go-to-Rust boundary: gRPC vs FFI (cgo) for the storage engine; decided by an ADR with a measured latency comparison in Phase 3.
- Sharding scheme: consistent hashing vs range partitioning, and client-side redirects vs a routing proxy; decided in the Phase 5 HLD.
- Whether Phase 2's Go WAL survives as the Raft log store once Phase 3's Rust engine owns data durability.

## 9. Failure Modes to Design Against

- A leader is partitioned but does not know it (zombie leader) and keeps serving reads; stale reads must be impossible on the linearizable path.
- Disk fills or fsync fails mid-WAL-append; the system must fail the write loudly rather than acknowledge unpersisted data.
- Compaction falls behind sustained write load and stalls the write path; backpressure must be explicit, not an OOM.
