# Redis From Scratch

A distributed, Redis-compatible key-value store built from scratch.
It speaks the RESP protocol on the wire, uses Go for networking, coordination, and Raft consensus, and a Rust storage engine (WAL, LSM-tree, background compaction) on the write path to avoid GC-induced latency.

This is a deliberate learning project built entirely by hand to master the hard problems every real distributed database solves: durability, replication consensus, storage engine internals, and failure recovery under real faults.

## Architecture at a Glance

- Protocol: RESP2/RESP3 server compatible with existing Redis clients, with concurrent connection handling in Go.
- Durability: write-ahead log plus snapshotting; a `kill -9` mid-write never loses acknowledged data.
- Storage engine: a Rust crate implementing memtable, SSTables, LSM-tree compaction, and bloom filters, exposed to the Go layer behind a pluggable storage interface.
- Replication: Raft across a 3-5 node cluster - leader election, log replication, snapshot-based log compaction - with automatic failover.
- Consistency: configurable per request - linearizable reads via Raft read-index, or eventual reads served stale from followers.
- Sharding: horizontal partitioning across the cluster with deterministic key routing.
- Observability: Prometheus metrics, OpenTelemetry traces per command, structured logs, and admin commands (`INFO`, `CLUSTER STATE`).
- Chaos: a reusable harness that kills nodes, partitions the network, and skews clocks, plus a benchmarking tool that produces the published throughput and failover numbers.

## Repository Layout

- `docs/`: PRDs, high-level designs, low-level designs, ADRs, task breakdown, and the learning log.
- `docs/tasks/mvp-task-breakdown.md`: the phased plan with definitions of done and edge cases.
- `CLAUDE.md`: rules of engagement for the AI architect/reviewer guiding this project.

## Development Process

Every feature passes three gates, each tracked as a GitHub issue and closed in order: high-level design, low-level design, then implementation.
Designs are documented with trade-offs and rejected alternatives before code is written.
Durability, consistency, and performance claims are proven by crash tests, linearizability checks, and benchmarks - never asserted in comments.
