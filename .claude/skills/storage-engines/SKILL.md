---
name: storage-engines
description: Storage engine mentor for Redis From Scratch. Use when designing or reviewing the WAL, snapshots, memtable, SSTables, LSM compaction, bloom filters, or any code that claims durability.
---

# Storage Engine Mentor

Durability is a proof, not a vibe. Every claim in this area is backed by a crash test.

## Durability Fundamentals

- A write is durable only after fsync returns on the data and, for new/renamed files, on the directory; `write()` alone is a promise to the page cache.
- Name the loss window of every fsync policy explicitly: `always` (none), `everysec` (up to 1s), `no` (until the OS flushes).
- Atomic file publication is temp-write, fsync, rename, fsync-dir; there is no atomic multi-file operation, so a manifest must arbitrate.
- Torn writes are normal at the WAL tail after a crash; checksums decide truncate-vs-refuse, and mid-log corruption is never silently skipped.

## WAL Rules

- Append-order equals ack-order: log first, apply second, ack third, always.
- Exactly one writer to the log file; interleaved appends from two goroutines are corruption.
- Records carry length plus CRC32C; recovery replays until the first bad checksum at the tail.
- Group commit amortizes fsync but must never ack a write whose batch has not fsynced.

## LSM Rules

- Know the three amplifications (write, read, space) of the chosen compaction strategy and measure them, not just cite them.
- Tombstones outlive the data they delete; dropping one early resurrects a deleted key.
- The read path order is law: active memtable, immutable memtables, L0 newest-first, then deeper levels; a wrong order returns stale data silently.
- Bloom filter sizing is math (bits/key vs false-positive rate); publish the measured FP rate against the design number.
- Write stalls are a designed behavior with explicit thresholds, never an emergent OOM.

## Review Checklist

- Find the window between "acked to client" and "bytes on disk"; if it exists, the fsync policy must admit it.
- Every background task (flush, compaction, snapshot) has a crash test at its midpoint and an idempotent recovery.
- File deletion waits for in-flight readers; file lifetime is reference-counted, not assumed.
- Recovery is idempotent: crashing during recovery and re-running lands in the same state.
- `EXPLAIN` equivalents here are the dump tool and the manifest; both exist before debugging is needed, not after.
