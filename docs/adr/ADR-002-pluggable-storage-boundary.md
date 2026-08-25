# ADR-002: One narrow storage interface, two engines, out of process

- Status: accepted
- Date: 2026-08-25
- Phase: 3

## Context

The project needs a storage engine written in Rust, and it needs the existing
in-process Go engine to keep working.
Three questions had to be answered together, because the answers constrain each
other: what the interface between the server and an engine should be, where
Redis semantics live, and how a Rust engine gets called from Go.

## Decision

**A narrow interface carrying no Redis semantics.**
`store.Store` has reads, writes, an atomic read-modify-write, a batch write and
an ordered scan.
It has no `INCR`, no `SET NX`, no TTL policy.
Those live in the command layer, expressed through exactly one primitive:
`Update`, which runs a caller-supplied function while the engine holds the key.

Pushing atomicity down to the engine rather than layering a lock table on top
means each backend uses the cheapest correct mechanism it has: the memory engine
takes one shard lock, the LSM client takes one stripe lock, and neither pays for
a second lock on the hot path.
Keeping Redis out of the interface is what would let the same engine sit behind
something that is not Redis.

**The Rust engine runs as a separate process, reached over gRPC.**
The alternative was cgo.
cgo would put a segfault in the storage engine inside the server's address
space, make every call cross the Go scheduler's boundary in a way that pins an
OS thread, and force both toolchains into one build.
A local gRPC hop costs tens of microseconds and buys a hard fault boundary, two
independently profileable and restartable processes, and a seam that already
works when the engine moves to another host.

**Read-modify-write for the remote engine lives in the Go client.**
The thing being compared is arbitrary Go code, so it cannot be shipped to the
engine.
The client holds a 1024-way stripe of mutexes and does the read and the write
between them.
That is correct exactly as long as this process is the only writer to the
engine, which is the deployment this phase targets: one node, one engine.
The constraint is documented at the top of the package rather than assumed.

## Consequences

The two engines have genuinely different performance and different capacity,
and the choice between them is a question about dataset size rather than about
which benchmark is larger.

| | in-process | Rust LSM over gRPC |
|---|---:|---:|
| GET, unpipelined | 145k/sec | 47k/sec |
| SET, unpipelined | 119k/sec | 22k/sec |
| dataset bound | RAM | disk |

Every command against the LSM engine crosses a process boundary, which is why
it is three times slower per operation.
What it buys is that only the memtable, the index blocks and the Bloom filters
stay resident while the data lives on disk.

Two differences are visible to clients and are documented rather than hidden:

- `DBSIZE` against the LSM engine is an **estimate**.
  In an LSM tree the same key can exist at every level, and counting distinct
  live keys requires merging all of them.
  An O(1) answer over-counts duplicates and uncompacted tombstones; an exact
  answer is a full table scan.
  The estimate is labelled as one everywhere it surfaces, including in `INFO`.
- `SCAN` cursors are mapped.
  The Redis protocol requires a numeric cursor and an LSM tree resumes from a
  key, so the client keeps a bounded table mapping small integers to resume
  keys, aged out after ten minutes.
  A client resuming a scan hours later gets a clear error rather than a
  silently wrong result. Redis makes no promise about cursor lifetime either.

## Alternatives rejected

**cgo.** Discussed above. The fault-isolation argument alone decided it.

**A wide interface with Redis semantics in the engine.**
Would remove the extra round trip for `INCR`, at the cost of reimplementing
Redis semantics in Rust and of an interface that only Redis can use.

**Unix domain sockets instead of TCP for the gRPC hop.**
Measurably faster and worth doing, but it forecloses the remote-engine
deployment for a gain that is small next to the round trip itself.
Left as a configuration option to add later.

**One process embedding Rust as a static library through FFI without cgo.**
Not possible; FFI from Go is cgo.
