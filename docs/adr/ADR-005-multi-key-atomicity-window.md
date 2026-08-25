# ADR-005: Multi-key commands are not atomic yet, and say so

- Status: accepted
- Date: 2026-08-25
- Phase: 1

## Context

`RENAME`, `RENAMENX`, `COPY` and `MSETNX` touch more than one key.
Redis executes them atomically because Redis is single-threaded: no other
command can run between the read and the write.

This server runs a goroutine per connection across all cores, which is where
its read throughput and its concurrency scaling come from.
The storage interface guarantees atomicity per key, not across keys.

## Decision

Implement these commands from single-key primitives, accept that a concurrent
writer can interleave, and document the window in the code at each site and
here.

Specifically:

- `RENAME` writes the destination and then deletes the source.
  A reader between the two sees both keys.
  A crash between the two leaves both, and recovery reproduces that state
  faithfully rather than inventing one.
- `MSETNX` checks every key for existence and then writes them.
  A concurrent `SET` landing in the gap makes `MSETNX` overwrite a key that
  appeared after the check.
- `COPY` reads the source and conditionally writes the destination.
  The source can change in between.

## Consequences

Under single-writer or low-contention workloads, which is what these commands
are used for in practice, the behaviour is indistinguishable from Redis.
Under concurrent writes to the same key pair it is not, and a program relying on
`MSETNX` as a distributed lock across several keys would be wrong.

Single-key compare-and-set is unaffected and fully atomic.
`SET NX`, `SETNX`, `INCR`, `APPEND`, `GETDEL` and `GETEX` all run inside the
engine's per-key lock, which is why 8000 concurrent `INCR`s against one key lose
nothing, tested in `test/e2e`.
`SET NX` remains a correct distributed lock.

## How it gets closed

Not with a global lock, which would cost exactly the multi-core throughput the
design exists for.

The replicated command log in the consensus phase provides a total order over
writes by construction: every write goes through the leader's log, so a
multi-key command becomes one log entry applied as a unit.
Multi-key atomicity arrives as a consequence of consensus rather than as a
separate mechanism, which is the right time to build it.

## Alternatives rejected

**A global write lock.**
Correct, trivial, and it makes the server single-threaded for writes.
It would erase the 1.12x advantage over Redis at 500 connections that is the
main argument for this architecture.

**Ordered multi-key locking in the command layer.**
Correct and deadlock-free if locks are always taken in sorted key order.
Rejected for now because it only works while all keys live in one process; the
sharding phase moves keys to different nodes, and the mechanism that solves it
there solves it here too.
Building the local version first means building it twice.

**Not implementing these commands.**
`RENAME` and `COPY` are common enough that omitting them is a bigger
compatibility gap than the narrow race is a correctness gap, given the race is
documented.
