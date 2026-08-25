# ADR-001: Durability is a write-ahead log plus fuzzy snapshots

- Status: accepted
- Date: 2026-08-25
- Phase: 2

## Context

The server must survive `kill -9` and a power cut without losing data a client
was told had been written.
Two mechanisms were candidates, and most real systems use both.

A **snapshot** is a periodic image of the whole keyspace.
It is cheap to load and bounds how long recovery takes, but everything written
since the last one is gone.

A **write-ahead log** records every mutation before it is applied.
It loses nothing, but replaying it from the beginning of time gets slower on
every restart, and it grows without bound.

Neither is sufficient alone.
The question was how to combine them, and specifically how to take a snapshot
without stopping writes for its duration.

## Decision

Both, composed in one specific order.

**The log records physical mutations, not commands.**
An append-only log of "SET k v", "INCRBYFLOAT k 0.1", "EXPIRE k 100" replays to
a *different* state than the original execution produced, because floating
point is not associative and because a relative TTL replayed hours later
expires at a different instant.
The log records "key k now holds exactly these bytes and expires at this exact
Unix millisecond", which cannot drift.

**Journalling happens while the engine holds the key.**
This is the load-bearing detail.
If the log append happened outside the key's lock, two writers to the same key
could be assigned log sequence numbers in one order and apply to memory in the
other: `SET k=A` takes LSN 5, `INCR k` takes the lock, reads the old value,
takes LSN 6, applies, and only then does the `SET` apply.
Memory ends at `A`, the log replays to `old+1`, and the database returns a
different answer after every restart.
Journalling under the key makes the two orders the same order by construction.

**Snapshots are fuzzy and paired with an LSN taken before the walk.**
Taking a consistent image means either forking, which Go cannot do safely, or
blocking every writer for the length of the walk.
Instead the walk proceeds shard by shard while writes continue, and the image
is paired with the log sequence number observed *before* iteration started.
Recovery loads the image and then replays every log record after that LSN,
which repairs anything the walk raced with.

The property that makes this sound is that log records are idempotent physical
mutations: replaying "key k holds exactly these bytes" over a value the
snapshot already captured is a no-op, not a double-apply.

**Recovery order is snapshot, then log tail.**
Trimming happens only after the snapshot is fsynced and renamed into place.

## Consequences

Recovery is bounded by snapshot size plus the log written since it, rather than
by the whole history.

A crash can only ever damage the very end of the log, because the log is
append-only and the kernel writes forward.
So a checksum failure in the final record is an interrupted write: expected,
benign, and repaired by truncating it away.
A failure anywhere earlier is real corruption, and recovery refuses to guess,
because silently skipping a bad record in the middle would resurrect whatever
the following records overwrote.
Both cases are tested at five truncation points and with single-byte flips.

Taking the LSN *after* the walk instead of before would be a silent data-loss
bug: the image would claim to cover writes it may have missed, and recovery
would skip exactly the records needed to fill the gap.
That ordering is the single most fragile line in the design and is commented as
such at the call site.

## Alternatives rejected

**Command logging, the way Redis AOF works.**
Redis makes it work by rewriting non-deterministic commands as it logs them -
`EXPIRE` becomes `PEXPIREAT`, `SPOP` becomes `SREM`.
That is a per-command correctness obligation that grows with the command
surface and is easy to forget when adding a command.
Physical logging is deterministic with no per-command work at all.

**Snapshot only, no log.**
Simpler and much faster to write, but every configuration is "lose up to the
snapshot interval", which is not a durability story.

**Log only, no snapshots.**
Correct, but restart time grows without bound and the disk fills.

**Stop-the-world snapshots.**
Correct and simple, at the cost of a multi-second stall on a large keyspace.
The fuzzy approach costs one extra invariant to reason about and stalls nothing.

## Verification

`internal/persist` tests a fuzzy snapshot under eight concurrent writers with
six overlapping snapshots and asserts the recovered key count exactly equals
the acknowledged write count.
`test/crash` kills the real process with SIGKILL under both fsync policies,
during pipelined writes, during sustained concurrent traffic, and five times in
a row, and checks that nothing acknowledged was lost.
