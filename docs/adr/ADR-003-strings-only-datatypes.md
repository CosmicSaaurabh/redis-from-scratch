# ADR-003: Strings only, not half-implemented containers

- Status: accepted
- Date: 2026-08-25
- Phase: 1

## Context

Redis has strings, lists, hashes, sets, sorted sets, streams and more.
`redis-benchmark` exercises several of them, so supporting lists and hashes
would make the benchmark output look more complete and the server feel more
finished.

## Decision

Strings only, with the full string command set and the full generic keyspace.
`TYPE` returns `string` or `none` and nothing else.

## Consequences

The datatype surface is honest: every command that exists works completely and
matches Redis byte for byte, verified in `test/compat`.
There is no partially-implemented `LPUSH` that works until someone uses
`LINSERT`.

The storage boundary stays clean.
Values are opaque bytes, which is what lets the same interface sit in front of
an in-process map and an LSM tree without either knowing what a list is.

The main cost is that this server cannot replace a Redis instance that uses
containers, which is most of them.

## Alternatives rejected

**Lists and hashes stored as one serialised blob per key.**
This is the tempting shortcut and it is a performance trap.
`LPUSH` on a thousand-element list would read, decode, prepend, encode and
write the entire list: an O(n) write for an operation whose whole appeal is
that it is O(1).
Against the LSM engine it would also write the full list into the log and a new
SSTable entry on every push, turning a small append into kilobytes of write
amplification.
Something that carries the name of a Redis command while having the wrong
complexity is worse than not having it.

**Lists and hashes as first-class engine types.**
The correct design, and a substantial amount of work in both languages: the
engine would need to understand structure, compaction would need to merge
partial updates, and the gRPC contract would grow a type system.
Deferred rather than rejected. It belongs after consensus, not before it.

**Accepting the commands and returning an error.**
Rejected because a client library that probes for capability by issuing a
command would see a runtime error rather than a clean "unknown command", which
is harder to diagnose than the honest answer.
