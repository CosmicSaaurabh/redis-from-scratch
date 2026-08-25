# ADR-004: One database, not sixteen

- Status: accepted
- Date: 2026-08-25
- Phase: 1

## Context

Redis ships sixteen numbered logical databases selected with `SELECT`.
They share one server, one memory budget and one persistence file, and they are
widely regarded as a design Redis would not repeat.

## Decision

One database.
`SELECT 0` succeeds; any other index returns an error naming this decision.
`SWAPDB` and `MOVE` are not implemented.
`COPY ... DB n` accepts only `0`.

## Consequences

The write-ahead log, the snapshot format and the gRPC contract carry no
database index.
Adding one later would be a format change to all three, which is precisely why
this is a decision recorded now rather than an omission discovered later.

This matches where the project is going.
**Redis Cluster itself supports only database 0**, because routing a key to a
shard and then to a numbered database inside that shard is ambiguous.
A project whose stated destination is a sharded cluster would have to remove
multiple databases before it got there.

Applications that use numbered databases for namespacing must use key prefixes
instead, which is what the Redis documentation already recommends.

`INFO keyspace` reports a single `db0` line, and `CONFIG GET databases` returns
`1`, so a client that inspects the server sees the truth rather than a lie that
happens to be compatible.

## Alternatives rejected

**Sixteen databases, implemented as sixteen store instances.**
Cheap for the memory engine, and it would mean sixteen memtables, sixteen sets
of SSTables and sixteen compaction schedules in the LSM engine, for a feature
Redis Cluster forbids anyway.

**One store with the database index prefixed onto every key.**
Cheaper still, and it puts a number nobody asked for inside every key on disk,
inside every log record, and inside the range boundaries compaction reasons
about. `FLUSHDB` becomes a range delete. All of that to emulate a feature the
target architecture cannot use.

**Accepting `SELECT n` silently and ignoring it.**
The worst option. A client that selected database 3 and then read database 0's
data would see corruption it could not explain.
