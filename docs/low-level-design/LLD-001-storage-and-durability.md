# LLD-001: Storage layout, on-disk formats and concurrency

- Status: implemented
- Phases: 2 and 3
- Related: [HLD-001](../high-level-design/HLD-001-single-node-architecture.md), [ADR-001](../adr/ADR-001-durability-model.md), [ADR-002](../adr/ADR-002-pluggable-storage-boundary.md)

## Package and crate layout

```
internal/
  resp/        RESP2/RESP3 codec. Reader is specialised for requests,
               Writer for replies, ReplyReader for the client side.
  command/     67 commands, the dispatch table, glob matching.
  server/      accept loop, connection lifecycle, deadlines, batching.
  store/       the storage interface. No Redis semantics.
    memory/    1024 sharded maps.
    lsm/       gRPC client for the Rust engine.
  wal/         segmented write-ahead log and recovery.
  snapshot/    point-in-time images.
  persist/     composes memory + wal + snapshot into one durable engine.
  bench/       load generator and latency histogram.

engine/src/
  types.rs     Record, Entry, Mutation.
  coding.rs    varints, length-prefixed bytes, CRC-32C.
  bloom.rs     Bloom filter.
  memtable.rs  sorted in-memory write buffer.
  sstable.rs   writer and reader for immutable on-disk tables.
  manifest.rs  which SSTables exist, and on which level.
  wal.rs       the engine's own write-ahead log.
  db.rs        the LSM tree: reads, flush, leveled compaction.
  service.rs   the gRPC surface.
```

## On-disk formats

### Go write-ahead log

Segment files `<dir>/wal/000000000001.wal`, rotated at 64 MiB.

```
segment header (28 bytes)
  magic "RFSWAL\x01\x00"        8
  first LSN                      8   little endian
  created at, Unix ms            8
  CRC-32C of the above           4

record (17 byte header + payload)
  CRC-32C                        4   covers everything after itself
  payload length                 4
  type                           1   put | delete | flushall | batch
  LSN                            8
  payload                        n
```

The checksum covers the length field as well as the payload.
A torn write that corrupted only the length would otherwise be undetectable and
would desynchronise every record after it.

Payloads are uvarint-prefixed. A put is `key, expireAt (zigzag varint), value`.
A batch is a count followed by tagged sub-records, framed as one record so a
crash cannot leave a multi-key write half applied.

### Snapshot

`<dir>/snapshot/dump.rfs`, written to a temporary file, fsynced, renamed, and
then the directory is fsynced.

```
header (32 bytes)
  magic "RFSSNAP\x01"            8
  format version                 4
  created at, Unix ms            8
  LSN this image is safe from    8
  CRC-32C of the above           4

body      repeated: key, expireAt, value, all length-prefixed

footer (28 bytes)
  entry count                    8
  body length                    8
  CRC-32C of the body            4
  magic "RFSSEND\x01"            8
```

The footer is read first, from a fixed offset at the end.
It carries the body length, which is what makes the body unambiguously
delimited rather than "read until it looks like a footer".

### SSTable

```
  data block 0 ... data block n     entries, sorted, ~4 KiB each
  bloom block                       one filter over every key in the file
  index block                       min key, max key, then per data block:
                                    last key, offset, length
  footer (56 bytes, fixed)          where the bloom and index blocks are
```

Every block ends with its own CRC-32C.
A checksum over the whole file would be cheaper to write and useless to read:
verifying it would mean reading the entire file to answer one point lookup.

The index and bloom blocks come *after* the data because a writer streams data
out before it knows how many entries there will be, and rewriting the head of a
finished file would turn an append into a read-modify-write.

Block size is 4 KiB to match a filesystem block, so a lookup that misses the
cache costs one physical read.
The writer cuts a block *after* passing the target rather than before, so a
single entry larger than the block size still gets a block of its own.

### Manifest

`<dir>/MANIFEST`, the whole level layout, rewritten atomically on every change.

An SSTable is invisible until the manifest names it and stops existing when it
stops naming it.
That indirection is what makes a flush or compaction atomic: write and fsync the
new files, replace the manifest by rename, then delete what it superseded.
A crash at any point leaves a manifest naming a complete, consistent set, with
at worst orphans that startup sweeps away.

Rewriting the whole thing rather than appending incremental edits is the
simplification. Edits are the better design at scale; at these file counts the
rewrite is a few kilobytes and buys a format with no replay logic in it at all,
which is one fewer place for a recovery bug to hide.

## Concurrency

### Go server

One goroutine per connection. Bounded by a buffered channel of slots, so a
connection over the limit is refused at accept time rather than queued into
memory the server does not have.

Shutdown cancels a context *and* closes every connection explicitly.
Cancelling alone is not enough: a goroutine blocked in a socket read does not
observe a context, and closing the socket is what actually unblocks it.

### Memory engine locks

1024 shards, each with an `RWMutex`, padded to keep adjacent shards off the
same cache line.

The shard count is also the SCAN cursor granularity.
A cursor that names a shard rather than a hash-bucket position gives SCAN its
"present throughout, returned at least once" guarantee for free, but only if one
shard fits in one reply, which is why 1024 rather than 64.

### Journalling under the key

```
shard.Lock()
  lsn := journal.LogPut(key, record)   // memcpy into a buffer
  shard.apply(key, record)
shard.Unlock()
journal.Await(lsn)                     // fsync, outside the lock
```

The append is inside the lock and the fsync is outside.
Inside, because two writers to the same key must take log sequence numbers in
the same order they apply to memory ([ADR-001](../adr/ADR-001-durability-model.md)).
Outside, because holding a shard lock across an fsync would serialise every
other key behind a syscall that can take milliseconds.

### Group commit

Two locks in the log, never held together in a way that can invert:

- `mu` guards the append buffer and the LSN counter. Held for a memcpy.
- `flushMu` guards the file and is the group-commit gate. Writers queue on it,
  and whichever gets in flushes everything buffered so far.

A lock-free fast path compares the last appended LSN to the last written one
before touching `flushMu` at all, so a read-only workload's per-batch flush call
costs one atomic load rather than a contended mutex.

Measured: 32 concurrent synced writers cost fewer than 32 fsyncs, asserted in
`internal/wal` and `engine/src/wal.rs`.

### LSM engine lock order

```
compaction -> manifest -> version -> memtables -> log
```

Writers only ever take `memtables` then `log`, a suffix of that order, so a
writer can never hold a lock a background job wants while waiting for one the
background job holds.

Level-0 backpressure stalls writers on a condition variable with a ten-second
escape hatch: a stall that long means compaction is not keeping up, and a stuck
writer is harder to diagnose than a deep level 0.

## Recovery procedures

### Go engine startup

1. Load `snapshot/dump.rfs` if present; note the LSN it is safe from.
   Absent is a first boot, not an error.
2. Replay every log record with a higher LSN.
3. On a checksum failure or short read in the **last** segment: truncate there,
   fsync the truncation, fsync the directory, log a warning naming file and
   offset, continue.
4. On the same failure anywhere earlier: refuse to start.
5. Open a **new** segment for appends. Never append to a file recovery may have
   just truncated; this removes a whole class of "wrote past the truncation
   point" bug for the cost of one extra file per restart.

### LSM engine startup

1. Load the manifest, validating that levels below zero do not overlap.
   A tree whose deep levels overlap returns whichever value the read path finds
   first, which is a silent wrong answer rather than a crash.
2. Replay every log at or above the manifest's log number into a fresh memtable.
3. Open a new log; record it in the manifest.
4. Sweep `.sst` files no manifest references and `.log` files numbered above the
   new one. They are unreachable by construction, and leaving them would let a
   crash loop fill the disk.

### Checkpoint

1. Read the current LSN. **Before** the walk, never after.
2. Walk the keyspace shard by shard, writing to a temporary file.
3. fsync, rename, fsync the directory.
4. Only now, trim log segments fully superseded by that LSN.

Reversing steps 3 and 4 is the classic way to build a database that loses
everything on exactly one unlucky reboot.

## Read path in the LSM tree

Newest source first, and the first source with an opinion wins.
A tombstone is an opinion: it stops the search rather than falling through to an
older SSTable that still holds the old value.

1. active memtable
2. frozen memtables, newest first
3. level 0, newest first, because those files overlap
4. levels 1 to 6, at most one file each, found by binary search

Each candidate file is gated twice before any I/O: a key-range check, then the
Bloom filter. Measured in `engine/tests/lsm.rs`: filters reject more lookups
than reach a block read.

## Compaction

Level 0 is scored by file count because its cost is paid on every read.
Deeper levels are scored by bytes, where the cost is space amplification.

A level-0 compaction takes **all** level-0 files, since they overlap each other
and taking a subset would leave two files at different levels holding different
versions of one key with no ordering between them.

A tombstone or expired record is dropped only when no level below could still
hold an older value for that key. Dropping early would resurrect it.

The output is cut into bounded files so that a later compaction of that level
does not have to rewrite all of it.

Measured: twelve rounds of overwriting the same 400 keys settle at under 4x the
live data size.

## Two bugs this design already caught

Recorded because the reasoning is more useful than the fix.

**The paged scan was O(n²).** Each page collected whole files before merging.
Bounding each source to `limit + 1` entries fixed the complexity, because any
key among the globally smallest N is also among the smallest N of its own
source.

**That bound then let the merge silently drop keys.** A source cut off at
`limit + 1` was read no further than its own last key, so beyond the *smallest*
such key the merged set could be missing entries. The scan now tracks that
boundary and resumes from it. It emitted no error and lost 367 of 1500 keys;
the property test that caught it asserts the full walk returns exactly the
expected set, which is the kind of assertion a spot check would have passed.
