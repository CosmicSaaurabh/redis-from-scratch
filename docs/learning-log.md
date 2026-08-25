# Learning Log

Running record of concepts learned while building Redis From Scratch, appended after each significant milestone or review.
Kept for revision before interviews.

Format per entry: date, phase/task, concepts learned, and the one thing that was most surprising.

---

## 2026-08-25 - Phases 1 to 3

### Ordering is the whole of durability

The single most important line in the persistence layer is not an fsync, it is
where the log append sits relative to the lock.

If the append happens outside the key's lock, two writers to the same key can
take log sequence numbers in one order and apply to memory in the other:
`SET k=A` takes LSN 5, `INCR k` takes the lock, reads the old value, takes LSN
6, applies, and only then does the `SET` apply. Memory ends at `A`. The log
replays to `old+1`. The database returns a different answer after every restart,
and nothing errors.

The fix is one line of placement, and it forced a design change: durability
became an engine concern rather than a decorator wrapped around the engine,
because only the engine holds the key.

The same shape appears three more times:

- The write-ahead log must reach the kernel **before** the reply reaches the
  socket. Reverse those and a client sees an acknowledgement for data that only
  exists in this process's memory.
- The snapshot LSN must be read **before** the keyspace walk, never after.
  After, and the image claims coverage of writes it may have missed.
- Log segments may be deleted only **after** the snapshot that supersedes them
  is fsynced and renamed. Reverse those and you get a database that loses
  everything on exactly one unlucky reboot in a thousand.

Every one of these is a two-line reordering that is invisible in a code review
and undetectable in testing without deliberately crashing the process.

### A crash can only damage the end of an append-only log

This asymmetry does more work than any other idea in the recovery path.

The log is append-only and the kernel writes forward, so an interrupted write
can only corrupt the final record. A checksum failure there is expected,
benign, and repaired by truncating it away. A checksum failure anywhere earlier
is something a crash could not have caused: bad hardware, a bad filesystem,
someone editing files by hand.

So recovery treats them completely differently. Torn tail: truncate, fsync the
truncation, log a warning naming the file and offset, carry on. Damage earlier:
refuse to start. Silently skipping a bad record in the middle would resurrect
whatever the following records overwrote, which is worse than not starting.

The checksum has to cover the length field, not just the payload. A torn write
that corrupted only the length would otherwise be undetectable and would
desynchronise every record after it.

### Group commit, and why the fast path matters more than the slow one

Under fsync-before-acknowledge, N concurrent writers should cost one fsync, not
N. The pattern is a leader/follower gate: writers queue on a mutex, whoever
gets in flushes everything buffered so far, and the ones behind find their
sequence number already covered and return without a second syscall.

What was not obvious: the *empty* case dominates. A read-only workload still
calls flush once per pipeline batch, and making fifty connections queue on a
mutex to each discover an empty buffer cost more than the write it was trying
to batch. Adding a lock-free comparison of "last appended" against "last
written" before touching the mutex at all was worth more than the batching.

### Go: profile before optimising, every time

Every guess about the hot path was wrong.

The guesses: per-command allocations, `strings.ToLower` on the command name,
`context.WithTimeout` per command, the reply Context struct. All fixed, all
real, and together they moved throughput by about 2%.

The profile said 48% of time was in raw syscalls, and command dispatch was
0.8%. The server is syscall-bound, not CPU-bound: one read, one write to the
log, one write to the socket, per command. That is also why pipelining is worth
25x - it amortises all three across many commands.

The second lesson was worse. The first "we are at 55% of Redis" measurement was
an artifact: `redis-benchmark` is single-threaded by default and was itself the
bottleneck. With `--threads 4` this server *beat* Redis on the same workload. A
benchmark whose client is saturated measures the client.

### Rust: ownership pushes decisions earlier

`BufWriter::into_inner()` on a type that implements `Drop` does not compile,
because `Drop` must still see a whole value. The fix is to flush and sync
through a reference. That is a small thing, but it is the compiler refusing to
let a resource be half-moved, which is exactly the class of bug that produces a
file descriptor leak in a language that allows it.

`Arc<[u8]>` rather than `Vec<u8>` for stored values: written once, read many
times, shared with a background flush, handed to readers. Reference counting it
means none of those paths copies. Choosing this is choosing a sharing policy at
the type level, and it is checked.

The `Drop` implementation on the SSTable writer removes a partial file if
`finish()` was never called. A failing compaction would otherwise leave debris
on every attempt until the disk filled. In Go this would be a `defer` someone
forgets; in Rust it is a property of the type.

### Bloom filters: a false positive costs a read, a false negative loses data

A Bloom filter can say "definitely not here" and "probably here", never
"definitely here". That asymmetry is the entire reason it is safe as a skip
decision: a false positive wastes one block read, and a false negative would
lose data.

Double hashing (Kirsch and Mitzenmacher) simulates k independent hashes as
`h1 + i*h2` with the same asymptotic error rate while computing one. At ten bits
per key the error rate is about 1%, which is where halving it again starts
costing more memory than the saved I/O is worth.

Measured in the engine's tests: filters reject more lookups than reach a block
read, which is the only way to know the filter is earning its space.

### The LSM read path is an ordering argument, not a search

The first source with an *opinion* wins, and a tombstone is an opinion. That
distinction is the one that matters. "This table does not mention the key" must
continue the search down the tree; "this table says the key was deleted" must
stop it. Collapsing the two into a single `Option` resurrects deleted data.

The same idea governs compaction: a tombstone may only be discarded once no
level below could still hold an older value for that key. Dropping it early is
how deleted data comes back.

### The two bugs the tests caught, and why spot checks would have missed both

The paged scan collected whole files per page, making it O(n²). Bounding each
source to `limit + 1` entries fixed the complexity, because any key among the
globally smallest N is also among the smallest N of its own source.

That bound then introduced a silent data-loss bug. A source cut off at
`limit + 1` was read no further than its own last key, so beyond the *smallest*
such key the merged set could be missing entries the scan then skipped past. It
emitted no error and lost 367 of 1500 keys.

Neither would have been found by spot-checking a few keys. What found them was a
test asserting the *complete* expected set after a full walk. The lesson is
about the shape of the assertion, not about the bug.

### Differential testing beats hand-written expectations

Hand-written expectations encode what you believe Redis does. Running the same
commands against a real `redis-server` and comparing byte for byte encodes what
it actually does.

That suite found four real bugs in an hour, including `ParseInt` being unable to
represent `-9223372036854775808` - because it accumulated in `int64` and negated
at the end, so the most negative value, whose magnitude is one past the largest
positive one, was rejected. `INCR` and `DECR` behaved differently at the two
ends of the range and every hand-written test had passed.

It also found something more interesting: **Redis does not agree with itself
across platforms.** `INCRBYFLOAT` accumulates in C `long double`, which is 80
bits on x86-64 Linux and 64 bits on arm64 macOS, so the same expression prints
`10.6` on one and `10.59999999999999964` on the other. Byte-exact agreement with
"Redis" is not a well-defined goal.

### Injected clocks are for semantics, not for the operating system

The end-to-end harness froze the clock so TTL tests would not need to sleep.
Every test then timed out, because the server was computing *socket deadlines*
from the same injected clock and arming the kernel with a deadline three years
in the past.

The rule that came out of it: an injected clock is for decisions the program
makes about time. A socket deadline is an instruction to the operating system
and belongs to real monotonic time. Mixing them is not a testing problem, it is
a layering problem that testing exposed.

### FLUSHALL and the meaning of "see everything"

The LSM engine's wipe scanned the keyspace at `i64::MAX`, intending "see
everything". It means the opposite: the scan resolves expiry against the clock
it is given, so at `i64::MAX` every key with a TTL looked expired, was skipped,
never got a tombstone, and came back alive on the next read against the real
clock.

Found by typing `FLUSHALL` into `redis-cli` and noticing one key still there.
Not by a test - by using the thing.

### Redis is single-threaded, and that is the whole comparison

Nearly every performance difference reduces to this. Redis serialises all
command execution on one core, which is why it needs no locks and why its
latency is so consistent. This server runs a goroutine per connection across all
cores, which is why it wins on pipelined reads (1.91x) and at 500 connections
(1.12x), and why it needs an explicit answer to every ordering question Redis
gets for free.

That is also the source of the one honest compatibility gap: multi-key commands
like `RENAME` are atomic in Redis by construction and are not here. The fix is
not a global lock, which would give back the entire advantage. It is a
replicated command log, which provides a total order as a side effect - which is
to say the answer arrives with consensus, in the next phase.

### Concepts to revise

fsync versus write and what the page cache survives · fsync on a directory for
rename durability · torn writes and why append-only logs bound the damage ·
CRC-32C and hardware checksums · group commit · fuzzy checkpoints and why
idempotent physical log records make them sound · LSM read amplification and
level-0 overlap · leveled compaction and tombstone lifetime · Bloom filter false
positive rate as a function of bits per key · write, read and space
amplification as a three-way trade · Go scheduler behaviour under netpoll ·
`-race` and the happens-before edges it can and cannot see · coordinated
omission · Rust `Drop` and partial moves · `Arc<[u8]>` as a sharing policy.
