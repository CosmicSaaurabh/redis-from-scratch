# Benchmark results

Every number here was produced by `scripts/bench-suite.sh`, which measures each
configuration with the same harness, the same workload and the same machine, and
measures a real `redis-server` alongside as a baseline. A throughput figure with
nothing beside it says almost nothing; the point of the suite is that every
number can be compared to another one produced under identical conditions.

## How this was measured

- **Machine**: darwin/arm64, 14 logical CPUs, go1.26.4.
- **Load generator ran on the same machine as the server.** Client and server
  competed for the same CPUs and there was no network. These are relative
  comparisons between configurations, not capacity figures for a deployed node.
- 50 connections, 200,000 distinct keys, 64 byte values, uniform key distribution.
- 8 seconds measured per configuration after a 3 second warmup that is discarded.
- Latency percentiles come from a log-linear histogram of **every** sample, not a
  reservoir, so the tail is real rather than a sampling artefact.
- Runs at pipeline depth above 1 report **batch** latency attributed to each
  command in the batch. A depth of 32 at 1000 us does not mean each command took
  1000 us; it means a batch of 32 did.
- The baseline `redis-server` runs with **persistence disabled**, which is the
  fastest it can be configured. Comparisons against the durable configurations
  below are therefore unfavourable to this server on purpose.

## Throughput, operations per second

| workload | this server (WAL everysec) | this server (cache) | Rust LSM engine | redis-server (no persistence) | vs redis |
|---|---:|---:|---:|---:|---:|
| get P=1 | 145,352 | 141,595 | 47,397 | 140,584 | 1.03x |
| set P=1 | 119,338 | 137,881 | 22,454 | 139,515 | 0.86x |
| mixed P=1 | 139,276 | 132,640 | 31,742 | 139,821 | 1.00x |
| get P=8 | 1,195,258 | 897,516 | 56,436 | 861,118 | 1.39x |
| set P=8 | 832,666 | 1,101,554 | 27,656 | 667,093 | 1.25x |
| mixed P=8 | 888,249 | 954,136 | 36,675 | 819,179 | 1.08x |
| get P=32 | 3,487,965 | 3,373,124 | 55,578 | 1,825,344 | 1.91x |
| set P=32 | 1,224,755 | 3,825,212 | 27,505 | 1,500,354 | 0.82x |
| mixed P=32 | 2,459,773 | 3,548,654 | 37,752 | 1,532,174 | 1.61x |

## Scaling with connection count, unpipelined mixed workload

| connections | this server | redis-server | vs redis |
|---|---:|---:|---:|
| 10 | 96,303 | 114,225 | 0.84x |
| 50 | 134,008 | 134,240 | 1.00x |
| 200 | 156,643 | 141,067 | 1.11x |
| 500 | 155,910 | 139,286 | 1.12x |

## Latency, unpipelined

| workload | server | p50 us | p99 us | p99.9 us |
|---|---|---:|---:|---:|
| get P=1 | this server | 324 | 827 | 1,958 |
| get P=1 | redis-server | 334 | 791 | 3,670 |
| set P=1 | this server | 346 | 1,565 | 9,175 |
| set P=1 | redis-server | 340 | 705 | 2,769 |
| mixed P=1 | this server | 311 | 1,081 | 6,128 |
| mixed P=1 | redis-server | 342 | 758 | 2,073 |

## What the numbers say

**Reads match or beat Redis, and the gap widens with pipelining.** At depth 32 this
server serves 3,487,965 GET/sec against Redis's 1,825,344. Redis is single-threaded by
design; this server runs a goroutine per connection across all cores, and once
pipelining removes the per-command round trip that difference is what is left.

**Throughput holds as connections grow, where Redis flattens.** From 50 to 500
connections this server goes 134,008 -> 155,910 ops/sec while Redis goes
134,240 -> 139,286. Same reason: more cores to spread across.

**Writes pay for durability, and the size of that payment is the point.**

| write configuration | SET/sec, unpipelined | what a crash costs |
|---|---:|---|
| persistence off | 137,881 | everything |
| WAL, fsync everysec | 119,338 | up to 1 second on power loss, nothing on a process kill |
| WAL, fsync before acknowledge | 3,714 | nothing that was acknowledged |

Fsyncing before every acknowledgement costs roughly
32x throughput on this machine. That is not a tuning
problem to be optimised away: it is the cost of asking a physical disk to confirm
a write before telling a client it happened, and the group-commit path already
amortises one fsync across every writer waiting at that moment. The number is
published so the trade can be made deliberately rather than discovered later.

**The LSM engine is much slower per operation, and that is the expected shape.**
It serves 47,397 GET/sec against the in-process engine's 145,352. Every
command crosses a process boundary over gRPC, which costs tens of microseconds
that an in-process map lookup does not pay. What it buys is a dataset that is not
bounded by RAM: the in-process engine holds every key and value in memory, while
the LSM tree keeps only the memtable, the index blocks and the Bloom filters
resident and leaves the data on disk. Choosing between them is a question about
dataset size, not about which benchmark is larger. See
[ADR-002](../adr/ADR-002-pluggable-storage-boundary.md).

## Reproducing this

```sh
make bench          # the full suite, roughly 15 minutes
scripts/bench-suite.sh 30s docs/benchmarks   # longer runs, less noise
```

The per-configuration reports carry their own methodology and the server's own
`INFO` counters at the end of each run:

- [memory-everysec.md](memory-everysec.md) - in-process engine, WAL fsync everysec
- [memory-cache.md](memory-cache.md) - in-process engine, persistence disabled
- [durability-cost.md](durability-cost.md) - fsync before acknowledge
- [lsm-engine.md](lsm-engine.md) - Rust LSM engine over gRPC
- [redis-baseline.md](redis-baseline.md) - real redis-server, same harness

## What these numbers are not

- Not a capacity plan. The client shared a machine with the server; a real
  deployment has a network between them, which dominates unpipelined latency.
- Not a claim that this server is faster than Redis. It is faster at some things
  on a 14-core laptop, mostly by using cores Redis does not use, and slower at
  durable writes because it is doing work Redis was configured not to do.
- Not stable across machines. Absolute figures move with core count, disk and
  kernel. The *ratios* between configurations are what carry over.
