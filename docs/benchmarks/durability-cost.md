# redis-from-scratch: the cost of fsync-before-acknowledge

Generated 2026-08-25T14:22:14Z on darwin/arm64, 14 logical CPUs, go1.26.4.

> **The load generator ran on the same machine as the server.** Client and server therefore competed for the same CPUs, and there was no network. Treat these as a relative comparison between configurations, not as an absolute capacity figure for a deployed node.

| run | conns | pipe | ops/sec | p50 us | p99 us | p99.9 us | max us | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| set P=1 fsync=always | 50 | 1 | 3,714 | 12124 | 33817 | 45089 | 61117 | 0 |

## How to read this

- Latency is measured client side, from the moment a request was due to be sent until its reply was fully read.
- Runs with a pipeline depth above 1 report **batch** latency, attributed to each command in the batch. A depth of 16 at 800 us does not mean each command took 800 us; it means a batch of 16 did.
- Runs with a target rate are open loop: latency is measured from the intended send time, so a stall shows up instead of being hidden by the client pausing with the server.
- Percentiles come from a log-linear histogram of every sample, not a reservoir, so the tail is real and not a sampling artefact. Bucket width bounds the error at 0.8%.

## Run details

### set P=1 fsync=always

- workload `set`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 1, 8.0s measured, 29,758 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `aof_appends=42446`, `aof_enabled=1`, `aof_fsync_policy=always`, `aof_fsyncs=2088`, `aof_writes=2176`, `connected_clients=50`, `db0=keys=27688,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=0`, `keyspace_misses=0`, `pipeline_batches=0`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory+wal`, `total_commands_processed=42447`, `used_memory_human=11.24M`, `wal_unsynced_records=0`

