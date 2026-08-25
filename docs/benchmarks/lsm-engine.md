# redis-from-scratch: Rust LSM engine over gRPC

Generated 2026-08-25T14:28:10Z on darwin/arm64, 14 logical CPUs, go1.26.4.

> **The load generator ran on the same machine as the server.** Client and server therefore competed for the same CPUs, and there was no network. Treat these as a relative comparison between configurations, not as an absolute capacity figure for a deployed node.

| run | conns | pipe | ops/sec | p50 us | p99 us | p99.9 us | max us | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| get P=1 | 50 | 1 | 47,397 | 1028 | 1892 | 2687 | 14198 | 0 |
| get P=8 | 50 | 8 | 56,436 | 6881 | 12845 | 32768 | 36310 | 0 |
| get P=32 | 50 | 32 | 55,578 | 28574 | 32637 | 37224 | 42037 | 0 |
| set P=1 | 50 | 1 | 22,454 | 2089 | 5210 | 14287 | 28766 | 0 |
| set P=8 | 50 | 8 | 27,656 | 14156 | 21365 | 30278 | 35082 | 0 |
| set P=32 | 50 | 32 | 27,505 | 56361 | 111149 | 117441 | 121724 | 0 |
| mixed P=1 | 50 | 1 | 31,742 | 1434 | 4719 | 17302 | 30765 | 0 |
| mixed P=8 | 50 | 8 | 36,675 | 10748 | 16318 | 21103 | 29931 | 0 |
| mixed P=32 | 50 | 32 | 37,752 | 41943 | 52953 | 58720 | 61939 | 0 |
| mixed c=10 | 10 | 1 | 23,130 | 389 | 1237 | 5407 | 19594 | 0 |
| mixed c=50 | 50 | 1 | 33,017 | 1434 | 3097 | 6095 | 10966 | 0 |
| mixed c=200 | 200 | 1 | 35,759 | 5177 | 14287 | 23331 | 49335 | 0 |
| mixed c=500 | 500 | 1 | 34,743 | 12255 | 51905 | 81265 | 176380 | 0 |

## How to read this

- Latency is measured client side, from the moment a request was due to be sent until its reply was fully read.
- Runs with a pipeline depth above 1 report **batch** latency, attributed to each command in the batch. A depth of 16 at 800 us does not mean each command took 800 us; it means a batch of 16 did.
- Runs with a target rate are open loop: latency is measured from the intended send time, so a stall shows up instead of being hidden by the client pausing with the server.
- Percentiles come from a log-linear histogram of every sample, not a reservoir, so the tail is real and not a sampling artefact. Bucket width bounds the error at 0.8%.

## Run details

### get P=1

- workload `get`, key distribution `uniform`, 50,000 keys, 64 byte values
- 50 connections, pipeline depth 1, 8.0s measured, 379,221 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `connected_clients=50`, `db0=keys=50000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=504052`, `keyspace_misses=0`, `pipeline_batches=350`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=lsm`, `total_commands_processed=554053`, `used_memory_human=4.53M`

### get P=8

- workload `get`, key distribution `uniform`, 50,000 keys, 64 byte values
- 50 connections, pipeline depth 8, 8.0s measured, 451,688 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `connected_clients=50`, `db0=keys=50000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=1103724`, `keyspace_misses=0`, `pipeline_batches=75659`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=lsm`, `total_commands_processed=1203726`, `used_memory_human=5.87M`

### get P=32

- workload `get`, key distribution `uniform`, 50,000 keys, 64 byte values
- 50 connections, pipeline depth 32, 8.0s measured, 445,440 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `connected_clients=50`, `db0=keys=50000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=1717516`, `keyspace_misses=0`, `pipeline_batches=95190`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=lsm`, `total_commands_processed=1867519`, `used_memory_human=6.16M`

### set P=1

- workload `set`, key distribution `uniform`, 50,000 keys, 64 byte values
- 50 connections, pipeline depth 1, 8.0s measured, 179,656 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `connected_clients=50`, `db0=keys=50000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=1717516`, `keyspace_misses=0`, `pipeline_batches=95190`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=lsm`, `total_commands_processed=2115333`, `used_memory_human=4.76M`

### set P=8

- workload `set`, key distribution `uniform`, 50,000 keys, 64 byte values
- 50 connections, pipeline depth 8, 8.0s measured, 221,432 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `connected_clients=50`, `db0=keys=50000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=1717516`, `keyspace_misses=0`, `pipeline_batches=133174`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=lsm`, `total_commands_processed=2419206`, `used_memory_human=5.09M`

### set P=32

- workload `set`, key distribution `uniform`, 50,000 keys, 64 byte values
- 50 connections, pipeline depth 32, 8.0s measured, 220,896 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `connected_clients=50`, `db0=keys=50000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=1717516`, `keyspace_misses=0`, `pipeline_batches=142828`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=lsm`, `total_commands_processed=2728135`, `used_memory_human=4.50M`

### mixed P=1

- workload `mixed`, key distribution `uniform`, 50,000 keys, 64 byte values
- 50 connections, pipeline depth 1, 8.0s measured, 253,980 operations
- closed loop: each connection waits for its replies before sending again
- 126,797 reads, 127,183 writes
- server reported: `connected_clients=50`, `db0=keys=50000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=1894415`, `keyspace_misses=0`, `pipeline_batches=143178`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=lsm`, `total_commands_processed=3132241`, `used_memory_human=4.07M`

### mixed P=8

- workload `mixed`, key distribution `uniform`, 50,000 keys, 64 byte values
- 50 connections, pipeline depth 8, 8.0s measured, 293,664 operations
- closed loop: each connection waits for its replies before sending again
- 146,502 reads, 147,162 writes
- server reported: `connected_clients=50`, `db0=keys=50000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=2093430`, `keyspace_misses=0`, `pipeline_batches=193351`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=lsm`, `total_commands_processed=3580826`, `used_memory_human=4.85M`

### mixed P=32

- workload `mixed`, key distribution `uniform`, 50,000 keys, 64 byte values
- 50 connections, pipeline depth 32, 8.0s measured, 302,944 operations
- closed loop: each connection waits for its replies before sending again
- 151,096 reads, 151,848 writes
- server reported: `connected_clients=50`, `db0=keys=50000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=2300442`, `keyspace_misses=0`, `pipeline_batches=206661`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=lsm`, `total_commands_processed=4045547`, `used_memory_human=6.92M`

### mixed c=10

- workload `mixed`, key distribution `uniform`, 50,000 keys, 64 byte values
- 10 connections, pipeline depth 1, 8.0s measured, 185,047 operations
- closed loop: each connection waits for its replies before sending again
- 92,185 reads, 92,862 writes
- server reported: `connected_clients=10`, `db0=keys=50000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=2431953`, `keyspace_misses=0`, `pipeline_batches=207011`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=lsm`, `total_commands_processed=4359496`, `used_memory_human=2.02M`

### mixed c=50

- workload `mixed`, key distribution `uniform`, 50,000 keys, 64 byte values
- 50 connections, pipeline depth 1, 8.0s measured, 264,173 operations
- closed loop: each connection waits for its replies before sending again
- 131,827 reads, 132,346 writes
- server reported: `connected_clients=50`, `db0=keys=50000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=2611818`, `keyspace_misses=0`, `pipeline_batches=207361`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=lsm`, `total_commands_processed=4769729`, `used_memory_human=5.96M`

### mixed c=200

- workload `mixed`, key distribution `uniform`, 50,000 keys, 64 byte values
- 200 connections, pipeline depth 1, 8.0s measured, 286,195 operations
- closed loop: each connection waits for its replies before sending again
- 142,686 reads, 143,509 writes
- server reported: `connected_clients=200`, `db0=keys=50000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=2804100`, `keyspace_misses=0`, `pipeline_batches=207711`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=lsm`, `total_commands_processed=5205209`, `used_memory_human=22.39M`

### mixed c=500

- workload `mixed`, key distribution `uniform`, 50,000 keys, 64 byte values
- 500 connections, pipeline depth 1, 8.0s measured, 278,182 operations
- closed loop: each connection waits for its replies before sending again
- 139,132 reads, 139,050 writes
- server reported: `connected_clients=500`, `db0=keys=50000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=2997304`, `keyspace_misses=0`, `pipeline_batches=208061`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=lsm`, `total_commands_processed=5641807`, `used_memory_human=52.26M`

