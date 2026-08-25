# redis-from-scratch: in-process engine, WAL fsync everysec

Generated 2026-08-25T14:22:02Z on darwin/arm64, 14 logical CPUs, go1.26.4.

> **The load generator ran on the same machine as the server.** Client and server therefore competed for the same CPUs, and there was no network. Treat these as a relative comparison between configurations, not as an absolute capacity figure for a deployed node.

| run | conns | pipe | ops/sec | p50 us | p99 us | p99.9 us | max us | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| get P=1 | 50 | 1 | 145,352 | 324 | 827 | 1958 | 25083 | 0 |
| get P=8 | 50 | 8 | 1,195,258 | 309 | 827 | 2097 | 10258 | 0 |
| get P=32 | 50 | 32 | 3,487,965 | 385 | 1679 | 6521 | 58948 | 0 |
| set P=1 | 50 | 1 | 119,338 | 346 | 1565 | 9175 | 56182 | 0 |
| set P=8 | 50 | 8 | 832,666 | 481 | 1466 | 7307 | 23706 | 0 |
| set P=32 | 50 | 32 | 1,224,755 | 963 | 9503 | 33292 | 92352 | 0 |
| mixed P=1 | 50 | 1 | 139,276 | 311 | 1081 | 6128 | 41380 | 0 |
| mixed P=8 | 50 | 8 | 888,249 | 373 | 2097 | 10682 | 62238 | 0 |
| mixed P=32 | 50 | 32 | 2,459,773 | 647 | 2179 | 11338 | 54830 | 0 |
| mixed c=10 | 10 | 1 | 96,303 | 94 | 220 | 934 | 30229 | 0 |
| mixed c=50 | 50 | 1 | 134,008 | 313 | 1245 | 6324 | 66581 | 0 |
| mixed c=200 | 200 | 1 | 156,643 | 1180 | 3015 | 8126 | 43951 | 0 |
| mixed c=500 | 500 | 1 | 155,910 | 2998 | 8913 | 25952 | 151640 | 0 |

## How to read this

- Latency is measured client side, from the moment a request was due to be sent until its reply was fully read.
- Runs with a pipeline depth above 1 report **batch** latency, attributed to each command in the batch. A depth of 16 at 800 us does not mean each command took 800 us; it means a batch of 16 did.
- Runs with a target rate are open loop: latency is measured from the intended send time, so a stall shows up instead of being hidden by the client pausing with the server.
- Percentiles come from a log-linear histogram of every sample, not a reservoir, so the tail is real and not a sampling artefact. Bucket width bounds the error at 0.8%.

## Run details

### get P=1

- workload `get`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 1, 8.0s measured, 1,162,854 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `aof_appends=200000`, `aof_enabled=1`, `aof_fsync_policy=everysec`, `aof_fsyncs=1`, `aof_writes=1400`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=1546176`, `keyspace_misses=0`, `pipeline_batches=1400`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory+wal`, `total_commands_processed=1746177`, `used_memory_human=49.73M`, `wal_unsynced_records=0`

### get P=8

- workload `get`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 8, 8.0s measured, 9,562,472 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `aof_appends=400000`, `aof_enabled=1`, `aof_fsync_policy=everysec`, `aof_fsyncs=3`, `aof_writes=2802`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=14783184`, `keyspace_misses=0`, `pipeline_batches=1657426`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory+wal`, `total_commands_processed=15183186`, `used_memory_human=57.32M`, `wal_unsynced_records=0`

### get P=32

- workload `get`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 32, 8.0s measured, 27,905,280 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `aof_appends=600000`, `aof_enabled=1`, `aof_fsync_policy=everysec`, `aof_fsyncs=4`, `aof_writes=4202`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=53515120`, `keyspace_misses=0`, `pipeline_batches=2869199`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory+wal`, `total_commands_processed=54115123`, `used_memory_human=49.41M`, `wal_unsynced_records=0`

### set P=1

- workload `set`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 1, 8.0s measured, 954,729 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `aof_appends=1876137`, `aof_enabled=1`, `aof_fsync_policy=everysec`, `aof_fsyncs=17`, `aof_writes=601599`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=53515120`, `keyspace_misses=0`, `pipeline_batches=2869199`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory+wal`, `total_commands_processed=55391261`, `used_memory_human=63.83M`, `wal_unsynced_records=24138`

### set P=8

- workload `set`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 8, 8.0s measured, 6,661,728 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `aof_appends=10710873`, `aof_enabled=1`, `aof_fsync_policy=everysec`, `aof_fsyncs=41`, `aof_writes=1337135`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=53515120`, `keyspace_misses=0`, `pipeline_batches=3973541`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory+wal`, `total_commands_processed=64225998`, `used_memory_human=60.96M`, `wal_unsynced_records=203445`

### set P=32

- workload `set`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 32, 8.0s measured, 9,798,624 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `aof_appends=24991353`, `aof_enabled=1`, `aof_fsync_policy=everysec`, `aof_fsyncs=74`, `aof_writes=1754648`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=53515120`, `keyspace_misses=0`, `pipeline_batches=4419806`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory+wal`, `total_commands_processed=78506479`, `used_memory_human=57.58M`, `wal_unsynced_records=21006`

### mixed P=1

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 1, 8.0s measured, 1,114,243 operations
- closed loop: each connection waits for its replies before sending again
- 556,662 reads, 557,581 writes
- server reported: `aof_appends=25942506`, `aof_enabled=1`, `aof_fsync_policy=everysec`, `aof_fsyncs=86`, `aof_writes=2266348`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=54264478`, `keyspace_misses=0`, `pipeline_batches=4421206`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory+wal`, `total_commands_processed=80206991`, `used_memory_human=44.47M`, `wal_unsynced_records=46537`

### mixed P=8

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 8, 8.0s measured, 7,106,256 operations
- closed loop: each connection waits for its replies before sending again
- 3,552,276 reads, 3,553,980 writes
- server reported: `aof_appends=31057250`, `aof_enabled=1`, `aof_fsync_policy=everysec`, `aof_fsyncs=105`, `aof_writes=3051713`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=59174542`, `keyspace_misses=0`, `pipeline_batches=5650707`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory+wal`, `total_commands_processed=90231800`, `used_memory_human=36.45M`, `wal_unsynced_records=13331`

### mixed P=32

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 32, 8.0s measured, 19,679,104 operations
- closed loop: each connection waits for its replies before sending again
- 9,833,685 reads, 9,845,419 writes
- server reported: `aof_appends=44499229`, `aof_enabled=1`, `aof_fsync_policy=everysec`, `aof_fsyncs=136`, `aof_writes=3822367`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=72403731`, `keyspace_misses=0`, `pipeline_batches=6479331`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory+wal`, `total_commands_processed=116902969`, `used_memory_human=42.51M`, `wal_unsynced_records=37168`

### mixed c=10

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 10 connections, pipeline depth 1, 8.0s measured, 770,435 operations
- closed loop: each connection waits for its replies before sending again
- 385,536 reads, 384,899 writes
- server reported: `aof_appends=45217910`, `aof_enabled=1`, `aof_fsync_policy=everysec`, `aof_fsyncs=149`, `aof_writes=4248403`, `connected_clients=10`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=72922602`, `keyspace_misses=0`, `pipeline_batches=6480731`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory+wal`, `total_commands_processed=118140522`, `used_memory_human=51.55M`, `wal_unsynced_records=2142`

### mixed c=50

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 1, 8.0s measured, 1,072,098 operations
- closed loop: each connection waits for its replies before sending again
- 535,610 reads, 536,488 writes
- server reported: `aof_appends=46165170`, `aof_enabled=1`, `aof_fsync_policy=everysec`, `aof_fsyncs=161`, `aof_writes=4746389`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=73667855`, `keyspace_misses=0`, `pipeline_batches=6482131`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory+wal`, `total_commands_processed=119833036`, `used_memory_human=56.55M`, `wal_unsynced_records=31342`

### mixed c=200

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 200 connections, pipeline depth 1, 8.0s measured, 1,253,249 operations
- closed loop: each connection waits for its replies before sending again
- 624,760 reads, 628,489 writes
- server reported: `aof_appends=47234931`, `aof_enabled=1`, `aof_fsync_policy=everysec`, `aof_fsyncs=173`, `aof_writes=5318404`, `connected_clients=200`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=74532018`, `keyspace_misses=0`, `pipeline_batches=6483531`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory+wal`, `total_commands_processed=121766961`, `used_memory_human=45.91M`, `wal_unsynced_records=49280`

### mixed c=500

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 500 connections, pipeline depth 1, 8.0s measured, 1,247,566 operations
- closed loop: each connection waits for its replies before sending again
- 622,566 reads, 625,000 writes
- server reported: `aof_appends=48306101`, `aof_enabled=1`, `aof_fsync_policy=everysec`, `aof_fsyncs=186`, `aof_writes=5884739`, `connected_clients=500`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=75399981`, `keyspace_misses=0`, `pipeline_batches=6484931`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory+wal`, `total_commands_processed=123706095`, `used_memory_human=86.17M`, `wal_unsynced_records=65787`

