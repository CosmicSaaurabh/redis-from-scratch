# redis-from-scratch: in-process engine, persistence disabled

Generated 2026-08-25T14:24:40Z on darwin/arm64, 14 logical CPUs, go1.26.4.

> **The load generator ran on the same machine as the server.** Client and server therefore competed for the same CPUs, and there was no network. Treat these as a relative comparison between configurations, not as an absolute capacity figure for a deployed node.

| run | conns | pipe | ops/sec | p50 us | p99 us | p99.9 us | max us | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| get P=1 | 50 | 1 | 141,595 | 319 | 1081 | 3293 | 21243 | 0 |
| get P=8 | 50 | 8 | 897,516 | 317 | 2474 | 16253 | 169260 | 0 |
| get P=32 | 50 | 32 | 3,373,124 | 379 | 1917 | 10682 | 155448 | 0 |
| set P=1 | 50 | 1 | 137,881 | 340 | 795 | 3277 | 38987 | 0 |
| set P=8 | 50 | 8 | 1,101,554 | 334 | 889 | 3408 | 88420 | 0 |
| set P=32 | 50 | 32 | 3,825,212 | 346 | 1729 | 5439 | 59935 | 0 |
| mixed P=1 | 50 | 1 | 132,640 | 350 | 934 | 3178 | 42351 | 0 |
| mixed P=8 | 50 | 8 | 954,136 | 338 | 1958 | 9306 | 94028 | 0 |
| mixed P=32 | 50 | 32 | 3,548,654 | 350 | 2097 | 9503 | 180640 | 0 |
| mixed c=10 | 10 | 1 | 99,837 | 93 | 206 | 643 | 26161 | 0 |
| mixed c=50 | 50 | 1 | 119,795 | 344 | 1917 | 8585 | 35320 | 0 |
| mixed c=200 | 200 | 1 | 149,438 | 1311 | 2064 | 3129 | 16018 | 0 |
| mixed c=500 | 500 | 1 | 142,993 | 3424 | 5833 | 15991 | 93045 | 0 |

## How to read this

- Latency is measured client side, from the moment a request was due to be sent until its reply was fully read.
- Runs with a pipeline depth above 1 report **batch** latency, attributed to each command in the batch. A depth of 16 at 800 us does not mean each command took 800 us; it means a batch of 16 did.
- Runs with a target rate are open loop: latency is measured from the intended send time, so a stall shows up instead of being hidden by the client pausing with the server.
- Percentiles come from a log-linear histogram of every sample, not a reservoir, so the tail is real and not a sampling artefact. Bucket width bounds the error at 0.8%.

## Run details

### get P=1

- workload `get`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 1, 8.0s measured, 1,132,775 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=1556625`, `keyspace_misses=0`, `pipeline_batches=1400`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory`, `total_commands_processed=1756626`, `used_memory_human=38.56M`

### get P=8

- workload `get`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 8, 8.0s measured, 7,180,400 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=12041425`, `keyspace_misses=0`, `pipeline_batches=1313400`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory`, `total_commands_processed=12441427`, `used_memory_human=60.59M`

### get P=32

- workload `get`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 32, 8.0s measured, 26,986,304 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=47930737`, `keyspace_misses=0`, `pipeline_batches=2436341`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory`, `total_commands_processed=48530740`, `used_memory_human=60.96M`

### set P=1

- workload `set`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 1, 8.0s measured, 1,103,069 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=47930737`, `keyspace_misses=0`, `pipeline_batches=2436341`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory`, `total_commands_processed=50082872`, `used_memory_human=45.22M`

### set P=8

- workload `set`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 8, 8.0s measured, 8,812,832 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=47930737`, `keyspace_misses=0`, `pipeline_batches=3974814`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory`, `total_commands_processed=62390657`, `used_memory_human=40.70M`

### set P=32

- workload `set`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 32, 8.0s measured, 30,603,072 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=47930737`, `keyspace_misses=0`, `pipeline_batches=5297992`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory`, `total_commands_processed=104732354`, `used_memory_human=60.18M`

### mixed P=1

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 1, 8.0s measured, 1,061,175 operations
- closed loop: each connection waits for its replies before sending again
- 530,201 reads, 530,974 writes
- server reported: `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=48659915`, `keyspace_misses=0`, `pipeline_batches=5299392`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory`, `total_commands_processed=106392403`, `used_memory_human=35.16M`

### mixed P=8

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 8, 8.0s measured, 7,633,336 operations
- closed loop: each connection waits for its replies before sending again
- 3,814,858 reads, 3,818,478 writes
- server reported: `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=54068335`, `keyspace_misses=0`, `pipeline_batches=6653725`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory`, `total_commands_processed=117415868`, `used_memory_human=66.90M`

### mixed P=32

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 32, 8.0s measured, 28,391,072 operations
- closed loop: each connection waits for its replies before sending again
- 14,187,758 reads, 14,203,314 writes
- server reported: `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=73505932`, `keyspace_misses=0`, `pipeline_batches=7870560`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory`, `total_commands_processed=156509789`, `used_memory_human=37.59M`

### mixed c=10

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 10 connections, pipeline depth 1, 8.0s measured, 798,698 operations
- closed loop: each connection waits for its replies before sending again
- 399,698 reads, 399,000 writes
- server reported: `connected_clients=10`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=74056718`, `keyspace_misses=0`, `pipeline_batches=7871960`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory`, `total_commands_processed=157811114`, `used_memory_human=39.87M`

### mixed c=50

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 1, 8.0s measured, 958,898 operations
- closed loop: each connection waits for its replies before sending again
- 479,149 reads, 479,749 writes
- server reported: `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=74734121`, `keyspace_misses=0`, `pipeline_batches=7873360`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory`, `total_commands_processed=159367459`, `used_memory_human=59.64M`

### mixed c=200

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 200 connections, pipeline depth 1, 8.0s measured, 1,195,654 operations
- closed loop: each connection waits for its replies before sending again
- 596,056 reads, 599,598 writes
- server reported: `connected_clients=200`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=75550281`, `keyspace_misses=0`, `pipeline_batches=7874760`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory`, `total_commands_processed=161204777`, `used_memory_human=56.52M`

### mixed c=500

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 500 connections, pipeline depth 1, 8.0s measured, 1,144,218 operations
- closed loop: each connection waits for its replies before sending again
- 571,020 reads, 573,198 writes
- server reported: `connected_clients=500`, `db0=keys=200000,expires=0,avg_ttl=0`, `expired_keys=0`, `keyspace_hits=76337005`, `keyspace_misses=0`, `pipeline_batches=7876160`, `redis_version=7.2.0-compatible`, `rfs_version=0.3.0-dev+82aa12e`, `storage_engine=memory`, `total_commands_processed=162980631`, `used_memory_human=60.49M`

