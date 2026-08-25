# redis-server baseline, persistence disabled, same harness and machine

Generated 2026-08-25T14:30:35Z on darwin/arm64, 14 logical CPUs, go1.26.4.

> **The load generator ran on the same machine as the server.** Client and server therefore competed for the same CPUs, and there was no network. Treat these as a relative comparison between configurations, not as an absolute capacity figure for a deployed node.

| run | conns | pipe | ops/sec | p50 us | p99 us | p99.9 us | max us | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| get P=1 | 50 | 1 | 140,584 | 334 | 791 | 3670 | 33448 | 0 |
| get P=8 | 50 | 8 | 861,118 | 440 | 831 | 3686 | 38702 | 0 |
| get P=32 | 50 | 32 | 1,825,344 | 844 | 1475 | 5636 | 30406 | 0 |
| set P=1 | 50 | 1 | 139,515 | 340 | 705 | 2769 | 27136 | 0 |
| set P=8 | 50 | 8 | 667,093 | 518 | 2056 | 4391 | 10064 | 0 |
| set P=32 | 50 | 32 | 1,500,354 | 1057 | 1917 | 3899 | 12126 | 0 |
| mixed P=1 | 50 | 1 | 139,821 | 342 | 758 | 2073 | 17414 | 0 |
| mixed P=8 | 50 | 8 | 819,179 | 475 | 786 | 1597 | 7190 | 0 |
| mixed P=32 | 50 | 32 | 1,532,174 | 1004 | 2228 | 8651 | 33593 | 0 |
| mixed c=10 | 10 | 1 | 114,225 | 73 | 285 | 1868 | 28438 | 0 |
| mixed c=50 | 50 | 1 | 134,240 | 346 | 860 | 2949 | 57514 | 0 |
| mixed c=200 | 200 | 1 | 141,067 | 1425 | 2056 | 2884 | 5009 | 0 |
| mixed c=500 | 500 | 1 | 139,286 | 3670 | 4981 | 6685 | 10075 | 0 |

## How to read this

- Latency is measured client side, from the moment a request was due to be sent until its reply was fully read.
- Runs with a pipeline depth above 1 report **batch** latency, attributed to each command in the batch. A depth of 16 at 800 us does not mean each command took 800 us; it means a batch of 16 did.
- Runs with a target rate are open loop: latency is measured from the intended send time, so a stall shows up instead of being hidden by the client pausing with the server.
- Percentiles come from a log-linear histogram of every sample, not a reservoir, so the tail is real and not a sampling artefact. Bucket width bounds the error at 0.8%.

## Run details

### get P=1

- workload `get`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 1, 8.0s measured, 1,124,730 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `aof_enabled=0`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0,subexpiry=0`, `expired_keys=0`, `keyspace_hits=1576872`, `keyspace_misses=0`, `redis_version=8.4.0`, `total_commands_processed=1776873`, `used_memory_human=28.33M`

### get P=8

- workload `get`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 8, 8.0s measured, 6,898,392 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `aof_enabled=0`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0,subexpiry=0`, `expired_keys=0`, `keyspace_hits=10885536`, `keyspace_misses=0`, `redis_version=8.4.0`, `total_commands_processed=11285538`, `used_memory_human=28.36M`

### get P=32

- workload `get`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 32, 8.0s measured, 14,603,808 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `aof_enabled=0`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0,subexpiry=0`, `expired_keys=0`, `keyspace_hits=31044576`, `keyspace_misses=0`, `redis_version=8.4.0`, `total_commands_processed=31644579`, `used_memory_human=28.43M`

### set P=1

- workload `set`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 1, 8.0s measured, 1,116,158 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `aof_enabled=0`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0,subexpiry=0`, `expired_keys=0`, `keyspace_hits=31044576`, `keyspace_misses=0`, `redis_version=8.4.0`, `total_commands_processed=33206374`, `used_memory_human=28.34M`

### set P=8

- workload `set`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 8, 8.0s measured, 5,336,920 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `aof_enabled=0`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0,subexpiry=0`, `expired_keys=0`, `keyspace_hits=31044576`, `keyspace_misses=0`, `redis_version=8.4.0`, `total_commands_processed=40766487`, `used_memory_human=28.34M`

### set P=32

- workload `set`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 32, 8.0s measured, 12,004,096 operations
- closed loop: each connection waits for its replies before sending again
- server reported: `aof_enabled=0`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0,subexpiry=0`, `expired_keys=0`, `keyspace_hits=31044576`, `keyspace_misses=0`, `redis_version=8.4.0`, `total_commands_processed=57140216`, `used_memory_human=28.34M`

### mixed P=1

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 1, 8.0s measured, 1,118,605 operations
- closed loop: each connection waits for its replies before sending again
- 558,821 reads, 559,784 writes
- server reported: `aof_enabled=0`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0,subexpiry=0`, `expired_keys=0`, `keyspace_hits=31789198`, `keyspace_misses=0`, `redis_version=8.4.0`, `total_commands_processed=58831340`, `used_memory_human=28.36M`

### mixed P=8

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 8, 8.0s measured, 6,553,656 operations
- closed loop: each connection waits for its replies before sending again
- 3,276,369 reads, 3,277,287 writes
- server reported: `aof_enabled=0`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0,subexpiry=0`, `expired_keys=0`, `keyspace_hits=36308834`, `keyspace_misses=0`, `redis_version=8.4.0`, `total_commands_processed=68074461`, `used_memory_human=28.36M`

### mixed P=32

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 32, 8.0s measured, 12,258,304 operations
- closed loop: each connection waits for its replies before sending again
- 6,126,721 reads, 6,131,583 writes
- server reported: `aof_enabled=0`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0,subexpiry=0`, `expired_keys=0`, `keyspace_hits=44820704`, `keyspace_misses=0`, `redis_version=8.4.0`, `total_commands_processed=85303742`, `used_memory_human=28.43M`

### mixed c=10

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 10 connections, pipeline depth 1, 8.0s measured, 913,811 operations
- closed loop: each connection waits for its replies before sending again
- 457,531 reads, 456,280 writes
- server reported: `aof_enabled=0`, `connected_clients=10`, `db0=keys=200000,expires=0,avg_ttl=0,subexpiry=0`, `expired_keys=0`, `keyspace_hits=45455439`, `keyspace_misses=0`, `redis_version=8.4.0`, `total_commands_processed=86772252`, `used_memory_human=28.26M`

### mixed c=50

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 50 connections, pipeline depth 1, 8.0s measured, 1,073,958 operations
- closed loop: each connection waits for its replies before sending again
- 536,516 reads, 537,442 writes
- server reported: `aof_enabled=0`, `connected_clients=50`, `db0=keys=200000,expires=0,avg_ttl=0,subexpiry=0`, `expired_keys=0`, `keyspace_hits=46193348`, `keyspace_misses=0`, `redis_version=8.4.0`, `total_commands_processed=88450051`, `used_memory_human=28.36M`

### mixed c=200

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 200 connections, pipeline depth 1, 8.0s measured, 1,128,636 operations
- closed loop: each connection waits for its replies before sending again
- 562,757 reads, 565,879 writes
- server reported: `aof_enabled=0`, `connected_clients=200`, `db0=keys=200000,expires=0,avg_ttl=0,subexpiry=0`, `expired_keys=0`, `keyspace_hits=46966496`, `keyspace_misses=0`, `redis_version=8.4.0`, `total_commands_processed=90200907`, `used_memory_human=28.72M`

### mixed c=500

- workload `mixed`, key distribution `uniform`, 200,000 keys, 64 byte values
- 500 connections, pipeline depth 1, 8.0s measured, 1,114,710 operations
- closed loop: each connection waits for its replies before sending again
- 556,350 reads, 558,360 writes
- server reported: `aof_enabled=0`, `connected_clients=500`, `db0=keys=200000,expires=0,avg_ttl=0,subexpiry=0`, `expired_keys=0`, `keyspace_hits=47709655`, `keyspace_misses=0`, `redis_version=8.4.0`, `total_commands_processed=91889519`, `used_memory_human=29.45M`

