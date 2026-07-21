---
name: raft-consensus
description: Raft consensus mentor for Redis From Scratch. Use when designing or reviewing leader election, log replication, snapshots, read paths, membership, or any claim about consistency under failure.
---

# Raft Consensus Mentor

The paper is the spec. Every deviation from Ongaro and Ousterhout is documented with a reason, and every safety rule has a test.

## Safety Rules (non-negotiable)

- Persist term, votedFor, and log entries to stable storage before answering any RPC that depends on them; replying first is the classic corruption.
- Election safety: at most one leader per term, enforced by single-vote-per-term persisted.
- A leader never overwrites or deletes its own entries; only followers truncate, and only on conflict.
- Commit only entries from the current term by counting replicas; prior-term entries commit implicitly (figure 8 of the paper is a required discussion).
- State machine apply is exactly-once and in log order; lastApplied advances atomically with the applied effect or apply is idempotent.

## Liveness Rules

- Election timeout is randomized per election, not per node, and sits well above broadcast time.
- The invariant chain: RPC latency << broadcast time << election timeout << MTBF; put numbers on each for this deployment.
- Term inflation from a partitioned node rejoining is expected; pre-vote is the fix and is either implemented or recorded as a known deferral.

## Replicated State Machine Rules

- Apply must be deterministic: anything clock-, random-, or local-state-dependent (expiry, `INCR` results) is resolved at propose time on the leader and carried in the entry.
- Reads are not free: linearizable reads require read-index with a leadership-confirmation round; a leader that skips the heartbeat round serves stale reads as a zombie.
- Stale follower reads are a legitimate mode only when labelled honestly: staleness is unbounded during a partition.

## Testing Doctrine

- Deterministic unit tests per paper rule: log matching after conflict, vote refusal on stale log, figure-8 commit scenario.
- The cluster test vocabulary is kill, restart, partition, heal, pause (SIGSTOP); a zombie-leader test with SIGSTOP/SIGCONT is mandatory before any consistency claim.
- Recorded histories under chaos go through a linearizability checker; a violation is a stop-the-line bug.
- A flaky consensus test is a real bug until proven otherwise.

## Review Checklist

- Point to the fsync that happens before each RPC reply.
- Point to the code enforcing single-goroutine (or locked) ownership of Raft state.
- Off-by-one audit around index 0/1, empty log, and the snapshot boundary term/index.
- What happens to in-flight client requests on leadership loss: error contract, not silence.
