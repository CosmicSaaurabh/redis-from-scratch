---
name: concurrency-go
description: Go concurrency mentor for Redis From Scratch. Use when designing or reviewing goroutine topologies, channels, locks, connection handling, backpressure, or tests for concurrent Go code.
---

# Go Concurrency Mentor

## Core Review Questions

Ask these of every piece of concurrent code before anything else:
- Which goroutine runs this line, who started it, and what guarantees it exits.
- What is shared, what guards it, and is the guard the same on every access path.
- What is the happens-before edge (channel op, mutex, atomic) that makes this read see that write.
- What happens when this blocks forever; what is the timeout and who enforces it.

## Goroutine Discipline

- Every goroutine is owned: started in one place, tied to a context or done-channel, with a known exit condition; "fire and forget" is a leak.
- Goroutine-per-connection is fine only with an enforced connection cap and per-connection read/write deadlines.
- Leak checks are part of tests (assert goroutine counts, or use a leak detector); a leaked goroutine per request is death at load.
- Prefer a single owner goroutine with a request channel (event-loop style) for complex shared state like Raft; locks for simple hot state like the keyspace.

## Channel and Lock Rules

- Every channel is bounded and has exactly one closer; closing from the receiver side or from multiple senders is a design smell.
- Behavior when full is chosen deliberately: block (backpressure), drop with a counter, or fail fast; never an unbounded buffer.
- Locks: smallest scope, never held across network or disk IO, never held while sending on a channel that can block.
- `sync.RWMutex` on the keyspace: know that writer starvation and cache-line contention are real; a sharded map decision belongs in the LLD with numbers.
- Atomics for flags and counters; check-then-act on an atomic is still a race.

## Failure and Shutdown

- Context cancellation flows from `main` outward; every blocking call accepts a context or a deadline.
- `SIGTERM` order is documented: stop accepting, drain in-flight, flush WAL, close engine, exit; a test proves it.
- A panicking goroutine takes the process down; recover only at goroutine top-level with a logged, counted, deliberate policy.

## Testing Concurrent Code

- `-race` on every test run, locally and in CI; a race finding is a build failure, never a flake.
- Race conditions are proven with stress tests: many goroutines, a start barrier to maximize collision, invariant assertions after.
- Deterministic seams: injectable clocks and listeners; no `time.Sleep` synchronization in tests.
- A flaky concurrent test is a real bug until proven otherwise; never add retries to hide it.
