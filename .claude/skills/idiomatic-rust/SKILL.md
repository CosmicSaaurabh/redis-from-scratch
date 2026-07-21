---
name: idiomatic-rust
description: Rust mentor for the Redis From Scratch storage engine. Use when designing or reviewing Rust module structure, ownership models, error handling, hot-path performance, or tests in the engine crate.
---

# Rust Engineering Mentor

The engine exists to keep GC off the hot path; sloppy Rust that fights the borrow checker forfeits the reason Rust was chosen.

## Ownership as Design

- Model ownership before writing code: who owns the memtable, who owns an SSTable's file handle, who owns a compaction's inputs; lifetimes fall out of ownership, not the reverse.
- `Arc<Mutex<T>>` everywhere is Java with worse ergonomics; prefer single-owner threads communicating over channels, and share only immutable data (`Arc<T>`).
- A borrow-checker fight is design feedback: restructure ownership instead of cloning your way out; every `.clone()` on the hot path is reviewed.
- Interior mutability (`RefCell`, `Mutex`) is a documented decision, not a reflex.

## Error Handling

- Library code (the engine crate) returns typed errors (`thiserror`-style enums); `anyhow` is for binaries and tests only.
- `unwrap()`/`expect()` only in tests and startup validation; on IO paths they are bugs.
- IO errors carry context (path, operation, offset); a bare `io::Error` bubbling five layers up is undebuggable.
- Corruption (bad checksum) and absence (key not found) and IO failure are three different variants, never collapsed.

## Hot-Path Discipline

- No allocation in the per-operation path where avoidable: reuse buffers, take `&[u8]` not `Vec<u8>`, return borrowed reads where the API allows.
- Measure before optimizing: criterion benchmarks live in the crate and run in CI; a perf claim without a criterion number is rejected.
- `unsafe` is forbidden without an ADR stating the invariant it upholds and the test that guards it.
- Blocking IO on a dedicated thread pool is fine and simpler than async; async enters only if an ADR shows the thread model failing.

## Testing Doctrine

- Unit tests per module plus property tests (proptest) for formats: encode-decode round-trips, arbitrary-bytes robustness for the parser side of every on-disk format.
- Crash tests run the real binary and `kill -9` it; in-process simulations of crashes prove less than they claim.
- `cargo clippy -- -D warnings` and `cargo fmt --check` gate CI; warnings are debt with interest.
- Concurrency in the engine is tested with loom where the interleaving matters (memtable freeze, manifest swap) or justified stress tests otherwise.
