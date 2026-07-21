---
name: observability
description: Observability mentor for Redis From Scratch. Use when designing or reviewing OpenTelemetry tracing, Prometheus metrics, structured logging, dashboards, INFO/CLUSTER STATE commands, or SLO definitions.
---

# Observability Mentor

Observability is designed, not sprinkled on. Every feature's LLD names its signals before implementation.
This mirrors the Sentinel Engine approach deliberately; consistency across both projects is part of the point.

## The Three Signals

- Traces answer "where did this one command spend its time"; one trace must span parse, route, replicate, apply, respond, across nodes and across the Go-Rust boundary.
- Metrics answer "how is the system doing in aggregate"; cheap, pre-aggregated, alert-friendly.
- Logs answer "what exactly happened here"; structured JSON, one event per line, correlated by trace id.

## Tracing Rules (OpenTelemetry)

- Context propagates across Raft RPCs and the engine boundary via explicit metadata; context never crosses a goroutine or process hop automatically.
- Span names are low-cardinality (`cmd.set`, `raft.append`, `engine.get`); keys and ids go in attributes, never in names.
- Record the sampling decision explicitly, even if MVP samples 100%.

## Metrics Rules (Prometheus)

- Label cardinality is a budget: command name, shard id, and node role are valid labels; keys and client addresses never are.
- Counters for events (commands_total, elections_total), gauges for states (keyspace_size, raft_commit_index), histograms for durations (command_seconds, fsync_seconds, failover_seconds); pick deliberately.
- Histogram buckets are chosen against the NFR targets: sub-2s failover and the p99 latency claims must land inside bracketing buckets or they are unmeasurable.
- Golden signals per component, plus the leading indicators specific to this system: WAL fsync latency, compaction backlog, Raft commit-apply lag, and follower staleness; those four predict trouble earliest.

## Logging and Admin Commands

- Every log line during a command carries trace id, shard id, and node role; Raft transitions log term, index, and reason.
- Log levels mean something: ERROR pages someone, WARN is actionable later, INFO tells the story, DEBUG is for development.
- Never log key values or payloads; they are unbounded and sensitive.
- `INFO` and `CLUSTER STATE` are the operator's first debugger: role, term, shard map, engine stats; they must be cheap enough to call every second.

## Review Checklist

- Can I answer "why is this write slow" and "who is leader and since when" from signals alone, without a debugger.
- Does every alert-worthy condition have a metric, and does every metric that exists get looked at (delete the rest).
- Dashboard-as-code committed under `infra/`; hand-edited dashboards die with the container.
