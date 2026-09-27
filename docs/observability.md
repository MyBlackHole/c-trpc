# Runtime observability

c-trpc exposes structured snapshots from the runtime instead of embedding a
metrics backend in the core library. Applications may export these snapshots to
Prometheus, logs, JSON, tracing systems, or project-specific monitoring.

## Cost model

Cheap counters and bounded-queue high-water marks are always collected.

Timing metrics are opt-in:

```c
limits.observability_flags |= TR_OBSERVABILITY_TIMING;
```

The same flag exists on the low-level `tr_reactor_config` and
`tr_rpc_endpoint_config`. With timing disabled, the scheduling hot paths do
not perform the extra monotonic-clock reads used by latency histograms.

`tr_latency_histogram` is a fixed 64-bucket log2 nanosecond histogram. It does
not allocate and snapshots can be aggregated by summing corresponding buckets,
sample counts, and totals and taking the maximum of `max_ns`.

## Reactor attribution

`tr_reactor_get_stats()` reports:

- command and completion queue `capacity/current/peak/full_events`;
- bounded per-turn work and budget-hit counters;
- `busy_ns` and `poll_ns` when timing is enabled;
- a per-turn busy-time histogram.

A useful saturation signal is:

```text
reactor_busy_ratio = busy_ns / (busy_ns + poll_ns)
```

A high busy ratio together with low RPC queue wait points toward the Reactor as
the limiting execution resource. Queue high-water and budget-hit counters show
whether the pressure is command/completion scheduling or RX/TX work.

## RPC executor attribution

`tr_rpc_endpoint_get_stats()` reports:

- executor queue `capacity/current/peak/full_events`;
- current and peak ready-Call count;
- total tasks accepted and taken by workers;
- `executor_admission_limit_hits`, when a new Call is stopped by the
  continuation reserve before the physical node pool is full;
- `executor_hard_full_events`, when the physical task node pool is exhausted;
- queue-wait and callback/task execution histograms when timing is enabled.

The admission-limit and hard-full counters are intentionally separate. A
reserve hit means the runtime protected capacity for already accepted Streaming
Calls; it does not mean the executor node pool was physically exhausted.

## Interpretation

Typical patterns:

- high Reactor busy ratio + low executor queue wait: investigate Reactor
  scalability before adding workers;
- low Reactor busy ratio + high executor queue wait: investigate worker count,
  callback cost, or executor sharding;
- high admission-limit hits with low hard-full count: continuation reserve is
  actively protecting accepted Streams;
- hard-full events: the bounded executor capacity is exhausted and should be
  correlated with queue wait and callback duration before changing capacity.

Observability counters are evidence, not automatic policy. CI should continue
to gate deterministic invariants rather than fixed QPS or latency thresholds.
