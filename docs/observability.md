# Runtime observability

c-trpc collects structured snapshots inside the runtime instead of embedding a
metrics backend in the core library. After the API-boundary cleanup, the
low-level Reactor/Channel/Endpoint snapshot structures are an **internal
diagnostics contract**, used by repository benchmarks/tests while a stable
semantic observability API is designed.

## Cost model

Cheap counters and bounded-queue high-water marks are always collected.

Timing metrics are opt-in:

```c
limits.observability_flags |= TR_OBSERVABILITY_TIMING;
```

The same flag is propagated internally to Reactor and RPC Endpoint engine
configuration. With timing disabled, scheduling hot paths do not perform the
extra monotonic-clock reads used by latency histograms.

Timing histograms are diagnostic samples, not transactional accounting
counters. A timing sample is recorded only when the relevant monotonic-clock
reads succeed, so `samples` may be lower than the corresponding task/turn
counter. The non-timing counters remain the source of truth for exact work
accounting.

`tr_latency_histogram` is a fixed 64-bucket log2 nanosecond histogram. It does
not allocate and snapshots can be aggregated by summing corresponding buckets,
sample counts, and totals and taking the maximum of `max_ns`.

## Reactor attribution

`tr_reactor_get_stats()` reports:

- command and completion queue `capacity/current/peak/full_events`；
  command `full_events` 表示 producer 首次撞到满 ring 的压力：SEND/RESUME 等
  异步 API 仍会立即 `TR_AGAIN`，CALL/QUIESCE/SET_HANDLER/STOP 这类同步或
  lifecycle request 会进入 queue-local capacity wait；
  completion `full_events` 表示 worker handoff 遇到满 ring 并进入 capacity wait；
- bounded per-turn work and budget-hit counters;
- `busy_ns` and `poll_ns` when timing is enabled;
- a per-turn busy-time histogram.

A useful saturation signal is:

```text
reactor_busy_ratio = busy_ns / (busy_ns + poll_ns)
```

A high busy ratio together with low RPC queue wait points toward the Reactor as
the limiting execution resource. Queue high-water and budget-hit counters show
whether the pressure is command/completion scheduling or RX/TX work。特别是
completion `full_events` 增长表示 worker 曾被 Reactor completion 消费速率反压，
不是 completion 被丢弃；需要结合 Reactor busy ratio、completion budget hits 和
executor queue wait 判断瓶颈位置。

## Reactor command attribution

The Reactor snapshot also exposes the existing per-turn command fairness data:

- `limits.commands`: command budget per event-loop turn;
- `total.commands`: commands processed;
- `max_per_turn.commands`: largest observed turn;
- `budget_hits.commands`: turns that consumed the full command budget.

Producer-side command queue pressure is split into `SEND`, `RESUME_RX`,
`CALL`, and other commands, each with accepted enqueue and first-full encounter counts.
对于异步 SEND/RESUME，这个 full count 对应立即 backpressure；对于同步 CALL 和
other 中的 QUIESCE/SET_HANDLER/STOP，它表示进入 capacity wait，而不是 command
被丢弃。These counters are updated under the command queue's existing mutex and add no
new allocation, clock read, or metrics lock.

A SEND issued while already executing on the owning Reactor may attach directly
to that connection's TX queue only when no command is pending and the Reactor is
not dispatching a command batch already copied out of the ring. This preserves
command FIFO ordering around the owner fast path. Direct owner-local sends do
not increment `command_send.enqueued`; owner sends that must preserve an
earlier command ordering boundary fall back to the ring and are counted normally.
The counter therefore measures actual queued SEND pressure rather than total
frames transmitted.

This distinction matters when the queue is full while Reactor busy time is low:
a high command-budget-hit rate points toward bounded per-turn fairness or burst
drain behavior, while one producer category dominating queue-full events points
toward a specific upstream pressure source.

## RPC executor attribution

The internal `tr_rpc_endpoint_get_stats()` snapshot reports:

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

## Server facade aggregation

The internal `tr_server_get_stats()` aggregation provides the repository benchmark with one full engine-level view. It includes:

- the Server-owned Reactor snapshot, including RX/TX/control TX pool pressure;
- RPC message and reassembly pool `capacity/current/peak/exhausted_events`;
- current peer/Call/Stream counts;
- lifetime RPC and Channel counters across both live peers and peers already
  reclaimed by the reaper;
- merged executor queue-wait and handler histograms across peer Endpoints;
- the largest per-Endpoint executor queue/ready-Call high-water marks.

A reaped peer is synchronously quiesced and its RPC executor is fully drained
before its final statistics are merged into the Server retirement totals. This
prevents disconnect/reaping from truncating the last worker completions.

The cross-component Server snapshot is structured rather than globally atomic:
individual component snapshots are coherent, but counters can advance while the
snapshot is collected. After `tr_server_drain()` has stopped accept/reap and
drained active work, the snapshot is stable enough for end-of-run benchmark
attribution.

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
