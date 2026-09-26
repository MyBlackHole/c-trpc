# 架构演进路线

**状态：实施计划**

不一次性同时引入 multi-Reactor、multi-socket、Pipeline recovery 和新的 RPC execution model。

## Phase 0 - CURRENT BASELINE

```text
Server:
  1 Reactor
  accept thread
  reaper thread
  shared executor
  Reactor-local timers

Channel:
  CONTROL + one BULK

RPC:
  immutable Task
  worker callback state changes -> Reactor owner command
  dedicated per-Reactor Completion Queue
```

## Phase 1 - RPC Completion Ownership

先 Unary：

```text
Reactor
 -> Task
 -> Worker
 -> Completion
 -> original Reactor
```

目标：worker 不直接修改 Call/Endpoint/Stream protocol state。

Unary completion、Streaming send/finish/cancel、metadata/cancellation 查询，
以及 worker RX-credit/close 路径均已回到 Reactor owner。

## Phase 2 - Completion Queue + Scheduler

**状态：IN PROGRESS**

已完成：

- per-Reactor bounded completion queue；
- 独立于 command queue 的 wake coalescing；
- completion batch drain；
- completion backlog 下的 nonblocking event-loop continuation；
- STOP 前已接受 completion 的 drain barrier。

仍待：

- CONTROL priority；
- command/TX/completion 更统一的 event-loop work budget 与可观测指标。

## Phase 3 - Runtime Shard Abstraction

引入 `tr_runtime` / `tr_runtime_shard`。

第一阶段仍：

```text
shard_count = 1
```

保证外部行为不变。

## Phase 4 - Multi-Reactor Listener

打开：

```text
shard_count = N
```

每 Reactor：

```text
SO_REUSEPORT listener
own connection table
own peer state
```

逐步删除中央 accept/reaper 模型。

## Phase 5 - Pipeline / Connection Group

增加：

- Pipeline object；
- CONTROL + DATA[N]；
- stream affinity；
- routing preface；
- cross-shard fd ownership transfer。

## Phase 6 - Backup Correctness

增加：

- backup_id / pipeline_id / epoch；
- STORED / CHECKPOINTED / COMMITTED；
- BARRIER；
- resume；
- epoch fencing。

## Phase 7 - Resource Driven Flow Control

增加：

- Pipeline inflight limit；
- shard memory budget；
- worker admission；
- storage admission；
- adaptive DATA parallelism。

## Phase 8 - Profile Before Further Complexity

重点测：

- Reactor CPU；
- completion queue；
- cross-shard route rate；
- memory；
- worker queue；
- storage queue；
- P99 latency；
- single huge Pipeline vs many small Pipelines。

只有出现单 Pipeline 单核瓶颈，再进入 FUTURE DataShard。

## 非目标

当前阶段不做：

- SCM_RIGHTS；
- Nginx master/worker process model；
- io_uring 基线；
- reuseport eBPF steering；
- lock-free everything；
- 单 Pipeline 跨 Reactor mutable shared state。
