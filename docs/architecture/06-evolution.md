# 架构演进路线

**状态：实施计划**

不一次性同时引入 multi-Reactor、multi-socket、Pipeline recovery 和新的 RPC execution model。

## Phase 0 - CURRENT

```text
Server:
  1 Reactor
  accept thread
  reaper thread
  shared executor
  shared maintenance

Channel:
  CONTROL + one BULK

RPC:
  worker 与 Endpoint/Call 仍存在共享协议状态
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

目标：worker 不直接修改 Call/Endpoint protocol state。

之后迁移 Streaming send/finish/cancel 为 owner command。

## Phase 2 - Completion Queue + Scheduler

引入：

- per-Reactor completion queue；
- wake coalescing；
- batch drain；
- CONTROL priority；
- bounded event-loop work budget。

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
