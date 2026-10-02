# 架构演进路线

**状态：实施计划**

不一次性同时引入 multi-Reactor、multi-socket、Pipeline recovery 和新的 RPC execution model。

## Phase 0 - CURRENT BASELINE

```text
Server:
  1 Reactor
  Reactor-owned accept
  shard-local executor
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
- STOP 前已接受 completion 的 drain barrier；
- completion admission 已下沉到 completion queue 自身锁域，worker hot path
  不再经过 Reactor `ctl_lock`；
- 每轮一个 command batch，wake 路径不重复消费命令预算；
- command backlog 的保守非阻塞续处理与公平性/退出回归测试；
- Command / Completion / Timer / RX / TX 共享整轮额度；
- RX/TX ready 轮转、单连接 quantum 与严格 wire 字节预算；
- owner 一致快照的累计工作、单轮峰值、额度耗尽、epoll 等待和 Timer 迟到采样；
- header-only PING/PONG/WINDOW_UPDATE 的有界帧边界优先级、控制顺序屏障
  与跨轮 DATA 防饥饿，见 [控制帧调度](../tx_priority.md)。

命令预算与唤醒推导见 [Reactor 命令调度公平性](../reactor_fairness.md)。
整轮限额、配置语义与统计口径见 [Reactor 整轮预算与调度诊断](../reactor_budget.md)。

仍待：

- 逻辑 CONTROL lane 业务 DATA 的独立优先策略（仍按 DATA FIFO）；
- 根据真实 profile 决定队列高水位、排队延迟与直方图等进一步指标，
  不把最小调度统计误认为完整性能观测体系。

已提供 CRC32C 组件微基准；它不是端到端 RPC/BULK 性能证明。
生产化与后续测量门槛见 [性能与生产化评估](../performance_readiness.md)。

## Phase 3 - Runtime Shard Abstraction

**状态：COMPLETE**

已完成第一阶段：

- 引入内部 `tr_runtime` / `tr_runtime_shard`；
- Client/Server 不再直接拥有 Reactor lifecycle；
- Runtime 统一负责 Reactor create/start/stop/destroy；
- 建立稳定 shard identity；Phase 3 facade 使用 `shard_id = 0`；
- public API、wire、线程数量与启动时机保持不变；
- 独立测试验证 single-shard identity 与完整生命周期语义；
- Server RPC executor group 已从 Server 全局 owner 下沉到 shard[0]；
- Server Endpoint 只进入所属 shard 的 worker pool，不再依赖 Server-global executor owner；
- Server listener fd / bound port 已从 Server 下沉到 shard[0] ownership；
- listener 已注册进 shard[0] Reactor epoll，独立 accept thread 已删除；
- accept callback 有固定批次上限，listener backlog 由后续 Reactor turn 继续处理；
- peer slot storage/capacity/high-water/reaping/ready/rejection counters 已下沉到 shard[0]；
- Server 仍负责 peer Channel/RPC 构造；disconnect teardown 已拆为 Reactor-owner detach + external finalize；
- owner detach 移除 Channel/RPC callback、deadline/keepalive timer source，并关闭 executor admission；
- 只有 owner detach 成功后 peer 才离开 shard table；reaper 只等待 worker refs、采集统计和 free；
- Reactor accept、peer reserve/publish/table detach/live snapshot 已全部 owner-only，
  不再通过 Server-global transition lock 串行化；
- shard[0] peer lifecycle eventfd 已注册进 Reactor epoll；Channel DOWN/rollback/
  publish 只发 deferred owner event；
- dedicated reaper thread 已删除；owner detach 后 peer slot 立即复用，旧
  Channel/Server 回收信息由 detached-finalizer context 持有；
- Endpoint owner ref 转交 last-ref finalizer，已有 worker ref 作为旧对象 lifetime
  fence，不需要 cleanup thread 阻塞等待；
- worker 数量、RPC 调度算法和 public/wire 行为保持不变。

Phase 3 结束时 Server facade 仍是：

```text
shard_count = 1
accept is Reactor-owned
peer lifecycle is Reactor-event driven
no dedicated accept/reaper/finalizer thread
listener/executor/peer resources are shard-owned
peer table is Reactor single-owner
```

completion event publication、listener ownership、peer resource ownership、
accept execution、peer lifecycle event、owner detach、last-ref finalization 以及
peer table mutation/snapshot 均已 shard owner 化，dedicated accept/reaper thread
和 Server-global peer transition lock 都已删除。剩余 `finalizer_lock` 仅属于
retired stats/shutdown 控制面，不在 hot path。Phase 3 ownership seam 至此收口。

## Phase 4 - Multi-Reactor Listener

**状态：IN PROGRESS**

已完成 Runtime foundation：

- `tr_runtime` 不再限制 `shard_count == 1`；
- Runtime config 使用 per-shard config array，不隐式复制资源预算；
- 每个 shard 独立创建 Reactor、peer storage/event source、RPC executor；
- multi-shard start 失败按已启动 shard 反向 rollback；
- 测试验证 3 shards 的 executor worker 总数与 Reactor 总数精确匹配配置；
- Server/Client facade 仍显式创建 1 shard，因此 public 行为未改变。

下一步才在 Server 打开：

```text
shard_count = N
per-shard budget split
SO_REUSEPORT listeners
```

每 Reactor：

```text
SO_REUSEPORT listener
own connection table
own peer state
```

中央 accept/reaper 与 peer transition lock 都已删除；Phase 3 single-shard
ownership seam 基本收口，可进入 Phase 4 multi-shard enablement。

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
