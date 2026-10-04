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

**状态：COMPLETE**

已完成：

- `tr_runtime` 支持 N shards 与 per-shard config；
- Server public config 暴露 `shard_count`，默认 1；
- Server-wide peer/worker/queue/buffer/listen backlog 保持 total-budget 语义；
- total budget 通过 deterministic base+remainder 拆到各 shard；
- 每个 shard 独立拥有 Reactor、peer table/event source、RPC executor、
  RPC message pool 与 reassembly pool；
- `tr_server_listen()` 创建同地址同端口的 N 个 `SO_REUSEPORT` listener；
- 每个 listener 只由所属 Reactor accept，不做跨 shard fd transfer；
- start/drain/destroy 遍历全部 shard；
- Server stats 聚合全部 Reactor、pool、peer、Channel 与 RPC 数据；
- dedicated accept/reaper thread 和 Server-global hot-path lock 均不存在。

每 Reactor：

```text
SO_REUSEPORT listener
own connection table
own peer state
```

中央 accept/reaper 与 peer transition lock 都已删除；Phase 3 single-shard
ownership seam 基本收口，可进入 Phase 4 multi-shard enablement。

## Phase 5 - Pipeline / Connection Group

**状态：IN PROGRESS**

已完成 connection-group foundation：

- 内部 bounded `tr_pipeline` soft-state object；
- 一个 Pipeline 固定一个 Reactor owner；
- CONTROL 与 DATA connection membership owner 校验；
- CONTROL/DATA physical connection membership 唯一；
- DATA slot generation 防 ABA；
- bounded DATA round-robin selection；
- bounded Stream -> DATA affinity；
- DATA removal 自动 invalidates 对应 generation 的 Stream affinity；
- 所有 mutable membership/affinity mutation 通过 owner Reactor 串行化。

已完成 routing identity foundation：

- 固定 48-byte `TRR1` routing preface；
- little-endian + CRC32C；
- pipeline_id / epoch / owner_shard / role / member index / member generation；
- CONTROL 与 DATA 的 member-index 语义校验；
- incremental parser 支持任意 TCP fragmentation；
- parser 精确停止在 preface 边界，保留同 read 的后续 TRP1 bytes；
- invalid complete preface terminal，禁止任意字节 resync；
- member_generation 明确定义为 Pipeline membership generation，不复用 Reactor slot generation。

已完成 registry / capability foundation：

- bounded shard-local Pipeline registry；
- registry 以 pipeline_id 为唯一 key，旧 epoch 未注销时新 epoch 不能并存；
- Pipeline identity 增加 owner_shard_id + epoch；
- DATA slot 状态拆成 FREE / RESERVED / ATTACHED；
- CONTROL-plane 可先 reserve `(data_index, generation)`；
- reserve/attach 都要求 active CONTROL；CONTROL clear 会撤销所有未 attach reservation；
- reservation 占容量但不参与 DATA selection/Stream affinity；
- route attach 再校验 role/shard/pipeline_id/epoch/index/generation/owner Reactor；
- routing mismatch 不消耗 reservation；
- exact attach 才把 RESERVED 转为 ATTACHED；
- registry 不拥有 Pipeline lifetime；unregister 前强制 CONTROL/DATA/reservation/affinity 全部 quiesce；
- registry 不向 owner domain 外返回裸 Pipeline pointer，按 ID 操作必须 owner 内 lookup+action。

已完成 accepted DATA ingress foundation：

- Reactor connection 支持 opt-in fixed-size preface gate；
- gate 在 TRP1 parser 前读取且只读取精确 preface 长度，不 over-read 后续 frame；
- Pipeline ingress 使用真实 accepted fd 执行 TRR1 parse + registry reservation attach；
- routing mismatch connection-fatal，但不消耗 reservation；
- exact retry capability 可以随后成功 attach；
- attach 后安装正常 TRP1 downstream handler；
- routed DATA connection CLOSED/ERROR 自动 exact detach membership；
- 普通 RPC adopt path 不启用 gate，现有协议行为不变。

已完成 internal CONTROL-plane foundation：

- `tr_pipeline_control` 负责 create/bind/register Pipeline 生命周期；
- CONTROL reserve DATA 时生成可直接编码为 TRR1 的 DATA offer；
- reservation 本身不构成 READY；
- `prepare_transfer(stream_id)` 只从 ATTACHED DATA 中选择，跳过 RESERVED；
- DATA selection + Stream affinity 在同一个 owner operation 内原子完成；
- 成功返回内部 TRANSFER_READY token `(stream_id, data_index, generation)`；
- duplicate Stream prepare 被拒绝，Stream 生命周期保持单 DATA affinity；
- CONTROL close 在 attached DATA/Stream 未 quiesce 时拒绝；
- close 会自动撤销仍未 attach 的 RESERVED capability，再 unregister/destroy Pipeline。

已完成 internal CONTROL wire foundation：

- 固定 48-byte `TRC1` payload，little-endian；
- DATA_OFFER / DATA_CANCEL / TRANSFER_READY 三种 message；
- wire identity 固定包含 owner_shard_id / pipeline_id / epoch /
  data_index / data_generation，TRANSFER_READY 额外包含 stream_id；
- 不在 wire 暴露 Reactor slot/generation；
- DATA_OFFER 可唯一转换为现有 TRR1 DATA route capability；
- reserve -> DATA_OFFER 编码失败时回滚 exact reservation；
- DATA_CANCEL decode 后只取消完全匹配的 RESERVED capability；
- prepare_transfer -> TRANSFER_READY 保持“先 exact DATA attach，再 READY”；
- READY 编码失败时回滚刚建立的 Stream affinity。

已完成 shard-local Pipeline Transport/control-plane integration：

- 新增 `TR_FRAME_PIPELINE_CONTROL`，CONTROL payload 使用 Reactor control TX pool；
- 一个 internal Pipeline listener 固定属于一个 Reactor/shard owner；
- listener 自己拥有 bounded Pipeline registry、connection/session slots 与
  fixed-size CONTROL message pool；
- CONTROL/DATA 共用同一个 exact TRR1 accepted-socket gate；
- CONTROL route 必须先通过 application authorize hook，不能把远端 route 当授权；
- CONTROL route 成功后 create/bind/register Pipeline 并安装真实 frame handler；
- DATA route 复用 registry exact reservation attach，再进入普通 TRP1 parser；
- server-side owner API 可通过真实 socket 发 DATA_OFFER / TRANSFER_READY；
- DATA_CANCEL 在 CONTROL frame callback 中 exact 校验并消费 reservation；
- CONTROL 断开先把 session 标为 CLOSING，再原子失效 membership/affinity；
- group teardown 注销 Pipeline 后 owner-immediate 关闭 DATA sockets；
- listener stop 会同步关闭 pending/CONTROL/DATA accepted sockets，不依赖后台线程；
- real loopback test 覆盖 authorize -> OFFER -> DATA attach -> READY -> CANCEL ->
  CONTROL failure group teardown -> same pipeline_id new epoch reuse。

下一步：

- 先完成 layer/module/API boundary cleanup，禁止业务直接碰 internal Reactor handle；
- 把 generic Connection Group / Pipeline 能力封装成不依赖 Reactor 的 public/advanced Transport capability；
- 分离 application RPC API 与 internal Endpoint engine API；
- 建立 stable public header allowlist；
- 只有真实 profile/部署拓扑证明需要时，才增加 cross-shard fd transfer。

当前仍不做单 Pipeline 跨 Reactor shared mutable state。

Backup identity、checkpoint、commit、resume 等 durable 语义属于业务层，不再作为
c-trpc core Phase。

## Phase 6 - API Boundary Cleanup

**状态：IN PROGRESS**

已完成：

- SDK header 从 glob publication 改为 explicit allowlist；
- CI 精确校验安装 header 集合；
- external install consumer 只使用 stable `tr/trpc.h` facade；
- `rpc.h` 已分离 application Method/Call/Streaming contract 与 internal Endpoint engine；
- `rpc.h -> channel.h -> reactor.h` public dependency 已解除；
- buffer/channel/reactor/frame/wire 以及 queue/parser/socket/wire codec helper 均退出安装 SDK；
- detailed Reactor/Channel/Endpoint diagnostics 已移入 internal diagnostics；
- retained RPC message 用固定 opaque release token，未增加 allocation/copy；
- `tr_rpc_call_handle` 已改为 16-byte opaque capability，不再公开 Endpoint pointer + slot/generation，stale-generation fencing 语义保持不变；
- P3 第一阶段已提供 Server-side generic Connection Group public facade，复用 internal Pipeline listener，并以 opaque RX ownership token 保持 DATA receive 零额外 payload copy；
- P3 第二阶段已提供 Client-side Group CONTROL connect/close；CONTROL route identity 保持 internal；
- P3 第三阶段已提供 bounded Client DATA lane 自动建立：DATA_OFFER 由 internal Group engine 消费，nonblocking connect 使用 Reactor auxiliary fd event + timer，不新增 connector thread；Server 对失败 route 只 exact-cancel 仍为 RESERVED 的 generation；peer DATA_CANCEL 对 exact generation 幂等，从而无需新增 DATA_ATTACH_ACK；
- P3 第四阶段已提供 Client TRANSFER_READY 消费与 bounded Stream affinity：READY 必须精确命中 ACTIVE DATA generation，public callback 不暴露 DATA routing identity，Client release 与 DATA-loss invalidation 都在原 Reactor owner 上完成；
- P3 第五阶段已提供 Client logical DATA send：borrowed application bytes 在 owner 内复制到 bounded internal ownership，send-byte quota 从 DATA/message semantic limits 推导，Reactor 继续负责 fragmentation/TX，临时资源压力以 TR_AGAIN 反馈；
- P3 第六阶段已完成 stable graceful drain 与 semantic Group stats：drain 与 force stop 分离，Client/Server 都能等待语义 work quiesce，stable stats 不暴露 Reactor/route/queue/pool internal diagnostics。
- Runtime 收敛阶段进一步抽出通用 Reactor-owned nonblocking connector：Channel automatic reconnect 与 Connection Group DATA establish 共用 connect/timeout ownership；Reactor auxiliary fd 从单槽升级为 bounded multi-source + generation fencing，因此同一 Client Reactor 可并行观察多个 connector，而不再为 Channel 创建 reconnect thread。
- Completion handoff 收敛阶段把 worker 满载路径从 `sched_yield()` 改为 bounded queue-local condition wait：Reactor batch pop 释放容量后唤醒 producer；stop 关闭 admission、广播 waiter 并推进 admission generation，防止旧 completion 在 Reactor restart 后跨 epoch 提交。
- Command handoff 收敛阶段进一步删除 Reactor 剩余 `sched_yield()`：异步 SEND/RESUME 继续以 `TR_AGAIN` 表达有界 backpressure，只有同步 CALL/QUIESCE/SET_HANDLER 使用 generation-fenced condition wait；STOP 关闭普通 waiter admission 后使用 force wait 等待 ring slot，保持 FIFO shutdown barrier。

当前 stable installed headers：

```text
trpc.h
client.h
server.h
rpc.h
rpc_codec.h
facade.h
observability.h
status.h
transport.h
```

下一阶段：

- high-level semantic limits 与 implementation tuning 分离；
- 继续设计 facade-wide stable semantic observability；
- Connection Group P3 capability 进入维护/验证阶段，不再扩大 routing/internal contract。

详细审查见 [分层、模块职责与 API 边界](07-layer-module-api-boundaries.md)。

## Phase 7 - Resource Driven Flow Control

增加：

- Connection Group inflight limit；
- shard memory budget；
- worker admission；
- application/backend admission hook；
- adaptive DATA parallelism。

## Phase 8 - Profile Before Further Complexity

重点测：

- Reactor CPU；
- completion queue；
- cross-shard route rate；
- memory；
- worker queue；
- backend queue / admission；
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
