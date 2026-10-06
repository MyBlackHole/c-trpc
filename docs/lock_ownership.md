# 锁与并发所有权审查

本文描述**当前 `main` 架构对应的同步模型**。判断一把锁是否应该存在时，先回答
“谁拥有这份可变状态”，再讨论互斥锁、原子变量或无锁实现。

核心原则：

> 能由 Reactor 单所有者串行化的协议状态，不使用互斥锁弥补所有权不清晰；
> 真正跨线程共享的队列、资源池和生命周期条件，保留最简单、可验证的同步机制。

## 1. Reactor

### `reactor->ctl_lock`

**结论：保留。**

它保护生产者与 Reactor 生命周期之间的控制面事务，主要包括：

- `started / accepting` 门禁；
- 停止流程与命令准入的线性化顺序；
- 连接槽位预留与命令提交之间的事务边界。

`ctl_lock` 不保护套接字 RX/TX、解析器、连接处理器等所有者热状态。

停止顺序必须保持：

```text
accepting = false
      ->
关闭完成事件准入
      ->
加入 STOP
      ->
所有者排空已经接受的完成事件
      ->
关闭连接
      ->
等待 Reactor 线程退出
```

`stop/destroy` 是外部生命周期屏障，禁止从 Reactor 所有者回调内执行。
所有者 TLS 会让 `tr_reactor_stop()` 在任何准入状态修改之前返回
`TR_ERR_STATE`，从而避免 `pthread_join(self)` 或部分停止状态。

这样可以保证 `STOP` 之后不会再出现“命令/完成事件已经取得资源所有权，
但所有者已经退出”的悬空工作。

### 命令队列/完成队列锁

**结论：保留。**

两者都是有界多生产者单消费者队列：

- 多个生产者提交；
- 单个 Reactor 所有者消费。

队列锁是准入/所有权转移的线性化点。当前互斥锁实现不位于套接字 I/O 热路径，
没有性能分析证据前不改成无锁实现。

完成队列与命令队列相互独立，工作线程完成事件不会占用控制命令容量。

两类队列的满载策略不同，但都不使用 `sched_yield()`：

- 完成队列：所有跨线程移交都在队列本地 `not_full` 上等待容量；
- 命令队列：`SEND/RESUME/CLOSE` 等异步 API 仍立即返回 `TR_AGAIN`；
  只有 `CALL/QUIESCE/SET_HANDLER` 等同步所有者请求才等待容量；
- `STOP` 使用仅生命周期可用的强制等待，不受普通命令等待者准入关闭影响。

停止流程先关闭完成事件准入和普通命令等待者准入，推进各自代次并唤醒等待者，
然后 `STOP` 自己等待真实命令槽位。已经成功入队的旧工作仍按 FIFO 在 `STOP` 前执行。

### TX 资源池锁

**结论：保留。**

普通 DATA/CONTROL TX 项可能由应用/RPC 生产者获取、由 Reactor 所有者释放，
因此空闲链表是真正的跨线程共享资源。后续如果性能分析证明争用明显，可以增加
每所有者/每线程缓存，而不是先改变所有权模型。

GOAWAY 是例外的终局生命周期控制帧，不再与普通 `control_tx_pool` 竞争。
每个物理 Connection 内嵌一个 header-only `lifecycle_tx` slot：

```text
普通 CONTROL
    -> control_tx_pool
    -> command ring
    -> connection TX FIFO

GOAWAY
    -> connection.lifecycle_tx
    -> 等待 barrier 前 command sequence 全部处理
    -> connection TX FIFO
```

GOAWAY 不占普通 TX pool，也不占新的 command ring slot，但不能越过 barrier 前
已经成功提交的 command。Command queue 在现有锁下为每个入队 command 分配单调
sequence；Channel drain 记录当前 FIFO frontier，Reactor 只在
`last_processed_sequence >= lifecycle_after_sequence` 时把内嵌 GOAWAY
挂到 TX tail。这样 terminal reserve 不会把 shutdown 资源竞争转化成 FIFO 破坏。

平时没有 pending lifecycle TX 时，Reactor 不扫描 Connection；只有
`lifecycle_tx_pending_count != 0` 才执行冷路径扫描。

### Connection 可变状态

**结论：仅所有者访问，不加互斥锁。**

以下状态由 Reactor 所有者修改：

- 解析器；
- TX/RX 队列；
- epoll 关注事件；
- 连接处理器/`callback_arg`；
- 连接状态机。

槽位只通过代次/状态原子元数据向非所有者暴露能力快照。

## 2. Reactor 本地定时器

RPC 截止时间、Channel 保活已经使用 Reactor 本地有界定时器队列。

定时器回调：

- 在 Reactor 所有者上执行；
- 必须短小、非阻塞；
- 只能推进所有者侧状态；
- 每轮受定时器预算限制。

当前不存在旧版共享维护调度器，也不存在每 Endpoint/Channel 一个
截止时间/保活/重连线程。客户端自动重连的退避与连接超时分别由
Reactor 本地定时器和共享非阻塞连接器驱动。

## 3. Buffer 资源池

### `tr_buffer_pool.lock`

**结论：保留。**

通用 Buffer 资源池中的缓冲区可以跨 Reactor、工作线程和应用回调转移所有权，
因此空闲链表是真共享状态。

Pool 还承担终局生命周期门禁：

```text
OPEN
  -> acquire: descriptor.checked_out = 1
  -> destroy:
       closed = 1
       outstanding != 0 -> TR_ERR_STATE，保留 storage/lock
       outstanding == 0 -> DEAD
CLOSED
  -> 新 acquire: TR_ERR_CLOSED
  -> 已有 holder release: 允许
  -> retry destroy
```

每个 descriptor 的 `checked_out` 位是 destroy 证明的一部分；仅依赖
`free_count == capacity` 不足以抵御 double-release 伪造空闲计数。
destroy 仍要求终局调用方先停止新的 API entrant；`closed` 解决的是已经持有
descriptor 的异步所有权收敛，不替代父对象的 external lifetime contract。

如果未来某个资源池被证明严格属于一个分片/所有者，可以新增所有者本地快速资源池，
不能直接改变通用资源池的同步契约。

## 4. Channel

### `channel->lock`

**结论：当前保留，职责已经开始拆分。**

Channel drain 使用单调 admission barrier。进入 `local_draining` 后：

- 本地 `tr_stream_open()` 不再分配 Stream；
- 在 peer 收到 GOAWAY 前已发出、但 barrier 之后才到达的
  `STREAM_OPEN` 不再创建本地 Stream，而是直接以 `STREAM_CLOSE` 拒绝；
- 该拒绝的 reciprocal peer-local `STREAM_CLOSE` 在 drain 期间作为尾包吸收；
- 因而 `active_streams` 在 barrier 后只减不增，`DRAINED` 是终态而不是
  一个可能被延迟 OPEN 推翻的瞬时快照。

Channel 的阻塞状态观察统一使用 `channel->lock + state_cond`。
同一个 waitqueue 可以服务多个 predicate，但每个 waiter 必须在循环中重新检查
自己的条件：

- `wait_ready(lane)`：对应 lane 的 `*_ready == 1`；
- `wait_drained()`：`active_streams == 0`。

状态 waitqueue 不是协议 owner。ready/drained waiter 都不发送帧、不推进握手、
重连或排空状态；owner 只在真实状态边沿 broadcast。这样避免为每个 predicate
增加一套 condvar/teardown accounting。

`wait_drained` 使用 `channel->lock + state_cond` 作为 waitqueue-like
同步原语：

- predicate 只有 `active_streams == 0`；
- 最后一个 active Stream 释放时 broadcast；
- waiter 只等待状态，不发送帧、不重试 GOAWAY、不修改协议状态；
- 正常等待直接睡到状态变化或总 deadline，不再每 1 ms 轮询 Stream 数。

GOAWAY admission 属于 `begin_drain()` 的 Reactor-owner 协议动作。
它使用 Connection 内嵌 lifecycle TX slot，并以 command sequence barrier 保留
drain 线性化点之前已经成功提交的命令顺序，因此不再向 facade 暴露普通
CONTROL TX pool / command ring 的 `TR_AGAIN`。Server one-shot drain 在
peer table 经过 listener/peer-event owner barrier 冻结后只需发布一次
`begin_drain()`，随后进入纯 `wait_drained()`。

条件变量使用 `CLOCK_MONOTONIC`，与原先 drain timeout 的时钟语义一致。
Channel destroy 与 waiter 不是并发安全组合。Channel 用
`state_wait_closed + state_waiters` 显式管理 waiter admission/ownership：
waiter 在同一把 lock 下登记后才能睡眠；destroy 先关闭新的 waiter admission，
若已有 waiter 尚未退出则 fail-closed，不开始 callback/timer/storage teardown。
只有 waiter 计数归零后才销毁 condvar/Stream/Pool/mutex。

普通 `tr_channel_destroy()` 是可失败 teardown barrier，而不是无条件
`void free()`。上层 Client/Server 只有看到 `TR_OK` 才能清空 Channel
指针并继续停止 Runtime；否则保持所有权不变，禁止把生命周期错误升级成 UAF。

Server detached 路径更严格：waiter admission 关闭与 `state_waiters == 0`
验证在 Reactor owner detach 阶段完成，此时对象仍在 peer table，失败就拒绝
ownership transfer。只有 barrier 已经收敛才设置 `teardown_detached` 并把
对象交给最后引用 finalizer；因此 detached finalizer 不再执行可失败同步，
只负责纯资源释放。

Channel 仍同时服务 Reactor 回调/所有者调用与部分应用 API，因此
Stream 表、流量控制、通道状态、排空和快照诊断暂时继续由
`channel->lock` 保护。

以下控制面已经退出该锁：

- 上层处理器发布/读取：仅 Reactor 所有者访问；
- 生命周期观察者发布/读取：仅 Reactor 所有者访问；
- 重连 `TCP_NODELAY` 策略发布：Reactor 所有者命令；
- 重连定时器/连接器推进：Reactor 所有者；
- 排空屏障 + 禁用重连 + 首次 `GOAWAY`：同一个 Reactor 所有者事务。

`set_handler()` 在运行期间是同步所有者发布，返回 `TR_OK` 时旧回调已经退出。
完全停止后只允许“仅销毁用途”的直接发布；正在停止时返回 `CLOSED`，
不会越过所有者屏障。

因此 `channel->lock` 当前主要剩余职责已经收敛到 Stream/通道/流量控制、
应用 Stream API 与快照诊断。后续是否继续把这些 API 所有者化，
应按调用语义和性能分析决定，不能为了“删除互斥锁”制造命令往返。

## 5. RPC Endpoint

### `endpoint->lock`

**结论：当前保留为协议/控制面过渡锁。**

RPC 可变协议状态的修改型工作线程 API 已经通过所有者调用回到 Reactor；
工作线程不再成为 Call/Stream 协议状态的共同所有者。

`endpoint->lock` 当前主要保护：

- Call 表/索引与兜底任务完成路径；
- Method 表/索引的一致快照；
- 等待中的执行器准入/Call 状态转换；
- 尚未所有者化的少量诊断/读取路径。

Method 注册已经迁到 Reactor 所有者命令；客户端一元/流式 Call
创建/启动原本也已经通过所有者调用执行。因此应用线程不再直接成为
Method/Call 创建写入者。Method 注册表与入站 `REQUEST` 现在由同一所有者事件顺序串行化。

它不再承担强引用等待、已解除关联最终清理生命周期或 Method 发布顺序。

### `endpoint->ref_lock`

**结论：保留为纯生命周期锁。**

只保护：

- 仅所有者强引用等待使用的 `ref_cond`；
- 已解除关联销毁标志；
- 已解除关联最终清理器指针/参数；
- 引用从 2 -> 1 时等待者唤醒的线性化。

所有强引用释放统一通过 `tr_rpc_endpoint_put()`，因此任务完成、
等待重试完成等不同引用来源都不会漏掉仅所有者等待者唤醒。

后续优化顺序：

1. 把剩余修改型应用 API 收敛到所有者；
2. 继续缩小 `endpoint->lock`；
3. `ref_lock` 保持独立，不把生命周期条件重新并入协议锁；
4. 性能分析仍显示争用后再考虑更细粒度结构。

禁止用大量原子变量重新制造隐式共享可变状态。

## 6. RPC 执行器/执行器组

### 执行器锁

**结论：保留。**

保护 Endpoint 内有界任务节点池、每 Call FIFO、就绪 Call 队列。

### 执行器组锁

**结论：保留。**

保护分片本地共享工作线程池的就绪 Endpoint 队列。

RPC 工作线程使用线程本地执行上下文标记。同步 Endpoint/Facade 销毁
不得从工作线程处理器/结果/事件回调中进入，因为销毁需要等待工作线程强引用
或等待工作线程退出；回调必须先返回，再由外部生命周期所有者执行销毁。

该队列使用 Endpoint 内部侵入式节点；每个 Endpoint 由 `group_enqueued`
保证最多发布一个调度令牌，因此不存在第二个固定大小就绪环，也不存在
正在退役的 Endpoint 重叠导致环形队列填满后丢失替换令牌的状态。

两把锁对应不同队列层级，不是重复锁。工作线程只执行业务任务，
完成后通过完成事件返回原 Reactor 所有者。

## 7. 服务端/运行时

当前服务端不再有旧版 `server->lock`、中央接收线程或独立回收线程。

### 对端表

对端预留/发布/移除/存活快照由所属 Reactor 分片单所有者串行化，
不使用服务端全局对端状态转换锁。

### `server->finalizer_lock`

**结论：保留，但不属于热路径。**

只保护：

- 已解除关联对端的最终统计合并；
- `reaping_current` 等待条件；
- 关闭条件广播。

它不保护对端表、Connection、Channel 或 Buffer 资源池。

### 监听器/对端生命周期 eventfd

每个分片监听器直接注册到对应 Reactor epoll；接收连接由所有者执行。

Channel DOWN/回滚只通知分片本地对端生命周期 eventfd，后续解除关联仍在
同一个 Reactor 所有者轮次执行，不创建回收线程。

## 8. 连接组/Pipeline

Pipeline、注册表、DATA 预留/附着、Stream 亲和关系都属于一个 Reactor 所有者。

热状态不使用 Pipeline 全局互斥锁：

```text
CONTROL/DATA 事件
      ->
Pipeline 所有者 Reactor
      ->
注册表/成员关系/亲和关系修改
```

跨线程调用必须通过所有者调用/命令；注册表不拥有 Pipeline 生命周期。

Pipeline Listener 的运行期状态同样属于 Reactor 所有者，包括
`draining`、监听源注册状态、当前连接数与当前 Pipeline 数。外部 lifecycle
线程执行 `listen/begin_drain/stop` 时，不能在同步 owner 调用前后直接
读取或修改这些字段。

监听 source 与 Listener 状态发布使用同一个生命周期事务：

```text
listen:
epoll ADD
      ->
发布 listen_fd / bound_port / registered / !draining
      ->
允许下一次 dispatch

drain:
epoll DEL
      ->
发布 !registered / draining
      ->
返回调用方
```

运行中，上述步骤在同一个 owner turn 完成；构造阶段允许“先 listen、后
start Reactor”，此时整个事务在 `ctl_lock` 排他区间完成，`start()`
不能插入 registration 与状态发布之间。正在 stop 的 Reactor 返回
`TR_ERR_CLOSED`，不能越过 teardown barrier 直接发布或释放 Listener。

Pipeline Listener 的 `connections_current/pipelines_current` 继续只由 owner
修改。Server Group `wait_drained` 不再每 1 ms 读取统计，而只等待单独发布的
drain generation：

```text
owner begin_drain
    -> listener source detach
    -> draining = 1
    -> drain_generation++

owner connection/session teardown
    -> connections_current-- / pipelines_current--
    -> 两者都为 0
    -> publish drained_generation
    -> cond broadcast

external waiter
    -> 只比较 generation
    -> 不读取 owner counters
```

Listener destroy 也不再是静默的 `void` 析构。它先通过
`tr_reactor_call_or_stopped()` 在 owner/stopped 串行化域验证 source 已 detach、
fd 已关闭、connection/pipeline 计数归零，再关闭 waiter admission。只有验证成功
才释放 registry/session/cond/mutex；正在 stop 或 waiter 尚未退出时 fail-closed。

Client Connection Group 也遵守同一 owner 原则。以下状态只允许 Reactor
所有者修改：

- CONTROL handle / route / generation；
- CONTROL connect reservation；
- remote address / port；
- `closing / draining`；
- DATA connection/transfer/connector scheduling 状态；
- `transfer_count / send_bytes_inflight` 等 drain predicate 原始状态。

外部 `wait_drained` 不再每 1 ms 通过同步 owner stats 命令轮询这些字段。
Group 只额外发布一个独立的 lifecycle generation：

```text
owner begin_drain
    -> drain_generation++

owner transfer/TX completion
    -> predicate 成立
    -> publish drained_generation
    -> cond broadcast

external waiter
    -> 只比较 generation
    -> 不读取/修改 owner protocol state
```

因此 wait metadata lock 不是第二把协议状态锁，不进入正常 DATA send/transfer
热路径；只有 drain lifecycle 的开始/完成边沿会取得它。

CONTROL 建立分为两个阶段：

```text
owner: IDLE -> CONTROL_CONNECTING
            -> 分配 generation
external:   blocking connect + preface
owner:      adopt fd + install handler + publish route/address/state
            -> CONNECTED
```

这样既不把阻塞 connect 放进 Reactor，也不会让外部线程在旧 CONTROL
callback 尚未退出时修改同一组状态。Client Group destroy 不允许通过
读取 `control.reactor` 判断“是否还需要同步”；无论 handle 是否已经被
close callback 清空，都必须先执行一次 owner barrier，确认所有可能引用
Group 的 callback 已退出，再释放 Group/Connector/Buffer storage。

Client Group DATA send buffer 的 release callback 会保存裸 `group *`，
因此 destroy 还必须满足 `send_bytes_inflight == 0`。Reactor connection
close 会先同步释放 TX queue，再分发 connection event；Group teardown
关闭全部 DATA connection 后检查该不变量，未归零时 fail-closed，禁止释放
仍可能被 buffer release callback 引用的 Group。

Client Group destroy 同时关闭 drain waiter admission。已有 waiter 尚未退出时
destroy 返回生命周期错误并保持 Group/Runtime 存活；waiter 会被 terminal
broadcast 唤醒并返回，调用方之后才能重试终局销毁。这与 Channel waiter
规则相同：对象释放不能与仍持有裸对象能力的阻塞 waiter 并发。

销毁顺序要求：

```text
停止新的准入
      ->
失效 DATA 成员关系 / Stream 亲和关系
      ->
清除 CONTROL + 取消 RESERVED
      ->
从注册表注销
      ->
销毁 Pipeline
```

“清除 CONTROL + 注销”必须作为同一个所有者侧提交，不能在取消
`RESERVED` 能力后再尝试通过重新绑定 CONTROL 伪造事务回滚。

## 9. 同步请求的临时互斥锁/条件变量

Reactor 的 `call/quiesce/set_handler` 等同步请求使用临时互斥锁/条件变量。

**结论：保留。**

它们实现：

```text
生产者提交
      ->
Reactor 所有者应用
      ->
生产者返回
```

这是请求/响应同步，不属于事件循环热路径。

## 10. 优化优先级

只有性能分析证明存在争用后，才按以下顺序处理：

1. 继续把纯协议状态收敛到 Reactor 所有者；
2. 缩短 Channel/RPC Endpoint 过渡锁持有范围；
3. 用命令/完成事件等待、队列满和预算指标确认真实调度压力；
4. 为确有争用的高频资源增加所有者本地/每线程缓存；
5. 最后才考虑 `futex`、原子环形队列、无锁空闲链表。

客户端重连、完成事件容量等待、同步命令容量等待已经完成 Reactor 化，
不再列为未来工作。

评审中如果出现“为了少一把锁而新增跨所有者可变共享”，默认视为架构回退。
