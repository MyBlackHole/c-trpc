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

TX 项可能由应用/RPC 生产者获取、由 Reactor 所有者释放，因此空闲链表
是真正的跨线程共享资源。后续如果性能分析证明争用明显，可以增加每所有者/
每线程缓存，而不是先改变所有权模型。

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

如果未来某个资源池被证明严格属于一个分片/所有者，可以新增所有者本地快速资源池，
不能直接改变通用资源池的同步契约。

## 4. Channel

### `channel->lock`

**结论：当前保留，职责已经开始拆分。**

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

Client Connection Group 也遵守同一 owner 原则。以下状态只允许 Reactor
所有者修改：

- CONTROL handle / route / generation；
- CONTROL connect reservation；
- remote address / port；
- `closing / draining`；
- DATA connection/transfer/connector scheduling 状态。

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
