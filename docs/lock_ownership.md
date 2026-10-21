# 锁与并发所有权审查

本文描述**R14 V2 分支当前架构对应的同步模型**（该分支仍待集成）。判断一把锁是否应该存在时，先回答
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

<a id="reactor-运行-epoch"></a>

### Reactor 运行轮次

`stop()` 不是 Reactor 存储的销毁操作。同一个 Reactor 在成功停止后允许
再次 `start()`，因此每次启动/停止都必须形成独立的运行轮次：

```text
CREATED / STOPPED
  accepting = 0
  没有所有者线程
        |
        | 在 ctl_lock 下执行 start()
        v
  stopping = 0
  创建所有者 pthread
  所有者等待 ctl_lock 启动门禁
  重新开放完成事件准入代次
  重新开放命令等待代次
  started = 1
  accepting = 1
        |
        | 解锁 ctl_lock
        v
RUNNING
        |
        | stop()
        v
  accepting = 0
  关闭完成事件准入 + 推进代次
  关闭命令等待者准入 + 推进代次
  在此前所有已准入命令之后加入 STOP
  所有者排空已接受的完成事件
  所有者关闭存活连接
  等待所有者线程退出
  started = 0
        v
STOPPED
```

关键不变量：

- `create()` 本身不开放生产者准入；
- 重新启动的新所有者线程在 `start()` 完整发布新运行轮次前不能分发
  已保留的定时器/监听器/辅助事件源；
- 完成事件生产者不取得 `ctl_lock`；一次移交开始时先在
  完成队列锁下取得明确的准入代次，并把同一代次
  带到最终 `push_wait`，因此即使生产者在入队前跨越完整的停止/重新启动过程，
  也不能重新认领新运行轮次；
- 命令/完成队列的代次每轮重新开放，旧运行轮次等待者或
  延迟生产者即使在重新启动后才继续执行，也只能返回 `TR_ERR_CLOSED`；
- 停止会回收所有存活的 Connection，旧 `tr_conn_handle` 不跨运行轮次存活；
- 定时器/监听器/辅助事件源注册属于 Reactor 对象，而不是某个运行轮次；
  未显式注销的事件源会保留到下一轮启动，并且只能在启动门禁
  之后恢复分发。

### 命令队列/完成队列锁

**结论：保留。**

两者都是有界多生产者单消费者队列：

- 多个生产者提交；
- 单个 Reactor 所有者消费。

队列锁是准入/所有权转移的线性化点。当前互斥锁实现不位于套接字 I/O 热路径，
没有性能分析证据前不改成无锁实现。

完成队列与命令队列相互独立，工作线程完成事件不会占用控制命令容量。

完成事件移交与命令等待一样绑定明确的运行轮次令牌：

```text
生产者
  -> 获取完成事件 admission_generation 快照
  -> 工作可能阻塞 / 被抢占
  -> push_wait(expected_generation)
       当前代次已变化 -> CLOSED
       代次相同                   -> 入队
```

代次必须在移交开始时取得，不能等到真正进入 `push_wait()` 后才
读取“当前代次”；否则一个旧生产者可以在停止/重新启动后误投到新运行轮次。

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
每个物理 Connection 内嵌一个仅含帧头的 `lifecycle_tx` 槽位：

```text
普通 CONTROL
    -> control_tx_pool
    -> 命令环
    -> Connection TX FIFO

GOAWAY
    -> connection.lifecycle_tx
    -> 等待屏障前的命令序列全部处理
    -> Connection TX FIFO
```

GOAWAY 不占普通 TX 资源池，也不占新的命令环槽位，但不能越过屏障前
已经成功提交的命令。命令队列在现有锁下为每个入队命令分配单调
序列；Channel 排空操作记录当前 FIFO 边界，Reactor 只在
`last_processed_sequence >= lifecycle_after_sequence` 时把内嵌 GOAWAY
挂到 TX 队尾。这样终局预留不会把关闭资源竞争转化成 FIFO 破坏。

平时没有待处理的生命周期 TX 时，Reactor 不扫描 Connection；只有
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

当前不存在旧版共享维护调度器或每次 RPC 调用一个计时线程。V2 Client/Server
Record 进展、Channel 保活和连接器超时由 Reactor 本地定时器驱动；客户端自动
重连的退避与连接超时分别由本地定时器和共享非阻塞连接器推进。

<a id="21-connector-callback-source-ownership"></a>

## 2.1 Connector 回调源所有权

Connector 的可变连接尝试状态仍由 Reactor 所有者单写，但它同时持有两类
异步回调源：

- 辅助 fd 事件源（EPOLLOUT）；
- 连接器超时定时器。

因此 `watched` / `timer_registered` 不是普通布尔缓存，而是回调
所有权发布。状态清除必须发生在事件源解除关联屏障之后：

```text
辅助 fd/定时器事件源已发布
        ->
unregister/disarm
        ->
TR_OK 或 STALE（已确认事件源不存在）
        ->
清除 watched / timer_registered
```

禁止：

```text
watched = 0
        ->
尝试 unregister
        ->
失败后释放 Connector
```

Connector 完成处理也必须先完成事件源解除关联，再把 fd/状态发布给上层
回调。完成回调允许同步销毁 Connector，因此回调返回后
Connector 栈帧不得再次解引用 Connector。

父对象遵守相同的两阶段提交：

```text
禁用重连 / 停止 DATA
        ->
拆除 Connector/定时器
        ->
成功后
        ->
清除父对象中的连接器指针 / 槽位 / 预留
```

任何拆除屏障失败都保留精确的所有者句柄/指针，使终局
操作可以重试；不能通过先清本地状态来“伪造已解绑”。

## 3. Buffer 资源池

### `tr_buffer_pool.lock`

**结论：保留。**

通用 Buffer 资源池中的缓冲区可以跨 Reactor、工作线程和应用回调转移所有权，
因此空闲链表是真共享状态。

资源池还承担终局生命周期门禁：

```text
OPEN
  -> acquire: descriptor.checked_out = 1
  -> destroy:
       closed = 1
       outstanding != 0 -> TR_ERR_STATE，保留存储/锁
       outstanding == 0 -> DEAD
CLOSED
  -> 新 acquire: TR_ERR_CLOSED
  -> 已有持有者 release: 允许
  -> 重试 destroy
```

每个描述符的 `checked_out` 位是销毁条件验证的一部分；仅依赖
`free_count == capacity` 不足以抵御重复释放伪造空闲计数。
销毁仍要求终局调用方先阻止新的 API 调用进入；`closed` 解决的是已经持有
描述符的异步所有权收敛，不替代父对象的外部生命周期契约。

如果未来某个资源池被证明严格属于一个分片/所有者，可以新增所有者本地快速资源池，
不能直接改变通用资源池的同步契约。

## 4. Channel

### `channel->lock`

**结论：当前保留，职责已经开始拆分。**

Channel 排空使用单调准入屏障。进入 `local_draining` 后：

- 本地 `tr_stream_open()` 不再分配 Stream；
- 在对端收到 GOAWAY 前已发出、但屏障之后才到达的
  `STREAM_OPEN` 不再创建本地 Stream，而是直接以 `STREAM_CLOSE` 拒绝；
- 对端本地针对该拒绝回应的 `STREAM_CLOSE` 在排空期间作为尾包吸收；
- 因而 `active_streams` 在屏障后只减不增，`DRAINED` 是终态而不是
  一个可能被延迟 OPEN 推翻的瞬时快照。

Channel 的阻塞状态观察统一使用 `channel->lock + state_cond`。
同一个等待队列可以服务多个判断条件，但每个等待者必须在循环中重新检查
自己的条件：

- `wait_ready(lane)`：对应通道的 `*_ready == 1`；
- `wait_drained()`：`active_streams == 0`。

状态等待队列不是协议所有者。就绪/排空等待者都不发送帧、不推进握手、
重连或排空状态；所有者只在真实状态边沿广播唤醒。这样避免为每个判断条件
增加一套条件变量/拆除记账。

`wait_drained` 使用 `channel->lock + state_cond` 作为类似等待队列的
同步原语：

- 判断条件只有 `active_streams == 0`；
- 最后一个活跃 Stream 释放时广播唤醒；
- 等待者只等待状态，不发送帧、不重试 GOAWAY、不修改协议状态；
- 正常等待直接睡到状态变化或总截止时间，不再每 1 ms 轮询 Stream 数。

GOAWAY 准入属于 `begin_drain()` 的 Reactor 所有者协议动作。
它使用 Connection 内嵌生命周期 TX 槽位，并以命令序列屏障保留
排空线性化点之前已经成功提交的命令顺序，因此不再向门面暴露普通
CONTROL TX 资源池/命令环的 `TR_AGAIN`。服务端一次性排空在
对端表经过监听器/对端事件所有者屏障冻结后只需发布一次
`begin_drain()`，随后进入纯 `wait_drained()`。

条件变量使用 `CLOCK_MONOTONIC`，与原先排空超时的时钟语义一致。
Channel 销毁与等待操作不是并发安全组合。Channel 用
`state_wait_closed + state_waiters` 显式管理等待者准入/所有权：
等待者在同一把锁下登记后才能睡眠；销毁先关闭新的等待者准入，
若已有等待者尚未退出则按失败即关闭原则处理，不开始回调/定时器/存储拆除。
只有等待者计数归零后才销毁条件变量/Stream/资源池/互斥锁。

普通 `tr_channel_destroy()` 是可能失败的拆除屏障，而不是无条件
`void free()`。上层 Client/Server 只有看到 `TR_OK` 才能清空 Channel
指针并继续停止 Runtime；否则保持所有权不变，禁止把生命周期错误升级成 UAF。

服务端已解除关联路径更严格：等待者准入关闭与 `state_waiters == 0`
验证在 Reactor 所有者解除关联阶段完成，此时对象仍在对端表，失败就拒绝
所有权转移。只有屏障已经收敛才设置 `teardown_detached` 并把
对象交给最后引用的最终清理器；因此已解除关联最终清理器不再执行可失败同步，
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

## 5. RPC V2 Record/dispatch

R14 已退役旧 V1 `tr_rpc_endpoint` 对象及其 `lock/ref_lock` 协议和生命周期锁。
不要把旧锁字段、Call 表或 Endpoint 共享状态当作当前并发合同。

当前 V2 Record/dispatch 协议状态由所属 Reactor 分片推进。跨执行域的业务工作
通过分片 `TaskExecutor` 提交，完成结果再回到原所有者；Client/Server 门面负责
生命周期准入和同步拆除屏障。当前实现细节见
[V2 SDK 合同](rpc-v2-sdk.md)与[运行时模型](architecture/01-runtime.md)。

V2 的锁只按仍存在的真实共享状态保留，例如 `TaskExecutor` 的有界工作队列，
以及 producer 域的跨线程引用/等待条件；不为已删除的 V1 Endpoint 复制旧锁模型。

## 6. V2 TaskExecutor

当前实现是每分片 `TaskExecutor`，不是旧 V1 Endpoint 执行器加共享 Executor Group。
保留的队列锁只保护 TaskExecutor 的真实生产者/消费者共享工作队列和关闭条件；
V2 Record/dispatch 状态与队列调度锁分离。工作线程执行业务任务，完成结果返回
原 Reactor 所有者。门面销毁必须在外部生命周期控制线程执行，不能从业务 work
或 I/O 回调中同步销毁当前正在使用的对象。

已退役的 `tr_rpc_executor_group`、就绪 Endpoint 队列、侵入式调度令牌及其
`group_enqueued` 不属于当前锁模型。具体队列合同见
[`src/execution/task_executor.c`](../src/execution/task_executor.c)。

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
`draining`、监听源注册状态、当前连接数与当前 Pipeline 数。外部生命周期
线程执行 `listen/begin_drain/stop` 时，不能在同步所有者调用前后直接
读取或修改这些字段。

监听事件源与 Listener 状态发布使用同一个生命周期事务：

```text
listen:
epoll ADD
      ->
发布 listen_fd / bound_port / registered / !draining
      ->
允许下一次分发

drain:
epoll DEL
      ->
发布 !registered / draining
      ->
返回调用方
```

运行中，上述步骤在同一个所有者执行轮次内完成；构造阶段允许“先监听、后
启动 Reactor”，此时整个事务在 `ctl_lock` 排他区间完成，`start()`
不能插入注册与状态发布之间。正在停止的 Reactor 返回
`TR_ERR_CLOSED`，不能越过拆除屏障直接发布或释放 Listener。

Pipeline Listener 的 `connections_current/pipelines_current` 继续只由所有者
修改。服务端连接组 `wait_drained` 不再每 1 ms 读取统计，而只等待单独发布的
排空代次：

```text
所有者执行 begin_drain
    -> 监听事件源解除关联
    -> draining = 1
    -> drain_generation++

所有者拆除连接/会话
    -> connections_current-- / pipelines_current--
    -> 两者都为 0
    -> 发布 drained_generation
    -> 条件变量广播唤醒

外部等待者
    -> 只比较代次
    -> 不读取所有者计数器
```

Listener 销毁也不再是静默的 `void` 析构。它先通过
`tr_reactor_call_or_stopped()` 在所有者/已停止状态串行化域验证事件源已解除关联、
fd 已关闭、连接/Pipeline 计数归零，再关闭等待者准入。只有验证成功
才释放注册表/会话/条件变量/互斥锁；正在停止或等待者尚未退出时按失败即关闭原则处理。

客户端连接组也遵守同一所有者原则。以下状态只允许 Reactor
所有者修改：

- CONTROL 句柄/路由/代次；
- CONTROL 连接预留；
- 远端地址/端口；
- `closing / draining`；
- DATA 连接/传输/连接器调度状态；
- `transfer_count / send_bytes_inflight` 等排空判断条件原始状态。

外部 `wait_drained` 不再每 1 ms 通过同步所有者统计命令轮询这些字段。
Group 只额外发布一个独立的生命周期代次：

```text
所有者执行 begin_drain
    -> drain_generation++

所有者处理传输/TX 完成事件
    -> 判断条件成立
    -> 发布 drained_generation
    -> 条件变量广播唤醒

外部等待者
    -> 只比较代次
    -> 不读取/修改所有者协议状态
```

因此等待元数据锁不是第二把协议状态锁，不进入正常 DATA 发送/传输
热路径；只有排空生命周期的开始/完成边沿会取得它。

CONTROL 建立分为两个阶段：

```text
所有者：IDLE -> CONTROL_CONNECTING
            -> 分配代次
外部：  阻塞连接 + 前导信息
所有者：接管 fd + 安装处理器 + 发布路由/地址/状态
            -> CONNECTED
```

这样既不把阻塞连接放进 Reactor，也不会让外部线程在旧 CONTROL
回调尚未退出时修改同一组状态。客户端连接组销毁不允许通过
读取 `control.reactor` 判断“是否还需要同步”；无论句柄是否已经被
关闭回调清空，都必须先执行一次所有者屏障，确认所有可能引用
Group 的回调已退出，再释放 Group/Connector/Buffer 存储。

客户端连接组 DATA 发送缓冲区的释放回调会保存裸 `group *`，
因此销毁还必须满足 `send_bytes_inflight == 0`。Reactor 连接
关闭会先同步释放 TX 队列，再分发连接事件；Group 拆除
关闭全部 DATA 连接后检查该不变量，未归零时按失败即关闭原则处理，禁止释放
仍可能被缓冲区释放回调引用的 Group。

客户端连接组销毁同时关闭排空等待者准入。已有等待者尚未退出时
销毁返回生命周期错误并保持 Group/Runtime 存活；等待者会被终局
广播唤醒并返回，调用方之后才能重试终局销毁。这与 Channel 等待者
规则相同：对象释放不能与仍持有裸对象能力的阻塞等待者并发。

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
2. 根据测量缩小 Channel 与 TaskExecutor 的真实共享区；
3. 用命令/完成事件等待、队列满和预算指标确认真实调度压力；
4. 为确有争用的高频资源增加所有者本地/每线程缓存；
5. 最后才考虑 `futex`、原子环形队列、无锁空闲链表。

客户端重连、完成事件容量等待、同步命令容量等待已经完成 Reactor 化，
不再列为未来工作。

评审中如果出现“为了少一把锁而新增跨所有者可变共享”，默认视为架构回退。
