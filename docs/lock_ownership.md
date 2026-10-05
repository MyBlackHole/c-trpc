# 锁与并发所有权审查

本文描述 **当前 main 架构对应的同步模型**。判断一把锁是否应该存在时，先回答
“谁拥有这份可变状态”，再讨论 mutex、atomic 或 lock-free。

核心原则：

> 能由 Reactor single-owner 串行化的协议状态，不使用 mutex 弥补 ownership 不清晰；
> 真正跨线程共享的队列、资源池和生命周期条件，保留最简单、可验证的同步机制。

## 1. Reactor

### `reactor->ctl_lock`

**结论：保留。**

它保护 producer 与 Reactor 生命周期之间的控制面事务，主要包括：

- `started / accepting` 门禁；
- stop 与 command admission 的线性化顺序；
- connection slot reservation 与 command 提交之间的事务边界。

`ctl_lock` 不保护 socket RX/TX、parser、connection handler 等 owner 热状态。

停止顺序必须保持：

```text
accepting = false
      ->
close completion admission
      ->
enqueue STOP
      ->
owner drain 已接受 completion
      ->
close connections
      ->
join Reactor
```

`stop/destroy` 是 external lifecycle barrier，禁止从 Reactor owner callback 内执行。
owner TLS 会让 `tr_reactor_stop()` 在任何 admission mutation 之前返回
`TR_ERR_STATE`，从而避免 `pthread_join(self)` 或 partial-stop。

这样 STOP 之后不会再出现“command/completion 已取得资源 ownership，但 owner 已退出”
的悬空工作。

### Command queue / Completion queue lock

**结论：保留。**

两者都是 bounded MPSC：

- 多 producer 提交；
- 单 Reactor owner 消费。

queue lock 是 admission/ownership transfer 的线性化点。当前 mutex 实现不位于
socket I/O 热路径，没有 profile 证据前不改成 lock-free。

Completion queue 与 Command queue 独立，worker completion 不占用控制 command
容量。

两类 queue 的满载策略不同但都不使用 `sched_yield()`：

- Completion：所有跨线程 handoff 都在 queue-local `not_full` 上等待容量；
- Command：SEND/RESUME/CLOSE 等异步 API 仍立即返回 `TR_AGAIN`；只有
  CALL/QUIESCE/SET_HANDLER 等同步 owner request 才等待容量；
- STOP 使用 lifecycle-only force wait，不受普通 command waiter admission 关闭影响。

stop 先关闭 completion admission 和普通 command waiter admission，推进各自
generation 并唤醒 waiter，然后 STOP 自己等待真实 command slot。已经成功入队的
旧工作仍按 FIFO 在 STOP 之前执行。

### TX pool lock

**结论：保留。**

TX item 可能由 application/RPC producer 获取、由 Reactor owner 释放，因此 free-list
是真正的跨线程共享资源。后续若 profile 证明争用明显，可增加 per-owner/per-thread
cache，而不是先改变 ownership 模型。

### Connection mutable state

**结论：owner-only，不加 mutex。**

以下状态由 Reactor owner 修改：

- parser；
- TX/RX queue；
- epoll interest；
- connection handler/callback_arg；
- connection state machine。

slot 只通过 generation/state atomic metadata 向非 owner 暴露 capability snapshot。

## 2. Reactor-local Timer

RPC deadline、Channel keepalive 已使用 Reactor-local bounded timer queue。

Timer callback：

- 在 Reactor owner 上执行；
- 必须短小、非阻塞；
- 只能推进 owner-side 状态；
- 每轮受 timer budget 限制。

当前不存在旧版 shared maintenance scheduler，也不存在每 Endpoint/Channel 一个
deadline/keepalive/reconnect thread。Client automatic reconnect 的 backoff 与 connect
timeout 分别由 Reactor-local timer 和 shared nonblocking connector 驱动。

## 3. Buffer pool

### `tr_buffer_pool.lock`

**结论：保留。**

通用 buffer pool 的 buffer 可以跨 Reactor、worker 和 application callback 转移
ownership，free-list 因此是真共享状态。

如果未来某个 pool 被证明严格属于一个 shard/owner，可以新增 owner-local fast pool，
不能直接改变通用 pool 的同步契约。

## 4. Channel

### `channel->lock`

**结论：当前保留，职责已经开始拆分。**

Channel 仍同时服务 Reactor callback / owner call 与部分 application API，因此
Stream table、flow-control、lane state、drain 和 snapshot diagnostics 暂时继续由
`channel->lock` 保护。

以下控制面已经退出该锁：

- upper-layer handler publication/read：Reactor owner-only；
- lifecycle observer publication/read：Reactor owner-only；
- reconnect TCP_NODELAY policy publication：Reactor owner command；
- reconnect timer/connector progression：Reactor owner；
- drain barrier + reconnect disable + initial GOAWAY：同一个 Reactor owner transaction。

`set_handler()` 在运行中是同步 owner publication，返回 TR_OK 时旧 callback 已退出。
完全 stopped 时只允许 teardown-only direct publication；正在 stop 时返回 CLOSED，
不会越过 owner barrier。

因此 `channel->lock` 当前主要剩余职责已经收敛到 Stream/lane/flow-control、
application Stream API 与 snapshot diagnostics。后续是否继续 owner 化这些 API，
应按调用语义和 profile 决定，不为了“删除 mutex”制造 command round-trip。

## 5. RPC Endpoint

### `endpoint->lock`

**结论：当前保留为 protocol/control-plane 过渡锁。**

RPC 可变协议状态的修改型 worker API 已经通过 owner-call 回到 Reactor；worker
不再成为 Call/Stream protocol state 的共同 owner。

`endpoint->lock` 当前主要保护：

- Call table/index 与 fallback task completion；
- Method table/index 的一致性 snapshot；
- pending executor admission/Call transition；
- 尚未 owner 化的少量 diagnostics/read 路径。

Method registration 已迁到 Reactor owner command；Client Unary/Streaming Call
creation/start 原本也已经通过 owner-call 执行。因此 application thread 不再直接
成为 Method/Call creation writer。Method registry 与 inbound REQUEST 现在由同一
owner event ordering 串行化。

它不再承担 strong-ref wait、detached-finalizer lifecycle 或 Method publication
ordering。

### `endpoint->ref_lock`

**结论：保留为纯 lifetime lock。**

只保护：

- owner-only strong-ref wait 的 `ref_cond`；
- detached teardown flag；
- detached finalizer pointer/arg；
- refs 从 2 -> 1 时的 waiter wakeup 线性化。

所有 strong-ref release 统一通过 `tr_rpc_endpoint_put()`，因此 task completion、
pending-retry completion 等不同引用来源都不会漏掉 owner-only waiter 唤醒。

后续优化顺序：

1. 把剩余修改型 application API 收敛到 owner；
2. 继续缩小 `endpoint->lock`；
3. `ref_lock` 保持独立，不把 lifetime condition 再并回 protocol lock；
4. profile 仍显示争用后再考虑更细粒度结构。

禁止用大量 atomic 重新制造隐式 shared mutable state。

## 6. RPC executor / executor group

### Executor lock

**结论：保留。**

保护 Endpoint 内 bounded task-node pool、per-Call FIFO、ready-Call queue。

### Executor-group lock

**结论：保留。**

保护 shard-local shared worker pool 的 ready-Endpoint queue。

RPC worker 使用 thread-local execution-context 标记。同步 Endpoint/Facade destroy
不得从 worker handler/result/event callback 中进入，因为 destroy 需要等待 worker
strong-ref 或 join worker；callback 必须先返回，再由 external lifecycle owner
执行 destroy。该队列使用 Endpoint
内部 intrusive node；每个 Endpoint 由 `group_enqueued` 保证最多发布一个 scheduling
token，因此不存在第二个固定-size ready ring，也不存在 retiring Endpoint overlap
把 ring 填满后丢失 replacement token 的状态。

两把锁对应不同队列层级，不是重复锁。worker 只执行业务 Task，完成后通过
Completion 返回原 Reactor owner。

## 7. Server / Runtime

当前 Server 不再有旧版 `server->lock`、central accept thread 或 dedicated reaper
thread。

### Peer table

Peer reserve/publish/remove/live snapshot 由所属 Reactor shard single-owner 串行化，
不使用 Server-global peer transition lock。

### `server->finalizer_lock`

**结论：保留，但不属于热路径。**

只保护：

- detached peer 最终统计合并；
- `reaping_current` 等待条件；
- shutdown condition broadcast。

它不保护 peer table、Connection、Channel 或 buffer pool。

### Listener / peer lifecycle eventfd

每 shard listener 直接注册到该 Reactor epoll；accept 由 owner 执行。

Channel DOWN/rollback 只 signal shard-local peer lifecycle eventfd，后续 detach 仍在
同一 Reactor owner turn 执行，不创建 reaper thread。

## 8. Connection Group / Pipeline

Pipeline、registry、DATA reservation/attach、Stream affinity 都属于一个 Reactor owner。

热状态不使用 Pipeline-global mutex：

```text
CONTROL/DATA event
      ->
Pipeline owner Reactor
      ->
registry / membership / affinity mutation
```

跨线程调用必须通过 owner-call/command；registry 不拥有 Pipeline lifetime。

teardown 的顺序要求：

```text
停止新 admission
      ->
失效 DATA membership / Stream affinity
      ->
clear CONTROL + cancel RESERVED
      ->
unregister registry
      ->
destroy Pipeline
```

`clear CONTROL + unregister` 必须作为同一 owner-side commit，不能在取消 RESERVED
capability 后再尝试通过重新绑定 CONTROL 伪造事务回滚。

## 9. 同步 request 的临时 mutex/cond

Reactor `call/quiesce/set_handler` 等同步 request 使用临时 mutex/cond。

**结论：保留。**

它们实现：

```text
producer submit
      ->
Reactor owner apply
      ->
producer returns
```

这是 request/reply 同步，不属于 event-loop 热路径。

## 10. 优化优先级

只有 profile 证明 contention 后，按以下顺序处理：

1. 继续把纯协议状态收敛到 Reactor owner；
2. 缩短 Channel/RPC Endpoint 过渡锁持有范围；
3. 用 command/completion wait/full/budget 指标确认真实调度压力；
4. 为确有争用的高频资源增加 owner-local/per-thread cache；
5. 最后才考虑 futex/atomic ring/lock-free freelist。

Client reconnect、completion capacity wait、同步 command capacity wait 已经完成
Reactor 化，不再列为未来工作。

评审中如果出现“为了少一把锁而新增跨 owner 可变共享”，默认视为架构回退。
