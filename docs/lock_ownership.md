# 锁与并发所有权审查

本文记录 c-trpc 当前主要 mutex 的职责、是否必要，以及未来可以删除它们的前提。

原则：

> 能通过 single-owner 解决的状态，不使用 mutex 弥补 ownership 不清晰；
> 真正跨线程共享的数据，保留最简单、可验证的同步机制。

## 1. Reactor

### `reactor->ctl_lock`

**结论：保留。**

保护内容：

- `started / accepting` 生命周期门禁；
- stop 与 producer enqueue 的顺序；
- slot reservation 与 command 提交之间的控制面事务。

虽然 command queue 自身已经有 mutex，但两者职责不同。

如果直接删除 `ctl_lock`，可能出现：

```text
producer 看到 accepting=true
        |
stop 设置 accepting=false
        |
STOP command 入队
        |
producer 再把带资源的 command 排到 STOP 后面
        |
Reactor 退出
        |
command 永远不消费 -> resource leak
```

因此只有当 command queue 自身支持“close + enqueue ordering”原语后，
`ctl_lock` 才有条件删除。

### `command_queue.lock`

**结论：保留。**

这是 bounded MPSC queue 的真实共享状态：

- 多 producer push；
- 单 Reactor consumer pop。

当前 mutex 实现简单、正确，而且不位于 socket RX/TX event hot path。

除非 profiling 证明这里成为瓶颈，否则不引入 lock-free MPSC。

### TX pool lock

**结论：保留。**

TX item：

- 由 application/RPC producer thread acquire；
- 由 Reactor owner thread release。

free-list 因此是真正跨线程共享资源。

未来可以替换为 per-thread cache 或 lock-free freelist，但不是 ownership 清理问题。

### 已删除：`slot_lock`

slot 的 generation/state 已改为 C11 atomic capability metadata。

connection handler、parser、TX/RX state、epoll state 归 Reactor owner thread 独占，
event-loop 不再通过 mutex 访问这些状态。

## 2. Buffer pool

### `tr_buffer_pool.lock`

**结论：保留。**

buffer 可能在：

- Reactor；
- RPC worker；
- application callback；
- maintenance/reassembly 生命周期

之间转移 ownership。

pool free-list 是真实 shared state，因此需要同步。

如果未来一个 pool 被严格限定为单 owner，可为该场景提供无锁专用 pool，
但不能删除通用 pool 的锁。

## 3. Channel

### `channel->lock`

**结论：保留。**

当前 Channel 同时被以下执行上下文访问：

- Reactor callback；
- application/API thread；
- reconnect thread；
- shared maintenance scheduler callback。

保护内容包括：

- connection handle/lane state；
- Stream table；
- flow-control counters；
- capability negotiation；
- keepalive/reconnect state；
- diagnostics counters。

上一轮 Reactor single-owner 重构暴露出的 connection-handle race 也证明，
这些状态目前确实是跨线程共享的。

未来如果 Channel 的全部状态也迁入 Reactor owner thread，才可能大规模减少该锁；
在当前模型下直接删除是不安全的。

## 4. RPC Endpoint

### `endpoint->lock`

**结论：保留。**

共享执行上下文：

- Reactor/Channel callback；
- RPC executor worker；
- application thread；
- deadline scheduler。

保护：

- Call slot；
- Method/Call 生命周期；
- deadline；
- cancellation；
- pending control/data；
- Endpoint refcount drain 条件。

这不是冗余锁。

后续若出现 contention，应优先考虑：

- Call 分片锁；
- owner queue；
- 减少持锁期间的编码/发送工作；

而不是直接换成 lock-free。

## 5. RPC executor / executor group

### executor lock

**结论：保留。**

保护 per-Endpoint bounded task node pool、Call FIFO、ready queue。

### executor-group lock

**结论：保留。**

保护 Server shared worker 的 ready-Endpoint queue。

这两把锁保护不同层级的队列，不是重复锁。

## 6. Shared maintenance scheduler

### scheduler lock

**结论：保留。**

保护：

- entry register/unregister；
- absolute deadline 更新；
- callback running 状态；
- scheduler stop。

重要约束：

- scheduler callback 在 **不持 scheduler lock** 的情况下执行；
- unregister 可以等待正在运行的 callback；
- callback 内可以安全获取 Channel/RPC lock；
- 避免形成 scheduler lock -> object lock 的长链。

因此 scheduler lock 只保护调度元数据，不包围业务 callback。

## 7. Server

### `server->lock`

**结论：保留。**

Server 同时有：

- accept thread；
- reaper thread；
- control/drain/destroy thread。

保护 peer table、peer_count、stop flags 和 listener 生命周期快照。

即使 stop flag 改成 atomic，peer table 仍然需要同步，因此单独删除该 mutex 收益很低。

## 8. 同步 request 的临时 mutex/cond

例如 Reactor quiescence / synchronous SET_HANDLER 使用的临时 sync object。

**结论：保留。**

这是跨线程 request/reply 的一次性同步，不在 event hot path。

它保证：

```text
producer submit command
        |
Reactor owner apply
        |
producer returns
```

如果未来引入统一 completion/future primitive，可以统一实现，但不能简单删除等待同步。

## 9. 本轮实际减少的同步开销

本轮 shared maintenance 改造后，Server 不再为每个 peer 创建：

- 1 个 RPC deadline thread；
- 1 个 Channel keepalive thread（启用 keepalive 时）。

替换为：

```text
Server
  |
  +-- one shared maintenance scheduler thread
       |
       +-- Endpoint deadline entry
       +-- Channel keepalive entry
       +-- Endpoint deadline entry
       +-- Channel keepalive entry
       ...
```

因此 peer 数量增加时，timer thread 数量保持 O(1)。

Client reconnect thread 暂时保留，因为 reconnect 包含 connect/poll/backoff，
可能长时间阻塞，不应该在共享 timer scheduler 上执行。

## 10. 后续锁优化优先级

只有 profiling 证明存在 contention 后，建议按以下顺序处理：

1. 缩短 Channel/RPC Endpoint 持锁范围；
2. 将纯 owner-state 继续迁移到 owner thread；
3. 为高频 pool 增加 per-thread/per-owner cache；
4. 最后才考虑 lock-free command queue / freelist。

不建议为了“锁更少”直接把当前有明确 shared ownership 的 mutex 换成原子操作。
