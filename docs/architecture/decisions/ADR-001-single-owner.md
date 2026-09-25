# ADR-001：Protocol state 采用 Reactor single-owner

**Status: Accepted**

## Context

Connection、Channel、Stream、RPC Call 等对象一旦允许 Reactor、worker、maintenance、application thread 共同直接修改，就需要大量 mutex/atomic，并容易产生生命周期 race。

## Decision

Mutable protocol state 由一个 Reactor shard 独占。

非 owner：

```text
command/completion
    -> owner Reactor
    -> mutate
```

不允许通过“先拿对象锁”成为临时共同 owner。

## Consequences

优点：

- hot path 大量状态无需锁；
- ownership/lifetime 更容易证明；
- stale generation 可以在 owner 入口统一处理；
- TSan 模型更清晰。

代价：

- cross-thread API 需要 command queue；
- 需要注意 queue backpressure 和 wakeup；
- owner Reactor 可能成为 elephant-flow 单核瓶颈。

如果 profiling 证明单 owner 成为结构瓶颈，应进一步拆 state shard，而不是让多个线程直接共享同一对象。
