# ADR-004：RPC worker 通过 Completion 回 owner

**Status: Accepted**

## Context

Blocking handler 必须在线程池执行，但如果 worker 直接持 Endpoint/Call 并修改协议状态，就会重新把 Reactor-owned state 变成跨线程 shared mutable state。

## Decision

执行路径：

```text
Reactor
  -> immutable Task
  -> Worker
  -> owned Result/Completion
  -> original Reactor
```

Worker 不直接修改 Call/Endpoint/Channel/Pipeline protocol state。

## Consequences

需要：

- per-Reactor completion queue；
- call/endpoint generation；
- stale completion 丢弃；
- cancellation token；
- streaming API 内部转 owner command。

第一阶段先迁移 Unary，Streaming 后续迁移。
