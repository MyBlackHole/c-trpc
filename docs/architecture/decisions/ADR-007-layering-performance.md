# ADR-007：分层/API 边界不能通过运行时额外 hop 实现

- Status: Accepted
- Date: 2026-10-03

## Context

c-trpc 同时追求：

- 清晰的 Runtime / Transport / Connection Group / RPC / Facade 分层；
- 明确 ownership；
- 高性能、低拷贝、低锁、低调度开销。

常见误区是把“模块化”实现成：

```text
Layer A
  -> queue
  -> thread B
  -> copy
  -> Layer B
```

如果每一层都引入 thread hop、serialization、heap allocation 或 buffer copy，
确实会伤害性能。

另一种反方向误区是为了避免上述开销，把 lower-layer mutable struct、slot、
generation、queue/pool 直接暴露给上层。这样虽然短期少了一层 API，但会破坏
ownership、lifetime 和 compatibility boundary。

## Decision

c-trpc 采用：

> source/ownership/dependency layering，而不是 mandatory runtime-hop layering。

分层只约束：

- 谁拥有状态；
- 谁允许修改；
- 谁调用谁；
- 哪些 API 属于 compatibility contract；
- lifetime/backpressure 在哪里闭环。

分层**不要求**：

- 每层一个线程；
- 每层一个 queue；
- 每层一次复制；
- 每层一次序列化；
- 每层一次 heap allocation。

同一 Reactor owner domain 内，upper internal module 可以直接调用 lower internal
function。

例如允许：

```text
RPC public API
  -> RPC internal
  -> Transport internal
  -> Reactor owner fast path
  -> sendmsg()
```

而不是强制：

```text
RPC thread
  -> queue
Transport thread
  -> queue
Runtime thread
```

## Performance Rules

允许在模块内部使用：

- direct owner fast path；
- static inline；
- batching；
- scatter/gather；
- ownership transfer；
- zero-copy/copy-minimal；
- bounded/preallocated state；
- shard-local resource；
- specialized internal API。

不允许默认以性能为理由：

- public API 暴露 internal owner pointer；
- public handle 固化 slot/generation；
- 跨 shard 共享 mutable hot state；
- 绕过 module ownership；
- 把 internal queue/pool struct 当 public tuning API。

## Exception Gate

如果 profile 证明 API boundary 本身是瓶颈，优先：

1. 增加 internal fast path；
2. inline/copy elision；
3. batching；
4. 调整 ownership transfer。

只有这些方法不足，且 benchmark 证明 public boundary 必须变化时，才允许新增 ADR。

## Consequences

优点：

- 模块职责可维护；
- public ABI 不被实现细节锁死；
- hot path 仍可 direct-call；
- single-owner 与 locality 更容易优化；
- 性能优化不会自然演变成 shared-state architecture。

代价：

- 需要维护 stable/advanced/internal API 分类；
- 某些测试需要 internal headers；
- public facade 与 internal engine 可能需要 adapter，但 adapter 不应自动增加 copy/hop。

## Rejected Alternatives

### 每层一个独立线程/queue

拒绝。它把模块边界变成调度边界，产生不必要 latency 和 cache transfer。

### 所有实现结构都公开，调用方自行组合

拒绝。它把 library internals 变成 compatibility contract，并把 ownership 正确性转嫁给应用。

### 为了零开销完全取消模块 API

拒绝。C function boundary/opaque handle 本身不是主要成本；真正需要优化的是 copy、
allocation、lock、cache locality 和 scheduling。
