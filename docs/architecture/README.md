# c-trpc 架构文档

本目录使用“分层边界 + ownership + 多视图 + ADR”表达 c-trpc 架构。

c-trpc core 的范围是通用 Runtime / Transport / Connection Group / RPC。
Backup、数据库同步、对象复制等属于上层业务；它们可以使用 c-trpc，但不反向定义 core API。

## Core 阅读顺序

1. [00-overview.md](00-overview.md)：系统边界、层次和当前核心数据流。
2. [07-layer-module-api-boundaries.md](07-layer-module-api-boundaries.md)：层职责、模块职责、public/internal API、当前边界审查。
3. [01-runtime.md](01-runtime.md)：Server/Client Runtime、Reactor shard、listener 和线程模型。
4. [02-ownership.md](02-ownership.md)：谁拥有状态、谁可以修改状态、哪些地方允许锁。
5. [03-rpc-execution.md](03-rpc-execution.md)：RPC Task → Worker → Completion → Reactor owner。
6. [08-rpc-streaming-conformance.md](08-rpc-streaming-conformance.md)：四种 RPC cardinality、half-close、final STATUS 与 terminal callback 契约。
7. [09-rpc-context-metadata.md](09-rpc-context-metadata.md)：Call Context、initial/trailing metadata。
8. [10-rpc-interceptor-v1.md](10-rpc-interceptor-v1.md)：固定 Call-level Interceptor phase、owner execution、rejection 与 reentry contract。
9. [04-backup-pipeline.md](04-backup-pipeline.md)：当前 legacy 文件名；其中 Pipeline/Connection Group foundation 属于 core，Backup durable 语义只作为上层映射参考。
10. [06-evolution.md](06-evolution.md)：c-trpc core 演进路线。

## Business-layer reference（non-core）

- [05-durability-recovery.md](05-durability-recovery.md)：Backup durability/recovery 示例。该文档不定义 c-trpc core 职责，也不作为 core roadmap 的实现要求。
- `04-backup-pipeline.md` 中 backup_id/checkpoint/commit 等内容同样属于业务层映射。

后续建议在 API 边界稳定后把这些业务参考迁移到独立业务文档目录，避免 legacy 文件名继续误导 core 设计。

架构决策记录见 [decisions/](decisions/)。

## 文档状态约定

- **CURRENT**：当前 `main` 已实现并可依赖。
- **TARGET V1**：已经确定的 core 目标设计。
- **FUTURE**：只保留扩展边界，不作为当前实现要求。
- **BUSINESS REFERENCE**：用于说明业务如何使用 c-trpc，不属于 c-trpc core contract。

## 核心原则

> Mutable protocol state 只允许一个 Reactor owner；跨执行域通过 command、task、completion 和资源所有权转移协作，而不是让多个线程共同加锁修改同一个协议对象。

> 每一层只通过 capability/API 向上一层提供能力；上层不依赖下层内部对象、slot/generation、queue/pool 或 wire parser 实现。

> 分层是 source/ownership/dependency boundary，不要求每层增加 thread hop、queue、copy 或 serialization。

详细规则见 [07-layer-module-api-boundaries.md](07-layer-module-api-boundaries.md) 与
[ADR-007](decisions/ADR-007-layering-performance.md)。
