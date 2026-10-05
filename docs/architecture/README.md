# c-trpc 架构文档

本目录使用“分层边界 + 所有权 + 多视图 + 架构决策记录”表达 c-trpc 架构。

c-trpc 核心的范围是通用运行时、传输、连接组和 RPC。
备份、数据库同步、对象复制等属于上层业务；它们可以使用 c-trpc，但不能反向定义核心 API。

## 核心文档阅读顺序

1. [00-overview.md](00-overview.md)：系统边界、层次和当前核心数据流。
2. [07-layer-module-api-boundaries.md](07-layer-module-api-boundaries.md)：层职责、模块职责、公开/内部 API、当前边界审查。
3. [01-runtime.md](01-runtime.md)：服务端/客户端运行时、Reactor 分片、监听器和线程模型。
4. [02-ownership.md](02-ownership.md)：谁拥有状态、谁可以修改状态、哪些地方允许锁。
5. [03-rpc-execution.md](03-rpc-execution.md)：RPC 任务 → 工作线程 → 完成事件 → 原 Reactor 所有者。
6. [08-rpc-streaming-conformance.md](08-rpc-streaming-conformance.md)：四种 RPC 基数关系、半关闭、最终 `STATUS` 与终止回调契约。
7. [09-rpc-context-metadata.md](09-rpc-context-metadata.md)：调用上下文、初始元数据与尾部元数据。
8. [10-rpc-interceptor-v1.md](10-rpc-interceptor-v1.md)：固定的调用级拦截器阶段、所有者执行、拒绝与重入契约。
9. [04-backup-pipeline.md](04-backup-pipeline.md)：当前保留的旧文件名；其中 Pipeline/Connection Group 基础能力属于核心，备份持久化语义只作为上层映射参考。
10. [06-evolution.md](06-evolution.md)：c-trpc 核心演进路线。

## 业务层参考（非核心）

- [05-durability-recovery.md](05-durability-recovery.md)：备份持久性/恢复示例。该文档不定义 c-trpc 核心职责，也不作为核心演进路线的实现要求。
- `04-backup-pipeline.md` 中的 `backup_id`、检查点、提交等内容同样属于业务层映射。

后续建议在 API 边界稳定后把这些业务参考迁移到独立业务文档目录，避免旧文件名继续误导核心设计。

架构决策记录见 [decisions/](decisions/)。

## 文档状态约定

- **当前实现**：当前 `main` 已实现并可依赖。
- **V1 目标**：已经确定的核心目标设计。
- **未来扩展**：只保留扩展边界，不作为当前实现要求。
- **业务参考**：用于说明业务如何使用 c-trpc，不属于 c-trpc 核心契约。

## 核心原则

> 可变协议状态只允许一个 Reactor 所有者修改；跨执行域通过命令、任务、完成事件和资源所有权转移协作，而不是让多个线程共同加锁修改同一个协议对象。

> 每一层只通过能力/API 向上一层提供能力；上层不依赖下层内部对象、槽位/代次、队列/资源池或线协议解析器实现。

> 分层是源码边界、所有权边界和依赖边界，不要求每层额外增加线程切换、队列、复制或序列化。

详细规则见 [07-layer-module-api-boundaries.md](07-layer-module-api-boundaries.md) 与
[ADR-007](decisions/ADR-007-layering-performance.md)。
