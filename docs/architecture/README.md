# c-trpc 架构文档

本目录使用“多视图 + 不变量 + ADR”表达 c-trpc 架构，避免把线程模型、资源所有权、RPC 执行、备份语义和演进计划混在一张大图里。

## 阅读顺序

1. [00-overview.md](00-overview.md)：一页理解系统边界和核心数据流。
2. [01-runtime.md](01-runtime.md)：Server/Client Runtime、Reactor shard、listener 和线程模型。
3. [02-ownership.md](02-ownership.md)：谁拥有状态、谁可以修改状态、哪些地方允许锁。
4. [03-rpc-execution.md](03-rpc-execution.md)：RPC Task → Worker → Completion → Reactor owner。
5. [04-backup-pipeline.md](04-backup-pipeline.md)：Backup Pipeline、CONTROL + DATA[N]、soft state。
6. [05-durability-recovery.md](05-durability-recovery.md)：STORED/CHECKPOINTED/COMMITTED、epoch、恢复。
7. [06-evolution.md](06-evolution.md)：从当前实现迁移到目标 V1 的阶段。

架构决策记录见 [decisions/](decisions/)。

## 文档状态约定

- **CURRENT**：当前 `main` 已实现并可依赖。
- **TARGET V1**：已经确定的目标设计，后续按阶段实现。
- **FUTURE**：只保留扩展边界，不作为当前实现要求。

## 核心原则

> Mutable protocol state 只允许一个 Reactor owner；跨执行域通过 command、task、completion 和资源所有权转移协作，而不是让多个线程共同加锁修改同一个协议对象。

这个原则比“无锁”更重要：真正目标是减少共享可变状态，而不是机械删除所有 mutex。
