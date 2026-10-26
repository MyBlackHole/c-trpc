# RPC V2 异步操作内核记录

日期：2026-10-27。对应设计：[RPC V2 异步操作内核设计](../superpowers/specs/2026-10-27-rpc-v2-operation-kernel-design.md)；实施计划：[固定容量异步操作池核心](../superpowers/plans/2026-10-27-async-operation-pool-core.md)。

## 已实现范围

`src/execution/async_operation.c` 提供固定容量的内部异步操作池。操作身份由 pool instance、slot generation 与可轮换 token 组成；owner 执行 reserve/accept/consume/retire，producer 可并发 publish/cancel。池区分 RESERVED、ACCEPTED、PUBLISHED、CONSUMED 与退役状态，支持先 accept 后 publish 和先 publish 后 accept、单次结果交接、关闭、显式静默证明后的停机回收，以及容量和代次耗尽统计。

三个真实调用方已接入：

- `src/rpc/rpc_record_dispatch.c`：Server V2 异步 authorize 使用操作引用；owner 完成路径复核调用代次、终局与 deadline 后再提交认证结果。
- `src/io/tls_provider_operation.c`：TLS provider adapter 由通用池管理操作生命周期，凭证及结果缓冲仍由 TLS adapter 和其领域预算管理。
- `src/runtime/message_service.c`：异步 transform 关联操作引用；输入、压缩输出和 CPU 预算继续由 MessageService 管理。

池不接管工作输入，也不取代 TaskExecutor、CompletionQueue 或 Reactor owner。只有 `publish` 成功才转移结果 payload 与析构责任。该边界避免将调度队列、应用缓冲字节和操作槽容量混为一个预算。

## 本地验证

2026-10-27 的本地完整验证记录：

- GCC Release：完整构建成功，测试 93/93。
- Clang Release：测试 93/93。
- GCC ASan/UBSan：测试 93/93。
- GCC TSan：测试 93/93；最终格式化后，直接相关的 6 个 TSan 目标再次通过。
- clang-format 18.1.8 自测 22/22；改动行无格式差异，新 C 文件格式检查通过；`git diff --check` 通过；`include/tr` 无改动。
- 所有测试临时路径都位于 worktree 的 `build/test-tmp`。

dev.159 的首轮 GitHub CI 暴露两项独立问题：GCC 11 报 `-Werror=maybe-uninitialized`，Clang 14 报 dispatch 构造失败清理分支可能泄漏 `routes`。前者因编译器不能证明 consume 错误路径不会落入局部取消状态的成功输出分支；`test_async_operation` 已有 stale completion 引用回归，验证失败时输出哨兵保持不变。dev.160 将两个局部量分别初始化为 `0` 与 `TR_OK`，输出参数仍只在消费成功后写入。后者通过在池清理前释放临时路由表，并在池销毁失败时把部分 dispatch 对象交还调用方；endpoint 也保留该对象和自身帧 arena，使 owner close / stopped reap 能继续清理。新增 dispatch 池销毁错误注入回归与 endpoint 部分构造交接回归。当前分支上的本地 Release 测试已验证这两条回归；dev.160 完整远端 CI 仍待推送后复验。

## 未结项边界

此实现完成 #243 中“通用异步操作内核及显式操作引用”的一段，并接入三个异步调用方；它不完成整个 #243 或 V2。仍需验证统一可持有 Call 能力与此操作池的衔接、跨 Reactor/嵌套调用、command/completion 队列压力、用户回调与资源回收屏障，以及 TLS/压缩共享调度和跨能力组合。#243 与总审查 #239 均保持 OPEN。
