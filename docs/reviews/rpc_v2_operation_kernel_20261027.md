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

dev.159 的首轮 GitHub CI 暴露两项独立问题：GCC 11 报 `-Werror=maybe-uninitialized`，Clang 14 报 dispatch 构造失败清理分支可能泄漏 `routes`。dev.160 初始化 consume 的局部取消状态，保持失败输出哨兵不变；并在池清理前释放临时路由表，池销毁失败时把部分 dispatch 对象交还调用方。endpoint 保留该对象和自身帧 arena，使 owner close / stopped reap 可继续清理。新增 dispatch 池销毁错误注入回归与 endpoint 部分构造交接回归。

dev.160 的 GCC 11 静态分析发现 TLS provider reserve 在表锁解锁失败时，已写出的输出句柄会随错误返回，结果缓冲及异步操作仍留在槽位。dev.161 将句柄发布移到成功解锁之后；解锁失败时撤销 reserve、清理结果缓冲与预算，并重试解锁。新增 pthread 解锁失败注入回归，验证输出哨兵、槽位预算和锁状态恢复。之后 GCC 11 TSan 构建暴露同一路径的 `slot may be used uninitialized`，dev.162 显式初始化该局部指针并检查非空；GCC 11 analyzer 对成功路径的 `result` 告警是 table-owned 槽位缓冲的所有权转移误报，因此仅对成功句柄发布行局部抑制 `-Wanalyzer-malloc-leak`。GCC 16 的 `-O1 -fsanitize=thread -Werror` 与 `-fanalyzer` 单元编译均无诊断。dev.162 已推送；PR #332 精确 head `d245e0b4bf0fec52e8d4e1db69613a597e35cb89` 的 13/13 GitHub 检查全部通过。

## 未结项边界

dev.163 的真实 TCP V2 Client/Server 回归覆盖了 CallRef 与 Server 异步 authorize 操作的生命周期交错：服务端 worker 确认看到取消后再迟到返回 ALLOW，Client 只发布一次 CANCELLED/LIVE 终局且服务端业务 work 未运行；服务端终局后操作已退役并可销毁，Client CallRef 仍能读取原快照、阻止 drain，release 后 drain 收敛。dev.164 又验证终局 CallRef 阻止 `tr_client_destroy()` 释放 Client；首次 destroy 返回 `TR_AGAIN` 时引用仍能读取快照，release 后重试销毁成功。两条回归的 `test_rpc_v2_server/default` 在 GCC Release、TSan、ASan/UBSan 均通过；GCC Release 全量测试为 93/93。它们只证明这两条 CallRef 生命周期场景，不表示所有 CallRef、异步操作或资源屏障均已通过。

此实现完成 #243 中“通用异步操作内核及显式操作引用”的一段，并接入三个异步调用方；dev.163 另补一个 CallRef 交互切片。它不完成整个 #243 或 V2。仍需验证跨 Reactor/嵌套调用、command/completion 队列压力、更多用户回调与资源回收屏障，以及 TLS/压缩共享调度和跨能力组合。#243 与总审查 #239 均保持 OPEN。
