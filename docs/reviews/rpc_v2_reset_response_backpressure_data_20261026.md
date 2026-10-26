# Split Channel RESET 响应背压期间 DATA 隔离回归

日期：2026-10-26。关联 #242/ERR-B。版本：`2.0.0-dev.140 -> 2.0.0-dev.141`。

## 覆盖范围

新增真实 Split Channel/Reactor 回归，分别让目标 Stream 位于 CONTROL、BULK lane，并在另一 lane 保留健康 Stream。对端先发送 RESET；测试发送拦截器只阻塞目标 Reactor fd，并确认本地 RESET 响应帧的 `sendmsg` 已实际返回 `EAGAIN`，确保响应仍未写出。随后对端按 FIFO 顺序在同一连接发送 DATA。

RESET 响应入队后目标 Stream 已收到 `CLOSED` 事件并退役，但目标物理连接仍保持 UP。响应仍受背压时到达的 DATA 以 `TR_ERR_STATE` 关闭对应物理 lane，不交付应用正文。CONTROL、BULK 两种方向均验证另一 lane 保持 UP，健康 Stream 仍能交付 DATA 并完成 RESET。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，真实 Channel 墓碑容量 40,000。
- GCC ThreadSanitizer（`halt_on_error=1:second_deadlock_stack=1`）：完整 `test_stream_reset` 通过，墓碑容量 5,000，无数据竞争报告。
- GCC ASan/UBSan（启用泄漏检测并在错误时停止）：完整 `test_stream_reset` 通过，墓碑容量 40,000，无 sanitizer 报告。
- clang-format 23.1.4 改动行检查及 `git diff --check` 通过。构建与测试输出位于仓库内 `build/test-tmp/`。
- 前置 dev.140 head `3b07371bf118b0bc35bc482165684c796a51a455` 的 GitHub CI 已 13/13 全通过；dev.141 head 的 CI 推送后核验。

## 影响与限制

只修改 `tests/test_stream_reset.c`、本报告、README、剩余工作记录和 Xmake 版本号；不改生产运行时、公开 API/ABI 或协议线格式。运行时性能无影响。

该回归只覆盖对端 RESET 后、本地 RESET 响应仍受发送背压时到达的同流 DATA，以及 CONTROL/BULK lane 故障隔离；不覆盖响应队列容量耗尽、不同类型帧抢占、其他 DATA/CLOSE/RESET 排列或完整错误矩阵，也不代表 #242 或 V2 整体验收完成。

## 回滚方案

撤销 dev.141 提交，删除此回归及报告，将 README、剩余工作记录与 Xmake 版本恢复至 dev.140，并在 GCC Release 下重跑完整 `test_stream_reset`。

## 验收标准

1. 测试拦截器确认目标 lane 的 RESET 响应帧实际遇到 `EAGAIN`，且在 DATA 到达前未写出。
2. FIFO DATA 以 `TR_ERR_STATE` 关闭承载目标 Stream 的物理 lane，不交付应用正文。
3. CONTROL、BULK 两种目标 lane 下，另一 lane 的健康 Stream 均能继续交付 DATA 并完成 RESET。
4. GCC Release、TSan、ASan/UBSan 下完整 `test_stream_reset` 均通过。
