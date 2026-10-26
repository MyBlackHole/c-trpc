# Split Channel RESET 响应背压期间 CLOSE 隔离回归

日期：2026-10-26。关联 #242/ERR-B。版本：`2.0.0-dev.142 -> 2.0.0-dev.143`。

## 覆盖范围

扩展 dev.142 的真实 Split Channel/Reactor 发送背压夹具。CONTROL、BULK 两种目标 lane 分别测试 DATA、`WINDOW_UPDATE` 和 `STREAM_CLOSE` 三种后续帧，共六个组合。对端先发送 RESET；测试确认本地 RESET 响应帧在目标 Reactor fd 上实际遇到 `EAGAIN`、尚未写出，再按 FIFO 顺序发送同流后续帧。

RESET 响应入队后目标 Stream 已退役，但目标 lane 仍保持 UP。背压期间到达的 DATA、`WINDOW_UPDATE` 或 `STREAM_CLOSE` 均以 `TR_ERR_STATE` 关闭目标 lane；DATA 不交付应用，`WINDOW_UPDATE` 不增加接收更新统计。六个组合都验证另一 lane 保持 UP，健康 Stream 仍能交付 DATA 并完成 RESET。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，真实 Channel 墓碑容量 40,000。
- GCC ThreadSanitizer（`halt_on_error=1:second_deadlock_stack=1`）：完整 `test_stream_reset` 通过，墓碑容量 5,000，无数据竞争报告。
- GCC ASan/UBSan（启用泄漏检测并在错误时停止）：完整 `test_stream_reset` 通过，墓碑容量 40,000，无 sanitizer 报告。
- clang-format 23.1.4 改动行检查及 `git diff --check` 通过。Xmake 构建目录和测试运行环境位于仓库内 `build/test-tmp/`。
- 前置 dev.142 head `d36967871a6ac0bce881712779b85ea88b206ee4` 的 GitHub Actions 已 13/13 全通过；dev.143 head 的 CI 推送后核验。

## 影响与限制

只修改 `tests/test_stream_reset.c`、本报告、README、剩余工作记录和 Xmake 版本号；不改生产运行时、公开 API/ABI 或协议线格式。运行时性能无影响。

该回归只覆盖 RESET 响应已入队、实际写出受 `EAGAIN` 阻塞时到达的同流 DATA、`WINDOW_UPDATE`、`STREAM_CLOSE`，以及 CONTROL/BULK lane 故障隔离；不覆盖响应队列容量耗尽、其他帧抢占、重复 RESET 或完整错误矩阵，也不代表 #242 或 V2 整体验收完成。

## 回滚方案

撤销 dev.143 提交，移除新增 CLOSE 变体和报告，将 README、剩余工作记录与 Xmake 版本恢复至 dev.142，并在 GCC Release 下重跑完整 `test_stream_reset`。

## 验收标准

1. 六个 lane/frame 组合均确认目标 RESET 响应实际遇到 `EAGAIN`，且后续帧到达前未写出。
2. DATA、`WINDOW_UPDATE` 或 `STREAM_CLOSE` 越过 RESET 屏障时，以 `TR_ERR_STATE` 关闭对应物理 lane；DATA 不交付，WU 接收统计不增长。
3. CONTROL、BULK 两种目标 lane 下，另一 lane 的健康 Stream 均能继续交付 DATA 并完成 RESET。
4. GCC Release、TSan、ASan/UBSan 下完整 `test_stream_reset` 均通过。
