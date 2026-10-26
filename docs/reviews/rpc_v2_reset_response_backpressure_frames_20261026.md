# Split Channel RESET 响应背压期间 DATA/WINDOW_UPDATE 隔离回归

日期：2026-10-26。关联 #242/ERR-B、ERR-C。版本：`2.0.0-dev.141 -> 2.0.0-dev.142`。

## 覆盖范围

扩展 dev.141 的真实 Split Channel/Reactor 发送背压夹具。CONTROL、BULK 两种目标 lane 分别测试 DATA 和 `WINDOW_UPDATE` 两种后续帧，共四个组合。对端先发送 RESET；测试拦截器只阻塞目标 Reactor fd，并确认本地 RESET 响应帧的 `sendmsg` 已实际返回 `EAGAIN`、尚未写出。然后对端按 FIFO 顺序在同一物理连接发送同流后续帧。

RESET 响应入队后目标 Stream 已退役，但 lane 保持 UP。背压期间到达的 DATA 或 `WINDOW_UPDATE` 均以 `TR_ERR_STATE` 关闭目标 lane；DATA 不交付应用，`WINDOW_UPDATE` 不增加接收更新统计。每种后续帧、两个 lane 方向都确认另一 lane 保持 UP，健康 Stream 可交付 DATA 并完成 RESET。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，真实 Channel 墓碑容量 40,000。
- GCC ThreadSanitizer（`halt_on_error=1:second_deadlock_stack=1`）：完整 `test_stream_reset` 通过，墓碑容量 5,000，无数据竞争报告。
- GCC ASan/UBSan（启用泄漏检测并在错误时停止）：完整 `test_stream_reset` 通过，墓碑容量 40,000，无 sanitizer 报告。
- clang-format 23.1.4 改动行检查及 `git diff --check` 通过。Xmake 构建目录和测试运行环境位于仓库内 `build/test-tmp/`。
- 前置 dev.141 head `24ab8f3ddcace05da8563535dcdc200ce2f23f63` 的 GitHub Actions 已 13/13 全通过；dev.142 head 的 CI 推送后核验。

## 影响与限制

只修改 `tests/test_stream_reset.c`、本报告、README、剩余工作记录和 Xmake 版本号；不改生产运行时、公开 API/ABI 或协议线格式。运行时性能无影响。

该回归只覆盖 RESET 响应已入队、实际写出受 `EAGAIN` 阻塞时到达的同流 DATA/WINDOW_UPDATE，以及 CONTROL/BULK lane 故障隔离；不覆盖响应队列容量耗尽、其他帧抢占、额外 DATA/CLOSE/RESET 排列或完整错误矩阵，也不代表 #242 或 V2 整体验收完成。

## 回滚方案

撤销 dev.142 提交，移除新增 WU 变体和报告，将 README、剩余工作记录与 Xmake 版本恢复至 dev.141，并在 GCC Release 下重跑完整 `test_stream_reset`。

## 验收标准

1. 四个 lane/frame 组合均确认目标 RESET 响应实际遇到 `EAGAIN`，且 follow-up 帧到达前未写出。
2. DATA 或 `WINDOW_UPDATE` 越过 RESET 屏障时以 `TR_ERR_STATE` 关闭对应物理 lane；DATA 不交付、WU 接收统计不增长。
3. CONTROL、BULK 两种目标 lane 下，另一 lane 的健康 Stream 均能继续交付 DATA 并完成 RESET。
4. GCC Release、TSan、ASan/UBSan 下完整 `test_stream_reset` 均通过。
