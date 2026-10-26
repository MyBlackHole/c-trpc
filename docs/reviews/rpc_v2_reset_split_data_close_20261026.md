# RESETTING DATA 后 FIFO CLOSE 回归

日期：2026-10-26。关联 #242/ERR-B、ERR-C。版本：`2.0.0-dev.139 -> 2.0.0-dev.140`。

## 覆盖范围

新增真实 Split Channel/Reactor 回归，分别让 RESETTING 目标 Stream 位于 CONTROL、BULK lane，并在另一 lane 保留健康 Stream。目标流本地发出 RESET 且尚未收到对端确认时，对端先发送合法的完整 DATA，再按 FIFO 顺序发送普通 `STREAM_CLOSE`。

DATA 必须由 RESETTING discard 路径消费：8 字节计入接收和消费账本，不交付应用，通告上限不变，也不产生 `WINDOW_UPDATE`。后续 CLOSE 只结束半侧，不能提前退役 RESETTING Stream 或关闭 lane。对端 RESET 到达后目标流正常收敛；另一 lane 的健康 Stream 仍可交付 DATA 并完成 RESET。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，真实 Channel 墓碑容量 40,000。
- GCC ThreadSanitizer（`halt_on_error=1:second_deadlock_stack=1`）：完整 `test_stream_reset` 通过，墓碑容量 5,000，无数据竞争报告。
- GCC ASan/UBSan（启用泄漏检测并在错误时停止）：完整 `test_stream_reset` 通过，墓碑容量 40,000，无 sanitizer 报告。
- clang-format 23.1.3 改动行检查及 `git diff --check` 通过。构建和测试临时目录位于仓库内 `build/test-tmp/`，未使用 `/tmp`。
- 前置 dev.139 head `5481200654d41ed5b96e817731b72494d1f2efc9` 的 GitHub CI 为 13/13 全通过；dev.140 head CI 推送后核验。

## 影响与限制

只修改 `tests/test_stream_reset.c`、本报告、README、剩余工作记录和 Xmake 版本号；不改生产运行时、公开 API/ABI 或协议线格式。运行时性能无影响。

该回归只覆盖 RESETTING 阶段合法 DATA 与同方向 FIFO CLOSE 的顺序、信用记账和 split lane 隔离，不代表所有 DATA/STATUS/半关闭组合、连接组排空 epoch、ERR-A/B/C/D/E 完整矩阵、#242 或 V2 整体验收完成。

## 回滚方案

撤销 dev.140 提交，删除此回归及报告，将 README 与 Xmake 版本恢复至 dev.139，并在 GCC Release 下重跑完整 `test_stream_reset`。

## 验收标准

1. RESETTING 期间先到的合法 DATA 被消费但不交付应用、不产生额外 `WINDOW_UPDATE`。
2. FIFO CLOSE 不提前退役 RESETTING Stream 或关闭所在 lane。
3. CONTROL、BULK 两种目标 lane 都能在对端 RESET 后正常收敛，另一 lane 健康流继续进展。
4. GCC Release、TSan、ASan/UBSan 下完整 `test_stream_reset` 均通过。
