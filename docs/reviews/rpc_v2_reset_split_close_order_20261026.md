# Split Channel RESET/CLOSE 顺序边界回归

日期：2026-10-26。关联 #242/ERR-B、ERR-C。版本：`2.0.0-dev.137 -> 2.0.0-dev.138`。

## 覆盖范围

新增真实 Split Channel/Reactor 回归，并分别让出错 Stream 位于 CONTROL、BULK lane。目标 Stream 本地发出 RESET 后、尚未收到对端 RESET 确认时，注入 FIFO 中更早的普通 `STREAM_CLOSE`。该 CLOSE 只能结束一侧，不能提前退役 RESETTING Stream 或关闭物理连接。

随后完成双向 RESET，再向同一 lane 注入晚到 `STREAM_CLOSE`。越过 RESET 静默边界的 CLOSE 必须以 `TR_ERR_STATE` 关闭承载目标 Stream 的物理连接。另一 lane 必须保持 UP，继续交付健康 Stream 的 DATA 并完成其 RESET。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，真实 Channel 墓碑容量 40,000。
- GCC ThreadSanitizer（`halt_on_error=1:second_deadlock_stack=1`）：完整 `test_stream_reset` 通过，墓碑容量 5,000，无数据竞争报告。
- GCC ASan/UBSan（启用泄漏检测并在错误时停止）：完整 `test_stream_reset` 通过，墓碑容量 40,000，无 sanitizer 报告。
- clang-format 18.1.8 改动行检查及 `git diff --check` 通过。构建和测试临时目录位于仓库内 `build/test-tmp/`，未使用 `/tmp`。
- dev.137 head `e7b39dea81a9a495bc7e7f723110c417ed9a509d` 的 GitHub CI 结果待登记；dev.138 head 的远端 CI 在推送后核验。

## 影响与限制

只修改 `tests/test_stream_reset.c`、本报告、README、剩余工作记录和 Xmake 版本号；不改生产运行时、公开 API/ABI 或协议线格式。运行时性能无影响。

该回归只覆盖两个 split lane 的普通 CLOSE 与 RESET 前后顺序，不代表所有 DATA/STATUS/半关闭组合、连接组排空 epoch、ERR-A/B/C/D/E 完整矩阵、#242 或 V2 整体验收完成。

## 回滚方案

撤销 dev.138 提交，删除此回归及报告，将 README 与 Xmake 版本恢复至 dev.137，并在 GCC Release 下重跑完整 `test_stream_reset`。

## 验收标准

1. RESET 确认前到达的 FIFO `STREAM_CLOSE` 不退役 RESETTING Stream、不关闭 lane。
2. 双向 RESET 完成后到达的 `STREAM_CLOSE` 以 `TR_ERR_STATE` 关闭对应物理连接。
3. CONTROL、BULK 两种出错方向都保留另一 lane 的 DATA 交付和 RESET 进展。
4. GCC Release、TSan、ASan/UBSan 下完整 `test_stream_reset` 均通过。
