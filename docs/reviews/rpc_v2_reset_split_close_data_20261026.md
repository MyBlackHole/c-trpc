# Split RESET/CLOSE 后 DATA 拒绝回归

日期：2026-10-26。关联 #242/ERR-B、ERR-C。版本：`2.0.0-dev.138 -> 2.0.0-dev.139`。

## 覆盖范围

新增真实 Split Channel/Reactor 回归，并分别让目标 Stream 位于 CONTROL、BULK lane。目标 Stream 本地发出 RESET 后，在收到对端 RESET 确认前注入 FIFO 普通 `STREAM_CLOSE`；该帧只结束一个方向，不提前退役 RESETTING Stream。随后从同一已关闭方向注入 DATA，协议错误必须以 `TR_ERR_STATE` 关闭承载目标 Stream 的物理 lane，不能交付该 DATA。

两种 lane 方向都检查被关闭 lane 上 Stream 以错误终结、没有 `CLOSED` 事件，另一 lane 保持 UP；另一 lane 的健康 Stream 仍能交付 DATA 并完成 RESET。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，真实 Channel 墓碑容量 40,000。
- GCC ThreadSanitizer（`halt_on_error=1:second_deadlock_stack=1`）：完整 `test_stream_reset` 通过，墓碑容量 5,000，无数据竞争报告。
- GCC ASan/UBSan（启用泄漏检测并在错误时停止）：完整 `test_stream_reset` 通过，墓碑容量 40,000，无 sanitizer 报告。
- clang-format 23.1.3 改动行检查及 `git diff --check` 通过。构建和测试临时目录位于仓库内 `build/test-tmp/`，未使用 `/tmp`。
- 前置 dev.138 head `d80aa33b16e3e9ea6b99ab0bea1920a39b608892` 的 GitHub CI 为 13/13 全通过；dev.139 head CI 推送后核验。

## 影响与限制

只修改 `tests/test_stream_reset.c`、本报告、README、剩余工作记录和 Xmake 版本号；不改生产运行时、公开 API/ABI 或协议线格式。运行时性能无影响。

该回归只覆盖 RESETTING 阶段普通 CLOSE 后同方向 DATA 的拒绝及 split lane 隔离，不代表所有 DATA/STATUS/半关闭组合、连接组排空 epoch、ERR-A/B/C/D/E 完整矩阵、#242 或 V2 整体验收完成。

## 回滚方案

撤销 dev.139 提交，删除此回归及报告，将 README 与 Xmake 版本恢复至 dev.138，并在 GCC Release 下重跑完整 `test_stream_reset`。

## 验收标准

1. RESET 确认前 FIFO `STREAM_CLOSE` 不退役 RESETTING Stream。
2. 已关闭方向随后到达的 DATA 以 `TR_ERR_STATE` 关闭对应物理 lane，且不交付目标 DATA。
3. CONTROL、BULK 两种出错方向都保留另一 lane 的 DATA 交付和 RESET 进展。
4. GCC Release、TSan、ASan/UBSan 下完整 `test_stream_reset` 均通过。
