# RESETTING 期间合法在途 WINDOW_UPDATE 回归

日期：2026-10-26。关联 #242/ERR-C。版本：`2.0.0-dev.136 -> 2.0.0-dev.137`。

## 覆盖范围

新增真实 Reactor/Channel 回归：本地对一条已准入 Stream 发起 RESET 后、对端 RESET 尚未确认时，对端发送合法递增的绝对 `WINDOW_UPDATE`，将发送上限从 256 提高到 320。RESETTING Channel 应校验并记录该在途控制帧，同时保持终止语义，不恢复应用发送。

测试确认 `window_updates_rx` 恰好增加一次、流发送上限变为 320、没有 WRITABLE 事件，且该流上的 `tr_stream_send()` 返回 `TR_ERR_CLOSED`。对端 RESET 确认后，目标流退役；同连接健康 Stream 仍能交付 DATA 并完成 RESET，Channel 不出现连接级错误。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，真实 Channel 墓碑容量 40,000。
- GCC ThreadSanitizer（`halt_on_error=1:second_deadlock_stack=1`）：完整 `test_stream_reset` 通过，墓碑容量 5,000，无数据竞争报告。
- GCC ASan/UBSan（启用泄漏检测并在错误时停止）：完整 `test_stream_reset` 通过，墓碑容量 40,000，无 sanitizer 报告。
- dev.136 前序 head `aef204826fa554cdfe50c91ea273fab756bed23b` 的 GitHub CI 为 13/13 全通过；dev.137 head 的远端 CI 在推送后核验。
- clang-format 18.1.8 改动行检查及 `git diff --check` 通过。构建和测试临时目录位于仓库内 `build/test-tmp/`，未使用 `/tmp`。

## 影响与限制

只修改 `tests/test_stream_reset.c`、本报告、README、剩余工作记录和 Xmake 版本号；不改生产运行时、公开 API/ABI 或协议线格式。运行时性能无影响。

该回归只覆盖 RESETTING 期间单条合法递增 `WINDOW_UPDATE`、发送不可恢复及同连接健康同伴，不代表所有控制帧顺序、Split Channel 连接组交错、ERR-C 完整信用矩阵、#242 或 V2 整体验收完成。

## 回滚方案

撤销 dev.137 提交，删除此回归及报告，将 README 与 Xmake 版本恢复至 dev.136，并在 GCC Release 下重跑完整 `test_stream_reset`。

## 验收标准

1. RESETTING 阶段接受单调递增且形状正确的 `WINDOW_UPDATE`，计数增加一次并记录新绝对发送上限。
2. 该更新不触发 WRITABLE，RESETTING 流的 `tr_stream_send()` 返回 `TR_ERR_CLOSED`。
3. 对端 RESET 确认后目标流正常退役；同连接健康流仍能交付 DATA 并完成 RESET。
4. GCC Release、TSan、ASan/UBSan 下完整 `test_stream_reset` 均通过。
