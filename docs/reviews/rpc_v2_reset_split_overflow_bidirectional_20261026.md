# Split Channel RESET 信用超限双向隔离回归

日期：2026-10-26。关联 #242/ERR-C。版本：`2.0.0-dev.135 -> 2.0.0-dev.136`。

## 覆盖范围

将 Split Channel RESETTING 信用超限测试参数化，分别以 CONTROL 和 BULK 作为出错物理连接。目标流先进入 RESETTING 并暂缓对端确认，接收合法 discard DATA 恰好填满 256 字节冻结窗口；测试检查接收/消费量均为 256、通告上限保持 256、应用交付数为零且 `window_updates_tx` 不增长。

随后仅在目标 lane 发送 1 字节 DATA。每个方向都必须以 `TR_ERR_BAD_LENGTH` 关闭出错连接，另一 lane 的健康流继续收到 DATA、通过 PING/PONG 屏障并完成双向 RESET。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，真实 Channel 墓碑容量 40,000。
- GCC ThreadSanitizer（`halt_on_error=1:second_deadlock_stack=1`）：完整 `test_stream_reset` 通过，墓碑容量 5,000，无数据竞争报告。
- GCC ASan/UBSan（启用泄漏检测并在错误时停止）：完整 `test_stream_reset` 通过，墓碑容量 40,000，无 sanitizer 报告。
- clang-format 18.1.8 改动行检查及 `git diff --check` 通过。构建、测试临时目录位于仓库内 `build/test-tmp/`，未使用 `/tmp`。

## 影响与限制

只修改 `tests/test_stream_reset.c`、测试计划、审查记录、README、剩余工作记录和 Xmake 版本号；不改生产运行时、公开 API/ABI 或协议线格式。运行时性能无影响。

该测试覆盖 CONTROL/BULK 两种物理连接发生 RESET 冻结信用超限时的 lane 隔离，不替代其他 DATA 格式、RESET/FIFO 交错或更长压力组合，也不代表 ERR-C 全矩阵、#242 或 V2 整体验收完成。

## 回滚方案

撤销 dev.136 提交，移除双向 Split Channel 变体与本报告，将 README 和 Xmake 版本恢复为 dev.135，并在 Release 下重跑完整 `test_stream_reset`。

## 验收标准

1. CONTROL、BULK 两种目标 lane 都接收 256 字节后保持冻结上限，无应用交付或 WINDOW_UPDATE。
2. 各自在超限 1 字节后只关闭目标物理连接，状态为 `TR_ERR_BAD_LENGTH`。
3. 另一 lane 的健康流仍能交付 DATA、通过 PING/PONG 并完成 RESET。
4. Release、TSan、ASan/UBSan 下完整 `test_stream_reset` 均通过。
