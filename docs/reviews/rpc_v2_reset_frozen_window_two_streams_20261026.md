# 双 RESETTING 流冻结窗口边界回归

日期：2026-10-26。关联 #242/ERR-C。版本：`2.0.0-dev.132 -> 2.0.0-dev.133`。

## 覆盖范围

在双 RESETTING 流并发释放测试中保留三档在途 DATA 变体：每条流在 RESET 前已接收并持有 128 字节消息；本地 RESET 后、对端确认前，分别再接收 0、64 或 128 字节合法 DATA。128 字节档以消息 ID 2 的两个 64 字节 DATA 分片构成，正好填满每条流通告窗口的剩余额度。

在 128 字节档，RESET discard 只丢弃 DATA，不交付应用；接收总量到达 256，消费量因 discard 增加到 128，通告上限保持 256。两个应用线程随后并发释放各自持有的 128 字节正文，消费量到达 256，通告上限仍为 256。测试检查 `window_updates_tx` 未增长，RESET wire 往返及 PING/PONG 屏障没有目标流 WINDOW_UPDATE，健康同伴仍可交付和完成 RESET。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，真实 Channel 墓碑容量 40,000。附带周转观测 P50/P95/P99 为 `869.41/1014.12/1293.50 µs`，OPEN/RESET 循环墙钟 `37.32 s`；不作为性能验收阈值。
- GCC ThreadSanitizer（`halt_on_error=1:second_deadlock_stack=1`）：完整 `test_stream_reset` 通过，墓碑容量 5,000，无数据竞争报告；附带 P50/P95/P99 为 `3113.61/4515.71/5477.94 µs`。
- GCC ASan/UBSan（启用泄漏检测并在错误时停止）：完整 `test_stream_reset` 通过，墓碑容量 40,000，无 sanitizer 报告；附带 P50/P95/P99 为 `918.16/1115.19/1423.82 µs`。
- clang-format 18.1.8 改动行检查与 `git diff --check` 通过。所有构建与运行临时目录设于仓库 `build/test-tmp/`，未使用 `/tmp`。

## 影响与限制

只修改 `tests/test_stream_reset.c`、测试记录、README、剩余工作记录和 Xmake 版本号；不改生产运行时、公开 API/ABI 或 wire。运行时性能无影响。

该测试验证窗口剩余额度被合法在途 DATA 用尽后，RESET discard 与应用释放都不会恢复信用；不验证超额 DATA 的连接关闭及其同连接/跨物理成员隔离。ERR-C 其他信用组合以及 ERR-A/B/D/E、#242 与 V2 整体验收仍未完成。

## 回滚方案

撤销 dev.133 提交，移除 128 字节边界变体和本报告，将 README 与 Xmake 版本恢复至 dev.132；在 Release 下重跑完整 `test_stream_reset`。

## 验收标准

1. 0、64、128 字节三档在途 DATA 变体均通过，RESET discard 不增加应用交付次数。
2. 128 字节档中，每条流接收量到达 256、消费量由 128 到达 256，通告上限始终为 256。
3. `window_updates_tx` 不增长，wire 无迟到目标流 WINDOW_UPDATE，健康同伴正常完成。
4. Release、TSan、ASan/UBSan 下完整 `test_stream_reset` 均通过。
