# Split Channel RESET 信用超限与物理连接隔离回归

日期：2026-10-26。关联 #242/ERR-C。版本：`2.0.0-dev.134 -> 2.0.0-dev.135`。

## 覆盖范围

在 Split Channel 中打开 CONTROL 上的目标流与 BULK 上的健康同伴。目标流本地进入 RESETTING 且暂不确认对端 RESET，随后在 CONTROL 物理连接上收到合法 discard DATA，累计精确达到 256 字节冻结通告上限。测试检查接收量与消费量均为 256、通告上限保持 256、应用没有收到目标流正文，且 `window_updates_tx` 没有增加。

随后在 CONTROL 连接再发送 1 字节 DATA，连接必须以 `TR_ERR_BAD_LENGTH` 关闭。BULK 连接上的健康流仍须成功交付 DATA、响应 PING/PONG 屏障并完成双向 RESET，验证该物理连接错误不会扩大到独立 lane。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，真实 Channel 墓碑容量 40,000。
- GCC ThreadSanitizer（`halt_on_error=1:second_deadlock_stack=1`）：完整 `test_stream_reset` 通过，墓碑容量 5,000，无数据竞争报告。
- GCC ASan/UBSan（启用泄漏检测并在错误时停止）：完整 `test_stream_reset` 通过，墓碑容量 40,000，无 sanitizer 报告。
- clang-format 18.1.8 改动行检查及 `git diff --check` 通过。构建和测试临时目录位于仓库内 `build/test-tmp/`，未使用 `/tmp`。

## 影响与限制

只修改 `tests/test_stream_reset.c`、测试记录、README、剩余工作记录和 Xmake 版本号；不改生产运行时、公开 API/ABI 或协议线格式。运行时性能无影响。

该测试验证 CONTROL 连接上的 RESET 冻结信用超限错误隔离到对应物理连接，并验证 BULK 同伴继续工作；不覆盖超限 DATA 的其他格式组合或完整 ERR-C/ERR-A/B/D/E 矩阵，也不代表 #242 或 V2 整体验收完成。

## 回滚方案

撤销 dev.135 提交，移除 split 连接超限隔离变体与本报告，将 README 和 Xmake 版本恢复至 dev.134，并在 Release 下重跑完整 `test_stream_reset`。

## 验收标准

1. CONTROL 上 RESETTING 流精确接收 256 字节后，账本和通告上限均为 256，且没有应用交付或 WINDOW_UPDATE。
2. 再接收 1 字节时 CONTROL 连接以 `TR_ERR_BAD_LENGTH` 关闭。
3. 独立 BULK 连接上的健康流仍能交付 DATA 并完成 RESET，且无 BULK down 事件。
4. Release、TSan、ASan/UBSan 下完整 `test_stream_reset` 均通过。
