# 双 RESETTING 流冻结信用超限拒绝回归

日期：2026-10-26。关联 #242/ERR-C。版本：`2.0.0-dev.133 -> 2.0.0-dev.134`。

## 覆盖范围

扩展双 RESETTING 流并发载荷释放场景。每条流先持有 128 字节已交付消息，再各自在 RESETTING 期间接收 128 字节合法在途 DATA，恰好填满其 256 字节冻结通告窗口。应用线程随后并发释放旧载荷；两条流的接收量与消费量均到 256，通告上限保持 256，`window_updates_tx` 不增长。

新增超限变体在上述精确边界后，向其中一条 RESETTING 流发送 1 字节 DATA。连接必须以 `TR_ERR_BAD_LENGTH` 关闭，应用交付次数保持不变，且不发送 WINDOW_UPDATE。0/64/128 字节在途 DATA 的既有变体继续核对合法范围，其中非超限变体仍验证 RESET 收敛后健康同伴正常完成。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，真实 Channel 墓碑容量 40,000。
- GCC ThreadSanitizer（`halt_on_error=1:second_deadlock_stack=1`）：完整 `test_stream_reset` 通过，墓碑容量 5,000，无数据竞争报告。
- GCC ASan/UBSan（启用泄漏检测并在错误时停止）：完整 `test_stream_reset` 通过，墓碑容量 40,000，无 sanitizer 报告。
- `git diff --check` 与 clang-format 18.1.8 改动行检查通过。构建、测试临时目录位于仓库内 `build/test-tmp/`，未使用 `/tmp`。

## 影响与限制

只修改 `tests/test_stream_reset.c`、测试记录、README、剩余工作记录和 Xmake 版本号；不改生产运行时、公开 API/ABI 或协议线格式。运行时性能无影响。

该变体验证双 RESETTING 流精确耗尽窗口后的一字节超限处理，不替代既有单流超限覆盖，也不代表 ERR-C 全部信用组合、ERR-A/B/D/E、#242 或 V2 整体验收完成。

## 回滚方案

撤销 dev.134 提交，移除双 RESETTING 流超限变体与本报告，将 README 和 Xmake 版本恢复至 dev.133，并在 Release 下重跑完整 `test_stream_reset`。

## 验收标准

1. 双流在途 DATA 各为 0/64/128 字节时，合法边界行为与信用账本符合通告上限。
2. 128 字节档并发释放后，每条流的接收量和消费量均为 256，通告上限保持 256。
3. 再接收 1 字节时连接以 `TR_ERR_BAD_LENGTH` 关闭，不增加应用交付次数或 WINDOW_UPDATE。
4. Release、TSan、ASan/UBSan 下完整 `test_stream_reset` 均通过。
