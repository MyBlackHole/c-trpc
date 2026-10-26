# RESETTING 并发载荷释放信用隔离回归

日期：2026-10-26。关联 #242/ERR-C。版本：`2.0.0-dev.129 -> 2.0.0-dev.130`。

## 覆盖范围

扩展真实 Reactor/Channel 的 `test_stream_reset`：同一条远端流接收并由应用持有两条各 64 字节的消息。远端 RESET 使流进入 RESETTING 后，测试阻塞 owner 的 RESET 事件回调，在槽位退役前由两个应用线程并发释放各自持有的正文。

窗口为 256 字节，更新阈值为 128 字节。释放前，该流接收 128 字节、消费 0 字节、通告上限为 256；释放完成后消费量可增加至 128，但通告上限必须仍为 256，Channel 的 `window_updates_tx` 计数也必须保持不变。恢复 RESET 回调并完成退役后，通过 PING/PONG 屏障确认目标流没有迟到 WINDOW_UPDATE。同一 Channel 上的健康同伴仍须完成 DATA 交付和 RESET。

## 验证

- Xmake `3.1.2+20261016`，GCC Release：完整 `test_stream_reset` 通过，真实 Channel 墓碑容量 40,000。该次运行附带的 40k OPEN/RESET 周转观测为 P50/P95/P99 `1170.01/2851.16/5481.29 µs`，循环墙钟 `58.36 s`、CPU `56.80 s`；这些值有机器负载波动，不是性能验收阈值或独立微基准。
- GCC ThreadSanitizer：完整 `test_stream_reset` 通过，墓碑容量 5,000，无数据竞争报告；附带周转观测 P50/P95/P99 `3121.32/4555.81/5225.91 µs`。
- GCC ASan/UBSan（启用泄漏检测并在错误时停止）：完整 `test_stream_reset` 通过，墓碑容量 40,000，无 sanitizer 报告；附带周转观测 P50/P95/P99 `993.53/1254.17/1539.05 µs`。
- 敏感性验证：临时移除 `tr_send_window_update_locked()` 中对 `rx_credit_stopped` 的保护，新用例在 `flow.rx_advertised_limit == WINDOW` 断言失败。临时变异已撤销，生产源码与提交基线一致；恢复后的 Release、TSan 和 ASan/UBSan 运行均通过。
- 所有构建和运行临时目录设为仓库内 `build/test-tmp/`，未使用 `/tmp`。

## 影响与限制

只修改 `tests/test_stream_reset.c`、测试记录、README 和版本号；不改生产运行时、公开 API/ABI 或协议线格式。运行时性能无影响。测试额外创建两个应用线程，执行两次持有载荷释放和一次 RESET。

该回归验证一个 RESETTING 流上两个独立载荷的并发释放，以及同 Channel 健康同伴继续进展；没有验证两条流同时处于 RESETTING、多 Channel/split 连接、压缩工作区释放或长期竞争。ERR-C 其余信用组合、ERR-A/B/D/E 和 #242/V2 整体验收仍未完成。

## 回滚方案

撤销 dev.130 提交，移除并发释放回归和本报告，将 README 与 Xmake 版本恢复至 dev.129；随后在 Release 下重跑完整 `test_stream_reset`。

## 验收标准

1. 两个应用线程可在 RESETTING 窗口并发释放各自持有的消息，均返回 `TR_OK`。
2. 两次释放后目标流接收量为 128、消费量为 128、通告上限仍为 256，`window_updates_tx` 不增长。
3. RESET 完成后 wire 上无目标流迟到 WINDOW_UPDATE，健康同伴仍可交付 DATA 并正常完成 RESET。
4. Release、TSan、ASan/UBSan 全目标通过；信用停止保护的临时变异会被新断言捕获。
