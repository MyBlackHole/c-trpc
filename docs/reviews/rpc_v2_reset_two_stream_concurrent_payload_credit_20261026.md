# 双 RESETTING 流并发载荷信用隔离回归

日期：2026-10-26。关联 #242/ERR-C。版本：`2.0.0-dev.130 -> 2.0.0-dev.131`。

## 覆盖范围

扩展真实 Reactor/Channel 的 `test_stream_reset`：两个远端已打开的流各接收一条由两段 64 字节 DATA 重组而成的 128 字节消息，并由应用持有。测试分别对两条流启动本地 RESET，暂不发送对端确认，使两个流同时留在 RESETTING；随后由两个条件变量同步的应用线程并发释放各自的正文。

每条流的接收窗口为 256 字节，WINDOW_UPDATE 阈值为 128 字节。释放前，每条流接收 128 字节、消费 0 字节、通告上限为 256；释放后，每条流消费量为 128，通告上限仍为 256。测试同时检查 Channel `window_updates_tx` 没有增长、RESET wire 往返和 PING/PONG 屏障中均没有目标流 WINDOW_UPDATE。两条流退役后，同 Channel 健康同伴仍须交付 DATA 并正常完成 RESET。

实现测试时曾尝试在 A 的 RESET 回调内嵌套启动 B 的 RESET。TSan 稳定复现 A 返回 `TR_ERR_STALE`，因为 B 的 owner 复位推进会同步处理并退役已收到对端 RESET 的 A。测试已改成分别启动两个本地 RESET、在对端确认前并发释放，不再假定暂停 A 回调会阻止其他 RESET 推进。生产源码未修改。

## 验证

- Xmake `3.1.2+20261016`，GCC Release：完整 `test_stream_reset` 通过，真实 Channel 墓碑容量 40,000。附带周转观测 P50/P95/P99 为 `864.31/1040.82/1294.79 µs`，OPEN/RESET 循环墙钟 `37.33 s`；这些值随测试机负载波动，不是性能验收阈值或独立微基准。
- GCC ThreadSanitizer（`halt_on_error=1:second_deadlock_stack=1`）：完整 `test_stream_reset` 通过，墓碑容量 5,000，无数据竞争报告；附带 P50/P95/P99 为 `3221.77/4632.56/5410.33 µs`。
- GCC ASan/UBSan（启用泄漏检测并在错误时停止）：完整 `test_stream_reset` 通过，墓碑容量 40,000，无 sanitizer 报告；附带 P50/P95/P99 为 `958.52/1178.06/1449.98 µs`。
- `git diff --check` 通过。所有构建和运行临时目录设置在仓库内 `build/test-tmp/`，未使用 `/tmp`。

## 影响与限制

只修改 `tests/test_stream_reset.c`、测试记录、README、剩余工作记录和 Xmake 版本号；不改生产运行时、公开 API/ABI 或协议线格式。运行时性能无影响。测试额外创建两个应用线程，并检查两个 RESETTING 流的信用隔离。

本回归覆盖同一共享 Channel 上两个同时 RESETTING 的流；没有覆盖 split 连接、多 Channel、压缩解码工作区释放或更大规模并发 RESET。ERR-C 其他信用组合及 ERR-A/B/D/E、#242 和 V2 整体验收仍未完成。

## 回滚方案

撤销 dev.131 提交，删除双流并发回归和本报告，将 README 与 Xmake 版本恢复至 dev.130；随后在 Release 下重跑完整 `test_stream_reset`。

## 验收标准

1. 两条流均已进入 RESETTING 且尚未收到对端确认时，各自持有的 128 字节正文可由两个应用线程并发释放，均返回 `TR_OK`。
2. 两条流的接收量、消费量分别为 128，通告上限各保持 256，`window_updates_tx` 不增长。
3. 两次 RESET 完成及 PING/PONG 屏障期间均无目标流迟到 WINDOW_UPDATE，健康同伴仍正常交付并结束。
4. Release、TSan、ASan/UBSan 下完整 `test_stream_reset` 均通过。
