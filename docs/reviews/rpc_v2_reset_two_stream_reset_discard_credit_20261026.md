# 双 RESETTING 流在途 DATA 与并发释放回归

日期：2026-10-26。关联 #242/ERR-C。版本：`2.0.0-dev.131 -> 2.0.0-dev.132`。

## 覆盖范围

扩展 `test_stream_reset` 的双流并发释放场景，保留原有无晚到 DATA 的基准变体，并新增在途 DATA 变体。两个已打开流各接收一条由两段 64 字节 DATA 重组而成的 128 字节消息并由应用持有。测试分别启动本地 RESET、暂不确认对端 RESET；随后向每条 RESETTING 流发送一条合法的 64 字节 DATA（消息 ID 2），模拟对端在观察 RESET 前已发出的在途帧。

RESETTING 路径必须只丢弃该 DATA、把 64 wire 字节计入接收和消费账本，不向应用交付，也不恢复信用。然后两个条件变量同步的应用线程并发释放各自持有的 128 字节正文。每条流最终接收量与消费量均为 192，通告上限仍为 256。测试核对 Channel `window_updates_tx` 不增长，RESET wire 收敛及 PING/PONG 屏障期间无目标流 WINDOW_UPDATE；两条流退役后，健康同伴仍能交付并完成 RESET。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，真实 Channel 墓碑容量 40,000。附带周转观测 P50/P95/P99 为 `829.25/925.01/1233.79 µs`，OPEN/RESET 循环墙钟 `35.53 s`；这些值会随负载波动，不是性能验收阈值。
- GCC ThreadSanitizer（`halt_on_error=1:second_deadlock_stack=1`）：完整 `test_stream_reset` 通过，墓碑容量 5,000，无数据竞争报告；附带 P50/P95/P99 为 `3126.76/4548.64/5146.58 µs`。
- GCC ASan/UBSan（启用泄漏检测并在错误时停止）：完整 `test_stream_reset` 通过，墓碑容量 40,000，无 sanitizer 报告；附带 P50/P95/P99 为 `884.01/1015.39/1342.75 µs`。
- clang-format 18.1.8 格式化新增代码范围并复查；`git diff --check` 通过。构建和运行临时目录设在仓库内 `build/test-tmp/`，未使用 `/tmp`。

## 影响与限制

只修改 `tests/test_stream_reset.c`、测试记录、README、剩余工作记录和 Xmake 版本号；不改生产运行时、公开 API/ABI 或协议线格式。运行时性能无影响。

本回归覆盖同一共享 Channel 上两条 RESETTING 流各自处理一条已在途 DATA，并与应用载荷释放并发；不覆盖 split 连接、压缩解码工作区、超过窗口剩余信用的恶意 DATA 或更大规模 RESET 压力。ERR-C 其余组合及 ERR-A/B/D/E、#242 与 V2 整体验收仍未完成。

## 回滚方案

撤销 dev.132 提交，移除在途 DATA 变体与本报告，将 README 和 Xmake 版本恢复至 dev.131；在 Release 下重新运行完整 `test_stream_reset`。

## 验收标准

1. 两条 RESETTING 流各接收 64 字节合法在途 DATA；不增加应用回调次数，接收与消费账本各增加 64。
2. 并发释放各自持有的 128 字节正文后，两条流接收/消费量均为 192、通告上限均保持 256。
3. `window_updates_tx` 不增长；RESET 与 PING/PONG 屏障期间无目标流 WINDOW_UPDATE，健康同伴正常完成。
4. Release、TSan、ASan/UBSan 下完整 `test_stream_reset` 均通过。
