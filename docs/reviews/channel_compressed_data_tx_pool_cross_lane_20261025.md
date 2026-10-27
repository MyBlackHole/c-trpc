# Shared Channel 跨 lane DATA TX 池耗尽恢复回归

日期：2026-10-25。版本：`2.0.0-dev.92 -> 2.0.0-dev.93`。关联 #241、#235、PR #324。

## 目标

验证 Shared Channel 共用的 DATA TX 池被一个逻辑 lane 占满时，另一个 lane 上等待压缩 DATA 的发送器能登记容量就绪通知，并在池项归还后继续发送。

## 场景与断言

- 真实 TCP Shared Channel 将 Reactor 的 `tx_item_capacity` 设为 1，CONTROL TX 池保持独立。
- Client 在 BULK Stream 发送 RAW 消息；测试夹具使对应 TCP 写入返回 `EAGAIN`，并确认唯一 DATA TX 项处于占用状态。
- Client 在同一物理连接上的 CONTROL Stream 提交 Zstandard 消息。压缩发送器发现池满后必须登记 DATA TX 就绪监听，不依赖延迟重试定时器，池占用数仍为 1。
- 放开 TCP 写入后，Server 依次收到 RAW 和压缩消息；按两条 Stream 的 ID、消息序号与正文逐字节核验。客户端和服务端都确认 RAW Stream 属于 BULK、压缩 Stream 属于 CONTROL。
- 两条消息交付后，Reactor 的 DATA TX 项占用数回到 0；压缩重试和就绪监听注册各发生一次，没有挂起的退避定时器。

## 验证结果

- GCC Release 定向 `test_transport/default` 通过。
- GCC Release 完整项目测试套件 91/91 通过，耗时 140.776 秒。
- `git clang-format --diff HEAD -- tests/test_transport.c` 未发现格式改动，`git diff --check` 通过。
- 本次仅改集成测试、文档与版本号；不改生产运行时、公开 API/ABI 或 wire 格式。

## 结论边界

该场景验证一个 BULK RAW 消息占满唯一 DATA TX 项时，CONTROL 压缩发送能在就绪通知后恢复，且接收 lane 和池项回收正确。它不证明多个持续发送者竞争下的公平性、每条消息延迟界、吞吐阈值、跨机器稳定基线或完整预算/取消矩阵；#241/#235 与 V2 整体验收仍未完成。
