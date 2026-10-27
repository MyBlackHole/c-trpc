# Shared Channel 多压缩等待者的 DATA TX 池恢复回归

日期：2026-10-25。版本：`2.0.0-dev.94 -> 2.0.0-dev.95`。关联 #241、#235、PR #324。

## 目标

扩展共享 DATA TX 池容量恢复场景，验证同一连接上多个不同逻辑 lane 的压缩发送者同时等待池项时，容量通知不会漏掉其中一个等待者。

## 场景与断言

- 真实 TCP Shared Channel 将 `tx_item_capacity` 设为 1，CONTROL TX 池仍独立。
- Client 在 BULK RAW Stream 发送短消息；测试夹具将 TCP 写入置为 `EAGAIN`，确认唯一 DATA TX 项被占用。
- Client 分别在 CONTROL 和另一条 BULK Stream 提交 1024 字节 Zstandard 消息。两条压缩消息均进入所有权转移并等待 DATA TX 项；池占用维持 1，记录至少两次池耗尽重试和就绪监听，且没有启动延迟退避定时器。
- 解除 TCP 写阻塞后，Server 收齐 RAW 与两条压缩消息。测试不依赖两条压缩消息的完成先后，而是按 Stream ID、消息 ID 和正文逐条核验；Client 与 Server 两端的诊断均确认 BULK/CONTROL lane 对应正确。
- 三条消息全部交付后，Client 发送计数为 3，Reactor DATA TX 项占用数回到 0。

## 验证结果

- GCC Release 完整 `test_transport/default` 连续 5/5 通过；每次均观察到 CONTROL 与 BULK 两个压缩等待者恢复交付，夹具记录 3 次池耗尽重试。
- GCC Release 完整项目测试套件 91/91 通过，耗时 143.453 秒。
- 本次仅扩展集成测试并更新记录与版本号；不改生产运行时、公开 API/ABI 或 wire 格式。

## 结论边界

该回归验证唯一 DATA TX 项归还后两个跨 lane 压缩等待者都能完成，不定义更多等待者长期竞争下的公平 SLA、每条消息延迟界、吞吐阈值、跨机器稳定基线或更广预算/取消矩阵；#241/#235 与 V2 整体验收仍未完成。
