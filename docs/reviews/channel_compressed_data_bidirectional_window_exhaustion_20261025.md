# Split Channel 双向窗口耗尽时的 CONTROL 进展回归

日期：2026-10-25。版本：`2.0.0-dev.93 -> 2.0.0-dev.94`。关联 #241、#235、PR #324。

## 目标

补齐 Split Channel 两个方向的 BULK 压缩窗口同时耗尽场景：确认双方 DATA 都停滞时，CONTROL 压缩消息仍可双向交付；分别恢复接收后，两条 BULK 消息都能完成。

## 场景与断言

- Client/Server 在真实独立 CONTROL/BULK TCP 连接上建立 Split Channel，并在两端启用 Zstandard 接收。
- 双方暂停各自的 BULK 接收；Client 与 Server 在同一 BULK Stream 的相反方向分别发送不同的 300 字节正文。
- 两条 DATA 发送均停在 `tx_sent_bytes == tx_send_limit == 128`，对应接收端尚未交付消息。
- 两条 BULK 都受阻期间，双方在 CONTROL Stream 互发 Zstandard 消息；对端均先解码交付，并按 Stream ID、消息 ID 和正文逐字节校验，同时再次确认两方向 BULK 仍停在 128/128。
- 先恢复 Server 的 BULK 接收，再恢复 Client 的 BULK 接收；双方分别校验收到的反向正文，两方向 TX 进度均推进到 330 字节，Channel 发送消息数均为 2。
- 测试关闭时确认压缩服务、Reactor 和预算均完成回收。

## 验证结果

- GCC Release 完整 `test_transport/default` 连续 5/5 通过；每次都观测到双向 128/128 停滞、双向 CONTROL 先完成、双向 BULK 恢复至 330/330。
- GCC Release 完整项目测试套件 91/91 通过，耗时 143.845 秒。
- 本次仅扩展集成测试并更新记录与版本号；不改生产运行时、公开 API/ABI 或 wire 格式。

## 结论边界

该回归验证一个 Stream 对上的双向窗口同时耗尽与恢复，以及受阻期间双向 CONTROL 的进展；它不定义多发送者持续公平 SLA、每条消息延迟界、吞吐阈值、跨机器稳定基线或更广预算/取消矩阵，#241/#235 与 V2 整体验收仍未完成。
