# Split Channel 压缩窗口跨 lane 同伴进展回归

日期：2026-10-25。版本：`2.0.0-dev.88 -> 2.0.0-dev.89`。关联 #241。

## 目标

核查一个 Split Channel 上，某条 RPC Record Stream 的 Zstandard 正文因接收端暂停而耗尽自身网络窗口时，另一个 lane 上仍有信用的小压缩正文是否会被队首阻塞。测试使用真实 TCP、Reactor、Channel 与 RPC Record Stream，不调用生产私有入口，也不改变运行时实现。

## 场景

- Client/Server 建立独立 CONTROL 与 BULK TCP 连接，且两端 Channel 均完成握手；各 lane 分别打开一条 RPC Stream。
- 两条 Stream 的 RPC Record MESSAGE 都使用 Zstandard 正文，Channel DATA 帧保持普通 DATA 标志。测试在 BULK RPC OPEN 已交付后暂停 Server 对 BULK 连接的接收。
- Client 提交 16 KiB 伪随机 BULK 正文，并等待公开 Stream 流控诊断显示 `tx_sent_bytes == tx_send_limit`。配置窗口为 4096 字节；OPEN 已消费的额度通过绝对 WINDOW_UPDATE 归还后，稳定边界为 4144/4144 字节。
- BULK 没有收到 WINDOW_UPDATE、完整压缩消息尚未交付时，Client 再提交 128 字节 CONTROL 压缩正文。测试要求 Server 先收到并校验 CONTROL 正文，同时 BULK 仍保持 4144/4144 且未调用业务消息回调。
- 恢复 Server 的 BULK 接收后，要求大消息完整交付、校验压缩标志和正文 checksum，并确认 BULK 发送字节数推进到 16,494。

## 验证结果

- GCC Release `test_rpc_record_stream` 连续运行 5/5；五次均得到相同流控观测：BULK 停在 4144/4144 字节，CONTROL 小消息先完成，BULK 恢复后达到 16,494 字节并交付。
- GCC Debug `test_rpc_record_stream` 完整目标通过，边界观测与 Release 一致。
- `git diff --check` 通过；系统 clang-format 的改动行检查通过。
- dev.89 推送后的 Push CI（40178672663）、PR CI（40178670054）和 RPC benchmark smoke（40178672659）均已通过。

## 结论边界

此回归证明当一个 lane 的 RPC Record Zstandard 正文受自身 Stream 网络信用限制时，同 Channel 另一条可发送 lane 能继续交付，且压力解除后大消息恢复。它没有覆盖 Channel `TR_FRAME_F_COMPRESSED` 分帧调度。接收暂停是有界、确定性的窗口门控，不代表真实慢链路上的吞吐模型。用例没有耗尽 Reactor 共享 DATA TX 项池，不覆盖同 lane 多 Stream 的最大等待界、长期持续饱和公平比例、跨机器基线或完整 #241 资源/取消矩阵。因此 #241 仍未完成。本切片只改测试、文档和开发版本号，无生产运行时、公开 API/ABI 或 wire 改动。
