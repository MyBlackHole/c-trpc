# Split Channel 通用压缩 DATA 跨 lane 信用进展回归

日期：2026-10-25。版本：`2.0.0-dev.89 -> 2.0.0-dev.90`。关联 #241、PR #324。

## 目标

验证通用 Channel 的 `TR_FRAME_F_COMPRESSED` DATA 在 Split Channel 中能否跨 lane 保持独立信用进展。此项补足 dev.89 的 RPC Record 正文压缩测试：RPC Record 压缩正文使用普通 Channel DATA 分帧，无法覆盖通用 Channel 压缩帧调度器。

## 场景

- Client/Server 在同一 Reactor 上建立独立 CONTROL 与 BULK TCP 连接，真实启动 Split Channel 并完成两个 lane 的握手和 Zstandard 能力协商。
- 分别在 CONTROL 与 BULK 打开 Stream。暂停 Server 对 BULK 连接的接收，向 BULK Stream 提交 300 字节伪随机正文并用 Zstandard 压缩。
- 配置 Stream 信用窗口为 128 字节；确认 BULK `tx_sent_bytes == tx_send_limit == 128`，消息尚未完整发送或交付。
- 在 BULK 信用停滞期间，通过 CONTROL Stream 发送并校验小型压缩正文；确认 CONTROL 已完成而 BULK 仍停在 128/128。
- 恢复 BULK 接收，校验 Server 收到的完整正文、Stream 与消息 ID，并确认 BULK TX 最终推进至 330 字节。

## 根因与修复

压缩 TX 调度器将 `tr_lane_flag(stream->lane)` 复制到了 DATA 帧。该标志用于 `STREAM_OPEN` 在共享物理连接上选择逻辑 lane；DATA 接收则根据已打开 Stream 的登记信息校验 lane，并要求标志只含 `FIRST`、`LAST`、`COMPRESSED`。因此 CONTROL 压缩 DATA 正常交付，而带 `LANE_BULK` 的 BULK 压缩 DATA 被隔离并以 `TR_ERR_STATE` RESET。

修复移除压缩 DATA 上的 lane 标志，保持 `FIRST/LAST/COMPRESSED` 分帧语义不变。Stream 身份及连接代次仍由接收路径校验；未改变公开 API/ABI 或 wire 格式。

## 验证结果

- GCC Release 完整 `test_transport` 连续 5/5 通过。五次均观测 BULK 停于 128/128、CONTROL 先完成、恢复后 BULK 推进至 330 字节。
- GCC Debug 完整 `test_transport` 通过，边界和正文断言一致。
- GCC Release 完整项目测试套件 91/91 通过，耗时 141.861 秒。
- dev.90 推送后的 Push CI（40180144592）、PR CI（40180148209）和 RPC benchmark smoke（40180148249）均通过。
- 新回归在修复前稳定复现接收超时；诊断显示双方 Stream RESET 状态为 `TR_ERR_STATE`，与接收器拒绝 DATA lane 标志的路径一致。

## 结论边界

该测试证明一个 BULK 压缩 Stream 耗尽当前网络信用时，同一 Split Channel 的 CONTROL 压缩 Stream 可以先交付，且恢复 BULK 接收后消息能够继续并完整交付。接收暂停构造的是确定性信用门控，不代表生产慢链路的吞吐模型。此项不建立长期持续饱和公平比例、每条消息最大等待界或吞吐 SLA，也未耗尽共享 DATA TX 项池；更广预算、取消、同 lane 多 Stream 组合仍待验收。因此 #241/#235 资源与活性工作仍未完成。
