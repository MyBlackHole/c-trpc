# Shared Channel 逻辑 BULK 压缩 DATA 回归

日期：2026-10-25。版本：`2.0.0-dev.90 -> 2.0.0-dev.91`。关联 #241、PR #324。

## 目标

补足通用 Channel 压缩 DATA 的 Shared Channel 覆盖。dev.90 已通过两个物理连接验证 Split Channel 的 CONTROL/BULK 跨 lane 进展；此项确认共享物理连接上，OPEN 所选逻辑 lane 与后续 DATA 标志之间的职责保持一致。

## 场景

- Client/Server 建立单连接 Shared Channel，打开慢压缩 Stream（CONTROL）与快压缩 Stream（BULK）。测试断言快 Stream 的诊断 lane 为 BULK。
- 慢 Stream 提交编码后超过当前窗口的 Zstandard 正文，接收侧暂停读取，使其发送等待网络信用。
- 快 Stream 提交小型 Zstandard 正文；确认其先完成交付并逐字节校验，而慢 Stream 仍受窗口约束。
- 恢复接收后，确认慢 Stream 的多帧压缩正文继续发送、完整交付并校验。

## 验证结果

- GCC Release 完整 `test_transport` 连续 5/5 通过。
- GCC Debug 完整 `test_transport` 通过。
- GCC Release 完整项目测试套件 91/91 通过，耗时 136.559 秒。
- dev.91 推送后的 Push CI（40180855026）、PR CI（40180851979）和 RPC benchmark smoke（40180855058）均通过。
- 该测试只变更既有场景的快 Stream lane 与对应断言；不改生产代码、公开 API/ABI 或 wire 格式。

## 结论边界

该项确认 Shared Channel 上的逻辑 BULK 压缩 Stream 可以在同连接 CONTROL 大消息等待窗口时先完成，并在压力解除后观察到 CONTROL 大消息继续进展。结合 dev.90 Split Channel 回归，可覆盖共用连接和独立连接两种 lane 承载方式。两个测试都是有界确定性门控，不建立长期公平 SLA、每条消息最大等待界或吞吐阈值，也未耗尽共享 DATA TX 项池；更广预算、取消及多流持续饱和组合仍待验收，#241/#235 尚未完成。
