# 非对称 Stream 初始窗口回归

日期：2026-10-25。版本：`2.0.0-dev.104 -> 2.0.0-dev.105`。关联 #241、#240、#235、PR #324。

## 目标与场景

在真实 TCP Shared Channel 中验证双方可以独立配置接收窗口，并且发送准入使用对端公布的窗口。Client
配置接收窗口 1 KiB，Server 配置 2 KiB；两端各打开一条 Stream，以覆盖双向控制与数据路径。

## 断言

- Client 发起的 Stream 在 Client 侧发送上限为 2 KiB，在 Server 接收侧公布 2 KiB。
- Server 发起的 Stream 在 Server 侧发送上限为 1 KiB，在 Client 接收侧公布 1 KiB。
- Client 到 Server 的 2 KiB `L=W` 消息，以及 Server 到 Client 的 1 KiB `L=W` 消息均逐字节交付。
- Server 在 Client 的 1 KiB 接收窗口上尝试发送 1,025 字节时返回 `TR_ERR_BAD_LENGTH`；信用不变且 Client 无 DATA。
- 接收端持有期间检查完整正文，随后释放消息，使流量账本及缓冲区池能正常回收。

## 验证结果

- GCC Release `test_transport/default` 通过。
- 临时把服务端初始 `WINDOW_UPDATE` 改为回显对端 OPEN 窗口后，Client 的 2 KiB 发送上限断言按预期失败；恢复生产代码后目标测试通过。
- 变更行 clang-format 检查和 `git diff --check` 通过。

本次只扩展集成回归、记录和开发版本号，不改生产运行时、公开 API/ABI 或 wire。该场景只覆盖 1/2 KiB
窗口的双向非对称组合；更广窗口比、消息能力不对称、压缩预算、慢消费者及跨机器基线仍待验收，#241/#235 和 V2 整体验收保持开放。
