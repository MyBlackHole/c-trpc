# Shared Channel 六存活压缩流十二轮公平性回归

日期：2026-10-25。版本：`2.0.0-dev.102 -> 2.0.0-dev.103`。关联 #241、#235、PR #324。

## 目标与场景

沿用真实 TCP Shared Channel 单 DATA TX 项池恢复场景和六个存活压缩 Stream，将每条高熵逻辑正文由
1 KiB 增至 2 KiB。正文小于 Channel 的 4 KiB 初始窗口，Zstandard 编码仍在已协商的 4 KiB 编码上限内；
每条流因此在本次观察期间保持有待发送的多帧消息，而不改变窗口配置或生产调度代码。

池由 BULK RAW Stream 在 socket `EAGAIN` 时占用。待 RESET 的 BULK 压缩候选先实际遇到池满并登记
DATA TX 就绪观察，再被 RESET。CONTROL/BULK 各三条存活压缩 Stream 等待池恢复。服务端仍使用单 worker，
有界任务容量和事件分发上限为 8；内存预算仍为 16 MiB。

## 调度与所有权断言

- 六个成功 FIRST DATA 帧来自六个不同存活 Stream，均为 FIRST、非 LAST。
- 紧随的 66 个续帧按相同 Stream 顺序轮转十一轮；合计前 72 帧完成十二轮，每条流再次获得机会前，
  其他五条均各发送一次。
- RESET 候选无 DATA；六条压缩正文和 RAW 正文逐字节交付，发送消息数正确，TX 池及最终消息预算归零。

该测试证明固定六条存活流、单项 DATA TX 池、2 KiB 消息的前 72 帧顺序，不构成任意流数、无限持续
负载下的最大等待时间、吞吐或跨机器 SLA。

## 验证结果

- GCC Release `test_transport/default` 连续 6/6 通过；每次检查 72 个帧记录并观察到 108 次池满重试。
- GCC Release 项目全量测试 91/91 通过，耗时 147.471 秒。
- clang-format 23.1.3 变更行检查和 `git diff --check` 通过。
- 临时令成功发送后的游标停留在同一 Stream，回归按预期在 FIRST 帧轮转断言失败；恢复原生产代码后，
  连续 6/6 和完整 Release 套件通过。

本次只扩展测试、文档和开发版本号，不改生产运行时、公开 API/ABI 或 wire。任意流数的长期公平保证、
逐消息延迟上界、吞吐阈值、跨机器基线及完整预算/取消矩阵仍待验收；#241/#235 与 V2 整体验收仍开放。
