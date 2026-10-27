# Shared Channel 三存活压缩流公平轮转回归

日期：2026-10-25。版本：`2.0.0-dev.98 -> 2.0.0-dev.99`。关联 #241、#235、PR #324。

## 目标

将 Shared Channel 唯一 DATA TX 池项等待场景从两个存活压缩发送者扩展到三个，验证 RESET
第三条等待流后，池项恢复期间剩余流可多轮轮转发送。

## 场景与断言

- 真实 TCP Shared Channel，DATA TX 项容量为 1，帧 payload 上限为 128 字节，Stream 上限为 5。
- BULK RAW Stream 在 socket `EAGAIN` 时持有唯一 DATA TX 项。先提交准备 RESET 的 BULK
  Zstandard 消息，逐条追踪并确认它自己的 Stream ID 遇到池耗尽返回，且共享连接登记了 DATA TX
  就绪观察；再提交另外三条 1024 字节高熵 Zstandard 消息，其中两条为 CONTROL、一条为 BULK。
- 保持池项由 RAW 独占，通过 Reactor owner RESET 已确认遇到池满的 BULK 压缩流，验证其预算下降。
- 解除阻塞后检查前九个成功压缩 DATA 帧：前三帧来自三个不同存活 Stream，且均为 FIRST、
  非 LAST；随后六帧以相同 Stream 顺序重复两个轮次，均为非 FIRST、非 LAST 的压缩续帧。
- 验证 RESET 流没有 DATA；三条压缩正文逐字节交付，RAW 正文完整交付；发送池和预算归零。

## 验证结果

- GCC Debug 完整 `test_transport/default` 连续 6/6 通过。
- GCC Release 项目全量测试 91/91 通过；该套件包含完整 `test_transport/default`。
- clang-format 18.1.8 修改行检查与 `git diff --check` 通过。
- 本次仅扩展真实 TCP 集成测试、开发记录和版本号，不改生产运行时、公开 API/ABI 或 wire。

## 结论边界

该回归验证三个存活压缩等待流在固定池容量下连续三轮 DATA 帧轮转，并覆盖一个等待者被
RESET 后的恢复场景；它不建立四个及以上存活流下的长期公平 SLA、逐消息等待上界、吞吐阈值、
跨机器稳定基线或完整预算/取消矩阵。#241/#235 和 V2 整体验收仍未完成。
