# Shared Channel 四存活压缩流公平轮转回归

日期：2026-10-25。版本：`2.0.0-dev.99 -> 2.0.0-dev.100`。关联 #241、#235、PR #324。

## 目标

将 Shared Channel 唯一 DATA TX 池项等待场景从三个存活压缩发送者扩展到四个，验证 RESET
当前池满候选后，四条存活流可以连续多轮交错发送。

## 场景与断言

- 真实 TCP Shared Channel，DATA TX 项容量为 1，帧 payload 上限为 128 字节，Stream 上限为 6。
- BULK RAW Stream 在 socket `EAGAIN` 时持有唯一 DATA TX 项。先提交准备 RESET 的 BULK
  Zstandard 消息，追踪并确认该 Stream ID 遇到池耗尽返回，且共享连接登记 DATA TX 就绪观察。
- 再提交四条 1024 字节高熵 Zstandard 消息，其中 CONTROL、BULK 各两条。池项仍由 RAW 独占时，
  通过 Reactor owner RESET 已确认遇到池满的 BULK 流，并验证其预算下降、TX 项仍被 RAW 占用。
- 解除阻塞后检查前十二个成功压缩 DATA 帧：前四帧来自四个不同存活 Stream，均为 FIRST、非
  LAST；随后八帧按相同 Stream 顺序再轮转两轮，均为非 FIRST、非 LAST 的压缩续帧。
- 验证 RESET 流没有 DATA；四条压缩正文与 RAW 正文逐字节交付；发送池和消息预算归零。

## 验证结果

- GCC Debug 完整 `test_transport/default` 连续 6/6 通过。
- GCC Release 项目全量测试 91/91 通过，耗时 53.463 秒；该套件包含完整 `test_transport/default`。
- clang-format 18.1.8 修改行检查与 `git diff --check` 通过。
- 本次仅扩展真实 TCP 集成测试、开发记录和版本号，不改生产运行时、公开 API/ABI 或 wire。

## 结论边界

该回归验证四条存活压缩流在固定池容量下连续三轮 DATA 帧轮转，并覆盖当前池满候选被 RESET
后的恢复场景；它不建立四条及以上存活流的长期公平 SLA、逐消息等待上界、吞吐阈值、跨机器
稳定基线或完整预算/取消矩阵。#241/#235 和 V2 整体验收仍未完成。
