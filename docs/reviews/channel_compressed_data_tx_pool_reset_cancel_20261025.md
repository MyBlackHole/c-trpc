# Shared Channel DATA TX 池满时 RESET 压缩等待流回归

日期：2026-10-25。版本：`2.0.0-dev.95 -> 2.0.0-dev.96`。关联 #241、#235、PR #324。

## 目标

验证共享 DATA TX 池耗尽期间，主动 RESET 一个已完成压缩、正等待池项的 Stream 后，
被取消工作的预算可立即回收，且同一物理连接上的其他压缩等待者仍可恢复交付。

## 场景与断言

- 真实 TCP Shared Channel 的 DATA TX 项容量为 1，CONTROL TX 池单独配置。
- BULK RAW Stream 在 TCP 写入被测试夹具置为 `EAGAIN` 后占用唯一 DATA TX 项。
- CONTROL 和两条 BULK Stream 分别提交 Zstandard 消息；确认三个压缩发送者都遇到池耗尽，
  DATA TX 就绪观察已注册，池占用保持为 1；池满阶段没有被测试夹具延迟的重试定时器。
- 通过 Reactor owner 对其中一条 BULK 压缩 Stream 发起 `TR_ERR_CLOSED` RESET。确认发送端
 立即收到本地 RESET 事件、总内存预算下降，唯一 DATA TX 项仍由 RAW blocker 持有。
- 解除 socket 阻塞后，Server 收到 RAW、CONTROL 压缩和另一条 BULK 压缩消息；被 RESET 的
  Stream 不交付 DATA，Server 收到 RESET。Client 的发送消息计数为 3，数据池占用归零。
- 关闭 Channel 和消息服务后，总内存预算归零。

## 验证结果

- GCC Release 完整 `test_transport/default` 连续 6/6 通过；每次均记录 4 次 DATA TX 池耗尽重试。
- GCC Release 项目完整测试套件 91/91 通过，耗时 148.688 秒。
- 本地改动行由 clang-format 18.1.8 检查通过，`git diff --check` 通过。
- 本次仅扩展真实 TCP 集成测试、开发记录和版本号；不修改生产运行时、公开 API/ABI 或 wire 格式。

## 结论边界

该回归证明单个被 RESET 的压缩 TX 等待流可释放预算且不阻断两个存活同伴的有限恢复场景，
不建立超过两个存活等待者下的持续公平 SLA、逐消息等待界、吞吐阈值或跨机器稳定基线；
也不完成 #241/#235 的完整预算与取消矩阵，V2 整体验收仍未完成。
