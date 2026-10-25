# RPC V2 真实 Channel 最大容量 RESET 墓碑周转

日期：2026-10-25。关联 #242/ERR-E。版本：`2.0.0-dev.69 -> 2.0.0-dev.70`。

## 覆盖范围

本切片只扩展 `test_stream_reset`，不改 Channel 生产实现、公开 API/ABI 或线协议。测试通过真实 socketpair、Reactor 和
Channel 依次完成 OPEN、双向 RESET，直到墓碑 FIFO 满，再验证最老和最新驻留墓碑均可吸收同原因重复 RESET；新增一条
RESET 后，最老墓碑被淘汰，同 ID/原因的晚到 RESET 按未知流错误关闭连接。

GCC Release 与 ASan/UBSan 配置 `max_streams=40000` 并周转 40,000 条墓碑，再加入第 40,001 条验证首次淘汰。TSan 下用
`max_streams=5000` 填满同一真实 Channel 状态机边界，以避免该目标超过现有 120 秒单测时限；最大容量的索引算法仍由
`test_reset_tombstone_index` 独立覆盖。夹具只循环复用 256 个 Stream handle 记录槽，不随压力轮数增长。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，输出 `real Channel tombstone capacity 40000 passed`。
- GCC ASan/UBSan：完整 `test_stream_reset` 通过，同样验证 40,000 条墓碑历史。
- GCC TSan：完整 `test_stream_reset` 通过，输出 `real Channel tombstone capacity 5000 passed`。
- `git diff --check` 在提交前执行。
- dev.70 的远程 GitHub CI 与 RPC benchmark smoke 需推送后确认。

## 边界、风险与回滚

该测试证明最大配置下真实 Channel 的 RESET 路径、墓碑驻留和 FIFO 首次淘汰行为，不测量 RESET 吞吐/延迟，不覆盖 40,000
个键全部映射到同一 home bucket 的极端碰撞，也不证明长时间高频负载的 CPU 上界。TSan 的真实 Channel 压力容量低于生产最大值；
最大容量索引的 TSan 覆盖来自独立索引测试。

本提交只更新回归测试、README、版本号和验证记录。回滚可撤销 dev.70 提交并恢复 dev.69 版本，再运行 GCC Release
`test_stream_reset` 与 `test_reset_tombstone_index`。

## 验收标准

1. Release 与 ASan/UBSan 真实 Channel 填满 40,000 条墓碑历史并验证驻留窗口。
2. 再增加一条 RESET 后，第一条历史确实被 FIFO 淘汰，晚到 RESET 关闭连接；最新驻留墓碑仍被吸收。
3. TSan 完整目标在容量 5,000 下验证同一状态机边界且不超过现有单测超时。
4. 不将 ERR-E 或 #242 的其他未完成验收误标为完成。
