# RPC V2 RESET 墓碑固定预算回归

日期：2026-10-25。关联 #242/ERR-E。版本：`2.0.0-dev.60 -> 2.0.0-dev.61`。

## 覆盖范围

`test_invalid(EVICTED_RESET)` 使用容量为 2 的真实 Channel，并向其注入 `tr_memory_budget`。Channel
创建完成后，测试把预算上限收紧到已占用的固定字节数，再通过三条流完成双向 RESET，使最早一条墓碑被
FIFO 淘汰。每个 RESET 周转点都检查预算当前值和峰值保持不变，且没有发生预算拒绝；Channel 销毁后，
预算当前值归零，峰值仍只等于固定值。

用例还分别验证淘汰边界两侧的协议行为：旧 RESET 在墓碑仍驻留时被重复发送，随后使用 PING/PONG 屏障
确认连接仍工作；填满并周转墓碑 FIFO 后再发送同一旧 RESET，连接必须按未知流错误关闭。这样预算断言
覆盖真实 RESET 墓碑历史周转，而不是只检查 Channel 创建时的通用固定存储。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，输出 `stream_reset: wire, feature, FIFO, pressure, retry, retirement, ID exhaustion passed`。
- GCC TSan：完整 `test_stream_reset` 通过，同一输出且无数据竞争报告。
- 预算反例变异：为每次墓碑写入临时增加预算预留；在固定预算上限下，回归于预算拒绝计数断言失败，证明用例能检测 RESET 历史产生的增量预算成本。生产源码已恢复并重新通过 Release 测试。
- `git diff --check` 通过。
- 只增加测试、开发版本和记录，不修改生产 C 逻辑、公开 API/ABI 或线协议。

## 边界与风险

该回归证明墓碑 FIFO 周转不按条目数增加内存预算，也覆盖窗口内命中与窗口外淘汰行为；它没有测量每次 RESET
查找/写入的 CPU 成本，不覆盖长时间高频压力、不同容量的大规模基准、信用欠账或压缩丢弃组合。运行时性能无影响；
测试新增真实账本采样、三条流的 RESET、一次驻留重复帧和 PING/PONG 屏障。

回滚可撤销 dev.61 提交，恢复 `xmake.lua` 与 README 版本，移除本回归和记录，并恢复此前 ERR-E 路线图表述。

## 验收标准

1. RESET 墓碑驻留与 FIFO 周转期间，预算当前值和峰值始终等于固定存储值，且无预算拒绝。
2. Channel 销毁后当前预算归零；驻留墓碑吸收重复 RESET，淘汰墓碑后旧 RESET 按未知流错误关闭连接。
3. Release 与 TSan 下完整 `test_stream_reset` 均通过。
