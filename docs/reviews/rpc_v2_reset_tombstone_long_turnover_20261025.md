# RPC V2 RESET 墓碑长周转回归

日期：2026-10-25。关联 #242/ERR-E。版本：`2.0.0-dev.63 -> 2.0.0-dev.64`。

## 覆盖范围

本切片只扩展 `test_stream_reset`，不改 Channel 生产逻辑、公开 API/ABI 或线协议。测试使用真实 socketpair、Reactor 和
Channel，在同一物理连接上顺序创建并完成 128 条流的双向 RESET；Channel 墓碑容量固定为 4，因此墓碑 FIFO 完整覆盖 32 轮。
每条流都完成双方 RESET 后才开始下一条，避免把在途流并发误当成墓碑环周转。

RESET 完成后，测试向连接发送与当前最老驻留墓碑和最新墓碑相同的 RESET，并以 PING/PONG 屏障确认重复帧已处理；连接仍保持
UP，活动流数为零。随后发送第一条流的重复 RESET；该墓碑已被 FIFO 淘汰，连接必须报告 `TR_ERR_STATE` 并关闭。累计关闭事件
必须正好为 128。

## 验证

- GCC Release：`test_stream_reset`，1/1 通过。
- GCC TSan：`test_stream_reset`，1/1 通过。
- GCC ASan/UBSan：`test_stream_reset`，1/1 通过。
- `git diff --check` 在提交前执行。

## 边界、风险与回滚

128 次 RESET/32 轮 FIFO 周转验证真实连接上的长期保留和淘汰语义，不是吞吐基准，不设置机器相关耗时门槛；容量只取 4，也不覆盖
哈希索引的恶意碰撞构造、极大流量连接或并发 OPEN/RESET 组合。索引最坏探测成本及高频 CPU 成本仍待独立验收。

回滚可撤销 dev.64 提交：将 `xmake.lua` 与 README 版本恢复为 dev.63，移除本测试函数和记录，并恢复 README/剩余工作台账中的 dev.63 描述；
最后在 Release 下重新运行 `test_stream_reset`。

## 验收标准

1. 在未重建的真实 Channel 上完成 128 次双向 RESET，覆盖 32 轮容量为 4 的墓碑 FIFO。
2. 当前最老和最新驻留墓碑吸收重复 RESET；最早已淘汰墓碑触发连接级未知流错误。
3. 测试在 Release、TSan、ASan/UBSan 下通过，且生产运行时实现没有改变。
