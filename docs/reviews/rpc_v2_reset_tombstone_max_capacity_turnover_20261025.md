# RPC V2 RESET 墓碑索引最大容量 FIFO 周转

日期：2026-10-25。关联 #242/ERR-E。版本：`2.0.0-dev.65 -> 2.0.0-dev.66`。

## 覆盖范围

本切片扩展 `test_reset_tombstone_index` 中已有的 40k 顺序流 ID 测试，不改索引生产实现、公开 API/ABI 或线协议。测试以
`max_streams=40000` 和容量 131072 的索引先插入 40000 个顺序奇数流 ID，再执行一整轮 40000 次 FIFO 淘汰和环槽复用。

每一轮都先从历史表移除旧 ID、确认它不可查，再在对应环槽写入一个全新的奇数 ID 并插入索引。完成周转后，所有新 ID 必须查回
各自原环槽，所有旧 ID 必须返回 stale。删除、淘汰查询、插入和最终驻留/淘汰查询的累计探测次数限制为 `48 * max_streams`，即
1.92m；原有 40k 插入与首次查询仍保持 `8 * max_streams` 上限。断言按操作次数计量，不依赖机器耗时。

## 验证

- GCC Release、TSan、ASan/UBSan 下 `test_reset_tombstone_index` 均为 1/1 通过。
- 临时让大于 8 槽的索引跳过 backward-shift 修复时，测试在 40k 周转删除断言失败；恢复实现后同一目标通过。
- `git diff --check` 在提交前执行。

## 边界、风险与回滚

该回归覆盖最大配置下顺序 ID 的完整 FIFO 淘汰与槽复用，不是实际 Channel 或网络性能基准；没有 40k 同 home 碰撞簇，因为此类开放寻址工作量为
二次增长，会把单元测试变成约 1.6b 次探测。最大容量的对抗碰撞与真实并发网络压力仍待单独评估。

回滚可撤销 dev.66 提交：将 `xmake.lua` 与 README 恢复至 dev.65，移除 40k 周转断言和本记录，恢复剩余工作台账中的 dev.65 边界；随后在 Release 下重跑
`test_reset_tombstone_index`。

## 验收标准

1. `max_streams=40000` 的满历史索引完整周转 40000 个 FIFO 环槽。
2. 所有淘汰 ID 均不可查，所有替换 ID 均返回正确的原环槽。
3. 插入、删除和查询累计探测满足既定确定性上限，Release、TSan、ASan/UBSan 下目标通过。
