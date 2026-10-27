# RPC V2 RESET 墓碑查询索引

日期：2026-10-25。关联 #242/ERR-E。版本：`2.0.0-dev.61 -> 2.0.0-dev.62`。

## 设计与实现

Channel 继续以大小为 `max_streams` 的 FIFO 墓碑环作为有限精确历史。新增开放寻址表以 32 位 `ring_slot + 1`
定位环槽；复合键为 `(物理连接 reactor/slot/generation, stream_id)`，键字段保存在墓碑记录中，避免索引重复保存
`stream_id`。复合身份是必要条件：split Channel 的 CONTROL/BULK 各自维护 OPEN ID 前沿，同一个流 ID 可以先后在
两条连接上退休，两个墓碑都必须保留。表容量沿用活动流索引的容量规则，为不小于 `2 * max_streams` 的 2 次幂。
命中后仍检查对端 RESET 原因，维持现有协议接受边界。

墓碑环覆盖旧槽时，Channel 在锁内先从索引移除旧键并修复线性探测簇，再覆写环槽并插入新键。索引与活动流
索引位于同一固定分配中，Channel 创建时一次性分配并计入 `tr_memory_budget`；每次 RESET 周转不分配内存。
索引使用每桶 4 字节，`max_streams=40000` 时容量为 131072，新增固定预算为 512 KiB。平均查询探测成本为
O(1)，人为构造碰撞时最坏仍受固定表容量约束。

## 验证

- `test_reset_tombstone_index`：验证表尾回绕的三键碰撞簇、删除后的簇修复、FIFO 槽复用、重复插入/无效槽拒绝、两条物理连接上同 ID 墓碑并存与独立删除，以及 40k 个顺序奇数流 ID 的全部插入与成功查询总探测数不超过 320k。
- `test_channel_create_transaction`：分别创建 `max_streams=1/2/3` 的 Channel，比较固定预算差分，确认当索引容量从 2 增至 4、再增至 8 时，新增每桶 4 字节已纳入预算。临时删除该预算项的变异使差分断言失败；恢复后目标通过。
- `test_stream_reset`：驻留墓碑吸收重复 RESET；经历两轮 FIFO 覆盖后旧 RESET 关闭连接；预算在周转中保持固定并于销毁后归零；真实 split Channel 的 CONTROL/BULK 先后各自退役同一 ID 后，双方重复 RESET 均由各自连接墓碑吸收。
- GCC Release、GCC TSan、GCC ASan/UBSan 下 `test_reset_tombstone_index`、`test_stream_reset`、`test_channel_create_transaction` 与 `test_channel_drain` 均通过。
- 将索引查找临时替换为线性历史扫描时，索引单元测试在已删除键仍被扫描命中的断言处失败；恢复哈希查找后测试通过。
- `git diff --check` 通过。
- 生产 Channel 内部查找与固定内存占用发生变化；公开 API/ABI、线协议及 RESET 帧语义不变。

## 边界与风险

40k 条目测试检查确定性探测次数，不设置依赖机器速度的耗时门槛，也未覆盖 40k 个真实网络 Stream。哈希均匀时
探测保持较低；恶意或退化碰撞仍可能触发线性探测，且表会为最大并发流数提前保留固定内存。长时间真实连接压力、
最坏碰撞工作量和不同容量下的内存/CPU 取舍仍待验收。

回滚可撤销 dev.62 提交，恢复 `xmake.lua` 与 README 的 dev.61 版本，移除索引模块/单元测试与本记录，恢复 Channel
原 FIFO 全表查询，并重新运行 Release `test_stream_reset` 与 Channel 构造事务目标。

## 验收标准

1. 命中查询只返回对应流 ID 的环槽，碰撞、删除、探测簇回绕和槽复用均保持正确。
2. RESET 墓碑仍按 FIFO 有界淘汰，重复 RESET 仍严格校验连接与原因；无每次 RESET 分配。
3. 哈希表固定字节计入 Channel 预算，40k 条目探测回归满足总探测上限。
4. Release、TSan、ASan/UBSan 下索引、RESET、构造事务和 Channel 排空目标通过。
