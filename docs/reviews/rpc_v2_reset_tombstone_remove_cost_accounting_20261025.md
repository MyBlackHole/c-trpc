# RPC V2 RESET 墓碑索引删除成本计量

日期：2026-10-25。关联 #242/ERR-E。版本：`2.0.0-dev.73 -> 2.0.0-dev.74`。

## 需求与背景

已有开放寻址碰撞测试报告插入、查找和 RESET 墓碑周转的探测上界，但 `tr_reset_tombstone_index_remove()` 的 `probes_out` 只统计键定位，没有计入删除前整段探测簇校验与 backward-shift。该计数因此低估了碰撞情况下的删除工作，不能作为最大容量碰撞成本证据。

## 实现

`remove` 的 `probes_out` 现在累加三段槽位检查：键定位、删除前簇校验、backward-shift 扫描。生产 Channel 调用仍传 `NULL`；计数在扫描结束后按区间长度计算，不在循环内逐槽计数，也不增加第二次扫描。

测试固定精确小簇计数：3 项环绕碰撞簇删除为 7 次槽位检查，256 个同 home 键的每次簇首淘汰为 513 次。`test_max_capacity_collision_fifo_turnover` 的最大历史总探测上界现在包含删除扫描，而非只覆盖索引查找。

## 验证

- RED：先加入 3 项环绕簇的 `probes == 7` 断言，GCC Release 的 `test_reset_tombstone_index` 在断言处失败；原实现只报告 1 次键定位。
- GREEN：GCC Release、TSan、ASan/UBSan 下 `test_reset_tombstone_index` 和 `test_stream_reset` 均重新构建运行并通过。
- Release 与 ASan/UBSan `test_stream_reset` 在 `max_streams=40000` 下通过；TSan 按现有时限配置以 5,000 容量通过。
- `git clang-format --diff` 对改动行无差异，`git diff --check` 通过。测试临时目录为仓库内 `build/test-tmp`，未在 `/tmp` 写测试文件。

## 影响、性能与风险

只改变内部计数输出的含义和测试上界，不改变墓碑查找、删除、FIFO 周转或协议行为，不涉及公开 API/ABI 或 wire 格式。生产调用传入 `NULL`，没有循环内的计数分支或额外扫描；成功删除路径新增一个扫描结束后的可预测空指针分支，计数加法只在测试传入输出指针时发生。该常数级开销尚未单独做 wall-clock benchmark。

本切片验证 256 路同 home 碰撞簇与 40,000 条目满历史混合碰撞周转，不声称覆盖 40,000 个键全部同 home 的极端输入，也不证明持续高频 CPU/延迟上界；这两项仍待单独验证。回滚可撤销 dev.74 并将版本恢复到 dev.73。

## 验收标准

1. 删除成本计数包含键定位、完整簇校验和 backward-shift 槽位检查。
2. 3 项环绕簇和 256 路同 home 碰撞簇计数精确，最大容量混合碰撞周转的总上界包含删除扫描。
3. 索引单测和真实 Channel RESET 集成测试在 Release、TSan、ASan/UBSan 下通过。
4. 40k 同 home 及持续高频 CPU/延迟成本仍列为未完成项，不由本切片推断通过。

版本变更：`xmake.lua` 与 README 从 `2.0.0-dev.73` 更新到 `2.0.0-dev.74`。
