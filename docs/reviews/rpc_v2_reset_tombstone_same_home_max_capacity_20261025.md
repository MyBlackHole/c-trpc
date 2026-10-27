# RPC V2 RESET 墓碑索引最大容量同 home 成本

日期：2026-10-25。关联 #242/ERR-E。版本：`2.0.0-dev.74 -> 2.0.0-dev.75`。

## 需求与背景

此前索引回归覆盖了 256 个同 home 键及 40,000 条历史的 FIFO 周转，但没有测量 40,000 个键全部落入同一 home 时的确定性槽位检查数。dev.74 已使删除探针计数包含键定位、整段簇校验和 backward-shift；本切片补齐最大配置的极端输入。

## 构造与实现

测试使用 `max_history=40000` 和容量 131072（不小于 `2 * max_history` 的最小 2 次幂）。它在索引层直接构造合法线性探测布局，不创建 Reactor、Channel 或真实网络连接。

设 `C=0x9e3779b97f4a7c15`，流 ID 哈希为 `H(id)`，测试句柄的 `slot/generation` 组合值取 `preimage XOR (H(id) * C)`，Reactor 指针取空。索引 home 函数在最终 avalanche 混合前会再次异或 `H(id) * C`，因此每个键进入 mixer 的值恒为同一个 preimage，最终 home 也相同。流 ID 取 1 到 79,999 的奇数；句柄非零且互异由断言验证。

用例将 40,000 个复合键按 `home+i` 连续放入槽表，依次验证：

- 尾部键成功查找：40,000 次槽检查。
- 删除簇首键：总计 80,001 次检查，包含一次键定位、40,000 次整簇校验和 40,000 次 backward-shift 扫描。
- 删除后首个幸存键查找为 1 次，末尾幸存键为 39,999 次。
- 复用已淘汰 FIFO 环槽并向簇尾插入新键：40,000 次；新键命中：40,000 次。
- 查询已淘汰旧键：扫描 40,000 个占用槽及一个空槽，共 40,001 次。

## 验证

- GCC Release：`test_reset_tombstone_index` 构建并运行通过。
- GCC TSan：`test_reset_tombstone_index` 构建并运行通过。
- GCC ASan/UBSan：`test_reset_tombstone_index` 构建并运行通过。
- `git clang-format --diff` 与 `git diff --check` 通过；测试临时目录位于仓库 `build/test-tmp`，未写入 `/tmp`。
- [dev.75 push CI](https://github.com/MyBlackHole/c-trpc/actions/runs/40156017294)、[PR CI](https://github.com/MyBlackHole/c-trpc/actions/runs/40156020823) 与 [RPC benchmark smoke](https://github.com/MyBlackHole/c-trpc/actions/runs/40156021211) 全部通过。

## 影响、性能与风险

仅增加内部索引单测、评估记录、README/剩余工作台账及版本号；生产代码、公开 API/ABI、协议和运行时行为不变。该构造让最坏探针计数确定且运行量为 O(40,000)，不依赖机器速度或数十亿次候选搜索。测试直接依赖当前 home 混合公式；若哈希公式变化，构造断言会失败并需同步更新。

精确槽位计数证明该人工输入下查找、插入和删除的线性工作量，但不等同 wall-clock benchmark，也不覆盖持续高频 RESET、真实连接 40k 并发、CPU 饱和或 P99/P999 延迟。TSan 在这里用于该测试目标的 sanitizer 构建与运行，不代表覆盖了并发竞态。

回滚可撤销 dev.75 提交，将 `xmake.lua` 与 README 版本恢复为 dev.74，并删除本报告与对应测试。

## 验收标准

1. 最大配置为 40,000 个唯一、非零句柄复合键，索引容量为 131072，全部落入同一 home。
2. 尾部查找、簇首删除、删除后查找、环槽复用、旧键 stale 查询的槽检查数与上文一致。
3. `test_reset_tombstone_index` 在 Release、TSan、ASan/UBSan 下通过；GitHub push/PR CI 与 smoke 检查通过。
4. 真实高频 CPU/延迟及连接组剩余 ERR-A/B/C/D/E 验收继续列为未完成项。

版本变更：`xmake.lua` 与 README 从 `2.0.0-dev.74` 更新到 `2.0.0-dev.75`。
