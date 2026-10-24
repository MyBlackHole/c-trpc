# RPC V2 TEST-C1 固定种子探索与轨迹缩减记录

日期：2026-10-25。关联 #247 TEST-C，基于 TEST-B4 `2abfd8fa3b37bc2c267b1a02f4ef8151cdd2cc63`。

## 交付范围

新增 `scripts/rpc_v2_trace_explorer.py`，用自有 xorshift32 生成 schema v1 的有限事件序列，
逐事件按 `ModelState` 计算期望并调用 `run_trace` 检查状态不变量。输入 seed 为 uint32，
0 使用固定内部初态 `0x6d2b79f5`；一次运行最多 64 个 seed，每条轨迹最多 48 个事件。
固定 64-seed 回归集合覆盖 17 种 schema 操作，以及 APPLIED、STALE、REJECTED、
RESOURCE_LIMIT 四种结果；非法事件样例包括陈旧代次、重复终局、重复租约释放和预算不足。

模型异常按异常类型与文本形成签名。缩减器以稳定的 ddmin 分块删除顺序和逐事件删除，
每次按参考模型重算 `expect`，只接受仍触发同一签名的候选。测试专用
`duplicate-terminal-callback` mutant 能把含无关操作的轨迹稳定缩为 OPEN、CANCEL 两个事件。
该 mutant 仅在单元测试传入的 `failure_probe` 中存在；普通 CLI、生产代码和组件适配器都不
启用它。

CLI 成功输出无时间戳的 JSON 探索摘要并返回 0；发现异常输出 seed、签名、原始轨迹和最小
轨迹，工作区写入 `original.json` / `minimized.json` 后返回 1；参数错误或工件 I/O 失败返回 2。
默认失败目录为 `build/test-tmp/rpc-v2-trace-explorer/`。现有 `build-test (gcc)` job 运行
单测和 64-seed 探索，并在失败时将该目录的工件保存 7 天；没有新增 job 或检查名。

## 本地验证

以下测试设置 `PYTHONDONTWRITEBYTECODE=1`，并将 `TMPDIR` 指向当前 worktree 的
`build/test-tmp/`：

| 验证 | 结果 |
|---|---:|
| `scripts/test_rpc_v2_trace_explorer.py -v` | 15/15 |
| `scripts/test_rpc_v2_model.py -v` | 28/28 |
| `scripts/test_rpc_v2_component_replay.py -v` | 11/11 |
| `scripts/rpc_v2_trace_explorer.py --seed 33047 --cases 64 --max-events 48` | 3072 个事件；摘要可重复 |
| TEST-B1–B4 Release 组件回放 | 4/4 `matched=true` |

以 seed 33047 为起点的 64-seed CLI 摘要统计：APPLIED 2454、REJECTED 410、
RESOURCE_LIMIT 49、STALE 159。
`git diff --check` 通过。实现只改 Python 测试工具、README、评估记录、既有 GCC 测试步骤及
版本号；生产 C、公开头、ABI、依赖和 Xmake 目标未改。完整 GCC Debug/Release 测试未重跑，
因为该切片没有生产 C 或构建目标改动。远端 CI 状态以此分支关联的 Draft PR 检查为准；
本地结果不用于推断远端通过状态。

## 语义边界与剩余工作

期望结果和轨迹不变量共用同一个参考模型，因此 TEST-C1 不能独立证明模型语义，也不证明
真实 Endpoint、Channel 或 Reactor 的随机故障行为。测试 mutant 只验证失败捕获和缩减链路，
不等于生产 C 故障注入。后续 TEST-C2 仍需定义并接入真实组件故障点；TEST-B 完整操作字母表、
真实网络/派发、TEST-D/E、#248 和 V2 整体验收也仍未完成。
