# RPC V2 TEST-C3 Endpoint RESET 先于终局帧重试

日期：2026-10-25。关联 #247 TEST-C；接续 TEST-C2，版本 `2.0.0-dev.35`。

## 交付范围

新增固定 schema v1 轨迹 `component-final-send-reset-before-retry-v1.json`。Endpoint 的最终错误状态帧第一次
发送得到 `TR_AGAIN` 后，在允许下一次重试前注入远端 RESET。C 夹具只接受预先登记的九事件序列，并以单个
owner turn 逐步泵送；RESET 后允许公平轮转最多 16 个 owner turn 完成回调与槽退役。验收要求发送尝试仍为一次、
最终帧未被接受、终局回调和退役各一次、最终状态为 `UNIMPLEMENTED`、静默且预算归零。

Python 适配器拒绝未知轨迹与事件顺序，并比较固定模型和真实 `src/rpc/rpc_record_endpoint.c` 的受控 Endpoint
观测。Dispatch、Channel、Reactor、Executor 与传输仍是受控替身；这个切片不覆盖任意生产故障时序、真实 TCP/派发，
也不替代 TEST-C、TEST-D、TEST-E 的完整验收。

变更限于 Endpoint 契约测试、Python 测试/适配器、新固定轨迹、既有 GCC 回放步骤、README、工具策略/剩余工作记录、
本证据文档、既有 CI job 的进程诊断步骤和版本号 dev.34→dev.35。没有修改生产 `src/`、公开 `include/`、ABI、协议、
依赖、Xmake target 或 job/check 名称。
按用户此前要求移除新增/修改源码行格式 CI 工作流；格式工具自测和翻译目录校验继续在既有 GCC 构建作业运行，本地
改动行格式检查仍可选执行。

## 验证

测试临时目录位于 worktree `build/test-tmp/`，并设置 `PYTHONDONTWRITEBYTECODE=1`：

| 验证 | 结果 |
|---|---:|
| C3 前置组件映射自测 | 新增测试在 RED 阶段按预期因 trace/profile/观测映射缺失而失败（实现记录） |
| TEST-B/C 组件映射自测 | 19/19 |
| L1 状态模型自测 | 28/28 |
| TEST-C1 explorer 自测 | 15/15 |
| 固定 seed 33047 探索 | 64 cases、3072 events、无模型失败 |
| Endpoint 契约目标 | Debug 35/35；Release 35/35；ASan+UBSan 35/35 |
| B1–B4、C2、C3 Release 组件回放 | 六条轨迹均 `matched=true`；C3 为一次发送、最终帧 0、回调 1、退役 1、预算 0 |
| 工具翻译目录与回归 | `compile_tool_catalog.py --check` 通过；翻译测试 10/10 |
| 格式工具回归 | 固定 clang-format 18.1.8 自测 22/22；源码 dry-run 与 YAML 解析、`git diff --check` 通过 |

## 分支与远端检查

分支 `feat/v2-test-c2-endpoint-backpressure-247` 对应 Draft PR
[#323](https://github.com/MyBlackHole/c-trpc/pull/323)，base 为
`feat/v2-test-c1-deterministic-shrinker-247`；PR 未合并。旧 head
`db4070c381c87dac0af0a5440ff9d9627bf36a40` 的 14 项 GitHub 检查为 11 项成功、3 项取消。
旧 head 的 GCC 全量构建/测试和 GCC TSan RPC smoke 的测试命令与制品上传均完成，随后日志停在 runner 的
`Cleaning up orphan processes` 并超时；旧日志没有残留 PID。新增/修改源码行格式工作流因 10 分钟超时且零步骤
而取消，已按用户要求移除。

C3 head `a858dd21fb5c3225cb4bf8b53aa5edc9ba0953d6` 的 Draft PR 远端检查为 13/13 成功（8 项 CI、5 项 RPC
smoke）。GCC 远端 Debug/Release 各 90/90，格式工具自测 22/22，C3 回放 `matched=true`。GCC 全量 job 和 GCC
TSan smoke 的收尾快照均未发现 `xmake`、测试二进制、`bench_rpc` 或 Python harness 残留；两个 job 都正常结束。
旧超时在新 head 未复现，现有证据倾向于当时 hosted runner 的清理异常，但旧日志缺少 PID，无法据此证明具体
历史根因。检查结果见 [CI 运行](https://github.com/MyBlackHole/c-trpc/actions/runs/40115911176) 和
[RPC smoke 运行](https://github.com/MyBlackHole/c-trpc/actions/runs/40115911209)。

本切片只验证一条固定 Endpoint 竞态窗口。通用生产 C 故障时序、TEST-B 完整事件字母表、真实网络/派发、TEST-D
平台矩阵、TEST-E 分层诊断、40k 容量性能，以及 #248 与 #239 总体验收仍待完成；当前不能宣称 RPC V2 已完成。
