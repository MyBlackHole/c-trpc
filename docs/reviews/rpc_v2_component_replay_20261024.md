# RPC V2 TEST-B1 确定性组件轨迹回放记录

日期：2026-10-24。关联 GitHub #247 TEST-B1；前置 TEST-A 记录见
[状态模型与轨迹回放](rpc_v2_state_model_replay_20261024.md)，设计与执行步骤见
[设计 spec](../superpowers/specs/2026-10-24-v2-component-replay-design.md) 和
[实施计划](../superpowers/plans/2026-10-24-v2-component-replay.md)。

## 本切片覆盖

轨迹 `component-cancel-late-work-v1` 先由 TEST-A 严格 JSON parser 和 L1 模型完整校验，
再映射为一个固定事件序列：`OPEN`、`ACCEPT`、`MESSAGE`、`WORK_START`、`CANCEL`、
`WORK_DONE`、`FINAL_STATUS`、`SLOT_RELEASE`。适配器拒绝任何其他 trace ID、seed、字段、
事件顺序、槽代次或结果。

L2 复用 `test_rpc_record_endpoint_contract`，链接实际 `src/rpc/rpc_record_endpoint.c` 和真实
memory-budget 原语。Reactor、Channel、Dispatch 与 Executor 为可控替身；手动时钟和 tick 固定
事件顺序。取消后的工作完成会进入真实 Endpoint 完成回调；FINAL_STATUS 事件会将重复终局
回调再次送入真实 Endpoint，检查已有终局不变且未发送 wire FINAL。C 侧只接受唯一完整操作串，
不会解析 JSON 或实现另一份 Endpoint 状态机。

这条轨迹证明单一取消/迟到工作交错在此组件边界下可重放。它不覆盖真实 TCP、生产 Dispatch
调度、真实 Reactor/Channel 并发、随机交错、故障矩阵或性能；不代表 #247、#241、#242 或 V2
整体验收完成。

## 本地验证

在 Python 3.14.8 下：

- `scripts/test_rpc_v2_model.py -v`：28/28 通过。
- `scripts/test_rpc_v2_component_replay.py -v`：8/8 通过，覆盖固定映射、seed/事件拒绝、
  观察字段差异、非零 C 退出、坏/重复 JSON 和槽代次/预算终态。
- Release `test_rpc_record_endpoint_contract` 默认路径：35/35 通过。
- `scripts/rpc_v2_component_replay.py --binary build/linux/x86_64/release/test_rpc_record_endpoint_contract`：
  `matched=true`，实际结果为 `CANCELLED`、终局回调 1、工作完成、FINAL 帧 0、退役 1、预算
  0、`quiescent=true`。空事件项的 C CLI 输入以退出码 2 拒绝。
- GCC Debug：全量构建通过，Xmake 90/90。
- GCC Release：全量构建通过，Xmake 90/90。

Python 测试关闭 bytecode 缓存，临时目录通过 `TMPDIR` 指向该 worktree 的 `build/test-tmp/`；
xmake 亦设置 `TMPDIR` 和 `XMAKE_TMPDIR`。未在 `/tmp` 写入测试文件、临时目录、日志或构建产物。

## CI 与版本

开发版本从 `2.0.0-dev.28` 升为 `2.0.0-dev.29`，只改变测试/维护资产，不改生产源码、公开头、
SDK、ABI、线协议或依赖。新增步骤放在既有 `build-test (gcc)` 作业中，位于已有 Debug/Release
Xmake 全量测试后；没有新增作业或重命名稳定 check。

TEST-A 前置 Draft [PR #317](https://github.com/MyBlackHole/c-trpc/pull/317) 的 head
`d51984ffb0435bebd75984502f3fa2af902db995` 已有 14/14 检查通过，当前仍 Open/Draft、未合并。
TEST-B1 Draft [PR #318](https://github.com/MyBlackHole/c-trpc/pull/318) 的实现提交
`f3fc0abb2766fc24bb6c737cd9f03d0702f4343c` 首轮 **14/14 GitHub 检查全部通过**，包括 GCC/Clang
全量构建与测试、ASan/UBSan、TSan、静态分析、RPC smoke、安装/Make、CRC32C 可移植路径和
新增/修改行格式门禁。PR 仍为 Open/Draft、尚未合并。后续只登记本验证记录的提交不改变实现；
不得以该 PR 的局部通过结果宣称 #247 或 V2 整体验收完成。
