# RPC V2 TEST-B4 成功终局后 RESET 组件回放

日期：2026-10-24。关联 GitHub #247 TEST-B；前置记录见
[TEST-B1 取消回放](rpc_v2_component_replay_20261024.md)、
[TEST-B2 Deadline 回放](rpc_v2_deadline_component_replay_20261024.md)和
[TEST-B3 旧代次回放](rpc_v2_stale_generation_component_replay_20261024.md)。

## 本切片覆盖

新增 `component-final-reset-after-accepted-v1` 固定轨迹：OPEN、ACCEPT、请求 MESSAGE 与
HALF_CLOSE、工作开始/完成、成功 FINAL_STATUS，随后注入 RESET 并确认旧终局不可覆盖。
L1 模型要求 FINAL_STATUS=OK 被接受、后到 RESET 被拒绝、调用槽最终释放。适配器只接受
登记好的 9 个事件、字段和代次。

C 夹具使用真实 `tr_rpc_record_endpoint` 和真实预算账本，受控 Dispatch 在工作完成时报告
已接受的成功终局。Endpoint 必须向 Channel 接受 CLOSE、报告 OK 一次；后续 RESET 到达后，
Endpoint 仍保留 OK 与 `final_accepted`，不重复报告，然后退役调用。最终观测为终局回调 1、
最终帧 1、退役 1、预算 0、quiescent。

## 本地验证

- TEST-A 状态模型：28/28；TEST-B 组件映射/输出测试：11/11。
- GCC Debug/Release 全量 Xmake 测试各 90/90；Endpoint 夹具默认回归各 35/35。
- B1/B2/B3/B4 四条回放在 Debug 与 Release 均 `matched=true`。B4 观测为 OK、终局回调 1、
  最终帧 1、退役 1、预算 0、quiescent。
- 无效组件事件序列的 C 入口退出码为 2；PR #320 的 14/14 检查已通过，PR 仍为 Open/Draft、未合并。
- Python/Xmake 临时目录固定在 worktree `build/test-tmp/`，不在 `/tmp` 写入测试文件或产物。

## 限制与状态

Endpoint 夹具中的 Channel、Dispatch、Reactor 和 Executor 均是受控替身；它验证先到终局不被
后到 RESET 覆盖，不证明真实 TCP 竞态或生产派发调度。开发版本从 `2.0.0-dev.31` 升为
`2.0.0-dev.32`。第四条回放接入既有 `build-test (gcc)` job，未新增 CI job/check。

Draft PR [#321](https://github.com/MyBlackHole/c-trpc/pull/321) 的实现提交
`93502c100d27b6c0a6cbbc07fe46be9329b75f3d` 已通过 14/14 GitHub 检查，PR 仍为
Open/Draft、未合并。TEST-B 字母表、TEST-C/D/E、#248 模块边界和 V2 整体验收仍未完成。
