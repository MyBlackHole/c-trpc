# RPC V2 TEST-B3 槽复用后旧代次 DATA 组件回放

日期：2026-10-24。关联 GitHub #247 TEST-B；前置记录见
[TEST-B1 取消回放](rpc_v2_component_replay_20261024.md)和
[TEST-B2 Deadline 回放](rpc_v2_deadline_component_replay_20261024.md)。

## 本切片覆盖

新增 `component-stale-message-after-reuse-v1` 轨迹：第一条调用在工作中取消并完成退役；
同一模型槽以代次 1 重开后，将代次 0 的旧 DATA 送入新 Endpoint，再继续新调用的受控取消
与退役。轨迹由严格 JSON parser 和 L1 模型检查，组件适配器仅接受这条登记过的 17 事件序列。

C 夹具使用真实 `tr_rpc_record_endpoint` 和真实预算账本。它在同一 Channel stream slot 上
先用 generation 1 建立旧调用，再用 generation 2 建立新调用；在新调用已打开时直接送入旧
stream handle 的 DATA callback，断言该回调返回 RELEASE、接收计数不增加、新 Dispatch 句柄
不变。旧、新调用都等待受控工作完成，均只报告一次 CANCELLED，最终退役两次、无 FINAL 帧、
预算为零。旧终局回调在调用退役后重放并被 Endpoint 忽略。

## 本地验证

- TEST-A 状态模型：28/28；TEST-B 组件映射/输出测试：10/10。
- `test_rpc_record_endpoint_contract` 默认回归：35/35。
- GCC Debug 与 Release 全量 Xmake 测试各 90/90。
- Release 下 TEST-B1、B2、B3 回放均 `matched=true`；B3 的终局回调 2、忽略旧 DATA 1 次、
  迟到工作完成、FINAL 帧 0、退役 2、预算 0、quiescent。
- C 适配器仍只接受精确登记的 Cancel、Deadline 与旧代次事件序列；非法序列返回退出码 2。
- Python 与 Xmake 临时目录均在 worktree `build/test-tmp/`，没有在 `/tmp` 写入测试数据。

## 限制与状态

该切片验证 Endpoint 按完整 stream handle 查找槽位的行为，不验证真实 Channel 是否会产生
迟到 DATA，不创建线程或 TCP，也不覆盖生产 Dispatch 的并发回收。当前开发版本从
`2.0.0-dev.30` 升为 `2.0.0-dev.31`；第三条回放加入现有 `build-test (gcc)`，未新增 CI
job/check。Draft PR 与本分支远程 CI 结果以 #320 最新 head 为准。

TEST-B 字母表、TEST-C/D/E、#248 模块边界和 V2 整体验收仍未完成。
