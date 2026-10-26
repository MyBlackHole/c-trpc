# RPC V2 TEST-B2 截止后迟到工作组件回放记录

日期：2026-10-24。关联 GitHub #247 TEST-B；前置 TEST-B1 记录见
[取消后迟到工作回放](rpc_v2_component_replay_20261024.md)。

## 本切片覆盖

新增 `component-deadline-late-work-v1` 固定轨迹：`OPEN`、`ACCEPT`、`MESSAGE`、
`WORK_START`、`DEADLINE`、`WORK_DONE`、被拒绝的重复 `FINAL_STATUS`、`SLOT_RELEASE`。
轨迹由 TEST-A parser 与 L1 模型验证，再映射到既有 Endpoint 合同夹具。适配器仍只接受
显式登记的轨迹 ID 和完整事件顺序。

C 夹具向受控 Dispatch 替身报告 `TR_RPC_STATUS_DEADLINE_EXCEEDED`，并先推进真实
`tr_rpc_record_endpoint`，断言它向 Channel 替身发出 `TR_ERR_TIMEOUT` RESET，且工作仍未完成。
随后释放迟到工作，检查 Deadline 结果保持不变、终局回调仅一次、未接受 wire FINAL；再将
重复终局回调送入实际 Endpoint 回调并检查被忽略。关闭 Endpoint 后真实 memory-budget 账本为零。

组件适配只验证真实 Endpoint 对受控 Dispatch/时钟事件的响应，不创建真实线程或 TCP，也不
证明生产 deadline 计算/调度公平性。它与 TEST-B1 共同提供两条可回放组件交错，但不完成
TEST-B 的完整事件字母表，不代表 #247、#241、#242 或 V2 整体验收完成。

## 本地验证

- TEST-A 模型单测：28/28；组件映射/进程测试：9/9。
- `test_rpc_record_endpoint_contract` 默认回归：35/35。
- GCC Debug 全量 Xmake：构建通过、90/90；GCC Release：构建通过、90/90。
- Release 回放中 TEST-B1 与 TEST-B2 均为 `matched=true`，状态分别为 `CANCELLED`、
  `DEADLINE`；两条轨迹均为终局回调 1、工作完成、FINAL 帧 0、退役 1、预算 0、quiescent。
- C 适配器拒绝不在完整固定序列中的输入，退出码为 2。

Python 测试设置 `PYTHONDONTWRITEBYTECODE=1` 并将 `TMPDIR` 指向 worktree 的
`build/test-tmp/`；xmake 设置 `TMPDIR` 与 `XMAKE_TMPDIR`。没有在 `/tmp` 写入测试文件、
临时目录、日志或构建产物。

## CI 与状态

开发版本从 `2.0.0-dev.29` 升为 `2.0.0-dev.30`。CI 仍在现有 `build-test (gcc)` job 中运行，
新增第二条回放命令；未新增 job/check，也未改变生产源码、公开头、ABI、协议或依赖。

Draft PR [#319](https://github.com/MyBlackHole/c-trpc/pull/319) 以 TEST-B1 分支为 base，
实现 head `683c6eff5330b2f74bb4aa7c2fa631e075b1c39c` 的 14/14 GitHub 检查通过；PR 仍
Open/Draft、未合并。TEST-B1 的前置 Draft PR #318 仍 Open、未合并；本切片与 TEST-C/D/E、
#248 模块依赖门禁仍须分别验收。
