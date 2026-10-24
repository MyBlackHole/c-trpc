# RPC V2 双 DATA 成员连接组排空记录

## 范围

本记录对应 `2.0.0-dev.53` 与 GitHub #242/ERR-D。新增回归通过公开连接组门面构造一个 CONTROL 和两个 DATA 成员，直接验证多 DATA 成员在排空期间的准入、活动传输及最终关闭边界。

## 场景与断言

1. 服务端最多接纳两个 DATA 成员；客户端分别收到两个 DATA Offer 并完成附着。
2. 服务端为 Stream 5001、5002 发出 Transfer Ready。测试 wrapper 记录两个 DATA 帧实际使用的 `tr_conn_handle`，断言 Reactor 相同而 slot/generation 不同，证明它们经不同物理 DATA 连接发送。
3. 服务端和客户端开始排空后，新 DATA Offer 与 Transfer Ready 均返回 `TR_ERR_CLOSED`；两个活动传输未释放时，两端 drain 等待均超时。
4. 只释放 Stream 5001 后，统计仍有一个活动传输且服务端仍有两个 DATA 成员，两端继续未排空。
5. 释放 Stream 5002 后，客户端排空完成，但两条 DATA 成员仍连接；服务端仍未排空。客户端关闭组并带走两条 DATA 连接后，服务端 drain 才完成，最终组、连接、DATA 成员和活动传输计数均归零。

SDK 测试的 Client 清理 helper 也统一改为在 `WAIT_MS` 内重试 `TR_AGAIN`。公开契约允许销毁因未退出的活动消息任务返回 `TR_AGAIN`，此时对象所有权仍由调用方持有；非 `TR_AGAIN` 错误仍立即使测试失败。

## 验证

- GCC Release：完整 `test_connection_group_facade` 通过。
- GCC Release：完整 `test_rpc_v2_sdk` 通过，含 `stats-peer-retirement`。
- GCC TSan：完整 `test_connection_group_facade` 通过，无数据竞争报告。
- GCC TSan：完整 `test_rpc_v2_sdk` 通过，无数据竞争报告。
- `git diff --check` 通过。

## 未覆盖范围

本用例在 drain 前确认 `send_bytes_inflight == 0`，因此不证明发送中或压缩中的 DATA 工作如何与 drain 交错；也不覆盖 DATA 成员故障、重连、多个组并发，以及 ERR-D 的完整竞争矩阵。相关验收仍应继续推进。
