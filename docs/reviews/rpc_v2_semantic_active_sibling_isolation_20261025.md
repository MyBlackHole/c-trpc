# RPC V2 活动同伴流语义错误隔离回归

日期：2026-10-25。关联 #242/ERR-A。版本：`2.0.0-dev.49 -> 2.0.0-dev.50`。

## 覆盖范围

`test_semantic_record_error_isolation` 使用真实 TCP、两个 Reactor、Channel、RPC Record Stream、Endpoint、
Router 与 Dispatch。两个调用先完成合法 OPEN；随后同伴调用发送 MESSAGE，并在真实业务 worker 内进入 gate，
保持调用活动且不处理完 MESSAGE。客户端绕过自己的记录状态机，向另一个调用发送线格式和字段均有效的重复
OPEN。该记录只违反目标调用的 RPC 记录顺序。

测试等待错误调用以 `INVALID_ARGUMENT` 唯一终结后，验证暂停中的同伴没有收到终局、业务 worker 仍活动、
MESSAGE 处理尚未完成，客户端和服务端的 Reactor 连接仍为 `TR_CONN_ACTIVE`。然后释放 gate，同伴处理完
MESSAGE 与 HALF_CLOSE，并以 OK 唯一终结。最后再建立一条新调用，验证错误后同一 TCP 连接仍接受新调用。
服务端 Endpoint 启动时接管 Channel 事件回调，因此服务端连接状态使用 Reactor 查询验证，不能依赖 fixture
原有 Channel 事件计数器。

## 验证

- GCC Release：完整 `test_rpc_record_endpoint_tcp` 通过，目标回归及整套目标均通过。
- GCC TSan：完整 `test_rpc_record_endpoint_tcp` 通过，未报告数据竞争。
- `git diff --check` 通过。
- 仅增加真实端到端回归、开发版本和记录，不修改生产 C、公开 API/ABI 或线协议。

## 边界与风险

本用例证明重复 OPEN 导致的单调用 `INVALID_ARGUMENT` 不会终结另一个已经进入业务处理的同伴流，并证明同伴
可在错误之后完成及错误后仍可接纳新调用。它只覆盖 ERR-A 的一个错误映射与一个活动同伴交错；其他业务拒绝、
方法错误、RESET、信用和资源组合，以及 ERR-B 至 ERR-E 仍需分别验证。运行时性能无影响；测试额外保持一个业务
MESSAGE 暂停至错误流终结。

回滚可撤销 dev.50 提交，恢复 `xmake.lua` 与 README 版本并移除此用例和记录，再运行 Release
`test_rpc_record_endpoint_tcp`。

## 验收标准

1. 重复 OPEN 只以 `INVALID_ARGUMENT` 终结目标调用，业务端只收到其初始 OPEN。
2. 错误期间已进入 worker 的同伴保持活动且不收到终局，两个物理连接保持 ACTIVE。
3. 放行同伴后其业务处理及两端 OK 终局各完成一次；错误后新调用也成功完成。
4. GCC Release 与 TSan 下完整 `test_rpc_record_endpoint_tcp` 均通过。
