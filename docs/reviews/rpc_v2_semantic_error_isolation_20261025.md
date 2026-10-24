# RPC V2 真实 Endpoint 语义错误隔离回归

日期：2026-10-25。关联 #242/ERR-A。版本：`2.0.0-dev.46 -> 2.0.0-dev.47`。

## 覆盖范围

`test_semantic_record_error_isolation` 使用真实 TCP、两个 Reactor、Channel、RPC Record Stream、
Endpoint、Router 与 Dispatch。测试先正常建立一个请求调用并等待业务处理完合法 OPEN；随后绕过客户端
记录状态机，向该调用发送一条线格式正确、字段有效的重复 OPEN RPC 记录。传输帧边界和流 ID 均可信，
错误只在 RPC 记录顺序层出现。

生产接收路径将该调用以 `INVALID_ARGUMENT` 终结，业务端只观察到最初一次 OPEN；客户端与服务端终局
回调各一次，且双方 Channel 没有 DOWN。错误终局后再建立第二条调用，仍在同一 TCP 连接完成请求、响应
和 `OK` 终局，证明单调用语义错误未破坏同伴前进。

## 验证

- GCC Release：完整 `test_rpc_record_endpoint_tcp` 通过，包含 `semantic error isolation passed` 与目标总结果 `ok`。
- GCC TSan：完整 `test_rpc_record_endpoint_tcp` 通过，同样包含上述场景，未报告数据竞争。
- `git diff --check` 通过。
- 只增加真实端到端回归、开发版本和记录，不修改生产 C、公开 API/ABI 或线协议。

## 边界与风险

本用例只覆盖一个已准入流上的重复 OPEN 顺序错误，验证 `INVALID_ARGUMENT` 与同连接健康调用隔离。它不
覆盖所有业务拒绝/方法错误、RPC 压缩解码失败、本地预算拒绝、不可恢复线帧错误或连接组控制接受边界；
这些仍需 ERR-A 及 #242 其他验收项补齐。运行时性能无影响；测试增加一个错误调用与后续健康调用。

回滚可撤销 dev.47 提交，恢复 `xmake.lua` 与 README 版本并移除该用例及记录，再运行 Release
`test_rpc_record_endpoint_tcp`。

## 验收标准

1. 有效传输帧内的重复 OPEN 只以 `INVALID_ARGUMENT` 终结目标调用。
2. 业务端只收到一次初始 OPEN，双方终局回调各一次，连接保持 UP。
3. 错误后同一 TCP 连接上的新调用成功完成。
4. Release 与 TSan 下完整 `test_rpc_record_endpoint_tcp` 均通过。
