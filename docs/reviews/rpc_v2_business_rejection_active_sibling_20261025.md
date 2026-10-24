# RPC V2 业务拒绝与活动同伴隔离回归

日期：2026-10-25。关联 #242/ERR-A。版本：`2.0.0-dev.50 -> 2.0.0-dev.51`。

## 覆盖范围

`test_business_rejection_preserves_active_sibling` 使用真实 TCP、两个 Reactor、Channel、RPC Record Stream、
Endpoint、Router 与 Dispatch。方法 1 调用先完成 OPEN，随后发送 MESSAGE 并在业务 worker 内进入 gate，保持
该调用活动。之后启用方法 0 的业务拒绝，并建立目标调用；服务端业务 worker 对其以
`PERMISSION_DENIED` 结束调用。

测试等待拒绝调用在客户端和服务端各唯一终结，并验证服务端已接受错误 FINAL。此时方法 1 worker 仍活动、
其 MESSAGE 尚未处理完成、双方尚无同伴终局，且客户端和服务端 Reactor 连接均为 `TR_CONN_ACTIVE`。释放 gate
后，同伴继续处理 MESSAGE 与 HALF_CLOSE，并以 OK 唯一终结。

## 验证

- GCC Release：完整 `test_rpc_record_endpoint_tcp` 通过，包含业务拒绝活动同伴场景。
- GCC TSan：完整 `test_rpc_record_endpoint_tcp` 通过，未报告数据竞争。
- `git diff --check` 通过。
- 仅增加真实端到端回归、开发版本和记录，不修改生产 C、公开 API/ABI 或线协议。

## 边界与风险

本用例证明一种业务 `PERMISSION_DENIED` 终局在同连接已有活动同伴时只影响目标调用，并证明同伴可继续完成。
它不覆盖所有业务拒绝、方法不存在、调用级鉴权、压缩和资源错误的交错，也不代表 ERR-A 完整分类矩阵或
ERR-B 至 ERR-E 已验收。运行库没有性能变化；测试额外保持一个 MESSAGE worker 到拒绝流终结。

回滚可撤销 dev.51 提交，恢复 `xmake.lua` 与 README 版本并删除该回归和记录，再运行 Release
`test_rpc_record_endpoint_tcp`。

## 验收标准

1. 方法 0 的业务拒绝以 `PERMISSION_DENIED` 唯一终结，服务端成功接受一个错误 FINAL。
2. 错误期间已活动的方法 1 同伴不终结，业务 MESSAGE 保持暂停且两端连接仍为 ACTIVE。
3. 放行同伴后，其 MESSAGE、HALF_CLOSE 和两端 OK 终局均完成一次。
4. GCC Release 与 TSan 下完整 `test_rpc_record_endpoint_tcp` 均通过。
