# RPC V2 路由拒绝与活动同伴隔离回归

日期：2026-10-25。关联 #242/ERR-A。版本：`2.0.0-dev.51 -> 2.0.0-dev.52`。

## 覆盖范围

`test_router_rejection_preserves_active_sibling` 使用真实 TCP、两个 Reactor、Channel、RPC Record Stream、
Endpoint、Router 与 Dispatch，分别运行未知方法与非法元数据两个场景。方法 1 同伴先完成 OPEN，发送 MESSAGE
后在业务 worker 内暂停。随后创建另一条目标调用：未知方法使用未注册的方法 ID，预期 `UNIMPLEMENTED`；非法元数据
在客户端记录发送边界后注入损坏字节，预期 `INVALID_ARGUMENT`。

两种拒绝均在业务 Dispatch 前完成。测试验证目标调用的错误 FINAL 被服务端接受且两端只终结一次，目标业务 OPEN/MESSAGE
计数均为零；同时，同伴 worker 仍活动、MESSAGE 尚未处理、两端无同伴终局，两个 Reactor 连接仍为
`TR_CONN_ACTIVE`。释放 gate 后，同伴处理完整请求并以 OK 唯一终结。

## 验证

- GCC Release：完整 `test_rpc_record_endpoint_tcp` 通过，包含未知方法、非法元数据及活动同伴恢复场景。
- GCC TSan：完整 `test_rpc_record_endpoint_tcp` 通过，未报告数据竞争。
- `git diff --check` 通过。
- 仅增加端到端回归、开发版本和记录，不修改生产 C、公开 API/ABI 或线协议。

## 边界与风险

本用例覆盖两种路由层调用错误与活动同伴的交错，证明未知方法和非法元数据都不影响已活动同伴。它不覆盖其他
RPC 语义字段错误、业务拒绝以外的鉴权分支、资源压力组合或连接组接受边界；ERR-A 完整生产分类和 ERR-B 至
ERR-E 仍待验收。运行库无性能影响；测试额外暂停一个业务 MESSAGE worker。

回滚可撤销 dev.52 提交，恢复 `xmake.lua` 与 README 版本并移除此测试和记录，再运行 Release
`test_rpc_record_endpoint_tcp`。

## 验收标准

1. 未知方法只终结目标流为 `UNIMPLEMENTED`，非法 OPEN 元数据只终结目标流为 `INVALID_ARGUMENT`。
2. 两种拒绝均不调用目标业务方法，均接受唯一错误 FINAL。
3. 拒绝期间同伴保持活动且连接 ACTIVE，释放 gate 后同伴以 OK 唯一终结。
4. GCC Release 与 TSan 下完整 `test_rpc_record_endpoint_tcp` 均通过。
