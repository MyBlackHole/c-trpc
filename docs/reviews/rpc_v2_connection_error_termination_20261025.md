# RPC V2 真实 Endpoint 连接级错误关闭回归

日期：2026-10-25。关联 #242/ERR-A。版本：`2.0.0-dev.47 -> 2.0.0-dev.48`。

## 覆盖范围

`test_connection_level_error_closes_all_calls` 使用真实 TCP、两端 Reactor、Channel、RPC Record Stream、
Endpoint、Router 与 Dispatch。测试建立两个调用，等待双方 OPEN 均进入服务端业务处理，然后直接向同一
客户端 TCP 连接发送一条头 CRC 错误的 TRP1 帧。

该帧的边界完整性无法验证，接收端必须退役整条物理连接。测试查询两个 Reactor 的连接代次状态，直到双方
连接都不再活动；两个客户端调用与两个服务端终局回调各仅发生一次，均为 `UNAVAILABLE`，服务端没有接受
成功 FINAL，双方 Channel 的活动网络流最终归零。服务端 Endpoint 启动时接管 Channel 回调，因此连接退役
通过 Reactor 状态观察，不依赖被替换的初始 Channel 观察回调。

## 验证

- GCC Release：完整 `test_rpc_record_endpoint_tcp` 通过，包含 `connection error closes all calls passed`。
- GCC TSan：完整 `test_rpc_record_endpoint_tcp` 通过，未报告数据竞争。
- `git diff --check` 通过。
- 只增加真实端到端回归、开发版本和记录；不修改生产 C、公开 API/ABI 或线协议。

## 边界与风险

本用例验证接收头 CRC 无效时整条连接关闭，并检查关闭后所有活动调用以失败终结，不覆盖其它线帧格式错误、
TLS 完整性错误、RPC 压缩解码/预算压力、RESET 交错或连接组 CONTROL/DATA 接受边界。生产运行时无性能影响；
测试额外创建两个活动调用并注入一个损坏帧。

回滚可撤销 dev.48 提交，恢复 `xmake.lua` 与 README 版本，移除此回归和记录，再运行 Release
`test_rpc_record_endpoint_tcp`。

## 验收标准

1. 两条已受理调用同时处于活动状态时，头 CRC 错误导致两端物理连接退出活动状态。
2. 两端调用各仅收到一次 `UNAVAILABLE` 终局，不接受成功 FINAL。
3. 双方 Channel 的活动网络流归零。
4. Release 与 TSan 下完整 `test_rpc_record_endpoint_tcp` 均通过。
