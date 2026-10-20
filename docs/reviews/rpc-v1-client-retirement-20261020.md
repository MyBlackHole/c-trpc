# 移除 Client V1 Endpoint 门面依赖（R11）

## 范围

基线为 #288 精确合并提交 `d638f265ec74f77d610b74ef95577ed7db31411f`。
本切片从 `tr_client` 移除 V1 `tr_rpc_endpoint` 的成员、构造、连接分支、调用、统计
与销毁；删除 Client legacy 构造/注册/调用入口和内部调优构造。公开普通 Client
只创建 V2。`tr_client_create_transport()` 保留连接组传输能力，并且不分配 V1 RPC
消息池或重组池。

Server V1、`rpc.c`、`rpc_wire`、RAW Codec 与独立协议测试继续留在范围外，进入 R12
及后续依赖清理。V1 Client 行为不通过兼容包装映射到 V2。

## 调用者去向

| 消费者 | 迁移结果 |
| --- | --- |
| `test_client_connect_deadline` | 使用公开 V2 `tr_client_create()` 与真实 connect 路径。 |
| `test_client_connect_rollback` | 改为注入 V2 Record Client 创建失败，验证 Channel 回滚、重试和销毁所有权。 |
| `test_facade_constructor_ownership`、`test_runtime_threads` | 构造、线程启动与回滚覆盖改走公开 V2 Client；删去只适用于 Client tuning 的旧配置断言。 |
| `test_inbound_handshake_deadline` | V1 Server 入站测试用 raw V1 Endpoint 作为普通协议对端；连接组子场景用公开 transport-only 工厂。 |
| `test_transport`、`test_server_stats_retiring` | 仍验证 V1 Server 生命周期时，通过测试专用 raw peer 直接持有 Reactor、Channel、Endpoint 与池。 |
| `test_rpc_message_ownership`、`test_rpc_executor_group_drain` | 继续覆盖仍保留的 V1 Server/Endpoint 与 Executor Group；通过测试专用 raw V1 Endpoint 取代旧 Client facade。R10 的 V2 大消息和多调用 drain 测试独立验证 V2 合同。 |
| `bench_rpc` | 保留旧 V1 基准，通过同一 raw peer 直接调用 Endpoint；文档明确此基准不能代表 V2 性能。 |

`rpc_v1_test_client_fixture.*` 仅服务测试与 `bench_rpc`，显式命名协议边界，不安装
到 SDK，也不定义 `tr_client_legacy_*` 兼容符号。

## 防回归与验证

源码扫描检查 Client 实现和三个 Client 内部头文件不再引用 V1 Endpoint 类型/函数、legacy
Client 名称或 V1 Endpoint 统计类型；CI `--deny` 同时拒绝已删除 facade 符号在源码、
Xmake 与静态库全局符号中恢复。保留稳定 V2 Client 的真实 TCP、构造回滚、期限、
排空/销毁及安装消费覆盖。

R11 首轮候选曾因 `facade_diagnostics_internal.h` 删除对 `rpc_internal.h` 的间接包含，
导致 `test_server_record.c` 的 linker-wrap 声明看不到 `tr_rpc_executor_group`，GCC、
Clang、ASan 与 TSan 构建失败。测试现显式包含 `rpc_internal.h`。审查还确认两组旧
V1 测试仍覆盖生产 Server V1 依赖：它们保留并通过 raw Endpoint fixture 移除 Client
facade 调用；inventory 正反例新增裸 `tr_rpc_endpoint` 和 `tr_rpc_executor_group` 类型名。

此切片完成后，旧 Server Endpoint 仍是生产构建的有效依赖。R11 不代表 RPC V1 状态机
已完全退出，也不核销 #288 或 main 的集成状态。
