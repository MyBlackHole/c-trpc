# R13：Runtime 与旧 V1 RPC Executor 解耦设计

## 状态与依据

- 状态：范围提案，尚未批准代码实施；本文与实施计划用于审阅。
- 基线：R12 head `2b4c360f6f620f4bc0360114030f29b34504b1f9`，PR #288 仍为 Draft，未合入 main。
- 依赖盘点：[R13 Runtime 旧 RPC Executor 依赖盘点](../../reviews/rpc-v1-runtime-executor-inventory-20261021.md)。
- 总账：[Issue #290](https://github.com/MyBlackHole/c-trpc/issues/290)。

## 问题

R12 已让普通 Client/Server facade 使用 V2，但 Runtime 内部仍保留可选的 `rpc_executor` 配置。`runtime.c` 经 `legacy_rpc_execution.c` 建立旧 `tr_rpc_executor_group`，并把它纳入 Runtime 启动、停止、销毁、失败回滚和线程预算。当前 V2 facade 不需要这组执行器；它们由 V1 Endpoint 的 `rpc.c` 实现管理。Runtime 适配对象只剩仓库内测试使用。

## 方案比较

1. **移除 Runtime 适配层，保留 Endpoint 执行组至 R14（推荐）**：删除 Runtime 私有配置、组 getter 和 `legacy_rpc_execution` 适配模块；保留 `rpc.c` 的 Endpoint/Executor Group 依赖闭包及其测试，直到 R14 一起退役。变更边界清晰，避免改写即将整体删除的 V1 调度器。
2. **现在拆出独立 V1 Executor 模块**：将 `rpc.c` 中 Endpoint 调度与通用 Group 生命周期分离，再移除 Runtime 调用。这样短期边界更细，但需要重构旧代码，R14 仍会删除这套实现。
3. **R13 一次性删除 Endpoint、Wire 和 Executor 全部 V1 核心**：跨度覆盖 public `tr/rpc.h` 中共享与 V1 类型、raw 测试、fixture、Wire/Codec 和静态库链接闭包，超出 R13 已确认的 Runtime 清理范围。

采用方案 1。归档扫描显示 `legacy_rpc_execution.c.o` 对 `rpc.c.o` 的 Executor Group 符号有五个未解析引用，而 `rpc.c` 自身实现 V1 Endpoint 调度并定义 Group；不能把 Runtime 依赖移除误报为整个 Executor 已删除。V2 的 `tr_rpc_endpoint_result` 等类型还与 V1 Endpoint 前缀重叠，因此禁用规则必须按符号精确匹配。

## 范围

### R13 完成

1. 从内部 Runtime 配置中移除 `tr_runtime_rpc_executor_config` 与 `rpc_executor` 字段。
2. 从 Runtime shard 移除 `legacy_rpc_execution` 状态、创建/启动/停止/销毁路径、配置验证及 `tr_runtime_shard_rpc_executor()` getter。
3. 删除 `src/runtime/legacy_rpc_execution.c`、对应头文件及 Xmake 库源引用。
4. Runtime 只按自身实际拥有的 Reactor、消息服务、连接/对端和内存预算资源启动、回滚与销毁；保留生命周期独占门禁及失败可重试合同。
5. 更新 Runtime 线程测试的线程记账，使每个普通分片只计数其实际 Reactor/已启用消息服务工作线程；移除只针对 Runtime 旧组的配置和 join-failure 用例。
6. 增加精确源码/归档门禁，拒绝 Runtime 旧组 getter、配置类型与适配器符号恢复。
7. 更新 Runtime 架构和测试策略文档，并记录 R13 验收证据。

### R13 保留

- `src/rpc/rpc.c` 的 V1 Endpoint 与内部 Executor Group 实现；Endpoint 仍通过它执行旧协议工作。
- `tr_rpc_executor_group_*`、Endpoint、Wire、Codec 的源码和静态库符号；R13 不增加这些名称的全局 deny。
- `test_rpc_executor_group_drain` 的 raw group 生命周期测试、`test_complex_constructor_faults` 的 group 构造故障覆盖，以及 `test_runtime_threads` 的独立 V1 Endpoint 创建/销毁失败重试覆盖；它们随 Endpoint 依赖闭包归 R14。
- `test_server_record.c` 针对 V2 Server 不创建旧 group 的测试期观测（若现有断言仍有价值）。
- 安装 SDK 的公开类型与签名。R13 不修改 `include/tr/rpc.h`、`include/tr/rpc_v2.h` 或 `include/tr/rpc_codec.h`。

## 生命周期合同

- `tr_runtime_start()` 保持先预检所有分片，再启动各 Reactor/消息服务；启动失败仍按逆序停止已启动分片，失败结果仍保留为可重试状态。
- `tr_runtime_stop()` 与 `tr_runtime_destroy()` 保持当前 Runtime 生命周期串行门禁；移除旧组后不改变 Reactor、消息服务、监听器、peer event source 与预算的所有权顺序。
- 默认分片启动线程数由 Reactor 和实际启用的消息服务决定，不再可由旧 `rpc_executor` 配置额外增加 V1 Group 线程。
- 此项仅移除 Runtime 的私有 V1 集成，不改变 V2 TaskExecutor 配置或工作调度。

## 验收标准

1. `src/runtime`、`xmake.lua` 与归档不再包含 `legacy_rpc_execution`、`tr_runtime_shard_rpc_executor` 或旧 Runtime executor config 标识。
2. `rpc.c.o` 仍定义旧 Endpoint/Executor Group 符号；这证明 R13 的 deny 边界没有误伤 R14 留项。
3. `test_runtime`、`test_runtime_threads` 的剩余 Runtime 场景覆盖 Reactor/消息服务启停、失败回滚、join retry、并发控制序列化及精确线程数；旧 Endpoint join/retry 测试仍运行。
4. `test_rpc_executor_group_drain`、`test_complex_constructor_faults` 仍通过，直到 R14 删除其实现闭包。
5. GCC debug 全量 Xmake 测试、RPC inventory 自测、源码/归档精确 deny 和 GCC release 归档检查通过。
6. 无公开 SDK 改动；CI 安装消费检查保持既有入口和必需检查名称。
7. `git diff --check` 通过；所有测试输出、清单和缓存留在仓库 `build/`，不写入 `/tmp`。

## 风险与限制

- 线程计数断言依赖测试自身对 `pthread_create/join` 的局部包装；更新期望值时要按每个注入场景逐条重算，不能仅修改最终总数。
- `nm` 对象引用与词法扫描不能证明跨线程调度/所有权正确；Runtime 重试、消息服务及 Reactor 覆盖不能删减。
- R13 不测量性能提升。默认 V2 facade 已不配置旧组，主要结果是删除内部死分支与显式旧资源配置，不宣称吞吐变化。
