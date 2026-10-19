# RPC V1/V2 依赖清点与安全删除门禁（阶段 3.1）

固定基线：`934b2e67f9967882e787d182aa016e1e357ef56b`（PR #281 的已验收提交）。本轮仅做静态源码清点，不修改生产代码，不声称完成全部符号引用图、链接闭包、运行时覆盖或 CI 回归。

## 已核实的入口事实

- `include/tr/client.h`、`include/tr/server.h`、`include/tr/rpc_v2.h`：普通工厂 `tr_client_create(config, methods, count, out)`、`tr_server_create(config, methods, count, out)` 明确仅构建 V2；连接组单独使用 `*_create_transport()`，不回退 V1。
- `src/client.c` 的公共构造函数构造 V2，但该文件仍保留 `tr_client_legacy_create`、旧方法注册/调用，以及 `client->rpc` / `record_mode` / `record_sdk_mode` 等双轨字段和控制分支。
- `src/server.c` 公共构造函数同样构造 V2，但保留旧方法注册、`peer->rpc`、端点销毁/统计、`record_mode` 分支。
- `src/facade_legacy_internal.h` 仍声明内部 V1 工厂、注册、一元及流调用接口；不能因为公开头不暴露这些符号就推断无内部联系。
- `src/rpc/rpc.c` 包含旧 Call、执行器组、deadline 和 Endpoint 实现；`src/rpc/rpc_internal.h` 是其内部契约。
- `src/runtime/runtime.c` **仍直接依赖旧执行器组**，其创建、启动、停止、销毁分别调用 `tr_rpc_executor_group_create/start/stop/destroy_checked`，并使用 `can_start`。因此目前严禁直接删除整个 `rpc.c` 或执行器组。
- `src/runtime/runtime_internal.h` 仍声明旧执行器组指针和 Endpoint 指针。
- `xmake.lua` 仍将 `src/rpc/rpc.c` 编入构建；测试链接配置包含旧 Endpoint/执行器包装和导出边界断言。

## 分层依赖图（已直接核查的引用，不代表全量调用图）

```text
公开 Client/Server 构造  -> V2 记录 Client/Server 路径
src/client.c             -> 旧 Endpoint（遗留内部入口/分支）
src/server.c             -> 旧 Endpoint（遗留对端路径/统计/回收）
src/runtime/runtime.c    -> 旧 RPC executor group（活跃生命周期）
src/runtime/runtime_internal.h -> 旧 executor group / Endpoint 类型
xmake.lua                -> src/rpc/rpc.c（生产编译及测试链接）
旧故障测试               -> Endpoint/执行器组链接包装
```

## 删除批次与硬门禁

1. **先做完整引用及构建闭包扫描**：逐个符号列出定义、全部调用点、公共或私有消费者、运行时代码/测试代码/基准及生成配置；包含静态/共享库和 C/C++ 安装消费。单靠本清单不得断言“不可达”。
2. **剥离 facade 的旧 API**：仅在证明无生产消费者后，按小提交移除 `tr_*_legacy_*` 及对应分支；保留销毁失败重试、所有者排空及部分构造失败契约。检查并维护当前 V2、transport-only 两种入口。
3. **解除 Runtime 对旧执行器组的依赖**：必须首先确认 V2 工作域与分片线程预算的所有权；不得直接删除执行器组从而让 Runtime 的 start/stop/destroy 失去真实资源管理。
4. **再移除旧 Endpoint、Call、deadline、队列**：每次改动后检查静态链接引用、头文件封闭性、导出符号与链接脚本；旧故障测试迁移为等价 V2 所有权与异常回滚测试，禁止只删除测试。
5. **最后移除旧编译目标及私有头**：确认生产构建、所有测试目标、安装消费与分析器基线均对应同一个提交。

## 否决条件

- 仍有 Runtime 或 V2 生产符号链接旧实现，或只有头文件/文本 grep 而没有编译链接证据。
- 删除了旧故障测试但尚无覆盖等价 V2 异常路径的测试。
- 放宽告警基线、超时、资源上限、Sanitizer、静态分析器或构建矩阵来通过门禁。
- 没有验证停止后拒绝、启动/销毁交错、部分构造回滚、任务回收、终局唯一性和计数守恒。

## 当前验收边界

本轮代码静态审查发现**真实结构性阻塞：Runtime 仍引用旧 RPC 执行器组**；不因此推断有运行时错误。此前 PR #281 在该基线下的 CI 通过仅证明当时树可测试，不是删除旧代码后仍通过的证据。阶段 3.1 尚欠完整符号/构建扫描，阶段 3.2—3.7 均未据此宣告完成。
