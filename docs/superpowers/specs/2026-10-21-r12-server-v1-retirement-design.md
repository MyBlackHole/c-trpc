# R12：Server V1 退役设计

## 状态与基线

- 状态：R12 架构基线已批准；实现与本地验收完成，等待 GitHub CI 验证
- 实现提交：`2a5387311c5614b49c79056656b6ce2a994f708d`（R12-D1 → R12-D2）
- V2 基线：`bench/results/rpc-v2-20261021.csv`，schema 2 / `rpc_v2`，27 个 client-phase 行；源码与 release 二进制哈希见 CSV
- 依据：本会话已批准的 R12 架构基线，以及项目总账 [Issue #290](https://github.com/MyBlackHole/c-trpc/issues/290) 中的 [Server 依赖盘点](https://github.com/MyBlackHole/c-trpc/issues/290#issuecomment-6289353664)、[旧测试迁移判定](https://github.com/MyBlackHole/c-trpc/issues/290#issuecomment-6289444491)、[基本可用验收证据](https://github.com/MyBlackHole/c-trpc/issues/290#issuecomment-6289475856)、[诊断与基准边界](https://github.com/MyBlackHole/c-trpc/issues/290#issuecomment-6289504726)、[transport-only Reactor 约束](https://github.com/MyBlackHole/c-trpc/issues/290#issuecomment-6305710858)
- 实施基线：Draft PR [#288](https://github.com/MyBlackHole/c-trpc/pull/288) 的头提交 `225feb13297e4b6701d98ed018c6cc91dcb1db19`，树 `527fc3677b44a8e23bffaf5c63924fb68f3cd364`
- 目标：使 V2 Server 成为普通 RPC 服务的唯一实现，完成“基本可用”阶段所需的服务端构造、生命周期、统计、测试和真实负载入口收敛

R12 在 #288 的精确树上独立开发和评审。R12 不把 #288 合入 main，也不提前删除 R13/R14 仍依赖的底层 V1 RPC 核心。

## 现状与问题

当前 `src/server.c` 同时持有 V1 `tr_rpc_endpoint` 和 V2 `tr_rpc_record_endpoint` 路径。内部构造通过 `record_mode` 分流；`tr_server_create_transport()` 先调用 V1 调优构造，再把 `transport_only` 置为真。因此普通 V2 RPC Server、旧 V1 Server 和传输专用 Server 共用含有旧 RPC 状态的构造、对端接入、统计和销毁代码。

V2 RPC 已有冻结方法表、记录端点、共享任务执行器、语义统计以及 drain/detach 生命周期。连接组监听器仍由 Runtime 分片 0 的 Reactor 持有；传输专用 Server 不能因为没有 RPC 就删除该 Runtime/Reactor。旧构造、注册、调优和 V1 统计仍被 Server 测试及 `bench/bench_rpc.c` 使用。

## 范围

### R12 要完成

1. 用显式的内部 Server 模式区分 `V2_RPC` 与 `TRANSPORT_ONLY`；移除 Server V1 端点、方法表、注册和旧调优构造路径。
2. 让普通 RPC Server 只由公开 `tr_server_create(config, methods, count, out)` 构造；传输专用 Server 只由 `tr_server_create_transport()` 构造。
3. 让传输专用 Server 保留 Runtime/Reactor、Channel 和 ConnectionGroup 所需资源，但不分配 RPC 方法表、RPC Endpoint、RPC 任务执行器、Server RPC 消息池或分片重组池。
4. 将旧 Server facade 测试逐项迁移到公开 V2 API 或 transport-only API；保留 R13/R14 仍需的底层 V1 RPC/Executor/Endpoint 测试。
5. 将 `bench_rpc` 及其真实负载驱动迁到 V2 Server 方法表和调用 API；保留负载记账，删除仅由 V1 调优/统计产生的字段，并发布新的版本化基准结果。
6. 清除 R12 范围内的旧入口文档、测试依赖和 CI 源码引用，并增加 Server V1 facade 的源码与归档符号检查。

### 明确不做

- 不移除或重写低层 `tr_rpc_endpoint`、V1 RPC 协议/执行器等 R13/R14 依赖闭包。
- 不改写 V2 方法描述、消息语义、取消/期限、terminal、排空或错误合同；本 R12 是 Server 实现收敛。
- 不公开新的池、执行器或 Reactor 调优参数，不添加生产热路径计时，也不以旧 V1 基准结果冒充 V2 对比数据。
- 不改变 Client V2 门面或 R11 已完成的 Client 退役范围。

## 目标架构

### 单一 RPC 模式与构造

内部构造使用 `TR_SERVER_MODE_RPC_V2` 和 `TR_SERVER_MODE_TRANSPORT_ONLY` 两个明确模式。构造期间确定模式，禁止先构造另一种模式再在对象发布后设置布尔标志。

| 资源/行为 | `tr_server_create()`：V2 RPC | `tr_server_create_transport()`：仅传输 |
|---|---|---|
| Runtime 与每分片 Reactor | 保留 | 保留；ConnectionGroup `PipelineListener` 由分片 0 Reactor 持有 |
| Channel 与传输收发资源 | 保留 | 保留 |
| V2 冻结方法表、record Endpoint、RPC task executor | 创建 | 不创建 |
| V1 Endpoint、V1 方法表/注册器 | 不创建 | 不创建 |
| Server RPC message pool | 不创建 | 不创建 |
| 分片 reassembly pool | 创建并供 V2 Channel 使用 | 不创建；transport-only 连接组路径不提供 RPC 分片重组 |
| 监听、启动与排空 | `tr_server_listen()`、`tr_server_start()`、`tr_server_drain()` 按 V2 RPC 合同工作 | `tr_server_listen()` 返回 `TR_ERR_UNSUPPORTED`；配置 ConnectionGroup listener 后由 `tr_server_start()` 启动，连接组优雅排空使用 `tr_server_connection_group_begin_drain()` / `wait_drained()` |

`tr_server_create()` 仍在构造时校验并复制 V2 方法描述，方法参数指针的现有借用期限不变。`tr_server_create_transport()` 继续拒绝 RPC interceptor。两种模式均使用同一 Server 所有权和回滚合同：完整回滚后 `out` 为 NULL；回滚失败时把部分对象交给调用方，由 `tr_server_destroy()` 重试终止收敛。

### 对端接入、销毁与排空

每个入站对端均建立 Channel。`V2_RPC` 模式只建立 V2 record Endpoint，并关联该分片共享的 RPC task executor；`TRANSPORT_ONLY` 模式只建立 ConnectionGroup/Channel 所需状态。对端接入失败必须按创建顺序回滚，不能留下未归还的 Channel、slot 或 RPC 状态。

移除 V1 endpoint detach/finalizer 分支后，V2 endpoint detach、任务完成屏障、executor join、peer slot 回收和 `tr_server_destroy()` 的可重试所有权合同保持不变。transport-only 仍须关闭/销毁 PipelineListener、Channel、Runtime 和 Reactor。

补齐一个 V2 Channel destroy 故障注入用例：首次 `tr_server_destroy()` 在 Channel 销毁处失败时，Server/Channel 所有权仍明确留给调用方；第二次销毁能完成收敛；失败期间不得释放仍被引用的状态或复用 peer slot。故障注入只能作用于测试目标，不给生产库增加测试钩子。

### 统计与诊断

- 保留公开 `tr_server_get_rpc_semantic_stats()` 的 V2 语义统计及其既有外部线程/输出原子性合同。
- 保留现有 V2 可用的 Reactor、Channel、peer/retirement、reassembly 和共享 task executor 诊断；V2 endpoint 数量保留为 V2 record endpoint 计数。
- 删除只由 V1 `tr_rpc_endpoint_get_stats()` 提供的调用队列、ready/active/terminal 队列、排队/handler 直方图、V1 endpoint 计数/峰值及 V1 累计调用计数。
- 删除 V1 `rpc_message_pool` 容量、峰值、耗尽统计与其对应资源；保留 V2 Channel 仍使用的 reassembly pool 观察值。
- transport-only 没有 RPC executor 或 reassembly pool；统计中的 RPC 任务与重组池观察值返回零值，不查询未初始化对象。
- 诊断结构是仓库内部接口。移除字段时，同步迁移所有仓库消费者；不得保留恒为零的 V1 兼容字段。

## 测试与消费者迁移

| 现有消费者 | R12 处理及必须保留的证据 |
|---|---|
| `test_server_stats_retiring` | 迁至 V2 语义统计和 peer retirement；覆盖任务仍运行时断连、累计统计不清零、peer slot 复用及最终调用计数，不再读取 V1 Endpoint 队列/直方图。可复用 `test_server_record` 与 `test_rpc_v2_sdk` 的现有 V2 场景。 |
| `test_server_destroy_retry` | 移除 V1 Endpoint destroy 注入，新增前述 V2 Channel destroy 失败后重试覆盖；保留对象所有权与再次收敛断言。 |
| `test_server_create_oom` | 使用公开 V2 方法表构造；保留逐 allocation 失败遍历、回滚销毁失败时 `out` 所有权、文件描述符守恒。 |
| `test_facade_constructor_ownership` | 保留 Runtime start/stop/destroy 回滚所有权；V1 RPC pool 注入改为 V2 reassembly/record 资源边界或与 OOM 覆盖合并；ConnectionGroup 构造留在 transport facade 测试。 |
| `test_server_peer_rollback`、`test_server_drain_deadline`、`test_inbound_handshake_deadline` | 改由公开 V2 方法表构造；保留 owner 侧 close、失败重试、统一绝对 deadline 和迟到握手拒绝。已有 V2 注入只可替代完全等价的具体路径。 |
| `test_runtime_threads` | Server facade 创建/线程预算场景改用 V2 Server 或 `create_transport`。独立 Runtime/executor group 生命周期场景保留到 R13。 |
| `test_rpc_executor_group_drain` | Server drain/任务取消语义迁到 V2；独立 raw V1 executor group 覆盖保留到 R13。 |
| `test_rpc_message_ownership` | 核对 V2 SDK 已有大消息场景是否覆盖输入复制、回复所有权和取消，再移除重复 V1 facade 场景；底层 Endpoint 所有权覆盖继续保留到 R14。 |
| `test_transport` | 按测试函数拆分：Server V1 facade/统计/调优依赖迁到 V2 或既有 V2 覆盖；Channel/Endpoint/Executor 原语继续由 raw fixture 验证，按 R13/R14 依赖保留。 |
| `bench/bench_rpc.c`、`bench/run_rpc_bench.py`、`run_rpc_capacity.py`、`run_rpc_fanout.py`、`run_rpc_generator_slots.py`、`run_rpc_headroom.py`、`run_rpc_repeatability.py`、`run_rpc_scalability.py` 及相关 `test_rpc_*.py` | Server 使用公开 V2 方法和真实 V2 调用；保持尝试、受理、`TR_AGAIN`/错误、完成、状态分类、响应校验、流量类别及场景覆盖记账等式。保留现有 `tr_facade_limits` 公开限制维度（`max_streams`、`max_methods`、`max_calls`、`max_frame_payload_bytes`、`max_message_bytes`、`initial_window_bytes`、`window_update_threshold_bytes`）；移除 V1 调优入口、V1 RPC queue/handler 直方图及 V1 message pool 字段。 |

CI 的必需 check 名称及现有 Xmake 测试入口保持不变。对 `scripts/rpc_v1_inventory.py` 增加 `tr_server_create_with_tuning` 规则；在源码扫描和已构建 `libtrcore.a` 符号扫描中拒绝 `tr_server_legacy_*` 与旧 Server tuning 构造符号。检查仅针对 Server facade，不得全局禁止 `tr_rpc_endpoint_*` 等仍由 R13/R14 使用的底层符号。

## 文档同步

- `README.md` 的普通 RPC 使用和 TCP 门面说明指向 V2；仍需保留的 V1 线协议说明明确标成底层/历史内容，避免看起来像已安装 Server 门面的用法。
- `docs/rpc-v2-sdk.md` 移除“完整门面切换和 V1 删除属于后续迁移”的过时说明，明确普通 Server 只构造 V2。
- `docs/testing_strategy.md` 从 R11 过渡状态更新为 R12 后的消费者去向，并准确区分 V2 覆盖与 R13/R14 的 raw V1 核心测试。
- `docs/observability.md` 删除 `tr_server_create_with_tuning()` 示例及 V1 Server 统计口径；保留 V2 公共语义统计和仍有效的仓库内诊断边界。
- `docs/rpc_benchmark.md` 更新为 V2 真实负载、`schema: 2`、新运行参数和独立基线；旧 V1 CSV 保留为历史结果，不重写成 V2。
- 带日期的 review/audit 记录保持当时事实，不回写历史结论。

## 基准结果合同

V2 基准继续执行真实 TCP Server/Client；不使用 mock 请求或把测试 fixture 计为吞吐。`tr_server_create()` 的公共方法描述及 V2 请求/响应回调负责业务处理。客户端仍校验响应 payload 和调用终局，服务端正常 drain 后退出。

将当前 JSON `schema: 1` 升为 `schema: 2`，记录 `api_generation: "rpc_v2"`，并保留原有请求账本恒等式。保留当前 `tr_facade_limits` 已公开且由基准场景使用的限制维度；删除无法由 V2 API 或保留的 V2 内部诊断稳定提供的字段，不为保留旧列而构造假零值。服务端仍可报告已有的 peer、Channel、reassembly、task executor 与非新增计时的 Reactor 诊断。负载端可继续测端到端延迟；不得为 R12 增加新的生产热路径 clock sampling。

V1 与 V2 的方法实现和诊断口径不同，新 V2 schema 建立独立性能基线。保留 `bench/results/rpc-20260926.csv` 作为历史 V1 数据；新结果写入 `bench/results/rpc-v2-YYYYMMDD.csv` 或作为 CI artifact。报告中不得将旧 V1 与新 V2 记录合并、直接拼接或标注为同口径纵向对比。

## 验收标准

R12 只有同时满足以下条件才可视为完成：

1. 普通 Server facade 的生产代码没有 V1 endpoint、V1 方法表/注册器、`tr_server_legacy_*` 或 `tr_server_create_with_tuning` 构造分支。
2. V2 Server 与 transport-only Server 都从精确的构造模式创建；transport-only 保持 ConnectionGroup/Runtime/Reactor 工作，且不创建 RPC 专属资源。
3. R12 Server 测试全部经 V2 或 transport-only facade 运行；未迁移的 raw V1 测试有明确 R13/R14 所有权，不从现有 CI 静默删除。
4. 覆盖 V2 真 TCP 请求/响应、取消/期限、drain、peer retirement、构造回滚和 Channel destroy retry；V2 semantic stats 与调用账本一致。
5. `bench_rpc` 和真实负载 Python 套件使用 V2 Server/Client，所有记账/响应校验仍通过，新的 schema 和性能基线已落盘或由 CI artifact 生成。
6. Server facade 源码/归档符号扫描通过，低层 R13/R14 V1 符号仍允许存在。
7. `xmake test -v -j1` 全量通过；CI 中 benchmark schema/accounting 测试与真实 V2 benchmark smoke 通过；安装 SDK 的既有 V2 C/C++ 消费验证通过。

## 已知边界

R12 结束 Server facade 的 V1 生命周期，不代表 `src/rpc/rpc_endpoint.c`、通用 V1 Executor 或所有 V1 类型已删除。它们各自的依赖、测试和归档符号退役属于后续 R13/R14，不作为本次完成条件。
