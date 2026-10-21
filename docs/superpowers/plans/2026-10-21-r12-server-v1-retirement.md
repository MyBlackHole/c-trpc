# R12 Server V1 退役实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 使普通 RPC Server 只使用 V2，并让 transport-only、诊断、测试、基准、CI 和文档与该构造边界一致，达到“基本可用”验收要求。

**Architecture:** 在 `src/server.c` 中以构造时确定的 `TR_SERVER_MODE_RPC_V2` / `TR_SERVER_MODE_TRANSPORT_ONLY` 取代 V1/V2 布尔分流。V2 RPC 资源仅由公开 V2 Server 构造分配；transport-only 保留 Runtime、Reactor、Channel 与 ConnectionGroup 资源，不分配 RPC 专属对象。测试与真实 TCP 基准改用公开 V2 门面，低层 V1 RPC 核心保留给 R13/R14。

**Tech Stack:** C11、Xmake、V2 RPC SDK、Python 3 基准驱动、GitHub Actions、`nm` 静态归档符号检查。

**Spec:** `docs/superpowers/specs/2026-10-21-r12-server-v1-retirement-design.md`

## Global Constraints

- 实施基线为 Draft PR #288 的精确头提交 `225feb13297e4b6701d98ed018c6cc91dcb1db19`，树 `527fc3677b44a8e23bffaf5c63924fb68f3cd364`；R12 不把 #288 合入 main。
- 不移除或重写 R13/R14 依赖的 `tr_rpc_endpoint`、V1 RPC 协议及通用 V1 Executor 核心。
- 不改变 V2 方法描述、消息语义、取消/期限、terminal、排空或错误合同；不公开池、执行器或 Reactor 调优参数。
- 不为 R12 增加生产热路径时钟采样；端到端基准延迟仍可由负载端测量。
- 保持 CI 必需 check 名称、Xmake 测试目标和测试入口不变。
- 保留 `bench/results/rpc-20260926.csv` 历史 V1 数据，不把它改标为 V2 或与新 V2 基线合并。
- 新增或修改的文档、注释和测试说明使用简体中文；不重写带日期的 review/audit 历史记录。

---

## 文件与职责地图

| 文件 | R12 职责 |
|---|---|
| `src/server.c` | Server 模式、资源分配、入站 peer、排空、销毁与统计实现；移除 Server facade V1 分支。 |
| `src/facade_legacy_internal.h`、`src/facade_tuning_internal.h`、`src/facade_diagnostics_internal.h` | 删除 Server V1 私有构造/注册入口和失效诊断字段；保留仍被 Client 使用的通用 tuning 类型。 |
| `tests/test_server_destroy_retry.c` | V2 Channel 销毁失败后的 Server 所有权保留与重试。 |
| `tests/server_v2_test_fixture.h` | 生命周期测试共享的最小合法 V2 方法表构造。 |
| `tests/test_server_create_oom.c`、`tests/test_facade_constructor_ownership.c` | V2 构造逐分配失败、Runtime 回滚和资源守恒。 |
| `tests/test_server_stats_retiring.c`、`tests/test_server_peer_rollback.c`、`tests/test_server_drain_deadline.c`、`tests/test_inbound_handshake_deadline.c` | Server lifecycle/统计场景改为 V2 facade。 |
| `tests/test_runtime_threads.c`、`tests/test_rpc_executor_group_drain.c`、`tests/test_rpc_message_ownership.c`、`tests/test_transport.c` | 迁移 Server facade 消费者；保留有 R13/R14 归属的低层 V1 原语覆盖。 |
| `bench/bench_rpc.c`、`bench/run_rpc_*.py`、`bench/test_rpc_*.py` | 使用真实 V2 Server/Client，输出并验证 schema 2 与 `api_generation: "rpc_v2"`。 |
| `scripts/rpc_v1_inventory.py`、`scripts/test_rpc_v1_inventory.py`、`.github/workflows/ci.yml` | 拒绝 Server V1 facade 源码/归档定义，同时允许低层 V1 RPC 核心。 |
| `README.md`、`docs/rpc-v2-sdk.md`、`docs/testing_strategy.md`、`docs/observability.md`、`docs/rpc_benchmark.md` | 同步 R12 后门面、测试归属、诊断和基准合同。 |
| `bench/results/rpc-v2-20261021.csv` | 保存独立 V2 基线；不混合历史 V1 CSV。 |

## 实施任务

> 执行依赖顺序：先完成 Task 4、Task 5 的 Server facade 消费者迁移，再完成 Task 2、Task 3 的生产收敛与统计删除，避免删除旧内部入口后暂时失去可编译测试集。

### Task 1：先锁定 V2 Channel destroy retry 回归

**Files:**
- Modify: `tests/test_server_destroy_retry.c`
- Modify: `xmake.lua` 中 `test_server_destroy_retry` 的链接包装配置

**Interfaces:** `tr_server_create(config, methods, method_count, out)`、`tr_server_destroy(server)`、`tr_server_get_stats(server, out)`、测试目标内的 `__wrap_tr_channel_destroy()`。

- [x] 将 Server 构造改为公开 V2 构造：定义一个 RAW 一元 `tr_rpc_v2_method` 作为接入所需的方法表，然后调用 `tr_server_create(&config, &method, 1U, &server)`；该用例不发起 RPC 调用。
- [x] 删除 `tr_rpc_endpoint_destroy` 故障变量、包装函数、计数器和链接 `--wrap`；只在 `tr_channel_destroy` 第一次调用时注入 `TR_ERR_SYS`。
- [x] 保留真实 TCP accept，等待 `stats.peers_current == 1U`；重试 V2 Endpoint 收敛阶段返回的 `TR_AGAIN`，直至 Channel destroy 注入 `TR_ERR_SYS`；断言失败后统计读取返回 `TR_ERR_STATE`，第二次 destroy 返回 `TR_OK`。
- [x] 运行 `xmake build test_server_destroy_retry && xmake run test_server_destroy_retry`，确认失败注入只命中该测试二进制且第二次销毁成功。
- [x] 提交该测试与 target 配置，提交说明使用中文并记录失败路径、根因、方案、影响范围和验证结果。

### Task 2：用构造时 Server mode 合并生产路径

**Files:**
- Modify: `src/server.c`
- Modify: `src/facade_legacy_internal.h`
- Modify: `src/facade_tuning_internal.h`
- Modify: `src/server_record_internal.h`

**Interfaces:** `tr_server_create(config, methods, count, out)` 是唯一 RPC Server facade 构造；`tr_server_create_transport(config, out)` 是唯一 transport-only 构造；内部构造显式接收 `enum tr_server_mode`。

- [x] 定义 `enum tr_server_mode { TR_SERVER_MODE_RPC_V2, TR_SERVER_MODE_TRANSPORT_ONLY }`；`tr_server_create_internal()` 在任何资源创建前收到确定的 mode，不再用 `record == NULL` 或之后写入的布尔值推断模式。
- [x] 让 `tr_server_create()` 冻结 V2 方法表并以 `TR_SERVER_MODE_RPC_V2` 调用内部构造；让 `tr_server_create_transport()` 直接以 `TR_SERVER_MODE_TRANSPORT_ONLY` 调用内部构造，保留 interceptor 拒绝检查。
- [x] 将 Server RPC 方法存储、`tr_rpc_endpoint`、旧注册辅助函数、RPC message pool、V1 RPC executor、V1 peer finalizer/detach 逻辑从 `src/server.c` 删除；peer 只持有 Channel 和 V2 record Endpoint。
- [x] 为 `RPC_V2` 创建 V2 task executor、冻结方法表及 reassembly pool；为 `TRANSPORT_ONLY` 不创建方法表、V2 Endpoint、task executor、RPC message pool 或 reassembly pool。两种模式均保留 Runtime/Reactor 和 Channel 传输资源。
- [x] transport-only Channel 配置不提供 `reassembly_pool`；普通 `tr_server_listen()` 对该模式仍返回 `TR_ERR_UNSUPPORTED`，ConnectionGroup listener 继续绑定分片 0 Reactor 并可 start/drain/destroy。
- [x] 将所有 `record_mode` / `transport_only` 条件改为 mode 比较，按 mode 校验 shard budget、初始化/销毁资源，并确保构造回滚失败时 `out` 保留唯一可重试 owner。
- [x] 删除 `tr_server_create_with_tuning()`、`tr_server_legacy_create()` 和 `tr_server_legacy_register_*()` 的声明与实现；仅在 Client 仍使用时保留 `struct tr_facade_tuning` 和 client tuning 字段。
- [x] 运行 `xmake build trcore test_connection_group_facade test_server_destroy_retry`，再运行这三个目标；预期两种 Server 模式均可构造销毁，transport-only 连接组测试通过。

### Task 3：删除 V1 Server 诊断口径并验证 transport-only 零值

**Files:**
- Modify: `src/server.c`
- Modify: `src/facade_diagnostics_internal.h`
- Modify: `tests/test_connection_group_facade.c`
- Modify: 仍引用移除字段的 `tests/*.c` 与 `bench/*.py`

**Interfaces:** 保留 `tr_server_get_stats()` 仓库内快照和公开 `tr_server_get_rpc_semantic_stats()`；transport-only 对 RPC task 与 reassembly observation 返回零值。

- [x] 从 `tr_server_stats` 移除 V1 `rpc_message_pool`、V1 endpoint 队列/ready/active/terminal 队列、handler/queue 直方图、V1 endpoint 数及 V1 调用累计字段；保留 peer、Reactor、Channel、reassembly、V2 task 与 semantic stats。
- [x] `tr_server_get_stats()` 只聚合 `record_rpc`、V2 task executor 与 semantic counters；mode 为 transport-only 时不查询未初始化 RPC/reassembly 对象，相关统计结构保持 `memset(..., 0, ...)` 的零值。
- [x] 在 `test_connection_group_facade` 增加断言：transport-only `tr_server_listen()` 返回 `TR_ERR_UNSUPPORTED`；listener 经 `tr_server_start()` 工作；RPC task/reassembly diagnostic 为零；ConnectionGroup 统计仍可读。
- [x] 按编译器错误逐个迁移所有移除字段的测试/基准消费者，禁止用恒为零的 V1 兼容字段恢复编译。
- [x] 运行 `xmake build test_connection_group_facade test_server_record test_server_stats_retiring`；目标测试通过。上述词法扫描只剩 `rpc_message_pool_count` 的共享 tuning/预算下限名，不再存在 Server message-pool 对象、统计或 V1 queue/handler 诊断路径。

### Task 4：迁移 Server facade 生命周期和构造测试

**Files:**
- Modify: `tests/test_server_create_oom.c`
- Modify: `tests/test_facade_constructor_ownership.c`
- Modify: `tests/test_server_peer_rollback.c`
- Modify: `tests/test_server_drain_deadline.c`
- Modify: `tests/test_inbound_handshake_deadline.c`
- Modify: `tests/test_runtime_threads.c`

**Interfaces:** 所有 Server fixture 均由公开 V2 `tr_server_create()` 或公开 `tr_server_create_transport()` 构造；方法描述使用 `tr_rpc_v2_method`。

- [x] `test_server_create_oom` 使用合法 V2 方法表和公开构造，继续逐个注入 `calloc` / `malloc` 失败，检查 `out`、重试 destroy 和 fd 数守恒；移除旧 RPC pool tuning 设置。
- [x] `test_facade_constructor_ownership` 将 Server 构造改为 V2 方法表，将旧 dynamic RPC pool 故障改为 reassembly pool 构造故障；保留 Runtime stop/destroy 回滚失败后 owner 保留与重试，Client 部分保持现状。
- [x] `test_server_peer_rollback`、`test_server_drain_deadline`、`test_inbound_handshake_deadline` 通过 V2 Server fixture 启动监听；保留 owner 侧 close、失败重试、统一绝对 deadline、迟到握手拒绝和 fd 守恒断言。
- [x] `test_runtime_threads` 将 Server facade 线程预算和构造用例改用公开 V2 方法；移除不再公开的 tuning/worker 数量拒绝场景；独立 raw Runtime/Executor 生命周期用例继续保留。
- [x] 分别运行 `xmake build test_server_create_oom test_facade_constructor_ownership test_server_peer_rollback test_server_drain_deadline test_inbound_handshake_deadline test_runtime_threads` 和各目标；确认注入路径仍被实际命中，且无需任何旧 Server facade 构造。

### Task 5：迁移统计、executor drain、消息所有权与混合传输测试

**Files:**
- Modify: `tests/test_server_stats_retiring.c`
- Modify: `tests/test_rpc_executor_group_drain.c`
- Modify: `tests/test_rpc_message_ownership.c`
- Modify: `tests/test_transport.c`
- Modify: `xmake.lua` 中这些目标的旧 V1 fixture 依赖

**Interfaces:** V2 客户端使用 `tr_client_create()`、`tr_client_unary_call()` / `tr_client_call_start()` 和 `tr_rpc_v2_call_*()`；V2 Server 工作回调使用方法描述中的 work/terminal 回调。

- [x] 将 `test_server_stats_retiring` 的 Server、Client 和调用改为 V2；gate 阻塞 V2 work，客户端断连后断言 peer 保持 current 且退出 ready、semantic `calls_started` 不清零；释放 gate 后断言 peer 回收累计增长、终局状态计数收敛，并通过后续循环连接证明槽位可复用。`xmake build test_server_stats_retiring && xmake run test_server_stats_retiring` 通过；去掉该目标的 V1 client fixture 依赖。
- [x] 将 Server destroy 等待已受理 work 的场景改为 V2：阻塞 unary OPEN work，首次 destroy 返回 `TR_AGAIN`，释放 work 后重试销毁并观察一次 terminal。独立 raw V1 Executor group 生命周期与通用 TaskExecutor 互操作覆盖保留，源码注明 R13；`xmake build test_rpc_executor_group_drain && xmake run test_rpc_executor_group_drain` 通过。
- [x] 对照 `test_rpc_v2_sdk` 已有的大消息、输入/元数据复制与取消覆盖；移除重复的 V1 Server facade 大消息所有权用例，只保留底层 buffer-pool 所有权测试。`xmake build test_rpc_message_ownership test_rpc_v2_sdk && xmake run test_rpc_message_ownership && xmake run test_rpc_v2_sdk` 通过。
- [x] 在 `test_transport.c` 按测试函数边界移除旧 Server facade 的 unary/reuseport/tuning/executor/message-retention/reaping 场景；对应语义覆盖由 V2 SDK、Server lifecycle、runtime-thread 测试承担。TCP_NODELAY 策略保留并改用 V2 Server/Client；raw Channel、Endpoint、Executor、协议用例继续留在原目标，入口保留不变。`xmake build test_transport && xmake run test_transport` 通过。
- [x] 删除 `test_server_stats_retiring`、`test_rpc_executor_group_drain` 和 `test_rpc_message_ownership` 对 `rpc_v1_test_client_fixture.c` 的依赖；`test_transport` 仍保留该 fixture，因为其 raw V1 Client/Endpoint 核心测试继续覆盖 R13/R14。
- [x] 运行 Task 5 四个目标及 SDK 对照：`test_server_stats_retiring`、`test_rpc_executor_group_drain`、`test_rpc_message_ownership`、`test_transport` 和 `test_rpc_v2_sdk` 均通过。

### Task 6：迁移真实 TCP 基准服务端与基准结果 schema

**Files:**
- Modify: `bench/bench_rpc.c`
- Modify: `xmake.lua` 的 `bench_rpc` 源文件依赖
- Modify: `bench/run_rpc_bench.py`
- Modify: `bench/test_rpc_bench.py`
- Create: `bench/export_rpc_v2_csv.py` 与 `bench/test_rpc_csv_export.py`
- Create: `bench/results/rpc-v2-20261021.csv`

**Interfaces:** benchmark server 使用 `tr_server_create()` 和 V2 methods；客户端通过公开 V2 Client/Call API 发送真实 TCP 调用；JSON 顶层包含 `schema: 2` 与 `api_generation: "rpc_v2"`。

- [x] 将 `bench_rpc` Server 入口改为一元 V2 method 描述与公开 V2 构造，业务 callback 原样回显请求；保留真实监听、正常 `tr_server_drain()` 和退出状态。
- [x] 将 Client load generator 改为公开 V2 Client/Call API，保留请求 attempt/accepted/`TR_AGAIN`/错误/completed/status/payload 校验、small/bulk/slow 分类和端到端 latency 测量。
- [x] 删除 `--rpc-message-pool`、V1 tuning、V1 queue/histogram/handler/message-pool 输出；保留 V2 支持的 `tr_facade_limits` 维度、peer/Channel/reassembly/task/已有 Reactor 诊断，不生成假零列。
- [x] 把 JSON schema 升为 2，写入 `api_generation: "rpc_v2"`；在 `validate_phase()` 和 `validate_server_exit()` 中拒绝旧 schema、错误 generation、漏计账、不匹配分类和无效响应。
- [x] 更新 `bench/test_rpc_bench.py` 的有效/负向 fixture，运行 `python3 -m unittest bench.test_rpc_bench`；所有计账恒等式和 schema 错误路径通过。
- [x] 用 release 构建运行一轮 CI smoke workload，把 V2 结果保存为独立 `bench/results/rpc-v2-20261021.csv`；确认文件只含 schema 2 / `rpc_v2`，README/报告不把 2026-09-26 V1 记录与之拼接。三轮真实 TCP 基准 21/21 case 通过；CSV 有 27 个 client-phase 行，均绑定源码 `2a5387311c5614b49c79056656b6ce2a994f708d` 和二进制 SHA-256 `cb837bc10100c2faf528e4ffc82a8c10bf1424939d4bc4f13e11a9169e831e25`。

### Task 7：迁移负载驱动、容量套件和版本化基线校验

**Files:**
- Modify: `bench/run_rpc_capacity.py`
- Modify: `bench/run_rpc_fanout.py`
- Modify: `bench/run_rpc_generator_slots.py`
- Modify: `bench/run_rpc_headroom.py`
- Modify: `bench/run_rpc_repeatability.py`
- Modify: `bench/run_rpc_scalability.py`
- Modify: 对应 `bench/test_rpc_*.py`
- Modify: `bench/results/rpc-v2-20261021.csv`

- [x] 从每个 Python driver 的 CLI、命令拼装和结果行移除 `rpc_message_pool_count`、`--rpc-message-pool` 及 V1 tuning 字段；保留 `max_streams`、`max_methods`、`max_calls`、`max_frame_payload_bytes`、`max_message_bytes`、`initial_window_bytes`、`window_update_threshold_bytes`。
- [x] 更新每个 suite 的 C 可执行参数拼装，确认启动的是 Task 6 的 V2 `bench_rpc` server/client，并沿用真实请求、分类账本、压力/恢复覆盖和负载端延迟测量。
- [x] 将所有 `test_rpc_*.py` fixture 和 schema 检查改为 `schema == 2` 及 `api_generation == "rpc_v2"`；删除 V1 pool/handler histogram 断言，不降低现有受理/完成/错误/响应一致性校验。
- [x] 运行 `python3 -m unittest discover -s bench -p 'test_rpc_*.py'`，并依次运行 `python3 bench/run_rpc_bench.py --binary build/linux/x86_64/release/bench_rpc --smoke --output build/r12-bench-smoke.jsonl`、`python3 bench/run_rpc_capacity.py --binary build/linux/x86_64/release/bench_rpc --smoke --output build/r12-capacity-smoke.jsonl`、`python3 bench/run_rpc_scalability.py --binary build/linux/x86_64/release/bench_rpc --smoke --output build/r12-scalability-smoke.jsonl`、`python3 bench/run_rpc_headroom.py --binary build/linux/x86_64/release/bench_rpc --smoke --output build/r12-headroom-smoke.jsonl`；输出字段无 V1 pool/queue 口径，所有 scenario accounting 通过。
- [x] 更新并重跑 `bench/results/rpc-v2-20261021.csv`，同一文件内所有行使用相同 V2 schema 与生成器版本；使用 `bench/export_rpc_v2_csv.py` 验证 schema、generation、源二进制哈希、分类账本与 Server calls 收敛，导出 27 行。

### Task 8：增加 facade 源码与已安装归档符号门禁

**Files:**
- Modify: `scripts/rpc_v1_inventory.py`
- Modify: `scripts/test_rpc_v1_inventory.py`
- Modify: `.github/workflows/ci.yml`

- [x] 在 inventory `LEGACY` matcher 中加入 `tr_server_create_with_tuning`，覆盖 `tr_server_legacy_*`，保留现有低层 V1 RPC 名称规则。
- [x] 为脚本自测增加负向样例：Server tuning 与 legacy 构造源码/归档符号都失败；`tr_rpc_endpoint_*` 仍被归类为 R13/R14 低层符号且不被 Server facade deny 单独误报。
- [x] 更新 CI 源码扫描 deny 列表，使 `tr_server_create_with_tuning` 与 `tr_server_legacy_create` / `tr_server_legacy_register_*` 出现即失败。
- [x] 在 SDK 静态库已构建的 job 中，对 `nm --defined-only "$prefix/lib/libtrcore.a"` 的输出添加精确 Server facade deny assertion；不要 deny `tr_rpc_endpoint_*` 或其他 R13/R14 底层符号。
- [x] 运行 `python3 scripts/test_rpc_v1_inventory.py`、`python3 scripts/rpc_v1_inventory.py --root . --output build/r12-inventory.json --deny tr_server_create_with_tuning tr_server_legacy_create`，并检查 `git diff -- .github/workflows/ci.yml`；确认源码/归档 deny 生效且必需 check 名称保持不变。

### Task 9：同步 R12 后用户文档

**Files:**
- Modify: `README.md`
- Modify: `docs/rpc-v2-sdk.md`
- Modify: `docs/testing_strategy.md`
- Modify: `docs/observability.md`
- Modify: `docs/rpc_benchmark.md`

- [x] README 删除“尚未迁移的 Server 测试使用 legacy 门面”的现状说明；普通 Server/TCP 连接组说明指向 V2/transport-only，保留的 V1 线协议材料明确标记为底层/历史用途。
- [x] `rpc-v2-sdk.md` 明确普通 `tr_server_create()` 只构造 V2，删除“完整门面切换和 V1 删除属于后续迁移”的过时句子。
- [x] `testing_strategy.md` 将 R11 过渡状态更新为 R12 后的 V2 facade 覆盖，并把仍留在 CI 的 raw Endpoint/Executor V1 测试明确归属 R13/R14。
- [x] `observability.md` 删除 `tr_server_create_with_tuning()` 示例和 V1 Server queue/message-pool 统计口径，说明 V2 semantic stats 及 transport-only diagnostic 零值边界。
- [x] `rpc_benchmark.md` 记录 V2 真实负载、schema 2、`api_generation`、基准参数与独立 CSV；旧 V1 CSV 仅保留历史说明。不得改写带日期审查记录。
- [x] 运行 `git diff --check` 和 `rg -n 'tr_server_create_with_tuning|tr_server_legacy_|schema: 1|V1.*bench_rpc|rpc_message_pool' README.md docs/rpc-v2-sdk.md docs/testing_strategy.md docs/observability.md docs/rpc_benchmark.md`；逐项审阅仍命中的历史说明，确认它们清楚标注旧版本而不描述当前入口。

### Task 10：全量验证、记录证据并更新 GitHub 分支

**Files:** R12 分支全部改动；Issue #290 实施记录。

- [x] 从干净的提交工作树执行 `xmake f -c -y -m debug --toolchain=gcc` 与 `xmake test -v -j1`；全量 85/85 通过，耗时 60.305 秒。
- [x] 执行 RPC Python suite（65/65）、inventory 自测（7/7）、源码 inventory 扫描及四组 V2 smoke；release smoke 均通过，scalability 对限速慢处理场景明确归因 `load_generator_drop`。正式基线 schema 为 2、generation 为 `rpc_v2`，CSV 为 `bench/results/rpc-v2-20261021.csv`。
- [x] 执行 release `xmake build trcore bench_rpc test_rpc_v2_sdk test_server_record` 和安装 SDK GCC/Clang C11、G++/Clang++ C++11 四编译器消费验证；安装归档 deny 未发现 Server V1 facade，raw V1 RPC 符号仍允许存在。
- [x] 执行 `git diff --check`、Server facade 词法扫描、归档符号扫描和 `git status --short`；输出与临时文件均在仓库 `build/`，未写入 `/tmp`。
- [ ] 更新 R12 spec 状态和 Issue #290 实施记录，附计划、提交、测试、基准证据及剩余 R13/R14 边界；推送 `docs/rpc-v1-retirement-r12-20261021` 分支，保留 PR #288 为 Draft 且不合入 main。

## Spec 覆盖自检

- 显式构造模式、资源边界、ConnectionGroup Reactor、V2 peer detach/retry：Task 1–3。
- V2 Channel destroy retry 与所有权、构造 OOM、Runtime 回滚、peer retirement、drain/deadline：Task 1、4、5。
- 统计字段移除、semantic stats、transport-only 零值：Task 3、5。
- `bench_rpc`、所有真实 workload driver、schema 2、独立 V2 baseline：Task 6、7。
- 源码及 archive deny，允许 raw V1 核心：Task 8。
- README、SDK、测试策略、observability、benchmark 文档：Task 9。
- 全量 Xmake、CI benchmark/schema、SDK 安装消费和 GitHub 证据：Task 10。

## 自检结果

- 覆盖 spec 的六类验收及所有明确排除项；R13/R14 raw V1 tests 只保留低层核心归属。
- 未使用 `TODO`、`TBD` 或“写对应测试”占位步骤；任务均列明文件、接口和验证命令。
- V2 method 表以公开 `tr_rpc_v2_method` 描述，Server mode 由构造参数确定；transport-only 不分配 RPC/reassembly 资源。
