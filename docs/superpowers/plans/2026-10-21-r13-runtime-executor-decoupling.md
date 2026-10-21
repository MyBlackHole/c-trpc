# R13 Runtime 旧 RPC Executor 解耦实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 删除 Runtime 对旧 V1 RPC Executor Group 的配置与生命周期依赖，同时保留仍被 raw V1 Endpoint 使用的执行器实现，供 R14 一起退役。

**Architecture:** Runtime 只拥有 Reactor、消息服务、对端资源和内存预算，不再通过适配模块创建或控制旧 Executor Group。`rpc.c` 中的旧 Endpoint/Executor Group 暂留；它的测试、线协议及 Codec 依赖不在 R13 删除范围。

**Tech Stack:** C11、POSIX pthread、Xmake、Python inventory 脚本、GNU `nm`。

**Spec:** `docs/superpowers/specs/2026-10-21-r13-runtime-executor-decoupling-design.md`

## 全局约束

- 不改安装 SDK 的公开头文件、API/ABI 或 V2 TaskExecutor 行为。
- 保持 Runtime/Reactor/消息服务的停止、回滚、所有权保留和可重试销毁合同。
- 不全局 deny `tr_rpc_executor_group_*`、`tr_rpc_endpoint_*`、V1 Wire 或 Codec；这些仍属于 R14 闭包。
- 保留 `test_rpc_executor_group_drain`、V1 Endpoint 线程故障测试与旧组构造故障覆盖，直到 R14 删除实现闭包。
- 测试、日志、缓存和产物只写仓库 `build/`，不写入 `/tmp`。
- 文档、注释和提交说明使用简体中文，CI 必需检查名称不变。

---

### Task 1：先固定 Runtime 线程计数与旧配置测试所有权

**Files:**
- Modify: `tests/test_runtime.c`
- Modify: `tests/test_runtime_threads.c`
- Inspect only: `tests/test_task_executor.c`, `tests/test_rpc_executor_group_drain.c`

**Interfaces:**
- Runtime 线程场景只计数 Reactor 与实际启用的 MessageService 工作线程。
- V1 Endpoint 独立启动/销毁测试仍使用 `tr_rpc_endpoint_create_with_executor_group()`，归 R14。
- raw Group 直接生命周期测试仍留在 `test_rpc_executor_group_drain`，归 R14。

- [x] 先把 `test_runtime_threads.c` 多分片场景的线程期望从 7 个线程改为 3 个 Reactor 线程，同时暂时保留非零旧 `rpc_executor` 配置作为回归输入。
- [x] 运行 `xmake build test_runtime_threads && xmake run test_runtime_threads`，确认当前实现因额外 4 个 V1 worker 导致新断言失败；记录实际观测值，不把失败当成通过。
- [x] 保留 `test_runtime.c` 的旧 group 所有权测试及 `test_runtime_threads.c` 的旧 group join-failure 用例到 Task 2，以便按实际移除的配置/API 同步删除。
- [x] 确认 Runtime Reactor/MessageService 生命周期测试及 raw V1 Endpoint join/retry 测试仍在原目标中；不得因共用 pthread wrapper 一并删除。

### Task 2：删除 Runtime Executor Group 适配与配置

**Files:**
- Modify: `src/runtime/runtime_internal.h`
- Modify: `src/runtime/runtime.c`
- Delete: `src/runtime/legacy_rpc_execution.c`
- Delete: `src/runtime/legacy_rpc_execution.h`
- Modify: `xmake.lua`

**Interfaces:**
- 删除 `struct tr_runtime_rpc_executor_config`、`tr_runtime_shard_config.rpc_executor` 与 `tr_runtime_shard_rpc_executor()`。
- 删除 shard 的 legacy executor 字段、配置校验函数、创建/启动/停止/销毁调用。
- 不改变 `tr_runtime_config` 的 shard 与 Reactor/MessageService 管理接口。

- [x] 从 Runtime shard config、shard 状态及内部声明删除旧 executor 类型/字段/getter。
- [x] 从 `runtime.c` 删除适配头引用、executor config validation、创建分支、can-start 分支及生命周期控制调用。
- [x] 删除 `test_runtime.c` 的 `test_runtime_shard_rpc_executor_ownership` 与注册项；删除 `test_runtime_threads.c` 的 Runtime group join-failure 用例和所有旧配置赋值，保留旧组执行器自身测试在 raw V1 测试目标中。
- [x] 更新 `test_runtime_without_legacy_execution` 注释与断言，不再调用将删除的 getter；保留单 Reactor 创建、停止和 join 的计数检查。
- [x] 保留 Runtime lifecycle lock、Reactor 先预检/启动/逆序回滚、MessageService 取消与 reaping、budget 收敛的既有顺序。
- [x] 从 `trcore` 源文件列表移除 `src/runtime/legacy_rpc_execution.c`。
- [x] 构建 `test_runtime`、`test_runtime_threads`、`test_server_record`；运行前两个 Runtime 测试，确认更新后的线程数及重试断言通过。

### Task 3：重算多分片与故障注入边界

**Files:**
- Modify: `tests/test_runtime_threads.c`
- Modify if needed: `tests/test_runtime.c`

**Interfaces:** 保留现有 `pthread_create/join` test-only wrapper；不增加生产测试钩子。

- [x] 调整多分片正常启停的创建/join 期望，只计数每个 Reactor 和配置启用的 MessageService worker。
- [x] 调整多分片 start rollback 与 join failure 的注入序号，逐项确认失败落在 Reactor/MessageService 路径而非已删除的 group 创建/回收路径。
- [x] 用 `test_runtime_threads` 验证创建失败、回滚失败后 owner 保留、再次 destroy 收敛，以及跨线程 start/stop/destroy 串行化。
- [x] 保留并运行独立 raw Endpoint join-failure 与 create rollback 测试；不得将它们因共享 pthread wrapper 而一起删除。

### Task 4：增加 Runtime 旧符号源码与归档门禁

**Files:**
- Modify: `scripts/rpc_v1_inventory.py`
- Modify: `scripts/test_rpc_v1_inventory.py`
- Modify: `.github/workflows/ci.yml`
- Preserve: `tests/test_rpc_executor_group_drain.c`, `tests/test_complex_constructor_faults.c`

**Interfaces:**
- 新的 deny 精确命中 `tr_runtime_shard_rpc_executor`、`tr_runtime_rpc_executor_config` 与 `tr_runtime_legacy_rpc_execution_*`。
- `tr_rpc_executor_group_*` 继续被 inventory 追踪，但不进入 R13 deny。

- [x] 扩展词法扫描器识别 Runtime 旧适配函数与配置类型；新增正/负例证明这些名字被拒绝而 `tr_rpc_executor_group_create`、V2 Endpoint 结果类型仍可存在。
- [x] 在 CI 的 RPC inventory deny 列表中加入所有 Runtime 适配器导出符号与 getter/config token。
- [x] 构建 release `trcore` 后，用归档扫描证明 Runtime 旧符号消失，并显式确认 `tr_rpc_executor_group_*` 仍定义在 `rpc.c.o`。
- [x] 运行 `PYTHONDONTWRITEBYTECODE=1 python3 scripts/test_rpc_v1_inventory.py`；测试临时目录必须使用脚本既有的仓库内 `build/r12-inventory-tests/`。

### Task 5：同步架构与测试策略

**Files:**
- Modify: `docs/architecture/01-runtime.md`
- Modify: `docs/testing_strategy.md`
- Modify: `tests/test_rpc_executor_group_drain.c` 中关于测试归属的注释

- [x] 删除 Runtime 目标结构中的旧 `rpc_executor` 字段，描述 Runtime 当前实际拥有的 Reactor/MessageService/peer/budget 资源。
- [x] 说明 Runtime 多分片线程预算不再包含 V1 Executor Group；raw group 和 Endpoint 测试仍归 R14。
- [x] 将 `test_rpc_executor_group_drain.c` 的留项注释从 R13 更正为 R14，并说明其实现仍在 `rpc.c` Endpoint 闭包内。
- [x] 保持带日期 review/audit 记录的历史结论不变。

### Task 6：干净构建、全量测试与 GitHub 验收记录

**Files:** R13 改动；Issue #290 实施记录。

- [x] 执行 `xmake f -c -y -m debug --toolchain=gcc`，构建受影响目标并运行 Runtime 定向测试，再运行 `xmake test -v -j1`。
- [x] 执行 GCC release `trcore` 构建；运行 inventory 自测和完整源码/归档 inventory，输出到 `build/r13-rpc-v1-inventory.json`。
- [x] 核对归档不含 `legacy_rpc_execution.c.o` 与 Runtime 旧 getter/适配器导出，但仍含 `rpc.c.o` 的 V1 Endpoint/Executor Group 定义。
- [x] 运行 `git diff --check`，检查 Runtime 源码/Xmake 不再引用旧配置或适配器；检查安装 SDK 门禁入口及 CI 必需名称未变。
- [ ] 确认 CI 的 GCC/Clang、ASan/UBSan、TSan、benchmark/schema 和安装 SDK 检查通过；未运行项如实记录。
- [ ] 将提交、测试证据、远程 Actions 链接和仍留给 R14 的 Endpoint/Executor/Wire 闭包写入 Issue #290；推送 `refactor/**` 分支。
