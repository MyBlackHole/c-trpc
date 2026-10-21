# R14 RPC V1 核心退役实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 删除旧 V1 Call、Endpoint、Executor Group、Wire 与不再实现的公共声明，同时保留 V2 RPC 合同和独立 RAW Codec。

**Architecture:** R14 退役 `src/rpc/rpc.c` 与 `rpc_wire` 闭包，安装 SDK 只保留 V2 共用 RPC 元数据、方法、状态与统计类型。`rpc_codec.c` 和 `tr_rpc_bytes`/`TR_RPC_CODEC_RAW`/`tr_rpc_raw_encode()`/`tr_rpc_raw_decode()` 继续独立提供复制式字节接口。V2 调用仍由 Record、Client、Server 和 TaskExecutor 实现，旧 V1 协议继续被拒绝。

**Tech Stack:** C11、POSIX pthread、Xmake 3.1.1、Python inventory 脚本、GNU `ar`/`nm`、GCC/Clang 与现有 GitHub Actions 矩阵。

**Spec:** `docs/superpowers/specs/2026-10-21-r14-v1-core-retirement-design.md`

## 全局约束

- 基线是 R14 分支设计提交 `febab4c4988f454e7dcafc19afe08c71d96bf23b`；实现前在 Issue #290 登记 R14-D2 的精确基线、唯一范围、不变量、验收条件与目标分支。
- 不改变 V2 帧、调用、所有权、取消、期限、背压、排空或销毁语义；不新增 V2 到 V1 回退。
- 删除公共 V1 Call/Endpoint/Interceptor 声明及仅用于拒绝旧拦截器字段的配置位；保留 RPC V2 共享类型。
- 不按 `tr_rpc_endpoint_` 前缀拒绝符号；逐个拒绝旧 V1 名称，并显式允许 V2 `tr_rpc_endpoint_result`、`tr_rpc_endpoint_terminal_fn`、`tr_rpc_endpoint_progress_fn` 及 RAW Codec。
- `test_rpc_message_ownership`、V2 Record/Client/Server/TaskExecutor 测试必须保留；删旧 V1 测试前先核对其适用于 V2 的断言已有对应证据。
- 保留 Xmake 构建/测试入口、CI 必需检查名称、矩阵和触发器；不合并 PR #288、不改写历史审计或 benchmark 记录。
- 所有构建、测试、清单、日志、缓存和临时目录都位于当前工作区的 `build/`，不得写入 `/tmp`。可能使用系统临时目录的命令先设置 `TMPDIR="$PWD/build/tmp"`。
- 文档、注释和提交说明使用简体中文；源码按项目 clang-format 18.1.8 格式化。

---

### Task 1：登记 R14-D2 并固化逐测试迁移清单

**Files:**
- Add: `docs/reviews/rpc-v1-core-retirement-inventory-20261021.md`
- Update: GitHub Issue #290
- Inspect only: `tests/test_rpc_deadline_failure.c`, `tests/test_rpc_deadline_fault.c`, `tests/test_rpc_terminal_backpressure.c`, `tests/test_rpc_final_close_retry.c`, `tests/test_rpc_overload.c`, `tests/test_rpc_stream_overload.c`, `tests/test_rpc_stream_backpressure.c`, `tests/test_rpc_unary_failure.c`, `tests/test_rpc_executor_reserve.c`, `tests/test_rpc_executor_group_drain.c`, `tests/test_transport.c`

- [ ] 先在 Issue #290 登记 R14-D2：基线 `febab4c4988f454e7dcafc19afe08c71d96bf23b`、目标分支 `refactor/rpc-v1-retirement-r14-20261021`、精确删除范围、V2/RAW 不变量及本计划验收条件；登记前不改实现代码。
- [ ] 生成 GCC release 基线归档和仓库内 inventory：先执行 `mkdir -p build/tmp build/r14`，再运行 `TMPDIR="$PWD/build/tmp" xmake f -c -y -m release --toolchain=gcc && TMPDIR="$PWD/build/tmp" xmake build trcore && python3 scripts/rpc_v1_inventory.py --root . --archive build/linux/x86_64/release/libtrcore.a --output build/r14/rpc-v1-before.json`。
- [ ] 在新增审计记录中逐项列出上述 10 个旧测试目标的场景、现有 V2 证据、处置（保留/迁移/补测后删除/V1 私有机制退役）及限制；期限与取消对应 `test_client_record`、`test_server_record_deadline`、`test_rpc_record_cancel`，FINAL/CLOSE 背压对应 `test_rpc_record_endpoint_tcp`，Server/Executor 排空对应 `test_server_record`、`test_task_executor` 和新 V2 drain 目标。
- [ ] 对每一条与 V2 合同相符但没有明确 V2 断言的旧行为，先在同一审计记录登记新增测试目标、确定性故障点和通过判据；不得将 V1 私有队列/每调用定时器/Executor Group 生命周期表述为已由 V2 等价覆盖。
- [ ] 记录基线归档中的 `rpc.c.o`、`rpc_wire.c.o`、`rpc_codec.c.o`，以及 `rpc.c.o` 对 RAW/Wire 符号的定义/未解析引用；在 Issue #290 链接审计文件及本计划。

### Task 2：先建立 RAW Codec 与旧协议拒绝的独立测试

**Files:**
- Add: `tests/test_rpc_raw_codec.c`
- Modify: `xmake.lua`
- Modify: `tests/test_rpc_record.c`

- [ ] 新增 `test_rpc_raw_codec` 目标，先只写测试并运行 `TMPDIR="$PWD/build/tmp" xmake build test_rpc_raw_codec && TMPDIR="$PWD/build/tmp" xmake run test_rpc_raw_codec`；在已有 `rpc_codec.c` 上确认通过后再开始删除生产代码。
- [ ] 覆盖非空字节往返、零长度往返（包括允许的 NULL 数据指针）、容量不足、非法 NULL 参数及 `written`/输出长度边界；断言既有错误码与输出写入行为，不增加 Codec 语义。
- [ ] `test_rpc_record.c` 删除 `rpc_wire.h` 和 `tr_rpc_wire_*` 使用，保留旧协议拒绝断言；构造固定 32 字节 V1 Header（`TRPC`、little-endian version=1、type=1，其余字段为 0），断言 V2 `tr_rpc_record_decode()` 返回 `TR_ERR_BAD_VERSION`。
- [ ] 运行 `TMPDIR="$PWD/build/tmp" xmake build test_rpc_record && TMPDIR="$PWD/build/tmp" xmake run test_rpc_record`，确认拒绝测试不再依赖旧 Wire 编解码器。

### Task 3：将 V2 Server drain 覆盖从旧 Group 目标独立出来

**Files:**
- Add: `tests/test_rpc_v2_server_destroy_drain.c`
- Modify: `xmake.lua`
- Modify then delete: `tests/test_rpc_executor_group_drain.c`

- [ ] 将 `test_v2_server_destroy_waits_for_accepted_work` 及它实际使用的条件变量、RPC V2 Server/Client 回调和真实 TCP fixture 移入新 V2 专属测试文件；不复制旧 Executor Group helper/wrapper。
- [ ] 将新目标加入 Xmake 测试目标并运行 `TMPDIR="$PWD/build/tmp" xmake build test_rpc_v2_server_destroy_drain && TMPDIR="$PWD/build/tmp" xmake run test_rpc_v2_server_destroy_drain`；验证已受理工作阻塞期间 `tr_server_destroy()` 返回 `TR_AGAIN`，释放工作后重试收敛、terminal 恰好一次。
- [ ] 对照 `test_server_record::test_stopped_work(false, false, true)` 的既有 V2 join 失败覆盖，确认新测试只保留原旧目标中独有的 accepted-work drain 不变量，不重复声称涵盖 Group 生命周期。

### Task 4：补齐 V2 Client 准入失败与终局回调销毁屏障

**Files:**
- Modify: `tests/test_client_record.c`
- Modify: `xmake.lua` only if an existing target's fault wrapper must be extended

- [ ] 先增加 G1 回归：用现有公开 Client 和确定性 clock/timer-arm 故障注入，先接受一个有期限调用，再让另一个更早期限调用的准入失败；断言失败调用不改写输出句柄、不发 OPEN、不产生幽灵终局，先前调用仍按原截止时间恰好终局一次，Client 之后可继续接纳/复用槽位。
- [ ] 先增加 G2 回归：用条件变量卡住真实 V2 Client terminal 回调；另一线程触发连接断开并调用销毁，验证销毁不越过活动回调；释放回调后销毁重试收敛、状态不被断连覆盖、terminal 恰一次且 Client/Buffer 预算归零。
- [ ] 每个新场景先运行对应 `test_client_record` 目标并确认断言能捕捉预期错误交错，再修正最小测试夹具；生产实现不得因旧 Endpoint 对照而改动。
- [ ] 将 G1/G2 的最终公开合同、故障注入点及通过输出写回 `docs/reviews/rpc-v1-core-retirement-inventory-20261021.md` 与 Issue #290；若现有 V2 接口不支持审计里写明的失败点，收窄到真实公开边界并记录理由，不能模拟 V1 私有堆状态。

### Task 5：补齐 V2 Server 容量恢复与同流背压隔离

**Files:**
- Modify: `tests/test_server_record.c`
- Modify: `tests/test_rpc_record_endpoint_tcp.c`
- Modify: `xmake.lua` only if the existing failure wrappers need extension

- [ ] 先增加 G3 回归：以 V2 Server 真实 TCP fixture、单 worker 条件变量门闩和固定任务容量填入已受理的一元/流式工作，再提交超额调用；断言超额调用在业务回调前只终局一次、已受理任务仍各自终局，释放容量后同一连接恢复成功，Server drain 和预算归零。
- [ ] G3 只断言 V2 已公开的资源拒绝状态，不复制旧 `16` 或 `14+2` Group 阈值；若 V2 合同不承诺具体状态，断言合法资源拒绝/不破坏已受理调用，并把边界写入审计记录。
- [ ] 先增加 G4 回归：沿用 V2 Record Endpoint TCP 真实窗口/发送夹具，使一条流的后续 MESSAGE 受阻，同时另一流继续完成；恢复窗口后断言受阻流按序交付且只终局一次、额度恢复，消息缓冲和 Endpoint 预算归零。
- [ ] G3/G4 的夹具只使用条件变量、现有 task/dispatch/send 包装或真实窗口，不通过任意 sleep 预测饱和时刻；新增测试必须先在旧/无断言状态观察到目标交错，再验证新断言通过。
- [ ] 将 G3/G4 的最终故障点、V2 状态合同和真实测试结果补入逐测试审计及 Issue #290；保持 `test_task_executor`、`test_server_record`、`test_rpc_record_endpoint_tcp` 原有覆盖。

### Task 6：收敛安装 SDK 中的 RPC 声明与门面配置

**Files:**
- Modify: `include/tr/rpc.h`
- Modify: `include/tr/client.h`
- Modify: `include/tr/server.h`
- Modify: `src/client.c`
- Modify: `src/server.c`
- Modify: `tests/test_connection_group_facade.c`

- [ ] 从 `rpc.h` 删除 V1 Call Handle、Context、Interceptor、Unary Handler/Result、Message/Disposition/Event、Stream Handler，以及 V1 Endpoint/注册/Call 续处理/Message release 声明；保留元数据、调用选项、基数、状态、语义统计、方法描述符与 Codec include。
- [ ] 从 Client/Server config 移除 V1 interceptor 字段和“不支持”注释，并从构造配置检查移除对应拒绝分支；保留 reconnect 等其他仍不支持的选项行为。
- [ ] 将 `test_connection_group_facade::test_transport_constructor_rejects_rpc_options` 中针对 interceptor 字段的输入和断言改为只验证仍有效的配置合同。
- [ ] 运行该目标及 `test_rpc_raw_codec`、`test_client_record`、`test_server_record` 的定向构建/测试；编译实际包含所有公开头的测试，发现仍有效的 V2 消费者时修正类型归属，不恢复无实现的 V1 API。

### Task 7：移除 V1 测试闭包及其故障注入依赖

**Files:**
- Modify: `tests/test_transport.c`
- Delete: `tests/rpc_v1_test_client_fixture.c`, `tests/rpc_v1_test_client_fixture.h`
- Delete: `tests/test_rpc_deadline_failure.c`, `tests/test_rpc_deadline_fault.c`, `tests/test_rpc_overload.c`, `tests/test_rpc_terminal_backpressure.c`, `tests/test_rpc_final_close_retry.c`, `tests/test_rpc_unary_failure.c`, `tests/test_rpc_stream_overload.c`, `tests/test_rpc_stream_backpressure.c`, `tests/test_rpc_executor_reserve.c`
- Modify then delete: `tests/test_rpc_executor_group_drain.c`
- Modify: `tests/test_server_record.c`
- Modify: `tests/test_complex_constructor_faults.c`
- Modify: `tests/test_connection_group_facade.c`
- Modify: `xmake.lua`

- [ ] 在 `test_transport.c` 移除 raw V1 Wire/Endpoint/Call/Interceptor 场景与 include；保留 Channel/Transport 测试和 `test_client_server_facade_nodelay_policy`。RAW Codec 专测由 Task 2 的新目标承担。
- [ ] 删除旧 deadline、过载、终局/流背压、一元失败、Executor reserve 和 Group 测试目标及注册项；保留 `test_rpc_message_ownership`、全部 V2 Record/Client/Server/TaskExecutor 目标。
- [ ] 从 Xmake 移除 `test_transport` 对 `rpc_v1_test_client_fixture.c` 的依赖，并清除只服务被删除目标的 pthread/clock/timer/旧 Wire linker wrappers；保留复杂构造故障测试里仍服务其他构造器的 malloc/pthread wrapper。
- [ ] 从 `test_server_record.c` 删除仅用于断言 `tr_rpc_executor_group_create` 未调用的 wrapper、计数与断言；保留 TaskExecutor 和 Record Endpoint 故障注入。
- [ ] 从 `test_complex_constructor_faults.c` 删除 V1 Group 构造失败场景；复核 `calloc/malloc/free/pthread_*` wrappers 仍被其余构造器故障用例使用。
- [ ] 运行 `TMPDIR="$PWD/build/tmp" xmake test -v -j1 'test_transport/*' 'test_connection_group_facade/*' 'test_server_record/*' 'test_complex_constructor_faults/*' 'test_rpc_v2_server_destroy_drain/*'`，确认剩余测试目标不引用 V1 API。

### Task 8：删除生产 V1 Call/Endpoint/Wire 实现与私有接口

**Files:**
- Delete: `src/rpc/rpc.c`, `src/rpc/rpc_internal.h`, `src/rpc/rpc_wire.c`, `src/rpc/rpc_wire.h`
- Modify: `xmake.lua`
- Modify if required by compile evidence: `src/client.c`, `src/server.c`, `tests/test_server_record.c`, `tests/test_runtime_threads.c`

- [ ] 确认 `rpc_internal.h` 的最后 include 已随 V1 测试和 client/server 拒绝路径清除，再从源码树删除四个 V1 文件。
- [ ] 从 `trcore` 源文件列表移除 `rpc.c`、`rpc_wire.c`；保留 `rpc_codec.c`、所有 `rpc_record*.c` 与 `rpc_v2.c`。
- [ ] GCC debug 下构建全体测试目标；按编译错误清除实际残留的 V1 类型/函数调用，不用宽泛文本替换，也不删除含 V2 `tr_rpc_endpoint_*` 名称的类型。
- [ ] 用 `rg` 检查生产源码、测试和 Xmake 无 `tr_rpc_wire_*`、V1 Endpoint/Executor/Call/Interceptor API、旧 fixture 路径与旧源文件引用。

### Task 9：精确 inventory/归档门禁与当前文档

**Files:**
- Modify: `scripts/rpc_v1_inventory.py`
- Modify: `scripts/test_rpc_v1_inventory.py`
- Modify: `.github/workflows/ci.yml`
- Modify: `README.md`, `docs/testing_strategy.md` 及仍描述当前 V1 RPC 能力的 SDK/架构文档
- Preserve: `docs/reviews/` 中带日期的历史审计与旧 benchmark 数据

- [ ] 在 CI 现有 RPC inventory 检查中扩展精确 V1 deny 符号/类型清单，包括旧 Wire、Endpoint、Executor Group、Call 续处理、注册、Interceptor 与消息释放符号；不改 job/check 名称、触发规则或检查范围。
- [ ] 扩充 inventory 自测：旧 V1 名称作为精确 deny 时失败；V2 `tr_rpc_endpoint_result`、`tr_rpc_endpoint_terminal_fn`、`tr_rpc_endpoint_progress_fn`、`tr_rpc_record_endpoint_*` 和 `tr_rpc_raw_encode/decode` 作为反例时通过。
- [ ] 增加 archive 审计断言：release `libtrcore.a` 不含 `rpc.c.o` / `rpc_wire.c.o`，仍含 `rpc_codec.c.o`；`nm -A -g` 无旧 V1 全局定义或归档内部未解析旧符号，RAW Codec 定义来自 `rpc_codec.c.o`。
- [ ] 更新仍作为当前使用说明的文档，准确写明默认 V2、V1 raw core/Wire 已退役、RAW Codec 仅为复制式字节工具；保留历史 review、故障结论和旧 benchmark 原文。
- [ ] 运行 inventory 自测、完整源码/归档扫描和 `git diff --check`，输出分别写入 `build/r14/`。

### Task 10：干净构建、SDK 消费、完整测试与远端验收

**Files:** 所有 R14 改动；Issue #290 R14-D2 验收记录

- [ ] GCC debug：先执行 `mkdir -p build/tmp build/r14`，再运行 `TMPDIR="$PWD/build/tmp" xmake f -c -y -m debug --toolchain=gcc && TMPDIR="$PWD/build/tmp" xmake build --all && TMPDIR="$PWD/build/tmp" xmake test -v -j1`。
- [ ] GCC release：干净配置并运行全量测试，构建 `trcore`，生成 inventory 与 `ar t`/`nm -A -g` 归档证据；确认 RAW Codec 保留且 V1 对象/符号清零。
- [ ] 运行已存在的安装后 C11/C++11 公开头编译与 V2 SDK smoke 测试；在安装消费源中加入 RAW Codec 空/非空复制调用，确认只使用安装目录中的头文件和静态库。所有安装目录使用 `build/r14/` 下路径。
- [ ] 执行仓库要求的 GCC/Clang、ASan/UBSan、TSan、CRC、Make/install、GCC/Clang 静态分析、源码格式与 RPC smoke 检查；若本地工具矩阵无法执行，准确列为未运行，不以文档基线 CI 替代实现提交 CI。
- [ ] 每个修改后的精确 head 检查 GitHub Actions 必需矩阵；记录真实状态、测试名称/结果、失败与未运行项，不宣称未完成或未合并的检查通过。
- [ ] 将实现 SHA、计划/审计链接、源码与归档结果、SDK/测试/CI 证据、剩余限制及 R15 下一步写入 Issue #290；R14 保持待集成状态，不将分支 CI 描述为已进入 PR #288 或 `main`。

## 计划自审

- 公共 V2 类型和 RAW Codec 都有显式保留路径及正向测试。
- 删除前安排测试行为逐项清单；G1–G4 Client 准入/回调屏障、Server 容量恢复、同流背压/跨流隔离均在旧测试删除前补 V2 断言，G5 Server drain 迁入独立目标；V1 私有实现机制不伪称语义等价。
- 旧协议拒绝测试不再链接待删除的 Wire 对象。
- 清理生产、SDK、Xmake、测试、inventory、CI 和当前文档均有独立验收项；保留历史证据。
- 构建/测试/安装/临时输出均定位至仓库 `build/`，没有 `/tmp` 路径。
