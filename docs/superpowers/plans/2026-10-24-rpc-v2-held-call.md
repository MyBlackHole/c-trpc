# RPC V2 可持有 Call 引用实施计划

> **执行约束：**本计划在当前隔离 worktree 内联逐项实施；用户禁止子 agent。测试、日志、缓存和构建产物全部保存在 worktree 的 `build/` 下。

**Goal:** 为普通 V2 Client 增加有界的 Call 持有引用，使应用能跨回调安全读取终局并取消活动调用。

**Architecture:** 每个 Call 固定槽最多发布一个唯一 hold token。终局后先照常释放协议绑定，再由 token 暂时 pin 结果槽和 Client；release CAS 精确清除 token。Client destroy 沿用现有 Runtime stop/reaper 顺序，reaper 保留被 pin 的槽并返回 `TR_AGAIN`。

**Tech Stack:** C11 atomics、现有 V2 Client/Call 槽、Runtime memory budget、Xmake、真实 TCP `test_client_record`、安装 SDK 消费端。

**Spec:** `docs/superpowers/specs/2026-10-24-rpc-v2-held-call-design.md`

## 全局约束

- 维持 `tr_rpc_v2_call` 既有 3-word ABI，不改变 TRP/RPC 线协议。
- 每个 Call 同时最多一个 hold；token 是 move-only 唯一所有权，不提供 clone。
- hold 不申请堆内存，不增加线程/队列；Call 固定槽仍归现有 Runtime 预算。
- held terminal slot 不保留 Stream/RPC binding，但占用原 Call 容量直到 release。
- 有 hold 时 `tr_client_destroy()` 可停止 Runtime 并交付 shutdown 终局，但 reaper 不得释放被 pin 的槽；返回 `TR_AGAIN`，release 后允许重试。
- 用户回调只观察已发布结果；FINAL、RESET、取消和 shutdown 的唯一终局选择规则不变。
- 所有 Xmake 临时目录、日志和缓存位于 `build/call-ref-243/`；构建产物位于工作区 `build/`。

## 文件职责

- `include/tr/rpc_v2.h`：定义 `tr_rpc_v2_call_ref`、终局快照和四个公开 API。
- `src/rpc/rpc_record_client.h/.c`：slot hold token、结果原子快照、token 生成/发布/消费、槽退役 pin。
- `src/client.c`：普通 facade API 桥接和活动期跨线程 cancel；已有 destroy/reaper 链负责保留对象并传递 `TR_AGAIN`。
- `tests/test_client_record.c`：真实 Client/Server/TCP 生命周期、终局回调、容量与 destroy 回归。
- `tests/test_rpc_v2_sdk.c`：安装 C11/C++11 消费端公开符号引用。
- `docs/rpc-v2-sdk.md`、`README.md`：唯一引用、结果可见性、容量影响、销毁重试语义与版本。
- `xmake.lua`：提升开发版本并保持现有目标和 CI 检查名。

---

### Task 1：定义公共 CallRef API 并先验证终局快照

**Files:**
- Modify: `tests/test_client_record.c`
- Modify: `include/tr/rpc_v2.h`
- Modify: `src/rpc/rpc_record_client.h`
- Modify: `src/rpc/rpc_record_client.c`
- Modify: `src/client.c`

**Interfaces:**
- `tr_rpc_v2_call_hold(call, &ref)` 只接受有效、已受理的 Call；成功发布唯一 token。
- `tr_rpc_v2_call_ref_result(ref, &result)` 在结果未选时返回 `TR_AGAIN`，终局快照发布后返回 `TR_OK`。
- `tr_rpc_v2_call_ref_cancel(ref)` 只对未终局的 held Call 请求取消。
- `tr_rpc_v2_call_ref_release(&ref)` 精确 CAS 消费 token，成功清零调用方值。
- `result` 的状态字段使用 `enum tr_rpc_status`，交付域使用 `enum tr_rpc_v2_delivery`。

- [x] **Step 1：添加真实 terminal 回调 hold 回归**

扩展 `public_result`，在 `public_callbacks().terminal` 中可选调用 `tr_rpc_v2_call_hold(call, &result->held_call)`。增加 `test_public_call_ref_terminal_snapshot`：通过真实 V2 一元调用取得 OK terminal；断言 terminal 回调中的 hold 成功、`tr_rpc_v2_call_ref_result()` 返回相同的 `status/delivery`，release 成功且清零 token。此测试首先应因公共类型/API 缺失而编译失败。

- [x] **Step 2：运行 RED 并确认失败点是 API 缺失**

```sh
mkdir -p build/call-ref-243/tmp build/call-ref-243/xmake-tmp
env TMPDIR="$PWD/build/call-ref-243/tmp" \
    XMAKE_TMPDIR="$PWD/build/call-ref-243/xmake-tmp" \
    PYTHONDONTWRITEBYTECODE=1 \
    xmake test -P . -v -j1 'test_client_record/*'
```

预期编译器报告新 `tr_rpc_v2_call_ref` 类型/函数尚未声明；不接受 fixture 或现有测试自身错误作为 RED 证据。

- [x] **Step 3：实现唯一 token 与结果发布**

在 public header 声明 spec 中的值类型和函数。slot 添加 `_Atomic uint64_t held_ref_token`、atomic `result_ready/status/delivery`。record Client 用 owner-only `next_ref_token` 生成非零、不回绕的 token。集中终局写入逻辑：先写 status/delivery，再 release-store `result_ready=true`；弱句柄/引用读者 acquire-load 后读取结果。hold 在 owner 校验 slot 和 generation，拒绝已有 token，成功时只写输出。result/cancel/release 在 facade 层按完整的 Client/generation/slot/token 校验。

- [x] **Step 4：运行聚焦测试并确认终局回调可 hold**

运行 Step 2 的命令；预期终局回调内 hold 成功，回调返回后结果保持一致，release 清零输入且重复 release 返回 `TR_ERR_STALE`。原有 `test_client_record` 组均通过。

### Task 2：验证取消、重放保护和槽生命周期

**Files:**
- Modify: `tests/test_client_record.c`
- Modify: `src/rpc/rpc_record_client.c`

- [x] **Step 1：添加 hold token 负例和 pending 结果测试**

增加测试覆盖：活动调用第一次 hold 成功、第二次 hold 返回 `TR_ERR_STATE` 且输出哨兵不变；终局前 result 返回 `TR_AGAIN` 且输出不变；ref cancel 选定 CANCELLED 后 result 返回 CANCELLED；终局后 ref cancel 返回 `TR_ERR_STATE`。另复制旧 token 值，release 原 token 后完成旧 Call 的槽回收并在同一槽建立新 Call/新 hold，断言释放旧 token 返回 `TR_ERR_STALE`，新引用仍可查询且 Client hold 计数不变。

- [x] **Step 2：运行聚焦回归并确认每个断言结果**

```sh
env TMPDIR="$PWD/build/call-ref-243/tmp" \
    XMAKE_TMPDIR="$PWD/build/call-ref-243/xmake-tmp" \
    PYTHONDONTWRITEBYTECODE=1 \
    xmake test -P . -v -j1 'test_client_record/*'
```

测试在 Task 1 API 实现后补入，因此本步记录 GREEN 结果；Task 1 的 API 缺失 RED 已验证。终局和槽复用使用回调/owner pump 同步，不通过 sleep 判断。

- [x] **Step 3：实现终局后协议退役、引用 pin 和 stale token 隔离**

调整 `tr_record_client_retire()`：终局回调已返回后先提交/确认 RESET 或 CLOSE、移除 route、销毁 binding；若 `held_ref_token != 0`，保留 active 固定槽及不可变结果，但不得保留 `stream`/`binding` 对象引用。token 为零时才清空槽。reap-stopped 使用同一 pin 规则。结果发布必须覆盖正常 terminal、timeout/cancel、owner-stopped 和 OPEN 准入失败后的 terminal；旧 token 的 CAS 失败不得访问或改变复用槽。

- [x] **Step 4：运行聚焦回归及重复槽复用场景**

运行 `test_client_record/*`。预期 CANCELLED/OK snapshot 逐项匹配，旧 token 无法清除新 token，重复 hold/release 不修改输出，现有 terminal exactly-once、迟到 FINAL 和 deadline 测试通过。

### Task 3：验证容量、drain 和 Client destroy 重试回收

**Files:**
- Modify: `tests/test_client_record.c`
- Modify: `src/client.c`
- Modify: `src/rpc/rpc_record_client.c`
- Modify: `src/rpc/rpc_record_client.h`

- [x] **Step 1：添加 capacity=1 和 shutdown result 回归**

在现有 public fixture 中把客户端 Call capacity 配为 1。取得并保留一个 LIVE 终局 CallRef 后，断言第二个 start 返回 `TR_AGAIN` 且输出 handle 不变；开始 drain 后 `wait_drained(timeout=0)` 返回 `TR_AGAIN`。另一个 pending Call 在 hold 后调用 `tr_client_destroy()`，destroy 停止 Runtime、由 reaper 交付 `DELIVERY_SHUTDOWN` 的 `CANCELLED` terminal，再因 hold 返回 `TR_AGAIN`；CallRef result 必须读取到准确的 shutdown 快照。此后弱 Call/普通 Client 操作均按 stopping 合同拒绝。release 后 destroy 重试成功、Runtime shard 预算为零。

- [x] **Step 2：运行聚焦回归并核验销毁重试行为**

```sh
env TMPDIR="$PWD/build/call-ref-243/tmp" \
    XMAKE_TMPDIR="$PWD/build/call-ref-243/xmake-tmp" \
    PYTHONDONTWRITEBYTECODE=1 \
    xmake test -P . -v -j1 'test_client_record/*'
```

测试在实现之后补入，本步验证 `TR_AGAIN`、shutdown 快照和 release 后 destroy 重试；Task 1 已通过 API 缺失 RED 建立 TDD 证据。

- [x] **Step 3：让 stopped-reaper 保留 held slot**

调整 `tr_rpc_record_client_reap_stopped()`：逐槽完成 `unroute`、binding reap、唯一终局报告和 unary 缓冲释放；若该槽 `held_ref_token != 0`，保留 active slot/原子快照并标记本轮 pending，不触碰 result/token。继续回收其他槽后返回 `TR_AGAIN`，保留 record Client 指针；没有 hold 时按现有路径 detach/free。release 后 destroy 重试可只完成剩余 slot/对象析构。drain 仍沿用 active slot 谓词，不特殊忽略 hold slot。

- [x] **Step 4：运行 Client/SDK 聚焦回归**

运行 `test_client_record/*` 和 `test_rpc_v2_sdk/*`。预期 destroy 停止 Runtime 后仍保留 held terminal result 并返回 AGAIN；弱句柄在停止后失效，ref 仍可读取/释放；retry 完成，旧无引用场景结果不变。

### Task 4：SDK 文档、开发版本与全量验收

**Files:**
- Modify: `tests/test_rpc_v2_sdk.c`
- Modify: `docs/rpc-v2-sdk.md`
- Modify: `README.md`
- Modify: `xmake.lua`

- [x] **Step 1：新增安装 C11/C++11 符号链接引用**

在 SDK 消费端引用 `tr_rpc_v2_call_hold`、`tr_rpc_v2_call_ref_result`、`tr_rpc_v2_call_ref_cancel` 和 `tr_rpc_v2_call_ref_release` 的函数地址，并实例化 `tr_rpc_v2_call_result`；保留既有静态配置运行不启用引用逻辑。运行 `test_rpc_v2_sdk/*`，缺少任何导出符号都必须链接失败。

- [x] **Step 2：文档化唯一所有权和生命周期**

在 `docs/rpc-v2-sdk.md` 说明 borrowed `tr_rpc_v2_call` 与 move-only CallRef 的区别、每 Call 至多一个 hold、token 的转移/release 责任、快照在 terminal callback 即将执行前发布、活动期取消、终局后禁止协议操作、hold 占用固定槽、`wait_drained` 与 `destroy` 的 AGAIN/重试语义。README 标记当前开发版 `.26` 和 #243 子项边界；不宣称 #243 或 V2 整体完成。

- [x] **Step 3：递增版本并运行完整 Release 测试**

将 `xmake.lua` 与 README 从 `2.0.0-dev.25` 同步递增到 `2.0.0-dev.26`。配置与测试命令：

```sh
env TMPDIR="$PWD/build/call-ref-243/tmp" \
    XMAKE_TMPDIR="$PWD/build/call-ref-243/xmake-tmp" \
    PYTHONDONTWRITEBYTECODE=1 \
    xmake f -P . -c -y -m release --toolchain=gcc \
      --builddir=build
env TMPDIR="$PWD/build/call-ref-243/tmp" \
    XMAKE_TMPDIR="$PWD/build/call-ref-243/xmake-tmp" \
    PYTHONDONTWRITEBYTECODE=1 \
    xmake test -P . -v -j1
```

Release 配置使用工作区标准 `build/`，以便 TLS identity 测试按项目约定定位仓库根目录；完整测试 90/90 通过。所有临时目录仍在 worktree 内。

- [x] **Step 4：验证并发结果读取及 token release**

使用 `test_client_record` 的 CallRef 终局闸门场景：一个普通线程持有 CallRef 并反复读取 pending/result；owner 报告 terminal 后，release 线程清除 token；owner pump 完成槽回收。全部线程用现有 gate/condition 同步，不依赖固定睡眠。分别运行：

```sh
env TMPDIR="$PWD/build/call-ref-243/tmp" \
    XMAKE_TMPDIR="$PWD/build/call-ref-243/xmake-tmp" \
    PYTHONDONTWRITEBYTECODE=1 \
    xmake f -P . -c -y -m tsan --toolchain=gcc \
      --builddir=build/call-ref-243-tsan
env TMPDIR="$PWD/build/call-ref-243/tmp" \
    XMAKE_TMPDIR="$PWD/build/call-ref-243/xmake-tmp" \
    PYTHONDONTWRITEBYTECODE=1 TSAN_OPTIONS=halt_on_error=1 \
    xmake test -P . -v -j1 'test_client_record/*'
env TMPDIR="$PWD/build/call-ref-243/tmp" \
    XMAKE_TMPDIR="$PWD/build/call-ref-243/xmake-tmp" \
    PYTHONDONTWRITEBYTECODE=1 \
    xmake f -P . -c -y -m asan --toolchain=gcc \
      --builddir=build
env TMPDIR="$PWD/build/call-ref-243/tmp" \
    XMAKE_TMPDIR="$PWD/build/call-ref-243/xmake-tmp" \
    PYTHONDONTWRITEBYTECODE=1 \
    ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
    UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
    xmake test -P . -v -j1
```

CallRef 闸门并发用例在 TSan 下 16 组通过；完整 Release 与 ASan/UBSan 测试各 90/90 通过。
安装 SDK 后另以 GCC/Clang 的 C11 与 G++/Clang++ 的 C++11 消费端编译、链接并运行，四种组合均通过。

- [x] **Step 5：格式与最终精确 SHA 检查**

运行 `git diff --check` 和 `python3 scripts/check_source_format.py --base origin/feat/message-layer-compression-235 --head HEAD`；该脚本使用项目固定 clang-format 18.1.8 检查相对集成基线新增/修改的 C/H 行。确认 `xmake.lua` 和 README 版本一致，new symbols 从安装 SDK C11/C++11 consumer 可链接。提交前 `git status --short --branch` 只包含本计划列出的文件。

- [x] **Step 6：按需求提交格式提交并推送**

最终实现提交标题格式为 `【F-0243】RPC: 增加可持有 Call 引用, 2.0.0-dev.25 -> 2.0.0-dev.26`，正文按 `feature-commit-format` 写明需求描述、背景、实现方案、影响范围、测试验证、相关需求、性能影响、风险评估、回滚方案和验收标准。将新分支推到远端；如果创建依赖 PR，base 使用当前压缩候选 `feat/message-layer-compression-235`，保持 Draft 并清楚标记依赖，不合并。

## 终态边界

该实现仅关闭 #243 的 Client 可持有 Call/result 子项。父子 deadline、跨 owner A→B→A、通用操作预约、更多引用所有权模型与模型检查仍待实施；更新范围记录时不得宣称整体 V2 完成。
