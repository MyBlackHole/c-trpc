# RPC V2 方法钩子实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** 为 RPC V2 方法提供语义分离的观察、快速准入拒绝与可取消异步认证钩子，并在认证完成回到 owner 后才允许业务 work 开始。

**Architecture:** 在冻结的 `tr_rpc_v2_method` 增加三种不同类型的 hook。观察和快速准入在 owner 执行；异步认证复用分片共享 `tr_task_executor`，完成回调重验调用终局、代次和 deadline，再放行 OPEN work。队列、取消、资源所有权沿用现有 dispatch 状态机。

**Tech Stack:** C11/C++11 public headers, RPC Record dispatch, shared `tr_task_executor`, Xmake tests.

**Spec:** `docs/superpowers/specs/2026-10-27-rpc-v2-method-hooks-design.md`

## Global Constraints

- 生产协议可变状态仅由其 Reactor owner 修改。
- 已接受任务必须恰好完成一次 done 交付；取消不提前释放任务参数。
- OPEN deadline 覆盖准入、认证排队、认证工作、业务工作和回复背压；迟到 ALLOW 不可复活终局调用。
- 每调用待处理输入仍受现有 `max_pending_per_call` 限制；不创建线程或无界队列。
- 公共文档和注释使用简体中文；函数、类型和 wire 标识保留机器兼容名称。
- 所有测试命令使用 `TMPDIR="$PWD/build/tmp"` 和 `-P .`；不得在 `/tmp` 写测试文件或产物。
- 从 `2.0.0-dev.155` 推进至 `2.0.0-dev.156`；保持 #329 为父 PR，不合并任何 PR。

---

## 文件结构

- `include/tr/rpc_v2.h`：观察事件、准入结果、认证结果和 V2 method hook 函数签名；hook 字段追加在既有 `arg` 后，保留原字段位置。
- `src/rpc/rpc_record_dispatch.h`：内部冻结 method hook 指针。
- `src/rpc/rpc_record_dispatch.c`：OPEN hook 顺序、认证任务阶段、owner 完成复核和终局观察。
- `src/server.c`：公开 method 到内部 method 的冻结复制和 hook 组合校验。
- `src/server_record_internal.h`：修正 V2 hook 支持契约说明。
- `tests/test_rpc_record_dispatch.c`：真实 Reactor/Executor dispatch 的行为回归。
- `tests/test_rpc_v2_sdk.c`：公开结构与回调签名编译/最小运行覆盖。
- `README.md`、`docs/rpc-v2-sdk.md`：当前候选版本、认证执行域和钩子边界。
- `docs/reviews/rpc_v2_method_hooks_20261027.md`：完成后的实现与验证记录。

### Task 1: 先加准入拒绝与观察时序回归

**Files:**
- Modify: `tests/test_rpc_record_dispatch.c`
- Modify: `src/rpc/rpc_record_dispatch.h`（仅添加测试所需的内部 hook 字段；实现延后）

**Interfaces:**
- `tr_rpc_dispatch_method` 增加 `tr_rpc_v2_observer_fn v2_observe` 和 `tr_rpc_v2_admit_fn v2_admit`。
- 方法上下文记录 OPEN/TERMINAL 观察次数、准入结果及业务 work 次数。

- [x] **Step 1: Write the failing tests**

新增一个有钩子的 V2 方法：OPEN observer 递增一次并校验请求 kind；admit 返回 `REJECT/PERMISSION_DENIED`；terminal observer 记录最终状态；业务 work 递增调用数。断言观察顺序为 OPEN→TERMINAL、业务 work 为 0、派发终局为 `PERMISSION_DENIED` 且只发生一次。另加 ACCEPT case，证明返回 ACCEPT 不改变现有 handler 处理。

- [x] **Step 2: Run the focused test to prove it fails**

Run: `TMPDIR="$PWD/build/tmp" xmake test -P . -v -j1 'test_rpc_record_dispatch/*'`

Expected: 测试目标因新 hook 类型/字段尚未声明而失败编译；此时不接受任何现有结果作为通过。

- [x] **Step 3: Add only the public/internal hook declarations needed by the test**

在 `rpc_v2.h` 定义专用 observation/admission 结构与函数指针；在内部 method 结构加对应字段，并由 `server.c` 冻结复制。`tr_dispatch_received()` 在完整 OPEN 与 peer identity 准备后构造只读 request，先调用 OPEN observer，再调用 admit。无效 result 和 REJECT 都将原 OPEN job 转为有界的空 FINAL，分别使用 `INTERNAL` 或指定合法状态。

- [x] **Step 4: Run the focused test**

Run: `TMPDIR="$PWD/build/tmp" xmake test -P . -v -j1 'test_rpc_record_dispatch/*'`

Expected: observer/admission tests pass; `REJECT` returns an empty error FINAL to the peer; existing no-hook cases remain unchanged.

### Task 2: 先加异步认证通过、拒绝与迟到取消测试

**Files:**
- Modify: `tests/test_rpc_record_dispatch.c`

**Interfaces:**
- Public `tr_rpc_v2_authorize_fn` receives call ID, immutable OPEN request, valid callback-only cancel token and method arg; returns `tr_rpc_v2_authorization_result`.
- The authorization function runs as a separate task; application OPEN work is a later task submitted only after owner completion validation.

- [x] **Step 1: Add ALLOW and REJECT tests**

Use the fixture's existing `gate` to pause authorization on a single-worker executor. In the ALLOW case queue OPEN and MESSAGE while auth is paused, release auth, then assert application observes OPEN followed by MESSAGE. In the REJECT case return `REJECT/UNAUTHENTICATED`; assert no application work runs and terminal status is `UNAUTHENTICATED`.

- [x] **Step 2: Add late ALLOW cancellation test**

Pause auth after it starts, abort/cancel the stream on owner, release auth with ALLOW, and assert terminal remains the original cancellation status, no application callback ran, and terminal was not delivered until the auth callback left.

- [x] **Step 3: Run and confirm the new cases fail**

Run: `TMPDIR="$PWD/build/tmp" xmake test -P . -v -j1 'test_rpc_record_dispatch/*'`

Expected: the test fails because public method has no `authorize` hook and dispatch currently starts OPEN work directly.

### Task 3: Implement split authorization task lifecycle

**Files:**
- Modify: `src/rpc/rpc_record_dispatch.h`
- Modify: `src/rpc/rpc_record_dispatch.c`
- Modify: `src/server.c`

**Interfaces:**
- `tr_rpc_dispatch_method` stores `v2_observe`, `v2_admit`, `v2_authorize`, `v2_terminal` and one borrowed `arg`.
- `tr_dispatch_slot` stores one `auth_job` and authorization completion state for one accepted OPEN; the OPEN job is detached from the per-call FIFO while auth runs.
- `tr_dispatch_done()` distinguishes authorization jobs from application work; on ALLOW it puts the same OPEN job back at the FIFO head before later events.

- [x] **Step 1: Add job stage and typed worker result storage**

Add `enum tr_dispatch_job_stage { TR_DISPATCH_JOB_APPLICATION, TR_DISPATCH_JOB_AUTHORIZATION }`, an authorization result field to each job, and an owner-only `authorized` flag to each slot. Initialize authorization result to an invalid sentinel before submission.

- [x] **Step 2: Factor immutable V2 request construction**

Extract a helper that maps the copied `tr_rpc_record_view` and held peer identity snapshot to `tr_rpc_v2_request`. Use it from both `tr_dispatch_v2_work()` and the authorization worker. The helper must not return pointers beyond the job's lifetime.

- [x] **Step 3: Submit authorization as its own executor task**

When the queue head is OPEN, the method has `v2_authorize`, and the slot is not yet authorized, submit an authorization-stage job. On acceptance, unlink it from `s->head`, save it as `s->auth_job`, set `s->submitted` and `s->task`, and leave subsequent records in the existing bounded FIFO. `TR_AGAIN` leaves the OPEN linked at the head for the existing retry behavior; permanent executor errors select `UNAVAILABLE`.

- [x] **Step 4: Revalidate and publish authorization only in owner `done`**

In `tr_dispatch_done()`, for an authorization job: clear submitted state; verify `s->auth_job == job`, the copied call identity matches the live slot, and the job is OPEN. If cleanup/terminal/stopping, detach and free `auth_job`, then settle. Otherwise call `tr_dispatch_live()`. ALLOW clears `auth_job`, sets `authorized`, and prepends the same job ahead of later records; the next pump submits application OPEN. REJECT validates the status, converts the same OPEN job to an empty error FINAL, and drops queued jobs. Invalid structured output becomes an `INTERNAL` FINAL.

- [x] **Step 5: Preserve accepted-task ownership on cancellation**

Keep `auth_job` and request/identity snapshots alive until the executor `done` callback returns. On a terminal route event, request executor cancellation as today. A late ALLOW after terminal only detaches/frees `auth_job`; `tr_dispatch_settle()` remains responsible for the single terminal callback after work retirement.

- [x] **Step 6: Run the focused test**

Run: `TMPDIR="$PWD/build/tmp" xmake test -P . -v -j1 'test_rpc_record_dispatch/*'`

Expected: ALLOW, REJECT, queued FIFO, late cancellation and all existing dispatch regressions pass.

### Task 4: Public API and facade contract verification

**Files:**
- Modify: `include/tr/rpc_v2.h`
- Modify: `src/server.c`
- Modify: `src/server_record_internal.h`
- Modify: `tests/test_rpc_v2_sdk.c`
- Modify: `docs/rpc-v2-sdk.md`

- [x] **Step 1: Freeze all hook pointers and arg through public Server creation**

Copy each optional hook pointer and `arg` into the internal method table. Reject inconsistent work/hook combinations only if the method is not a public V2 method; preserve all valid public methods. Transport-only construction keeps an empty RPC method table.

- [x] **Step 2: Add public SDK compile/behavior coverage**

Construct a public `tr_rpc_v2_method` with all three hook types. Verify its hooks are copied through the public factory and compile the callback declarations under C11/C++11 consumer tests. Observe one OPEN and one TERMINAL for a valid invocation; keep the existing client callbacks unchanged.

- [x] **Step 3: Document execution and lifetime boundaries**

Document owner-domain observer/admit, bounded-worker authorize, cancellation cooperation, result normalization and borrowed arg lifetime. Replace the obsolete blanket statement that V2 has no interceptors with the precise statement that V2 has fixed typed method hooks and no dynamic/global middleware chain.

- [x] **Step 4: Run public SDK target**

Run: `TMPDIR="$PWD/build/tmp" xmake test -P . -v -j1 'test_rpc_v2_sdk/*'`

Expected: public method creation, callbacks and all previous SDK tests pass.

### Task 5: Version, review evidence and validation

**Files:**
- Modify: `xmake.lua`
- Modify: `README.md`
- Create: `docs/reviews/rpc_v2_method_hooks_20261027.md`

- [x] **Step 1: Advance version and release boundary**

Set `xmake.lua` to `2.0.0-dev.156`; make README candidate string match and add one sentence for typed V2 method hooks. Keep unresolved #243 and total V2 status explicit.

- [x] **Step 2: Run focused and complete verification**

Run focused dispatch and SDK targets in GCC Release, ASan/UBSan, TSan, and Clang Release with `TMPDIR="$PWD/build/tmp"` and `-P .`. Then run `TMPDIR="$PWD/build/tmp" xmake test -P . -v -j1` for the complete registered suite. Verify installed public C11/C++11 consumers through the existing project target/CI path.

- [x] **Step 3: Run formatting and consistency checks**

Run `git diff --check`; run `python3 scripts/check_source_format.py --base origin/feat/rpc-v2-parent-context-243 --head HEAD`; verify `xmake.lua` and README candidate versions match and inspect exact changed files.

- [x] **Step 4: Record implementation review**

Write the review record with exact commit SHA, hook ordering, owner/worker responsibility, cancellation/late result behavior, test matrix and remaining #243/V2 gates. Do not state #243 or V2 is complete from this slice.

- [ ] **Step 5: Commit and push the stacked change**

Commit implementation as `【F-0243】RPC: 增加类型化 V2 方法钩子, 2.0.0-dev.155 -> 2.0.0-dev.156`. Push `feat/rpc-v2-method-hooks-243`; base its PR on `feat/rpc-v2-parent-context-243` and leave it open.

## Self-review

- Spec sections for hook types, ordering, execution domain, cancellation, pressure and terminal semantics map to Tasks 1–4.
- Late authentication completion and bounded pending records are explicitly tested in Task 2 and implemented in Task 3.
- Public ABI/documentation and release version are covered by Task 4–5.
- Test output is confined to `build/`; no `/tmp` artifact is used.
- Scope excludes dynamic middleware, Client hooks, TLS-provider API changes and wire-format changes.
