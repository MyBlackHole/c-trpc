# RPC V2 方法钩子设计

日期：2026-10-27。关联 GitHub #243 / ARCH-07。基线为 `feat/rpc-v2-parent-context-243`，版本 `2.0.0-dev.155`。

## 问题

RPC V2 当前把业务 `work` 放在分片共享的有界 `tr_task_executor` 上，协议状态由 Reactor owner 修改。公共 V2 API 仍声明不支持拦截器。旧 V1 的单个 `int` 钩子曾把观察、准入、后置处理等不同语义混在一起，且多个阶段的返回值被忽略。#243 要求这些能力具有不同的类型、执行域与终局责任。

## 目标与边界

本设计在冻结的 V2 方法描述中增加三类固定钩子：

1. **观察钩子**：只读 `void` 回调，观察合法 OPEN 和最终终局，不返回准入或 RPC 状态。
2. **准入钩子**：在 Reactor owner 上同步运行，返回专用的 `ACCEPT/REJECT` 结构；仅用于短小、非阻塞的本地策略拒绝。
3. **认证钩子**：作为独立的 `tr_task_executor` 工作运行，接收只读 OPEN、协作取消令牌，返回专用的 `ALLOW/REJECT` 结构。完成必须先回到原 owner；owner 复核调用仍活动、代次和 deadline 后才能提交业务 OPEN work。

钩子按方法配置，不建立动态中间件链，不创建每个调用的线程或无界队列。认证复用该分片已有的有界执行器，和 RPC work 共享队列容量、取消与完成交付责任。认证函数不得等待需要同一执行器或 Reactor owner 前进的工作；长时间/外部阻塞的认证应由应用使用受限 worker 和自己的 I/O deadline 实现。Client 已有的消息、元数据、writable、terminal 回调继续作为客户端观察面，本切片不增加第二套 Client 观察 API。

## 公共契约

在 `include/tr/rpc_v2.h` 中定义：

```c
enum tr_rpc_v2_observation_event {
    TR_RPC_V2_OBSERVE_OPEN,
    TR_RPC_V2_OBSERVE_TERMINAL
};

struct tr_rpc_v2_observation {
    struct tr_rpc_v2_call_id call;
    enum tr_rpc_v2_observation_event event;
    const struct tr_rpc_v2_request *request; /* 仅 OPEN 有效 */
    enum tr_rpc_status status;               /* 仅 TERMINAL 有效 */
    enum tr_rpc_v2_delivery delivery;         /* 仅 TERMINAL 有效 */
};
typedef void (*tr_rpc_v2_observer_fn)(
    const struct tr_rpc_v2_observation *observation, void *arg);

enum tr_rpc_v2_admission_decision {
    TR_RPC_V2_ADMISSION_ACCEPT,
    TR_RPC_V2_ADMISSION_REJECT
};
struct tr_rpc_v2_admission_result {
    enum tr_rpc_v2_admission_decision decision;
    enum tr_rpc_status rejection_status;
};
typedef struct tr_rpc_v2_admission_result (*tr_rpc_v2_admit_fn)(
    struct tr_rpc_v2_call_id call,
    const struct tr_rpc_v2_request *request, void *arg);

enum tr_rpc_v2_authorization_decision {
    TR_RPC_V2_AUTHORIZATION_ALLOW,
    TR_RPC_V2_AUTHORIZATION_REJECT
};
struct tr_rpc_v2_authorization_result {
    enum tr_rpc_v2_authorization_decision decision;
    enum tr_rpc_status rejection_status;
};
typedef struct tr_rpc_v2_authorization_result (*tr_rpc_v2_authorize_fn)(
    struct tr_rpc_v2_call_id call,
    const struct tr_rpc_v2_request *request,
    const struct tr_rpc_v2_cancel *cancel, void *arg);
```

`struct tr_rpc_v2_method` appends optional `observe`, `admit`, and `authorize` fields after the existing `arg`, preserving the position of existing aggregate members. The function pointers and `arg` are copied/frozen at `tr_server_create`; the pointed-to argument remains borrowed until successful Server destroy. Existing zero-initialized methods preserve behavior.

Result validation is explicit. `ACCEPT` and `ALLOW` require `rejection_status == TR_RPC_STATUS_OK`; `REJECT` requires a valid non-OK RPC status. Unknown enum values or inconsistent statuses become `TR_RPC_STATUS_INTERNAL`. No hook returns a generic `int` whose meaning is ignored.

## Lifecycle and ownership

For a complete, valid, known-method OPEN, the owner performs these steps in order:

```text
validate OPEN and fixed deadline
bind peer identity snapshot
publish semantic call start and terminal responsibility
observe(OPEN)
admit(OPEN)                         [owner, optional]
submit authorize(OPEN, cancel)      [bounded executor, optional]
owner receives auth completion and revalidates call/generation/deadline
submit application work(OPEN)       [bounded executor]
run queued MESSAGE/HALF_CLOSE FIFO
select one final status
wait for submitted work to exit and done delivery
observe(TERMINAL)
application terminal callback
```

The observer receives a borrowed immutable request only during `OBSERVE_OPEN`. The owner calls it after method/identity resolution and before admission. Observation callbacks must be short and non-blocking; their return type cannot affect protocol state.

The admission callback receives the validated OPEN and peer-identity snapshot on the owner. `ACCEPT` proceeds to authorization or application work. `REJECT` selects its requested non-OK RPC status, does not start authentication or business work, and sends an empty error FINAL through the existing bounded reply path. It then settles exactly one semantic terminal callback after that FINAL is accepted or the route terminates.

The authorization callback runs as a distinct task on the same shard executor as application work. Its OPEN metadata/body and peer identity are job-owned snapshots, borrowed until the callback returns. Its cancel token is valid only in that callback. It cannot access route, stream, owner slot, or mutable call state.

While authorization runs, the owner detaches the OPEN job into one `auth_job` pointer in the existing fixed Call slot. Later records use only the existing bounded per-call event queue and `max_pending_per_call`; no second queue or unbounded pending state is introduced. On ALLOW, the original owner rechecks terminal state, task/route generation, call identity, and absolute deadline, then places the same OPEN job at the front of the event queue before later records. Business work therefore remains OPEN/MESSAGE/HALF_CLOSE FIFO. On REJECT, queued records are discarded.

Cancellation, RESET, deadline, or endpoint stop may select the terminal status while authorization runs. The owner requests task cancellation, retains `auth_job` and its request/identity snapshots until the executor `done` callback returns, discards a late `ALLOW`, and emits the terminal callback only after that task has exited. Authentication cannot re-admit a terminal call or release a slot early. A rejected authorization selects its validated RPC status, converts the original OPEN job into an empty error FINAL, and drops queued request jobs.

`OBSERVE_TERMINAL` runs on the same delivery domain as the existing V2 server terminal callback, immediately before it, only after submitted work has returned and queued resources are reclaimed. It is invoked at most once for each accepted OPEN. `delivery` distinguishes live owner delivery from shutdown cleanup. The observation hook does not own reply, route, or resource release.

## Error and compatibility behavior

- Malformed/unmatched OPEN does not call any method hook or create semantic call responsibility.
- Hook result structure errors map to one `INTERNAL` terminal result and never close an otherwise valid connection.
- Admission rejection and authentication rejection are call-scoped; they do not terminate the Channel.
- Executor queue pressure keeps the OPEN job pending under the existing bounded retry/progress loop. Permanent executor closure terminates the call as `UNAVAILABLE`.
- A cancelled/deadline-expired call waits for accepted authentication work to return before terminal notification and release.
- Wire format is unchanged. Method-table ABI changes are acceptable in the pre-release V2 API; callers must rebuild.
- No hook configured means current V2 behavior and ordering are unchanged.

## Verification

Tests must run against the actual `TaskExecutor` and RPC Record dispatch path. Required cases:

1. Observer receives OPEN and TERMINAL exactly once; its `void` result cannot admit/reject.
2. Typed admission rejection prevents auth and application work and returns the requested status.
3. Authorization ALLOW returns to owner before application work; one worker preserves OPEN/MESSAGE FIFO.
4. Authorization REJECT prevents application work and produces one terminal callback with its status.
5. Cancellation/deadline while auth is blocked requests cancellation; a late ALLOW cannot start work or change the terminal result, and terminal waits for auth exit.
6. Pending events during authentication remain bounded; full per-call queue rejects the call without starting application work.
7. Invalid admission/auth result structures map to INTERNAL.
8. Public SDK header compiles as C11 and C++11; install consumer exercises method hook layout.

Xmake tests and all generated test output must remain under the worktree `build/` tree. Run Xmake with `TMPDIR="$PWD/build/tmp"` and `-P .`; do not create test artifacts under `/tmp`.
