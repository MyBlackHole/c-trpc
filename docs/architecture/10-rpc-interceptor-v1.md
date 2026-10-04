# RPC Interceptor V1

> Status: CURRENT V1 contract

Interceptor V1 是 Call-level control hook，不是 message middleware，也不是新的 worker execution domain。

## 1. Public shape

```c
enum tr_rpc_interceptor_phase {
    TR_RPC_INTERCEPTOR_CLIENT_PRE_CALL,
    TR_RPC_INTERCEPTOR_SERVER_PRE_HANDLER,
    TR_RPC_INTERCEPTOR_SERVER_POST_HANDLER,
    TR_RPC_INTERCEPTOR_CLIENT_POST_CALL,
};

typedef int (*tr_rpc_interceptor_fn)(
    struct tr_rpc_call_handle call,
    enum tr_rpc_interceptor_phase phase,
    int status,
    void *arg);
```

Client/Server facade 各配置一个 `struct tr_rpc_interceptor`。Server 创建每个 peer RPC Endpoint 时复制同一 hook/arg；hook arg 的生命周期必须覆盖对应 Client/Server。

V1 不建立动态 middleware list。需要组合 auth/tracing/metrics 时，由应用提供一个固定 composite callback。

## 2. Execution domain

所有 phase 都同步运行在 Call 所属 Reactor owner。

要求：

- hook 必须短小、非阻塞；
- 不允许磁盘、网络、KMS、数据库等待；
- 不允许等待依赖同一 Reactor 前进的 future/condition；
- hook 运行时不会持有 `endpoint->lock`。

最后一条很重要：hook 可以安全调用 owner-safe Context/metadata API，而不会死锁。

需要阻塞认证时，V1 Interceptor 不负责执行；应由未来 async auth work-unit / application handler 承担，而不是阻塞 Reactor。

## 3. Allowed reentry

Interceptor 内允许：

```text
tr_rpc_call_get_context()
tr_rpc_call_get_peer_metadata()
tr_rpc_call_get_peer_trailing_metadata()
tr_rpc_call_set_metadata()               (仍处于合法窗口时)
tr_rpc_call_set_trailing_metadata()      (Server Streaming STATUS 前)
tr_rpc_call_is_cancelled()
```

同一个 Call 在 hook 执行期间禁止：

```text
send
send_buffer
close_send
finish
cancel
```

这些 API 会返回 `TR_ERR_STATE`。这是运行时 ownership guard，不只是文档约定。

Interceptor 也不应从 hook 内创建递归 RPC 或等待会反向依赖当前 owner turn 的工作。

## 4. CLIENT_PRE_CALL

执行点：

```text
allocate Call
apply options / deadline / call initial metadata
CLIENT_PRE_CALL
encode/open first request path
```

因此 hook 可以：

- 读取 Method identity/cardinality/deadline；
- 注入 auth token / request-id / trace initial metadata；
- 做轻量 metrics/tracing start。

V1 `CLIENT_PRE_CALL` 的返回值不用于本地 RPC rejection，建议返回 `OK`。若后续要支持 client-side policy rejection，应单独定义 Call result 语义，而不是把 RPC status 偷映射成 library error。

Client PRE 发生在 Stream open 之前，所以后续 Stream open 失败时 hook 仍可能已经执行；它表示 Call attempt，而不是“已经发到 peer”。

## 5. SERVER_PRE_HANDLER

执行点：

```text
decode first REQUEST
allocate/bind Server Call
import peer initial metadata
validate request cardinality
SERVER_PRE_HANDLER
executor admission
application handler
```

这是 V1 唯一允许通过返回值做 admission rejection 的 phase。

规则：

- `OK`：继续；
- 合法非 OK RPC status：在 application handler 运行前拒绝；
- 非法 status：归一化为 `INTERNAL`。

Unary rejection 使用现有 Unary response/status path；Streaming rejection 使用 final STATUS path。两者都属于 RPC-level rejection，不关闭整个 connection。

Server PRE 可以读取 request initial metadata，因此适合：

- token/tenant lightweight validation；
- ACL lookup in preloaded in-memory table；
- request-id/trace context extraction；
- method-level rate/admission policy（必须非阻塞）。

## 6. SERVER_POST_HANDLER

Unary：worker result 回 Reactor owner 后、RESPONSE 编码前运行。

Streaming：`finish(status)` 已通过 cardinality/initial-metadata 合法性检查后、final STATUS 编码前运行。

因此 hook 可以：

- 观察最终 status；
- Unary 在首个 RESPONSE 前补 response initial metadata；
- Streaming 在 STATUS 前补 trailing metadata；
- 结束 trace span / 记录 lightweight metrics。

若一个 Streaming `finish()` 尝试本身非法并返回 `TR_ERR_STATE`，POST hook 不会提前触发。POST 每 Call 最多一次。

POST 返回值 V1 忽略，建议返回 `OK`。

## 7. CLIENT_POST_CALL

执行在应用 terminal callback 入队前。

覆盖：

- Unary normal response；
- Streaming final STATUS；
- explicit cancel；
- deadline exceeded；
- Stream/Channel unavailable terminal。

因此业务 result/FINISHED/ERROR callback 运行时，Client POST 已经完成。

正常 Streaming STATUS 已导入 trailers 后才运行 Client POST，所以 hook 可以读取 final trailing metadata。

POST 每 Call 最多一次，返回值 V1 忽略。

## 8. Metadata usage pattern

典型 tracing/auth：

```text
CLIENT_PRE_CALL
  -> set initial metadata: trace-id / auth-token

SERVER_PRE_HANDLER
  -> read peer initial metadata
  -> auth / trace start

SERVER_POST_HANDLER
  -> set trailing metadata: server-id / retry-after / trace result

CLIENT_POST_CALL
  -> read peer trailers
  -> close trace / metrics
```

Initial/Trailing scope 仍遵守 `09-rpc-context-metadata.md`，Interceptor 不增加第三种 metadata namespace。

## 9. Ownership

Interceptor callback 持有的 `tr_rpc_call_handle` 是 phase-duration capability。

它不能获得：

```text
tr_rpc_endpoint *
tr_stream_handle
Reactor pointer
slot/generation
executor queue/node
```

Call mutable protocol state 仍然只有 Reactor owner。

## 10. Failure semantics

Interceptor callback 本身没有 library-error channel。返回值按 RPC status 解释，但 V1 只有 SERVER_PRE 使用它做 rejection。

因此 hook 内部若出现本地实现错误，推荐：

- SERVER_PRE 返回 `INTERNAL`；
- 其他 phase 自行记录诊断并返回 `OK`；
- 不抛出跨 Call 的 transport close。

## 11. V1 non-goals

暂不支持：

- dynamic interceptor chain；
- message-level interceptor；
- blocking interceptor；
- async interceptor continuation；
- client PRE local rejection；
- interceptor priority/reordering；
- per-Method interceptor list；
- hook 直接访问 transport internals。

这些能力只有在真实需求/profile 出现后再扩展。