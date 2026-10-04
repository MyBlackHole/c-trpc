# RPC Streaming Conformance

> Status: CURRENT V1 contract

本文冻结 c-trpc RPC 的四种 cardinality、half-close、final STATUS 和 terminal callback 语义。

## 1. 四种 RPC shape

| Request | Response | Shape | V1 request count | STATUS(OK) response count |
| --- | --- | --- | --- | --- |
| ONE | ONE | Unary | exactly 1 | exactly 1 |
| ONE | MANY | Server Streaming | exactly 1 | 0..N |
| MANY | ONE | Client Streaming | **1..N** | exactly 1 |
| MANY | MANY | Bidirectional Streaming | **1..N** | 0..N |

V1 的 MANY request 不是 0..N。当前 wire 没有独立 Method-open envelope；第一条 REQUEST 同时携带 service_id、method_id、codec、initial metadata 和第一条业务消息。因此 0 条 REQUEST 时 Server 无法确定 Method。未来若要支持 zero-message streaming，应新增显式 Method-open envelope，而不是把空业务消息当 open。

## 2. 统一 Call 模型

```text
FREE -> OPENING -> ACTIVE -> TERMINAL -> FREE(next generation)

request direction            response direction
request cardinality          response cardinality
tx/rx message count          tx/rx message count
request half-close           final STATUS
```

half-close 不等于 Call terminal；Call generation 防止 slot reuse ABA。

## 3. Client request half-close

`tr_rpc_call_close_send()` 是 Client request-side half-close API。

- Client Endpoint only；
- ONE 必须已经成功发送 exactly 1 条 REQUEST；
- MANY 在 V1 必须已经成功发送至少 1 条 REQUEST；
- terminal/cancel/local-close 后禁止再次 half-close。

Server 不允许用 `tr_rpc_call_close_send()` 结束 response side。Server 必须调用 `tr_rpc_call_finish(status)`，保证 final STATUS 先于 response-side transport close。

## 4. Server final STATUS

`tr_rpc_call_finish(call, status)` 的规则：

- status 必须位于有效 `TR_RPC_STATUS_*` 域；
- STATUS payload 为 0；
- service_id/method_id/response codec 与 Method 一致；
- `STATUS(OK)` + response ONE 时必须已经发送 exactly 1 条 RESPONSE；
- response MANY 允许 `STATUS(OK)` 前发送 0..N 条 RESPONSE；
- 非 OK STATUS 允许在 nominal ONE response 尚未产生时终止；
- final STATUS 只能发送一次。

正常顺序：

```text
RESPONSE*
STATUS
transport response-half close
```

STATUS 后禁止继续发送业务 RESPONSE。

## 5. Client STATUS validation

Client 收到 streaming STATUS 时必须验证：known streaming Call、同 service/method、同 response codec、合法 status、zero payload、STATUS 未重复。

对于 `STATUS(OK)` + response ONE，还要求之前已经收到 exactly 1 条 RESPONSE。STATUS 之后任何 RESPONSE 都是协议错误。

## 6. Client event ordering

```text
OPENED
[WRITABLE]*
[MESSAGE]*
FINISHED(status)
```

`FINISHED` 是 application terminal barrier：

- FINISHED 后不允许 MESSAGE；
- FINISHED 后不允许 WRITABLE；
- 正常 STATUS 后的 transport half-close 不再额外上报 REMOTE_CLOSED；
- FINISHED 每 Call 最多一次。

`REMOTE_CLOSED` 只在尚未观察到 final STATUS 时有意义。Stream 在 final STATUS 前异常结束时，Client 得到 terminal `ERROR(UNAVAILABLE)`，不能伪造成功 FINISHED。

## 7. Server callback ordering

同一 Call 的 Server worker callback 由 per-Call executor FIFO 串行：

```text
on_open
on_message #1
...
on_message #N
on_half_close
on_close(status)
```

half-close 不允许越过尚未处理的 message callback；`on_close` 是 Server application terminal callback，最多一次。

## 8. Shape lifecycle

### Unary: ONE -> ONE

Client REQUEST -> Server handler -> Server RESPONSE -> terminal。

### Server Streaming: ONE -> MANY

```text
Client REQUEST
Client close_send
Server on_message
Server on_half_close
Server RESPONSE*
Server STATUS
Client FINISHED
```

Client 第二条 REQUEST 必须失败。

### Client Streaming: MANY -> ONE

```text
Client REQUEST+
Client close_send
Server on_message+
Server on_half_close
Server RESPONSE exactly one
Server STATUS(OK)
Client FINISHED
```

Server 未发送唯一 RESPONSE 时 `finish(OK)` 必须失败。

### Bidirectional Streaming: MANY -> MANY

REQUEST 与 RESPONSE 独立推进，不要求 1:1 配对。Client 结束 request side 用 close_send；Server 最终用 finish(status) 发送 STATUS 并结束 Call。

## 9. Cancel / deadline

Cancel 与 deadline 进入统一 terminal transition：停止新 callback admission、清 deadline、清/取消 pending continuation、发布 terminal callback once、best-effort CANCEL/control、关闭 Stream。

允许的 cancel status：`CANCELLED`、`DEADLINE_EXCEEDED`。

## 10. Overload

- first-message admission 失败：final `RESOURCE_EXHAUSTED`；
- 已接受 Call 的 continuation pressure：bounded per-Call pending continuation；
- 无法继续时终止该 Call，不建立 unbounded side queue；
- continuation 优先于新 Call admission。

## 11. Status domain

V1 使用 0..16 的标准 RPC status 域，包括 `UNKNOWN = 2`。

- Streaming `finish()` 收到非法 status：`TR_ERR_INVALID`；
- Unary handler 返回非法 status：归一化为 `INTERNAL`；
- peer wire status 非法：协议错误。

## 12. 长期不变量

1. ONE 方向最多一条业务消息；
2. V1 Client MANY request 在 close_send 前至少一条业务消息；
3. Server response termination 必须经过 final STATUS；
4. `STATUS(OK)` + response ONE 必须有 exactly one RESPONSE；
5. STATUS 后不允许 RESPONSE；
6. FINISHED 是 Client 最后一个正常 application event；
7. terminal callback 每 Call 最多一次；
8. half-close 不等于 Call terminal；
9. Call generation 防止旧 callback/completion 命中新 Call；
10. executor backpressure 不改变 per-Call callback 顺序。

## 13. Future

V1 暂不支持 zero-message Client/Bidi streaming、独立 Method-open envelope、transparent streaming replay/reconnect、automatic streaming retry。