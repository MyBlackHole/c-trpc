# RPC Context and Metadata

> Status: CURRENT V1 context/metadata foundation

本文定义 Call Context、initial metadata 与 trailing metadata 的 V1 语义，并规定未来 Interceptor 只能依赖这些稳定能力，不直接访问 Endpoint/Stream/slot。

## 1. Call Context

`tr_rpc_call_get_context()` 返回 owner-consistent snapshot：

```text
service_id
method_id
request_cardinality
response_cardinality
has_deadline
deadline_remaining_ms
cancelled
cancel_status
```

Context 是只读 snapshot，不是 live Call pointer。它不暴露 Reactor、Endpoint、Stream、slot、generation、executor queue 等 engine identity。

`deadline_remaining_ms` 是读取时刻基于 CLOCK_MONOTONIC 的相对值；没有 deadline 时 `has_deadline == 0`。

## 2. Metadata scopes

V1 明确区分两个 scope：

```text
initial metadata
  first REQUEST / first RESPONSE

trailing metadata
  final STATUS only
```

Call 内部使用四块有界 storage：

```text
local_initial
peer_initial
local_trailing
peer_trailing
```

每块上限仍为 `TR_RPC_METADATA_MAX_BYTES`，不存在 unbounded metadata allocation。

## 3. Initial metadata

Client Call options 的 `metadata[]` 与 `tr_rpc_call_set_metadata()` 都写 local initial metadata。

规则：

- 只能在本方向第一条业务 envelope 提交前修改；
- Client initial metadata 跟 first REQUEST；
- Server initial metadata 跟 first RESPONSE；
- duplicate key 拒绝；
- user key 不能以 `:` 开头；
- reserved `:timeout-ms` 只允许协议内部放在 first REQUEST。

如果 Server Streaming response cardinality MANY 最终 0 条 RESPONSE，则没有 envelope 可以表达 initial response metadata。应用若已经设置 initial metadata，此时 `finish()` 返回 `TR_ERR_STATE`；需要使用 trailing metadata，而不是静默把 initial 重分类成 trailers。

## 4. Trailing metadata

V1 trailing metadata 只支持 Server Streaming -> Client：

```text
RESPONSE*
STATUS + trailing metadata
transport close
```

API：

```c
tr_rpc_call_set_trailing_metadata(...);
tr_rpc_call_get_peer_trailing_metadata(...);
```

规则：

- Server streaming Call only；
- 必须在 final STATUS 提交前设置；
- STATUS 已提交或 Call terminal 后不可修改；
- Client 在 final STATUS 到达前读取返回 `TR_AGAIN`；
- Client FINISHED callback 内可稳定读取 trailers；
- Unary V1 没有独立 STATUS envelope，因此不支持 trailing metadata。

STATUS metadata 永远解释为 trailers，即使前面已经有多个 RESPONSE。它不会覆盖 first RESPONSE 的 initial metadata。

## 5. Deadline propagation

Client local deadline 使用 monotonic timer；首次 REQUEST 额外携带 reserved `:timeout-ms` relative metadata。Server 收到后按自己的 CLOCK_MONOTONIC 重新建立 deadline。

Context 只暴露相对 remaining snapshot，不暴露跨机器不可比较的绝对 monotonic timestamp。

## 6. Cancellation

Context 中的 cancellation 字段是读取时刻 snapshot：

```text
cancelled == 0 -> cancel_status = OK
cancelled == 1 -> cancel_status = CANCELLED / DEADLINE_EXCEEDED
```

业务 handler 若需要持续观察 cancellation，仍可使用 `tr_rpc_call_is_cancelled()`；Context 不承诺自动更新。

## 7. Ownership

Context/metadata API 都通过 synchronous Reactor owner-call 读取或修改 Call protocol state。Worker 不直接持有 Endpoint/Call mutable pointer，也不通过 metadata API 绕过 owner。

FINISHED callback 之所以仍能读取 Context/Trailers，是因为 callback task 的 strong-ref/task_refs 在 callback 完成前保持 Call capability 有效。

## 8. Future Interceptor boundary

未来 Interceptor 不应获得：

```text
tr_rpc_endpoint *
tr_stream_handle
Call slot/generation
Reactor pointer
executor internals
```

它的稳定输入应建立在：

```text
tr_rpc_call_handle capability
+ tr_rpc_context snapshot
+ initial/trailing metadata API
+ explicit interceptor phase/status
```

这样 auth、trace、metrics、request-id、rate-limit 可以扩展，而不成为 RPC protocol state 的共同 owner。

在冻结以下 contract 前不应公开 Interceptor config：

1. hook 执行在 owner 还是 worker；
2. hook 是否允许阻塞；
3. hook 返回 library error 还是 RPC status；
4. client/server start/finish phase；
5. hook 对 initial/trailing metadata 的修改窗口；
6. deadline/cancellation 与 hook 的 ordering。

## 9. Long-term invariants

1. initial 与 trailing metadata 永不共用同一 logical scope；
2. STATUS metadata 永远是 trailers；
3. reserved protocol metadata 不暴露成 user key；
4. Context 不暴露 engine identity；
5. metadata storage 永远 bounded；
6. worker 不直接修改 Call metadata storage；
7. FINISHED callback 可读取 final Context/Trailers；
8. Interceptor 不能重新引入 shared mutable protocol ownership。