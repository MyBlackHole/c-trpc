# RPC 上下文与元数据

> 状态：当前 V1 上下文/元数据基础

本文定义 Call 上下文、初始元数据与尾部元数据的 V1 语义，并规定未来拦截器只能依赖这些稳定能力，不能直接访问 Endpoint/Stream/槽位。

## 1. Call 上下文

`tr_rpc_call_get_context()` 返回所有者一致快照：

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

上下文是只读快照，不是存活 Call 指针。它不暴露 Reactor、Endpoint、Stream、槽位、代次、执行器队列等引擎内部标识。

`deadline_remaining_ms` 是读取时刻基于 `CLOCK_MONOTONIC` 计算的相对值；没有截止时间时 `has_deadline == 0`。

## 2. 元数据范围

V1 明确区分两个范围：

```text
初始元数据
  第一条 REQUEST / 第一条 RESPONSE

尾部元数据
  仅最终 STATUS
```

Call 内部使用四块有界存储：

```text
local_initial
peer_initial
local_trailing
peer_trailing
```

每块上限仍为 `TR_RPC_METADATA_MAX_BYTES`，不存在无界元数据分配。

## 3. 初始元数据

客户端 Call 选项中的 `metadata[]` 与 `tr_rpc_call_set_metadata()` 都写入本地初始元数据。

规则：

- 只能在本方向第一条业务信封提交前修改；
- 客户端初始元数据跟随第一条 `REQUEST`；
- 服务端初始元数据跟随第一条 `RESPONSE`；
- 重复键拒绝；
- 用户键不能以 `:` 开头；
- 保留键 `:timeout-ms` 只允许协议内部放在第一条 `REQUEST` 中。

如果服务端流式调用的响应基数为 MANY 且最终没有 `RESPONSE`，则没有信封可以表达初始响应元数据。应用如果已经设置初始元数据，此时 `finish()` 返回 `TR_ERR_STATE`；应改用尾部元数据，而不是静默把初始元数据重新分类为尾部元数据。

## 4. 尾部元数据

V1 尾部元数据只支持服务端流式调用 -> 客户端：

```text
RESPONSE*
STATUS + 尾部元数据
传输关闭
```

API：

```c
tr_rpc_call_set_trailing_metadata(...);
tr_rpc_call_get_peer_trailing_metadata(...);
```

规则：

- 仅服务端流式 Call；
- 必须在最终 `STATUS` 提交前设置；
- `STATUS` 已提交或 Call 终止后不可修改；
- 客户端在最终 `STATUS` 到达前读取返回 `TR_AGAIN`；
- 客户端 `FINISHED` 回调内可以稳定读取尾部元数据；
- 一元调用 V1 没有独立 `STATUS` 信封，因此不支持尾部元数据。

`STATUS` 元数据始终解释为尾部元数据，即使前面已经有多个 `RESPONSE`。它不会覆盖第一条 `RESPONSE` 的初始元数据。

## 5. 截止时间传播

客户端本地截止时间使用单调时钟定时器；首次 `REQUEST` 额外携带保留的 `:timeout-ms` 相对元数据。服务端收到后按自己的 `CLOCK_MONOTONIC` 重新建立截止时间。

上下文只暴露相对剩余时间快照，不暴露跨机器不可比较的绝对单调时钟时间戳。

## 6. 取消

上下文中的取消字段是读取时刻快照：

```text
cancelled == 0 -> cancel_status = OK
cancelled == 1 -> cancel_status = CANCELLED / DEADLINE_EXCEEDED
```

业务处理器如果需要持续观察取消状态，仍可使用 `tr_rpc_call_is_cancelled()`；上下文不承诺自动更新。

## 7. 所有权

上下文/元数据 API 都通过同步 Reactor 所有者调用读取或修改 Call 协议状态。工作线程不直接持有 Endpoint/Call 可变指针，也不能通过元数据 API 绕过所有者。

`FINISHED` 回调之所以仍能读取上下文/尾部元数据，是因为回调任务的强引用/`task_refs` 在回调完成前保持 Call 能力有效。

## 8. 未来拦截器边界

未来拦截器不应获得：

```text
tr_rpc_endpoint *
tr_stream_handle
Call slot/generation
Reactor pointer
executor internals
```

它的稳定输入应建立在：

```text
tr_rpc_call_handle 能力句柄
+ tr_rpc_context 快照
+ 初始/尾部元数据 API
+ 显式拦截器阶段/状态
```

这样认证、链路追踪、指标、请求标识、限流都可以扩展，而不会成为 RPC 协议状态的共同所有者。

在冻结以下契约前不应公开拦截器配置：

1. 钩子运行在所有者还是工作线程；
2. 钩子是否允许阻塞；
3. 钩子返回库错误还是 RPC 状态；
4. 客户端/服务端开始/结束阶段；
5. 钩子对初始/尾部元数据的修改窗口；
6. 截止时间/取消与钩子的顺序。

## 9. 长期不变量

1. 初始元数据与尾部元数据永不共用同一逻辑范围；
2. `STATUS` 元数据永远是尾部元数据；
3. 保留协议元数据不暴露成用户键；
4. 上下文不暴露引擎内部标识；
5. 元数据存储永远有界；
6. 工作线程不直接修改 Call 元数据存储；
7. `FINISHED` 回调可以读取最终上下文/尾部元数据；
8. 拦截器不能重新引入共享可变协议所有权。
