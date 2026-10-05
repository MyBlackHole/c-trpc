# RPC 流式调用一致性

> 状态：当前 V1 契约

本文冻结 c-trpc RPC 的四种基数关系、半关闭、最终 `STATUS` 和终止回调语义。

## 1. 四种 RPC 形态

| 请求 | 响应 | 形态 | V1 请求数量 | `STATUS(OK)` 响应数量 |
| --- | --- | --- | --- | --- |
| ONE | ONE | 一元调用 | 恰好 1 条 | 恰好 1 条 |
| ONE | MANY | 服务端流式调用 | 恰好 1 条 | 0..N |
| MANY | ONE | 客户端流式调用 | **1..N** | 恰好 1 条 |
| MANY | MANY | 双向流式调用 | **1..N** | 0..N |

V1 的 MANY 请求不是 0..N。当前线协议没有独立的 Method 打开信封；第一条 `REQUEST` 同时携带 `service_id`、`method_id`、编解码器、初始元数据和第一条业务消息。因此没有 `REQUEST` 时服务端无法确定 Method。未来若要支持零消息流式调用，应新增显式 Method 打开信封，而不是把空业务消息当作打开动作。

## 2. 统一 Call 模型

```text
FREE -> OPENING -> ACTIVE -> TERMINAL -> FREE（下一代次）

请求方向                    响应方向
请求基数                    响应基数
TX/RX 消息计数              TX/RX 消息计数
请求半关闭                  最终 STATUS
```

半关闭不等于 Call 终止；Call 代次用于防止槽位复用时出现 ABA。

## 3. 客户端请求半关闭

`tr_rpc_call_close_send()` 是客户端请求方向的半关闭 API。

- 仅客户端 Endpoint 可用；
- ONE 必须已经成功发送恰好 1 条 `REQUEST`；
- MANY 在 V1 必须已经成功发送至少 1 条 `REQUEST`；
- 终止、取消或本地关闭后禁止再次半关闭。

服务端不允许用 `tr_rpc_call_close_send()` 结束响应方向。服务端必须调用 `tr_rpc_call_finish(status)`，保证最终 `STATUS` 先于响应方向的传输半关闭。

## 4. 服务端最终 STATUS

`tr_rpc_call_finish(call, status)` 的规则：

- `status` 必须位于有效 `TR_RPC_STATUS_*` 域；
- `STATUS` 载荷长度为 0；
- `service_id`、`method_id`、响应编解码器必须与 Method 一致；
- `STATUS(OK)` + 响应 ONE 时必须已经发送恰好 1 条 `RESPONSE`；
- 响应 MANY 允许在 `STATUS(OK)` 前发送 0..N 条 `RESPONSE`；
- 非 `OK` 的 `STATUS` 允许在名义上的 ONE 响应尚未产生时终止；
- 最终 `STATUS` 只能发送一次。

正常顺序：

```text
RESPONSE*
STATUS
传输层响应方向半关闭
```

`STATUS` 后禁止继续发送业务 `RESPONSE`。

## 5. 客户端 STATUS 校验

客户端收到流式 `STATUS` 时必须验证：对应已知流式 Call、服务/方法一致、响应编解码器一致、状态合法、载荷长度为 0、`STATUS` 未重复。

对于 `STATUS(OK)` + 响应 ONE，还要求此前已经收到恰好 1 条 `RESPONSE`。`STATUS` 之后任何 `RESPONSE` 都属于协议错误。

## 6. 客户端事件顺序

```text
OPENED
[WRITABLE]*
[MESSAGE]*
FINISHED(status)
```

`FINISHED` 是应用终止屏障：

- `FINISHED` 后不允许 `MESSAGE`；
- `FINISHED` 后不允许 `WRITABLE`；
- 正常 `STATUS` 后的传输半关闭不再额外上报 `REMOTE_CLOSED`；
- 每个 Call 最多出现一次 `FINISHED`。

`REMOTE_CLOSED` 只在尚未观察到最终 `STATUS` 时有意义。Stream 在最终 `STATUS` 前异常结束时，客户端得到终止 `ERROR(UNAVAILABLE)`，不能伪造成功的 `FINISHED`。

## 7. 服务端回调顺序

同一个 Call 的服务端工作线程回调由每 Call 执行器 FIFO 串行：

```text
on_open
on_message #1
...
on_message #N
on_half_close
on_close(status)
```

半关闭不允许越过尚未处理的消息回调；`on_close` 是服务端应用终止回调，最多一次。

## 8. 各形态生命周期

### 一元调用：ONE -> ONE

客户端 `REQUEST` -> 服务端处理器 -> 服务端 `RESPONSE` -> 终止。

### 服务端流式调用：ONE -> MANY

```text
客户端 REQUEST
客户端 close_send
服务端 on_message
服务端 on_half_close
服务端 RESPONSE*
服务端 STATUS
客户端 FINISHED
```

客户端第二条 `REQUEST` 必须失败。

### 客户端流式调用：MANY -> ONE

```text
客户端 REQUEST+
客户端 close_send
服务端 on_message+
服务端 on_half_close
服务端恰好一个 RESPONSE
服务端 STATUS(OK)
客户端 FINISHED
```

服务端未发送唯一 `RESPONSE` 时，`finish(OK)` 必须失败。

### 双向流式调用：MANY -> MANY

`REQUEST` 与 `RESPONSE` 独立推进，不要求 1:1 配对。客户端结束请求方向使用 `close_send`；服务端最终使用 `finish(status)` 发送 `STATUS` 并结束 Call。

## 9. 取消与截止时间

取消与截止时间进入统一终止状态转换：停止新的回调准入、清除截止时间、清理/取消等待中的续处理、只发布一次终止回调、尽力发送 `CANCEL`/控制消息、关闭 Stream。

允许的取消状态：`CANCELLED`、`DEADLINE_EXCEEDED`。

## 10. 过载

- 第一条消息准入失败：最终返回 `RESOURCE_EXHAUSTED`；
- 已接受 Call 的续处理压力：每 Call 有界等待续处理；
- 无法继续时只终止该 Call，不建立无界旁路队列；
- 续处理优先于新 Call 准入。

## 11. 状态域

V1 使用 0..16 的标准 RPC 状态域，包括 `UNKNOWN = 2`。

- 流式 `finish()` 收到非法状态：`TR_ERR_INVALID`；
- 一元处理器返回非法状态：归一化为 `INTERNAL`；
- 对端线协议状态非法：协议错误。

## 12. 长期不变量

1. ONE 方向最多一条业务消息；
2. V1 客户端 MANY 请求在 `close_send` 前至少一条业务消息；
3. 服务端响应终止必须经过最终 `STATUS`；
4. `STATUS(OK)` + 响应 ONE 必须恰好有一条 `RESPONSE`；
5. `STATUS` 后不允许 `RESPONSE`；
6. `FINISHED` 是客户端最后一个正常应用事件；
7. 每个 Call 的终止回调最多一次；
8. 半关闭不等于 Call 终止；
9. Call 代次防止旧回调/完成事件命中新 Call；
10. 执行器背压不改变每 Call 回调顺序。

## 13. 未来扩展

V1 暂不支持零消息客户端/双向流式调用、独立 Method 打开信封、透明流式重放/重连、自动流式重试。
