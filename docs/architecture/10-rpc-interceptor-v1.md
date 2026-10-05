# RPC V1 拦截器

> 状态：当前 V1 契约

V1 拦截器是 Call 级控制钩子，不是消息中间件，也不是新的工作线程执行域。

## 1. 公开形态

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

客户端/服务端门面各配置一个 `struct tr_rpc_interceptor`。服务端创建每个对端 RPC Endpoint 时复制同一钩子/参数；钩子参数的生命周期必须覆盖对应客户端/服务端。

V1 不建立动态中间件列表。需要组合认证/链路追踪/指标时，由应用提供一个固定组合回调。

## 2. 执行域

所有阶段都同步运行在 Call 所属的 Reactor 所有者上。

要求：

- 钩子必须短小、非阻塞；
- 不允许磁盘、网络、KMS、数据库等待；
- 不允许等待依赖同一 Reactor 前进的 future/condition；
- 钩子运行时不会持有 `endpoint->lock`。

最后一条很重要：钩子可以安全调用所有者安全的上下文/元数据 API，而不会死锁。

需要阻塞认证时，V1 拦截器不负责执行；应由未来的异步认证工作单元或应用处理器承担，而不是阻塞 Reactor。

## 3. 允许的重入

拦截器内允许：

```text
tr_rpc_call_get_context()
tr_rpc_call_get_peer_metadata()
tr_rpc_call_get_peer_trailing_metadata()
tr_rpc_call_set_metadata()               （仍处于合法窗口时）
tr_rpc_call_set_trailing_metadata()      （服务端流式 STATUS 前）
tr_rpc_call_is_cancelled()
```

同一个 Call 在钩子执行期间禁止：

```text
send
send_buffer
close_send
finish
cancel
```

这些 API 会返回 `TR_ERR_STATE`。这是运行时所有权保护，不只是文档约定。

拦截器也不应从钩子内创建递归 RPC，或等待会反向依赖当前所有者轮次的工作。

## 4. CLIENT_PRE_CALL

执行点：

```text
分配 Call
应用选项/截止时间/Call 初始元数据
CLIENT_PRE_CALL
编码/打开第一条请求路径
```

因此钩子可以：

- 读取 Method 标识/基数/截止时间；
- 注入认证令牌/请求标识/链路追踪初始元数据；
- 执行轻量指标记录/链路追踪开始。

V1 `CLIENT_PRE_CALL` 的返回值不用于本地 RPC 拒绝，建议返回 `OK`。若后续要支持客户端策略拒绝，应单独定义 Call 结果语义，而不是把 RPC 状态偷偷映射成库错误。

客户端前置阶段发生在 Stream 打开之前，所以后续 Stream 打开失败时钩子仍可能已经执行；它表示一次 Call 尝试，而不是“已经发送到对端”。

## 5. SERVER_PRE_HANDLER

执行点：

```text
解码第一条 REQUEST
分配/绑定服务端 Call
导入对端初始元数据
校验请求基数
SERVER_PRE_HANDLER
执行器准入
应用处理器
```

这是 V1 唯一允许通过返回值执行准入拒绝的阶段。

规则：

- `OK`：继续；
- 合法非 `OK` RPC 状态：在应用处理器运行前拒绝；
- 非法状态：归一化为 `INTERNAL`。

一元调用拒绝使用现有一元响应/状态路径；流式调用拒绝使用最终 `STATUS` 路径。两者都属于 RPC 级拒绝，不关闭整个连接。

服务端前置阶段可以读取请求初始元数据，因此适合：

- 令牌/租户轻量校验；
- 预加载内存表中的 ACL 查询；
- 提取请求标识/链路追踪上下文；
- 方法级限流/准入策略（必须非阻塞）。

## 6. SERVER_POST_HANDLER

一元调用：工作线程结果回到 Reactor 所有者后、`RESPONSE` 编码前运行。

流式调用：`finish(status)` 已通过基数/初始元数据合法性检查后、最终 `STATUS` 编码前运行。

因此钩子可以：

- 观察最终状态；
- 一元调用在第一条 `RESPONSE` 前补充响应初始元数据；
- 流式调用在 `STATUS` 前补充尾部元数据；
- 结束链路追踪区间/记录轻量指标。

如果一次流式 `finish()` 尝试本身非法并返回 `TR_ERR_STATE`，后置钩子不会提前触发。每个 Call 的后置钩子最多一次。

V1 忽略后置钩子的返回值，建议返回 `OK`。

## 7. CLIENT_POST_CALL

在应用终止回调入队前执行。

覆盖：

- 一元调用正常响应；
- 流式调用最终 `STATUS`；
- 显式取消；
- 截止时间超时；
- Stream/Channel 不可用终止。

因此业务结果/`FINISHED`/`ERROR` 回调运行时，客户端后置阶段已经完成。

正常流式 `STATUS` 已经导入尾部元数据后才运行客户端后置阶段，所以钩子可以读取最终尾部元数据。

每个 Call 的后置阶段最多一次，V1 忽略返回值。

## 8. 元数据使用方式

典型链路追踪/认证流程：

```text
CLIENT_PRE_CALL
  -> 设置初始元数据：trace-id / auth-token

SERVER_PRE_HANDLER
  -> 读取对端初始元数据
  -> 认证 / 开始链路追踪

SERVER_POST_HANDLER
  -> 设置尾部元数据：server-id / retry-after / trace result

CLIENT_POST_CALL
  -> 读取对端尾部元数据
  -> 结束链路追踪 / 指标记录
```

初始/尾部范围仍遵守 `09-rpc-context-metadata.md`，拦截器不增加第三种元数据命名空间。

## 9. 所有权

拦截器回调持有的 `tr_rpc_call_handle` 是仅在当前阶段有效的能力句柄。

它不能获得：

```text
tr_rpc_endpoint *
tr_stream_handle
Reactor pointer
slot/generation
executor queue/node
```

Call 可变协议状态仍然只有 Reactor 所有者可以修改。

## 10. 故障语义

拦截器回调本身没有库错误通道。返回值按 RPC 状态解释，但 V1 只有 `SERVER_PRE_HANDLER` 使用它执行拒绝。

因此钩子内部如果出现本地实现错误，推荐：

- `SERVER_PRE_HANDLER` 返回 `INTERNAL`；
- 其他阶段自行记录诊断并返回 `OK`；
- 不触发跨 Call 的传输连接关闭。

## 11. V1 非目标

暂不支持：

- 动态拦截器链；
- 消息级拦截器；
- 阻塞式拦截器；
- 异步拦截器续处理；
- 客户端前置阶段本地拒绝；
- 拦截器优先级/重排序；
- 每 Method 拦截器列表；
- 钩子直接访问传输层内部实现。

这些能力只有在真实需求或性能分析出现后再扩展。
