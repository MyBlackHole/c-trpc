# RPC V2 父子调用上下文设计

## 目标

让服务端业务工作可以安全地把父 Call 的取消和绝对期限传给一个或多个下游
RPC Call。子 Call 使用自己的 `timeout_ms` 时，其绝对期限必须是本地期限与父期限
中的较早者；父 Call 终止后，尚未终止的子 Call 不得继续交付业务消息或把迟到工作
重新准入。

本设计实现 Issue #243 的父子取消/期限子项，并补足跨回调的有限所有权。它不关闭
#243 的全部生命周期验收，也不引入透明重试或业务副作用回滚。

## 公共接口

在 `tr_rpc_v2.h` 增加唯一拥有型 `struct tr_rpc_v2_call_context`，以及：

```c
int tr_rpc_v2_cancel_context_hold(
        const struct tr_rpc_v2_cancel *cancel,
        struct tr_rpc_v2_call_context *out);
int tr_rpc_v2_call_context_release(
        struct tr_rpc_v2_call_context *context);
```

`hold` 只允许在对应 Server V2 `work` 回调中调用；成功返回的句柄可跨回调保存。
句柄不得复制成多个所有者，但可在其所有者仍持有期间以只读方式传给多个并发
`tr_client_call_start`。每个成功受理的 Client Call 独立取得一份内部引用。应用必须
避免 `release` 与尚未返回的 `call_start` 并发；成功释放会清零句柄。

`hold` 的 NULL 参数或非零 `out` 返回 `TR_ERR_INVALID`，仅在 `TR_OK` 时修改 `out`。
无效执行域返回 `TR_ERR_STATE`，状态池耗尽返回 `TR_AGAIN`，期限已过返回
`TR_ERR_TIMEOUT`，取消已发布返回 `TR_ERR_CANCELLED`。`release(NULL)` 返回
`TR_ERR_INVALID`；成功时清零输入句柄，释放陈旧代次返回 `TR_ERR_STALE`。Server
因上下文引用返回 `TR_AGAIN` 时仍保留状态池，句柄可释放后再重试销毁。

在 `struct tr_rpc_call_options` 尾部增加借用指针
`const struct tr_rpc_v2_call_context *parent_context`。`call_start` 只在同步受理期间
借用该指针；只有 `TR_OK` 会让 Client Call 固定持有内部引用。零初始化保持原有行为。

## 生命周期与资源

- 每个 Server Dispatch 固定预留与 Call 容量相同数量的上下文状态槽；状态槽属于
  Dispatch 已记账的固定内存，不为每个上下文动态分配内存或线程。
- 首次 `hold` 时从固定状态槽中取得一个状态，并与当前父 Call 的绝对期限绑定。
  同一父 Call 后续 `hold` 可复用该状态；池耗尽返回 `TR_AGAIN`，不接管输出。
- 状态槽有独立引用计数和不可回绕代次。父 Call 退役时放弃其内部基准引用；外部
  句柄和子 Call 引用仍可保持上下文状态，但不占用已关闭的网络 Stream 槽。
- 外部上下文引用数非零时，Server 销毁返回 `TR_AGAIN` 并保留对象。最后一个上下文
  引用释放后，调用方可以重试销毁。无效或代次不匹配的句柄返回 `TR_ERR_STALE`。
- 每个成功受理的子 Call 在自己的固定 Client Call 槽中保存一份上下文引用，并在
  该槽完全退役后释放。其内存仍由原 Client Call 槽和 Server 上下文池预算约束。

## 取消、期限和终局语义

- 父取消以原子状态发布；父期限作为不可变的 `CLOCK_MONOTONIC` 绝对时间保存。
  子 Client 通过既有进度泵观察父取消，不进行跨 Reactor 同步等待或新增阻塞队列。
- 子 Call 的有效期限为 `min(parent_absolute_deadline,
  child_accept_time + timeout_ms)`；`timeout_ms == 0` 表示没有额外子期限，不会清除
  父期限。子期限较短只终止该子 Call，不改变父或兄弟 Call。
- 在子 Call 尚未受理时，已取消父上下文返回 `TR_ERR_CANCELLED`，已过期父期限返回
  `TR_ERR_TIMEOUT`；不写输出句柄、不调用用户回调，也不接管请求正文。
- 上下文在子 Call 受理后取消时，子 Call 以 `TR_RPC_STATUS_CANCELLED` 终止；父期限
  到期时以 `TR_RPC_STATUS_DEADLINE_EXCEEDED` 终止。`FINAL_STATUS`、取消和期限仍由
  现有单 owner 状态机选择唯一结果。
- 父 Call 一旦选择任意终局，仍活动的子 Call 被取消，防止父请求结束后遗留下游工作。
  父期限终局保留 `DEADLINE_EXCEEDED`；其他父终局对仍活动子 Call 映射为 `CANCELLED`。
- 子 Client 在每次进度和消息回调入口检查上下文。取消已可见时，先提交子 Call 终局，
  不向应用交付随后到达的消息/元数据；已经先由子 Call 状态机接纳的终局保持
  first-final-wins。
- Server 停止时先取消全部活动上下文；清理返回 `TR_AGAIN` 期间，已存在上下文只可
  释放，不能再创建新子 Call。应用业务线程仍须合作退出，库不强制终止线程或撤销业务
  副作用。

## 线程与代次

上下文状态槽由固定 Dispatch 池拥有。父 owner 发布终局，子 Client owner 以 acquire
语义读取；引用计数使用原子操作。公开句柄携带状态槽指针与代次，状态槽只有在所有
引用归零后才能复用。所有状态检查在取得引用之后进行；引用释放完成后不再访问状态槽
或 Dispatch，避免 Server 销毁与子 Client 回收交错时的悬空访问。

取消通知不调用另一个 Reactor 的同步入口。子 Client 的既有定时进度周期给出传播
上界；父期限本身直接参与子 Call 的绝对期限计算，不受该轮询周期延长。

## 验收

1. 父取消后，活动子 Call 恰好交付一次 `CANCELLED`，后续消息不进入应用回调。
2. 父期限短于子 `timeout_ms` 时，子 Call 以 `DEADLINE_EXCEEDED` 终结；零子期限
   继承父期限，较短子期限只影响自身。
3. 已取消/过期上下文不能受理新子 Call，失败不修改 `out`、不回调、不接管正文。
4. 父上下文被释放前已有多个子 Call 受理时，各子 Call 独立持有引用；父 Server
   销毁在这些引用归零前返回 `TR_AGAIN`，归零后可重试成功。
5. 状态池耗尽以 `TR_AGAIN` 明确拒绝，不溢出、回绕或分配动态内存；状态槽复用后旧
   代次句柄不能访问新父 Call。
6. 父终局、子 FINAL、子取消、期限、迟到消息和 Server/Client 停止交错时，唯一终局、
   回调屏障及所有引用/预算均收敛。
7. GCC Debug/Release、Clang Release、ASan/UBSan、TSan 和安装后 C11/C++11 SDK 消费端
   覆盖公开声明及核心生命周期。
