# Reactor 定时器队列：槽位与回调边界

`src/timer_queue.*` 是 Reactor owner 内部组件，不提供跨线程同步。
外部调用仍通过 Reactor 的 register / arm / unregister 接口串行化。
本次变更不新增线程，不改变公开 API，也不迁移 reconnect。

## 两种独立的资源状态

```text
free slot --register--> registered, disarmed --arm(t)--> armed
    ^                         ^                         |
    |                         +--arm(0) / one-shot fire--+
    +----------------------unregister-------------------+
```

注册槽位和最小堆成员不是同一概念：disarm 或一次性回调结束只移出堆，
不会释放注册槽位。只有 unregister 将槽位放回空闲链表。

初始化时按槽位索引建立空闲链表。注册弹出表头，注销将槽位压回表头。
已注册槽位使用 `heap_pos`，空闲槽位使用同一 union 中的 `next_free`，
因此不需要为每个定时器增加独立的链表字段或额外分配。
`free_head` 为队列级字段；槽位复用顺序不属于接口保证。

| 操作 | 复杂度 |
|---|---|
| 初始化 | O(capacity) |
| 注册 / 容量耗尽检查 | O(1) |
| 空闲槽位入链 | O(1) |
| arm / disarm / 注销已入堆定时器 | O(log n)，n 为已入堆数量 |
| 查询最近 deadline | O(1) |

队列容量仍固定，注册失败仍返回 `TR_AGAIN`，且不修改输出 token。
注册、回收与调度路径均不新增动态分配。

## 复用与回调修改

同一存活队列内，槽位复用继续递增 generation，跳过零。
失效 token 的 arm / unregister 返回 `TR_ERR_STALE`，不会重复入链。
该机制没有扩大为跨队列生命周期或 generation 完整回绕后的保证。

回调内显式 arm / disarm 会改变 version；unregister 后重新注册会改变
该槽位的 generation。旧回调返回的下一 deadline 不得覆盖这些显式操作。
回调可以在容量为一的队列中注销自身并立即复用该槽位。

`run_due(..., max_callbacks=0, ...)` 不运行回调，但仍准确设置
`has_more_due`。NULL / 空队列报告 0，存在已到期定时器时报告 1。

## 验证入口

```sh
make test-timer CC=gcc
make clean
make test-timer CC=clang
make test
```

独立测试包含容量耗尽、disarm 保留槽位、失效 token、回调显式修改与
自注销复用、相同 deadline 的预算边界，以及固定种子的模型测试。
模型测试覆盖容量 1、7、31，各执行 20,000 次操作，每步核对空闲链表、
已注册集合、堆成员集合、堆顺序、最近 deadline 和模型状态。
另有 10,000 次高占用槽位复用检查；测试不使用墙钟耗时作为性能门槛。

`make test` 同时执行原有 Transport/RPC 集成测试和新的定时器测试，
因此现有 GCC、Clang、ASan/UBSan 与 TSan CI 矩阵都会覆盖新测试。
