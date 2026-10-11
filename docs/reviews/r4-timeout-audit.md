# R4：超时路径收尾审计与就绪判定修复

审计基线：`main@ad0c34fb4cf53d9695a1fa35fded80694637738d`。
本地审计源码来自 PR #191 的 CI 源码归档，其 Git tree
`4bd69fa9fd8e727414a92495632d491e35be661a` 与该主线提交完全一致。

## 本轮结论

PR #189、#190、#191 已统一同步连接与条件变量等待的截止时间构造。
本轮扫描 `src/` 和 `include/` 的等待、时钟、期限及零值入口，发现并修复
`tr_channel_wait_ready_deadline()` 的就绪快速路径仍可接受已过期的成功结果。
本 PR 不把 R4 全部标记为完成：下述异步计时问题仍需独立验证和整改。

## 同步路径清单

| 路径 | 截止时间与零值契约 | 本轮核查 |
| --- | --- | --- |
| `tr_client_connect()` | TCP 与 HELLO 共享一个 `tr_deadline`；配置 0 归一化为默认 5000ms | 新增真实 Client/Channel 路径验证：TCP 消耗 80ms 后，HELLO 仍使用原 100ms 截止时间；失败回滚后同一 Client 可再次连接 |
| `tr_client_group_connect()` | TCP 与 CONTROL 前导发送共用期限；内部配置拒绝 0 | `EINTR`/`EAGAIN` 后不重建期限；正数分段发送后也检查期限；未增加新行为 |
| `tr_deadline_poll_fd()` | 有限等待向上取整并限制到 `INT_MAX`；只有显式无限值使用 -1 | `EINTR`、提前返回、分段等待均重算剩余值；现有 `test_deadline` 保留；真实 Client 测试补充连续中断 |
| Channel / Client 就绪等待 | 0 表示无限等待；有限到期返回 `TR_ERR_TIMEOUT` | 本轮修复拿锁后已就绪的成功判定窗口；条件唤醒后的循环仍使用同一期限 |
| Channel / Client 排空等待 | 0 表示非阻塞状态检查；未排空或有限等待超时返回 `TR_AGAIN` | 保留谓词、等待者计数与关闭屏障，不改成就绪等待的超时返回码 |
| `tr_server_drain()` | 0 逐 peer 检查；非零共享一个截止时间 | 下一 peer 前总期限耗尽返回 `TR_ERR_TIMEOUT`；单 Channel 的 `TR_AGAIN` 聚合规则不变 |
| Client Group 排空等待 | 0 表示无限等待；有限超时返回 `TR_ERR_TIMEOUT` | 共用 `tr_deadline_init_ms()`；代际失效、关闭和错误传播不变 |
| Pipeline Listener 排空等待 | 0 表示无限等待；有限超时返回 `TR_ERR_TIMEOUT` | 同上；修正文档中把所有排空超时笼统写成 `TR_AGAIN` 的错误 |

库内只有 `deadline.c` 执行同步 `poll()`；四处
`pthread_cond_timedwait()` 分别属于 Channel 的就绪/排空等待、Client Group
和 Pipeline Listener。Reactor 使用 `epoll_wait()`，在 `EINTR` 后返回主循环，
重新处理定时器并计算等待值。

未携带超时参数的队列容量等待、Reactor 同步命令、执行器休眠、引用排空和
Server finalizer 等待属于生命周期/调度屏障，不应机械添加独立计时器。
这些屏障可能超过业务期限；本轮保证到期不能被误报为握手成功，
不承诺任意调度、锁竞争或用户回调下整个 API 的返回耗时都有硬上限。

## 已修复：就绪快速路径跨越截止时间

旧路径：

```text
锁外读取单调时钟：未到期
  -> 等待 channel->lock，期间期限耗尽
  -> 取得锁，发现 control_ready 或 bulk_ready
  -> 直接返回 TR_OK
```

修复只在“持锁观察到就绪、准备返回成功”的分支再次检查原期限。
恰好到期或超过期限返回 `TR_ERR_TIMEOUT`，该次时钟读取失败返回
`TR_ERR_SYS`。所有返回路径均释放同一把锁；0ms 无限等待不新增时钟依赖。
不修改 Channel 状态、公开接口、线协议、线程模型或回调所有权。

### 可复现证据

`test_channel_ready_deadline` 先创建真实 Reactor、socketpair 和已完成 HELLO
的两端 Channel。仅调用线程的链接期包装在拿到真实 mutex 后推进单调时钟，
不直接修改 Channel 私有字段，也不改变 Reactor 所有者时钟。

基线整库运行新增测试，在第一条精确到期路径失败：

```text
lane=0 after_lock=10000000 fail=0: got 0, expected -16
```

修复后覆盖 CONTROL/BULK 两条逻辑通道：锁后仍有 1ns、恰好到期、超过 1ns、
锁后时钟读取失败；另外覆盖锁前已到期及无限等待无时钟依赖。
每条路径之后执行正常就绪查询，最终销毁完整对象，验证锁未泄漏。

`test_client_connect_deadline` 使用真实 Client、延迟启动的服务端 Channel 和
socketpair，仅模拟非阻塞 TCP 返回与耗时以及 HELLO 条件等待超时。
分别覆盖 80ms TCP + 剩余 20ms HELLO、三次 `EINTR`、TCP 就绪时恰好到期、
初始时钟失败；校验 fd 已关闭，且同一 Client 的后续连接不被残留 session 拒绝。
该测试证明实际模块间期限传递，不声称测试了真实 TCP 网络延迟。

## 异步路径整改进度

### Connector：已在 PR #193 处理

`src/io/connector.c` 的前导数据发送此前在 `EINTR` 后立即循环，可能长时间
占住 Reactor owner，让已注册的超时定时器得不到调度。本次调整为与
`EAGAIN/EWOULDBLOCK` 相同：注册或复用 `EPOLLOUT` aux source 后立即返回
owner，下一次可写事件再恢复 `preface_sent` 偏移。使用原来的同一个定时器，
不创建新的 worker、无界队列或并行 callback。

单次 attempt 保存 timer arm 时产生的 `deadline_ns`。每轮推进及每段发送后
都重新检查它：I/O 刚好到期不得在 timer callback 运行之前抢先报告 `TR_OK`；
时钟读取失败使用 `TR_ERR_SYS` 结束 attempt，仍按原顺序完成 fd/source detach。
`test_connector` 使用仅限测试的可写 socketpair、`send`/单调时钟链接包装，
覆盖多次 `EINTR`、到期瞬间发送成功、两字节部分发送后恢复剩余内容；
原有 cancel-vs-complete 终态锁存与注销失败重试测试继续保留。

### 尚未完成：RPC 与极端换算边界

1. `src/rpc/rpc.c`：`tr_rpc_timeout_deadline_ns()` 没有单独处理
   `tr_rpc_now_ns()==0` 的读取失败哨兵；`tr_rpc_deadline_set_locked()`
   丢弃重设定时器的错误。需要按调用准入、既有 Call 和回收分别设计失败语义。
2. Reactor / RPC 仍有 `(delta_ns + 999999) / 1000000` 形式的向上取整。
   极端 `uint64_t` 边界可能先加法回绕，需专门的边界回归；不将这种极端
   算术边界描述为普通运行时已经复现的故障。

以上剩余项不在 PR #193 中修改；不能通过新增无界工作队列或削弱
终态锁存、回调撤销屏障来规避问题。
