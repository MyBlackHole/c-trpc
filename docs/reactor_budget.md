# Reactor 整轮预算与调度诊断

本轮是 Phase 2 的预算收敛，不引入 CONTROL 优先级、多 Reactor 或重连状态机。
命令 FIFO、同步 owner call、quiesce 与 STOP 前已接受 Completion 的清理语义保持不变。

## 一轮只有一份额度

一轮从 command dequeue 开始，到 epoll 返回后的 RX / Timer / TX 调度结束。
轮次中途的 wake、可读、可写事件不会补充预算。

| 工作 | 每轮上限 | 单位 |
|---|---|---|
| Command | 64 | 已出队命令，含失效句柄命令 |
| Completion | 64 | 已出队 Completion |
| Timer | 64 | Timer callback |
| RX | `rx_budget_bytes` | 实际 recv 的 wire 字节，包含帧头 |
| TX | `tx_budget_bytes` | 实际 sendmsg 的 wire 字节，包含帧头 |
| RX ready / TX ready | 各 64 | 连接调度次数，不是不同连接数量 |

每次 RX/TX 连接调度还有 64 KiB quantum，达到 quantum 时移至对应就绪队列尾部。
只要还有整轮额度，一个连接可以再次获得调度；已有其他就绪连接会先得到机会。
队列节点嵌入 connection，调度热路径不为此分配内存。关闭连接时同时移除 RX/TX
就绪节点，再复用槽位；epoll token 的 generation 校验继续有效。

**配置语义收紧：** `tr_reactor_config` 布局和零值选择默认值的行为不变，但两个
字节预算从各 connection/flush 的局部额度变为所有连接共享的整轮额度。
默认 RX/TX 各 4 MiB；应用不能再把它们理解为每连接每次回调额度。
在多连接负载下，这可能改变吞吐与轮转次数，应按实际负载调优，不宣称吞吐提升。

TX 不只是发送后扣减计数：发送前按剩余字节截断 iovec 列表，保证单次系统调用
也不会超额。帧头、scatter/gather 跨片段、DATA 自动分帧均沿用 `wire_pos` 续传；
预算可以小于一个帧头，不会因此插入别的消息或改变 wire 顺序。

## 续处理不等于忙等

```text
命令/Completion/Timer 仍有工作，或 RX/TX 就绪队列非空
    -> epoll_wait(timeout=0)，让其他事件获得机会，下一轮继续

TX 返回 EAGAIN
    -> 移出可运行集合，等待 EPOLLOUT 后重新进入 TX ready

RX 返回 EAGAIN / RX buffer pool 暂停
    -> 不因“连接存在”而进入就绪队列；等待可读 / resume

没有当前可运行工作
    -> 等待最近 Timer deadline，或无限期等待 I/O/wake
```

Completion 的零预算 pop 不消费条目，但在队列锁内观察并返回 `has_more`。
非空时保留 `wake_pending`，空队列允许未来 producer 发出新唤醒。不会无锁读取
队列计数，也不会依赖已经被合并的唤醒来继续处理积压。

wake 使用非 semaphore eventfd：一次成功读取已消费累计计数；新写入仍保持可读。
不再反复读到 EAGAIN，避免持续唤醒生产者把 owner 留在 wake drain 循环。

STOP 仍按 FIFO 处理。停止接收后，所有已接受 Completion 必须完整 drain，
不受正常轮次的 64 条限制；不会因为为了公平性而丢弃退出清理。

## 最小诊断 API

新增公开的 `tr_reactor_work`、`tr_reactor_stats` 与 `tr_reactor_get_stats()`。
原有公开函数签名、配置布局和 connection stats 保留。

```c
struct tr_reactor_stats stats;
int ret = tr_reactor_get_stats(reactor, &stats);
if (ret == TR_OK) {
    /* stats.total、stats.max_per_turn、stats.budget_hits 与 stats.limits
     * 使用相同的工作字段，可比较上限和实际累计工作。 */
}
```

统计由 owner 在轮次结束时一次性累计。运行中的外部线程通过现有同步 owner call
获取一致快照；owner 回调直接读取，不自等待。创建后启动前、stop 返回并 join 后
也可读取。与 stop 并发时可能返回 `TR_ERR_CLOSED`；失败不修改输出。
调用方必须保证 Reactor 存活；这是同步诊断接口，不是信号安全或无等待接口。

| 字段 | 精确含义 |
|---|---|
| `turns` | 已完成的轮次，含收到 STOP 的命令轮次 |
| `limits` | 本 Reactor 的整轮工作上限 |
| `total` | 已完成正常轮次累计工作；不含当前尚未完成轮次 |
| `max_per_turn` | 各字段在已完成轮次中的最大值 |
| `budget_hits` | 各字段额度减到零的轮次数；不代表当时必有积压 |
| `epoll_polls / epoll_waits` | timeout 为零 / 非零的 epoll 调用次数；不代表实际睡眠时长 |
| `timer_lateness_ns_max` | 有额度时，Timer batch dispatch 对最早到期项的迟到采样最大值 |
| `shutdown_completions` | STOP drain 完成数，单独计数，不并入 `total.completions` |

Timer 迟到采样不是每个回调的直方图，也不等于 P99。统计只覆盖调度器出队工作；
回调内直接嵌套的 owner call/Completion 不重复计入命令/Completion 数量。
本轮没有新增队列等待时间、积压高水位、CPU 时间或延迟直方图。

预算是工作数量约束，不是硬实时保证。不能抢占耗时 callback，也不限制任意
应用 callback 的内部工作；CRC、协议解析、锁竞争与 EINTR 重试不等价于字节数。

## 验证

```sh
xmake f -c -y -m debug --toolchain=gcc
xmake test -v -j1 'test_reactor_budget/*'
xmake test -v -j1
```

`test_reactor_budget` 仅在测试链接时包装 queue pop、recv、sendmsg、
`tr_parser_produce` 和 epoll_wait；生产库没有测试钩子。RX 的实际字节数及
首字节暂停门槛在解析器入口观察：当前 Reactor 将每次正数 recv 返回值原样
传入该入口，避免把 libc 符号包装是否生效作为测试继续执行的前提。
不会绕过 sanitizer 的系统调用拦截器。观察到的 RX/TX 总字节还必须与 owner
快照一致，防止钩子漏执行时用零计数误报通过。

覆盖 1、17、4096 字节额度，双连接双向 wire 校验，四 buffer 的 scatter/gather
与自动分帧，200 个 Completion 与 200 次立即到期 Timer，EAGAIN 后等待并恢复，
RX 待续处理时关闭并同槽位复用，以及零预算 Completion 标志。另有 70 连接、
每轮 1 字节的场景，超过一次 epoll 的 64 事件批次：每个连接的首帧必须在任何
连接第二帧完成前被接收，不能只靠最终收齐数据证明轮转公平性。测试为所有
连接提供足够 RX buffer，避免把内存池背压混入调度顺序验证。

各运行场景同时核对运行中 owner/外部快照、停止后快照、精确字节累计、
单轮峰值和预算耗尽计数。条件变量、执行计数和实际字节边界决定测试时序；
10 秒具名条件等待和 60 秒进程 alarm 仅检测挂死，不把毫秒级耗时作为性能门槛。
标准输出即时刷新，失败会报告具体等待条件。原有 fairness/退出、runtime threads、
timer 与 Transport/RPC 测试继续纳入完整 CI，覆盖同步调用、STOP drain、
RX pause/resume 等原有行为。Sanitizer CI 保留失败退出码，同时上传完整诊断日志；
失败时附上精确 Git 源码快照，便于复现，不启用自动重试或错误抑制。
