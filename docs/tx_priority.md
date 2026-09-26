# 有边界的控制帧优先调度

## 范围

本轮只实现 Transport 内的 header-only 控制帧优先级，不把逻辑 CONTROL lane、
wire 帧类型和调度优先级混为一谈。CONTROL lane 上的业务 DATA（包括 RPC 消息）
仍遵守 DATA FIFO；没有新增 wire 字段、公开 API、运行时线程或连接组。

| 帧 | 可以越过 DATA 的条件 |
|---|---|
| PING / PONG | payload 长度为 0、flags 为 0、stream_id 为 0 |
| WINDOW_UPDATE | payload 长度为 0、flags 为 0、stream_id 非 0 |
| DATA | 不重排，不区分逻辑 lane |
| HELLO / HELLO_ACK / STREAM_OPEN / STREAM_CLOSE / GOAWAY / ERROR | 保持 FIFO 顺序屏障 |
| 其他形态的 PING / PONG / WINDOW_UPDATE | 不提升优先级，保持顺序屏障 |

这不是新的输入校验：原先 raw Transport 能提交的非标准控制帧不会因此被拒绝，
只是不会被提升优先级。Channel 的协议校验仍按原路径执行。

## 两个索引，一份所有权

每条 connection 保留原有所有消息 FIFO，同时维护所有 non-DATA 的 FIFO 索引。
索引包含普通控制帧和顺序屏障，而不只是可优先帧。只有该索引的表头有机会被
提前选择，因此控制帧不会互相越过，也不会跨过尚未发送的握手、开流或关流。

当最老的待发送消息是 DATA，且 non-DATA 索引的表头满足上表条件，调度器才
允许它越过 DATA。DATA 的消息/分片顺序完全保留。选择、入队及已完成项移除
均为 O(1)，没有每次选帧遍历整个队列的隐藏成本，也没有新增热路径分配。

`next/prev` 维护所有权 FIFO，`control_next` 只是同一对象的辅助索引。
活动帧仍属于所有权 FIFO，不能额外释放一次。关闭会遍历 FIFO 释放每项一次，
清空索引、活动帧和连续服务计数，然后才允许连接槽位复用。

## frame boundary 与公平性

只要当前帧已有字节成功交给 socket（`wire_pos != 0`），就固定活动帧；跨轮预算
耗尽及 EAGAIN 都不允许插入其他帧。若尚未成功发送任何字节，则仍可重新选择。

一个完整 DATA 帧完成后才重置控制帧连续服务计数。计数跨轮、跨 quantum、
跨 EAGAIN 保留并饱和在 8；最老的 DATA 因优先级被越过至多 8 个合格控制帧，
随后必须发送它的下一帧。每个合格控制帧只有 40 字节帧头。

这是对“优先级越过”的限制，不是对整条连接连续控制帧数量的绝对限制：已经
按 FIFO 排在 DATA 前面的控制项仍按原顺序发送，不能为了凑 8:1 而破坏顺序。
生产者后来提交的消息只能追加，不能不断插入 DATA 之前。阻塞 socket 或没有
被 owner 接受的工作不属于可运行性保证，额度规则也不是墙钟延迟保证。

所有发送仍消耗同一轮的 TX 字节与 dispatch 额度。发送前的 iovec 裁剪、
64 KiB connection quantum、ready 队列轮转与 EAGAIN 后等待 EPOLLOUT 都保留。
DATA 后续帧的 header/CRC 延迟到该帧真正被选择时才准备，避免在刚完成的
帧之后先为另一个 DATA 分片计算 CRC，再服务已经就绪的控制帧。

## 效果边界

优先级只能调整尚未交给 socket 的工作，不能撤回内核已经接受的 TCP 字节。
TCP 本身仍是有序字节流；共享连接上的丢包重传、socket backlog 和部分帧
剩余字节仍可能延迟控制消息。参见 RFC 9293 §2.2：
https://www.rfc-editor.org/rfc/rfc9293.html#section-2.2

它主要改善保活与流控消息在 DATA 积压下获得发送机会的能力，不等于给所有
短 RPC 提供优先级，不保证硬实时，不替代独立 CONTROL 连接及全链路背压。
顺序屏障前的大 DATA 仍可能阻塞其后的控制帧，这是保守兼容策略的明确取舍。

## 验证

```sh
xmake f -c -y -m debug --toolchain=gcc
xmake test -v -j1 'test_tx_priority/*'
xmake test -v -j1
```

新测试使用真实 Reactor、buffer pool、wire 编解码、CRC 与非阻塞 socketpair。
测试目标独有 sendmsg 包装观察实际发出的字节，并把累计量与 owner stats 核对，
不在生产库添加测试钩子。测试 gate 有意阻塞 owner，只用于构造确定性时序。

覆盖 1/17/4096 字节额度的优先级与 DATA 顺序、十种顺序屏障、部分帧及完整帧
边界到达的控制消息、单次 EAGAIN 故障注入后的可写恢复、持续通过真实提交 API
补入控制帧时的跨轮 DATA 进展、较早控制项 FIFO，以及 partial frame + 满控制池
的关闭/同槽位复用/失败发送所有权。EAGAIN 注入只验证错误分支；真实 socket
背压与非忙等由原有 test_reactor_budget 的真实 EAGAIN 场景继续覆盖。

负向验证使用临时副本：旧 FIFO 实现、移除活动帧固定、每轮重置连续服务计数、
允许 HELLO 跨 DATA，均必须在新测试的 wire 或顺序断言中失败。故障版本不提交。
超时只检测挂死，不作为性能门槛。性能评估另见 performance_readiness.md。
