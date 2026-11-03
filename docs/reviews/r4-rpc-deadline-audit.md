# R4：RPC deadline、终态与半关闭失败边界

原缺陷复现基线：`4917ba6`；集成基线：`648e0f3`（包含 PR #193 至 #198 已合并修复）。本轮保持单 Reactor owner、单 Endpoint timer、有界 Call
heap/executor，以及已有取消、终态回调和销毁屏障；不增加生产测试开关。

## 已修复的边界

1. 有限 Call 的初始单调时钟读取失败继续返回 0 哨兵，由 options 准入返回
   `TR_ERR_SYS`。Unary 与 streaming 都回收未发布的 Call，不创建 Stream，
   稳定 semantic stats 不计入 started。0ms 仍表示不设置 deadline。
2. 新 deadline 的 timer arm 错误向准入传播，沿原失败路径删除 heap 项、回收
   Call。失败的 arm 不会覆盖原先的 timer；既有 Call 保留自己的唤醒。
3. 保留 PR #195 的事务语义：新增/修改 deadline 的 arm 失败恢复原值；
   终态删除不回滚，不复活 Call。删除后的 rearm 即使失败，原 timer 仍保留，
   callback 返回值负责后继重排；回归允许这些显式尝试失败，但所有终态必须到齐。
4. Deadline callback 处理到期 Call 后返回新的最早期限，使用 timer queue
   原有的 callback 返回值契约。queue 已取出当前 timer，合法 registration
   的重新插入有容量；显式 arm/unregister 的 version/generation 优先级不变。
5. 客户端在持锁接收 RESPONSE/STATUS、接受结果之前复核 deadline。精确到期
   和超过期限都走既有 `DEADLINE_EXCEEDED` 取消路径，不能依赖该轮稍后才运行
   的 timer。该次时钟读取失败同步中止所在 lane，走现有 `UNAVAILABLE`
   连接错误路径；不制造协议不允许的 INTERNAL CANCEL。
6. 两处剩余纳秒转毫秒使用商加非零余数，避免 `remaining_ns + 999999`
   在 `uint64_t` 极值回绕。timeout metadata 与 context 使用同一取整语义。

## 已提交结果仍然拥有传输收尾义务

最终 STATUS、Unary 成功 RESPONSE 和 Unary 失败收尾，都可能已作出终态决定，
但 `STREAM_CLOSE` 仍因 CONTROL TX 背压返回 `TR_AGAIN`。正常 Unary 响应原先
缺少这一共享 timer 重试路径，本轮补齐，重试不重新发送业务响应。

第一次安排重试时，如果读取时钟或 arm 失败，不能只向调用者返回错误后留下
一个没有唤醒的 Stream。现在通过内部 owner-only Channel lane helper 复用
`tr_reactor_abort_on_owner()`，在释放 Endpoint/Channel 锁之后同步退休该精确
connection，由现有错误回调收敛 Call、Stream、payload 和 executor 引用。
它不依赖可能已满的 Reactor command 队列。

同步错误回调可能已经退休原 Call。Unary 失败分支与 timer 遍历会检查该状态，
不在旧 Call 上继续补发、再次安排或重新标记终态。

这是明确的 fail-closed 成本：该物理连接上的其他 Call 也会失败；split 模式
通过请求 Call 的 lane 选取连接，shared 模式自然影响共享连接。已经被对端接受
的最终结果不会重复通知；已进入本端 TX 所有权但尚未抵达对端的 STATUS 不保证
送达，对端可能观察到 `UNAVAILABLE`。本端已经锁存的 semantic 终态仍只记录一次。

## 入站 lane 不变量

首次 REQUEST 在分配 RPC Call 之前，通过精确 Stream 代次读取实际 lane，并要求
与注册 Method 声明一致。两者不符只拒绝该 Stream，不调用应用处理器，也不记录
已开始的服务端 Call。这保证后续按 Method lane 选择 abort 连接、消费 Channel
DOWN 事件时，处理的就是实际承载该 Call 的连接。

新增两条物理连接都存活时的 CONTROL→BULK、BULK→CONTROL 错配回归，随后在另一
lane 完成一次匹配 RPC，证明无关连接仍可用。测试把请求发送与客户端半关闭放在
同一 owner turn，以隔离这里的准入边界。

## 确定性验证

`test_rpc_deadline_failure` 使用真实 Reactor、socketpair、完成 HELLO 的两端
Channel 和真实 Endpoint；仅通过测试链接 wrapper 控制时钟和 timer arm。

- 初始 clock 失败：Unary/streaming 拒绝，0ms 不受调用线程时钟故障影响。
- 超过 Call 容量次数的失败准入：heap、Stream 和 semantic started 不泄漏。
- 失败的新较早期限：原 Call 仍准确进入 deadline 终态。
- 删除 heap 根后及多个 timer callback 之间：即使显式 arm 持续故障，剩余
  accepted Call 仍全部收到一次 deadline 终态。
- 饱和到 `UINT64_MAX`：实际 context 和线上 `:timeout-ms` 验证安全取整。
- 真实对端发送 Unary RESPONSE / streaming STATUS；decoder wrapper 只在 owner
  完成真实报文解析后，使下一次时钟读取落在 deadline 前 1ns、精确到期、后 1ns
  或读取失败。验证结果、一次回调、semantic 分布和回收。

`test_rpc_final_close_retry` 与 `test_rpc_unary_failure` 增加真实半关闭背压后的
clock、arm SYS、arm STALE 故障，验证 lane DOWN、Stream/Call/池回收及一次通知。
Unary 测试还覆盖正常 RESPONSE 后多次半关闭背压与 timer 自动收尾。

旧实现运行新增用例的反证：初始 clock/arm 错误错误地接受 Call；cleanup 和
callback 用例等待不到全部终态；极值 context 回绕；首次 retry 故障后 streaming
`server_closed` 停留在 0，Unary 请求不再产生结果；精确到期的 STATUS 仍返回 OK。

测试 fixture 必须等两端 Stream 和所有 Call 槽排空后才 stop Reactor。终态回调
早于 peer 半关闭，立刻 stop 会合法地拒绝仍在执行的 peer close send；不能把
这个测试停机竞态误判成 RPC deadline 错误，也没有通过放宽断言掩盖它。

## 验证范围

Focused aggregate 为全部 `test_rpc_*`、`test_transport`、`test_timer_queue`
共 13 个目标；测试命令及 Release/TSan/ASan 结果由集成报告记录。上述验证不等同于
所有仓库测试、网络长稳或任意应用回调下的 API 返回耗时硬上限。

## 追加修复：对端先半关闭

错配回归另外暴露客户端 streaming 收尾顺序问题：REMOTE_CLOSED 先到达，随后
`close_send()` 完成最后半关闭时，Channel 已回收 Stream，但原 RPC 只标记
local_closed，缺少终态/Call 回收。现在由同一 owner 主动进入已有 CLOSED 终态
处理路径，保留该路径原有的回调、executor 保留义务和索引/槽位回收。

无完整 RPC 结果的传输 EOF 统一记录为 `UNAVAILABLE`，修复另一关闭顺序下用户
收到 UNAVAILABLE、semantic stats 却记录 OK 的不一致。新增无 deadline 的两种
关闭顺序回归；remote-first 用公开 REMOTE_CLOSED 回调固定顺序，连续三个 Call
都只通知一次且回收完成。旧实现在第一例等待不到终态。

Unary 回归还覆盖第一次 close 背压正常 arm、原任务在同一 owner turn 归还引用后，
第二次 timer close 背压时注入 STALE；确认安全回收，再替换两端连接，用同一
Endpoint 成功发起新 Call，验证槽位与代次复用。
