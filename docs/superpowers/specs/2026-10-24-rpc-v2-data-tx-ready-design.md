# RPC V2 DATA TX 容量恢复通知设计

日期：2026-10-24。范围：#247/#241 的 DATA 发送池恢复通知子项。
基线：`feat/rpc-v2-call-ref-243`，开发版本 `2.0.0-dev.26`。

## 问题

压缩后的 Channel DATA 消息会保留有界 wire 存储，并由 Reactor owner 分帧发送。
`tr_reactor_send_data_frame_on_owner()` 因普通 DATA TX item 池已满而返回
`TR_AGAIN` 时，Channel 保留消息并设置 `tx_retry_pending`，目前最早要等
25ms 维护定时器再次尝试。普通 TX item 释放时 Reactor 已知道“空池变为有空位”，
但没有向 Channel 压缩发送器发布这个容量事件。

本切片只补这一类 DATA TX item 池等待。Channel 内存预算不足、credit 不足及其他
重试仍沿用既有定时器/协议事件，不修改默认容量，也不重新解释 #247 已完成的
控制池通知或流槽拒绝。

## 合同

1. Reactor 为普通 DATA TX item 池提供内部 owner-only watch/unwatch API。
2. 每个精确 Connection 代次同时最多一个 DATA pool waiter；多个 waiter 以有界
   intrusive FIFO 排队，不分配对象。关闭或替换 handler 会自动撤销等待项。
3. DATA TX 池有空闲项且命令队列有容量时，owner 在安全调度点发出一次
   `DATA_TX_READY`。通知先从队列摘除，再运行 event callback；通知不预约/保留
   item，收到后发送仍可返回 `TR_AGAIN`，调用者必须重新登记。命令队列暂满时留在
   waiter 队列，owner 消费命令后再次检查。
4. DATA 与 CONTROL 使用独立的有界 intrusive FIFO，但通知与普通 TX 共用 Reactor
   每轮 `tx_dispatches` 上限；容量通知在两类池间轮转优先级，保持各自 FIFO，避免
   一类回调持续重登记时饥饿另一类。跨线程归还最后一个占用项时仅写既有 eventfd，
   不在池锁或发送释放栈执行 callback。
5. Reactor 内部 DATA 分帧发送接口明确报告 `TR_AGAIN` 是否由 DATA TX item 池耗尽
	引起。Channel 只在该原因下登记精确 lane Connection；分片内存预算不足、命令
	容量不足等其他重试继续使用原有维护定时器。ready callback 继续使用现有压缩
	进度函数；竞争失败重新登记，watch 注册失败也保留定时器兜底。
6. 不增加公开 SDK/API、线程、消息队列、动态 wait 节点或线协议字段。普通池已
   按现有配置有界；每 Connection 增加固定 waiter links/flag。

## 实现选择

复用 `control_tx_pending` 的 per-Connection intrusive queue 结构和
`control_tx_pool.waiter_reactor` 唤醒方式，为 DATA `tx_pool` 单独建等待 FIFO、
计数、watch API 和 `TR_CONN_EVENT_DATA_TX_READY`。Pool ready 检查同时确认 DATA pool
有空闲项、命令队列有容量且未关闭；发送接口的 `pool_exhausted` 输出避免将命令容量
不足误认为池耗尽。DATA 与 CONTROL 分开排队，各自保持 FIFO，并以轮转顺序共享同一轮
TX 通知预算。

Channel 记录一个正在等待 DATA 池的精确 connection handle。下一次压缩进度先撤销
旧观察；watcher callback 自身已先被 Reactor 摘除，timer 重试则显式 unwatch。若
发生关闭/代次回收，Reactor 自动清理队列，不访问陈旧 Connection。

## 验收

- Reactor 测试覆盖：单槽满池等待、跨线程唯一唤醒、池空位先于 watch 登记的竞态、
	两类池混合通知的 FIFO/轮转公平/每轮上限、重复登记、取消、handler 替换、连接
	关闭与槽代次复用。
- Channel 真实两端测试：在消息压缩完成后占满 DATA TX pool，压缩发送等待；不让
  25ms fallback 提前触发，释放一项后必须由 DATA_TX_READY 唤醒并完整发送，其他流
  继续工作；保留预算与 message exactly-once 断言。
- 验证 Release、聚焦 ASan/UBSan、TSan 与 clang-format 18.1.8；全量 Release 和
  CI 结果单独记录。所有测试临时文件、缓存、日志与构建产物位于当前 worktree 的
  `build/`。

本子项完成后，#247/#241 的 model-checking、RX/TX 更广调度、发生器负载质量、
分域预算与压缩慢消费者矩阵仍保持开放；不声称 40k 吞吐达标。
