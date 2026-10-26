# Reactor 跨所有者同步等待图设计

日期：2026-10-26。关联 GitHub #243 / ARCH-07；基于累计到 `2.0.0-dev.153` 的 PR #327 链尾，作为 `dev.153 -> dev.154` 后续切片，避免重写 #315–#327 已有版本推进。

## 决策

保留无环的同步跨 Reactor 调用，只拒绝会闭合等待环的请求，错误码为 `TR_ERR_STATE`。这满足 #243 对同步 owner 调用的要求，同时保留已有的合法 A→B 用法；不把所有 owner→other Reactor 调用一律禁掉。

## 问题与范围

每个 Reactor 由一个 owner 线程推进。owner A 若同步等待 B，而 B 的同步回调再等待 A，两个线程都无法推进对方的命令队列。现有 `tr_current_reactor_owner` 只避免等待自身，没有记录跨 Reactor 的依赖边。

本切片仅约束会阻塞等待另一个 Reactor 的内部同步入口：

- `tr_reactor_call`
- `tr_reactor_set_handler`
- `tr_reactor_quiesce`
- `tr_reactor_close_graceful`（边必须覆盖命令完成与 TLS 关闭完成两个等待阶段）
- `tr_reactor_stop`（边必须覆盖 STOP 队列准入和线程 join）

`tr_reactor_call_or_stopped`、统计、timer 等经 `tr_reactor_call` 委派的接口继承该保护。异步命令、completion 提交和非阻塞 Reactor API 不添加边。

## 不变量与算法

1. 仅当调用线程当前是某个 Reactor owner，且 API 将阻塞等待另一个 Reactor 时，才登记 source→target 边。普通应用线程无需等待图节点。
2. 每个 owner 至多有一条活动出边。边和入边计数由单个静态 wait-graph mutex 保护；不持有该 mutex 执行用户代码、等待条件变量、访问队列或 join。
3. 登记 source→target 前，沿 target 的出边链查找 source。若找到 source，则返回 `TR_ERR_STATE`，不入队、不改变输出或所有权。否则登记边后才允许同步队列准入和等待。
4. 同一 source 在相同 target 上嵌套登记时增加 scope depth。该规则用于 `tr_reactor_close_graceful` 外层生命周期覆盖内层 `tr_reactor_call` 的情形；不同 target 的嵌套边返回 `TR_ERR_STATE`。最外层 scope 退出时清边并归还 target 的 inbound waiter 计数。
5. 现有入边等待者退出前仍须访问 target 的图计数。因此 `tr_reactor_destroy` 在 stop 后原子地关闭新 wait-scope 准入；如果仍有 inbound waiter，保留对象并返回 `TR_AGAIN`。waiter 清边后可重试 destroy。destroy 已开始后禁止重新 start。
6. 所有早退和错误出口都必须释放 scope。一个 owner 等待链 A→B→C 可以存在；闭合边 C→A 会被拒绝。清除任何边后，后续合法调用继续可用。
7. 图互斥锁不与 `ctl_lock`、队列锁、sync mutex 同时持有；先完成图登记并释放图锁，再进入 Reactor 锁/队列路径。destroy/start 的生命周期串行化继续遵守现有外部同步契约。

## 失败与生命周期语义

- 只有检测到自等待或环等待才返回 `TR_ERR_STATE`；目标已停止/开始销毁仍由既有关闭状态返回 `TR_ERR_CLOSED`。
- 命令环满时，已经登记的边覆盖容量等待；队列关闭、入队失败、所有者错误、join 错误均清理 scope。
- 目标销毁的 inbound waiter 计数只保护跨 owner 等待 scope。它不替代调用方对 Reactor 生命周期 API 的外层串行化，也不将裸 Reactor 指针变成通用引用。
- 不改变公共 ABI、RPC/transport wire、默认容量或既有无环调用的返回值。

## 验收

- 无环 A→B 与 A→B→C 同步调用成功。
- A→B→A 和 A→B→C→A 确定性拒绝最后一条边并收敛；环拒绝后 subsequent acyclic call 仍成功。
- owner 发起的 `quiesce`、`set_handler`、`stop/join` 参与环检测。
- graceful close 外层 scope 覆盖内层同步命令；使用真实 TLS 关闭路径确认等待期间的 owner 反向同步调用被拒绝而关闭完成。
- 持有一个 in-flight inbound wait scope 时 destroy 停止 Reactor 后返回 `TR_AGAIN`、对象不释放且不能 restart；scope 退出后 retry destroy 成功。
- Release、ASan/UBSan、TSan 聚焦/全 Reactor 测试通过；新增测试失败由 watchdog 有界收敛，不依赖固定 sleep 判序。
- 既有 TLS smoke、Reactor fatal-exit/stop-full 测试与 SDK ABI 检查无回归。

## 性能与风险

等待图只在同步跨 owner 入口取一次全局互斥锁并遍历活动出边；单 owner 快路径、普通外部线程和数据/传输热路径不触碰该锁。最坏检查成本与当前同时阻塞的 Reactor owner 数成正比。主要风险是任一错误出口遗漏清边，导致虚假环拒绝或销毁重试不收敛；测试覆盖成功、cycle reject、target stop、队列/线程终止及 nested scope 生命周期。
