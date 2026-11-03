# R8：满命令队列下的停止进展保证

基线：`9396f51d9ac2f1b89cdc076d73085b83932e947f`，对应 #154 / R8。

## 问题与取舍

旧 `tr_reactor_stop()` 在持有 `ctl_lock` 时，通过强制入队等待普通队列空位。
条件等待只释放队列锁。此时 owner 回调若调用需要 `ctl_lock` 的
CLOSE、ABORT、SEND 或 RESUME，就无法返回下一轮消费队列，形成等待环。
“pop 自己不获取 ctl_lock”不足以证明 owner 能到达 pop。

本修复采用固定 STOP 保留槽，不改变 Reactor 控制锁边界或线程回收流程。
相比解锁后等待，不引入新的 STOP 竞争窗口；相比扩大普通容量，不允许业务
占用停止资源。`tr_command_queue_push_wait_force()` 为兼容现有内部调用及
测试包装器保留名称，但现在只接受 STOP，不分配内存、不等待普通队列容量。
原调用处关于强制等待 ring 空位的解释不再适用，以此契约及 queue.h 为准。

## 不变量

1. 每个队列只保留一个 STOP，普通 ring 的容量、计数和峰值仍只计普通命令。
   保留槽内嵌于队列，随 Reactor 的 `sizeof` 自动纳入固定内存预算。
2. STOP 与普通入队使用同一队列锁和序号；提交 STOP 后，本 epoch 不再接受命令。
   普通等待者被唤醒并返回 CLOSED。重复 STOP 不覆盖原 STOP，也不推进序号。
3. `pop_batch()` 先排普通 ring，再取 STOP；二者共用原命令预算。
   普通命令恰好用完 batch 预算时，STOP 留到下一轮，不能提前执行。
4. 只要 STOP 尚未弹出，队列不算空，唤醒标志不提前复位；禁止提前 open。
   STOP 弹出后仍保持关闭，只有下一 epoch 的 open 才重开准入并更新代次。
5. Reactor 仍先关闭 accepting/completion/command-wait，再提交 STOP。
   `stop_submitted` 继续保证 join 失败后的顺序重试不会提交第二个 STOP。
   本变更不扩大并发生命周期契约：同一对象的 start/stop/destroy 由外部 owner
   串行协调，不宣称支持任意并发 destroy/free 或同一 pthread 的并发 join。

不新增线程，不新增无界队列，不改变 RPC 工作排空策略；R7 独立处理。

## 回归

`test_reactor_stop_full` 链接真实 trcore。测试门闩让 owner 停留在回调，
另一生产者填满容量为 1 的命令队列；STOP 包装器确认队列确实满后放行回调。
四种回调操作分别测试正常 join 与单次 join 失败，共八种交错。
检查回调返回 CLOSED、先前接受的命令先于 STOP 弹出、STOP 恰好一次、
join 失败可重试，以及 stop 后重新 start 能处理新命令。
看门狗仅限制测试挂起，不以固定休眠制造交错。

`test_command_queue` 检查保留槽独立于普通容量、关闭等待者、批次预算、
STOP 只投递一次、未消费前禁止重开、空队列唤醒、序号回绕和新 epoch。
原有 Reactor fairness、runtime thread/join 测试仍需通过。

旧基线真实库已经在第一个 CLOSE 场景触发看门狗：

```text
R8: operation=0 join_fault=0
R8 regression: timeout while stop and owner callback wait
```

这不是抽取模型。最终以修复提交的测试结果和 CI 为验收依据。
