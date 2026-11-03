# 2026-10-14：GCC 11 生产静态分析与待确认告警

## 完整分析输入

- 基线：`main@c8062b55ab7aa13f33b72e18a5fdeebb37ed159b`。
- Ubuntu 22.04 / GCC 11.4 / Linux x86_64 / Xmake 3.1.1 / release / 自动 CRC。
- 使用经校验的真实 `trcore` 生产编译数据库；保留 `-c` 和
  `-O2/-Werror/C11` 等选项，仅隔离对象文件路径并添加 `-fanalyzer`。
- 31/31 个生产翻译单元完成实际编译与分析；正确自动清理的正例通过，
  故意泄漏的反例被 GCC 检测并返回失败。
- 首轮报告 4 条 GCC 分析器诊断，原始证据见
  [首次 GCC 分析运行](https://github.com/MyBlackHole/c-trpc/actions/runs/38515643260/artifacts/11872027070)。
  这不是 Clang 14 已有 11 条跟踪警告的重复计数。

## 四条必须继续核实的诊断

| 位置 | GCC 检查 | 源码级第一轮核查 | 尚待验证 |
|---|---|---|---|
| `src/execution/reactor.c:1194` | `analyzer-null-dereference` | TX 循环要求 `tx_head != NULL`；`tr_connection_next_tx()` 仅返回部分写活跃项或从非空 TX 队列/控制子链选择的项 | 生产代码所有者线程和 TX FIFO 不变量在故障切换、发送失败时是否始终满足 |
| `src/rpc/rpc.c:2698` | `analyzer-use-after-free` | `tr_rpc_schedule_pending_executor_retry()` 在提交异步完成前获得 Endpoint 强引用；`tr_rpc_queue_task_locked()` 的额外引用失败后立即还原，最后在回调末尾释放外层引用 | 背压/终止回调/最终强引用竞争下的销毁顺序，需精确交错和引用计数验证 |
| `src/server.c:2067` | `analyzer-null-dereference` | 构造器先成功分配 `server->shards` 并绑定 Runtime，之后才递增可遍历的 `server->shard_count` | 部分构造后清理重试、并发销毁与统计查询的契约范围，不能只靠正常创建 |
| `src/transport/channel/channel.c:581` | `analyzer-malloc-leak` | \`tr_channel_protocol_buffer\` 把 `tr_buffer` 放在首成员，通过内部释放回调释放外层分配；HELLO 发包在 `TR_OK` 时转移所有权，其他返回值使用 `TR_AUTO` 清理 | 发送准入失败、连接关闭与重连竞争的所有权最终收敛 |

**以上都是第一轮路径审查，不得仅凭这张表将它们标记为已确认误报。**
尤其是 RPC 使用后释放与 Server 不完整构造路径，应优先补充确定性并发/销毁测试。

## 诊断集合的精确回归门禁

当前 GCC 11 原始 `-fanalyzer` 在生产 `-Werror` 下会将这四条
尚未处理的诊断升级为编译错误。为保留完整分析输出并实施增量阻断，
分析专用命令**仅对出现的三个类别**添加
`-Wno-error=analyzer-null-dereference`、
`-Wno-error=analyzer-use-after-free`、
`-Wno-error=analyzer-malloc-leak`。

这三个选项**只将严重性降为警告，不关闭 GCC 检查项**；正常生产编译仍保留
全部 `-Werror`。精确门禁从逐生产单元日志重新读取每条告警的
翻译单元、源码路径、行号、列号、检查项、完整说明并按多重集合比较，
不接受新增、漂移、重复、消失、漏日志、跳过翻译单元或失效的正反例。

基线记录保存在 `docs/gcc_analyzer_known_warnings.json`，
`review_state` 保持 `unverified`。比较产物 `warning-gate.json` 留存供审查。
这是一道对**新增或漂移告警严格阻断**的门禁，不是整个项目“零告警通过”的证书，
也不授权今后将这些诊断固定为永久例外。待逐项验证并修复后必须收缩基线。

当前既有 Clang 14 精确基线（11 条）与 GCC 11 基线（4 条）互不替代，
不得相加得出唯一缺陷数量，也不得由任何一个分析器结果推断运行时无缺陷。

## Server 统计访问边界修复后的再验证

在 `fix/server-partial-owner-stats-20261014` 分支增加了私有 `published`
生命周期准入：只有成功构造并完成全部分片绑定后，Server 才允许跨分片统计；
构造失败后的部分所有权对象以及已经进入终局销毁（即使销毁屏障失败）的
对象，`tr_server_get_stats()` 和 `tr_server_get_rpc_semantic_stats()`
均返回 `TR_ERR_STATE`，不会继续读取分片指针。
逐分片还检查 Runtime 绑定；不会释放、替换或延长任何资源的所有权。

现有 `test_server_create_oom` 的逐分配失败与回滚保留所有权矩阵、
`test_server_destroy_retry` 的多次销毁屏障失败矩阵现均加入了禁止读取
部分对象的断言。该状态标志属于 Server 私有实现，不修改公开结构体、
API 或协议。

在修改生产代码、但尚未修订诊断基线的首次远端运行中，GCC 分析器报告
`added=0, removed=1`；消失的就是 `server.c:2067` 的历史空指针诊断。
见 [首次修复后 GCC 门禁](https://github.com/MyBlackHole/c-trpc/actions/runs/38520248407)。
据此将 Server 项**单独**从精确告警文件移除；其余三条 GCC 诊断仍为
`unverified`，Clang 既有 11 条记录完全不变。

这一证据证明新增准入让 GCC 不再报告这条路径，并且针对已测试的
构造失败和终局销毁拒绝统计查询；不证明任意错误并发访问是合法的，
也不意味着全部其他生命周期路径已经验收。

## RPC 重试回调的强引用路径审查与新增回归

针对 `rpc.c:2698` 的 GCC 释放后使用诊断，分析器路径从
`tr_rpc_retry_pending_executor_on_owner()` 中的 `tr_rpc_queue_task_locked()`
追踪到入队失败后 `tr_rpc_endpoint_put()`，假设这次释放会成为最后一次
引用归还，进而推断后续重试状态写入发生在对象释放之后。

需要分清两条独立的引用：`tr_rpc_schedule_pending_executor_retry()`
在提交 Reactor 完成回调前先通过 `tr_rpc_endpoint_get()` 获取一份
**覆盖整段待重试回调执行期**的强引用；`tr_rpc_queue_task_locked()`
尝试入队时再获取一份临时任务引用，失败时仅归还后者。
重试回调在释放 `endpoint->lock` 后才归还第一份引用。
终局 `tr_rpc_endpoint_destroy_with_stats()` 又必须在解绑事件源、关闭
执行器后等待引用数收敛到仅剩所有者引用。这一套引用关系表明
GCC 首轮路径未完整建模外层引用是合理怀疑，但**尚不是彻底的并发正确性证明**。

现有 `test_rpc_terminal_backpressure` 以真实一元 RPC、满执行器队列、
取消与响应回调重试覆盖后者的可达场景。新增加两种负向检查：
- 第一条工作线程结果回调仍在运行时调用 Endpoint 销毁，必须
  返回 `TR_ERR_STATE` 且不能改变后续回调、连接与资源的回收结果。
- 在 Reactor 所有者回调内尝试 Endpoint 销毁，同样必须拒绝，
  防止等待自己的事件处理轮次。

运行时负向测试通过后，仍需覆盖异常执行器准入错误、终止强引用交错、
以及错误销毁重试路径；`rpc.c:2698` 继续保留
`review_state=unverified`，**不允许因这两项新增断言就删掉 GCC 告警基线**。

## RPC 待重试回调提交失败的实际交错验证

针对 GCC `rpc.c:2698` 释放后使用诊断，新增独立**测试链接阶段**
`--wrap=tr_reactor_complete` 注入；只在 `arg` 与当前客户端
`tr_rpc_endpoint` 完全相等时识别待重试完成项调度，
不干预 Server Endpoint、普通 RPC 工作项或者生产静态库。

扩展 `test_rpc_terminal_backpressure` 已有的真实端到端场景：
1. 先以一个被门禁阻塞的客户端回调占住唯一工作线程，填满 16 个
   执行器槽位，并真实制造 CANCELLED 与成功响应两份终止任务背压。
2. 确认对应队列满压力事件已发生，并跨过连接 ABORT 的所有者静默屏障；
   此时工作线程仍不能从执行器 take()，故不会抢先调用 retry 调度。
3. 设置唯一一次 `tr_reactor_complete()` 错误注入，然后释放工作线程回调。
   第一次调度必须命中 `TR_ERR_SYS`，调度器清除已计划标记并归还本次
   回调持有的强引用；下一次 take() 应重新提交成功。
4. 断言真实注入恰好发生一次、至少一次再次提交成功、所有取消与结果
   回调**恰好交付一次**、缓冲池全部归还，并正常停止反应器、销毁
   Client/Server Endpoint 与 Channel。
5. 同一用例随 GCC、Clang、ASan/UBSan、TSan 运行，检验该失败交错
   不导致双重释放、释放后使用或任务/连接存储悬挂。

注入计数与待监测 Endpoint 身份采用 C11 原子变量，不对正常生产端添加
测试开关；测试不通过睡眠来制造第一次提交失败。
这能加强外层重试回调引用和临时任务引用在**可达错误分支**上的证据，
但还不能证明所有异常强引用状态，故本轮仍保留
`rpc.c:2698` 的 GCC `unverified` 精确基线，不全局抑制检查。

## 后续收敛顺序

1. 优先复核 RPC 引用计数与 Server 部分构造的生命周期交错。
2. 确认 Reactor 所有者/队列不变量和 Channel 发送缓冲所有权转移。
3. 对实际缺陷修改生产代码并补专测；对有证据的工具误报精确说明。
4. 缩小上述两套基线，再升级 GCC 和 Clang 到尽可能完整的零告警阻断。
5. #154 的跨架构、长时间负载、故障恢复和安全边界验收仍须独立完成。

## 2026-10-19：RPC 重试完成项提交之前的强引用时序验证

当前集成候选 PR #281 没有修改旧 RPC 生产引用计数、任务队列或网络协议。
本轮针对仍保留的 GCC 11 `src/rpc/rpc.c:2692` / `analyzer-use-after-free`
增加了可重复的并发测试，尚未将告警从精确基线中删除或更改 `unverified` 状态。

### 所有权与线性化点

1. 初始 Endpoint 所有者持有一份引用；`tr_rpc_schedule_pending_executor_retry()`
   在将完成回调提交 Reactor **之前**通过 `tr_rpc_endpoint_get()` 获取额外引用。
   失败时清除 `pending_retry_scheduled` 并只归还这份引用。
2. Reactor 接受完成项后，回调拥有这一份引用；
   `tr_rpc_retry_pending_executor_on_owner()` 必须先解除 `endpoint->lock`，
   再归还自己的引用，不能在锁内触发 Endpoint 最终释放。
3. `tr_rpc_queue_task_locked()` 为拟提交的执行器任务另取一次临时强引用。
   入队失败时归还的是**任务引用**，而不是前述重试完成项引用；
   因此正常协议所有者和仍存活的重试回调都在时，临时归还不应成为最后一次释放。
4. 正常销毁会先解绑 Channel 回调和定时器、停止执行器、等待引用收敛，
   之后才释放初始所有者引用。持有 Endpoint 锁和执行器锁的顺序不可颠倒。

这解释了 GCC 对临时 `put` 视为最后一次释放后继续访问 Endpoint 的一条
可能误判路径，但**源码推导不等同于独立证明，也不授权关闭告警**。

### 新增可达交错

使用既有 `test_rpc_terminal_backpressure`，保留满容量执行器、
一个正在执行的客户端回调，以及 CANCELLED/成功回复两类待重试终止任务。
仅在该测试可执行文件通过 `--wrap=tr_reactor_complete` 拦截匹配 Endpoint
的重试完成项，不污染生产库：

- 第一次提交确定性地返回 `TR_ERR_SYS`，要求调度失败计数恰好一次；
- 第二次完成项提交在真实工作线程中、取得外层强引用后、执行
  `__real_tr_reactor_complete()` 之前被条件变量门闩暂停；
- 测试线程等待明确进入门闩，随后核对失败计数、成功计数为零，
  `tr_reactor_quiesce()` 仍可推进，Endpoint 统计查询成功，
  Reactor 所有者上下文拒绝递归销毁，业务尚未全部完成；
- 打开门闩后要求真实完成项成功提交，所有业务终局回调各一次、
  原结果不被取消/断连替换，池预算收敛，然后按真实停止屏障销毁。

门闩没有通过 `sleep` 猜测发布时序；等待设置 15 秒失败上限，
以显式状态谓词控制交错。测试目标、总测试数量和编译配置保持原样。
本地 GCC 14 O2 使用既有完整生产静态对象链接，八轮执行均通过；
这只是定向本地证据，原生 GCC/Clang、ASan/UBSan、TSan、安装与两套
静态分析仍须以**该修订精确 head** 的远端 CI 为准。

### 尚需验证

此测试没有撤销所有者引用来制造真正的最后一次释放，也没有证明
所有异常执行器准入错误及共享执行器停止竞争。GCC 11 的此条诊断仍为
`review_state=unverified`；Clang 14 的 11 条与 GCC 另一条 Channel 告警保持不变。
本测试是补齐重试成功提交窗口与回调静默边界，不是发布或合并批准。
