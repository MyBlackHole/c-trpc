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

## 后续收敛顺序

1. 优先复核 RPC 引用计数与 Server 部分构造的生命周期交错。
2. 确认 Reactor 所有者/队列不变量和 Channel 发送缓冲所有权转移。
3. 对实际缺陷修改生产代码并补专测；对有证据的工具误报精确说明。
4. 缩小上述两套基线，再升级 GCC 和 Clang 到尽可能完整的零告警阻断。
5. #154 的跨架构、长时间负载、故障恢复和安全边界验收仍须独立完成。
