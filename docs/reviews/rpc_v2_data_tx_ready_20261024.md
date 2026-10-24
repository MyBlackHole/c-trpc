# RPC V2 DATA TX 容量恢复通知审查记录

日期：2026-10-24。基线：`feat/rpc-v2-call-ref-243`，提交
`fe4dc8747bcb65168247fed40fb3b32f9f86a59`。本地分支：
`fix/v2-data-tx-ready-247`。版本：`2.0.0-dev.26 -> 2.0.0-dev.27`。

## 范围

本切片处理压缩 DATA 分帧因 Reactor 普通 TX 项池耗尽而等待 25ms 维护定时器的情况。
Reactor 为 DATA TX 池增加固定 intrusive FIFO waiter、跨线程归还唤醒和 owner ready 事件；
DATA 与 CONTROL 容量事件轮转并共享每轮 TX 分派预算。Channel 仅在 DATA TX 池真实耗尽时
登记等待；分片预算、命令队列等其他 `TR_AGAIN` 仍保留 timer fallback。

未新增 SDK API、后台线程、动态等待节点或线协议字段。Channel 在所有者串行上下文登记和
撤销精确连接代次的 waiter；连接关闭和 handler 替换会清理等待项。

## 验证

所有测试临时目录、缓存、日志和构建产物都位于当前 worktree 的 `build/`。

- Release：`xmake test -P . -v -j1`，90/90 通过，耗时 88.320 秒。
- ASan/UBSan：GCC 下 `test_reactor_control_ready/*` 18 组通过；`test_transport/*` 全部通过。
- TSan：GCC 下 `test_reactor_control_ready/*` 18 组通过；`test_transport/*` 全部通过。
- 格式：`scripts/check_source_format.py` 使用 clang-format 18.1.8 检查 6 个变更源码文件，
  0 处失败；merge base 为 `fe4dc8747bcb65168247fed40fb3b32f9f86a59`。

Reactor 测试覆盖重复登记、取消、handler 替换、连接关闭与槽复用、跨线程唯一唤醒、
watch 前容量恢复竞态、各队列 FIFO、混合通知轮转和共享预算。Transport 集成测试使用
单项 DATA 池、真实双端 Channel 与压缩消息，延后 retry timer 后释放池项，验证 ready
通知推进发送、两条流各自只收到一条正确消息，并检查预算归还。

## 性能、风险与回滚

每个 Connection 固定增加一个 DATA waiter 标志和两个链表指针；没有为等待动态分配对象。
跨线程池项释放仅在最后一个占用项归还且存在 waiter 时写既有 eventfd。容量恢复仍是
通知而非资源预留，竞争失败重新登记，必要时由既有 timer 兜底。当前没有 40k 吞吐或
全局性能声明。

回滚可通过 revert 本切片提交恢复 `2.0.0-dev.26` 并移除新增 watcher、Channel 接入、测试
与记录；没有数据迁移和线协议兼容步骤。

## 未完成范围

本记录只覆盖 #247/#241 的 DATA TX 池容量恢复切片，不表示 #247、#241 或 RPC V2 整体完成。
状态模型检查、确定性故障调度与失败重放、分层诊断、分域预算、多 DATA/慢消费者压缩矩阵、
40k 容量和性能验收仍需后续工作。详见
[`v2_remaining_work.md`](v2_remaining_work.md) 与 [GitHub #247](https://github.com/MyBlackHole/c-trpc/issues/247)。
