# 2026-10-14：Reactor 发送队列所有者不变量与失败回收

## 路径与状态

针对 GCC 11 `src/execution/reactor.c:1194` 的
`analyzer-null-dereference`，以主分支 `fb2a5cf7` 的生产代码为审查基线。

`tr_connection_flush_tx()` 只有在连接处于 `TR_CONN_ACTIVE`、
本轮有 TX 字节预算且 `connection->tx_head != NULL` 时调用
`tr_connection_next_tx()`。该函数按顺序选择：

1. `tx_active != NULL && tx_active->wire_pos != 0`：部分写帧不可重排；
2. 否则选择 `tx_head`（主 FIFO 队首非空）；
3. 仅当队首是 DATA、`tx_control_head` 非空、突发配额未耗尽且控制项允许越过 DATA 时，改选控制链首项。

正常所有者模型内，三种选择都不应返回空指针。主 FIFO 的链接和控制
FIFO 的链接仅由 Reactor 所有者线程调整；单个 TX 项释放后，完成流程清除
`tx_active`，同时调整主链表、控制子链以及资源池计数。关闭连接先清除
就绪调度，再归还整条 TX FIFO 的载荷和 TX 项，最后使连接代次失效。

**修复**：选择后、首次解引用 `item->wire_len` 前显式验证非空。
若不变量失效，向现有 `tr_connection_close_internal()` 提交
`TR_CONN_EVENT_ERROR/TR_ERR_STATE`，而非直接解引用、无声继续、
暴露局部 TX 项或令库 `abort()`。异常连接在同一个所有者线程收敛，
其他连接不受影响。该保护是最后一道防线，不代表故意破坏列表是受支持操作。

## 确定性回归

扩展已有 `test_tx_priority`：在真实单 Reactor / socketpair 上，
用发送预算 1 字节先让 DATA 帧真正成功写出首字节，再使包内
`--wrap=sendmsg` 在执行下一个发送时注入恰好一次 `EIO`。
首次写出后由条件变量阻塞 Reactor 工作线程，调用方此时插入 PING、
WINDOW_UPDATE 和第二个 DATA 缓冲，然后放行。
实际连接错误回调作为释放完毕同步点，之后验证：

- 错误事件恰好出现一次、状态为 `TR_ERR_SYS`；
- 注入的 `EIO` 恰好发生一次；旧连接只写出第一个字节；
- 已移交的两个 DATA 缓冲都归还原池，`48/48` 全部空闲；
- 连接状态转为 `FREE`，旧句柄返回 `TR_ERR_STALE`；
- 同一槽位可安全复用新代次，新的完整 DATA 帧仍可发出且 CRC 正确；
- Reactor 总 TX 字节与真实发送字节、轮次字节/分派预算仍一致。

该测试证明**真实错误发送后的清理、资源回收与槽位隔离**，
但不直接制造内部非法 FIFO 指针（不向生产对象写入未定义的损坏状态）。
已有 `test_tx_priority` 同时覆盖控制队列越过、突发限制、
EAGAIN、部分头部/载荷写入、强制关闭后复用。

## 告警边界与门禁

- `reactor.c:1194` 旧告警先保持 `unverified`，直到真实 GCC 11 的完整
  生产编译数据库证明其消失；届时只移除这个**已消失的**精确记录。
- 其他 GCC/Clang 告警不能据此分类或移除。
- 每次更改后的精确提交必须通过 GCC/Clang、ASan+UBSan、TSan、
  两套静态分析、格式、SDK/CRC 与 RPC 冒烟。
- 独立的压缩与 TLS 设计需求不在本修复 PR 中引入任何生产能力或 SDK 字段。
  #154 在这项完成后仍保持开放。
