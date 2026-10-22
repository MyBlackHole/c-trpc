# 发布前静态诊断路径审查（2026-10-18）

## 范围、证据与结论

本记录审查 `b39dafac818b9f94976068b24eb6021447cdb2ac`，完整源码树
`20c96f0dd165de055cfb36c1b9940dfe019cb087`。该源码来自原生制品归档，已在独立目录重算 Git 树。
本提交只增加审查文档，不改变生产代码、测试、构建、资源容量、超时、诊断基线或检查项。

逐项对照固定 GCC 11 的两条和 Clang 14 的十一条原始路径后，**本轮将这十三条报告路径判定为分析模型误报**：十一条未跟随作用域清理，一条未跟随协议缓冲的所有权转移，一条没有维持已取得强引用与配对释放的数量关系。此结论只适用于下列明确路径及当前源码，不是整个模块、全部调用者或所有故障路径的正确性证明。

这是一次源码与已有原生证据的路径审查，仍须维护者独立复核。两份 `*_known_warnings.json` 原样保留 `review_state: unverified`，不删除、改写或全局抑制任何诊断；本记录不充当独立审批，不宣布零告警，也不自动关闭 #154。未来有新调用者、所有权变化、坐标或诊断文本变化时必须重新审查。

证据基线：[完整 CI 39119146561](https://github.com/MyBlackHole/c-trpc/actions/runs/39119146561)。本輪重新核对 ASan/UBSan、TSan 两份原始归档，各85个唯一测试开始与85个唯一通过，且包含下述四个重点测试；不是在本机重新执行了85项测试。固定双静态各45个生产单元。四份原始 ZIP 摘要为：

| 制品 | ID | SHA-256 |
|---|---:|---|
| ASan/UBSan | 12133706963 | `30ad25d46e3d5f3fceadead5032cd5a7cbc8bde874c99c750a170934b26fc5ab` |
| TSan | 12133487907 | `133a5951e1fe05a041622525a6cfbb584c9dd551ac0e55abb7eb1e22f3ed03c3` |
| GCC 11 | 12133242751 | `e195750e6c4d2085c2964acf0fb201d9c259ebb072b10e1b947004bfd8ae6f9c` |
| Clang 14 | 12133791261 | `7edb989a385779a9e473a545e49fbfa5baf953d928f78457185bd2fcaff4eafb` |

## 十一条 Clang 诊断

以下均为 `clang-analyzer-unix.Malloc`；完整文本、列号和生产单元身份仍以 [精确基线](../static_analysis_known_warnings.json) 为准。这里逐条列出，而不是按一个 cleanup 最小实验豁免所有构造器。

| 编号 | 精确报告位置及对象 | 报告分支、真实清理路径与已有故障证据 |
|---|---|---|
| C01 | `src/execution/buffer.c:67:9`，`buffers` | storage 分配失败，转入 fail_budget 归还两档预算；buffers 尚未 take，返回时执行 `tr_buffer_array_cleanup`。`test_primitive_constructor_faults` 的第二分配失败用例跟踪逐地址释放、预算归零和重新初始化。 |
| C02 | `src/execution/buffer.c:75:9`，`storage` | mutex 初始化失败，两个指针都未转移，fail_budget 后分别由 storage/array cleanup 释放；不销毁未初始化锁。原语故障测试注入该 mutex 失败，检查两份内存与预算配平。 |
| C03 | `src/execution/buffer.c:139:3`，`buffers` | 动态池 mutex 初始化失败，先归还描述符预算，返回时 cleanup 释放未转移的描述符；原语故障测试验证精确失败及失败后重建。 |
| C04 | `src/execution/command_queue.c:26:10`，`items` | mutex 失败发生在 items 发布前，`tr_command_array_cleanup` 释放；cond 失败还先销毁已初始化 mutex。原语故障测试逐一覆盖分配、mutex、cond 失败及重建。 |
| C05 | `src/execution/completion_queue.c:27:10`，`items` | 同上，使用独立的 `tr_completion_array_cleanup`；原语故障测试核对分配与同步资源分别只回收一次。 |
| C06 | `src/execution/reactor.c:320:10`，`items` | `tr_tx_pool_init` 的 mutex 失败时 items 尚未 take，由 `tr_tx_item_array_cleanup` 释放。`test_complex_constructor_faults` 分别注入 TX/CONTROL TX 的分配与锁失败，检查 Reactor 未发布、预算和 fd 归还。 |
| C07 | `src/execution/timer_queue.c:123:10`，`entries` | entries 成功、heap 失败，返回时释放 entries；两个分配在条件前独立执行。原语测试分别注入第一、第二次分配失败，检查另一份成功分配也被释放。 |
| C08 | `src/execution/timer_queue.c:123:10`，`heap` | entries 失败但 heap 成功，返回时释放 heap；不能因同一行而与 C07 合并成一条。原语故障测试要求两次分配均实际尝试。 |
| C09 | `src/rpc/rpc.c:3533:10`，`group_mem` | threads 分配失败，group_mem 尚由 `TR_AUTO(tr_rpc_group_mem_cleanup)` 持有，返回时 free。复杂构造故障测试验证第二分配失败不发布组指针，随后重新构造成功。 |
| C10 | `src/rpc/rpc.c:3536:10`，`threads` | group mutex 失败时 group_mem/threads 均未 take，作用域退出分别释放；cond 失败分支还先销毁 mutex。复杂构造故障测试覆盖两种同步初始化失败。此构造尚未创建工作线程。 |
| C11 | `src/transport/channel/channel.c:3254:10`，`build.channel` | 原诊断明确选择非法 role 的早期返回。`build.channel` 已登记到 `TR_AUTO(tr_channel_build_cleanup)`，但未发布 handler/timer；cleanup 释放 Channel，两个 ready 标志仍为0，不销毁未初始化的同步对象。复杂故障测试验证非法角色/版本等拒绝与连接继续有效。 |

共同语义来自 [cleanup.h](../../src/cleanup.h)：cleanup 变量持有资源，只有 `take()` 清空本地指针并移交。上述十一条路径在失败前都没有相应 take。`buffer.c` 的预算归还与词法内存释放分别核对；队列和组的同步对象按成功初始化标志回滚。Channel C11 仅证明早期非法配置分支，不把它扩展为发布后撤销回调的完整证明。

归档内 `cleanup-model-probe.json` 记录固定 Clang 14 对照：实际编译后的自动清理执行通过，分析器却对自动清理和真实泄漏都报告诊断。它解释工具差异；项目级判定仍以上表逐路径及真实故障注入为依据。

## G01：GCC 引用计数路径的释放后访问报告

位置：`src/rpc/rpc.c:2692:65`，`analyzer-use-after-free`，原消息 `use after ‘free’ of ‘arg’ [CWE-416]`。

原路径为：`tr_rpc_retry_pending_executor_on_owner` → `tr_rpc_queue_task_locked` → push 失败 → `tr_rpc_endpoint_put` → 推断最终释放 → 回到回调写 `pending_executor_work`。审查不能只说“已有引用计数”，而要确认这个具体 put 不是最后一次。

1. 全源码只有 `tr_rpc_schedule_pending_executor_retry` 向 Reactor 提交该重试回调。它在 `rpc.c:2716` 成功 get 后才发布完成项，成功时由回调持有这一个强引用；提交失败才在调用方 put，未提交的回调不得执行。
2. 回调直到解锁后的 `rpc.c:2700` 才归还自己的强引用。扫描期间，该引用不能被任务或外部销毁借用。终局及销毁等待所有者引用的其他机制不是这一步推导的唯一依据。
3. `tr_rpc_queue_task_locked` 在 `2236` 另取任务引用，成功后才增加 task_refs 并 push。push 拒绝时在 `2248` 只归还刚取得的任务引用。
4. 因而该失败 put 前至少有“回调引用 + 临时任务引用”两个；配对 put 后至少剩回调引用。`refcount.h` 的 get/put 通过 CAS 对当前计数增减，只有实际 `1 → 0` 才返回 last。原诊断到达 free 所需的 last==1 与这条引用账本矛盾。不能从“锁住了”单独推导寿命，关键是未归还的回调强引用。
5. 回调结束才允许归还自己的引用，此后没有再次解引用 endpoint。

判定：在上述来源和所有权契约下，报告的释放后访问路径不可达，属于原子引用计数关系未被分析器维持的模型误报。`test_rpc_terminal_backpressure` 已覆盖真实执行器满、终局保存、重试提交失败及后续恢复；该测试在本证据基线两个检查器中通过，但它不是所有引用交错的穷举，更不是对原诊断分支的逐指令故障复现。若回调新增调用来源或 get/put 配对改变，本项立即重新审查。

## G02：GCC 协议缓冲逃逸报告

位置：`src/transport/channel/channel.c:687:9`，`analyzer-malloc-leak`，原消息 `leak of ‘owned’ [CWE-401]`。

诊断在 `tr_channel_protocol_buffer_acquire()` 的成功 return 上终止，认为 calloc 得到的 owned 丢失。实际结构把 `struct tr_buffer buffer` 放在首成员，函数以 `*out = &owned->buffer` 显式向调用者交付这份分配，并设置 `release_cb = tr_channel_protocol_buffer_release`，该回调将首成员地址转换回宿主后 free。

`tr_channel_send_hello` 的输出保存在带 `TR_AUTO(tr_buffer_cleanup)` 的局部变量中：发送失败时仍由调用者释放；只有 `tr_reactor_send` 返回 TR_OK 才 take，让已接受的 TX 项持有。`tr_reactor_sendv_limited` 的拒绝分支会清空 item 的载荷指针后退还发送项，不会错误接管载荷；成功则由发送完成/关闭回收调用 `tr_buffer_release`，再分派 release_cb。

判定：成功返回时所有权并未消失，原报告漏掉通过首成员输出和释放回调表达的逃逸关系。`test_channel_hello_ownership` 在真实发送边界记录每个协议缓冲地址，覆盖 HELLO 暂时/永久拒绝、成功握手、ACK 拒绝、并发启动以及停止后恰好一次回收；已在该基线的 ASan/UBSan 和 TSan 中实际通过。不把此结论扩大为所有连接替换、恶意对端或无限期不关闭场景的证明。

## 主线集成与剩余发布门禁

#281 已改为直接面向 main 的同一组合候选入口，临时恢复草稿。#278/#279 是祖先阶段记录，不先逐个合并旧版本；#280/#282/#283 已选定的代码和必要覆盖在组合里，最终落地主线之前不自动关闭这些历史 PR。

当前阶段的确定成果是：13条报告路径有逐项判定和依据，历史未解释的“全部待查”已收敛成可独立复核的清单；不是十三个生产缺陷被改掉，也不是原告警从工具输出中消失。

仍需以本次 main 基线与最终 head 完成累积格式检查、完整原生 CI、安装和精确合并树核对。源码增删或格式整理都不能直接继承原 head 的绿色。维护者还需独立审查整链生命周期、未选用的早期 Runtime/Endpoint 方案以及此表结论；本文件不表示139个累积文件已经全部人工审查。

开发预览仍明确不提供 TLS、网络压缩、自动重连、类型化编解码或 HELLO 自动恢复；无新功能不能成为跳过安全审查的理由。稳定版、跨架构、长稳和 V2 性能结论不在本次批准范围。main、发布标签和版本号均未由本记录改变。
