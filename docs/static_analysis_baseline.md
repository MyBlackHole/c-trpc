# 2026-10-12：生产静态分析首轮诊断基线

## 可追溯输入

- 原始分析提交：`a6d8f7d9df163e4c38aae771999cf08923bb2d70`（PR #223 首轮，非其最终提交）。
- 已验证的基础主分支：`29bf384aa59903b5d79453d5d5cf607c9bb6fa62`（PR #222）。
- 环境：Ubuntu 22.04 / x86_64，Clang release，CRC 自动后端，`clang-tidy-14`（LLVM 14.0.0）。
- 正式输入：真实 Xmake `trcore` 编译数据库，经生产源码与完整配置身份复查。
- 运行结果：31 个生产翻译单元全部执行，11 条日志级告警，分析器非零退出 0 个。
- [完整运行日志及数据库产物](https://github.com/MyBlackHole/c-trpc/actions/runs/38189946792/artifacts/11720818222)。

## 告警位置与初步成因

| 生产源文件 | 行号 | 告警数量 |
|---|---|---:|
| `src/execution/buffer.c` | 67、75、139 | 3 |
| `src/execution/command_queue.c` | 26 | 1 |
| `src/execution/completion_queue.c` | 27 | 1 |
| `src/execution/reactor.c` | 305 | 1 |
| `src/execution/timer_queue.c` | 123（两条独立分配） | 2 |
| `src/rpc/rpc.c` | 3539、3542 | 2 |
| `src/transport/channel/channel.c` | 2602 | 1 |

全部 11 条均为 `clang-analyzer-unix.Malloc` 的“潜在内存泄漏”告警。
逐个对照当前源代码，以上报告均涉及采用
`TR_AUTO(...cleanup)` 的作用域资源或封装构造状态；可在
`src/cleanup.h` 和各生产文件中找到清理或所有权转移定义。
Clang 14 分析器对 GCC/Clang `cleanup` 属性的处理能力可能影响这些路径的
泄漏推断。**这只是初步归因，尚未对每条路径完成独立正确性证明，
不能将 11 条直接登记为已确认误报，更不能删掉或全局关闭 malloc 检查。**

## 本轮失败路径的代码级初查与工具差异验证

| 告警位置 | 触发条件 | 源码所有权依据 | 仍需证明的边界 |
|---|---|---|---|
| `buffer.c:67` | 数据存储分配失败 | `buffers` 仍在 `TR_AUTO` 作用域内 | 分配失败与预算归还 |
| `buffer.c:75` | 互斥锁初始化失败 | `buffers/storage` 均仍由词法作用域持有 | 两项释放、预算完整归还 |
| `buffer.c:139` | 动态池互斥锁初始化失败 | `buffers` 尚未通过 `take()` 转移 | 构造失败无残留 |
| `command_queue.c:26` | 命令队列互斥锁失败 | `items` 尚未发布，由 `TR_AUTO` 清理 | 同步对象失败矩阵 |
| `completion_queue.c:27` | 完成队列互斥锁失败 | `items` 尚未发布，由 `TR_AUTO` 清理 | 同步对象失败矩阵 |
| `reactor.c:305` | TX 池互斥锁失败 | `items` 尚未发布，由 `TR_AUTO` 清理 | Reactor 整体回滚 |
| `timer_queue.c:123`（两条） | 定时器入口/堆分配失败 | `entries/heap` 两个独立 `TR_AUTO` 所有者 | 部分成功的分配矩阵 |
| `rpc.c:3539、3542` | 工作线程数组或组互斥锁失败 | `group_mem/threads` 未转移到执行器组 | 线程与条件变量初始化失败 |
| `channel.c:2602` | Channel 配置校验失败 | 未发布的 `build.channel` 由 `tr_channel_build_cleanup()` 清理 | 其他发布后回滚的所有权屏障 |

上述只确认各告警分支处存在明确的词法作用域清理路径，不代表完整故障矩阵通过，
尤其不将 Channel 发布之后的复杂失败情况等同于早期参数校验失败。

补充了 `scripts/probe_clang_cleanup_analyzer.py` 作为工具行为对照：
同一最小 C 程序分别包含 `__attribute__((cleanup))` 自动清理和真正没有释放的分支。
实际编译执行要求提前返回、正常返回分别调用清理函数；
同时使用固定版本 `clang-14 --analyze` 的 `unix.Malloc` 检查识别两条潜在泄漏。
这个对照只能证明分析器在最小场景中没有正确推导自动清理，
**不能自动将项目中全部 11 条宣告为误报**。
参见 [Clang cleanup 属性定义](https://clang.llvm.org/docs/AttributeReference.html#cleanup) 与
[Clang 静态分析误报处理说明](https://clang.llvm.org/docs/analyzer/user-docs/FAQ.html)。

## 精确告警回归门禁

`docs/static_analysis_known_warnings.json` 明确保存固定分析器版本、构建配置，以及 11 条
**尚未独立确认为误报**的诊断。每条以生产翻译单元、报告文件、行号、列号、
检查项和完整警告文字作为唯一身份；定时器队列同一行的两条不同告警分别记录。

新的 `scripts/check_static_analysis_baseline.py` 从 CI 原始逐单元日志重新解析，
核对数据库身份、固定工具版本、已启用检查项、生产覆盖、每单元退出码和警告总数。
新增诊断、告警文本或位置漂移、原有告警消失、日志缺失或格式不可解析都使门禁失败；
消失的告警也须修订基线，不允许陈旧例外长期遗留。报告
`warning-gate.json` 保留两侧差异供审查。

这是一道**增量回归阻断门禁，不是零告警安全门禁**：已记录的 11 条风险不会凭此获得
真实性豁免；未来确认所有告警处理完成后，才可删除临时基线并启用 `--strict`。
这项门禁不改变生产分配/清理代码，也不关闭 `clang-analyzer-unix.Malloc`。

## 基础构造器的确定性故障注入（#225）

上一轮 11 条中有 7 条集中在 Buffer、Command Queue、Completion Queue 和
Timer Queue 的构造失败路径。新增
`tests/test_primitive_constructor_faults.c`，使用 **仅测试目标**的链接期
`--wrap` 包装真实 `malloc/calloc/free/pthread_mutex_init/`
`pthread_mutex_destroy/pthread_cond_init/pthread_cond_destroy` 调用；不向生产库
加入注入开关，也不修改资源所有权语义。

| 诊断位置 | 真实故障注入 | 失败后核对 |
|---|---|---|
| `buffer.c:67、75` | 首次/第二次分配失败、互斥锁初始化失败；同时校验两档预算拒绝 | 已获分配只释放一次、预算归零、失败池可重新初始化 |
| `buffer.c:139` | 动态池描述符分配及互斥锁初始化失败 | 未发布的描述符按作用域释放、预算归零 |
| `command_queue.c:26` | 描述符分配、互斥锁和条件变量初始化失败 | 分配及同步资源各自配平，失败后重新初始化可用 |
| `completion_queue.c:27` | 描述符分配、互斥锁和条件变量初始化失败 | 同上，重新初始化后完成项仍可准入/消费 |
| `timer_queue.c:123`（两条） | 两次独立分配各自失败，包括第一失败但第二仍执行 | 已成功分配的另一份内存被释放、重建后定时器可注册/注销 |

测试包含各构造器正常成功/销毁和失败后的原对象重建，共 20 个顶层场景
（35 轮定向注入/重建计数验证）；失败路径不可对尚未初始化的队列同步对象
强行调用完整销毁。对每轮校验成功分配与释放数相等、每个地址只释放一次，
每个成功初始化的互斥锁/条件变量恰好销毁一次，Buffer 预算在失败或销毁后为零。
地址和线程检查器矩阵也执行相同测试，而不替代上述定向故障证据。

[PR #225](https://github.com/MyBlackHole/c-trpc/pull/225) 对应的 CI
[首次完整通过记录](https://github.com/MyBlackHole/c-trpc/actions/runs/38214487924)
覆盖 GCC（debug/release）、Clang（release）、ASan+UBSan、TSan，以及兼容安装、
CRC 和原有 Clang 14 静态分析回归门禁；实际发布前应以最终合并提交的 CI 为准。

**这属于这 7 条告警所指向的特定构造失败边界的运行时证据，不等于 Clang
误报的形式化证明。** 不修改现有 11 条 `review_state=unverified` 跟踪状态，
不关闭 `clang-analyzer-unix.Malloc`，也不因此宣布零告警。
另外 4 条 Reactor/RPC/Channel 生命周期风险继续等待下轮审查。

## Reactor、RPC 与 Channel 的定向构造故障验证

本轮新增 `tests/test_complex_constructor_faults.c`，只在测试可执行文件中
使用链接器 `--wrap` 截获分配、释放、互斥锁和条件变量初始化/销毁；
生产静态库、公共 API、协议及运行时配置均不增加故障注入开关。

| 既有静态分析告警 | 定向注入/检查 | 证明边界 |
|---|---|---|
| `reactor.c:305` | TX 与 CONTROL TX 两个发送池分别在分配及锁初始化处失败 | Reactor 创建失败不发布指针；词法清理和先前原语销毁配平；预算与 fd 归还，失败后重新构造、启动、停止、销毁 |
| `rpc.c:3539、3542` | 执行器组首次/第二次分配失败，组锁/条件变量初始化失败 | 失败后组指针仍为 NULL，数组和组存储各释放一次，成功的同步初始化均对应销毁，失败后重新构造 |
| `channel.c:2602` | 角色/协议版本无效、重组池上限不匹配、Channel 三次分配及锁/条件变量初始化失败 | 未发布的 Channel 使用作用域清理，不遗留数组或同步对象，底层 Connection 仍然存活，之后可创建并正常销毁 |

计数器由 C11 线程局部状态维护，要求**故障实际触发**，并逐地址校验
已跟踪分配和释放、成功初始化和销毁次数一致。Reactor 的 TX 池锁/分配
顺序一旦变化，既有精确注入位置也必须重新审查，不能让未触发的故障测试假通过。

Channel 成功发布后，跨所有者线程撤销 handler 与 timer 的事务边界
已由 `test_channel_create_transaction`、`test_channel_detach` 等专测覆盖。
本轮补充的是它们之前的构造错误路径，**不**将早期分配失败等同于
发布后回调静默的完整证明，也不声称工具检查得到零告警。
11 条静态分析告警的 `review_state=unverified` 继续保留。

以此分支最终精确提交的 GitHub CI 为验收证据；没有实际远端通过记录前，
这些新增用例仍只视为待验证实现。

## 下一轮验收

1. 对每条告警还原错误分支以及对应 `cleanup` 释放或 `take()` 转移边界。
2. 对分配失败、互斥锁/条件变量初始化失败、复杂构造回滚设计可重复的故障注入。
3. 独立验证分析器是否未建模 `__attribute__((cleanup))`；必要时与编译后代码和其他分析器交叉核对。
4. 若需建立基线例外，必须精确绑定源文件、检查项和已复核的路径；禁止全局抑制 `clang-analyzer-unix.Malloc`。
5. 处理告警后才将 `--strict` 加入合并门禁，并在该模式下验证真实生产编译单元。

本记录不是“零告警”证书，也不意味着 #154 P1-4/D1 或正式发布就绪。
