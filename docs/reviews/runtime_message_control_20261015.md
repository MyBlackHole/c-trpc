# MSG-D2：Runtime 消息取消与多分片、半销毁回归

## 基线与有限范围

基于 #256 的 `511e691407826939fcfabe19a12532462909f584`，原 Git 树为
`c23f5105741ba6629e2d17da8cae5040f7d7f1dd`。该分支已经实现 Runtime 持有
MessageService、父 Reactor 停止后恢复租约、显式停止域交付和输出预算外壳。
本轮不重做这些机制，也不继承 #257/#259 平行分支的实现或验证结论。
#258 的重复回收实现已关闭；其指出的同步对象半销毁场景在本轮使用 #256
真实接口独立验证，不复制第二套回收 API。

本轮改变的是库内 Runtime 消息控制入口，并补两组真实集成回归：

| 文件 | 范围 |
|---|---|
| `src/runtime/runtime.c` | 增加分片消息取消；提交/取消校验指定 owner 及半销毁状态 |
| `src/runtime/runtime_internal.h` | 明确取消返回值、执行上下文与生命周期责任 |
| `tests/test_runtime_message_control.c` | 三分片中的取消隔离、启动回滚、保留输出与预算外壳 |
| `tests/test_message_service_partial_destroy.c` | 真实条件变量销毁后的失败接管与换线程重试 |
| `xmake.lua` | 只追加两个真实集成目标及测试专用链接包装 |

未修改 SDK 安装头清单、线协议、任务/消息服务回收实现、既有测试、工作流或
静态分析基线。仍不是公开 Client/Server 压缩配置或 RPC 网络压缩功能。

## 取消入口与接受责任

`tr_runtime_shard_message_cancel(shard, handle)` 不向消费者暴露 MessageService
指针；通过所属 Reactor 的同步操作，仅在对应所有者上借用当前服务，再由服务
及执行器检查实例、槽位和代次。新入口不创建任务池、额外消息队列或后台线程。

| 状态 | 返回与责任 |
|---|---|
| 运行中的排队/执行任务，取消先于终局提交 | `TR_OK`，仅接受取消意图，原完成回调仍需交付 |
| 另一启用消息服务的分片、旧实例、已退役任务 | `TR_ERR_STALE`，不能按同号槽位取消另一任务 |
| 结果已提交、回调正在交付而槽位尚未退役 | `TR_ERR_STATE`，不能再次完成 |
| 分片没有配置消息服务 | `TR_ERR_UNSUPPORTED` |
| 父已停止或 Runtime 已释放该 Reactor、仅预算外壳仍存活 | `TR_ERR_CLOSED` |
| 空分片指针 | `TR_ERR_INVALID` |

普通外部线程和目标分片 owner 可调用。另一 Reactor 的 owner 发起同步提交或
取消会立即返回 `TR_ERR_STATE`，而不是在一个 owner 内同步等待另一 owner。
这是明确收窄的内部契约，避免形成 A→B→A 的同步等待链；本轮没有声称复现了
原来的死锁，也没有把该校验描述为安全认证。

取消成功不代表最后回调已结束，不允许提前释放 arg、输入提供方或输出引用。
句柄和 shard 都不是强引用，操作仍须与 Runtime start/stop/destroy 在外部
串行协调；本轮不提供任意并发 free 的保证。

原提交入口同时处理半销毁外壳：当 reactor 已释放而预算仍由应用输出持有时，
返回 CLOSED，不将内部空 reactor 泄漏成 INVALID，也不消费输入或修改任务句柄。

## 多分片真实集成测试

`test_runtime_message_control` 创建三个真实 Runtime 分片，前两个配置消息服务，
第三个保持关闭；使用真实工作线程、Zstandard、Reactor 及恢复/预算逻辑。

- 分别对编码和解码固定首个实际算法调用后的工作线程；取消运行任务与排队任务，
  重复取消仍正确，清理前不提前释放输入；另一分片同时可以正常完成。
- 同一任务句柄在另一启用分片返回 STALE，在未配置分片返回 UNSUPPORTED。
  错误 owner 的提交和取消返回 STATE，原输入地址/长度、句柄和预算不变。
- 完成回调内取消返回 STATE；跨过 owner 回调返回屏障后，旧能力返回 STALE。
  停止/重启后，新实例不接受旧能力，已停止的 Runtime 返回 CLOSED。
- 在第二个启用分片创建消息工作线程时注入 EAGAIN，验证前面已经启动的分片
  进入正式回滚，各预算恢复创建后基线；去除失败后重新启动成功。
- 两个分片分别保留成功输出；stop 后 destroy 逐步释放资源却保留 Runtime
  预算外壳。第一份输出还存在切片时不能早退役；释放第一分片最后引用后仍要
  等待第二分片输出，后者数据继续有效；全部释放后最终 destroy 成功。
- 部分销毁后 start 被拒绝，消息提交/取消返回 CLOSED。没有把预算计数为零
  当作线程静默的替代品，测试按实际 stop 与回调屏障同步。

## 实际同步对象的半销毁测试

`test_message_service_partial_destroy` 不仅包装整个 executor_destroy 让其立即
失败，而是调用真实实现，让 pthread_cond_destroy 先成功，再让
pthread_mutex_destroy 返回注入的 EBUSY 且不实际销毁互斥锁。

第一次在原 Reactor owner 中失败；旧 owner 尚未 join 时禁止回收线程接管。
父 stop/join 完成后，第二次回收再次失败，fd、预算及父恢复租约仍保留，父
start/destroy 均被阻止。第三次只完成尚未完成的互斥锁销毁，条件变量不得
重复销毁、signal、broadcast 或 wait。测试在调用 pthread 前检查已退役地址，
不靠实际触发未定义行为来判断。

第二种交错让中途失败的回收线程也退出，再由新的串行回收线程继续。条件变量
实际销毁总数仍为一次，互斥锁两次失败后一次成功，最终 fd 已关闭、预算归零。

这验证了 #256 已有 `executor_retiring` 和销毁进度保护的正确路径，**不宣称
本轮修复了一个已复现的半销毁生产缺陷**。POSIX 规定已销毁同步对象不得继续
使用；测试注入的 EBUSY 是失败契约验证，不表示正常无竞争销毁必然返回该错误。

参考一手规范：
- [pthread_cond_destroy](https://pubs.opengroup.org/onlinepubs/009695399/functions/pthread_cond_destroy.html)
- [pthread_mutex_destroy](https://pubs.opengroup.org/onlinepubs/9799919799/functions/pthread_mutex_destroy.html)

## 负向对照与证据

两个局部反向实验只改临时源码副本，未上传至生产树：

1. 删除新增的跨 owner 拒绝，真实控制测试在错误 owner 取消的 STATE 断言处失败。
2. 去掉 #256 已有的 retiring 分支保护，让停止回收再次 broadcast 已销毁的
   条件变量，半销毁测试在 pthread 调用前的存活检查处失败。

两者均精确触发预期断言，不以超时或无关链接错误代替负向通过。第二项证明
新增测试能阻止已有防护退化，不代表当前原分支执行过非法 pthread 操作。

## 本地实际验证身份

复用已存在的只读源码快照工作流，归档解压后核对 285 个 Git blob；原树与
上述 #256 完整相符。归档 ZIP SHA-256 为
`622982bae02bba978594669e2ab9a4469c38b6eb20c251e48791390daf9ae04e`。
本轮没有使用 Reactor 替身；按 Xmake 清单的 36 个实际生产翻译单元进行本地
编译交叉验证。工作容器没有 Xmake，故不称为正式 Xmake 构建或安装验收。

| 检查 | 本地实际结果 |
|---|---|
| GCC 14.2 / Clang 17，O0/O2，两个新增测试 | 四组构建并执行通过 |
| GCC O2 两个新增测试重复 | 20/20 轮通过 |
| Clang ASan/UBSan，完整生产对象与两个新增测试，含泄漏检查 | 通过，无诊断 |
| GCC TSan，完整生产对象与两个新增测试 | 通过，无诊断 |
| 五个既有真实相关测试，GCC O2 | 全部通过 |
| 修改的 runtime.c，GCC fanalyzer / Clang analyzer | 无诊断 |
| 修改的 runtime_internal.h，GCC/Clang 自包含 | 2/2 通过 |
| 两个局部反向实验 | 均在预期断言失败 |

五个相关测试是 test_runtime_message_recovery、test_message_service、test_runtime、
test_runtime_threads 和 test_task_executor。Zstandard 为本机预构建的 1.5.7
静态库，不能宣称第三方对象自身被 ASan/TSan 插桩。新测试使用 CHECK，不依赖
断言宏里的副作用；本轮没有另行声称完成 NDEBUG 专项矩阵。

首轮编译暴露新测试结构初始化的 missing-braces 警告，已修正为正确初始化，
没有降低告警等级或改生产配置。固定 clang-format 18.1.8 只处理变更行及新测试，
不重排旧源码。正式 Xmake、固定分析器、完整 CI 及新测试实执行结果须按新提交
在 PR 中记录，不能继承 #256、#257 或其他平行分支的绿色。

## 剩余范围

这里新增的是已具备恢复租约的 Runtime 分支上的控制入口与回归证据。#259 另有
基于 #257 的可选绑定方案，两者尚未合并；本轮不关闭、覆盖或混合两条方案的
接口与验证。后续应明确统一合并路线，不能把本分支证据当作另一方案也已验收。

仍无公开 Client/Server 压缩配置、RPC 独立信封、方向性算法协商、网络信用、
连接组 DATA 或 TLS。取消 API 不改变服务的完成执行域，停止域结果仍按 #256
约定仅交付取消清理，不把回收线程伪装成正常网络 owner。没有性能结论，也没有
扩大到任意并发生命周期或不合作工作线程强制终止。阶段 PR 保持草稿，不关闭
#235/#241/#243/#248 等整体任务。
