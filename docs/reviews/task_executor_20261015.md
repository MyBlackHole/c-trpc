# EXEC-A：有界任务执行、完成交付与停止屏障

## 范围与基线

基于 MSG-B / #253 的 `eadfbad6b59320a6331a6d4c16f975d6adf6da0b`。
关联 #235、#241、#243、#248、#249 和总审查 #239。

本切片新增通用执行底座及两种测试，不修改已有 Message、RPC、Channel、
Reactor、SDK、工作流、旧测试或静态分析基线。现有 RPC 执行器尚未迁移，
新执行器也尚未被生产网络门面创建，不能将新原语视为整个运行时已经重构。

| 文件 | 职责 |
|---|---|
| `src/execution/task_executor.h` | 库内不透明执行器、任务能力、工作/完成及停止契约 |
| `src/execution/task_executor.c` | 预分配槽位、工作线程、完成队列、代次及回收 |
| `src/execution/task_executor_internal.h` | 模块私有的防回绕原语及其白盒测试入口 |
| `tests/test_task_executor.c` | 故障、容量、取消、回调静默、代次与并发 |
| `tests/test_task_message.c` | 与真实 MSG-B 的受限链接组合测试，不是网络适配器 |

## 借鉴与不照搬

参考 [libuv 工作池](https://docs.libuv.org/en/v1.x/threadpool.html) 将计算和
完成回调分到不同执行域的机制；不复制其全局线程池或任务存储模型。
本实现是显式创建的有界执行器，完成由创建者主动 dispatch，不建立隐式全局池。

[POSIX pthread_join](https://pubs.opengroup.org/onlinepubs/9799919799.2024edition/functions/pthread_join.html)
规定成功 join 的线程已经终止，并发 join 同一线程的行为未定义。
因此 start/join/dispatch/destroy 固定到创建者线程，记录逐线程 join 进度，
失败后保留未回收责任，禁止从工作线程或完成回调中自等待。

与 libuv 可跳过尚未启动工作的取消不同，本切片接受的 work 始终恰好执行一次：
工作函数必须检查取消令牌并收敛它已持有的上层操作，随后完成回调也恰好执行一次。
这适配 MSG-B prepare 已经接管输入、仍必须 run/finish 的责任，不能静默丢弃任务。

## 容量、所有权与线性化点

```text
FREE -> QUEUED -> RUNNING -> COMPLETED -> DELIVERING -> FREE
         提交接受             终局提交       回调返回才归还容量
```

全程保持：

```text
outstanding = queued + running + completed + delivering <= capacity
```

待执行队列与完成队列复用同一槽位的链接。一个已接受任务完成时，必定已经拥有
可存放完成结果的槽位，工作线程不需要申请新对象或等待完成队列空位。
完成不立即释放容量；所有者不领取结果会造成新提交 TR_AGAIN，而不是无限积累结果。

submit 在锁下复制任务描述、分配代次并加入队列是接受点；拒绝不调用用户函数、
不接管 arg、不改输出句柄。submit 不进行堆分配、不等待容量，但互斥锁争用仍可能
使调用线程短暂等待，不能宣称无锁或硬实时非阻塞。

完成回调在创建者 dispatch 的锁外执行。回调可以释放 arg，引擎随后只清空槽位，
不再解引用 arg。回调内 submit/stop 可用，递归 dispatch/join/destroy 明确拒绝。
单次 dispatch 限制回调个数；并不强制中断慢回调，因此组合层仍须约束回调工作量。

非所有者可以提交；所有者在另一线程 dispatch 时，回调可能先于 submit 返回。
任务参数必须在提交前完全初始化，输出能力的存储应与回调可释放的 arg 分开。
成功 submit 之后没有任何隐含的“调用方继续改写参数”窗口。

## 取消不等于释放

取消者只在锁下标记排队或运行任务的原子令牌，不执行释放或用户回调。
工作线程在同一把锁下提交 work_status 和 cancelled，再将结果入完成队列。
取消先发生则 cancelled 为真；终局提交后取消返回 TR_ERR_STATE，不再改写结果。

work_status 保留实际工作返回值，cancelled 单独表达执行器的最终交付决定。
某个后端可能已经生成成功结果，但执行器提交之前取消获胜；完成方必须收敛该
私有成功结果，不得将其继续向应用交付。算法结果不是 RPC 或任务的最终结果。

任务能力携带实例标识、槽位及槽位代次。旧实例、旧代次和已退役任务拒绝为 STALE；
已提交终局但尚未退役的任务拒绝取消为 STATE。实例和槽位代次均防回绕，耗尽明确
返回错误而不重新授权旧能力；极端槽位代次耗尽时需退役并创建新执行器实例。
能力不是强引用，外部仍必须保证执行器对象有效，不能用 generation 修复悬空指针。

## 三种屏障和失败收尾

1. stop：停止新准入；DRAIN 可升级 CANCEL，后来的 DRAIN 不能撤销已有取消。
2. join：用户工作尚未退出时返回 TR_AGAIN，让所有者可以继续推进完成交付；
   全部工作线程离开工作循环后才逐个 pthread_join。成功 join 不重复调用，错误
   保留对象及剩余 join 进度。线程尾部系统回收仍可能短暂等待，不承诺硬实时。
3. dispatch/destroy：join 后仍可能有完成责任；必须交付全部回调且它们返回，
   destroy 才能释放存储。完成队列未领取时返回 TR_AGAIN，不能丢弃结果后假装销毁。

create 只分配并初始化，不创建线程。start 在全部线程建立后才开放准入；中途
pthread_create 失败时停止启动，已建线程仍归对象，调用方必须 join/destroy。
这避免构造函数隐式回滚失败后丢失对象。同步原语最终销毁失败保留对象，且以后
只允许重试 destroy；成功销毁过的条件变量不重复销毁。

工作函数禁止 pthread_exit、longjmp、外部 pthread_cancel，以及同步等待依赖本
执行器或所有者继续推进的工作。协作取消不保证任意业务代码终止；不合作时保留
仍在使用的对象并向外返回未静默状态，而不是强制释放或谎称有界停机完成。

## 预算边界

create 在分配前预约执行器、槽位数组及 pthread_t 数组的请求字节；构造失败归还，
直到最终 destroy 才归还预分配容量。任务数量受同一 capacity 约束，submit 和
完成入队均不增加元数据分配。用户载荷、第三方工作区仍由上层各自记账；共享预算
域时可以同时计入总账，但不能将一个 capacity 当作所有载荷字节的硬上限。

pthread 内部分配、线程栈、分配器驻留页及整个进程 RSS 不在此字节保证之内。
stop(CANCEL) 逐槽标记，工作量与配置容量有关；这不是 CPU 时间预算。

## 新增测试

`test_task_executor` 只链接实际生产 task_executor 对象与 pthread，不借整个
trcore、Message、Zstandard 或 RPC 补齐符号。覆盖三个 calloc 失败点、mutex/cond
初始化失败、三个线程启动失败位置、部分 join 失败重试、cond/mutex 销毁重试；
满载连续拒绝且输出不变；排队/运行取消、排空升级取消；join 不依赖完成被领取；
回调退出前不能复用容量；回调释放自身参数；错误线程和重入拒绝；过期实例/代次；
实际使用的代次原语在 UINT64_MAX 边界不回绕。另用四个提交者、三个工作线程、
七个槽位处理 512 个任务，逐次核对计数守恒和最终完成一次。

`test_task_message` 只链接执行器及三个 Message 生产对象和静态 Zstandard。
测试正常压缩交付、运行前取消、算法已成功但执行器终局前取消；执行器拒绝时由
提交方回滚已经 prepare 的变换；输出引用可在执行器销毁后继续持有并最终释放。
输入、变换及执行器在同一预算域，所有者回调领取结果后核对完整预算收敛。

组合测试适配只在 run 前将令牌传给 MSG-B；运行中取消通过执行器终局阻止成功
交付，不提前中断算法。未来生产适配仍需将取消令牌安全传入后端检查点，并建立
Reactor 唤醒/完成事件源、连接/Call 代次以及回调与所有者停止的组合屏障。
不能把测试适配当成已经交付的网络压缩服务。

## 实际局部证据与限制

工作容器无法解析 github.com 且无 Xmake。本轮通过已读取的仓库内容建立局部
依赖，核对 Git blob：memory_budget.h 为 `500b95a044db5dbfdc95e1219f4808d6d4084c05`，
status.h 为 `8a9c2219e52df261164364a35ec7577db69b413e`，Xmake 修改前为
`71ba05d6cb8398840b2dddc8f8631dfdbc8ba99a`。上传的新核心/头/测试与本地字节核对。

对新执行器核心及 test_task_executor 的最终源码：GCC 14.2/Clang 17、O0/O2、
NDEBUG 四组编译并运行成功；GCC O2 20 轮通过；Clang ASan/UBSan 含泄漏检查通过；
GCC TSan 非 PIE 构建运行通过；GCC fanalyzer 与 Clang analyzer 无诊断；
两个新头用两套编译器分别自包含编译共四组通过。

本地未执行 Message 组合程序或 Xmake 全库构建，不能继承 #253 的结果冒称完成。
本地使用 clangd 17 格式器；固定 clang-format 18.1.8 和完整 CI 以新提交的远端
实际结果为准。Xmake 差异只有一个生产源和两个受限链接测试目标，不改变旧目标、
生产编译数据库覆盖规则或告警基线。远端验证链接在 PR 中随实际结果补充。

## 下一有限阶段

本切片形成显式拉取完成的执行内核，不提供自动 eventfd 唤醒、期限调度、优先级、
租户公平性、每 Call 顺序、重启、fork 或 TLS。生产 Message/现有 RPC 执行器迁移
仍需独立适配，不应在每个功能里再复制一套线程池。新的 task_executor 暂无生产
门面调用方，不能长期保持两套执行器同时演进；下一阶段需迁移具体入口并验收
停止、代次和消息资源的组合责任。PR 保持草稿，不关闭 #235/#241/#243/#248。
