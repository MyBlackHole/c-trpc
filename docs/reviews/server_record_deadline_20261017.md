# V2 Server 截止时间、任务取消与迟到结果回收

## 本阶段范围

本阶段接续 [V2 Client 入口集成](client_record_integration_20261017.md)，分支
基于 [PR #271](https://github.com/MyBlackHole/c-trpc/pull/271) 的
`4167abcf8ca0f32f77e99f880a3187c7416f0603`，让已接入真实 Server 的 V2
分发器执行 OPEN 携带的剩余期限。显式 V2 入口和 V1 默认入口保持各自契约；
本阶段不发布稳定 SDK，也不把整体 RPC 迁移标记为完成。

期限事实保存在现有分发槽和已提交任务内；原路由仍负责选择终局，原任务执行器
仍负责取消令牌和完成交付，原 Endpoint 定时器仍负责推进。没有新增公开接口、
配置项、线程池、任务队列、定时器或并列的 RPC 状态机。

## 期限从哪里开始

完整 OPEN 通过唯一记录绑定验证后，在所有者线程进入业务事件存储准入之前：

- `remaining_timeout_ns == 0` 表示无限。本次新增的期限逻辑不为无限调用读取时钟。
- 非零值加到当次 `CLOCK_MONOTONIC` 纳秒时间上，形成调用唯一的本机绝对期限。
  加法超出 `uint64_t` 时饱和到 `UINT64_MAX`，不回绕成较早时间或零。
- 任务排队、记录重试、后续 MESSAGE 或 HALF_CLOSE 不重新建立期限。
  调用槽退役并复用时，原有槽位初始化会清除期限。
- 当前时间不可读取、秒数为负、纳秒分量非法或转换溢出，都选择 `INTERNAL`；
  只有成功取时且 `now >= deadline` 才选择 `DEADLINE_EXCEEDED`。

服务端期限从完整有效 OPEN 的接收时刻起算。线协议传递相对预算，不能用不同
主机的单调时钟相减来扣除网络时间。Client 仍从调用受理时执行自己的本地期限，
OPEN 重试时传递实际剩余值；两端期限不等价于跨主机硬截止时间。

OPEN 尚未收齐、验证之前的等待不在本阶段期限内。不能先信任部分固定头，
再复制一套解析器来计时；慢 OPEN 需要独立的入站限制。业务收到的
`view.header.remaining_timeout_ns` 保留原始线协议值，不是工作函数开始时的
实时剩余预算，业务不得按此值重新计时。

## 检查与取消如何收敛

| 边界 | 实际行为 |
|---|---|
| 已有调用的接收入口 | 先检查期限，再进入路由及分段缓存。到期或取时失败选择终局、执行清理并返回 `TR_ERR_STATE`，不返回暗示可以重放网络字节的 `TR_AGAIN`。 |
| 完整记录验证后 | 重新检查，再预约业务事件存储，避免解析过程跨过期限后继续准入。 |
| 每个进度步骤 | 为当前调用重新取时，包括有任务已经提交或正在执行的情况。每步只提交一个任务或发送一条记录。 |
| 工作线程调用业务前 | 检查任务内提交前固定的期限。任务在共享执行器排队至过期时，不进入业务；工作线程不访问所有者槽位或路由。 |
| 已完成任务交付 | 先处理明确的任务取消或工作错误，再对普通成功结果重新检查期限。迟到成功只释放其拥有的回复。 |
| 每次回复及背压重试 | 经原进度步骤重新取时，不复用上一轮或其他调用的旧时间。尚未被 Transport 接受的成功不能越过已经观察到的期限。 |

任务在开始前检查到超时返回内部 `TR_ERR_TIMEOUT`，完成所有者将其映射为
`DEADLINE_EXCEEDED`；时钟错误返回 `TR_ERR_SYS`，映射为 `INTERNAL`。
明确的任务错误先于普通完成路径的再次取时，避免工作线程已知的取时错误被
后续成功采样掩盖。

终局仍由原路由选择。正常运行时已经选择的终局不被迟到结果改写；完成、接收和
进度路径都使用原静默屏障。到期会取消原任务令牌并释放尚未提交的排队事件或
已经交付的回复；正在执行的任务及其输入仍须保留，直到业务真实退出并完成原
`done` 交付。终局通知至多一次，不能在不合作的业务仍访问输入时提前回收。

父所有者停止后的恢复继续沿用既有 Server 清理契约：尚未通知的残留调用在真实
清理线程交付 `CANCELLED / OWNER_STOPPED`，不发送停止后的网络记录。本阶段
没有改写这项既有停止域规则。

## 资源和交付边界

分发槽和任务各增加一个 `uint64_t deadline`。两类对象原本就按实际 `sizeof`
预约硬预算，新增存储直接计入既有预算，不新增无界映射或单独的期限对象。

Endpoint 的单个进度定时器继续按 `progress_limit`、`completion_limit` 和周期
推进。到期被观察之后才会合作取消，不提供实时抢占；繁忙所有者、任务扫描以及
不合作业务都可能推迟通知。业务不返回时，已经提交的任务与调用不能硬性回收。

`final_wait_ns` 独立约束本地终局之后等待错误 FINAL 被 Transport 接受的时间，
不是业务执行期限。错误 FINAL 可能因背压、断连或停止无法发送；
`final_accepted` 只说明 Transport 接受，不是对端已收到或业务已撤销的证明。
已经被 Transport 接受的响应无法由期限撤回。

V2 仍没有调用级 CANCEL/RESET 记录。本地 Client 取消不等同于服务端撤销；
请求 HALF_CLOSE 之后的晚响应仍沿用客户端有界排空与信用回收契约。

## 回归验证方法

新增 `test_server_record_deadline` 使用真实 V2 Client 与 Server 门面、真实 TCP、
共享任务执行器和原 Endpoint 进度路径。Client 调用不设置本地 timeout；测试
只复制并替换它拥有的 OPEN 前缀中的线协议剩余期限，再由真实 Transport 接受。
超时证据来自服务端选择的终局以及 Client 收到的实际错误 FINAL，不能用 Client
自身超时冒充服务端调度成功。

精确边界使用限定在 RPC 入口的线程局部时钟作用域；Reactor、传输、等待和业务
回调自身继续使用真实时钟。排队、执行和完成等待由有界任务表、真实工作门闩和
所有者命令控制，不通过读取私有布局或增加生产测试查询接口来伪造状态。

排队任务和已完成结果场景先等 Server 实际接收 MESSAGE 与 HALF_CLOSE，再暂停
推进并跨过期限，避免迟到网络接收先触发超时而掩盖工作线程或完成路径缺失的
检查。分段 OPEN 携带真实合法元数据，在最后一个元数据字节之前暂停；固定头
已经完整仍不能提前建立期限。恰好到期的接收场景只交 MESSAGE 的七字节前缀，
同时检查 `TR_ERR_STATE`、未执行业务和持有预算不增加。

既有 `test_client_record` 的本地超时后多窗口排空场景也保留。新 Server 会执行
线上期限，原先期待三条晚回复的场景因此在等待响应计数时实际失败。临时诊断
确认只有 `deadline=true` 分支停在响应计数 0；取消分支正常收到三条回复。
修正仅在该超时场景复制并替换一次 OPEN 前缀，把线上期限设为无限，同时保持
Client 本地一秒超时。成功发送才转移所有权并停止替换；失败释放替身并保留原
前缀，最终核对替身创建和释放计数。原 12,435 字节跨 8,192 字节窗口、单槽
回收、两个后续成功调用和旧句柄断言完整保留。该场景明确保护无限期限对端的
晚响应排空责任，服务端自身期限则由新专测独立证明。

## 实际验证记录

GCC 13.3 debug 定向构建成功。含元数据 OPEN 加强版与 Client 夹具修正后，
真实 Client 八组全部通过，耗时 2.498 秒；新 Server 期限七组全部通过，耗时
1.234 秒。共享状态核的 6,240 条序列、派发器、18 个 Endpoint 契约场景、
真实 TCP Endpoint 及 Server 16 场景也通过；首次旧 Client 超时夹具失败和
精确定位日志均保留在 `build/validation-server-record-deadline/initial/`。

实现冻结检查点为 `26418d4bcad4320e9e062ab30a8006df6ae2c832`，代码树为
`33fb9db280ae1a05ad02518f7e1b23ebbe2dd375`。以下五组本地完整运行都针对该版本，
使用 Xmake 3.1.1 的真实构建图，不手写编译命令：

```sh
xmake f -c -y -m <mode> --toolchain=<compiler> --crc32c_portable=n -o <output>
xmake build --all -v
xmake test -v -j1
```

| 模式 | 实际编译器 | 全部测试 | Xmake 测试耗时 |
|---|---|---:|---:|
| debug | GCC 13.3.0 | 77/77 | 30.054 秒 |
| release | GCC 13.3.0 | 77/77 | 28.590 秒 |
| release | Clang 18.1.3 | 77/77 | 28.650 秒 |
| ASan + UBSan | GCC 13.3.0 | 77/77 | 31.073 秒 |
| TSan | GCC 13.3.0 | 77/77 | 36.526 秒 |

五组配置、全目标构建和全测的退出码均为 0。每组实际构建日志包含 42 个
生产编译命令，77 个唯一注册测试均通过；GCC debug 还重新导出并验证真实
编译数据库。ASan 启用 `detect_leaks=1:halt_on_error=1`，UBSan 启用
`halt_on_error=1:print_stacktrace=1`，TSan 启用
`halt_on_error=1:second_deadlock_stack=1`。核对了真实插桩选项，完整运行
没有编译、内存、泄漏、未定义行为、竞争或死锁诊断。

源码格式门禁固定 clang-format 18.1.8，针对 Client 修订基准检查本阶段
7 个变化 C/头文件，0 失败。其余 C/头文件没有为通过门禁而整库重排。

固定静态分析也使用两个独立的冻结检出，重新导出并核对 Xmake 生产数据库：

| 分析器 | 生产单元 | 精确保留的旧告警 | 正式结果 |
|---|---:|---:|---|
| Clang / clang-tidy 14.0.0 | 42/42 | 11 | 分析退出 0、精确告警门禁退出 0；分析耗时 84.122 秒。 |
| GCC 11.4.0 | 42/42 | 2 | 每个单元实际生成生产对象，分析与门禁退出 0；耗时 31.397 秒。 |

分发器和 Client 宿主的 Clang 诊断日志均为空。两套分析的原告警记录未再修改，
`review_state` 仍是 `unverified`；门禁通过不表示历史告警已经解决。分析器
驱动/门禁自测、Clang 自动清理对照和 GCC 真实泄漏正反例继续通过。

日志位于 `build/validation-server-record-deadline/`，完整运行分别在
`matrix/` 和 `static-analysis/`。每种模式保留命令、源码与编译器身份、
实际退出码、耗时和日志 SHA-256；`matrix/four-mode-summary.json` 汇总
GCC release、Clang release 和两类运行时检查器，GCC debug 独立保留
`matrix/gcc-debug/result.json`，静态汇总为 `static-analysis/static-summary.json`。

随后衔接 Client 最终远端提交仅带入上一阶段验证文档。首轮远端提交
`e51e0bd24289c49966ab6119071f5d364811391c` 的代码树为
`d547b940a43f939870a5fe42ba777d2e5e92b768`，逐文件核对源码与测试均和上述
冻结检查点一致，差异仅为两份验证文档。下面单独记录此后远端暴露的测试问题
及修订，不能将原检查点的测试结果直接视为修订版本的结果。

## 首轮远端失败与 join 故障注入修订

[首轮 CI](https://github.com/MyBlackHole/c-trpc/actions/runs/39008822802)
与格式、RPC smoke 共 14 个预期作业，13 个成功、1 个失败。唯一失败是
ThreadSanitizer 作业的旧 `test_server_record/default`：
`test_join_retry()` 中 `destroy_until_stable(&f) == TR_ERR_SYS` 断言不成立。
该作业全测结果为 76/77，新增的 `test_server_record_deadline` 七组已全部
通过；日志没有 ThreadSanitizer 数据竞争诊断。

根因在测试包装器：它先通过 `get_stats` 单独观察 `workers_alive`，仅在观察
为零时注入 join 失败。如果观察时工作线程仍存活，但线程在真实 join 再次取锁
之前退出，真实 join 就会合法成功，Server 随之成功销毁，测试故障开关却从未
消费。两个观察各自正确加锁，仍存在检查与使用之间的时序竞态。

修订只修改旧 Server 测试及它的 Xmake 链接配置，生产实现没有变化。执行器
join 包装器进入并恢复线程局部作用域，故障移动到这个作用域内真实调用
`pthread_join` 的边界，在实际回收线程之前一次返回 `EINVAL`。执行器原实现
将它映射为 `TR_ERR_SYS` 并保留未完成的 join 状态；Reactor 及其他线程的
join 原样透传，不消费故障。

原有系统错误、Server 对象保留、执行器尚未销毁和重试成功断言全部保留。
新增断言要求故障恰好命中一次，第一次失败时实际成功回收工作线程数为零，
重试后成功回收数等于创建的工作线程数。没有放宽断言、删除场景或通过重复
旧提交的 CI 来掩盖失败。

修订冻结检查点为 `df0ced4708f8fcd04560717743790a47c884f04c`，代码树为
`fd5103fdec55a6c77548baa135a91a77f05941ae`。固定 clang-format 18.1.8
针对本阶段基准重新检查 8 个变化 C/头文件，0 失败。

两个独立干净检出对该修订运行 Xmake 定向验证：GCC 13.3 debug 的 Server
16 场景通过，测试耗时 1.343 秒；GCC 11.4 TSan 构建以下四个测试目标，并
使用 `halt_on_error=1:second_deadlock_stack=1` 执行六次测试：

| GCC 11.4 TSan 测试 | 次数 | 每次结果 | Xmake 测试耗时 |
|---|---:|---|---|
| `test_server_record` | 3 | 16 场景通过；新增注入和真实 join 计数断言生效。 | 1.212、1.042、1.012 秒 |
| `test_server_record_deadline` | 1 | 七组服务端期限场景通过。 | 1.288 秒 |
| `test_task_executor` | 1 | 通过。 | 0.050 秒 |
| `test_server_destroy_retry` | 1 | 通过。 | 0.015 秒 |

两套配置、构建和所有测试退出码均为 0。真实链接命令包含新增的
`--wrap=pthread_join`；GCC 11 版本同时确认 `-fsanitize=thread` 和对应
`libtsan.so.0`，六次运行均无 TSan 诊断。编译器与运行库来自同版本的
Ubuntu 11.4.0-1ubuntu1~22.04.3 软件包，恢复时核对官方签名索引和软件包
SHA-256。本地宿主仍是 glibc 2.39、binutils 2.42，不能将其称为完整复刻
ubuntu-22.04 CI 环境；远端仍按 PR 的实际提交重新构建和运行全量矩阵。

修订的格式和 debug 日志位于 `join-injection-revision/`，GCC 11 TSan 的
源码、编译器、运行库、实际命令、退出码及原始日志 SHA-256 汇总在
`gcc11-tsan-join-fix-df0ced47/result.json`。两者均位于上述阶段验证目录。
该修订之后补充的文件仅为本评审记录，测试及生产源码保持冻结。

首轮全部 14 个作业的完整日志、归档和源码身份保留在
`build/validation-server-record-deadline/remote-initial-failure/`；汇总文件
`remote-initial-ci.json` 明确保留失败结论。该次 CI/RPC 实际检出合并提交
`4ad8ecb3c62944d50a9441ceae65a6610fa73501`，其代码树与首轮 head 相同，
格式检查直接检出首轮 head。验证证据不扩大为实时性、跨平台或所有并发调度
均已覆盖的保证。
