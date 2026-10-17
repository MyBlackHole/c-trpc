# V2 记录接入真实 Client 与两端门面互通

## 本阶段边界

本阶段接续 [PR #270](https://github.com/MyBlackHole/c-trpc/pull/270) 的
`8bfed3a69f0aa8e84316ce00c05b917ce97cf973`，把 V2 调用宿主接入既有
`tr_client`。服务端使用既有 `tr_server_create_record()`；客户端不再依赖测试
自行拼装 Reactor、Channel 和记录绑定来证明互通。

显式入口在 [`src/client_record_internal.h`](../../src/client_record_internal.h)。
它不是安装 SDK，不自动切换 `tr_client_create()`、旧公开 Call 句柄或 V1
默认路径。模块约束沿用
[#239](https://github.com/MyBlackHole/c-trpc/issues/239)、
[#240](https://github.com/MyBlackHole/c-trpc/issues/240)、
[#241](https://github.com/MyBlackHole/c-trpc/issues/241)、
[#242](https://github.com/MyBlackHole/c-trpc/issues/242)、
[#243](https://github.com/MyBlackHole/c-trpc/issues/243) 与
[#248](https://github.com/MyBlackHole/c-trpc/issues/248)。本阶段没有把这些迁移任务
整体标记为完成。

## 实际接口与责任

| 操作 | 行为与所有权 |
|---|---|
| `tr_client_create_record()` | 接收普通 Client 配置、内部调优、记录配置和方法表。创建时复制并冻结描述；先建立 Runtime 的硬预算，再创建预算内的重组池和方法存储。 |
| `tr_client_connect()` | TCP 与 HELLO 共用原绝对期限。一次 owner 事务内接管 fd、创建 deferred Channel、安装记录宿主及定时器，再开始 HELLO。 |
| `tr_client_record_call_start()` | `TR_OK` 才发布句柄并承担一次终局；OPEN 仍在等待 ACK、信用或发送容量时也属于已受理调用。失败不接管回调参数、不改输出。 |
| `tr_client_record_send()` | 只有 `TR_OK` 接管并清空正文句柄；失败保留正文及本次发送状态。正文不隐式排队，空 MESSAGE 仍计一条。 |
| `tr_client_record_half_close()` | 提交请求方向的 HALF_CLOSE；ONE 必须恰好已有一条 MESSAGE，MANY 可以为零。半关闭不代表调用成功。 |
| `tr_client_record_cancel()` | 选择本地 CANCELLED。请求尚未 HALF_CLOSE 时重试传输半关闭；已 HALF_CLOSE 时保留固定槽排空响应至对端关闭。遵循最先选定终局获胜，不能撤销服务端已受理的业务。 |
| `tr_client_begin_drain()/wait_drained()` | 沿用 Channel 的单调准入屏障及流静默等待，不额外宣称远端执行完成或完整 RESET 协议。 |
| `tr_client_destroy()` | 先停止并 join 父 Reactor，在真实清理线程交付残留终局，解绑宿主和 Channel，再释放依赖预算的存储。失败保留剩余责任供重试。 |

普通线程通过既有 `tr_reactor_call()` 同步提交短操作；同一 owner 直接执行，其他
Reactor owner 拒绝跨域同步等待。命令准入可能等待容量，因此这些入口不是实时、
信号安全或完全无等待接口。阻塞生命周期拒绝 owner、旧 RPC worker、通用 task
worker 以及新客户端回调域。

句柄是借用能力。宿主实例、调用槽及不回绕的 64 位代次共同决定身份；槽位复用后
旧句柄不能操作新调用。首次 destroy 尝试开始后只允许销毁重试，即使错误返回且
外层 Client 尚存，也不得继续使用调用句柄：内部宿主可能已经回收。

## 回调、终局与停止线程

`received` 借用完整记录 view 到回调返回，不能保留解析缓存指针，也不冒充旧
`tr_rpc_message` 的长期持有语义。正常回调在所属 owner 上运行，只适合短小、
非阻塞工作。回调可发送、半关闭或本地取消，不可递归受理新调用或销毁宿主。

`writable` 是重试提示，不预约信用，也不保证下一次发送一定成功。OPEN 被本地
传输接受后发出初次可写通知；共享 TX 暂满等压力由同一个已有 Reactor 定时器
继续推进。应用仍持有 `TR_AGAIN` 的正文，并自行原样重试。

终局回调恰好一次，延后到当前应用回调退出；其参数可以在终局回调中释放。
记录绑定先选终局，宿主分别管理终局交付和槽位退役，不能把 wire `opened`
当成应用受理标记。父停止时，已经受理但尚未发出 OPEN 的调用同样收到一次终局。

终局状态和实际交付线程域分别记录：已选定的取消、超时或连接错误不能被停止流程
覆盖；若其回调尚未交付就发生父停止，后续交付必须标记 `OWNER_STOPPED`。
只有尚未选终局的残留调用才在清理域选择 CANCELLED。清理线程不冒充原 owner，
也不发送停止后的网络记录。

Reactor 正常停止仍会在退出清理连接时从 owner 发出流错误；若这个事件先到，
调用合法地以 `UNAVAILABLE / ON_OWNER` 完成，后续销毁不得改写为 CANCELLED。
真实门面回归分别覆盖这个正常路径，以及显式切断 Channel handler 后留下的
已受理、尚未 OPEN 的调用；后一故障路径才严格断言 `CANCELLED / OWNER_STOPPED`，
并验证清理回调不能递归进入 Client 或 Server 的阻塞生命周期。

## 共享语义核的早期拒绝

Server 在接到 STREAM_OPEN 时可能已经没有调用容量，因而在 V2 OPEN 到达前
直接返回非 OK FINAL。与此同时，Client 的 OPEN 可能仍因 TX 背压未提交。
旧状态核只允许 OPEN 作为首事件，会把合法的资源拒绝误报成协议错误。

本次只在共同的 `tr_rpc_record_state_apply()` 中补充一条规则：尚未 opened 时，
允许响应方向的非 OK FINAL 选择终局，保持 opened、消息计数与 request_closed
原值。其他提前响应，包括 OK FINAL、INITIAL_METADATA 和 MESSAGE，仍拒绝。
字段、元数据和记录解析校验仍在原有路径执行，Client 不单独解析或旁路 FINAL。

## 有界存储与发送能力

Client 不建立旧 RPC 消息池或另一个工作线程池。Runtime 仍是单分片；记录宿主
持有固定调用槽、固定路由索引、有界 OPEN 元数据和一个进度定时器。每次进度扫描
受 `progress_limit` 限制；定时周期必须在 1 毫秒至 1 秒之间，不承诺实时抢占。

硬预算覆盖已参与 Runtime 记账的 Reactor、重组池、冻结方法表、调用槽、路由索引、
记录缓存及发送前缀。发送正文来自调用者，其原有释放责任在真实发送接受后转给
Transport；发送前缀或正文可能活得比调用绑定更久。预算必须保留到这些引用释放，
不能在宿主对象释放后立即销毁 Runtime。硬预算不等于进程 RSS、线程栈或尚未迁移
的 Client/Channel 元数据上限。

本地整条传输消息上限和初始窗口必须容纳最大控制记录，以及方法最大正文加固定头。
Client 首次收到 OPENED 时冻结对端初始窗口：实际记录大于该固定上限时明确拒绝，
不返回暗示未来必可重试的永久 AGAIN；仅当前剩余信用不足属于暂时背压。
Transport 的协商消息上限仍独立生效。这是明确的发送支持边界，没有解决任意
`L > W` 的流式活性。

仅要求整记录能放进初始窗口仍不够。Channel 在已消费但未通告的信用增量小于
更新阈值 T 时不会发送 WINDOW_UPDATE；若之后没有新的可接收记录，这些信用
也不会自动积累到 T。两端 V2 构造因此共同要求
`T <= W - min(W, max_message_bytes) + 1`。未通告的已消费量最多为 `T-1`，
剩余可见窗口仍足以容纳任一被 Transport 接受的整记录。

这里使用传输上界而非方法表上界，因为方法限额不在线协商；即使对端发来了超出
本地方法上限的记录，也必须先使其可到达验证层，才能明确拒绝。V1 的配置与流程
不受这个显式 V2 约束影响。该约束保证兼容两端的信用推进，不承诺任意外部对端
都会归还信用或遵守同样配置。

OPEN 的元数据按原有受限 TLV 规则复制，最大 512 字节，拒绝重复键和非法键。
本地 `timeout_ms` 从受理开始计算单调绝对期限；重试 OPEN 时发送实际剩余值，
到期本地终止，不能把过期值编码成代表无限期限的零。

## 明确未开放的能力

V2 没有 CANCEL/RESET 记录。`tr_stream_close()` 只是本地发送方向半关闭；服务端
在完整请求 HALF_CLOSE 后收到干净的 REMOTE_CLOSED，仍可继续已受理业务。因此
本地取消和本地超时不表示远端停止、撤销成功或执行失败，也不能通过断开整条连接
代替调用级取消。

请求已经 HALF_CLOSE 的调用若先在本地取消或超时，宿主立即交付一次终局，应用
可以释放回调参数；宿主保留固定槽和精确流路由，只释放晚到载荷并归还信用，
不再交付用户回调。收到对端 STREAM_CLOSE、断连或本地销毁后才完成回收。
这样即使服务端继续发送超过剩余窗口的多条响应，也能完成排空。远端不关闭或
业务不返回时，这个槽可能持续占用；容量耗尽会对新调用返回 AGAIN，没有无条件
限时回收承诺。未 HALF_CLOSE 的请求关闭后，服务端仍须等待已提交工作返回；
之后错误 FINAL 受既有 `final_wait_ns` 限制，不会无限等待响应信用。

真实连续调用测试还发现，终局路径若先排 STREAM_CLOSE，再由当前或迟到 DATA
消费排 WINDOW_UPDATE，对端可能已经退役该流，进而按未知流协议错误关闭整条
连接。本次增加仅供终局回收的 `tr_stream_stop_credit_on_owner()`：精确校验 owner
和活跃代次，单调停止新增信用并清除未接受的窗口更新，保留已广告额度和已排队
更新。两端宿主在安全退役的 CLOSE 前调用它，因此已接受的信用更新仍由 FIFO
排在 CLOSE 前，后续释放继续记入 consumed，但不再产生关闭后的 WINDOW_UPDATE。
普通 Stream 半关闭继续归还信用。

两端 DATA 回调的尾部也不再立即 pump 关闭，由既有进度定时器推进。正常接受合法
FINAL 可以直接进入停止信用与关闭；本地先终局后的晚 FINAL 不会旁路共享语义核
重新解析，排空只依赖精确的 Transport 关闭事件。这里没有放宽未知流 WINDOW_UPDATE
的协议检查，也没有新增 wire CANCEL/RESET 或第二套 RPC 状态机。

自动重连配置明确返回 `TR_ERR_UNSUPPORTED`。现有 Channel 重连连接器及事件源
要求父 Reactor 仍运行时销毁，尚没有可覆盖父 fatal exit 的停止域回收契约；仅在
正常 destroy 之前禁用重连无法补齐该缺口。后续必须先完成连接器与 Channel 的
停止恢复，再开放 V2 自动重连，并验证旧调用绝不透明重放。

本阶段同样不接受 interceptor、连接组配置、旧方法注册/调用和稳定 V1 RPC
语义统计；这些入口明确拒绝而不伪造输出。压缩仍只支持 NONE。稳定 V2 SDK、
长期异步生产者/消息持有、远端协作取消、服务端期限调度、TLS、网络压缩、公开默认
切换与旧 RPC 路径退役，仍须分别完成契约和对等验证后推进。

## 验证记录

实现冻结提交为 `52304cfb580a055482e9eeb9489914873618a584`，代码树为
`9c07f872e45583e893905528ef3280f783fff386`。下表均使用 Xmake 3.1.1，完整
构建全部目标后执行 `xmake test -v -j1`；每组配置、构建和测试退出码均为 0。
数据库与实际构建日志核对了 42 个生产翻译单元，测试汇总是 76 个唯一通过项。

| 模式 | 实际编译器 | 全部测试 | Xmake 测试耗时 |
|---|---|---:|---:|
| debug | GCC 13.3.0 | 76/76 | 29.640 秒 |
| release | GCC 13.3.0 | 76/76 | 27.018 秒 |
| release | Clang 18.1.3 | 76/76 | 25.960 秒 |
| ASan + UBSan | GCC 13.3.0 | 76/76 | 29.198 秒 |
| TSan | GCC 13.3.0 | 76/76 | 34.421 秒 |

ASan 启用 `detect_leaks=1:halt_on_error=1`，UBSan 启用
`halt_on_error=1:print_stacktrace=1`，TSan 启用
`halt_on_error=1:second_deadlock_stack=1`。核对了实际生产编译选项与完整运行
日志，没有内存、泄漏、未定义行为、竞争或死锁诊断。结果只证明这组真实运行，
不能代替所有并发调度与所有失败路径的证明。

`test_client_record` 的八组真实门面场景覆盖构造/回滚、冻结方法表、ONE/MANY、
零长度及跨帧正文、元数据复制、早期拒绝、槽位复用与旧句柄、发送背压、回调内
发送/半关闭/取消、时钟故障、断连、截止时间、排空和两种销毁交付域。特别保留
请求 HALF_CLOSE 后取消或超时的三条晚响应，共 12,435 字节超过 8,192 字节
初始窗口；随后在同一连接、单调用槽和单 Stream 容量下再完成两个正常调用。

共享语义核继续运行 6,240 条有界序列，Endpoint 契约测试继续覆盖 18 个场景。
Channel 回归同时验证终局停止信用和普通半关闭持续归还信用。clang-format
18.1.8 的正式变化行门禁检查 15 个改动 C/头文件，0 个失败。

原始日志在 `build/validation-client-record/`。GCC debug 位于 `gcc/`，其余
四种最终完整运行位于 `resumed-matrix/`；`matrix-summary.json` 记录源码身份、
各阶段退出码、编译器、耗时与每个原始日志的 SHA-256。

首次恢复的 GCC release 将二进制输出放在工作区，4 个程序在启动前返回
`Not access because it is busy`，该次实际为 72/76、非零退出；其失败日志
完整保留，不能计为通过。后续只将可重建输出改至独立临时目录，未修改源码、
跳过测试或调整程序权限，重新完整构建并通过 76/76。其他被中断的早期构建
同样不计为通过。开发阶段发现的关闭后信用更新与停止域断言问题及其首轮
失败日志，也保留在 `initial/` 供复核。

固定分析器首轮也实际覆盖 42 个生产翻译单元：GCC 11.4 保留 2 条旧诊断；
Clang 14.0 保留 11 条旧诊断，并在新 Client 宿主分配后报告一条
`Use of memory allocated with size zero`。完整路径审查确认现有乘加上界保护
已排除零总长；本次沿用 Message 服务的防御方式，在预算预约和分配前显式校验
聚合总长至少容纳完整控制块，保留原有所有算术上界检查。未增加分析专用宏、
关闭检查项或将该新诊断加入告警记录。

两个原始日志精确门禁只允许以下已核对的位置迁移：Channel 既有 GCC 诊断
从 581 行移到 582 行，既有 Clang 诊断从 2602 行移到 2607 行。原文件、
翻译单元、列、检查项、完整文字、数量及 `review_state: unverified` 不变。
首轮原始诊断和失败门禁在 `resumed-static/` 保留；包含防御检查的修订仍须
重跑正式分析和验证，尚未完成的检查不计为通过。
