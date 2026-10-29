# RPC 信用、帧派发与工作进度追踪

日期：2026-10-30；关联 #247、#243、#241；延续[CONTROL TX 与准入诊断](rpc_v2_control_tx_admission_20261030.md)。

15 个新增固定 40k 场景仍全部不满足原 clean 判定。时间线把等待分到初始信用、
发送至服务端完整帧派发、业务开始及终局，未确定单一根因。每轮 64 帧的临时候选
消除了三场控制池耗尽，但实际准入没有改善，已撤回。生产代码、版本和原验收条件保持不变。

## 输入与身份

基线 `3ca47d15c9ac571882d2b61a522e73fb315bfb72`，生产代码仍等同
`0d81342e85739bcd8ff0253bdbcd894c602696c0` / dev.182。五组独立临时 ELF 各运行
原明文三 profile 一次：40k 计划速率、250ms、10000 次到达、16 个发生器、每个
64 槽/64 窗口、3000ms 期限、服务端延时零，正文仍为 32 字节请求与响应。
`--capacity` 为原 32/128/256，bulk 参数仍为 16384/65536/65536。
本版 configure_limits 实际将 max_calls、max_streams 都设为 capacity 的两倍，即
64/256/512。前两份诊断表格的 call limit 栏实际列的是基准 capacity 参数；此处
明确更正其含义，原始命令、字段和观测值不重写，下面也统一标为基准 capacity。

五组分别为进度 CPU/任务阶段计数、业务时间关联、客户端信用关联、双端帧关联、
同帧关联加每轮 64 帧候选。每组均从原文件独立生成、冻结再恢复，没有叠加前次
候选。源码补丁和五个 ELF 摘要见[探针身份](experiments/rpc_progress_trace_20261030/probes.json)。
共享开发机、亲和性 0–15、IPv4 loopback；没有硬件隔离、严格配对或统计性能结论。

150000 次计划中实际提交/接受/完成均为 29542，29460 次 OK、82 次
RESOURCE_EXHAUSTED、120458 次发生器丢弃。submit_again、submit_errors、deadline、
无效响应均为零。全部不干净样本保留，没有降低负载或调整原 clean 判定。
这些诊断包含插桩成本，未覆盖 TLS、mTLS、压缩或流式基数的同类追踪。

## 工作阶段与端到端关联

进度 CPU 探针在 worker/owner 原交接边界保存提交、开始、返回、done 时间；
队列/工作/交付只在真实完成后统计。直方图上界从 1us 按二倍增长，以下是合并
同场 16 个 peer 后的 P99 上界，不是精确分位数：

| 基准 capacity | 任务提交至 worker 开始（us） | worker 执行（us） | worker 返回至 owner done（us） |
|---:|---:|---:|---:|
| 32 | 64 | 16 | 2048 |
| 128 | 128 | 16 | 8192 |
| 256 | 128 | 16 | 4096 |

该组单次 Endpoint pump 的最大墙钟时间为 0.586–0.773ms；CPU 累计为包含内部调用的
边界计数，不能与 Server 生命周期 CPU 相除得到独占贡献。它们没有把百毫秒等待
定位为长时间执行的 echo 业务。

后续探针把原 32 字节 RAW 正文前八字节改作 PID/计划序号，原回包逐字节校验继续
执行。Server 在业务 MESSAGE 入口/返回保存标识与时间，Client 保存 API 入口、
消息回调和终局；只在测量与清理之后输出。成功调用完整关联，失败/缺少业务
MESSAGE 的调用单独计数，不补造时间点。

仅业务关联组的成功调用中，API 入口至 Server MESSAGE 工作开始占累计终局等待
约 53%–85%。进一步的客户端关联组观察到：初始信用等待 P50 约 18–83ms，
MESSAGE 被本地接受至工作开始约 32–45ms；信用到达后的 RPC OPEN/MESSAGE 准备
处于微秒级。这些是不同单次运行，不能相减拼出同一次调用的额外阶段。

## 从发送调用到完整帧派发

帧探针以真实 TCP 端口对和 wire stream_id 关联两端。TX 保存完成当前帧的那次
发送调用开始/返回时间；RX 在完整帧派发前保存时间。32 字节场景的相关消息均为
单帧。以下分位数合并同场所有已关联 OK 调用，使用 nearest-rank，单位毫秒：

| 基准 capacity | OPEN 发送开始至 Server RX | Server OPEN RX 至信用发送 | 请求发送开始至 Server RX | Server 请求 RX 至工作开始 | API 至终局 |
|---:|---:|---:|---:|---:|---:|
| 32 | 67.186 | 8.527 | 20.811 | 5.448 | 186.252 |
| 128 | 30.590 | 5.287 | 18.074 | 13.577 | 138.044 |
| 256 | 10.422 | 3.744 | 11.334 | 26.339 | 137.098 |

这些是各阶段 P50，不能把分位数相加当作总分位数。信用发送开始至 Client 完整
RX 的 P50 为 0.116–0.155ms。两端 getsockopt 实测 TCP_NODELAY=1；没有修改 TCP
选项。发送至对端派发的区间同时包含发送调用、内核与接收/解析/owner 排队，
不能称为独立网络时延。

19 次观测中，对端已派发完整帧时发送方尚未记录 syscall 返回；因此关联采用
发送开始时间，并保留返回时间作区间边界，没有将这些正常并发记成负时延。
完整分段逐调用相加等于 API 至终局时间。

对原帧记录再按成功调用拆分发送时间：从完成当前帧的发送调用开始，到 TX 记录
时刻的 P50 仅 12–17us；TX 记录到服务端完整帧派发的 P50 仍为 OPEN 10.403–67.181ms、
请求 11.317–20.791ms。反向的服务端信用帧发送结束至客户端完整 RX P50 为
0.103–0.143ms。分析器精确关联原帧探针的全部成功调用（每档 1866、2110、1946 条），
按 nearest-rank 计算；同一主机单调时钟下保留有符号间隔，19 个接收事件先于发送方
TX 记录的并发样本仍单独计数。

这把较长等待定位到客户端发送完成之后、服务端完整帧派发之前，排除了本地发送调用
自身耗时是主要部分；该区间仍混合内核 socket 处理、服务端 Reactor 唤醒与接收/解析，
不能单凭这些数据确认是调度器、内核还是具体 Reactor 路径。原记录也不能将 TX 记录时刻
等同于 syscall 返回时刻。

后续 v3 插桩增加服务端 epoll_wait 返回、批次索引/大小、连接可读处理、RX 调度入队及
owner 开始读取。仅看 epoll 返回不早于客户端 TX begin 的成功帧，TX begin 至 poll 返回
P50 为 9.91–47.98ms；poll 返回至连接可读处理 P50 仅 14–18us；返回批次 P50 为 13–16
个事件，目标事件索引 P50 为 4–8。RX schedule 至 owner 开始的 P50 为 2.97–11.73ms，
owner 开始至 recv P50 为 0.52–1.39ms。这说明 epoll 返回后的事件处理本身很短，明显等待
分布在 poll 返回之前和 RX ready 入队之后；但 poll 返回可能由同批其他 fd 触发，且该子集
条件不能证明目标 socket 字节在 poll 返回时已完整可读，因此仍不能将前段归为内核等待。
连接级调度锚点也会早于某帧 TX，分析器单独审计了这类样本。

v4 在同样负载中增加每连接最近一次已完成的 `rx_held` 与 `rx_paused` 区间。三个 profile
均再次出现 `control_tx_pool_exhausted`，并且所有样本仍是 dirty。按最近一次已完成 hold
与 `[客户端 TX begin, 服务端 recv begin]` 求交，OPEN 有重叠的样本占 56.7%–79.9%，但
该 hold 覆盖等待长度的 P50 仅 4.2%–8.8%；REQUEST 重叠占 18.3%–40.8%，覆盖比例 P50
均为 0。pool pause 没有观测到重叠。由此可确认 RX hold 是一部分样本中的真实等待因素，
却不足以解释总体长尾；探针只保存最近一个已完成区间，不能代表该连接等待窗口中的全部
hold。当前证据仍不足以据此改写调度、控制池容量或 RX hold 行为。

v5 在当前 release-readiness HEAD `006af2aec502d59a4fbcdf7c0f9e212795548618` 上重跑
同一明文三档，只把 hold 观测扩成有界的完整区间历史；成功解除 `rx_held` 后，在 owner
线程用 `ioctl(FIONREAD)` 非消费地采样 socket 排队字节，并记下解析器状态。关闭时仍未
结束的区间标作不完整，溢出只计数。本次没有溢出、未完成关闭区间或 ioctl 失败。
探针仅保存 131072 条/反应器；插桩增加了内存、时钟读取和 ioctl 开销，结果仅用于归因，
不用于性能比较。

将服务端非预热业务回调正文 ID 映射到客户端 `stream_map` 后，每个成功业务 MESSAGE
都与同一 TCP 连接上的客户端 DATA/TX 和服务端 DATA/RX 一一对应；请求 MESSAGE 使用
每 unary stream 的 `message_id=2`，OPEN/HALF_CLOSE 分别为 1/3。下表窗口为该 MESSAGE
发送调用开始至服务端对应 recv 调用开始，统计仅含成功业务回调：

| profile | 成功并关联 | 完整 hold / 解除时有待收字节 | 请求窗口 hold 重叠 | 窗口内 send 已返回且解除时有待收字节 | 全体窗口覆盖比例 P50 / P90 |
|---|---:|---:|---:|---:|---:|
| small_call_limit | 1889 | 280 / 279（99.6%） | 832（44.0%） | 832（44.0%） | 0% / 47.7% |
| default_call_limit | 1962 | 207 / 205（99.0%） | 683（34.8%） | 681（34.7%） | 0% / 46.8% |
| large_call_limit | 1767 | 70 / 70（100%） | 391（22.1%） | 390（22.1%） | 0% / 26.8% |

每个有 hold 重叠的业务请求窗口都包含一次正 `FIONREAD` 解除采样；这些正值样本中，
parser 都处于干净 HEADER 边界（header_have/payload_have 均为零）。排队字节 P50 为
4,032–6,288 bytes，P90 为 9,216–13,208 bytes。单个解除区间可覆盖多个连续请求窗口，
共有 268/185/65 个不同的正值区间落在至少一个“客户端 send 已返回、服务端 recv 尚未
开始”的窗口内。

这提供了比 v4 更直接的证据：部分慢请求的同一连接在 RX hold 解除时确有尚未进入解析器
的 socket 数据，且业务请求的发送调用已经返回。它仍是连接级队列快照，不能标记排队字节
属于哪个 stream；单次共享主机运行也不能证明所有请求长尾都由 hold 引起。按所有成功请求
算，覆盖比例 P50 仍为零，只有 22.1%–44.0% 的请求窗口重叠。因此 RX hold 是可验证的
子集因素，不是总体长尾的充分解释；控制 TX 压力、整体 40k 清洁度和池/调度策略仍未
解决。三档继续出现发生器丢弃/迟到和 `control_tx_pool_exhausted`，本次全部 dirty。

v3/v4 原始输入、两版补丁与执行身份见[原始归档](data/rpc_ingress_poll_hold_probe_20261030_raw.zip)；
[epoll 阶段复算](experiments/rpc_progress_trace_20261030/analyze_rpc_rx_poll.py) /
[结果](data/rpc_ingress_poll_probe_20261030_summary.json)和
[hold 重叠复算](experiments/rpc_progress_trace_20261030/analyze_rpc_rx_hold.py) /
[结果](data/rpc_ingress_hold_probe_20261030_summary.json)均从归档逐 stream 关联成功调用。
[v5 完整 hold 区间复算](experiments/rpc_progress_trace_20261030/analyze_rpc_rx_hold_intervals.py) /
[结果](data/rpc_ingress_rx_hold_v5_20261030_summary.json)及
[109 项原始归档](data/rpc_ingress_rx_hold_v5_20261030_raw.zip)保存完整区间、FIONREAD、原始
stderr/stdout、补丁与逐成员 INDEX。补丁在隔离树可逐字节复放；源文件恢复后 Xmake GCC
Release 重建成功，插桩源无差异。v5 是新的单轮 dirty 诊断，不与 v4 分位数拼接；当前没有
证据支持更改帧预算或池容量。

读取得到的当前 cgroup 祖先均无有限 cpu.max；可读控制器祖先的 nr_throttled=0。
scope 自身未提供 cpu.max。快照保留在原始归档，不能由一次快照证明主机始终无
其他干扰，也没有修改 cgroup、调度策略或系统参数。

## v6：pending RX hold 的原因与解除来源

在同一提交、明文 40k/250ms/10000 计划到达/16 发生器三 profile 上，v6 只为 pending
OPEN/REFUSED 背压区间增加原因与解除触发记录；没有调大池、改变重试或改变解析行为。
Xmake GCC Release 编译成功。运行仍全部 dirty，且本次没有 FIONREAD 采样；结果只用于
解释该轮完整 hold 区间，不能与 v5 的 FIONREAD 数值拼接。

三个 profile 共记录 684 个完整区间：683 个为初始 WINDOW_UPDATE 暂不能入队的 pending
OPEN，最终均由 `CONTROL_TX_READY` 路径解除；另有 1 个 STREAM_REFUSED pending，经维护
定时器解除。所有重试最终都被 Reactor 接受，探针无溢出、重叠、未发布记录或连接关闭
终止。OPEN 区间的 P50/P90/P99/最大值如下：

| profile | OPEN 区间 | P50/P90/P99/最大值 (ms) | progress_calls / send_attempts |
|---|---:|---:|---|
| small_call_limit | 294 | 5.884 / 11.781 / 18.156 / 18.289 | 219 条为 1/2；75 条为 2/3 |
| default_call_limit | 289 | 5.109 / 7.289 / 9.322 / 10.682 | 243 条为 1/2；46 条为 2/3 |
| large_call_limit | 100 | 2.872 / 5.113 / 5.775 / 5.821 | 90 条为 1/2；10 条为 2/3 |

`send_attempts` 含最初失败的发送；例如 2/3 表示初次失败后，经历两次 pending 进度，
第三次发送才被接受。三个 profile 的控制 TX 池容量及峰值均为 128，耗尽计数分别为
6770/1532/704；命令队列满计数均为 0。源码路径与观测一致：非 DATA `tr_reactor_send`
从 control TX pool 取得发送项，池空即返回 `TR_AGAIN`；Channel 因此保留 OPEN、暂停 RX，
并登记控制 TX 可用通知。REFUSED 使用每连接保留的终止发送项，不等待该普通池，所以
只有那一个 REFUSED 区间走 25ms 维护重试。此前“多数 hold 由拒绝路径定时重试造成”的
假设被这组数据否定；主要观察到的是 OPEN 的控制池背压及其就绪恢复路径。

同一 CONTROL_TX_READY 通知可能重复进度后才发送成功；在 10%–26% 的 OPEN 区间中，
记录到两次 pending 进度。由于就绪通知先检查空位、发送项并未预留给该 OPEN，这与空位
在重试前被其他发送者取得相符；本探针没有记录竞争者身份，故这仍是对调度交错的推断。
当前证据把 RX hold 的主要来源定位到控制发送池耗尽，不能判断 128 是否是产品合适容量，
也不能证明 pool exhaustion 是整体 40k 长尾的唯一原因。三个 profile 仍有控制池耗尽和
发生器迟到/丢弃，全部不符合 clean 判定；不据此更改默认容量或宣称性能通过。

[复算器](experiments/rpc_progress_trace_20261030/analyze_rpc_pending_cause.py)、
[684 条记录与完整复现归档](data/rpc_ingress_pending_cause_20261030_raw.zip)和
[逐 profile 汇总](data/rpc_ingress_pending_cause_20261030_summary.json)保留该轮结果。运行后
仅恢复本探针修改的三个受跟踪源码；在隔离 worktree 用 Xmake GCC Release 重建基线 bench
成功，摘要为 `57f1e6ca012320bb9cdc8ea978652c203dffdf186fe58bf45c7f47b9152466ec`。
受跟踪源码回到基线；原有未跟踪 `bench/__pycache__/` 保持原样。诊断 ELF 摘要
`7cde692eb5fca3561e3a1b0abeeaa6e319309128b11182ddd0301a02332b33c7` 记在执行元数据中，未归档 ELF。

## 每轮帧配额候选未采用

候选给既有 Reactor turn 增加内部 64 个完整 RX 帧额度，前后两个 RX 调度点共享，
用尽后仍由原 ready 队列续跑。原字节/dispatch 配额、池、线程和调用窗口均未改。
这只用于验证批次假设，未进入产品：

| 基准 capacity | 原帧探针：接受 / CONTROL 耗尽 | 64 帧候选：接受 / CONTROL 耗尽 | 原 / 候选 API 至终局 P50（ms） |
|---:|---:|---:|---:|
| 32 | 1960 / 5778 | 1465 / 0 | 186.252 / 310.141 |
| 128 | 2110 / 1613 | 1703 / 0 | 138.044 / 230.289 |
| 256 | 1866 / 460 | 1799 / 0 | 137.098 / 207.142 |

候选三场仍都有发生器丢弃/迟到，实际准入和观察到的终局时间不足以支持采用。
单次共享主机运行不证明退化的独立因果关系；同样，消除一个池计数不证明容量或
延迟改善。两个版本都保持原 dirty 判定，候选已按原字节撤销。

## 无插桩 ELF 的指令路径

另外用 Valgrind 3.25.1 / Callgrind 对原 Xmake ELF 运行单 Client、64 窗口、
256 次测量加 32 次预热的明文 small 场景，capacity=128（实际 max_calls/max_streams=256）。只在 tr_reactor_thread_main 收集指令，
线程分别输出；两端退出零，Server 288 项开始/结束并排空。它与 40k/16 发生器的
负载不同，仅定位用户态指令路径，不代表墙钟、CPU 周期或容量。

owner 共 18796047 个 Ir。Dispatch pump 自身 17.49%，Endpoint pump 自身 9.77%，
延后事件处理自身 7.69%；741 次 Endpoint pump 调用了 47424 次单步 Dispatch pump
和 96330 次延后事件检查，检查次数不等于实际事件数。内核与工作线程指令不计入
该收集域。这重新定位了推进成本，没有证明原[活跃环候选](../superpowers/plans/2026-10-29-rpc-active-dispatch-progress.md)
应恢复；该候选已因先前实际对照结果被拒绝。

首次驱动在 Client 完成后，以位置参数误用 validate_phase 的关键字参数，未执行
Server 正常排空核验，finally 使用 stop_owned 回收。原脚本、部分 profile 和失败
说明均保留；修正为 clean=True 后在新目录重跑，上述 288 项结论只使用有效重跑。
首次未记录 Server 退出码，不推断其正常排空。

现有 tr_endpoint_data 明确等待后续 pump，避免在 Channel 尚未归还借用 DATA 信用时
关闭流；不能为了立即推进而破坏这个边界。后续重点是核对工作提交、完成通知、
安全 owner 推进与单轮成本的关系，并以反例验证变更，而非只据热点替换容器。

## 证据与恢复

[15 场账本](data/rpc_progress_trace_20261030_cases.csv)、
[分层汇总](data/rpc_progress_trace_20261030_summary.json)、
[596 份原始成员及 INDEX.json](data/rpc_progress_trace_20261030_raw.zip)、
[补丁与复现说明](experiments/rpc_progress_trace_20261030/README.md)保留全部结果。
归档逐项 SHA-256、字节数与 ZIP CRC 已核对；分析器索引化后对原帧汇总逐字节
复算一致，原分析器也保留。五份补丁独立重放后摘要与实际插桩源码一致；从 ZIP
冻结副本运行四个分析器、复算三场任务直方图也一致，见[验证账本](data/rpc_progress_trace_20261030_validation.json)。未收录 ELF 或凭证内容。

所有临时源码均已恢复，隔离工作树的受跟踪源文件无差异。恢复后在该工作树用
Xmake GCC Release 重建得到 SHA-256 `57f1e6ca012320bb9cdc8ea978652c203dffdf186fe58bf45c7f47b9152466ec`；
它与此前冻结的候选 ELF `55d1c4b990f1878f7999017960c2c6cf5443fdf998be9ece389c5fa946bd137e`
不同。既有源码对照审计记录该冻结候选与当前 HEAD 间唯一差异为
`src/transport/channel/channel.h` 的生命周期注释。随后将已留存并校验过的冻结候选 ELF
恢复到隔离工作树，当前其 SHA-256 为 `55d1c4b990f1878f7999017960c2c6cf5443fdf998be9ece389c5fa946bd137e`；
不把本次重建产物声称为逐字节相同。生产 src/include/bench/xmake.lua 无差异，版本保持
dev.182。没有为未交付候选重复运行全量 95 项 C 测试，也没有以 Callgrind 完成冒充容量成功。

基线 `3ca47d1` 已精确核对 13/13 关联检查成功：
[CI](https://github.com/MyBlackHole/c-trpc/actions/runs/40910160626)、
[RPC smoke](https://github.com/MyBlackHole/c-trpc/actions/runs/40910160548)。本归档 head
单独跟踪。40k、固定主机/平台、分域/全局预算、服务目标/Attempt、CTM1 生成工具/
桩、后端等价比较及完整发布验收继续未完成。
