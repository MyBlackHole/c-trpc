# V2 开发剩余项

核对基线：2026-10-25，主线 TLS-C merge commit `0fa90817c15286485ee2e1cfe8372bb8ab8a8e5a`；TLS-D1 当前开发分支为 `feat/v2-tls-d-234`（2.0.0-dev.17）。
GitHub 总账：[架构总审查 #239](https://github.com/MyBlackHole/c-trpc/issues/239)。这里记录开发顺序与证据入口，不以单个 PR 或旧 Issue 勾选框代替整体验收。

## 已落地主线

V1 核心退役（#290）、统一 RPC Record 2、四种基数、普通 Client/Server、元数据、取消/截止、异步 producer、RESET 与单连接排空边界。#301 修复初始信用等待控制池容量时的唤醒；#302 在流槽耗尽时独立拒绝新流，详见 [PERF-V2-03](rpc_v2_stream_refusal_20261023.md)。完整可用边界见 [R17](rpc_v2_release_boundary_20261023.md)。

本次 dev.5 的 [LIVE-V2-01](rpc_v2_message_window_20261023.md) 已实现并本地验证
共享 Channel 的双向 L/W 准入、残余信用恢复和普通服务端响应收敛。
候选与实际主线验收以文中链接的 GitHub 登记为准。

dev.6 的 [Channel 预算修复](rpc_v2_channel_budget_20261023.md) 将普通 V2
Client/Server 的 Channel 固定对象、流槽和索引接入所属 shard 的共享内存预算。
这只覆盖 #241 的一个固定存储缺口；分域/全局账本与组合矩阵仍在下表。

dev.7 的 [服务端共享预算](rpc_v2_shared_memory_budget_20261023.md) 将普通 V2
Server 的 shard 本地账目连接到单个 Server 总额，避免旧等分额度过早拒绝。
租户、跨 Server、压缩等预算域和完整资源矩阵仍未完成。

dev.8 的 [DATA 标志隔离](rpc_v2_data_flag_isolation_20261023.md) 将可定位到
已打开流的非法 DATA 类型标志收敛为单流 RESET；#242 的连接组边界、
排空竞争及完整错误矩阵仍未完成。

dev.27 的 [DATA TX 容量恢复通知](rpc_v2_data_tx_ready_20261024.md) 为压缩 DATA 分帧
补上 Reactor 普通发送项池恢复事件，并由 Channel 在 owner 上恢复重试；Release 90/90、
聚焦 ASan/UBSan 与 TSan 已通过。该切片只覆盖 #247/#241 的一个活性缺口，确定性状态模型、
故障重放、分层诊断、分域预算和压缩慢消费者矩阵仍待实现，不代表横切验证或资源验收完成。

dev.28 的 [TEST-A 状态模型与轨迹回放](rpc_v2_state_model_replay_20261024.md)
增加独立 Python 标准库参考模型、严格 JSON schema v1、手写边界轨迹和确定性 CLI，
并接入现有 GCC 功能测试作业。Draft PR #317 的 14/14 GitHub 检查已通过，head 为
`d51984ffb0435bebd75984502f3fa2af902db995`；PR 仍未合并。它只建立 L1 oracle；TEST-B 真实组件确定性执行、
TEST-C 故障注入/缩减、TEST-D 集成与平台矩阵、TEST-E 诊断和容量性能仍未完成，
#247 与整体 V2 验收继续保持未完成。

dev.29 的 TEST-B1 在同一 TEST-A 轨迹上驱动真实 RPC Record Endpoint 测试夹具，检查取消后
迟到工作、重复终局回调和账本归零；Reactor、Channel、Dispatch 与 Executor 仍为受控替身，
未覆盖 TCP/真实派发调度。当前本地 GCC Debug/Release 均为 90/90，组件夹具为 35/35；
Draft PR #318 的实现提交 `f3fc0abb2766fc24bb6c737cd9f03d0702f4343c` 已通过 14/14 GitHub
检查，仍未合并；完整边界与本地/远端证据见
[TEST-B1 记录](rpc_v2_component_replay_20261024.md)。

dev.30 的 TEST-B2 在 TEST-B1 上增加 Deadline 终局后迟到工作的组件轨迹，验证 TIMEOUT RESET、
唯一终局和预算归还。它仍是受控下层交错，不代表真实 Dispatch 调度或 TCP 集成。本地 GCC
Debug/Release 全量各 90/90，模型 28/28、组件映射 9/9、Endpoint 夹具 35/35；Draft PR #319
实现 head `683c6eff5330b2f74bb4aa7c2fa631e075b1c39c` 的 14/14 GitHub 检查通过，PR 仍未合并；
证据见[TEST-B2 记录](rpc_v2_deadline_component_replay_20261024.md)。

dev.31 的 TEST-B3 增加同一 Endpoint 槽复用后旧代次 DATA 迟到轨迹。真实 Endpoint 必须
忽略旧流句柄的数据回调，不能增加新调用接收次数；新旧两次调用各自只报告一次取消终局并
退役，账本为零。当前模型 28/28、组件映射 10/10，GCC Debug/Release 全量各 90/90，
Endpoint 夹具 35/35，三条 Release L1/L2 回放均匹配；Draft PR #320 head
`b3bd7b8318cb5490a532a4a29c4a3e86865b0ea3` 的 14/14 GitHub 检查通过，PR 仍未合并。证据见
[TEST-B3 记录](rpc_v2_stale_generation_component_replay_20261024.md)。

dev.32 的 TEST-B4 增加成功 FINAL 已接受后收到 RESET 的组件轨迹。它检查 Endpoint 保留
先到的 OK 终局、不增加重复回调，并在 RESET 后退役与归还预算。当前模型 28/28、组件映射
11/11，GCC Debug/Release 全量各 90/90、Endpoint 夹具各 35/35，四条 B1–B4 回放在两种
模式均匹配；Draft PR #321 实现提交 `93502c1` 已通过 14/14 GitHub 检查，PR 待合并。证据见
[TEST-B4 记录](rpc_v2_final_reset_component_replay_20261024.md)。

dev.33 的 TEST-C1 增加固定 seed 模型探索、失败签名与确定性事件缩减，并扩展既有
`build-test (gcc)` 检查和失败工件归档。固定 64-seed 测试集覆盖 schema v1 的 17 种操作，
生成轨迹包含 APPLIED、STALE、REJECTED、RESOURCE_LIMIT；测试专用重复终局回调 mutant
能缩减到 OPEN/CANCEL。它只验证 L1 模型工具链，不是生产 C 故障注入。Draft PR #322 的 GCC、
Clang、ASan/UBSan、TSan、RPC smoke 与静态分析通过；Make/install job 在 SDK V2 消费端
`tr_client_destroy(client->handle) == TR_OK` 断言失败，详见 CI 首轮记录，原因待单独核实。
详细证据见 [TEST-C1 记录](rpc_v2_test_c1_explorer_20261025.md)。

dev.34 的 TEST-C2 增加一条固定 Endpoint 最终错误状态发送背压回放：模型 `ERROR` 映射到
夹具 `UNIMPLEMENTED`，真实 Endpoint 的 `tr_stream_sendv` 首次返回一次 `TR_AGAIN`、第二次
接受唯一最终帧；组件观察与 L1 在终局次数、工作完成、最终帧、槽退役及零预算上匹配。
该测试仍使用受控 Channel/Dispatch/Executor，不覆盖 TCP、通用故障日程或 TEST-C 完整要求；
Draft PR #323 的旧 head 远端检查结果单独记录；父 PR #322 的首次 Make/install 失败及后续本地复现状态
也单独保留。详见 [TEST-C2 记录](rpc_v2_test_c2_endpoint_send_backpressure_20261025.md)。

dev.35 的 TEST-C3 在同一受控 Endpoint 夹具中覆盖相邻竞争窗口：错误最终帧首次发送返回
`TR_AGAIN` 后、Endpoint 下一次发送重试前收到远端 RESET。组件必须不再尝试发送、不报告已接受
最终帧，并在有界轮转后只交付一次终局、退役槽且归还全部预算。新增/修改源码行格式 CI 门禁按
用户要求移除；格式工具和翻译目录回归并入既有 GCC 构建作业，本地改动行检查保留。
此前 PR #323 head `db4070c381c87dac0af0a5440ff9d9627bf36a40` 的格式作业因 GitHub 10 分钟超时
且零步骤执行而取消；同一 head 的本地格式检查为 1 个源码文件、0 处格式错误。该远端结果不代表
源码格式检查失败。C3 head `a858dd21fb5c3225cb4bf8b53aa5edc9ba0953d6` 的既有 CI 与 RPC smoke 共 13/13
通过；GCC 全量 Debug/Release 各 90/90，两个收尾进程快照均未发现项目子进程，旧 runner cleanup 超时未复现。
详见
[TEST-C3 记录](rpc_v2_test_c3_endpoint_reset_before_retry_20261025.md)。

dev.36 修复压缩 DATA 接收路径漏检 DATA 专属标志的问题，并补充真实 Client/Server
用例验证 GOAWAY 期间已接纳 CALL_OPEN 的 `UNAVAILABLE` 不代表“肯定未执行”、客户端不
自动重放。活动流收到额外 `LANE_BULK` 标志时只 RESET 目标流；RESET 丢弃态收到同类坏帧
时关闭对应物理连接，不影响 split 模式另一物理连接上的流。#242 其余错误分类、连接组
边界和组合故障仍待完成。详见[压缩 DATA 标志隔离记录](rpc_v2_compressed_data_flag_validation_20261025.md)
及[GOAWAY 未知结果记录](rpc_v2_goaway_unknown_outcome_20261025.md)。

dev.37 修正 GOAWAY 测试清理只允许 `tr_client_destroy()` 首次返回 `TR_OK` 的错误假设：
按公开对象所有权合同，在 `WAIT_MS` 总期限内重试 `TR_AGAIN`，其余错误仍立即失败。该变更只影响
测试夹具，不改变运行时路径。GCC Release 全量 90/90，ASan/UBSan 下 `test_rpc_v2_sdk` 连续
5/5；新 head 的 GitHub 检查仍待完成。#242 的连接组接受边界、其他错误分类和组合故障未完成。
细节见[GOAWAY 未知结果记录](rpc_v2_goaway_unknown_outcome_20261025.md)。

dev.38 修正 Clang Release 暴露的 Client call 槽异步退休测试假设：弱引用释放后，旧传输流
退休前可能暂时占满容量；stale-token 复用用例现对合法 `TR_AGAIN` 做有界 owner pump/retry，
并验证输出句柄保持不变、旧 token 仍失效。Clang Release 下 `test_client_record` 连续 10/10；
全量 90/90，格式化后的目标复验 1/1；GCC ASan/UBSan 下该目标 5/5。Draft PR #324 的 dev.38
head `6f60dbbeb87f5036e36447fe39b1ade9792b1f9f` 全部 GitHub 检查通过。该改动只影响测试夹具，
V2 的连接组接受边界、其他错误分类和组合故障仍待完成。

dev.39 增加 #242/ERR-A 的错误分类表和不可变参考状态转换：同一单流语义错误仅在帧边界、
流标识及 RESET 能力都可信时重置目标流；否则关闭连接并让每个活动调用恰好终结一次。业务拒绝
与本地资源压力分别终结目标调用，保留同连接其他流；TLS 完整性、协商、帧边界和未知私有状态损坏
默认关闭连接。新增六项 Python 回归并接入已有 GCC 模型自测步骤；这是 L1 设计参考，不证明
生产 C 行为符合该矩阵。ERR-B 标识/RESET 时序、ERR-C 信用与组合故障、ERR-D 多成员排空边界仍待验收；
ERR-E 已验证墓碑固定预算、真实 Channel 长周转与 FIFO 淘汰语义，也有 256 路确定性碰撞簇压力回归；最大容量下的碰撞成本、更高频负载和更广组合矩阵仍待实现和验收。详见
[ERR-A 分类与模型记录](rpc_v2_error_scope_model_20261025.md)。

dev.40 补上 #242/ERR-B 的本地流 ID 耗尽分配边界：客户端奇数与服务端偶数最后一个
`uint32_t` ID 各只分配一次，之后保持耗尽状态、不回绕；`tr_stream_open()` 在观察到
耗尽后释放锁并启动 Channel GOAWAY 排空。纯分配边界回归与现有 Channel 排空套件通过，
但没有运行时测试将真实 Channel 计数器注入上限，故只登记为部分实现。RESET FIFO、
tombstone/连接代次组合以及 ERR-B 全矩阵仍待验收。详见
[本地流 ID 耗尽记录](rpc_v2_stream_id_exhaustion_20261025.md)。

dev.41 为同一边界补上真实 Channel 集成回归：GNU ld wrapper 仅在测试链接中把活动
Channel 的下一 ID 游标置为耗尽，不改变生产代码开关；验证 `tr_stream_open()` 返回
`CLOSED`、真实对端收到 GOAWAY，Channel 从 DRAINING 转为 DRAINED。Release 下
`test_stream_reset` 和 `test_channel_drain` 均通过。本地流 ID 耗尽到排空的集成路径已有
直接回归，但 ERR-B 的 RESET/FIFO/代次组合与 #242 其余验收仍未完成。

dev.42 修复上述新增 helper 漏入手工列举源码的 TCP Endpoint 测试目标，避免全量 CI 在
ThreadSanitizer/独立组件链接阶段出现未定义引用。本地 TSan 下 `test_rpc_record_endpoint_tcp`
与 `test_stream_reset` 构建、运行通过；dev.42 新 head 的 GitHub 检查全部通过。

dev.43 为 #242/ERR-A 补上真实 RPC Record Stream 配额失败隔离回归：两条调用共用同一
Channel，第一条在接收不完整 RAW MESSAGE 时因服务器硬预算不足而本地终结；测试再模拟上层
将该 RESOURCE_EXHAUSTED 映射为 `TR_ERR_NOMEM` RESET，验证对端终局只发生一次。释放预算后，
第二条调用仍可完成，缓存归零。Release `test_rpc_record_stream` 全目标与 dev.43 GitHub 检查通过。该切片没有
覆盖压缩缓冲、信用争用及连接级组合故障；ERR-A 的完整生产路径对照和 ERR-B/C/D/E 仍待验收。

dev.44 为 #242/ERR-B 增加 RESET 墓碑的物理连接代次回归：在旧连接完成双向 RESET 并保留
墓碑后，关闭连接、复用同一 Reactor 连接槽为新代次、替换 Channel 并完成新 HELLO。新连接
收到旧流 ID/原因的 RESET 时必须因未知流触发协议错误，不能被旧代次墓碑吸收。该场景与既有
墓碑容量淘汰、Stream 槽复用测试互补。Release 与 TSan `test_stream_reset` 全目标通过；
不覆盖拆分连接或多次快速重连，ERR-B 其他组合及 ERR-C/D/E 仍待验收。

dev.45 为 #242/ERR-B 增加 split Channel 的 RESET 墓碑物理连接身份回归：CONTROL 上已完成的
双向 RESET 墓碑不能吸收 BULK 物理连接上相同流 ID/原因的 RESET；BULK 必须因未知流关闭，
CONTROL 保持 UP 并继续接纳、交付和终止新流。Release 与 TSan `test_stream_reset` 全目标通过。
本切片仅补测试；其他 RESET/FIFO 交错与 ERR-C/D/E 仍待验收。详见
[split 连接 RESET 墓碑回归](rpc_v2_reset_tombstone_split_connection_20261025.md)。

dev.46 为 #242/ERR-C 增加 RESET 与迟到载荷释放组合回归：双向 RESET 完成后释放旧流已交付载荷，
后续 PING 必须直接得到 PONG，不能先收到退役流的 WINDOW_UPDATE；同 Channel 的另一条流仍能收发并正常
完成 RESET。Release 与 TSan `test_stream_reset` 全目标通过。本切片不覆盖信用债务跨消息、压缩预算与
多流并发故障的 ERR-C 全矩阵；其他 ERR-A/B/D/E 项仍待验收。详见
[RESET 后迟到载荷释放与信用记录](rpc_v2_reset_late_payload_credit_20261025.md)。

dev.47 为 #242/ERR-A 增加真实 TCP/Channel/RPC Endpoint 语义错误隔离回归：已准入调用收到
重复 OPEN 后以 `INVALID_ARGUMENT` 终结，后续调用仍在同一 TCP 连接完成；两端 Reactor 连接仍为 ACTIVE。
服务端 Endpoint 接管 Channel 事件回调，连接状态通过 Reactor 直接查询验证。
Release 与 TSan 下 `test_rpc_record_endpoint_tcp` 全目标通过。该用例证明一项 RPC 语义错误映射，
不代表完整分类矩阵、压缩失败、资源压力及连接级错误路径均已覆盖。详见
[真实 RPC 语义错误隔离记录](rpc_v2_semantic_error_isolation_20261025.md)。

dev.48 为 #242/ERR-A 增加真实 TCP 连接级错误关闭回归：两个 OPEN 已进入业务处理后注入头 CRC 错误，
服务端不得降级为单流处理；两端 Reactor 连接均退出活动状态，两个 Endpoint 调用各以
`UNAVAILABLE` 唯一终结，没有成功 FINAL，活动网络流归零。Release 与 TSan 下完整
`test_rpc_record_endpoint_tcp` 通过。本切片覆盖一个不可恢复线帧错误；其他连接失败、压缩/资源组合与
ERR-A 全矩阵及 ERR-B/C/D/E 仍待验收。详见
[真实 RPC 连接级错误关闭记录](rpc_v2_connection_error_termination_20261025.md)。

dev.49 为 #242/ERR-A 扩展压缩接收配额失败隔离：真实 Zstandard MESSAGE 在服务器解码工作区预算不足时只终结
目标调用，以 `RESOURCE_EXHAUSTED` 映射 RESET 通知对端；同共享 Channel 的 CONTROL/BULK lane 继续 UP。释放预算后，
同伴调用成功接收压缩正文并完成，终局缓存归零。Release 与 TSan 下完整 `test_rpc_record_stream` 通过。
该切片不覆盖完整压缩/信用组合和连接组边界；ERR-A 全分类及 ERR-B/C/D/E 仍未验收。详见
[压缩解码配额同伴隔离记录](rpc_v2_compressed_quota_peer_progress_20261025.md)。

dev.50 扩展 #242/ERR-A 的重复 OPEN 语义错误隔离证据：错误发生前，同一 TCP 连接上的同伴调用已进入
业务 MESSAGE 并暂停在 gate 中；错误流以 `INVALID_ARGUMENT` 终结期间，同伴保持未终结、连接保持 ACTIVE。
释放 gate 后该在途同伴以 OK 完成，错误后的新调用也成功。GCC Release 与 TSan 下完整
`test_rpc_record_endpoint_tcp` 通过。dev.47 的原验证仅覆盖错误后新开调用；本切片补齐已活动同伴时序，
并不代表 ERR-A 全分类或 ERR-B/C/D/E 已验收。详见
[活动同伴流语义错误隔离记录](rpc_v2_semantic_active_sibling_isolation_20261025.md)。

dev.51 为 #242/ERR-A 增加业务拒绝期间的活动同伴隔离回归：方法 0 返回 `PERMISSION_DENIED` 时，同一
TCP 连接上的方法 1 调用已进入业务 MESSAGE 并暂停；拒绝流终结后，同伴仍活动、连接仍 ACTIVE。释放 gate
后同伴以 OK 唯一终结。GCC Release 与 TSan 下完整 `test_rpc_record_endpoint_tcp` 通过。本切片只证明一项
业务终局与同伴交错，不代表 ERR-A 全分类矩阵或 ERR-B/C/D/E 已完成。详见
[业务拒绝活动同伴隔离记录](rpc_v2_business_rejection_active_sibling_20261025.md)。

dev.52 为 #242/ERR-A 增加路由拒绝期间的活动同伴隔离回归，覆盖未知方法 `UNIMPLEMENTED` 与非法 OPEN 元数据
`INVALID_ARGUMENT`。路由拒绝必须在业务 Dispatch 前完成；同连接活动同伴在拒绝期间保持未终结、连接保持 ACTIVE，
随后同伴以 OK 完成。GCC Release 与 TSan 下完整 `test_rpc_record_endpoint_tcp` 通过。本切片覆盖两种路由错误，
不代表 ERR-A 完整分类矩阵或 ERR-B/C/D/E 已完成。详见
[路由拒绝活动同伴隔离记录](rpc_v2_router_rejection_active_sibling_20261025.md)。

dev.53 为 #242/ERR-D 增加公开连接组双 DATA 成员排空回归：两个 DATA Stream 通过不同物理成员承载；开始排空后
新 Offer/Ready 被拒绝，活动传输未释放前 Client 和 Server 均保持未排空。释放两个传输后 Client 排空完成，Server
继续等待客户端关闭两条 DATA 成员，最终组/连接/DATA/传输计数均归零。SDK 测试销毁 helper 同时对 API 合法返回的
`TR_AGAIN` 作有界重试，仍会暴露其他错误。GCC Release 与 TSan 下 `test_connection_group_facade`、`test_rpc_v2_sdk`
完整目标均通过。本切片尚未覆盖发送中/压缩中 drain、成员故障/重连及完整多组竞争矩阵。详见
[双 DATA 成员连接组排空记录](rpc_v2_group_multi_data_drain_20261025.md)。

dev.54 扩展同一双成员回归：DATA 发送已被接受且停留在 Reactor TX 队列时开始 drain，逐个释放客户端 transfer 后，
Client 活动传输数为零但 `send_bytes_inflight` 仍为 1，等待继续超时。解除 socket `EAGAIN` 后服务端确实收到消息，
发送字节归还，Client 才完成本地 drain；Server 继续等待成员关闭。GCC Release 与 TSan 下完整
`test_connection_group_facade` 通过。本切片只覆盖未压缩 DATA 发送在途，不覆盖压缩任务在途、成员故障/重连或多组竞争。
详见[DATA 在途发送排空记录](rpc_v2_group_data_tx_drain_20261025.md)。

dev.55 补充 Zstandard 压缩任务取消与 drain 交错：暂停已完成压缩、尚未交付的 worker 回调，开始双端排空并释放
客户端 transfer；transfer 计数归零后，仍归属压缩任务的输入字节使 Client drain 保持未完成。放开 worker 后任务被
取消收敛、不发送 DATA，字节归还后 Client drain 完成；Server 仍等待 DATA 成员关闭。GCC Release 与 TSan 下完整
`test_connection_group_facade` 通过。本切片不覆盖压缩完成并接受发送后的 encoded wire/TX 在途排空、成员故障或多组竞争。
详见[Zstandard 任务取消排空记录](rpc_v2_group_compression_cancel_drain_20261025.md)。

dev.56 修复并覆盖压缩 wire 在途 drain：已附着 DATA 成员上的压缩消息在 Client/Server drain 后仍继续发送和交付；
此前 `tr_pipeline_listener_message_done()` 把 `listener->draining` 当作取消条件，导致连接/路由代次仍有效的解码结果被静默
丢弃。现在仍用精确连接身份判定回调有效，drain 只关闭新准入。测试阻塞真实 DATA socket 写入并暂停压缩完成回调，
验证服务端消息交付、Client TX wire 归还后排空完成、Server 等待成员关闭。GCC Release 与 TSan 下完整
`test_connection_group_facade` 通过。连接故障/重连和多组竞争仍待验收。详见
[压缩 wire 在途排空修复记录](rpc_v2_group_compressed_wire_drain_20261025.md)。

dev.57 为 #242/ERR-D 增加双 DATA 成员故障与替代回归：中止一条真实物理连接后，旧 transfer 从 Client/Server 亲和统计中
退役，另一成员仍能发送；同一 group/epoch 重新 Offer/Ready 后，替代连接可接纳新 transfer 并交付消息。释放剩余亲和并
排空后，Client 先完成本地等待，Server 在控制与所有 DATA 成员关闭后完成等待，组/连接/DATA/传输计数归零。Release 与
TSan 下完整 `test_connection_group_facade` 通过；变异版本若以错误代次撤销 transfer，会在客户端活动传输计数断言处失败。
多组并发成员故障与 ERR-D 全矩阵仍未验收。详见[双 DATA 成员故障与重建回归](rpc_v2_group_member_failure_replacement_20261025.md)。

dev.58 为 #242/ERR-D 增加单 shard 双组故障隔离回归：两个不同 group/epoch 同时持有 CONTROL 和 DATA；A 组 DATA 故障并
替代、随后关闭期间，B 组的活动 DATA transfer 仍成功交付。A 组关闭后服务端只剩 B 组，B 组继续发送并独立完成 drain，
最终组/连接/DATA/transfer 计数归零。GCC Release 与 TSan 下完整 `test_connection_group_facade` 通过。该用例不覆盖多组
同时故障、连续替代和资源竞争压力矩阵。详见[双组成员故障隔离记录](rpc_v2_group_multi_group_failure_isolation_20261025.md)。

dev.59/dev.60 为 #242/ERR-D 增加双组同时故障、两轮成员替代及组级独立关闭回归；完整连接组门面目标在 Release 与
TSan 下通过，dev.60 Release 连续运行 8/8。多轮高频竞争和资源压力仍待验收。详见
[双组重复故障与快速替代记录](rpc_v2_group_repeated_failure_replacement_20261025.md)。

dev.61 为 #242/ERR-E 增加真实内存账本下的 RESET 墓碑固定预算回归：容量为 2 的 Channel 完成三次 RESET，
验证驻留墓碑吸收重复 RESET、FIFO 淘汰后旧 RESET 触发未知流错误；周转期间 `current_bytes` 与 `peak_bytes`
均保持创建时固定值，预算拒绝数为零，销毁后当前预算归零。Release 与 TSan 下完整 `test_stream_reset` 通过。
仍待验证高频 RESET 的 CPU 成本、长时间压力及更广的信用/丢弃组合。详见
[RESET 墓碑固定预算回归记录](rpc_v2_reset_tombstone_fixed_budget_20261025.md)。

dev.62 为 #242/ERR-E 将精确墓碑历史查询改为固定开放寻址索引，复合键为物理连接代次与 stream ID；允许 split
Channel 两条连接独立前沿产生相同 ID 墓碑。FIFO 覆盖时删除旧键并修复碰撞簇，查询命中后仍核对对端原因；
索引字节纳入 Channel 固定预算。单元测试覆盖跨表尾碰撞簇、删除/槽复用，以及
40k 个单调流 ID 下插入与成功查询总探测数不超过 8 倍条目数；构造事务测试以 max_streams=1/2/3 的预算差分
确认索引字节确实计费。Release、ASan/UBSan、TSan 下索引、完整 `test_stream_reset`、Channel 构造事务和排空目标
通过。索引期望 O(1)，碰撞最坏仍为 O(max_streams)；实际连接上的长期高负载与碰撞压力仍待验收。详见
[RESET 墓碑查询索引记录](rpc_v2_reset_tombstone_index_20261025.md)。

dev.63 补齐 `test_rpc_record_endpoint_tcp` 手动维护的生产对象闭包，使其包含 dev.62 新增的
RESET 墓碑索引模块；GCC 精确诊断基线同步反映 `channel.c` 顶部结构搬移导致的既有告警行号变化，诊断本身未变。
该修复只影响测试链接和诊断定位，不改运行时逻辑；PR #324 的 GitHub 检查 21/21 通过。详见
[RESET 索引测试链接修复记录](rpc_v2_reset_tombstone_build_linkage_20261025.md)。

dev.64 在一条真实 Channel 上完成 128 次双向 RESET，让容量为 4 的墓碑环连续周转 32 轮；重复发送当前最老
和最新驻留墓碑均被吸收，重复发送第一条已淘汰墓碑则关闭连接并报告未知流。Release、TSan、ASan/UBSan 下
`test_stream_reset` 通过。该回归验证长期 FIFO 周转后的保留窗口和淘汰边界，不测量吞吐/CPU，也不覆盖恶意
碰撞压力。dev.64 的两组 GitHub CI 与 RPC smoke 检查 21/21 通过。ERR-E 的更高频负载仍待验收。详见
[RESET 墓碑长周转回归](rpc_v2_reset_tombstone_long_turnover_20261025.md)。

dev.65 为墓碑索引加入确定性碰撞簇压力回归：256 个不同流 ID 被构造为命中同一 home bucket，表容量为 512；
初始填充后连续淘汰最老键、复用 FIFO 环槽 256 次，检查每次删除后的探测终止位置、替换插入探测数，以及最终全部驻留键可查、
所有淘汰键不可查。关闭大表向后移位修复的临时变异在该新断言失败，恢复后 Release、TSan、ASan/UBSan 下
`test_reset_tombstone_index` 均通过；dev.65 两组 GitHub CI 与 RPC smoke 检查 21/21 通过。它覆盖 256 路合成碰撞，
不测量耗时，也未推断 `max_streams=40000` 的最坏碰撞 CPU 成本。详见
[RESET 墓碑索引碰撞压力回归](rpc_v2_reset_tombstone_collision_stress_20261025.md)。

dev.66 将已有 40k 顺序奇数 ID 的索引测试扩展为最大配置 FIFO 周转：填满 `max_streams=40000` 历史后，连续删除原键并
在对应环槽插入 40k 个新 ID；每个旧 ID 在淘汰后返回 stale，最终所有新 ID 均映射回原环槽。删除、淘汰查询、重插与
最终保留/淘汰查询的累计探测数不超过 1.92m。跳过大表簇修复的变异在最大容量删除断言失败；恢复后 Release、TSan、ASan/UBSan
下 `test_reset_tombstone_index` 均通过。该确定性边界不设耗时门槛，也不等同真实 Channel 40k 并发压力。最大容量对抗碰撞仍待验收。详见
[RESET 墓碑索引最大容量 FIFO 周转](rpc_v2_reset_tombstone_max_capacity_turnover_20261025.md)。

dev.67 在 `max_streams=40000`、131072 槽表和满历史占用下加入 256 路同 home 碰撞簇 FIFO 周转。每轮删除簇首后、
复用环槽前，先确认其余碰撞键仍可查，再验证旧键已淘汰；最终核对其余 39,744 个键和新驻留键槽位不变。跳过大表
簇修复的变异在复用前幸存键断言失败；恢复后 Release、TSan、ASan/UBSan 下 `test_reset_tombstone_index` 均通过。
测试累计探测预算为 2.56m，但 `remove` 的探测输出不包含 backward-shift 扫描，因此不视为 CPU 成本上界；40k 键同 home
和真实 Channel 大规模连接压力仍待验收。详见[最大容量碰撞周转记录](rpc_v2_reset_tombstone_max_capacity_collision_20261025.md)。

dev.68 扩展 #242/ERR-A、ERR-C 压缩工作区配额场景：目标流以 `RESOURCE_EXHAUSTED` 终结并 RESET 后、预算预留尚未释放时，
同一真实 Channel 的 CONTROL keepalive PING/PONG 仍可往返；随后释放预算，压缩同伴调用完整完成。GCC Release、TSan、
ASan/UBSan 下完整 `test_rpc_record_stream` 均通过。本切片只加回归测试，不证明连接组的控制面错误分类或完整资源公平矩阵。详见
[压缩配额期间控制帧进展记录](rpc_v2_compressed_quota_control_progress_20261025.md)。

dev.69 将同一 CONTROL keepalive 进展断言用于 RAW 重组预算失败：预算全部预留期间，失败流仍可通过 RESET 终结，
PONG 往返完成后释放预算，同 Channel 同伴继续成功。RAW 重组与 Zstandard 解码配额两种场景都在 GCC Release、TSan、
ASan/UBSan 下通过。仅增加测试；持续压力下延迟上界、连接组错误矩阵与其他预算/信用组合仍待验收。详见
[重组与解码配额控制面进展记录](rpc_v2_receive_quota_control_progress_20261025.md)。

dev.70 为 #242/ERR-E 增加真实 Channel 最大容量墓碑周转：Release 与 ASan/UBSan 在 `max_streams=40000` 下完成 40,000 次
OPEN/双向 RESET，验证最早与最新驻留墓碑、再加入一条后的 FIFO 淘汰及未知 RESET 连接错误；TSan 使用容量 5,000 填满同一
状态机边界，最大容量索引仍由独立索引目标执行 TSan 压力。仅增加真实传输集成测试，不改生产实现。真实 40k 键同 home
极端碰撞、持续高频 CPU/延迟上界及 ERR-A/B/C/D 其他组合仍待验收。详见
[真实 Channel 最大容量 RESET 墓碑周转记录](rpc_v2_reset_tombstone_channel_capacity_20261025.md)。

dev.18/.19 的 [连接组成员代次修复](rpc_v2_group_member_generation_20261024.md)
让 DATA 路由代次在单个分片注册表生命周期内唯一，阻止旧 CONTROL 关闭后未完成的
DATA 前导跨同一 `(group_id, epoch)` 的 Pipeline 重建附着。dev.19 另修正旧 Pipeline 预留
入口未初始化新请求字段的问题，并覆盖一个 DATA 已附着、另一个前导仍 pending 时的
CONTROL 丢失交错。该切片不覆盖 GOAWAY 接受边界、RESET/错误分类或 #242 的完整组合故障矩阵。

#234 的 [TLS 接入 ADR-009](../architecture/decisions/ADR-009-tls-byte-endpoint.md)
界定字节端点、认证先于 TRR1/TRP1、WANT 状态与关闭所有权。dev.9 的
[TLS-A 原型](rpc_v2_tls_a_20261023.md)完成 BIO 对照和内部端点合同；TLS-B
冻结公开配置并加入 OpenSSL 3 后端；TLS-C 普通 V2 Client/Server Reactor 已由
PR #310 以 `0fa90817c15286485ee2e1cfe8372bb8ab8a8e5a` 合入主线，主线 CI 全绿。
TLS-D1 Task 1–4 已在 `feat/v2-tls-d-234` 实现并推送：普通 RPC mTLS/URI SAN、连接组
CONTROL/DATA 主体绑定、profile generation 轮换和旧连接撤销。TLS-D1 `.16` 的五种本地
工具链/消毒器矩阵均为 88/88，SDK header 安装修复 `.17` 的 GCC Release 复测也为
88/88；安装 SDK 的 C11/C++11 消费端和 CRC32C 无 SSE4.2 路径已验证。
GCC 11/Clang 14 静态分析和 PR #311 全部 GitHub 检查已在 `b713801` 通过；Clang 14
既有告警行号已按未变函数体证据校准，8 条告警及 `unverified` 状态保留。合并后的 main CI、
#234 部分交付登记与远程分支清理仍待 Task 5；
异步凭证属于 TLS-D2，资源总账和公网验收属于 TLS-E。

## 持续开发顺序

| 范围 | 当前剩余工作 | GitHub |
|---|---|---|
| 错误隔离 | ERR-A 分类表与纯状态参考模型已实现；真实 RPC Record Stream 覆盖 RAW/压缩接收配额失败、预算保留期间 CONTROL keepalive 往返和同 Channel 同伴恢复；真实 TCP Endpoint 覆盖重复 OPEN、业务 `PERMISSION_DENIED`、未知方法、非法 OPEN 元数据期间活动同伴继续完成，以及头 CRC 错误导致全部活动调用唯一 `UNAVAILABLE` 终结；其余分类到生产 C 路径的对照和连接组接受边界仍待补齐。普通/压缩 DATA 类型标志覆盖活动流与 RESET 丢弃态，单连接 GOAWAY cutoff 和已接纳 CALL_OPEN 未知结果/不重放有回归。ERR-B 本地 ID 耗尽及 GOAWAY 排空集成、墓碑 FIFO 淘汰、Stream/物理连接代次、split 连接相同 ID 双墓碑、驻留重复 RESET 与索引 FIFO 周转各有回归，其他 RESET/FIFO 组合仍待验收；ERR-C 信用与组合故障已有 RESET 后迟到载荷释放不补信用回归，其余信用边界待验收；ERR-D 已有双 DATA 成员 drain、逐个 transfer 释放、未压缩 DATA TX、压缩任务/编码 wire 在途、单成员故障与同组替代、双组同 shard 先后及并发故障隔离、双组各自两轮替代及组级关闭回归；多轮高频压力竞争仍待验收；ERR-E 墓碑固定预算和复合查询索引、真实 Channel 128 次 RESET/32 轮墓碑环周转、256 路小表确定性碰撞、max_streams=40000 的顺序 FIFO 周转和满历史下 256 路同 home 碰撞簇 FIFO 周转、真实 Channel 40k 历史填满与首次淘汰已有回归；40k 键同 home 最坏输入及持续高频 CPU/延迟成本仍待验收 | #242 |
| 资源与活性 | 分域/全局预算账本、压缩、多 DATA 与慢消费者组合矩阵；共享 Channel 的 L/W 与持有释放已有回归 | #241、#235 |
| 运行模型 | 操作持有/取消上下文、父子 deadline、跨 owner 依赖和回收证明 | #243 |
| TLS 与身份 | TLS-C 已合入；TLS-D1 当前分支已实现普通 RPC/连接组 mTLS、主体连续性与 profile 轮换，本地五矩阵 88/88；固定版分析、GitHub 验收与 #234 更新待完成；TLS-D2 和 TLS-E 仍待开发 | #234 |
| 网络压缩 | 将已有独立 none/Zstd 原语接入普通 V2；方向协商和安全预算 | #235 |
| 服务目标 | 解析快照、选路、健康、等待就绪、有界 Attempt/重放 | #244 |
| 类型契约 | codec、描述符指纹、C/Rust 生成桩和有界解码 | #245 |
| 后端决策 | 明确字节/流/消息/RPC 契约，形成 TRP 与 HTTP/2 等价评估证据 | #246 |
| 横切验证 | TEST-A PR #317、TEST-B1 PR #318、TEST-B2 PR #319、TEST-B3 PR #320 与 TEST-B4 PR #321 的实现提交均已通过 14/14 GitHub 检查但待合并；TEST-C1 完成固定种子模型探索与 mutant 缩减，TEST-C2 验证 Endpoint `TR_AGAIN` 后恢复，TEST-C3 验证 RESET 阻止背压终局帧重试；通用生产 C 故障时序、TEST-B 完整事件字母表、真实网络/派发、TEST-D 集成平台矩阵、TEST-E 分层诊断与 40k 容量性能仍待完成 | #247 |
| 最终核对 | 将已实现能力逐项映射到验收，收口阶段性架构与模块任务 | #239、#240、#248、#154 |

长期治理 #249 属于持续维护职责。正式发布仍需完成上述有限范围的实现和验收；当前不得把 dev 预览称为已完成的 V2。
