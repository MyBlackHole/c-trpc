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
生产 C 行为符合该矩阵。ERR-B 标识/RESET 时序、ERR-C 信用与组合故障、ERR-D 多成员排空边界、
ERR-E 有界历史与成本仍待实现和验收。详见
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
| 错误隔离 | ERR-A 分类表与纯状态参考模型已实现；真实 RPC Record Stream 已覆盖 RAW 接收配额失败、压缩解码工作区配额失败、单流 RESET 与同 Channel 同伴调用继续完成，真实 TCP Endpoint 已验证重复 OPEN 语义隔离和头 CRC 错误连接关闭；其余分类到生产 C 路径的对照仍待补齐；普通/压缩 DATA 类型标志覆盖活动流与 RESET 丢弃态，单连接 GOAWAY cutoff 和已接纳 CALL_OPEN 未知结果/不重放有回归；ERR-B 本地 ID 耗尽及 GOAWAY 排空集成、墓碑 FIFO 淘汰、Stream/物理连接代次、split 连接墓碑身份各有回归，其他 RESET/FIFO 组合仍待验收；ERR-C 信用与组合故障已有 RESET 后迟到载荷释放不补信用回归，其余信用边界待验收；ERR-D 连接组排空边界、ERR-E 有界历史和成本仍待验收 | #242 |
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
