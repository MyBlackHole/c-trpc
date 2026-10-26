# c-trpc

面向 Linux 的 C 传输/RPC 库，重点关注有界资源、显式所有权、高吞吐批量数据流，以及通用传输/RPC 与备份业务语义之间的清晰分离。

当前持续验证范围为 Ubuntu 22.04 / x86_64 的 C11 SDK；C++11 消费端校验公开头文件、C 链接约定和真实 V2 客户端／服务端调用。其他内核、架构与工具链须单独验证。部署安全、许可及兼容边界见 [平台与发布契约](docs/platform-support.md)。

当前 `2.0.0-dev.143` 候选基于 OpenSSL 3 TLS 1.3 和 mTLS 已接入普通 V2
Client/Server，以及 transport-only 连接组 Client/Server。连接组启用 TLS 时强制要求
双向客户端证书认证；CONTROL/DATA 都在 TLS 认证后发送或解析路由，并要求每条 DATA
连接的服务端 URI SAN 主体与活动 CONTROL 主体一致。TLS profile 支持 graceful 轮换和
撤销旧代次；已接管的会话保留原 profile，旧 CONTROL 不得接受新 Server 代次的 DATA。
TLS-D2 增加有界异步本地凭证获取/轮换和连接组 CONTROL/DATA 异步授权，取消须由 provider
确认；授权在 Reactor owner 重验后才会提交路由。dev.26 为普通 V2 Client 增加唯一
CallRef，可跨回调读取终局快照、活动期请求取消，并在持有期间固定占用有界调用槽。
dev.27 为压缩 DATA 分帧增加有界的 DATA TX 池就绪通知，池恢复后由 Reactor owner
推进重试；其他资源预算与信用不足仍沿用既有维护和协议事件。
默认仍为明文 TCP，TLS 失败关闭连接，不回退明文。TLS-D1/D2 全验证矩阵与 GitHub
验收仍未完成；TLS-E 和 #243 的父子 deadline、跨 owner 环依赖等 Call 范围也未完成，
本版本不代表 #234 或 V2 整体验收完成。
dev.120–dev.126 为 #242/ERR-D 增加真实 TCP 双组 32 轮并发 DATA 成员故障替代回归、CONTROL 致命拆除后的活动成员清理与旧 epoch 路由隔离组件回归、异构 DATA 状态回收和组间故障隔离、32 轮真实 TCP CONTROL teardown/epoch 重建与早期旧 route 重放回归，以及 route 前导延迟到新 epoch 后提交时的旧 route 拒绝回归；这些切片不代表持续压力、资源争用和完整错误矩阵已验收。
dev.127 为 #242/ERR-C 增加 RESETTING 窗口中应用线程释放已交付 DATA 的信用隔离回归；Release、TSan、ASan/UBSan 的 `test_stream_reset` 均通过。该测试只增加竞态边界证据，不表示 ERR-C 信用与组合故障全矩阵或 V2 整体验收完成。
dev.128 为 #242/ERR-C 增加跨消息信用债务回归：两个小消息累计到 WINDOW_UPDATE 阈值后，下一条已交付消息在 RESETTING 窗口释放，目标流不得额外获赠信用；Release、TSan、ASan/UBSan 的 `test_stream_reset` 均通过。该切片仍不代表 ERR-C 全矩阵或 V2 整体验收完成。
dev.129 为 #242/ERR-C 增加真实压缩接收回归：应用持有解压后 payload 时目标流进入 RESETTING，释放只归还内存，接收/消费/通告信用与 WINDOW_UPDATE 计数不变；同 Channel 压缩同伴继续收发。Release、TSan、ASan/UBSan 的 `test_transport` 均通过。该切片仍不代表 ERR-C 全矩阵或 V2 整体验收完成。
dev.130 为 #242/ERR-C 增加 RESETTING 窗口中的并发多载荷释放回归：同一流持有两条 64 字节消息，在两个应用线程并发释放时不增加通告信用或 WINDOW_UPDATE，同 Channel 健康同伴继续完成。Release、TSan、ASan/UBSan 的 `test_stream_reset` 均通过。该切片仍不代表 ERR-C 全矩阵或 V2 整体验收完成。
dev.131 为 #242/ERR-C 增加双 RESETTING 流并发释放回归：两条流各持有一条 128 字节消息，分别进入 RESETTING 且对端暂不确认时，由两个应用线程并发释放；每条流通告信用保持 256、无 WINDOW_UPDATE，健康同伴仍能完成。Release、TSan、ASan/UBSan 的 `test_stream_reset` 均通过。该切片仍不代表 ERR-C 全矩阵或 V2 整体验收完成。
dev.132 扩展双 RESETTING 流回归：对端确认到达前，两条流各收到一条已在途的合法 64 字节 DATA；RESET 丢弃路径只消费 wire 字节、不交付应用也不恢复信用，两个应用线程再并发释放各自持有的 128 字节正文。两条流通告上限保持 256、`window_updates_tx` 不增长，健康同伴仍能完成。Release、TSan、ASan/UBSan 的 `test_stream_reset` 均通过。该切片仍不代表 ERR-C 全矩阵或 V2 整体验收完成。
dev.133 将 RESET discard 在途 DATA 扩至填满每条流冻结窗口的剩余 128 字节；各流接收总量达到通告上限 256，消费量达到 128，仍无信用更新。并发释放此前持有的 128 字节后，消费量到 256、通告上限仍为 256，wire 无 WINDOW_UPDATE，健康同伴仍完成。Release、TSan、ASan/UBSan 的 `test_stream_reset` 均通过。该切片仍不代表 ERR-C 全矩阵或 V2 整体验收完成。
dev.134 在双 RESETTING 流窗口精确填满并并发释放后，再向其中一条流发送超限 1 字节 DATA，验证连接以 `TR_ERR_BAD_LENGTH` 关闭且不额外交付应用数据、不发送 WINDOW_UPDATE。Release、TSan、ASan/UBSan 的完整 `test_stream_reset` 均通过。该切片仍不代表 ERR-C 全矩阵或 V2 整体验收完成。
dev.135 在 Split Channel 的 CONTROL 连接上验证 RESETTING 流耗尽冻结窗口后的一字节超限仍以 `TR_ERR_BAD_LENGTH` 关闭该物理连接，而 BULK 独立连接上的健康流仍能交付并完成。Release、TSan、ASan/UBSan 的完整 `test_stream_reset` 均通过。该切片仍不代表 ERR-C 全矩阵或 V2 整体验收完成。
dev.136 将 Split Channel 的冻结信用超限隔离回归扩展到两个方向：CONTROL 超限时 BULK 健康流仍完成，BULK 超限时 CONTROL 健康流仍完成；两种情况下只关闭出错物理连接。Release、TSan、ASan/UBSan 的完整 `test_stream_reset` 均通过。该切片仍不代表 ERR-C 全矩阵或 V2 整体验收完成。
dev.137 增加 RESETTING 期间的合法在途 `WINDOW_UPDATE` 回归：接收信用上限可记录递增，但不产生 WRITABLE、不恢复 `tr_stream_send`；对端 RESET 确认后，同连接健康流仍可交付并完成。Release、TSan、ASan/UBSan 的完整 `test_stream_reset` 均通过。该切片仍不代表 ERR-C 全矩阵或 V2 整体验收完成。
dev.138 验证 Split Channel 的 RESET/CLOSE 顺序边界：对端 RESET 确认前的 FIFO 普通 CLOSE 只结束半侧、不提前退役流；双向 RESET 完成后的晚到 CLOSE 只关闭对应物理 lane，另一 lane 的健康流仍能交付并完成。Release、TSan、ASan/UBSan 的完整 `test_stream_reset` 均通过。该切片仍不代表 #242 或 V2 整体验收完成。
dev.139 验证 Split Channel RESETTING 流在 FIFO CLOSE 后又收到 DATA 时，只关闭违反半关闭方向规则的物理 lane；CONTROL、BULK 两种方向都确认 DATA 未交付、另一 lane 的健康流仍能完成。Release、TSan、ASan/UBSan 的完整 `test_stream_reset` 均通过。该切片仍不代表 #242 或 V2 整体验收完成。
dev.140 验证 Split Channel RESETTING 流先收到合法 DATA、再收到 FIFO CLOSE：DATA 仅计入接收/消费账本、不交付应用或补发 WINDOW_UPDATE，CLOSE 不提前退役流；CONTROL、BULK 两种方向均能正常完成 RESET，另一 lane 的健康流仍可交付并完成。Release、TSan、ASan/UBSan 的完整 `test_stream_reset` 均通过。该切片仍不代表 #242 或 V2 整体验收完成。
dev.141 验证 Split Channel 对端 RESET 后，本地 RESET 响应入队但因发送背压未写出时收到 FIFO DATA：CONTROL、BULK 两个方向均以 `TR_ERR_STATE` 关闭目标物理 lane，不交付 DATA，另一 lane 的健康流仍能交付并完成 RESET。Release（40k 墓碑）、TSan（5k）、ASan/UBSan（40k）的完整 `test_stream_reset` 均通过。该切片仍不代表 #242 或 V2 整体验收完成。
dev.142 扩展 RESET 响应背压回归，将响应仍遇到 `EAGAIN` 时的 FIFO 后续帧覆盖到 DATA 和 `WINDOW_UPDATE`；CONTROL、BULK 两方向的四个组合均只关闭目标 lane，WU 不增加接收更新统计，健康同伴仍能交付并完成 RESET。Release（40k 墓碑）、TSan（5k）、ASan/UBSan（40k）的完整 `test_stream_reset` 均通过。该切片仍不代表 #242 或 V2 整体验收完成。
dev.143 将 RESET 响应背压窗口的后续帧扩展到 `STREAM_CLOSE`，与 DATA、`WINDOW_UPDATE` 一起覆盖 CONTROL、BULK 两种目标 lane 的六个组合。六个组合均只关闭目标物理 lane，另一 lane 的健康流继续交付并完成 RESET。Release（40k 墓碑）、TSan（5k）、ASan/UBSan（40k）的完整 `test_stream_reset` 均通过。该切片仍不代表 #242 或 V2 整体验收完成。
dev.28 增加只供测试使用的 RPC V2 有限状态参考模型与严格 JSON 轨迹回放，覆盖
Call 终局、方向消息基数、迟到工作、槽代次和抽象预算租约。它为 #247 后续真实组件
测试提供语义 oracle，不直接测试生产并发代码，也不代表 #247 或 V2 验收完成；运行
`python3 scripts/test_rpc_v2_model.py -v` 可执行模型自测。
dev.29 增加 TEST-B1 确定性组件回放：用同一固定 JSON 轨迹驱动该模型和真实
RPC Record Endpoint 测试夹具，验证取消后的迟到工作、重复终局回调和资源静默回收。
夹具的 Reactor、Channel、Dispatch 和 Executor 是受控替身，不覆盖真实 TCP 或生产
Dispatch 调度；运行 `python3 scripts/test_rpc_v2_component_replay.py -v` 可执行映射自测，
构建 Release 后可执行模型与组件对照：

```sh
python3 scripts/rpc_v2_component_replay.py \
  --binary build/linux/x86_64/release/test_rpc_record_endpoint_contract
```

该切片不代表 #247 或 V2 验收完成。
dev.30 在 TEST-B1 之上增加 TEST-B2 截止终局后迟到工作轨迹：测试夹具核对 TIMEOUT RESET、
迟到完成不改写终局以及资源归还。它仍是单组件、可控下层交错，不代表真实 Dispatch 调度
或 TCP 集成；可用同一回放命令并通过 `--trace` 指向
`tests/fixtures/rpc_v2_model/component-deadline-late-work-v1.json` 执行第二条轨迹。
dev.31 增加 TEST-B3 同槽复用后的旧代次迟到 DATA 轨迹：旧流代次消息必须被忽略，
且不能进入复用该槽的新调用；旧、新调用都完成唯一终局并归还资源。仍只验证真实
Record Endpoint 配合受控 Dispatch/Channel 回调，不覆盖生产 Reactor、真实线程或 TCP。
可将 `--trace` 指向
`tests/fixtures/rpc_v2_model/component-stale-message-after-reuse-v1.json` 执行第三条回放。
它接入现有 `build-test (gcc)` job，没有新增 CI job/check。
dev.32 增加 TEST-B4 成功 FINAL 已接受后收到 RESET 的竞争轨迹；Endpoint 必须保留 OK
终局与一次回调，再退役并归还资源。它与 B1/B2/B3 一样是受控组件交错，不声称覆盖生产
派发调度或真实 TCP；可用 `--trace` 指向
`tests/fixtures/rpc_v2_model/component-final-reset-after-accepted-v1.json` 执行第四条回放。
四条回放仍运行在既有 `build-test (gcc)` job 中。
dev.33 增加 TEST-C1 固定种子模型探索与确定性轨迹缩减：自有 xorshift32 PRNG
生成 schema v1 事件，默认探索 64 个 seed、每条最多 48 个事件；同一模型负责生成期望
并检查回放不变量。已知重复终局回调 mutant 只用于 Python 测试，验证失败报告和缩减器，
不进入 CLI 默认路径、生产 C 或 Endpoint 适配器。这是 L1 工具链验证，不代表生产对象故障
注入或 #247/V2 验收完成。运行自测与探索器：

```sh
mkdir -p build/test-tmp
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/build/test-tmp" \
  python3 scripts/test_rpc_v2_trace_explorer.py -v
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/build/test-tmp" \
  python3 scripts/rpc_v2_trace_explorer.py \
  --seed 33047 --cases 64 --max-events 48 \
  --failure-dir build/test-tmp/rpc-v2-trace-explorer
```

发现模型回放或不变量异常时，命令返回 1，在 stdout 输出包含原始与最小轨迹的 JSON，并在工作区
保存 `original.json` 和 `minimized.json`；参数错误或工件写入失败返回 2。
普通成功仅输出固定摘要。

dev.34 的 TEST-C2 增加一条真实 Endpoint 最终错误状态发送背压回放。模型中的抽象 `ERROR`
在受控 Dispatch 夹具中映射为 `UNIMPLEMENTED`；夹具让首次 `tr_stream_sendv` 返回一次
`TR_AGAIN`，断言第二次发送被接受、只有一帧最终状态、用户终局回调一次、调用槽退役且预算归零。
该回放验证实际 `rpc_record_endpoint.c`，但 Channel、Dispatch、Reactor 与 Executor 仍为受控替身，
不覆盖真实 TCP、通用故障时序或 TEST-C 完整要求。运行方式：

```sh
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/build/test-tmp" \
  python3 scripts/test_rpc_v2_component_replay.py -v
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/build/test-tmp" \
  python3 scripts/rpc_v2_component_replay.py \
  --trace tests/fixtures/rpc_v2_model/component-final-send-backpressure-v1.json \
  --binary build/linux/x86_64/release/test_rpc_record_endpoint_contract
```

该切片已让一次确定性故障进入真实 C Endpoint 组件测试；任意故障序列、真实网络/派发、TEST-D/E、
#248 与 V2 整体验收仍未完成。
dev.35 再增加 TEST-C3：错误最终帧第一次发送遇到 `TR_AGAIN` 后、Endpoint 下一次重试前
收到远端 RESET；回放验证发送不重试、无已接受最终帧、终局回调/槽退役各一次且预算归零。
它仍是固定 Endpoint 场景，不替代通用生产 C 故障时序、真实网络/派发或 TEST-C/D/E 完整验收。
新增/修改源码行格式门禁已移除；GCC 既有构建作业继续运行翻译目录和格式工具自测，改动行格式检查可本地执行。

TEST-C3 回放：

```sh
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/build/test-tmp" \
  python3 scripts/rpc_v2_component_replay.py \
  --trace tests/fixtures/rpc_v2_model/component-final-send-reset-before-retry-v1.json \
  --binary build/linux/x86_64/release/test_rpc_record_endpoint_contract
```
dev.39 增加 #242/ERR-A 错误作用域分类表与不可变 Python 参考状态模型，覆盖
业务终局、可信单流 RESET、本地资源拒绝、解析可信度变化及连接级失败。它是 L1
设计参考，不证明生产 C 路径已经满足这些分类；运行
`python3 scripts/test_rpc_v2_error_scope_model.py -v` 可执行六项边界回归。
RESET/标识代次、信用保留、连接组排空边界与资源成本仍在后续切片中验证。

dev.40 收口 #242/ERR-B 的一个标识耗尽边界：本地流 ID 到达奇数/偶数最大值后
饱和为耗尽状态，不回绕；再次申请会尝试启动 Channel GOAWAY 排空，受理后返回 CLOSED，
排空启动失败则保留耗尽状态并返回失败。
新增的纯分配边界回归与真实 Channel 排空测试通过；该版本随后补上链接层游标注入，
直接验证耗尽后的 GOAWAY 与排空状态转换。

dev.41 将流 ID helper 移至独立内部编译单元，并用仅链接到 `test_stream_reset` 的
GNU ld wrapper 注入真实 Channel 的耗尽状态：`tr_stream_open()` 返回 CLOSED，真实对端
收到 GOAWAY，Channel 从 DRAINING 转为 DRAINED。此测试没有在生产代码加入测试开关；
`test_stream_reset` 与 `test_channel_drain` Release 套件通过。ERR-B 的 RESET/FIFO/代次
组合及 #242 其余验收仍未完成。

dev.42 修复 dev.41 CI TSan 构建暴露的源码清单遗漏：独立编译 V2 TCP Endpoint 的测试
目标也需链接 `channel_stream_id.c`。本地 TSan 下该目标和 `test_stream_reset` 均构建、运行
通过；dev.42 的 GitHub 检查全部通过。该修复不改变运行逻辑。

dev.43 为 #242/ERR-A 增加真实双 Reactor/Channel 的配额失败隔离回归。测试在同一
Channel 上保持第二条 RPC 调用活动，通过仅发送第一条调用的 MESSAGE 固定头，触发服务器
为后续正文预约缓存时的硬预算拒绝；服务器只终结该调用，上层夹具发送 `TR_ERR_NOMEM`
RESET 后客户端收到 `RESOURCE_EXHAUSTED`。释放预算后，第二条调用继续收发并以 OK 完成；
两端终局回调各一次且接收缓存归零。此测试覆盖 RAW 不完整记录与单流隔离，不覆盖压缩、
信用压力和连接级组合故障；#242 与 V2 整体验收仍未完成。详见
[配额失败隔离回归记录](docs/reviews/rpc_v2_quota_failure_isolation_20261025.md)。
dev.43 的全部 GitHub 检查均已通过。

dev.44 为 #242/ERR-B 增加 RESET 墓碑连接代次回归：旧连接完成双向 RESET 后退役，测试在
同一 Reactor 槽位安装新代次并完成 Channel 握手；新连接上对旧流 ID/原因发送 RESET 时，旧
墓碑不得匹配，未知流仍按协议错误使新连接关闭。此回归补充既有墓碑 FIFO 淘汰和 Stream 槽
代次复用测试，只覆盖共享 Channel 的一次连接替换，不改变生产代码或线协议；ERR-B 其余
RESET/FIFO 交错及 ERR-C 至 ERR-E 仍未完成。详见
[RESET 墓碑连接代次回归记录](docs/reviews/rpc_v2_reset_tombstone_connection_generation_20261025.md)。

dev.45 为 #242/ERR-B 增加 split Channel 的 RESET 墓碑物理连接身份回归：CONTROL 上完成的
双向 RESET 墓碑不能吸收 BULK 连接上相同流 ID/原因的 RESET；该未知帧只关闭 BULK，CONTROL
仍可继续接纳并完成新流。Release 与 TSan 下 `test_stream_reset` 全目标通过。该切片只增加测试，
不改变生产代码；ERR-B 的其他 RESET/FIFO 交错及 ERR-C 至 ERR-E 仍待完成。详见
[split 连接 RESET 墓碑回归](docs/reviews/rpc_v2_reset_tombstone_split_connection_20261025.md)。

dev.46 为 #242/ERR-C 增加 RESET 与迟到载荷释放组合回归：双向 RESET 完成后才释放旧流已交付
载荷，测试确认不再为退役流补发 WINDOW_UPDATE；同 Channel 的另一条流仍能收发并正常终止。
Release 与 TSan 下完整 `test_stream_reset` 均通过。该切片只增加测试，不改生产代码；ERR-C 的其他
信用与组合场景及 ERR-A/B/D/E 仍待验收。详见
[RESET 后迟到载荷释放与信用记录](docs/reviews/rpc_v2_reset_late_payload_credit_20261025.md)。

dev.47 为 #242/ERR-A 增加真实 TCP/Channel/RPC Endpoint 语义错误隔离回归：客户端在有效 OPEN 后
发送第二条合法编码但顺序非法的 OPEN，只有该调用以 `INVALID_ARGUMENT` 终结；随后同一 TCP 连接
上的新调用仍以 OK 完成，双方 Reactor 连接仍为 ACTIVE。服务端 Endpoint 接管 Channel 事件回调，
因此服务端连接状态直接查询验证。Release 与 TSan 下完整
`test_rpc_record_endpoint_tcp` 均通过。此测试补强生产 C 路径的一项映射证据；完整 ERR-A 矩阵仍待
验收。详见[真实 RPC 语义错误隔离记录](docs/reviews/rpc_v2_semantic_error_isolation_20261025.md)。

dev.48 为 #242/ERR-A 增加真实 TCP 连接级错误关闭回归：两条调用已被服务端业务接收后，客户端发送
头 CRC 错误帧；客户端与服务端的 Reactor 连接均退役，两个 Endpoint 调用各以 `UNAVAILABLE` 终结一次，
且没有接受成功 FINAL。Release 与 TSan 下完整 `test_rpc_record_endpoint_tcp` 均通过。该用例补上一个
不可恢复线帧错误的真实生产路径证据；其他连接错误、资源压力与组合矩阵仍待验收。详见
[真实 RPC 连接级错误关闭记录](docs/reviews/rpc_v2_connection_error_termination_20261025.md)。

dev.49 为 #242/ERR-A 补上压缩解码工作区配额隔离回归：第一条 Zstandard MESSAGE 在服务器工作区预算不足时
以 `RESOURCE_EXHAUSTED` 终结，经 RESET 通知对端；同一共享 Channel 的 CONTROL/BULK lane 保持 UP。释放预算后，
第二条调用成功解码压缩正文并正常完成。Release 与 TSan 下完整 `test_rpc_record_stream` 均通过。该用例覆盖
一个压缩解码配额失败与恢复组合；其他资源/信用和连接组矩阵仍待验收。详见
[压缩解码配额同伴隔离记录](docs/reviews/rpc_v2_compressed_quota_peer_progress_20261025.md)。

dev.50 为 #242/ERR-A 补强重复 OPEN 语义错误隔离：让同一 TCP 连接上的已活动同伴流进入业务 MESSAGE，
并在 gate 中暂停处理；注入另一流的重复 OPEN 后，错误流以 `INVALID_ARGUMENT` 唯一终结，同伴仍未终结且
两端连接保持 ACTIVE。随后释放 gate，同伴以 OK 唯一终结；错误发生后新调用也仍可成功。GCC Release 与 TSan
下完整 `test_rpc_record_endpoint_tcp` 均通过。该用例补足 dev.47 只验证错误后新调用的时序证据，ERR-A 全分类
矩阵及 ERR-B/C/D/E 仍待验收。详见
[活动同伴流语义错误隔离记录](docs/reviews/rpc_v2_semantic_active_sibling_isolation_20261025.md)。

dev.51 为 #242/ERR-A 增加业务拒绝期间的活动同伴隔离回归：方法 0 返回 `PERMISSION_DENIED` 时，同一 TCP
连接上的方法 1 调用已在业务 MESSAGE 中暂停；拒绝调用唯一终结期间，同伴保持活动且连接保持 ACTIVE。释放 gate
后，同伴以 OK 唯一终结。GCC Release 与 TSan 下完整 `test_rpc_record_endpoint_tcp` 均通过。该切片验证一项
业务终局与同伴交错，不代表 ERR-A 全分类矩阵或 ERR-B/C/D/E 已验收。详见
[业务拒绝活动同伴隔离记录](docs/reviews/rpc_v2_business_rejection_active_sibling_20261025.md)。

dev.52 为 #242/ERR-A 增加路由拒绝期间的活动同伴隔离回归，覆盖未知方法 `UNIMPLEMENTED` 和非法 OPEN 元数据
`INVALID_ARGUMENT`。拒绝调用在业务 Dispatch 前以错误 FINAL 唯一终结；同一 TCP 连接上已活动的同伴保持暂停且
连接保持 ACTIVE，释放 gate 后同伴以 OK 唯一完成。GCC Release 与 TSan 下完整 `test_rpc_record_endpoint_tcp`
均通过。该切片补充两类路由错误，不代表 ERR-A 完整分类矩阵或 ERR-B/C/D/E 已完成。详见
[路由拒绝活动同伴隔离记录](docs/reviews/rpc_v2_router_rejection_active_sibling_20261025.md)。

dev.53 为 #242/ERR-D 增加公开连接组双 DATA 成员 drain 回归：两个真实 DATA 帧经不同物理连接发送；Client/Server
开始排空后拒绝新 Offer/Ready，任一传输未释放时双方仍不能完成排空。释放两个传输后 Client 可完成本地排空，
但 Server 会继续等待两条 DATA 成员由 Client 关闭；成员全部退出后才报告 Server drain 完成，最终组/连接/传输计数归零。
同时把 SDK Client 测试清理改为对契约允许的 `TR_AGAIN` 做有界重试，其他错误仍立即失败。GCC Release 与 TSan 下
完整 `test_connection_group_facade`、`test_rpc_v2_sdk` 均通过。该切片不覆盖发送中/压缩中排空、连接故障和完整组合矩阵。
详见[双 DATA 成员连接组排空记录](docs/reviews/rpc_v2_group_multi_data_drain_20261025.md)。

dev.54 在双成员场景中补上 DATA 发送在途与排空的交错：测试将已接受的数据帧暂时阻塞在 Reactor 发送队列，逐个释放客户端 transfer；即使活动 transfer 已归零，Client drain 仍因 `send_bytes_inflight` 保持未完成。放开发送后，服务端收到消息、发送字节归还，Client drain 才完成；服务端仍等待两条连接关闭。GCC Release 与 TSan 下完整 `test_connection_group_facade` 均通过。该用例不覆盖压缩任务在途、成员故障/重连和完整多组竞争矩阵。详见
[DATA 在途发送排空记录](docs/reviews/rpc_v2_group_data_tx_drain_20261025.md)。

dev.55 增加 Zstandard 压缩任务在途取消与 drain 交错：worker 完成压缩但暂停在结果回调前；Client/Server 开始 drain 后释放客户端 transfer，Client 仍因压缩任务持有的输入字节未归还而等待。放开 worker 后，取消结果收敛且不会向服务端发送已撤销 transfer 的消息；输入字节归还后 Client drain 完成，Server 继续等待 DATA 连接关闭。GCC Release 与 TSan 下完整 `test_connection_group_facade` 均通过。该用例不覆盖压缩完成后已编码 wire 在 Reactor TX 中的发送排空。详见
[Zstandard 任务取消排空记录](docs/reviews/rpc_v2_group_compression_cancel_drain_20261025.md)。

dev.56 修复连接组服务端 drain 与 Zstandard DATA 完成回调的冲突：现有 DATA 成员上的压缩消息在 drain 期间仍按连接/路由代次校验后交付，不再因 listener 的 drain 标志被静默丢弃。回归先暂停客户端压缩完成回调，再开始双端 drain 并阻塞压缩 wire 的 socket 发送；放开发送后服务端收到消息，Client 在 wire 归还后完成 drain，Server 继续等待连接关闭。GCC Release 与 TSan 下完整 `test_connection_group_facade` 均通过。详见
[压缩 wire 在途排空修复记录](docs/reviews/rpc_v2_group_compressed_wire_drain_20261025.md)。

dev.57 为 #242/ERR-D 增加双 DATA 成员故障与替代回归：中止其中一条真实物理连接后，旧 transfer 失效、另一成员继续发送；同一 group/epoch 上重新 Offer/Ready 后，替代成员使用新物理连接并成功交付。随后释放剩余亲和、完成 Client drain，并在所有成员关闭后完成 Server drain，最终计数归零。GCC Release 与 TSan 下完整 `test_connection_group_facade` 均通过；变异验证确认遗漏客户端 transfer 失效会触发回归断言。该切片不覆盖多个组并发故障或完整 ERR-D 竞争矩阵。详见
[双 DATA 成员故障与重建回归](docs/reviews/rpc_v2_group_member_failure_replacement_20261025.md)。

dev.58 为 #242/ERR-D 增加两个并行连接组的故障隔离回归：服务端单 shard 同时承载不同 group/epoch；A 组 DATA 成员故障并替代期间，B 组的 DATA 与活动 transfer 保持可用。关闭 A 组后，B 组仍成功发送并独立完成 drain，最终服务端组/连接/DATA/transfer 计数归零。GCC Release 与 TSan 下完整 `test_connection_group_facade` 均通过。该切片不覆盖多组同时故障与竞争压力矩阵。详见
[双组成员故障隔离记录](docs/reviews/rpc_v2_group_multi_group_failure_isolation_20261025.md)。

dev.59 扩展双组真实 TCP 故障回归：两个 Client Reactor 通过测试屏障并发中止各自 DATA 成员，断言两组 transfer 同时失效、服务端只保留 CONTROL 组状态；随后分别替代两个物理成员并交付消息，关闭一组后另一组仍可继续发送和独立 drain。GCC Release 与 TSan 下完整 `test_connection_group_facade` 均通过。该切片不覆盖多轮快速替代和资源压力矩阵。详见
[双组并发故障与替代记录](docs/reviews/rpc_v2_group_simultaneous_failure_replacement_20261025.md)。

dev.60 在 dev.59 场景中加入第二轮双组并发故障与替代：第一轮替代成员成功交付后立即再次失效，两组旧 transfer 再次变为 stale；第二代新物理成员分别恢复并交付。关闭 A 组后 B 组仍可发送，最终独立排空。完整门面测试 Release 连续 8/8 与 TSan 均通过。验证同时修正两个压缩 drain 测试把合法 `TR_AGAIN` 销毁结果当作终态的问题，改用现有有界重试辅助；不改生产代码。压力资源竞争和完整 ERR-D 矩阵仍待覆盖。详见
[双组重复故障与快速替代记录](docs/reviews/rpc_v2_group_repeated_failure_replacement_20261025.md)。

dev.61 为 #242/ERR-E 增加 RESET 墓碑固定预算回归：用带真实 `tr_memory_budget` 的容量为 2 的
Channel 完成三条流的 RESET，使最早墓碑被 FIFO 淘汰；在墓碑仍驻留时重复 RESET 必须被吸收，
淘汰后旧 RESET 必须按未知流错误关闭连接。整个周转过程中预算当前值和峰值保持在 Channel 创建时的
固定值，且没有预算拒绝；销毁后当前值归零。GCC Release 与 TSan 下完整 `test_stream_reset` 均通过。
这验证墓碑不按 RESET 数量增量占用预算，不覆盖高频 CPU 成本及更大规模资源压力矩阵。详见
[RESET 墓碑固定预算回归记录](docs/reviews/rpc_v2_reset_tombstone_fixed_budget_20261025.md)。

dev.62 为 #242/ERR-E 将重复 RESET 墓碑查询从 FIFO 全表扫描改为固定开放寻址索引，索引容量为不小于
`2 * max_streams` 的 2 次幂，使用 `(物理连接代次, stream_id)` 复合键、32 位环槽编码并复用墓碑中的流 ID。
这样 split Channel 两条连接各自前进的 OPEN ID 前沿仍可产生相同 ID 墓碑。FIFO 覆盖时先删除旧键并修复探测簇，
再登记新槽；命中后仍精确核对 RESET 原因。索引与活动流索引合并为一次固定分配，并计入 Channel 预算；
`max_streams=40000` 时新增固定索引空间为 512 KiB。Release、ASan/UBSan、TSan 下 RESET、索引、Channel
构造事务和排空目标均通过；40k 条目探测回归限制插入与查询总探测数。碰撞最坏成本仍随表容量增长，长期高负载
及恶意碰撞压力仍待验证。详见[RESET 墓碑查询索引记录](docs/reviews/rpc_v2_reset_tombstone_index_20261025.md)。

dev.63 修复 dev.62 完整 CI 构建暴露的独立 TCP Endpoint 测试链接遗漏：该测试手动枚举生产对象，现将墓碑索引对象加入对象闭包；同步移动 GCC 已知告警基线的位置，诊断类别和文本未变。该修复不改变运行时行为，PR #324 的 GitHub 检查 21/21 通过。详见
[RESET 索引测试链接修复记录](docs/reviews/rpc_v2_reset_tombstone_build_linkage_20261025.md)。

dev.64 为 #242/ERR-E 增加真实 Channel 的长时间 RESET 墓碑 FIFO 周转回归：容量为 4 的同一
socketpair/Reactor/Channel 连续完成 128 次双向 RESET，覆盖墓碑环 32 轮完整覆盖；当前最老与最新驻留墓碑
可吸收重复 RESET，最早已淘汰墓碑的重复 RESET 则按未知流错误关闭连接。Release、TSan、ASan/UBSan 下
`test_stream_reset` 均通过，dev.64 的两组 GitHub CI 与 RPC smoke 检查 21/21 通过。本切片只增加集成测试，不改生产行为；恶意碰撞压力和更大规模连接负载仍待验证。详见
[RESET 墓碑长周转回归记录](docs/reviews/rpc_v2_reset_tombstone_long_turnover_20261025.md)。

dev.65 为 #242/ERR-E 增加墓碑索引确定性碰撞压力回归：在 512 槽索引中构造 256 个落在同一 home bucket
的键，反复淘汰并复用 FIFO 环槽 256 次，逐项检查插入/查询探测次数、全部驻留键和已淘汰键。临时关闭大表
向后移位的变异会在新断言处失败，恢复后测试通过。Release、TSan、ASan/UBSan 下
`test_reset_tombstone_index` 均通过，dev.65 的两组 GitHub CI 与 RPC smoke 检查 21/21 通过。本测试只验证 256 路合成碰撞与删除修复，不代表 40k 最大容量下的最坏 CPU 成本；不改生产行为。详见
[RESET 墓碑索引碰撞压力回归记录](docs/reviews/rpc_v2_reset_tombstone_collision_stress_20261025.md)。

dev.66 将墓碑索引的顺序流 ID 周转扩展到最大配置：在 `max_streams=40000` 的 131072 槽表中完成 40k 次
删除/插入，逐项检查淘汰键不可查、重用键映射到原环槽，并把删除、重插及最终查询的总探测数限制在 1.92m。
跳过大表簇修复的临时变异在最大容量删除断言失败；恢复后 Release、TSan、ASan/UBSan 下
`test_reset_tombstone_index` 均通过。本测试不设时钟门槛，也不代表真实 40k 并发网络连接的 CPU 基准。详见
[RESET 墓碑索引最大容量 FIFO 周转记录](docs/reviews/rpc_v2_reset_tombstone_max_capacity_turnover_20261025.md)。

dev.67 在相同最大配置下加入对抗碰撞周转：131072 槽索引装满 40000 个历史键，其中 256 个键故意映射到同一
home bucket；连续淘汰并复用这 256 个墓碑槽，删除后、插入前确认碰撞簇中的幸存键仍可查，最后验证其余历史键、
当前驻留键和全部淘汰键的映射。跳过大表簇修复的变异在“复用前幸存键”断言失败；恢复后 Release、TSan、
ASan/UBSan 下 `test_reset_tombstone_index` 均通过。探测计数不包括删除时 backward-shift 扫描，且本回归
不等同于 40k 键同 home 的最坏 CPU 成本或真实网络压力。详见
[RESET 墓碑索引最大容量碰撞周转记录](docs/reviews/rpc_v2_reset_tombstone_max_capacity_collision_20261025.md)。

dev.68 补上 #242/ERR-A、ERR-C 的配额期控制面进展回归：真实 RPC Record Stream 在压缩解码工作区不足、目标流已 RESET
且预算预留仍保持期间，持续验证 CONTROL keepalive PING/PONG 往返；预算释放后，同 Channel 压缩同伴请求仍能完成。
GCC Release、TSan、ASan/UBSan 下 `test_rpc_record_stream` 均通过。仅增加测试、记录和版本号，不改生产路径。详见
[压缩配额期间控制帧进展记录](docs/reviews/rpc_v2_compressed_quota_control_progress_20261025.md)。

dev.69 将配额期间控制帧进展回归扩展到 RAW 重组配额不足：预算全部预留、失败流以 RESET 终结期间，CONTROL PING/PONG
仍往返；释放预算后同 Channel 同伴调用完成。RAW 重组与 Zstandard 解码两种配额路径共享同一 keepalive 断言。GCC Release、
TSan、ASan/UBSan 下 `test_rpc_record_stream` 均通过。仅增加测试、记录和版本号，不改生产路径。详见
[重组与解码配额控制面进展记录](docs/reviews/rpc_v2_receive_quota_control_progress_20261025.md)。

dev.70 为 #242/ERR-E 增加真实 Channel 最大容量墓碑周转：Release 与 ASan/UBSan 下，配置
`max_streams=40000` 并真实完成 40,000 次 OPEN/双向 RESET，确认 FIFO 两端驻留墓碑可吸收重复 RESET；新增一条后，最早墓碑
被淘汰并按未知流错误关闭连接。TSan 下用容量 5,000 填满并验证相同边界，以控制单测耗时；最大容量的索引层 TSan 压力仍由
`test_reset_tombstone_index` 覆盖。本切片只增加集成测试和记录，不改生产路径。详见
[真实 Channel 最大容量 RESET 墓碑周转记录](docs/reviews/rpc_v2_reset_tombstone_channel_capacity_20261025.md)。

dev.71 修复 #242/ERR-A 连接组 listener draining 接受边界：同步授权的 DATA route 前导若在 listener 开始排空后才完整，现会拒绝连接并撤销对应预留；此前 CONTROL 和异步授权路径已有同类检查。真实 listener 回归先发送 DATA_OFFER、保持 DATA 前导最后一字节未发送、开始 drain 后再补齐，确认拒绝计数增加而 DATA 接受计数不变。Release、TSan、ASan/UBSan 下 `test_pipeline_listener` 与连接组门面目标均通过。详见
[连接组 DATA 接受排空边界记录](docs/reviews/rpc_v2_group_data_admission_drain_20261025.md)。

dev.72 扩展连接组 listener 排空接受边界回归：一个新 CONTROL route 与一个已预留 DATA route 都在 begin_drain 前只发送不完整前导；drain 后补齐时，两类连接均被拒绝，CONTROL/DATA 接受计数保持不变，route rejection 增加两次。GCC Release、TSan、ASan/UBSan 下 `test_pipeline_listener` 均通过。本切片只扩展集成测试、文档和版本号。详见
[CONTROL/DATA pending route 排空回归](docs/reviews/rpc_v2_group_drain_pending_routes_20261025.md)。

dev.73 扩展连接组跨成员 draining 回归：同一 CONTROL 下两个不同 DATA member route 和另一个有效 CONTROL route 同时保持 pending，分别在 begin_drain 前只发送 route 前导的 N-1 字节；drain 后补齐时三条 route 均被拒绝，CONTROL/DATA 接受计数不变、route rejection 增加三次，pending socket 全部关闭。GCC Release、TSan、ASan/UBSan 下 `test_pipeline_listener` 均通过。本切片只扩展集成测试、文档和版本号。详见
[双 DATA 成员 pending route 排空回归](docs/reviews/rpc_v2_group_drain_multi_member_pending_20261025.md)。

dev.74 修正 RESET 墓碑索引删除成本计量：`remove` 的测试探测计数现包含定位、探测簇校验与 backward-shift 的全部槽位检查；3 项环绕簇删除为 7 次检查，256 路同 home 碰撞删除为 513 次。最大容量碰撞周转的总工作上界现计入删除扫描。Release、TSan、ASan/UBSan 下 `test_reset_tombstone_index` 与 `test_stream_reset` 均通过；40k 同 home 碰撞和真实高频延迟仍待独立验收。详见
[RESET 墓碑删除成本计量记录](docs/reviews/rpc_v2_reset_tombstone_remove_cost_accounting_20261025.md)。

dev.75 为 #242/ERR-E 在 `max_streams=40000`、131072 槽下增加同 home 最坏输入：代数反解生成 40,000 个句柄非零且互异的复合键，验证尾部查找 40,000 次、簇首删除 80,001 次、复用槽位后的插入与命中各 40,000 次，以及旧键 stale 查询 40,001 次。Release、TSan、ASan/UBSan 下 `test_reset_tombstone_index` 均通过；push CI、PR CI 和 RPC benchmark smoke 全部通过。这是索引层合成状态，不测量真实连接的持续高频 CPU/延迟。详见
[RESET 墓碑索引最大容量同 home 成本记录](docs/reviews/rpc_v2_reset_tombstone_same_home_max_capacity_20261025.md)。

dev.76 为 #242/ERR-E 在真实 socketpair/Reactor/Channel `max_streams=40000` 周转中记录 RESET 往返 P50/P95/P99，以及包含 OPEN/RESET 的循环墙钟、进程 CPU 时间与吞吐。三次 GCC Release 运行的 RESET 延迟中位数为 P50 1151.42 µs、P95 1395.03 µs、P99 1712.33 µs；各次结果波动明显。TSan 使用 5,000 条目守住运行时限，Release 与 ASan/UBSan 使用 40,000 条目。该项是本机测试夹具观测，没有隔离生产微基准、跨机器基线或性能阈值，不构成持续高频负载验收。详见
[真实 Channel RESET 高频成本初测](docs/reviews/rpc_v2_reset_tombstone_high_frequency_cost_20261025.md)。

dev.77 为 #241 增加真实连接组慢消费者隔离回归：两个 transfer 分属不同 DATA 物理连接；第一成员的应用消息继续持有接收缓冲时，第二成员仍独立进入服务端消息回调并完成释放，测试还核对两个路由确属不同连接。Release 连续 5/5、TSan、ASan/UBSan 下完整 `test_connection_group_facade` 均通过。该用例证明单条持有消息不会阻断另一成员，不覆盖预算耗尽、压缩或持续饱和压力矩阵。详见
[多 DATA 成员慢消费者隔离回归](docs/reviews/rpc_v2_group_slow_consumer_multi_data_20261025.md)。

dev.78 为 #241 扩展连接组慢消费者回归：两条不同 DATA 物理连接都发送 Zstandard 消息；第一条解压消息被应用接管并持有时，第二条仍完成解压、正文校验、交付和释放。测试确认两次 DATA 提交均实际带压缩标志、两条路由连接代次不同，并在第二条完成后复核第一条缓冲仍有效。GCC Release 连续 5/5、TSan、ASan/UBSan 下完整 `test_connection_group_facade` 通过。本切片未耗尽全局预算，也不覆盖 RAW/Zstandard 混合、多个应用持有者或持续饱和公平性。详见
[压缩与多 DATA 慢消费者隔离回归](docs/reviews/rpc_v2_compressed_group_slow_consumer_20261025.md)。

dev.79 继续验证 #241 的混合 DATA 路径：第一成员的 Zstandard 解压消息由应用持有期间，第二成员先完成另一条 Zstandard 消息，再交付一条 RAW 消息；两次进展之后首条缓冲仍完整。Release 连续 5/5、TSan、ASan/UBSan 下完整 `test_connection_group_facade` 通过。该用例不耗尽 Server 全局预算，预算耗尽、多个持有消息和持续饱和公平性仍待验收。详见
[压缩持有期间 RAW 同伴进展回归](docs/reviews/rpc_v2_mixed_data_slow_consumer_20261025.md)。

dev.80 将 Zstandard 变换工作区上限从每任务 1 MiB 调整为 4 MiB，并在消息预算公式中按压缩任务槽计入；旧上限会让 1 MiB 窗口配置下的大正文编码以 `TR_MESSAGE_TRANSFORM_LIMIT` 结束。新增真实 TCP 连接组回归：应用持有 1 MiB 解压正文时，只配置一个压缩解码任务槽的服务端仍可从另一 DATA 成员接收并释放 RAW 小消息，之后首缓冲仍逐字节完整。Release `test_connection_group_facade` 连续 5/5、TSan、ASan/UBSan 和完整 91 项测试通过。该用例未耗尽全局预算，也不覆盖多个持有者和持续饱和公平性。详见
[大正文压缩持有期间 RAW 同伴进展回归](docs/reviews/rpc_v2_large_decode_slow_consumer_20261025.md)。

dev.81 在真实 TCP `pipeline_listener` 测试中让一条 Zstandard 解码输出被应用持有，再用受控预算预约填满监听器消息预算；账目精确到达上限时，另一条不同 DATA 成员的 RAW 消息仍到达回调，且首条输出保持有效。释放测试预约和持有输出后，预算依次恢复并最终归零。Release 完整 91 项测试、该定向测试连续 5 次、ASan/UBSan（含泄漏检查）和 TSan 均通过。该组件测试不是 Server 自然耗尽派生全局预算的证据，也不覆盖多持有者或持续饱和公平性；本次不改生产行为、API/ABI 或 wire。详见
[监听器消息预算满额时 RAW 同伴进展回归](docs/reviews/rpc_v2_full_budget_raw_peer_20261025.md)。

dev.82 扩展公开 Server 慢消费者回归：应用同时持有六条真实 Zstandard 解码输出，正文合计约 5.06 MiB；这些缓冲都仍有效时，另一 DATA 成员上的 RAW 消息继续交付。本次不手工预约消息预算，也不改生产运行时、API/ABI 或 wire。该测试证明多条自然产生的应用持有消息下同伴可进展，但没有观察并断言 Server 全局预算精确满额，也不覆盖持续饱和公平性。Release `test_connection_group_facade` 连续 5/5、ASan/UBSan、TSan 与完整 Release 91 项测试通过。详见
[公开 Server 多消息持有期间 RAW 同伴进展回归](docs/reviews/rpc_v2_multi_hold_raw_peer_20261025.md)。

dev.83 将同一真实公开 Server 场景扩展为有界 RAW 突发：六条 Zstandard 解码输出仍由应用持有时，另一 DATA 物理连接连续交付并校验 64 条 RAW 消息；测试确认整个突发保持在第二成员的连接代次上，结束后再次检查所有已持有缓冲。该结果证明有界突发中的同伴进展，不代表持续饱和公平性或全局预算精确耗尽。Release `test_connection_group_facade` 连续 5/5、ASan/UBSan、TSan 和完整 Release 91 项测试通过；只改测试、记录和版本，不改生产运行时、API/ABI 或 wire。详见
[公开 Server 慢消费者期间 RAW 突发回归](docs/reviews/rpc_v2_raw_burst_slow_consumer_20261025.md)。

dev.84 在真实公开 Server 的派生消息预算下自然持有 36 条 1 MiB Zstandard 解码输出；下一条真实解码任务因可用字节不足被预算拒绝一次，相关压缩 DATA 连接关闭。此时 budget current 为 42,147,072、limit 为 46,908,160，剩余字节已小于下一任务所需的 1 MiB 输出加 4 MiB 工作区，测试不要求 current 数值等于 limit。压缩成员关闭后，另一 DATA 成员仍交付 64 条 RAW 消息，持有正文完整。Release 目标连续 5/5、ASan/UBSan、TSan 和完整 91 项测试通过。只改测试、记录和版本，不改生产运行时、API/ABI 或 wire；持续饱和公平性仍未验收。详见
[公开 Server 自然预算拒绝期间 RAW 同伴进展](docs/reviews/rpc_v2_server_natural_budget_rejection_20261025.md)。

dev.85 新增双 DATA 并发负载回归：一成员连续提交 32 条 1 MiB Zstandard 消息，应用保留前五条解码正文并释放后续输出；另一成员以最多 32 条 RAW 在途消息持续补发。测试记录首尾 Zstandard 回调之间的 RAW 完成数，要求至少 32 条，并验证所有持有正文及两条不同物理路由。七次 GCC Release 观测的并发区间为 1,372–2,598 条 RAW；ASan/UBSan、TSan 和完整 Release 91 项套件也通过。该固定批次验证并发来源下的有限进展，不定义公平延迟/吞吐阈值，也不证明长期持续饱和公平性。只改测试、记录与版本，不改生产运行时、API/ABI 或 wire。详见
[公开 Server 并发 RAW/Zstandard 进展回归](docs/reviews/rpc_v2_server_concurrent_raw_zstd_progress_20261025.md)。

dev.86 修正 dev.85 首轮 GCC Debug CI 暴露的测试容量配置：原场景只给服务端配置 8 个压缩任务槽，却连续发送 32 条压缩消息，慢速 Debug 下服务端按容量上限关闭 DATA 通道。现将该场景的任务容量设为 32，与固定批次相匹配；这只改变测试配置，不改生产代码。GCC Debug/Release 完整套件均 91/91；Release 定向目标连续复跑、ASan/UBSan 与 TSan 定向目标通过。七次 Release 观测中，压缩窗口内 RAW 交付为 1,489–2,842 条；Debug 四次为 154–273 条。详见
[公开 Server 并发 RAW/Zstandard 进展回归](docs/reviews/rpc_v2_server_concurrent_raw_zstd_progress_20261025.md)。

dev.87 将固定 32 条压缩负载扩展为四批、共 128 条 1 MiB Zstandard 消息；每批完成回调后才提交下一批，服务端压缩任务容量保持 32。另一 DATA 成员在全程维持最多 32 条 RAW 在途消息，测试分别断言每批首末压缩回调之间至少交付 32 条 RAW，并逐字节复核仍被应用持有的五条解码正文。GCC Release 定向目标连续 7/7，四批间 RAW 进展总和为 6,397–11,011；GCC Debug 连续 4/4，为 2,150–3,231；Clang Release、ASan/UBSan 与 TSan 定向目标通过，完整 GCC Release 套件 91/91。此切片扩展有限窗口证据，不定义公平延迟/吞吐阈值，也不证明长期持续饱和公平性。仅改测试、文档与版本号，不改生产运行时、公开 API/ABI 或 wire。详见
[公开 Server 并发 RAW/Zstandard 进展回归](docs/reviews/rpc_v2_server_concurrent_raw_zstd_progress_20261025.md)。
dev.88 将同一公开 Server 场景扩展到八批、共 256 条 1 MiB Zstandard 消息，RAW 在途窗口与逐批最低进展条件不变；记录每批压缩提交至全部回调完成的墙钟时间。GCC Release 定向目标六次通过，观测总阶段为 390–433 ms、每批 RAW 为 1,718–3,171 条；GCC Debug、Clang Release、ASan/UBSan（含泄漏检查）和 TSan 目标均通过，完整 GCC Release 套件 91/91（130.110 秒）。这些时间和计数只作本机诊断，不作为 SLA；八批仍是有限压力，不证明长期公平性。仅改测试、文档和版本号，不改生产运行时、公开 API/ABI 或 wire。详见
[公开 Server 并发 RAW/Zstandard 进展回归](docs/reviews/rpc_v2_server_concurrent_raw_zstd_progress_20261025.md)。

dev.89 增加真实 Split Channel 跨 lane 的 RPC Record Zstandard 正文发送回归：暂停服务端 BULK 接收，使 16 KiB 压缩正文耗尽该流窗口；同一 Channel 的 CONTROL 小压缩正文仍先到达，恢复 BULK 接收后大消息完整交付。GCC Release 完整 `test_rpc_record_stream` 连续 5/5，GCC Debug 完整目标也通过；五次 Release 均观察到 BULK `4144/4144` 字节停滞、CONTROL 先完成、BULK 恢复到 16,494 字节。只增加测试、记录和版本号，不改生产运行时、公开 API/ABI 或 wire；Channel `TR_FRAME_F_COMPRESSED` 分帧、持续饱和 SLA、共享 TX 池耗尽和同 lane 多流公平仍待验收。详见
[Split Channel 压缩窗口跨 lane 同伴进展回归](docs/reviews/rpc_v2_compressed_tx_cross_lane_progress_20261025.md)。

dev.90 修复通用 Channel 的 Split Channel Zstandard DATA 跨 lane 进展：此前压缩发送器把 `LANE_BULK` 带进 DATA 标志，接收器按协议校验将 BULK 流 RESET 为 `TR_ERR_STATE`。现由 Stream 登记和连接代次确定 lane，DATA 只携带分片与压缩标志。真实 TCP 回归暂停服务端 BULK 接收，确认 300 字节 BULK 消息在 128/128 信用处停住时，CONTROL 小压缩消息仍先交付；恢复 BULK 后正文完整交付，压缩 DATA 发送推进至 330 字节。GCC Release 完整 `test_transport` 连续 5/5，GCC Debug 完整目标通过，GCC Release 全套 91/91。该修复改变生产 Channel 内部标志生成，不改变公开 API/ABI 或 wire 格式；长期公平 SLA、共享 TX 池耗尽与更广取消/预算组合仍待验收。详见
[Split Channel 通用压缩 DATA 跨 lane 信用进展回归](docs/reviews/channel_compressed_data_split_lane_progress_20261025.md)。

dev.91 扩展通用 Channel 压缩 DATA 覆盖到 Shared Channel 的逻辑 BULK Stream：慢 CONTROL 压缩流因窗口不足停住时，同一物理连接上的小 BULK 压缩流仍完整交付；随后 CONTROL 流也恢复并通过正文校验。该回归锁定 dev.90 移除 DATA lane 标志后的共享连接路由行为。GCC Release 完整 `test_transport` 连续 5/5、GCC Debug 完整目标通过、GCC Release 全套 91/91。只改测试、文档与版本号，无额外生产代码/API/ABI/wire 变更；长期公平 SLA、共享 TX 池耗尽及更广预算/取消组合仍待验收。详见
[Shared Channel 逻辑 BULK 压缩 DATA 回归](docs/reviews/channel_compressed_data_shared_bulk_lane_20261025.md)。

dev.92 扩展 Split Channel 压缩信用回归为双向同时发送：Client 的 BULK 大压缩 DATA 在 Server 端停于 128/128 时，Server 通过同一 Channel 的 CONTROL Stream 回发压缩消息，Client 先完整解码并校验回发正文；恢复 BULK 接收后，大正文继续推进并完整交付。GCC Release 完整 `test_transport` 连续 5/5、GCC Debug 完整目标通过，Release 全套 91/91（136.923 秒）。该项只增加测试和文档，不改生产代码、公开 API/ABI 或 wire；跨 lane 双向进展仅覆盖此有界交错，持续饱和 SLA、共享 DATA TX 池耗尽与更广预算/取消组合仍待验收。详见
[Split Channel 压缩 DATA 双向同时发送回归](docs/reviews/channel_compressed_data_bidirectional_cross_lane_20261025.md)。

dev.93 扩展 Shared Channel 的 DATA TX 池耗尽回归：Reactor 仅配置一个 DATA TX 项，BULK RAW Stream 在 TCP 写入受阻时占满池；CONTROL 压缩 Stream 在同一物理连接上登记容量就绪等待，池项归还后恢复发送并完整交付。客户端与服务端均核对两条 Stream 的逻辑 lane，接收完成后确认 TX 项归零。GCC Release 完整 `test_transport` 通过，完整测试套件 91/91（140.776 秒）。该项只增加测试、文档与版本号，不改生产代码、公开 API/ABI 或 wire；持续多发送者饱和公平性、消息等待界、吞吐阈值和更广预算/取消组合仍待验收。详见
[Shared Channel 跨 lane DATA TX 池耗尽恢复回归](docs/reviews/channel_compressed_data_tx_pool_cross_lane_20261025.md)。

dev.94 将 Split Channel 压缩进展回归扩展为双向 BULK 窗口同时耗尽：Client 与 Server 互发 300 字节 Zstandard 消息，各自在对端暂停接收后停于 128/128；窗口都停住期间，双向 CONTROL 压缩消息仍先交付并校验。分别恢复两端 BULK 接收后，双方正文均完整交付且发送进度到 330 字节。GCC Release 完整 `test_transport` 连续 5/5 通过，完整测试套件 91/91（143.845 秒）。该项只增加测试、记录和版本号，不改生产运行时/API/ABI/wire；多发送者持续公平性、消息延迟界、吞吐阈值及更广预算/取消组合仍待验收。详见
[Split Channel 双向窗口耗尽时的 CONTROL 进展回归](docs/reviews/channel_compressed_data_bidirectional_window_exhaustion_20261025.md)。

dev.95 扩展 Shared Channel 的 DATA TX 池耗尽覆盖为两个跨 lane 压缩等待者：BULK RAW Stream 占住唯一 DATA TX 项时，CONTROL 和另一条 BULK Stream 都提交压缩消息并等待容量通知；释放池项后，两条消息均恢复交付。测试按 Stream ID 验证两端 lane 和全部正文，不依赖重试定时器，并确认 TX 池归零。GCC Release 完整 `test_transport` 连续 5/5 通过，完整测试套件 91/91（143.453 秒）。该项只改测试、记录和版本号，不改生产运行时/API/ABI/wire；更多等待者下的持续公平性、消息等待界、吞吐阈值及更广预算/取消组合仍待验收。详见
[Shared Channel 多压缩等待者的 DATA TX 池恢复回归](docs/reviews/channel_compressed_data_tx_pool_multi_waiter_20261025.md)。

dev.96 在 Shared Channel 的唯一 DATA TX 项仍被 BULK RAW Stream 占用时，RESET
一个已完成压缩但仍等待 TX 项的 BULK Stream；测试确认 RESET 立即归还该流的压缩预算，
并且没有消费或占用 RAW 所持的 DATA 池项。解除阻塞后，CONTROL 和另一个 BULK 压缩等待者
都完成交付，被 RESET 的流只完成 RESET、不交付 DATA；RESET 两端收敛、TX 池与总消息预算
最终归零。GCC Release 完整 `test_transport` 连续 6/6 通过，项目全量测试 91/91 通过
（148.688 秒）。该项只改测试、记录和版本号，
不改生产运行时/API/ABI/wire；持续饱和公平 SLA、逐消息等待界、吞吐阈值、跨机器基线和
更广预算/取消矩阵仍待验收。详见
[共享 DATA TX 池等待期间 RESET 压缩发送者回归](docs/reviews/channel_compressed_data_tx_pool_reset_cancel_20261025.md)。

dev.97 将 Shared Channel 池满恢复回归扩展到多帧高熵 Zstandard 消息，验证 RESET
一个等待者后，CONTROL 与存活 BULK 流的首个压缩 DATA 帧会先后轮转。该用例首次暴露
DATA TX 池耗尽时调度游标越过尚未发送的等待者，导致同一 Stream 连续取得发送机会；修复后
池满分支保留该等待者为下次扫描起点。GCC Release 定向目标修复后连续 2/2 通过，项目全量
测试 91/91（53.237 秒），帧记录确认 RESET 流没有残留 DATA 且 TX 池和预算归零。详见
[Shared Channel 多帧压缩 DATA 池恢复公平轮转回归](docs/reviews/channel_compressed_data_tx_fairness_20261025.md)。

dev.98 修正连接组 TLS 集成测试的异步清理时序：撤销旧 TLS 代次并确认连接统计清零后，
Client owner 仍可能有短暂的销毁屏障待收敛；测试原先只调用一次 `tr_client_destroy`，将
可重试状态误报为失败。现按同文件已有的 Server 销毁助手约定，对 `TR_ERR_STATE`/
`TR_AGAIN` 做有界重试。GCC Debug `test_connection_group_tls/default` 修改后连续 6/6 通过。
详见
[连接组 TLS Client 异步销毁测试收敛](docs/reviews/connection_group_tls_client_destroy_retry_20261025.md)。

dev.99 将共享 DATA TX 池恢复的压缩发送公平性回归扩展为三个存活等待流：唯一池项仍由
BULK RAW 占用，测试先确认准备 RESET 的 BULK 压缩 Stream 自身触发池耗尽并登记共享连接的
DATA TX 可写观察，再提交另外三条压缩消息；池恢复后，CONTROL 两条与 BULK 一条
存活压缩流的首帧各出现一次，后续六帧按同序再轮转两轮。测试确认 RESET 流无 DATA、三条正文
完整交付，发送池与预算最终归零。GCC Debug 定向目标 6/6、Release 全量测试 91/91 通过。
本次仅扩展测试、记录和版本号，不改生产/API/ABI/wire。
详见
[Shared Channel 三存活压缩流公平轮转回归](docs/reviews/channel_compressed_data_tx_three_waiter_fairness_20261025.md)。

dev.100 将同一 Shared Channel 池恢复场景扩展到四条存活压缩流：测试先确认准备 RESET 的 BULK
Stream 自身遇到池耗尽，再提交四条同伴（CONTROL/BULK 各两条）。池恢复后，四条存活流的
FIRST 帧各出现一次，随后八帧按同序继续轮转两轮；RESET 流无 DATA，四条正文完整交付，发送池
和预算归零。GCC Debug 定向目标连续 6/6 通过，Release 全量结果见回归记录。本次只改测试、记录
和版本号，不改生产/API/ABI/wire。详见
[Shared Channel 四存活压缩流公平轮转回归](docs/reviews/channel_compressed_data_tx_four_waiter_fairness_20261025.md)。

dev.101 将四条存活压缩流的连续轮转检查扩展到 24 个成功 DATA 帧：确认四个 FIRST 帧来自不同
Stream，之后五轮续帧严格按同一顺序轮转；RESET 流无 DATA，原有正文交付与池/预算清零断言保留。
Release `test_transport/default` 连续 6/6 通过；临时停滞调度游标的变异按预期被新断言捕获。
本次只改测试、记录和版本号，不改生产/API/ABI/wire；长期 SLA、消息等待上界和更大存活流数仍未验收。
详见 [四存活压缩流六轮公平性回归](docs/reviews/channel_compressed_data_tx_six_round_fairness_20261025.md)。

dev.102 将同一池满恢复场景扩展到六条存活压缩 Stream（CONTROL/BULK 各三条），并检查 36 个
连续成功 DATA 帧的六轮轮转；RESET 等待者仍无 DATA，六条压缩正文与 RAW 正文完整交付，TX 池和
消息预算归零。GCC Release `test_transport/default` 连续 6/6 通过；变异停滞游标后该测试按预期失败。
本次只扩展真实集成回归、记录和版本号，不改生产/API/ABI/wire；结果见
[六存活压缩流六轮公平性回归](docs/reviews/channel_compressed_data_tx_six_survivor_fairness_20261025.md)。

dev.103 保持六条存活压缩流，将每条高熵正文扩至 2 KiB（仍小于 4 KiB 初始窗口），逐帧检查前 72
个成功 DATA 帧，六条流完成 12 轮顺序轮转；RESET 等待者无 DATA，正文交付和 TX 池/预算归零断言不变。
Release `test_transport/default` 连续 6/6 通过，停滞游标变异仍按预期失败。本次仅扩展测试、记录和版本号，
不改生产/API/ABI/wire。详见
[六存活压缩流十二轮公平性回归](docs/reviews/channel_compressed_data_tx_twelve_round_fairness_20261025.md)。

dev.104 补上 #241 的消息长度/窗口边界断言：初始窗口 `W=1024` 时，`L=1025` 的 RAW 消息立即返回
`TR_ERR_BAD_LENGTH`，不改变已发送信用、不产生接收 DATA，调用方仍能归还原缓冲区；原 `L=W` 准入、
窗口耗尽时的 `TR_AGAIN` 和消费后的恢复路径继续通过。本次只扩展集成测试、记录和版本号，不改生产/API/ABI/wire。
详见 [Stream 单消息窗口准入回归](docs/reviews/channel_stream_message_window_admission_20261025.md)。

dev.105 增加非对称窗口双向回归：Client 接收窗口 1 KiB、Server 接收窗口 2 KiB，两个方向各自的
发送上限均匹配对端接收窗口；Client 的 2 KiB 消息与 Server 的 1 KiB 消息都完整交付，反向 1,025 字节
请求在 1 KiB 窗口下立即拒绝。回显对端窗口的变异被测试捕获。本次仅扩展测试、记录和版本号，不改生产/API/ABI/wire。
详见 [非对称 Stream 初始窗口回归](docs/reviews/channel_asymmetric_stream_windows_20261025.md)。

dev.106 补齐 #241 的 `L<W` 窗口关系：Client 向 Server 发送 1,536 字节，低于 Server 的 2,048 字节
接收窗口；反向继续覆盖 `L=W=1,024` 和 `L=1,025>W=1,024` 拒绝。错误使用本地窗口的变异被现有超窗拒绝断言捕获，恢复后
Release transport 目标通过。本次仅扩展测试、记录和版本号，不改生产/API/ABI/wire。详见
[Stream 消息长度与非对称窗口三档回归](docs/reviews/channel_stream_window_inequality_20261025.md)。

dev.107 扩展非对称窗口场景的持有与恢复矩阵：双向消息在等待接收前均已提交，接收端分别持有
1,536/2,048 字节与 1,024/1,024 字节正文；未释放时额外的 513 字节与 1 字节发送都返回 `TR_AGAIN`，信用及接收计数不变；释放后复用原缓冲区重试并双向交付。本地 Release transport 目标连续 3/3 通过。本次只扩展测试、记录和版本号，不改生产/API/ABI/wire。详见
[非对称窗口应用持有与信用恢复回归](docs/reviews/channel_asymmetric_window_hold_recovery_20261025.md)。

dev.108 将 Channel 六帧重组回归压到小资源池：唯一 DATA TX 项、两个 RX buffer、一个 2 KiB 重组槽和单项发送缓冲池，传输 1,500 字节正文并重组为一条消息；应用持有时重组槽可用数为零，释放后恢复为一。Release transport 目标连续 4/4 通过；既有 parser 单字节片段测试继续覆盖字节级拆分。本次只收紧测试资源配置、增加断言和记录，不改生产/API/ABI/wire。详见
[小池预算下的多帧消息重组回归](docs/reviews/channel_fragmentation_minimal_pools_20261025.md)。

dev.109 为 #241 增加真实 TCP Channel 压缩接收扩张比回归：4 KiB 的 `0x5a` 正文编码后小于 64 字节，含 envelope 的发送 wire 小于 128 字节初始窗口；服务端仍解码并逐字节交付完整 4 KiB 正文。Release transport 目标连续 3/3 通过。本次仅扩展测试、记录和开发版本，不改生产/API/ABI/wire。该用例只覆盖单消息编码体/解码体不对称，不代表大规模 RSS、全局预算或跨机器安全验收完成。详见
[小编码体大解码正文接收回归](docs/reviews/channel_compressed_expansion_20261025.md)。

dev.110 为 #241 增加真实 TCP Channel 对 Zstandard 帧头未知内容长度的接收回归：发送端显式关闭帧内 content size，断言帧头报告 `ZSTD_CONTENTSIZE_UNKNOWN`；消息 envelope 仍声明 4 KiB 解码长度，服务端通过 Channel 解码并逐字节交付完整正文。Release transport 目标连续 3/3 通过。本次仅增加测试、记录和开发版本，不改生产/API/ABI/wire。详见
[未知帧内容长度压缩消息接收回归](docs/reviews/channel_zstd_unknown_content_size_20261025.md)。

dev.111 为 #241 增加真实 Channel 对虚报解码长度的隔离回归：压缩体是有效 Zstandard 帧，帧头原始长度与实际正文一致，但外层 envelope 少报 1 字节；该消息触发流级 RESET，应用接收数不增加，既有同连接流保持活动。本次仅增加测试、记录和开发版本，不改生产/API/ABI/wire。Release transport 目标连续 3/3 通过。详见
[压缩 envelope 虚报原始长度隔离回归](docs/reviews/channel_compressed_forged_length_20261025.md)。

dev.112 为 #241 增加压缩解码窗口超限的真实 Channel 隔离回归：有效 4 KiB Zstandard 帧的未知长度头被设置为声明 2 MiB 窗口，超过 Channel 配置的 1 MiB 上限；服务端以 `TR_ERR_NOMEM` 复位当前流，不交付正文，池与活动流回到基线。本次仅增加测试、记录和开发版本，不改生产/API/ABI/wire。Release transport 目标连续 3/3 通过。详见
[压缩解码窗口超限隔离回归](docs/reviews/channel_compressed_window_limit_20261025.md)。

dev.113 为 #241 增加未知内容长度压缩体的解码上界回归：有效 4 KiB Zstandard 帧没有帧内原始长度，envelope 却少报 1 字节；解码器在已预约容量边界用探针检测到多出的字节并返回资源限制，Channel 以 `TR_ERR_NOMEM` 复位当前流，不交付正文且缓冲池归还。本次仅增加测试、记录和开发版本，不改生产/API/ABI/wire。Release transport 目标连续 3/3 通过。详见
[未知长度帧超出 envelope 解码容量回归](docs/reviews/channel_compressed_unknown_output_overrun_20261025.md)。

dev.114 为 #241 增加未知内容长度帧的短输出隔离回归：有效 Zstandard 帧实际解码长度比 envelope 声明少 1 字节；帧结束时解码器拒绝不完整输出，Channel 以 `TR_ERR_STATE` 复位当前流，不交付正文且缓冲池归还。本次仅增加测试、记录和开发版本，不改生产/API/ABI/wire。Release transport 目标连续 3/3 通过。详见
[未知长度帧短于 envelope 声明长度回归](docs/reviews/channel_compressed_unknown_output_underrun_20261025.md)。

dev.115 修正 #241 压缩多帧接收回归的空切片：原用例将首帧 body 长度设为 0，实际只把 envelope 放在首帧；现在首帧携带完整 envelope 和一半 Zstandard body，后续 DATA 帧承载余下 body，服务端仍完整解码交付。本次仅改测试、记录和开发版本，不改生产/API/ABI/wire。Release transport 目标连续 3/3 通过。详见
[压缩 body 跨 DATA 帧接收回归](docs/reviews/channel_compressed_body_fragmentation_20261025.md)。

dev.116 将未知 content size 的有效 Zstandard 帧与压缩 body 跨 DATA 帧切分组合：帧头不提供原始长度，首帧携带完整 envelope 和部分 body，后续帧承载余量；真实 TCP Channel 仍完整解码交付，wire 计数与缓冲池归还通过验证。本次仅改测试、记录和开发版本，不改生产/API/ABI/wire。Release transport 目标连续 3/3 通过。详见
[未知长度压缩 body 跨 DATA 帧接收回归](docs/reviews/channel_unknown_size_compressed_body_fragmentation_20261025.md)。

dev.117 为 #241 增加有效 Zstandard 帧串接拒绝回归：两段 body 分别都是可独立解码的完整帧，envelope 声明两帧的总解码长度；真实 TCP Channel 仍只 RESET 当前流，不交付正文且缓冲池归还。本次仅增加测试、记录和开发版本，不改生产/API/ABI/wire。Release transport 目标连续 3/3 通过。详见
[串接有效 Zstandard 帧拒绝回归](docs/reviews/channel_concatenated_zstd_frame_rejection_20261025.md)。

dev.118 为 #241 增加 Zstandard 帧校验和损坏隔离回归：启用帧内容校验并确认原帧有效后翻转校验字节；真实 TCP Channel 以 `TR_ERR_STATE` 复位当前流，不交付正文，活动流和接收缓冲池回到基线。本次仅增加测试、记录和开发版本，不改生产/API/ABI/wire。Release transport 目标连续 3/3 通过。详见
[Zstandard 帧校验和损坏隔离回归](docs/reviews/channel_zstd_checksum_corruption_20261025.md)。

dev.119 为 #241 增加不支持的 Zstandard 帧隔离回归：真实 TCP Channel 收到合法 skippable frame 与 dictionary ID 非零的帧时，均只 RESET 目标流、不交付正文且接收池归还。本次仅增加测试、记录和开发版本，不改生产/API/ABI/wire。Release transport 目标连续 3/3 通过。详见
[不支持的 Zstandard 帧隔离回归](docs/reviews/channel_unsupported_zstd_frame_isolation_20261025.md)。

构建需要 OpenSSL 3 开发包，手动链接静态 SDK 时在 `-ltrcore` 后增加
`-lssl -lcrypto`。阶段证据见 [TLS-C 实现记录](docs/reviews/rpc_v2_tls_c_20261023.md)、
[TLS-D1 验证记录](docs/reviews/rpc_v2_tls_d1_20261023.md)、
[TLS-C 设计与计划](docs/superpowers/specs/2026-10-23-tls-c-reactor-integration-design.md)
与 [ADR-009](docs/architecture/decisions/ADR-009-tls-byte-endpoint.md)。
dev.8 将有效边界内、可定位到已打开 Stream 的非法 DATA 类型标志隔离为该流的 RESET，
同连接其他流可继续；未知流和不可证明安全的边界仍按连接错误处理。验证见
[DATA 标志隔离评估](docs/reviews/rpc_v2_data_flag_isolation_20261023.md)。上一版让普通 V2 Server 的所有 shard 共用同一个内存硬上限，
每个 shard 保留本地账目，不再因旧的等分额度提前拒绝。设计与验证见
[共享预算评估](docs/reviews/rpc_v2_shared_memory_budget_20261023.md)。上一版将普通 V2 Channel 的固定存储计入所属 shard 的共享内存预算，
预算不足时在分配前拒绝创建，构造失败或最终销毁时归还。实现和限制见
[Channel 预算评估](docs/reviews/rpc_v2_channel_budget_20261023.md)。上一版明确拒绝超过对端固定窗口的整消息，并保证已消费信用不会
因更新阈值阻塞下一条合法消息；同步回复和异步 producer 同样覆盖。
设计与验证见 [LIVE-V2-01 评估](docs/reviews/rpc_v2_message_window_20261023.md)。
此前流槽耗尽时单独拒绝新流、保留已有流的实现与性能边界见
[PERF-V2-03 评估](docs/reviews/rpc_v2_stream_refusal_20261023.md)；Channel 握手仍为 3。
此前控制容量通知的测量见 [PERF-V2-02 评估](docs/reviews/rpc_v2_control_ready_20261023.md)，
整体剩余工作见 [V2 开发总账](docs/reviews/v2_remaining_work.md)。
此前 `2.0.0-dev.1` 的交付范围与主线验收保存在
[R17 发行边界评估](docs/reviews/rpc_v2_release_boundary_20261023.md)。当前仍是开发预览，未发布稳定版。

**接口迁移状态**：普通 `tr_client_create(config, methods, count, out)` 与
`tr_server_create(config, methods, count, out)` 现在只创建 V2；方法在创建时冻结。
一元调用使用 `tr_client_unary_call(..., options, callbacks, out)`，正文回调与
唯一终局分离。旧两参数构造、运行时注册和 V1 一元回调不再是公开门面；不兼容
旧调用签名，须重新编译。连接组改用 `*_create_transport()`。完整接入合同以
[公共 SDK 文档](docs/rpc-v2-sdk.md)及两个 Echo 示例为准。

此版本已移除旧 V1 Call、Endpoint、Executor Group 与 Wire 核心；旧
V1 对端不会通过默认门面协商或回退。RAW Codec 仍作为独立字节工具提供：
`tr_rpc_raw_encode()` 将字节复制到调用方缓冲区，`tr_rpc_raw_decode()` 返回指向输入的
借用视图，调用方须保持输入存储有效。它不包含 RPC Call 或 Wire 协议。`bench/results/rpc-20260926.csv` 是独立的历史 V1 基线，
新的真实 TCP 基准通过公开 V2 Client/Server 运行，不能与旧 CSV 合并比较。

## 架构

```text
应用/业务层
     |
     v
RPC 服务/方法
V2 Record Call + 任务执行器
ONE/MANY <-> ONE/MANY
     |
V2 Record + 应用字节
     |
     v
+--------- Channel ---------+
|                           |
CONTROL 通道              BULK 通道
|                           |
+------ 物理映射 -----------+
|                           |
控制连接                 批量连接
|                           |
+------------+--------------+
             |
          Stream 层
        字节流量控制
             |
          传输帧
             |
      Reactor 所有者
             |
      epoll LT + eventfd
        /             \
     recv()          sendmsg()
       |                 |
    增量解析         头部 + 分片
       |              iovec[]
       |                 |
   RX 缓冲区         TX 项资源池
    资源池       （批量 + 控制预留）
        \             /
              TCP
```

`Channel` 可以运行在两种模式：

- `TR_CHANNEL_SHARED_CONNECTION`：CONTROL 与 BULK 通道映射到同一个 TCP 连接。
- `TR_CHANNEL_SPLIT_CONNECTIONS`：CONTROL 与 BULK 使用独立 TCP 连接，但共享同一个 Reactor。

传输/RPC 核心不执行常规文件系统 I/O，也不包含备份专用的提交/存储语义。

**安全与可选能力现状**：普通 V2 Client/Server 可显式要求 TLS 1.3 和双向证书认证；
已验证的唯一 URI SAN 可作为 RPC work 的连接身份视图。transport-only 连接组也支持 TLS
mTLS，Server 授权 CONTROL/DATA 主体，Client 要求 DATA 服务端主体与 CONTROL 连续一致。
默认仍使用明文 TCP。Message 层压缩已接入普通 Stream、RPC 业务正文和连接组 DATA，
按消息显式选择且默认关闭；连接组本机矩阵已完成，真实业务负载和发布性能阈值验收仍待完成。
TLS profile 轮换和全部 TLS 内部内存的硬预算仍待完成。
CRC32C 不能替代认证和加密；
部署到不可信网络前仍须完成 [TLS-D/E 验收](docs/architecture/decisions/ADR-009-tls-byte-endpoint.md)。
消息压缩要求见[压缩与 TLS 需求](docs/architecture/11-compression-tls-requirements.md)。

## 运行时可观测性

运行时仍会收集无额外分配的结构化诊断数据，用于基准测试和内部瓶颈归因。
队列/资源池计数器与高水位仍保留在引擎内部；单调时钟计时直方图仍通过
仓库内部诊断调优按需启用。

详细的 Reactor/Channel/Endpoint 快照、计时标志以及诊断直方图/队列/资源池类型，
有意**不属于**稳定安装 SDK。仓库内基准测试和测试程序使用内部诊断接口。

V2 稳定语义观测包括与内部布局无关的 RPC 生命周期快照，通过
`tr_client_get_rpc_semantic_stats()` 和
`tr_server_get_rpc_semantic_stats()` 获取，并保留现有连接组语义统计。
RPC 快照只暴露已经开始/已经结束/进行中的 Call 和最终 RPC 状态分布；
不会暴露工作线程、队列、资源池、Reactor、槽位或计时布局。
V2 在调用能力受理、业务终局提交时记账；服务端累计值由分片持有，断连和
对端回收不会清空。统计失败不改变 RPC 结果，也不返回截断或回绕的快照。
详见 [`docs/observability.md`](docs/observability.md)。

## 高层客户端/服务端门面

库现在提供第一版高层门面，用于不希望直接管理 Reactor 槽位、连接代次、
Channel 构造或已接收套接字接管的应用：

```text
应用
 |
 +--> tr_client -------------------+
 |                                 |
 +--> tr_server                    |
                                   v
                            RPC Endpoint
                                   |
                                Channel
                                   |
                               Connection
                                   |
                                Reactor
                                   |
                                  TCP
```

客户端使用 `tr_client_config_init()` 初始化配置，并准备非空方法描述数组。
创建时调用 `tr_client_create(&cfg, methods, method_count, &client)`，然后
`connect()` / `wait_ready()`；`tr_client_unary_call()` 自动发送一元请求，
流式使用 `tr_client_call_start()` 与 `tr_rpc_v2_call_*()`。两者共用同一个
V2 调用和终局内核，不再在连接后注册旧方法。

服务端准备 `tr_rpc_v2_method` 数组，在 `tr_server_create()` 中复制冻结工作
函数、终局函数及方法描述，然后监听、启动、排空和销毁。默认门面的完整可运行
示例为 `examples/echo_client.c` 和 `examples/echo_server.c`，程序诊断继续使用
原国际化机制。门面持有运行时、有限任务/调用槽及缓冲预算；内部布局不进入 SDK。

库内显式迁移入口 [`tr_server_create_record()`](src/server_record_internal.h)
已将 V2 记录宿主接入同一 `tr_server` 的监听、接纳、排空与销毁。它冻结业务方法表，
按全 Server 总额分摊硬预算、线程数和任务容量，每分片由所有对端共享一个通用执行器。
该内部入口不安装为 SDK。普通 `tr_server_create()` 只构造 V2，
`tr_server_create_transport()` 只保留传输与连接组资源；两者都不提供 V1 协商。
V2 的回复能力只在当前工作回调内有效；稳定 RPC 语义统计由 V2 Server 公共入口读取。
大小/窗口限制、失败回滚与实际测试范围见
[V2 Server 入口集成记录](docs/reviews/server_record_integration_20261017.md)。

对应的库内 [`tr_client_create_record()`](src/client_record_internal.h) 复用既有
`tr_client` 与单分片 Reactor，冻结客户端方法表，通过有界调用宿主连接上述 V2
Server。调用受理后即承担一次终局责任，包括 OPEN 因背压尚未发出的情况；发送
仅在成功时接管正文，旧句柄按调用代次失效。它支持本地单调期限、有界 OPEN
元数据和调用级取消。两端显式要求 RESET 能力，取消或超时通过单流 RESET
通知远端任务令牌；请求 `HALF_CLOSE` 后仍可取消。Channel 接管重置责任后，
客户端可退役调用，网络层继续以有界状态等待双向停止屏障，校验并丢弃晚到帧。
取消不证明业务线程已经退出，不合作对端仍可占网络槽到断连或销毁。
自动重连因连接器尚缺停止域回收而明确拒绝，旧公开调用接口不用于此显式入口。
连接装配、对端窗口限制及实际验证范围见
[V2 Client 入口集成记录](docs/reviews/client_record_integration_20261017.md)。

### 公共 V2 SDK

安装后的 [`tr/rpc_v2.h`](include/tr/rpc_v2.h) 提供
`tr_client_create()`、`tr_server_create()`，复用原有连接、监听、就绪、
排空和销毁操作。方法表在创建时复制冻结，业务无需包含内部记录、缓冲区或任务头。
普通接口只选择 V2，不自动协商或回退到 V1。

- Client 通过 `tr_client_call_start()` 取得弱调用能力，使用
  `tr_rpc_v2_call_send()`、`half_close()`、`cancel()` 推进调用。发送仅借用正文，
  `TR_OK` 后已经复制并接受，应用可立即复用原内存；`TR_AGAIN` 不隐式保存消息，
  后续可写通知负责提示重试，包括复制池满后的恢复。
- 需要跨回调保留终局时，可用 `tr_rpc_v2_call_hold()` 取得唯一 `tr_rpc_v2_call_ref`，
  用 `result()` 读取不可变快照、在活动期用 `cancel()` 请求取消，并用 `release()`
  归还固定调用槽。残留引用会让 `wait_drained()` 未完成，且要求 destroy 返回 AGAIN 后重试。
- 消息和元数据是回调内的只读借用视图；元数据通过按索引或键访问，不暴露 TLV。
  Client 终局恰好一次，尾部元数据先于终局交付。终局可能早于外部 start 返回，
  因而回调参数必须预先准备。
- Server 工作回调接收 OPEN、MESSAGE、HALF_CLOSE 业务事件及合作取消令牌，
  使用有界复制式回复。事件在同一调用内顺序执行，不同调用可并行。
  不透明业务身份跨连接和 Server 唯一；完整合法 OPEN 准入后即承担业务终局责任，
  终局等待工作回调和完成交付真正退出。
- ONE/MANY 支持四种组合，MANY 可为零条；零字节消息仍计一条。
  V2 构造的正文大小和窗口上限自动补入协议开销，无需业务计算记录固定头。
- Server 可在 work 内复制设置初始和尾部响应元数据，每阶段编码后最多 512 字节。
  初始元数据先于正文，尾部元数据只随同批终局发送；合法业务错误也可携带元数据。
  追加正文或设置元数据失败会锁存整批错误，不能忽略错误产生截断成功。
- 需要回调外持续回复时，可在当前空批次中取得独立 producer，跨线程复制发送
  初始元数据、正文和带尾部元数据的终局。单条待发送记录形成有限背压，普通线程
  可限时等待可写；取得句柄后 HALF_CLOSE 不再自动完成回复。取消及销毁关闭入口，
  应用持有的句柄可安全长于 Server，但外部线程和私有状态仍由应用管理。
- 两端通过既有语义统计入口读取开始、结束、进行中调用数及最终状态分布。
  Client 自身 LIVE 回调可读取本地快照；Server 聚合快照由外部控制线程读取。

普通 RPC 接口尚未提供重连或网络压缩；普通 V2 的 TLS 必需模式见
[TLS-C 实现记录](docs/reviews/rpc_v2_tls_c_20261023.md)。连接组是独立的传输工厂，也不是 RPC 协议回退。所有权、资源限制和接入顺序见
[V2 SDK 使用合同](docs/rpc-v2-sdk.md)。完整的可编译运行消费端见
[`tests/test_rpc_v2_sdk.c`](tests/test_rpc_v2_sdk.c)；CI 将同一源码复制到仓库外，
仅用安装头文件与静态库，分别以 C11/C++11 的 GCC、Clang 编译运行。

V2 Server 从完整有效 OPEN 中的非零剩余预算建立本机单调期限，检查接收准入、
任务开始、完成交付和每次回复。到期后选择 `DEADLINE_EXCEEDED`，通过原任务令牌
合作取消业务，丢弃尚未接受的迟到成功。已选本地终局可先通过 RESET 停止网络，
正在执行的业务退出后才通知服务端终局并回收其持有资源。
零预算仍表示无限期限。该行为不提供跨主机绝对时钟、线程抢占或错误 FINAL 必达，
调用预算只在完整 OPEN 验证后开始。实现与验证边界见
[V2 Server 截止时间记录](docs/reviews/server_record_deadline_20261017.md)。

尚未完成的 RPC OPEN 另有本机准入期限：从传输流 `OPENED` 起固定计时，
默认 10 秒；分段字节、可写通知或重试都不会续期。到期选择
`DEADLINE_EXCEEDED` 并直接进入 RESET，取时失败选择 `INTERNAL`。
该期限不借用尚未验证的 RPC 元数据，也不承诺强制回收不合作对端的网络槽。
V2 门面排空同时等待业务任务、完成交付、终局回调、网络流和发送资源；
所有等待共用一个本机单调绝对截止时间。合同及验收见
[OPEN 准入与分层排空记录](docs/reviews/rpc_record_admission_drain_20261017.md)。

单流终止使用双方各一次的对称 RESET，沿原发送 FIFO 形成双向停止屏障；
每个连接独立预留 RESET 发送项，普通 DATA/控制池压力不占用该名额。流 ID、流槽和
物理连接槽代次不回绕，有限精确退役记录只接纳保留窗口内的重复 RESET。已归属
调用的 RPC 错误按目标流终止；尚未接受本地普通 CLOSE 的活流也能隔离消息
语义错误和重组资源不足。无法恢复的帧、身份或信用错误仍关闭连接。完整错误
分类和验证范围见 [V2 单流重置记录](docs/reviews/rpc_record_reset_20261017.md)。

稳定 SDK 还在 `tr/transport.h` 中包含第一版通用连接组传输能力。
该能力按需启用，默认关闭：

```c
tr_server_config_init(&cfg);
cfg.connection_groups.max_groups = 64;
cfg.connection_groups.max_connections = 256;
cfg.connection_groups.max_data_connections_per_group = 4;
cfg.connection_groups.max_streams_per_group = 1024;
cfg.connection_groups.authorize = authorize_group;
cfg.connection_groups.on_message = on_group_data;
cfg.connection_groups.callback_arg = app;

tr_server_create_transport(&cfg, &server);
tr_server_connection_group_listen(
    server, "0.0.0.0", 9100, 128, &group_port);
tr_server_start(server);
```

授权回调对每条 CONTROL 和 DATA 路由分别调用。`struct
tr_connection_group_authorization` 提供组、角色、TLS 对端身份，以及 DATA 对应的
CONTROL 主体；明文连接的身份认证类型为 `TR_PEER_IDENTITY_NONE`。连接组启用 TLS 时，
Server 必须配置 `TR_TLS_REQUIRED` 和 `TR_TLS_CLIENT_AUTH_REQUIRED`。

同一个稳定传输门面现在也提供客户端 CONTROL 生命周期，并且不暴露路由内部实现：

```c
struct tr_connection_group_id group = {
    .group_id = 42,
    .epoch = 1,
};

tr_client_config_init(&client_cfg);
/* 0 保持仅 CONTROL 行为；非 0 启用有界自动 DATA 通道。 */
client_cfg.connection_groups.max_data_connections = 4;
/* 0 继承 client_cfg.limits.max_streams，保持向后兼容行为。 */
client_cfg.connection_groups.max_active_transfers = 256;
/* 启用组 mTLS 时，Server 同时要求客户端证书；Client 对每条 DATA
 * 复用此 profile，并校验服务端 URI SAN 与 CONTROL 主体一致。 */
client_cfg.tls.mode = TR_TLS_REQUIRED;
client_cfg.tls.ca_file = client_ca_file;
client_cfg.tls.expected_peer_name = "server.example.test";
client_cfg.tls.client_certificate_file = client_certificate_file;
client_cfg.tls.client_private_key_file = client_private_key_file;
tr_client_create_transport(&client_cfg, &client);
tr_client_connection_group_connect(
    client, "127.0.0.1", group_port, &group);
...
tr_client_connection_group_close(client);
assert(tr_client_destroy(client) == TR_OK);
```

门面复用现有单所有者流水线引擎。应用只看到 `group_id/epoch`、
逻辑 Stream/消息标识以及字节视图；Reactor 句柄、注册表状态、TRR1 解析、
所有者分片和成员代次全部保持内部化。
返回 `TR_CONNECTION_GROUP_MESSAGE_TAKE_OWNERSHIP` 会保留原始 RX 缓冲区，
不增加额外载荷复制，并要求之后调用一次
`tr_connection_group_message_release()`。

当 `max_data_connections > 0` 时，客户端在内部消费 `DATA_OFFER`：
打开非阻塞 DATA 套接字，通过所属 Reactor 等待连接/前导信息推进，
然后在同一所有者上提交精确 TRR1 DATA 路由。启用 TLS 时，每条 DATA 都独立完成
mTLS；TCP、TLS 握手和 TRR1 前导共享一次连接绝对期限。只有服务端证书完成验证且
URI SAN 与 CONTROL 完全相同、加密 TRR1 完整发出后，该 DATA 才可用于发布 READY。
同一时刻只允许一个 DATA 连接建立过程进行，但可以同时保持多个已经附着的
DATA 通道活动。不引入连接线程，也不公开 DATA 索引/代次。
默认 `max_data_connections == 0` 时，`DATA_OFFER` 仍在内部取消。

DATA 通道成员关系变为活动状态本身并不授权逻辑传输。
只有服务端 `TRANSFER_READY` 隐藏的 DATA 索引/代次精确匹配一个
`ACTIVE` 客户端通道时才接受。随后客户端建立有界本地 Stream 亲和关系，
并调用可选 `on_transfer_ready` 回调；回调只暴露组/Stream/消息标识。

客户端传输并发由 `connection_groups.max_active_transfers` 独立限制；
值为 0 时继承现有 `limits.max_streams`，因此旧配置保持原行为。
`tr_client_connection_group_send()` 通过精确亲和关系发送一条逻辑消息；
应用字节只在本次调用期间借用，并在返回 `TR_OK` 前复制到有界内部所有权。
连接组的发送内存上限由
`max_data_connections * max_message_bytes` 推导，现有 Reactor 继续负责
DATA 分片与 TX 调度。临时准入压力返回 `TR_AGAIN`，调用方稍后重试。
本阶段不引入第二个连接组发送队列，也不增加可写通知契约。
`tr_client_connection_group_release_transfer()` 用于移除本地亲和关系。

连接组生命周期现在除了强制关闭 `stop()` 外，还具有显式优雅排空语义。
服务端开始排空时只关闭连接组接收套接字，并拒绝新建
`DATA_OFFER / TRANSFER_READY`；现有连接和传输继续运行，直到应用释放它们且
客户端自然关闭。客户端开始排空是单调本地准入屏障：停止建立新的 DATA 通道，
并忽略屏障后观察到的 `TRANSFER_READY`；屏障前已经 READY 的传输和进行中的
发送仍可完成。稳定语义统计只报告组/连接/DATA/传输以及发送字节生命周期状态；
Reactor 槽位、路由代次、队列占用和资源池内部状态仍不属于 SDK 契约。

普通 V2 RPC 门面有意使用 `TR_CHANNEL_SHARED_CONNECTION`；transport-only
门面只构造传输与连接组资源，不构造 RPC Endpoint。客户端和服务端门面的
TCP 套接字默认启用 `TCP_NODELAY`，避免小请求/响应流量受到 Nagle/延迟 ACK
耦合影响。策略是显式的：
`TR_TCP_NODELAY_DEFAULT` 和 `TR_TCP_NODELAY_ENABLED` 启用该行为，
`TR_TCP_NODELAY_DISABLED` 保持 Linux 默认值不变。
客户端自动重连在 Reactor 接管前继承相同设置。
原始 `tr_tcp_*` 辅助接口和低层 Channel 连接不会强制应用该门面策略。

低层 Channel API 仍支持拆分 CONTROL/BULK 连接。
生产级多客户端拆分门面需要在握手中加入连接绑定标识，使服务端能够证明
独立接收的 CONTROL 与 BULK 套接字属于同一个逻辑 Channel；
该配对协议不会根据接收顺序猜测或推断。

服务端门面在运行期间回收已断开的对端对象。
Reactor 回调销毁使用显式静默屏障：先禁用回调，再由所有者线程等待
Reactor 穿过屏障，之后才释放 RPC/Channel 存储。
因此 `max_peers` 限制的是同时保留的对端对象数量，而不是服务端整个生命周期
累计接收的连接数量。

入站握手另有独立的 `handshake_timeout_ms` 总期限（默认 10000ms，0 采用默认值）。
无 HELLO、部分 HELLO 或未完成 TRR1 的连接到期后由所属 Reactor 回收，不会永久占用
`max_peers` 或连接组容量；部分字节到达不会续期。只有真正握手完成才计入就绪对端。
配置、源级兼容性及回归范围见[入站握手期限](docs/inbound_handshake.md)。

默认构建两个真实进程示例。分别在两个终端运行：

```sh
xmake run echo_server 9000
xmake run echo_client 127.0.0.1 9000 hello
```


## 已实现

### 传输线协议/分帧

- 显式小端线协议编解码；不使用打包 C 结构体作为线协议 ABI；
- 40 字节传输帧头；
- 头部与载荷使用 CRC32C：提供可移植查表后端，并在运行时选择 x86-64 SSE4.2 后端；
- 帧类型/标志/长度校验；
- 每个传输帧的最大载荷有界；
- 超过单帧的逻辑 DATA 消息使用 `FIRST/LAST` 自动分片，并保持同一个稳定 `message_id`；
- 支持 `HELLO`、`HELLO_ACK`、`GOAWAY`、`STREAM_OPEN`、`STREAM_CLOSE`、`WINDOW_UPDATE`、`DATA`、`PING/PONG`；
- 在 Stream 打开时声明逻辑通道。

### 接收路径

- 增量解析器支持任意头部/载荷分片；
- `recv()` 直接写入最终 RX 载荷缓冲区；
- 有界固定大小 RX 缓冲资源池；
- 资源池耗尽时对解析器/RX 施加背压；
- RX 暂停时禁用 `EPOLLIN`，由 `tr_reactor_resume_rx()` 恢复；
- RX 所有权可以自动释放，也可以向上层转移。

### 发送路径

- 固定大小 BULK TX 项资源池；
- 独立预留的 CONTROL TX 项资源池；
- 使用固定容量分散/聚集的 `sendmsg()`（`tr_reactor_sendv()`）；
- 每帧最多 `TR_REACTOR_MAX_TX_SLICES` 个传输载荷分片；
- 部分写游标可以跨头部和多个分片继续推进；
- 仅在套接字返回 `EAGAIN` 后启用 `EPOLLOUT`；
- 每连接 TX 字节预算和 Reactor 本地就绪队列；
- 稳态传输 DATA 提交不为每帧执行堆分配；
- 仅当相应 API 返回 `TR_OK` 后才转移发送所有权。

### 运行时

- Linux epoll 电平触发 Reactor；
- Reactor 线程独占可变连接状态；事件循环处理器/连接热路径不依赖槽位互斥锁；
- 槽位代次/状态使用 C11 原子能力元数据，跨线程处理器更新通过同步命令提交；
- 有界互斥锁保护的多生产者单消费者命令环；
- eventfd 唤醒合并；
- 基于代次的连接句柄和陈旧事件拒绝；
- 每连接上层回调处理器；
- 非阻塞 IPv4 TCP 辅助接口（`listen`、`connect`、`accept`）；
- 连接状态快照 API 用于验证 Channel 替换是否安全。

### Channel / Stream

- 逻辑 `Channel` 位于物理 TCP 连接之上；
- 每个连接在通道可用前执行对称 `HELLO / HELLO_ACK` 能力握手；
- Channel 握手协议版本 3；与帧封装版本 1、RPC V1/V2 分别管理；
- 对端接收能力协商，包括帧载荷、逻辑消息大小、特性位和通道映射；
- 通道状态区分 `DOWN / RECONNECTING / HANDSHAKING / UP`；只有 `UP` 后才允许新 Stream；
- 协商后的帧上限由 `tr_reactor_sendv_limited()` 强制执行，因此能力不对称的对端实际按较小接收上限发送；
- CONTROL / BULK 流量通道；
- 共享连接与拆分连接策略；
- 客户端/服务端 Stream 标识使用奇偶分配；
- Stream 打开与半关闭生命周期；
- 基于代次的 Stream 句柄；
- 每 Stream 单调递增消息标识；
- 基于字节的 Stream 流量控制；
- 绝对窗口上限（`WINDOW_UPDATE` 是绝对字节边界，不是增量）；
- 只有上层消费载荷后才归还流量控制额度；
- `tr_stream_send()` 用于单缓冲逻辑消息；
- `tr_stream_sendv()` 用于分散/聚集逻辑消息；
- TX 分片保留原始缓冲区，并通过 `sendmsg()` 直接发送分片范围，不拼接载荷；
- 单帧 RX 消息从 Reactor 到 Channel 使用者保持零复制；
- 分片 RX 消息使用可选有界 `reassembly_pool`，先进行一次连续重组复制，再触发一次逻辑消息回调；
- 上层可以保留 RX 逻辑消息载荷，并在之后通过 `tr_stream_release_payload()` 归还；
- 拆分模式将 BULK 连接故障与 CONTROL 连接运行隔离；
- 故障通道对应的 Stream 槽位立即失效，因此重连不会泄漏 `max_streams` 容量；
- 逻辑 Channel 在物理连接丢失后仍然存活；
- `tr_channel_replace_connection()` 可以在不重建 Channel/RPC Endpoint 的情况下附着新的 Reactor 所有连接；
- 共享模式同时替换 CONTROL+BULK；拆分模式独立替换各通道；
- 底层客户端 Channel 支持针对数字 IPv4 端点的可选自动重连；普通 V2 RPC Client
  不启用该配置；
- 有界指数退避重连，并支持配置连接超时；
- 服务端有意使用显式替换：应用接收并认证新套接字后再完成连接替换；
- Channel 发出 DOWN/UP/GOAWAY 事件，并暴露每通道 `DOWN / RECONNECTING / HANDSHAKING / UP` 状态；
- 连接替换后旧 Stream 不会存活；旧句柄全部变为陈旧句柄。

### 能力握手与优雅排空

物理 TCP 连接不会在接管后立即被视为 Channel 可用。双方都会发送固定大小的
`HELLO`，声明自身接收能力：

当前 Channel 协议版本为 **3**，仅接受该版本。它包含带受理边界的 GOAWAY，
以及流槽满时的单向 STREAM_REFUSED；后者不授予信用、不进入业务、不要求回包。
旧版 Channel 在握手时被拒绝，避免误解未受理流的回收责任。
它与仍为 1 的帧封装版本、RPC V1/V2 记录版本属于不同层次。

```text
TCP 已接管
   |
   v
HANDSHAKING
   |  HELLO / HELLO_ACK
   |  - 协议版本范围
   |  - 通道掩码
   |  - 接收最大帧载荷
   |  - 接收最大逻辑消息
   |  - 特性位
   v
UP
   |
   +--> 允许新 Stream
```

声明的大小上限具有方向性：对端声明较小的本地重组上限时，只限制**发送给该对端**
的消息；不会无必要地限制反方向发送的消息。

优雅关闭通过以下 API 暴露：

```c
tr_channel_begin_drain(channel);
tr_channel_wait_drained(channel, timeout_ms);
```

`begin_drain()` 是幂等操作，会禁用重连、冻结新 Stream 准入，并在每条存活且已就绪
的物理连接上接管 `GOAWAY`；尚在握手中的连接待就绪后推进。帧的 `message_id`
携带最后受理的对端流 ID，0 表示未受理过流；
边界在首次排空时冻结。初始 `WINDOW_UPDATE` 必须先于对应的 GOAWAY，接收方据此
校验边界恰好等于该连接此前确认过的最高本地流 ID，重复 GOAWAY 只能保留原边界。

收到 GOAWAY 后，同一物理连接停止双向新建流并回复自己的 GOAWAY；拆分模式的
另一条物理连接可继续工作，只有显式 `begin_drain()` 才冻结整个 Channel。
边界外仍在 `OPENING` 的本地流直接以 `TR_ERR_CLOSED` 失败并退役。
本地尚未取得初始确认的 Stream 不能发送 CLOSE，`tr_stream_close()` 返回 `TR_AGAIN`；
调用者必须保留重试责任，直到确认、GOAWAY 拒绝或连接失败。

已经受理的 Stream 可以继续运行，直到自然关闭或取消。排空完成分为以下责任：

| 层次 | 完成条件 |
| --- | --- |
| Channel | 已显式开始排空，活跃流为零，存活连接已接管本方 GOAWAY 并收到对方 GOAWAY |
| Reactor 发送 | 观察前已接受的命令全部处理，普通发送项与 GOAWAY、RESET 预留项全部归还 |
| V2 RPC 宿主 | 原业务任务已完成交付，终局回调已返回，绑定已退役或停止责任已交给 Channel |
| V2 Client/Server 门面 | RPC 与 Channel 条件共同满足，使用同一个单调绝对截止时间等待 |

Channel 的 `DRAINED` 同时要求网络和 Reactor 发送条件，单独观察流数量为零不够。
发送排空证明本机发送资源已归还，成功发送的字节已交给内核，不证明对端应用收到或处理。
等待超时不会撤销排空，也不延长原期限；`timeout_ms == 0` 只立即检查。
业务工作仍使用合作取消，停止网络不代表工作线程已退出；不合作的任务或对端会使
有限等待返回未完成，调用者可随后重试等待或按生命周期规则关闭。

### 保活/存活检测

Channel 可以按需运行低频传输层存活探测：

```c
struct tr_channel_keepalive_config cfg = {
    .interval_ms = 30000,
    .timeout_ms = 10000,
};

tr_channel_enable_keepalive(channel, &cfg);
```

保活与服务健康检查有意分离。它只回答“对端/连接是否仍在推进？”
当对端 RX 空闲达到 `interval_ms` 后，Channel 发送 `PING`。
匹配的 `PONG` 会记录 RTT 样本。探测之后观察到任意对端流量都足以证明存活；
如果 `timeout_ms` 内没有观察到对端活动，Channel 使用 `TR_ERR_TIMEOUT`
中止物理连接，随后正常进入现有连接丢失与重连处理。

共享连接模式对共享 TCP 连接只发送一组探测；拆分模式独立跟踪 CONTROL 和 BULK。
每个 Channel 在所属 Reactor 中注册一个初始未启用的保活定时器；独立使用和高层
客户端/服务端 Channel 都使用同一条所有者本地路径。保活不再创建私有定时器线程，
调度全部留在 Reactor 内。

### 指标/诊断

详细 Reactor/Channel/Stream/Endpoint 快照仍提供给仓库内部诊断与基准测试代码，
但不再属于稳定安装 SDK。

这种分离是有意的：

```text
稳定应用 API
    -> RPC/门面语义契约

内部诊断
    -> Reactor 队列/资源池压力
    -> Channel/Stream 状态
    -> Endpoint 执行器细节
```

实现继续收集相同的有界计数器和可选计时直方图；变化的只是兼容性边界。

### Channel 重连/连接替换

重连有意恢复**逻辑 Channel**，而不是恢复旧 Stream 的字节状态：

```text
TCP 连接 #1
       X
       |
       +--> 故障通道上的全部 Stream -> ERROR / 陈旧句柄
       +--> 进行中的 RPC Call        -> UNAVAILABLE
       |
       v
Channel 继续存活
       |
       +--> 客户端：Reactor 定时器 + 非阻塞连接器
       |       或
       +--> 服务端：应用接收替换套接字
       |
       v
新的 Reactor 所有 Connection
       |
       v
tr_channel_replace_connection()
       |
       v
HELLO / HELLO_ACK
       |
       v
新的 Stream / 新的 RPC Call
```

这个边界是有意的。传输层不会仅因为 TCP 故障就重放 RPC；
它无法知道操作是否幂等。通用 RPC 重试和备份专用续传都属于上层策略。

客户端自动重连通过以下配置启用：

```c
struct tr_channel_reconnect_config cfg = {
    .ipv4_address = "127.0.0.1",
    .control_port = 9000,
    .bulk_port = 9001,          /* 0 表示使用 control_port */
    .initial_delay_ms = 200,
    .max_delay_ms = 10000,
    .connect_timeout_ms = 5000,
};

tr_channel_enable_client_reconnect(channel, &cfg);
```

客户端重连完全由 Reactor 拥有：退避使用 Reactor 本地定时器，连接完成使用有界辅助 fd
连接器路径。不会为每个 Channel 创建维护线程。共享模式为两个通道重连一个物理连接；
拆分模式独立重连故障通道。

服务端不会自动接收或信任替换套接字。其监听/认证层接收套接字、交给 Reactor，
然后调用：

```c
tr_channel_replace_connection(channel, lane, new_connection);
```

这样监听器、TLS/认证和服务策略都保持在 Channel 之外。


### RPC V2 与 RAW Codec

普通 `tr_client_create()` / `tr_server_create()` 只构造 V2，方法表在创建时复制并
冻结。V2 使用 `tr_rpc_v2_call` 能力和记录式调用，不协商、不回退到旧 V1。
V2 支持 `ONE` / `MANY` 请求与响应组合；`TR_RPC_NONE` 尚不支持。工作函数、调用
回调、取消、元数据、终局与所有权规则见[公共 V2 SDK 使用合同](docs/rpc-v2-sdk.md)。

R14 已从源码和静态库移除 V1 raw Call、Endpoint、Executor Group 与旧 Wire 实现，
也删除了相应的安装声明及测试闭包。连接到只支持旧 V1 Wire 的对端不会得到兼容
回退。

`tr/rpc_codec.h` 保留独立 RAW Codec：`tr_rpc_bytes`、`TR_RPC_CODEC_RAW`、
`tr_rpc_raw_encode()` 和 `tr_rpc_raw_decode()`。编码会把输入复制到调用方提供的目标
缓冲区；解码只返回指向输入缓冲区的借用视图，不分配或复制数据，调用方须在使用该视图
期间保持输入存储有效。它不创建 Call、Method、Endpoint，也不编码旧 V1 RPC 头部。
RAW Codec 的存在不代表旧 V1 RPC 协议仍然可用。

端到端 V2 示例位于 `examples/echo_client.c` 和 `examples/echo_server.c`；公开安装
头文件的使用方式和错误处理以 V2 SDK 文档为准。

## 流量控制模型

对于每个逻辑 Stream：

```text
发送方：
    tx_sent_bytes <= peer_absolute_send_limit

接收方：
    rx_received_bytes <= rx_advertised_limit

上层消费字节后：
    rx_consumed_bytes += n
    new_limit = rx_consumed_bytes + configured_window
    WINDOW_UPDATE(stream_id, absolute=new_limit)
```

使用绝对上限可以避免更新被重试或合并时重复增加额度。

## 架构文档

架构设计以“核心范围、分层/API 边界、运行时、所有权、RPC、连接组、演进路线、架构决策记录”为主线，
入口见 [`docs/architecture/README.md`](docs/architecture/README.md)。

备份持久性/恢复只作为上层业务参考，不属于 c-trpc 核心契约。
文档使用中文状态说明区分当前实现、未来扩展、已退役设计的历史记录与业务参考，
避免把历史 V1 能力或业务规划误认为当前 RPC/传输合同。

## 重要所有权规则

详细 C11 所有权/自动清理规则见
[`docs/resource_ownership.md`](docs/resource_ownership.md)。
当前互斥锁所有权与锁删除审查见
[`docs/lock_ownership.md`](docs/lock_ownership.md)。

代码注释、API 契约说明、README、设计文档和测试说明都必须使用中文。
API、类型、变量、宏、协议字段、状态值、命令及机器可解析的日志/错误标记保持原样；
新写的纯人读说明使用中文。语言边界见
[项目语言约定](docs/PROJECT_LANGUAGE.md)，注释要求见
[`docs/code_comments.md`](docs/code_comments.md)。

- 词法作用域所有者在适合时应使用类型化 `TR_AUTO(...)` 自动清理；
- 所有权转移时使用显式 `*_take()` 辅助接口解除自动清理；
- 清理按声明逆序执行，因此声明顺序本身就是生命周期依赖；
- 异步共享使用者在发布前取得强引用，并通过匹配的释放操作归还；
- `tr_reactor_adopt_fd()` 返回 `TR_OK` 时，fd 所有权转移给 Reactor；
- `tr_reactor_send()` 返回 `TR_OK` 时，其载荷缓冲区所有权发生转移；
- `tr_reactor_sendv()` 返回 `TR_OK` 时，所有传入载荷缓冲区所有权发生转移；
- `tr_stream_send()` / `tr_stream_sendv()` 遵循相同的“仅成功时转移”规则；
- 内部 Buffer 快速路径保持相同的“仅成功时转移所有权”语义；
- 稳定发送 API 失败时，应用字节仍由调用方拥有；
- 命令一旦被有界命令环接受，即由命令环拥有其资源，即使后续 eventfd 唤醒系统调用失败也不回退所有权；
- `TR_STREAM_DATA_TAKE_OWNERSHIP` 要求之后显式释放；
- 释放 RX 载荷时才把对应字节额度归还给 Stream 接收窗口；仅收到网络数据不会补充应用窗口。

## 当前测试覆盖

传输层：

- 小端编码和 CRC32C 参考向量；
- 帧编解码以及损坏的头部/载荷 CRC；
- 每次一个字节的分片输入；
- 解析器缓冲资源池背压；
- 非法标志/保留字段；
- 有界命令环 / eventfd 唤醒行为；
- 真实非阻塞回环 TCP；
- 1 MiB 帧配合刻意缩小的套接字发送缓冲区，覆盖部分 `sendmsg()` / `EPOLLOUT`；
- RX 资源池耗尽 -> 暂停 `EPOLLIN` -> 释放 -> 恢复；
- Stream 打开和绝对字节窗口背压；
- 保留 RX 载荷 -> 释放 -> `WINDOW_UPDATE`；
- 拆分 CONTROL/BULK 通道的故障隔离；
- 1500 字节逻辑 Stream 消息分成 256 字节 DATA 帧，并精确重组为一次回调；
- 重组缓冲区保留/释放和流量控制额度归还；
- RPC 批量快速路径中的分散/聚集 TX 所有权；
- 共享 Channel 在真实 TCP 监听器上的客户端自动重连；
- 连接重连后故障 Stream 句柄变陈旧，Stream 槽位可重新使用；
- 服务端手动连接替换和 Channel UP 事件；
- 拆分模式下 BULK 替换时 CONTROL 连接保持活动；
- 通道进入 UP 前完成对称 `HELLO/HELLO_ACK` 能力协商；
- 非对称接收上限（一侧可接收 2048 字节消息，另一侧只声明 256 字节）；
- 协议版本不匹配时关闭通道，而不是进入 UP；
- `GOAWAY` 传播和优雅 `RUNNING -> DRAINING -> DRAINED` 生命周期；
- 排空过程中已有 Stream 流量继续运行，新 Stream 打开被拒绝。

RPC V2 与 Codec：

- `test_rpc_raw_codec` 验证 RAW 编码复制、解码借用视图、零长度、容量和参数边界；
- `test_rpc_record` 用固定 V1 字节样本确认 V2 解码器拒绝旧版本，不再链接旧 Wire；
- `test_rpc_v2_sdk` 通过真实 TCP 覆盖四种 ONE/MANY 基数组合、元数据、取消、期限、
  终局和排空，并作为安装 SDK 消费测试源码；
- `test_client_record`、`test_server_record` 与 `test_rpc_record_endpoint_tcp` 覆盖
  V2 记录、准入、期限、取消、终局、背压和跨流进展；
- `test_rpc_v2_server_destroy_drain` 验证已受理的 V2 工作阻塞期间服务端销毁重试；
- `test_rpc_message_ownership` 与 `test_task_executor` 保留通用所有权和执行器合同；
- RPC inventory 自测与 release 静态归档扫描拒绝精确的旧 V1 类型/符号，确认旧对象
  不存在，并核实 RAW Codec 仍由 `rpc_codec.c.o` 提供。

这些目标不再测试 V1 私有 Call 队列、旧 Executor Group 或旧 Wire 编解码器。

Xmake CI 矩阵检查：

- GCC 和 Clang 在 debug/release 模式下的构建与测试；
- AddressSanitizer + UndefinedBehaviorSanitizer；
- ThreadSanitizer；
- Make 兼容入口、编译器切换、安装后 SDK 消费；
- Echo 客户端/服务端作为独立进程运行。

## 有意暂未实现

- 接收端分散/聚集逻辑消息表示（分片 RX 当前执行一次有界重组复制）；
- 类型化编解码器注册表 / Protobuf 集成；
- 通用流式调用自动发送队列（流式调用方当前在可写通知后重试 `TR_AGAIN`）；
- 通用 RPC 自动重试 / 透明进行中 Call 重放；
- DNS/名称解析器以及非 IPv4 重连端点；
- RPC/服务健康检查（传输层保活/存活检测已经实现）；
- 如果未来有界 V2 调度扫描成为可测瓶颈，再按证据评估可扩展定时结构；
- 共享连接模式下严格 CONTROL 优先调度器（拆分模式已经提供物理隔离）；
- 多 DATA 连接资源池；
- TLS profile 原子轮换、旧代次撤销与 TLS 全部内部内存的硬预算；
- 备份语义 / 持久提交 / 备份续传 / 存储 / 文件系统 I/O；
- 拆分连接客户端/服务端门面的配对/绑定协议；
- 无锁命令队列 / io_uring / 内核零复制优化。

## 当前 RPC V2 限制

- 方法 codec 编号由应用约定；SDK 传递原始正文，不提供 Protobuf 等类型化编解码器；
- `TR_RPC_NONE` 尚不支持；V2 的 `ONE` / `MANY`、终局和所有权合同见
  [公共 SDK 文档](docs/rpc-v2-sdk.md)；
- 流式发送遇到 `TR_AGAIN` 时由调用方等待可写提示并重试；一元便利 API 另行负责
  OPEN、正文和半关闭的自动发送；
- 当前不提供透明 RPC 重试、进行中 Call 重放或 DNS 名称解析；消息压缩通过显式 API 选择，
  默认关闭；普通 V2 TLS 必需模式和连接组 mTLS 都须显式配置；
- 逻辑消息受配置的消息上限、传输窗口和重组资源约束；
- 普通 RPC V2 Client 当前不配置自动重连。低层 Channel 重连只恢复传输连通性，
  故障连接上已打开的 Stream/Call 已终止，应用须重新发起业务调用；
- 高层门面使用共享 CONTROL/BULK TCP 映射，低层 Channel API 仍提供拆分连接模式。

# 构建

要求 Linux、GCC 或 Clang，以及 **Xmake 3.1.1 或更新版本**。
项目语言基线为 ISO C11。GCC/Clang 的清理属性只通过类型化
`TR_AUTO()` 所有权辅助接口使用。

```sh
xmake f -m release --toolchain=gcc
xmake                         # 构建库与 Echo 示例
xmake test -v -j1             # 构建并运行全部测试可执行程序

# 只运行定时器队列测试：
xmake test -v -j1 'test_timer_queue/*'
```

`xmake.lua` 是唯一构建定义。产物位于
`build/<platform>/<architecture>/<mode>/`，不放入 `src/`、`tests/` 或 `examples/`。
模式包括 `debug`、`release`（默认）、`asan`（ASan + UBSan）和 `tsan`。
`release` 模式保持原有 `-O2 -g` 构建并启用断言；测试会显式取消定义 `NDEBUG`，
确保写在断言中的操作仍然执行。

`make`、`make test`、`make test-timer` 和 `make clean` 保留为轻量转发入口，
同样依赖 Xmake；不存在第二套 Make 构建图。
编译器切换、内存/线程检查器命令、SDK 安装和兼容选项见
[构建与测试指南](docs/build.md)。

## 下一里程碑

高层客户端/服务端门面、能力协商、优雅排空、保活/存活检测、诊断、
回调静默和运行时对端回收已经实现，V2 已提供显式公共 SDK。
V2 已支持回调外异步生产者的引用、有限背压及取消收口；Message 层压缩也已接入普通
Stream、RPC 业务正文和连接组 DATA，并保持默认关闭。真实业务负载、生产网络与发布性能阈值
仍须验收。其他生产工作包括：

1. 为拆分 CONTROL/BULK 门面增加连接绑定标识，使独立接收的套接字能够安全配对；
2. 共享连接模式下，在 DATA 帧边界实现严格 CONTROL 优先级；
3. 在保留 RAW 批量分片的同时增加类型化编解码器注册表，例如 Protobuf；
4. 为 TLS profile 增加跨 owner 原子轮换、旧代次撤销和完整资源验收；普通 V2 与连接组 TLS/mTLS 已接入，分别见 TLS-C 与 TLS-D1 记录；
5. 提供显式 RPC/服务健康检查，与传输层保活分离；
6. 只有标记为可重试/幂等的方法才允许使用通用 RPC 重试策略；
7. 增加模糊测试/长稳测试/基准测试套件和真实 RPC/BULK 性能分析；CRC32C 已有可移植/运行时选择加速后端（见 [CRC32C 后端](docs/crc32c.md)），可选 RX 向量消息仍由性能分析驱动。

备份始终位于该通用 RPC/传输库之上的应用层。


## 许可证

Copyright 2026 Black Hole。当前项目内容采用 [Apache-2.0](LICENSE)，署名见
[NOTICE](NOTICE)；第三方工具和历史 Linux 配置的范围见[许可说明](docs/licensing.md)。
