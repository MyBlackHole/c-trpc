# c-trpc

面向 Linux 的 C 传输/RPC 库，重点关注有界资源、显式所有权、高吞吐批量数据流，以及通用传输/RPC 与备份业务语义之间的清晰分离。

当前持续验证范围为 Ubuntu 22.04 / x86_64 的 C11 SDK；C++11 消费端校验公开头文件、C 链接约定和真实 V2 客户端／服务端调用。其他内核、架构与工具链须单独验证。部署安全、许可及兼容边界见 [平台与发布契约](docs/platform-support.md)。

当前 `2.0.0-dev.27` 候选基于 OpenSSL 3 TLS 1.3 和 mTLS 已接入普通 V2
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
