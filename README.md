# trpc_transport_v1

面向 Linux 3.x+ 的 C 传输/RPC 原型，重点关注有界资源、显式所有权、高吞吐批量数据流，以及通用传输/RPC 与备份业务语义之间的清晰分离。

## 架构

```text
应用/业务层
     |
     v
RPC 服务/方法
Call + 执行器
ONE/MANY <-> ONE/MANY
     |
RAW 控制消息
或 RAW 分片
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

## 运行时可观测性

运行时仍会收集无额外分配的结构化诊断数据，用于基准测试和内部瓶颈归因。
队列/资源池计数器与高水位仍保留在引擎内部；单调时钟计时直方图仍通过
仓库内部诊断调优按需启用。

详细的 Reactor/Channel/Endpoint 快照、计时标志以及诊断直方图/队列/资源池类型，
有意**不属于**稳定安装 SDK。仓库内基准测试和测试程序使用内部诊断接口。

稳定语义观测现在包括与内部布局无关的 RPC 生命周期快照，通过
`tr_client_get_rpc_semantic_stats()` 和
`tr_server_get_rpc_semantic_stats()` 获取，并保留现有连接组语义统计。
RPC 快照只暴露已经开始/已经结束/进行中的 Call 和最终 RPC 状态分布；
不会暴露工作线程、队列、资源池、Reactor、槽位或计时布局。
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

客户端生命周期：

```c
struct tr_client_config cfg;
struct tr_client *client;

tr_client_config_init(&cfg);
tr_client_create(&cfg, &client);
tr_client_connect(client, "127.0.0.1", 9000);
tr_client_register_method(client, &method);
tr_client_unary_call(client, ...);
tr_client_destroy(client);
```

服务端生命周期：

```c
struct tr_server_config cfg;
struct tr_server *server;
uint16_t bound_port;

tr_server_config_init(&cfg);
tr_server_create(&cfg, &server);
tr_server_register_method(server, &method, handler, arg);
tr_server_listen(server, "0.0.0.0", 9000, &bound_port);
tr_server_start(server);
...
tr_server_drain(server, 5000);
tr_server_destroy(server);
```

门面拥有 Reactor、RPC 消息资源池和 Channel 重组资源池。
稳定的 `tr_facade_limits` 描述应用可见的协议/并发语义，而不是当前
Reactor/资源池/执行器布局。命令/TX/RX、资源池数量、工作线程数量、
执行器节点容量和续处理预留都在内部推导；仓库基准测试/架构测试只能通过
不安装为 SDK API 的内部调优入口覆盖它们。

RPC 编码消息所有权使用有界内部槽位池，其缓冲区按需增长到语义
`max_message_bytes` 上限；安装后的 SDK 不暴露编码消息存储大小。

`tr_server_register_method()` / `tr_server_register_stream_method()`
在 V1 中属于启动前操作；每个已接收对端都会获得包含已注册方法表的 RPC Endpoint。
`tr_client_call_start*()` 在不暴露 Reactor/Connection 内部结构的前提下提供流式 Call。
返回的 Call 句柄继续使用现有 `tr_rpc_call_*()` API 完成调用级发送/取消/元数据操作。

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

tr_server_create(&cfg, &server);
tr_server_connection_group_listen(
    server, "0.0.0.0", 9100, 128, &group_port);
tr_server_start(server);
```

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
tr_client_create(&client_cfg, &client);
tr_client_connection_group_connect(
    client, "127.0.0.1", group_port, &group);
...
tr_client_connection_group_close(client);
tr_client_destroy(client);
```

门面复用现有单所有者 Pipeline 引擎。应用只看到 `group_id/epoch`、
逻辑 Stream/消息标识以及字节视图；Reactor 句柄、注册表状态、TRR1 解析、
所有者分片和成员代次全部保持内部化。
返回 `TR_CONNECTION_GROUP_MESSAGE_TAKE_OWNERSHIP` 会保留原始 RX 缓冲区，
不增加额外载荷复制，并要求之后调用一次
`tr_connection_group_message_release()`。

当 `max_data_connections > 0` 时，客户端在内部消费 `DATA_OFFER`：
打开非阻塞 DATA 套接字，通过所属 Reactor 等待连接/前导信息推进，
然后在同一所有者上提交精确 TRR1 DATA 路由。
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

V1 门面有意使用 `TR_CHANNEL_SHARED_CONNECTION`。客户端和服务端门面的
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
- 协议版本范围协商（V1 当前实现版本 1）；
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
- 客户端 V1 支持针对数字 IPv4 端点的可选自动重连；
- 有界指数退避重连，并支持配置连接超时；
- 服务端有意使用显式替换：应用接收并认证新套接字后再完成连接替换；
- Channel 发出 DOWN/UP/GOAWAY 事件，并暴露每通道 `DOWN / RECONNECTING / HANDSHAKING / UP` 状态；
- 连接替换后旧 Stream 不会存活；旧句柄全部变为陈旧句柄。

### 能力握手与优雅排空

物理 TCP 连接不会在接管后立即被视为 Channel 可用。双方都会发送固定大小的
`HELLO`，声明自身接收能力：

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

`begin_drain()` 是幂等操作，会禁用重连、拒绝新的本地 Stream，并在每个就绪通道上
发送 `GOAWAY`。收到 `GOAWAY` 后禁止向该对端打开新 Stream。
已有 Stream 可以继续完整运行，直到自然关闭/取消：

```text
RUNNING
   |
   | begin_drain + GOAWAY
   v
DRAINING  -- 已有 Stream 继续 -->
   |
   | active_streams == 0
   v
DRAINED
```

排空过程中传输层不会强制取消应用工作；RPC/应用截止时间决定未完成操作最多存活多久。

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


### RPC model

- `Method Descriptor`
  - service / method IDs
  - request / response cardinality
  - request / response codec IDs
  - CONTROL / BULK lane
  - per-message size limits
- generation-based `Call` handles bound to Transport Streams
- `ONE -> ONE` Unary compatibility API
- generic Streaming Call API using the same Call implementation
- executable V1 shapes:
  - `ONE -> ONE`
  - `ONE -> MANY` (server streaming)
  - `MANY -> ONE` (client streaming)
  - `MANY -> MANY` (bidirectional streaming)
- V1 request `MANY` means **1..N**, not 0..N: the first REQUEST is also the Method-open envelope
- `TR_RPC_NONE` is reserved for a future explicit method-open envelope and is not executable in V1
- Client `close_send()` closes only the request half; Server terminates streaming Calls with final `STATUS` through `finish(status)`
- `STATUS(OK)` requires exactly one RESPONSE for response cardinality `ONE`; `MANY` permits 0..N
- Client `FINISHED` is the normal application terminal barrier: no MESSAGE/WRITABLE/normal REMOTE_CLOSED follows it
- final streaming `STATUS` RPC envelope separate from ordinary response messages
- call-level deadline with monotonic local timers
- deadline propagation to the server as a relative reserved metadata value
- cooperative local/remote cancellation using the existing `CANCEL` RPC envelope
- bounded initial metadata side channel on the first message in each direction
- client Call events: opened, writable, remote-close, finished, error
- server streaming callbacks: open, message, half-close, writable, close
- cardinality enforcement is centralized; there is no separate RPC stack per streaming shape

### Deadline, cancellation, and metadata

Call creation now has optional extended APIs:

```c
tr_rpc_unary_call_ex(..., const struct tr_rpc_call_options *options, ...);
tr_rpc_call_start_ex(..., const struct tr_rpc_call_options *options, ...);
```

`timeout_ms` starts a local monotonic deadline. The client also places the
remaining relative timeout in reserved initial metadata so the server arms its
own monotonic deadline without depending on synchronized wall clocks.

Deadline expiry and explicit cancellation share the same Call terminal path:

```text
ACTIVE
  |
  +-- tr_rpc_call_cancel() ------> CANCELLED
  |
  +-- deadline expiry -----------> DEADLINE_EXCEEDED
                                      |
                                      +-> local completion callback
                                      +-> best-effort CANCEL envelope
                                      +-> Stream half-close
```

Cancellation is deliberately cooperative. It does **not** roll back application
side effects. A running handler can query:

```c
tr_rpc_call_is_cancelled(call, &status);
```

RPC deadlines use `CLOCK_MONOTONIC` and scan the bounded Call table.
Each RPC Endpoint registers one timer in its owning Reactor; standalone and
high-level Client/Server endpoints use the same owner-local deadline path.
There is no per-Endpoint deadline thread; scheduling is entirely Reactor-local.

RPC metadata has two bounded scopes:

**Initial metadata**

- maximum encoded block: 512 bytes
- user keys: lowercase ASCII `[a-z0-9_.-]`, maximum 63 bytes
- values: binary bytes
- duplicate keys are rejected in V1
- user keys starting with `:` are reserved for protocol use
- Client initial metadata is emitted with the first REQUEST
- Server initial metadata is emitted with the first RESPONSE

**Trailing metadata**

- maximum encoded block: 512 bytes
- V1 supports Server Streaming -> Client trailers
- trailers are carried only by final STATUS
- Client may read trailers from the FINISHED callback
- Unary V1 has no independent STATUS envelope, so Unary trailers are not yet supported
- STATUS metadata never overwrites first-RESPONSE initial metadata

Each Call keeps bounded local/peer initial and trailing storage; there is no dynamic metadata queue. Metadata remains separate from the application RAW payload.

The RPC wire header stays 32 bytes. When metadata is present the body is:

```text
RPC header (32 B)
metadata_len (LE16)
metadata TLVs
application payload
```

`payload_len` continues to describe only the application payload. The sender
bulk fast path therefore remains scatter/gather friendly; a first bulk message
can be sent as a small RPC header/metadata slice plus the original application
buffer.

Public Context/metadata helpers:

```c
tr_rpc_call_get_context(...);

tr_rpc_call_set_metadata(...);
tr_rpc_call_get_peer_metadata(...);

tr_rpc_call_set_trailing_metadata(...);
tr_rpc_call_get_peer_trailing_metadata(...);
```

`tr_rpc_context` is a read-only owner-consistent snapshot of Method identity,
cardinality, relative deadline state, and cancellation state. It intentionally
does not expose Endpoint/Stream/Reactor/slot identity.

### RPC Interceptor V1

Client/Server can install one fixed Call-level interceptor:

```c
struct tr_rpc_interceptor {
    tr_rpc_interceptor_fn fn;
    void *arg;
};
```

Phases:

```text
CLIENT_PRE_CALL
SERVER_PRE_HANDLER
SERVER_POST_HANDLER
CLIENT_POST_CALL
```

The hook runs synchronously on the owning Reactor and must be short/non-blocking.
It may use Call Context/metadata APIs, but same-Call protocol mutation
(`send/close_send/finish/cancel`) is rejected with `TR_ERR_STATE` while the
hook is active.

`SERVER_PRE_HANDLER` is the only V1 phase whose return value controls the Call:
`OK` continues, while a valid non-OK RPC status rejects before the application
handler runs. Other phases are observational/metadata hooks and should return
`OK`.

Typical usage:

```text
CLIENT_PRE_CALL
  -> add trace/auth initial metadata

SERVER_PRE_HANDLER
  -> read auth/tenant/trace metadata
  -> optional lightweight RPC rejection

SERVER_POST_HANDLER
  -> add trailers / finish metrics

CLIENT_POST_CALL
  -> read trailers / finish trace
```

V1 deliberately has no dynamic interceptor chain and no blocking/async interceptor
continuation.

Server handlers can read request metadata and set response metadata before the
first response is encoded.

### RPC executor boundary

- application RPC callbacks never execute on the network Reactor thread
- bounded fixed-capacity executor task-node pool per RPC endpoint
- optional Server continuation reserve partitions admission logically inside that same node pool: new Unary / first Streaming tasks stop at `capacity - reserve`, while already-accepted Streaming message/half-close/writable/close tasks may use the full capacity; it is not a second queue and does not change worker scheduling
- low-level standalone RPC endpoints retain their own configurable worker pool
- the high-level Server facade creates one bounded shard-local worker pool shared by peer RPC endpoints; exact worker/node counts are internal tuning, not stable facade API
- one FIFO task queue per Call plus an endpoint-local ready-Call queue
- the shared Server executor schedules ready endpoints while preserving endpoint-local Call ordering
- at most one executor task for a Call runs at a time, so callbacks for one Call remain strictly serialized and ordered
- different Calls, including Calls from different peers, may run concurrently on shared workers
- one task is taken per ready-Call scheduling turn, providing fairness without letting thread count scale with accepted peers
- if a Server Unary request reaches a full executor queue, its handler is not invoked; the Call completes with `RESOURCE_EXHAUSTED` while the Channel remains usable
- `UNAVAILABLE` remains reserved for connection/transport loss rather than local executor admission failure
- incoming RPC payload ownership is held until executor processing completes
- this naturally delays Stream receive-credit return while application processing is outstanding
- each queued executor task holds a C11 strong reference on its RPC Endpoint
- Call `task_refs` remain only for Call-slot reuse; Endpoint lifetime uses `tr_refcount`

The executor implementation therefore separates network progress from application latency without sacrificing in-Call message ordering. Server worker count is now O(1) with respect to peer count; endpoint task queues remain independently bounded.

### RAW codec and bulk fast path

The stable application API uses `tr_rpc_call_send()` with a borrowed
`tr_rpc_bytes` view.

The engine still retains an internal copy-minimal Buffer fast path:

```text
32-byte RPC envelope buffer  ----\
                                 +--> Transport scatter/gather
original application buffer  ----/          |
                                           v
                                       sendmsg(iovec)
```

That path remains available to internal benchmarks/tests while the public
zero-copy abstraction is redesigned around an opaque ownership API instead of
publishing allocator/Channel/Reactor structures. No extra copy was added by the
API-boundary cleanup itself.

### RPC retained-message ownership

Streaming callbacks receive:

```c
struct tr_rpc_message {
    struct tr_rpc_bytes bytes;
    uintptr_t _private[TR_RPC_MESSAGE_PRIVATE_WORDS];
};
```

Only `bytes` is application-visible. `_private` is an opaque fixed-size
release capability; callers retaining a message must copy the descriptor
unchanged and never inspect those words.

Returning `TR_RPC_MESSAGE_RELEASE` returns the RX buffer after the callback.

Returning `TR_RPC_MESSAGE_TAKE_OWNERSHIP` transfers the buffer to the callback owner; it must later call `tr_rpc_message_release()`. This also delays receive-window credit until the buffer is actually consumed.

## Flow-control model

For each logical Stream:

```text
sender:
    tx_sent_bytes <= peer_absolute_send_limit

receiver:
    rx_received_bytes <= rx_advertised_limit

when upper layer consumes bytes:
    rx_consumed_bytes += n
    new_limit = rx_consumed_bytes + configured_window
    WINDOW_UPDATE(stream_id, absolute=new_limit)
```

Absolute limits avoid duplicate-credit bugs if updates are retried or coalesced.

## Architecture documents

架构设计以“Core scope、分层/API 边界、Runtime、Ownership、RPC、Connection Group、演进路线、ADR”为主线，入口见
[`docs/architecture/README.md`](docs/architecture/README.md)。

Backup durability/recovery 只作为上层业务参考，不属于 c-trpc core contract。
文档区分 CURRENT、TARGET V1、FUTURE 与 BUSINESS REFERENCE，避免把业务规划误认为 RPC/Transport 必须实现的能力。

## Important ownership rules

The detailed C11 ownership/automatic-cleanup rules are documented in
[`docs/resource_ownership.md`](docs/resource_ownership.md). Current mutex
ownership and the lock-removal audit are documented in
[`docs/lock_ownership.md`](docs/lock_ownership.md).

代码注释和 API 契约说明以中文为主。ownership、refcount、quiescence、
Reactor、Channel、RPC、Call、Stream 等与实现结构直接对应的术语保留英文。
具体规范见 [`docs/code_comments.md`](docs/code_comments.md)。

- lexical owners should use typed `TR_AUTO(...)` cleanup where practical
- explicit `*_take()` helpers disarm automatic cleanup when ownership moves
- cleanup runs in reverse declaration order, so declaration order is a lifetime dependency
- asynchronous shared users acquire a strong reference before publication and release it with a matching put
- `tr_reactor_adopt_fd()` returning `TR_OK` transfers fd ownership to Reactor.
- `tr_reactor_send()` returning `TR_OK` transfers its payload buffer.
- `tr_reactor_sendv()` returning `TR_OK` transfers every supplied payload buffer.
- `tr_stream_send()` / `tr_stream_sendv()` follow the same success-only transfer rule.
- internal Buffer fast paths preserve success-only ownership transfer.
- any failed stable send leaves caller-owned application bytes with the caller.
- a command accepted by the bounded command ring owns its resources even if a later eventfd wake syscall fails.
- `TR_STREAM_DATA_TAKE_OWNERSHIP` and `TR_RPC_MESSAGE_TAKE_OWNERSHIP` require explicit later release.
- releasing an RX payload returns its byte credit to the Stream receive window; network receipt alone does not replenish the application window.

## Tests currently cover

Transport:

- little-endian encoding and CRC32C reference vector
- frame encode/decode and corrupt header/payload CRC
- one-byte-at-a-time fragmentation
- parser buffer-pool backpressure
- invalid flags/reserved fields
- bounded command ring / eventfd wake behavior
- real nonblocking loopback TCP
- 1 MiB frame with deliberately small socket send buffer, exercising partial `sendmsg()` / `EPOLLOUT`
- RX pool exhaustion -> pause `EPOLLIN` -> release -> resume
- Stream open and absolute byte-window backpressure
- retained RX payload -> release -> `WINDOW_UPDATE`
- split CONTROL/BULK lane failure isolation
- logical 1500-byte Stream message fragmented into 256-byte DATA frames and reassembled into exactly one callback
- reassembly-buffer retain/release and returned flow-control credit
- scatter/gather TX ownership through the RPC bulk fast path
- shared Channel automatic client reconnect over a real TCP listener
- failed Stream handles become stale and Stream slots are reusable after reconnect
- server-side manual connection replacement and Channel UP events
- split mode BULK replacement while the CONTROL connection remains live
- symmetric HELLO/HELLO_ACK capability negotiation before lane UP
- asymmetric receive limits (one side may accept 2048-byte messages while the other advertises only 256 bytes)
- protocol-version mismatch closes the lane instead of entering UP
- GOAWAY propagation and graceful `RUNNING -> DRAINING -> DRAINED` lifecycle
- existing Stream traffic continues during drain while new Stream opens are rejected

RPC:

- RPC envelope + RAW codec encode/decode
- real TCP Unary request -> executor handler -> response -> half-close
- real TCP `ONE -> MANY` server streaming
- real TCP `MANY -> ONE` client streaming
- real TCP `MANY -> MANY` bidirectional streaming
- RAW bulk `send_buffer()` sender-side slice path
- response final `STATUS` and Call finish
- STATUS service/method/codec/status-domain validation
- `STATUS(OK) + response ONE` requires exactly one RESPONSE
- Client `FINISHED` remains the last normal application event
- V1 zero-message MANY request half-close is rejected until an explicit Method-open envelope exists
- Server `close_send()` is rejected; response termination must go through final STATUS
- RPC message retain/release path
- cardinality enforcement (`ONE` rejects a second message)
- executor task lifetime / Call generation ownership
- initial request/response metadata over real TCP
- explicit client cancellation propagated to the server
- server handler cancellation observation through `tr_rpc_call_is_cancelled()`
- short client deadline producing `DEADLINE_EXCEEDED` locally and remotely
- service-side completion when the peer was already half-closed
- multi-worker executor: separate Calls execute concurrently while eight messages on one Streaming Call remain strictly serialized
- high-level Server shared executor: four peer Calls with two configured workers never run more than two handlers concurrently
- RPC Endpoint deadlines run on Reactor-local timers without per-Endpoint timer threads
- Channel keepalive runs on Reactor-local timers without a private timer thread
- facade create/start/destroy thread accounting and seven thread-start failure points, using test-only pthread wrapping
- large Unary RPC request/response (1500/1700 bytes) transparently fragmented/reassembled with a 256-byte Transport frame limit
- in-flight Unary interrupted by connection loss completes as `UNAVAILABLE` and is not replayed
- a new Unary succeeds on replacement Connections without rebuilding RPC endpoints
- high-level `tr_server` + `tr_client` facade performs a real TCP Unary RPC end-to-end
- standalone `examples/echo_server` and `examples/echo_client` run as separate processes
- Reactor callback quiescence during RPC/Channel teardown
- server facade peer reclamation with `max_peers=1` across sequential clients
- failed high-level Client connect attempts roll back and can be retried on the same Client object
- C11 refcount invariants: no resurrection, no underflow, no saturation overflow
- server peer retirement while a shared RPC handler is still running; Endpoint/Channel lifetime remains valid until the task reference drains

The Xmake CI matrix checks:

- GCC and Clang builds/tests in both debug and release modes
- AddressSanitizer + UndefinedBehaviorSanitizer
- ThreadSanitizer
- Make compatibility entry points, compiler switching, installed SDK consumption,
  and the Echo client/server as separate processes

## Deliberately not implemented yet

- receiver-side scatter/gather logical-message representation (fragmented RX currently performs one bounded reassembly copy)
- typed codec registry / Protobuf integration
- automatic generic Streaming send queue (Streaming caller currently retries `TR_AGAIN` after writable notification)
- automatic generic RPC retry / transparent in-flight Call replay
- DNS/name resolver and non-IPv4 reconnect endpoints
- RPC/service Health service (Transport keepalive/liveness is implemented)
- scalable timer wheel if bounded endpoint scans become measurable
- strict CONTROL priority scheduler in shared-connection mode (split mode provides physical isolation)
- multi-data-connection pool
- TLS/mTLS transport provider integration
- backup semantics / durable commit / backup resume / storage / filesystem I/O
- split-connection Client/Server facade pairing/binding protocol
- lock-free command queue / io_uring / kernel zerocopy optimizations

## Current V1 constraints

- one Transport frame remains bounded by `max_payload_len`; one logical Stream/RPC message may span many DATA frames
- fragmented receive requires a configured bounded Channel `reassembly_pool`; `max_message_bytes` must fit one reassembly buffer
- fragmented RX currently becomes one contiguous buffer before RPC dispatch; single-frame RX remains zero-copy
- TX logical-message size is bounded by the current 32-bit message/reassembly sizing policy and configured flow-control limits
- executor workers run different Calls in parallel, but one Call is intentionally serialized and can therefore be delayed by its own slow handler
- generic Streaming writes do not internally queue arbitrary application messages: `TR_AGAIN` is intentional backpressure and the caller retries after `TR_RPC_CALL_EVENT_WRITABLE` / server `on_writable`
- Server executor saturation before the first Streaming handler callback is an admission rejection and returns final `RESOURCE_EXHAUSTED`; after callbacks have begun, one continuation task per Call may wait outside the executor while retaining its RX payload/credit, and a second not-yet-admitted task on that Call terminates only that Call with final `RESOURCE_EXHAUSTED` (earlier callbacks may already have produced side effects)
- the Server executor supports an internal continuation reserve that can keep bounded task nodes available to already-accepted Streaming continuation/lifecycle work; the stable facade does not expose node-count tuning, and future application-facing admission policy should use semantic limits instead
- direct destruction must not run from a Reactor callback; RPC/Channel teardown now uses a Reactor quiescence barrier, while normal shutdown should still drain application work first
- reconnect restores Channel connectivity only; all Streams from the failed physical connection are terminal and must be recreated
- automatic Client reconnect is Reactor-owned and does not create a per-Channel thread; backoff is timer-driven and connect completion is nonblocking
- server connection replacement remains explicit so accept/TLS/authentication policy stays outside the generic Channel
- RPC Endpoint deadlines, Channel keepalive, and automatic reconnect scheduling are Reactor-local
- the high-level Client/Server facade currently uses shared CONTROL/BULK TCP mapping; split mode remains available through the lower-level Channel API

## Build

Requires Linux, GCC or Clang, and **Xmake 3.1.1 or newer**. The project language
baseline is ISO C11. GCC/Clang's cleanup attribute is used only through the
typed `TR_AUTO()` ownership helper.

```sh
xmake f -m release --toolchain=gcc
xmake                         # library and Echo examples
xmake test -v -j1              # builds and runs all test executables

# Run only the timer queue tests:
xmake test -v -j1 'test_timer_queue/*'
```

`xmake.lua` is the only build definition. Artifacts live under
`build/<platform>/<architecture>/<mode>/`, not in `src/`, `tests/` or `examples/`.
The modes are `debug`, `release` (default), `asan` (ASan + UBSan), and `tsan`.
Release preserves the original `-O2 -g` build and enabled assertions; tests
explicitly undefine `NDEBUG` so their assertion-contained operations still run.

`make`, `make test`, `make test-timer`, and `make clean` remain thin forwarding
entry points and also require Xmake; there is no second Make build graph.
See the [build and test guide](docs/build.md) for compiler switching, sanitizer
commands, SDK installation, and compatibility options.

## Next milestone

The high-level Client/Server facade, capability negotiation, graceful drain,
keepalive/liveness, diagnostics, callback quiescence, and runtime peer
reclamation are implemented. The next production work should focus on:

1. split CONTROL/BULK facade binding identity so independently accepted sockets can be paired securely.
2. strict CONTROL priority at DATA-frame boundaries in shared-connection mode.
3. typed codec registry (for example Protobuf) while retaining RAW bulk slices.
4. TLS/mTLS provider integration before a connection enters Channel HELLO.
5. explicit RPC/service Health service, separate from Transport keepalive.
6. generic RPC retry policy only for methods marked retryable/idempotent.
7. fuzz/soak/benchmark suites and real RPC/BULK profiling; CRC32C has portable/runtime-selected acceleration (see [CRC32C backends](docs/crc32c.md)), while optional RX vectored messages remain profile-driven.

Backup remains an application layer above this generic RPC/Transport library.
