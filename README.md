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


### RPC 模型

- 方法描述符
  - 服务/方法标识；
  - 请求/响应基数；
  - 请求/响应编解码器标识；
  - CONTROL / BULK 通道；
  - 每消息大小限制；
- 基于代次的 `Call` 句柄，与传输层 Stream 绑定；
- `ONE -> ONE` 一元调用兼容 API；
- 使用同一 Call 实现的通用流式调用 API；
- V1 可执行形态：
  - `ONE -> ONE`；
  - `ONE -> MANY`（服务端流式）；
  - `MANY -> ONE`（客户端流式）；
  - `MANY -> MANY`（双向流式）；
- V1 请求方向的 `MANY` 表示 **1..N**，不是 0..N：第一条 `REQUEST` 同时也是 Method 打开信封；
- `TR_RPC_NONE` 为未来显式 Method 打开信封保留，V1 不可执行；
- 客户端 `close_send()` 只关闭请求方向；服务端通过 `finish(status)` 发送最终 `STATUS` 来终止流式 Call；
- `STATUS(OK)` 对响应基数 `ONE` 要求恰好一条 `RESPONSE`；`MANY` 允许 0..N；
- 客户端 `FINISHED` 是正常应用终止屏障：之后不会再出现 `MESSAGE/WRITABLE` 或正常 `REMOTE_CLOSED`；
- 流式调用最终 `STATUS` 使用独立 RPC 信封，与普通响应消息分离；
- Call 级截止时间使用本地单调时钟定时器；
- 截止时间通过保留元数据中的相对值传播到服务端；
- 使用现有 `CANCEL` RPC 信封实现协作式本地/远端取消；
- 每个方向第一条消息支持有界初始元数据旁路；
- 客户端 Call 事件：打开、可写、远端关闭、结束、错误；
- 服务端流式回调：打开、消息、半关闭、可写、关闭；
- 基数约束统一执行，不为每种流式形态建立独立 RPC 栈。

### 截止时间、取消与元数据

Call 创建现在提供可选扩展 API：

```c
tr_rpc_unary_call_ex(..., const struct tr_rpc_call_options *options, ...);
tr_rpc_call_start_ex(..., const struct tr_rpc_call_options *options, ...);
```

`timeout_ms` 启动本地单调时钟截止时间。客户端还会把剩余相对超时时间放入
保留初始元数据，使服务端能够在不依赖墙上时钟同步的情况下建立自己的单调时钟截止时间。

截止时间到期和显式取消共用同一条 Call 终止路径：

```text
ACTIVE
  |
  +-- tr_rpc_call_cancel() ------> CANCELLED
  |
  +-- 截止时间到期 ------------> DEADLINE_EXCEEDED
                                      |
                                      +-> 本地完成回调
                                      +-> 尽力发送 CANCEL 信封
                                      +-> Stream 半关闭
```

取消有意采用协作式语义。它**不会**回滚应用副作用。正在运行的处理器可以查询：

```c
tr_rpc_call_is_cancelled(call, &status);
```

RPC 截止时间使用 `CLOCK_MONOTONIC`，并扫描有界 Call 表。
每个 RPC Endpoint 在所属 Reactor 中注册一个定时器；独立 Endpoint 和高层
客户端/服务端 Endpoint 都使用同一条所有者本地截止时间路径。
不会为每个 Endpoint 创建截止时间线程，调度全部位于 Reactor 本地。

RPC 元数据有两个有界范围：

**初始元数据**

- 最大编码块：512 字节；
- 用户键：小写 ASCII `[a-z0-9_.-]`，最大 63 字节；
- 值：二进制字节；
- V1 拒绝重复键；
- 以 `:` 开头的用户键保留给协议使用；
- 客户端初始元数据随第一条 `REQUEST` 发送；
- 服务端初始元数据随第一条 `RESPONSE` 发送。

**尾部元数据**

- 最大编码块：512 字节；
- V1 支持服务端流式调用 -> 客户端尾部元数据；
- 尾部元数据只由最终 `STATUS` 携带；
- 客户端可在 `FINISHED` 回调中读取尾部元数据；
- 一元调用 V1 没有独立 `STATUS` 信封，因此暂不支持一元尾部元数据；
- `STATUS` 元数据不会覆盖第一条 `RESPONSE` 的初始元数据。

每个 Call 都保持有界的本地/对端初始和尾部存储；不存在动态元数据队列。
元数据与应用 RAW 载荷保持分离。

RPC 线协议头仍为 32 字节。有元数据时，消息体为：

```text
RPC 头部（32 字节）
metadata_len（LE16）
metadata TLV
应用载荷
```

`payload_len` 仍只描述应用载荷。因此发送端批量快速路径仍适合分散/聚集发送；
第一条批量消息可以由一个小型 RPC 头部/元数据分片加原始应用缓冲区组成。

公开上下文/元数据辅助 API：

```c
tr_rpc_call_get_context(...);

tr_rpc_call_set_metadata(...);
tr_rpc_call_get_peer_metadata(...);

tr_rpc_call_set_trailing_metadata(...);
tr_rpc_call_get_peer_trailing_metadata(...);
```

`tr_rpc_context` 是所有者一致的只读快照，包含 Method 标识、基数、
相对截止时间状态与取消状态。它有意不暴露 Endpoint/Stream/Reactor/槽位标识。

### RPC V1 拦截器

客户端/服务端可以安装一个固定 Call 级拦截器：

```c
struct tr_rpc_interceptor {
    tr_rpc_interceptor_fn fn;
    void *arg;
};
```

阶段：

```text
CLIENT_PRE_CALL
SERVER_PRE_HANDLER
SERVER_POST_HANDLER
CLIENT_POST_CALL
```

钩子同步运行在所属 Reactor 上，必须短小且非阻塞。
它可以使用 Call 上下文/元数据 API，但同一 Call 的协议修改
（`send/close_send/finish/cancel`）在钩子活动期间会被拒绝并返回
`TR_ERR_STATE`。

`SERVER_PRE_HANDLER` 是 V1 唯一允许返回值控制 Call 的阶段：
返回 `OK` 继续；返回合法非 `OK` RPC 状态时，在应用处理器运行前拒绝。
其他阶段用于观测/元数据处理，建议返回 `OK`。

典型用途：

```text
CLIENT_PRE_CALL
  -> 添加链路追踪/认证初始元数据

SERVER_PRE_HANDLER
  -> 读取认证/租户/链路追踪元数据
  -> 可选轻量 RPC 拒绝

SERVER_POST_HANDLER
  -> 添加尾部元数据 / 完成指标

CLIENT_POST_CALL
  -> 读取尾部元数据 / 结束链路追踪
```

V1 有意不提供动态拦截器链，也不提供阻塞/异步拦截器续处理。

服务端处理器可以读取请求元数据，并在第一条响应编码前设置响应元数据。

### RPC 执行器边界

- 应用 RPC 回调绝不在网络 Reactor 线程上执行；
- 每个 RPC Endpoint 使用有界固定容量执行器任务节点池；
- 可选服务端续处理预留在同一节点池内进行逻辑准入分区：新的 Unary/第一条 Streaming 任务在 `capacity - reserve` 处停止准入，已经接受的 Streaming 消息/半关闭/可写/关闭任务可以使用完整容量；它不是第二个队列，也不改变工作线程调度；
- 低层独立 RPC Endpoint 保留自己的可配置工作线程池；
- 高层服务端门面创建一个有界分片本地工作线程池，由同分片对端 RPC Endpoint 共享；精确工作线程/节点数量属于内部调优，不是稳定门面 API；
- 每个 Call 一条 FIFO 任务队列，再加一条 Endpoint 本地就绪 Call 队列；
- 共享服务端执行器调度就绪 Endpoint，同时保持 Endpoint 内部 Call 顺序；
- 同一个 Call 同时最多运行一个执行器任务，因此该 Call 的回调严格串行且有序；
- 不同 Call（包括来自不同对端的 Call）可以在共享工作线程上并行执行；
- 每次就绪 Call 调度轮次只取一个任务，在不让线程数随对端数增长的同时提供公平性；
- 服务端一元请求到达已满执行器队列时，不调用其处理器；该 Call 以 `RESOURCE_EXHAUSTED` 结束，而 Channel 保持可用；
- `UNAVAILABLE` 保留给连接/传输丢失，不用于本地执行器准入失败；
- 入站 RPC 载荷所有权保持到执行器处理结束；
- 因此应用处理未完成时自然延迟 Stream 接收额度归还；
- 每个已排队执行器任务都持有 RPC Endpoint 的一个 C11 强引用；
- Call 的 `task_refs` 只用于 Call 槽位复用；Endpoint 生命周期使用 `tr_refcount`。

因此，执行器实现把网络推进与应用延迟分离，同时不牺牲同一 Call 内消息顺序。
服务端工作线程数量相对于对端数量保持 O(1)，各 Endpoint 任务队列仍独立有界。

### RAW 编解码与批量快速路径

稳定应用 API 使用 `tr_rpc_call_send()` 和借用的
`tr_rpc_bytes` 视图。

引擎内部仍保留尽量少复制的 Buffer 快速路径：

```text
32 字节 RPC 信封缓冲区 ----\
                            +--> 传输层分散/聚集
原始应用缓冲区 ----------/          |
                                      v
                                  sendmsg(iovec)
```

在围绕不透明所有权 API 重新设计公开零复制抽象期间，该路径仍供内部基准测试/测试使用，
不会发布分配器/Channel/Reactor 结构。API 边界清理本身没有增加额外复制。

### RPC 保留消息所有权

流式回调接收：

```c
struct tr_rpc_message {
    struct tr_rpc_bytes bytes;
    uintptr_t _private[TR_RPC_MESSAGE_PRIVATE_WORDS];
};
```

应用只可见 `bytes`。`_private` 是固定大小的不透明释放能力；
调用方保留消息时必须原样复制描述符，绝不能检查这些字段。

返回 `TR_RPC_MESSAGE_RELEASE` 会在回调结束后归还 RX 缓冲区。

返回 `TR_RPC_MESSAGE_TAKE_OWNERSHIP` 会把缓冲区转移给回调所有者；
之后必须调用 `tr_rpc_message_release()`。在缓冲区真正消费前，
接收窗口额度也会继续被占用。


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
文档使用中文状态说明区分当前实现、V1 目标、未来扩展与业务参考，
避免把业务规划误认为 RPC/传输层必须实现的能力。

## 重要所有权规则

详细 C11 所有权/自动清理规则见
[`docs/resource_ownership.md`](docs/resource_ownership.md)。
当前互斥锁所有权与锁删除审查见
[`docs/lock_ownership.md`](docs/lock_ownership.md)。

代码注释、API 契约说明、README、设计文档和测试说明都必须使用中文。
API、类型、变量、宏、协议字段、状态值、命令、日志和错误原文保持原样；
具体规则见 [`docs/code_comments.md`](docs/code_comments.md)。

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
- `TR_STREAM_DATA_TAKE_OWNERSHIP` 和 `TR_RPC_MESSAGE_TAKE_OWNERSHIP` 都要求之后显式释放；
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

RPC：

- RPC 信封 + RAW 编解码；
- 真实 TCP 一元请求 -> 执行器处理器 -> 响应 -> 半关闭；
- 真实 TCP `ONE -> MANY` 服务端流式调用；
- 真实 TCP `MANY -> ONE` 客户端流式调用；
- 真实 TCP `MANY -> MANY` 双向流式调用；
- RAW 批量 `send_buffer()` 发送端分片路径；
- 响应最终 `STATUS` 和 Call 结束；
- `STATUS` 的服务/方法/编解码器/状态域校验；
- `STATUS(OK) + response ONE` 要求恰好一条 `RESPONSE`；
- 客户端 `FINISHED` 保持最后一个正常应用事件；
- 在存在显式 Method 打开信封前，V1 拒绝零消息 MANY 请求半关闭；
- 服务端拒绝 `close_send()`；响应终止必须经过最终 `STATUS`；
- RPC 消息保留/释放路径；
- 基数强制约束（`ONE` 拒绝第二条消息）；
- 执行器任务生命周期 / Call 代次所有权；
- 真实 TCP 上的初始请求/响应元数据；
- 显式客户端取消传播到服务端；
- 服务端处理器通过 `tr_rpc_call_is_cancelled()` 观察取消；
- 短客户端截止时间在本地和远端产生 `DEADLINE_EXCEEDED`；
- 对端已经半关闭时的服务端完成；
- 多工作线程执行器：不同 Call 并发执行，同时一个流式 Call 的八条消息仍严格串行；
- 高层服务端共享执行器：4 个对端 Call 配置 2 个工作线程时，同时运行的处理器不超过 2 个；
- RPC Endpoint 截止时间运行在 Reactor 本地定时器上，不创建每 Endpoint 定时器线程；
- Channel 保活运行在 Reactor 本地定时器上，不创建私有定时器线程；
- 门面创建/启动/销毁线程记账和 7 个线程启动故障点，使用仅测试目标的 pthread 包装；
- 大型一元 RPC 请求/响应（1500/1700 字节）在 256 字节传输帧上自动分片/重组；
- 进行中的一元调用被连接故障打断时以 `UNAVAILABLE` 结束，不执行重放；
- 替换 Connection 后可以在不重建 RPC Endpoint 的情况下成功执行新一元调用；
- 高层 `tr_server` + `tr_client` 门面通过真实 TCP 完成端到端一元 RPC；
- 独立 `examples/echo_server` 与 `examples/echo_client` 作为两个进程运行；
- RPC/Channel 销毁期间的 Reactor 回调静默；
- 服务端门面在 `max_peers=1` 下跨多个顺序客户端回收对端；
- 高层客户端连接失败会回滚，并允许在同一个 Client 对象上重试；
- C11 引用计数不变量：不允许复活、不允许下溢、不允许饱和溢出；
- 共享 RPC 处理器仍在运行时服务端对端进入退役；任务引用耗尽前 Endpoint/Channel 生命周期保持有效。

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
- 如果未来有界 Endpoint 扫描成为可测瓶颈，再引入可扩展定时轮；
- 共享连接模式下严格 CONTROL 优先调度器（拆分模式已经提供物理隔离）；
- 多 DATA 连接资源池；
- TLS/mTLS 传输提供者集成；
- 备份语义 / 持久提交 / 备份续传 / 存储 / 文件系统 I/O；
- 拆分连接客户端/服务端门面的配对/绑定协议；
- 无锁命令队列 / io_uring / 内核零复制优化。

## 当前 V1 限制

- 单个传输帧仍受 `max_payload_len` 限制；一个逻辑 Stream/RPC 消息可以跨多个 DATA 帧；
- 分片接收要求配置有界 Channel `reassembly_pool`；`max_message_bytes` 必须能放入一个重组缓冲区；
- 分片 RX 当前在 RPC 分发前重组为一个连续缓冲区；单帧 RX 仍保持零复制；
- TX 逻辑消息大小受当前 32 位消息/重组大小策略和已配置流量控制限制约束；
- 执行器工作线程可以并行运行不同 Call，但单个 Call 有意串行，因此可能被自身慢处理器延迟；
- 通用流式写入不会在内部排队任意应用消息：`TR_AGAIN` 是有意的背压，调用方在 `TR_RPC_CALL_EVENT_WRITABLE` / 服务端 `on_writable` 后重试；
- 服务端执行器在第一条流式处理器回调前饱和属于准入拒绝，并返回最终 `RESOURCE_EXHAUSTED`；回调已经开始后，每个 Call 最多允许一个续处理任务在执行器外等待并保留 RX 载荷/额度，同一 Call 第二个尚未准入任务会只终止该 Call 并返回最终 `RESOURCE_EXHAUSTED`（更早回调可能已经产生副作用）；
- 服务端执行器支持内部续处理预留，可以为已经接受的流式续处理/生命周期工作保留有界任务节点；稳定门面不暴露节点数量调优，未来面向应用的准入策略应使用语义上限；
- 不能从 Reactor 回调直接执行销毁；RPC/Channel 销毁现在使用 Reactor 静默屏障，正常关闭仍应先排空应用工作；
- 重连只恢复 Channel 连通性；故障物理连接上的全部 Stream 都已终止，必须重新创建；
- 客户端自动重连由 Reactor 拥有，不创建每 Channel 线程；退避由定时器驱动，连接完成使用非阻塞路径；
- 服务端连接替换保持显式方式，使接收/TLS/认证策略位于通用 Channel 之外；
- RPC Endpoint 截止时间、Channel 保活和自动重连调度都位于 Reactor 本地；
- 高层客户端/服务端门面当前使用共享 CONTROL/BULK TCP 映射；低层 Channel API 仍提供拆分模式。

## 构建

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
release 保持原有 `-O2 -g` 构建并启用断言；测试会显式取消定义 `NDEBUG`，
确保写在断言中的操作仍然执行。

`make`、`make test`、`make test-timer` 和 `make clean` 保留为轻量转发入口，
同样依赖 Xmake；不存在第二套 Make 构建图。
编译器切换、sanitizer 命令、SDK 安装和兼容选项见
[构建与测试指南](docs/build.md)。

## 下一里程碑

高层客户端/服务端门面、能力协商、优雅排空、保活/存活检测、诊断、
回调静默和运行时对端回收已经实现。下一阶段生产工作应重点处理：

1. 为拆分 CONTROL/BULK 门面增加连接绑定标识，使独立接收的套接字能够安全配对；
2. 共享连接模式下，在 DATA 帧边界实现严格 CONTROL 优先级；
3. 在保留 RAW 批量分片的同时增加类型化编解码器注册表，例如 Protobuf；
4. 在连接进入 Channel `HELLO` 前集成 TLS/mTLS 提供者；
5. 提供显式 RPC/服务健康检查，与传输层保活分离；
6. 只有标记为可重试/幂等的方法才允许使用通用 RPC 重试策略；
7. 增加模糊测试/长稳测试/基准测试套件和真实 RPC/BULK 性能分析；CRC32C 已有可移植/运行时选择加速后端（见 [CRC32C 后端](docs/crc32c.md)），可选 RX 向量消息仍由性能分析驱动。

备份始终位于该通用 RPC/传输库之上的应用层。

