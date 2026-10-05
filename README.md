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


## Implemented

### Transport wire / framing

- explicit little-endian wire codec; packed C structs are not used as wire ABI
- 40-byte transport frame header
- CRC32C for header and payload: portable table fallback and runtime-selected x86-64 SSE4.2 backend
- frame type / flag / length validation
- bounded maximum payload per transport frame
- logical DATA messages larger than one frame are fragmented transparently using `FIRST/LAST` with one stable `message_id`
- `HELLO`, `HELLO_ACK`, `GOAWAY`, `STREAM_OPEN`, `STREAM_CLOSE`, `WINDOW_UPDATE`, `DATA`, `PING/PONG`
- logical lane declared at Stream open

### Receive path

- incremental parser supporting arbitrary header/payload fragmentation
- `recv()` writes directly into the final RX payload buffer
- bounded fixed-size RX buffer pool
- parser/RX backpressure when the pool is exhausted
- `EPOLLIN` disabled while RX is paused and restored by `tr_reactor_resume_rx()`
- RX ownership can be automatically released or transferred upward

### Send path

- fixed-size BULK TX-item pool
- independent reserved CONTROL TX-item pool
- `sendmsg()` with fixed-capacity scatter/gather (`tr_reactor_sendv()`)
- up to `TR_REACTOR_MAX_TX_SLICES` transport payload slices per frame
- partial-write cursor works across header and multiple slices
- `EPOLLOUT` enabled only after socket `EAGAIN`
- per-connection TX byte budget and reactor-local ready queue
- no per-frame heap allocation in steady-state Transport DATA submission
- send ownership transfers only after the corresponding API returns `TR_OK`

### Runtime

- Linux epoll level-triggered reactor
- Reactor thread 独占 mutable connection state；event-loop handler/connection 热路径不依赖 slot mutex
- slot generation/state 使用 C11 atomic capability metadata，跨线程 handler 更新通过同步 command 提交
- bounded mutex-protected MPSC command ring
- eventfd wakeup coalescing
- generation-based connection handles and stale event rejection
- per-connection higher-layer callback handlers
- nonblocking IPv4 TCP helpers (`listen`, `connect`, `accept`)
- connection-state snapshot API used to validate safe Channel replacement

### Channel / Stream

- logical `Channel` above physical TCP connections
- symmetric per-connection `HELLO / HELLO_ACK` capability handshake before a lane becomes usable
- protocol version range negotiation (V1 currently implements version 1)
- peer receive capability negotiation for frame payload, logical message size, feature bits, and lane mapping
- lane state now distinguishes `DOWN / RECONNECTING / HANDSHAKING / UP`; new Streams are gated until `UP`
- negotiated frame ceilings are enforced by `tr_reactor_sendv_limited()`, so asymmetric peers actually send at the smaller receiver-supported frame size
- CONTROL / BULK traffic lanes
- shared-connection and split-connection policies
- odd/even client/server Stream IDs
- Stream open and half-close lifecycle
- generation-based Stream handles
- monotonically increasing per-stream message IDs
- byte-based Stream flow control
- absolute window limits (`WINDOW_UPDATE` is an absolute byte boundary, not a delta)
- flow-control credit is returned only after upper-layer payload consumption
- `tr_stream_send()` for one-buffer logical messages
- `tr_stream_sendv()` for scatter/gather logical messages
- TX fragmentation keeps the original buffers and sends fragment ranges directly through `sendmsg()`; it does not concatenate the payload
- single-frame RX messages remain zero-copy from Reactor to Channel consumer
- fragmented RX messages use an optional bounded `reassembly_pool` and one contiguous reassembly copy before one logical-message callback
- upper layer can retain an RX logical-message payload and later return it through `tr_stream_release_payload()`
- split mode isolates BULK connection failure from CONTROL connection operation
- failed-lane Stream slots are invalidated immediately, so reconnect cannot leak `max_streams` capacity
- logical Channel survives physical connection loss
- `tr_channel_replace_connection()` attaches a new reactor-owned connection without recreating the Channel/RPC endpoint
- shared mode replaces CONTROL+BULK together; split mode replaces lanes independently
- client V1 optional automatic reconnect for numeric IPv4 endpoints
- bounded exponential reconnect backoff with configurable connect timeout
- server side intentionally uses explicit replacement after the application accepts/authenticates a new socket
- Channel emits DOWN/UP/GOAWAY events and exposes per-lane `DOWN / RECONNECTING / HANDSHAKING / UP` state
- existing Streams never survive a connection replacement; old handles become stale

### Capability handshake and graceful drain

A physical TCP connection is not immediately considered Channel-ready. Both
sides send a symmetric fixed-size `HELLO` describing their receive capability:

```text
TCP adopted
   |
   v
HANDSHAKING
   |  HELLO / HELLO_ACK
   |  - protocol version range
   |  - lane mask
   |  - receive max frame payload
   |  - receive max logical message
   |  - feature bits
   v
UP
   |
   +--> new Streams allowed
```

The advertised size limits are directional: a peer declaring a small local
reassembly limit constrains what is sent **to that peer**; it does not
unnecessarily constrain messages sent in the opposite direction.

Graceful shutdown is exposed by:

```c
tr_channel_begin_drain(channel);
tr_channel_wait_drained(channel, timeout_ms);
```

`begin_drain()` is idempotent, disables reconnect, rejects new local Streams,
and sends `GOAWAY` on each ready lane. Receiving `GOAWAY` prevents opening new
Streams toward that peer. Existing Streams remain fully usable until they
close/cancel naturally:

```text
RUNNING
   |
   | begin_drain + GOAWAY
   v
DRAINING  -- existing Streams continue -->
   |
   | active_streams == 0
   v
DRAINED
```

Transport does not force-cancel application work during drain; RPC/application
deadlines decide how long outstanding operations may remain alive.

### Keepalive / liveness

Channel can optionally run low-rate transport liveness probes:

```c
struct tr_channel_keepalive_config cfg = {
    .interval_ms = 30000,
    .timeout_ms = 10000,
};

tr_channel_enable_keepalive(channel, &cfg);
```

Keepalive is intentionally different from service health. It answers only
"is the peer/connection still making progress?". After peer-RX has been idle
for `interval_ms`, Channel sends a `PING`. A matching `PONG` records an RTT
sample. Any peer traffic observed after the probe is sufficient proof of
liveness; if no peer activity is observed within `timeout_ms`, Channel aborts
the physical connection with `TR_ERR_TIMEOUT`. Existing connection-loss and
reconnect handling then runs normally.

Shared-connection mode emits one probe for the shared TCP connection; split
mode tracks CONTROL and BULK independently. Every Channel registers one
initially-disarmed keepalive timer in its owning Reactor; standalone and
high-level Client/Server Channels use the same owner-local path. Keepalive no
longer creates a private timer thread; scheduling stays inside the Reactor.

### Metrics / diagnostics

Detailed Reactor/Channel/Stream/Endpoint snapshots remain available to
repository-internal diagnostics and benchmark code, but are no longer part of
the stable installed SDK.

This separation is intentional:

```text
stable application API
    -> semantic RPC/facade contract

internal diagnostics
    -> Reactor queue/pool pressure
    -> Channel/Stream state
    -> Endpoint executor details
```

The implementation continues collecting the same bounded counters and optional
timing histograms; only the compatibility boundary changed.

### Channel reconnect / connection replacement

Reconnect deliberately restores the **logical Channel**, not the byte state of
old Streams:

```text
TCP connection #1
       X
       |
       +--> all Streams on the failed lane -> ERROR / stale handle
       +--> in-flight RPC Calls            -> UNAVAILABLE
       |
       v
Channel remains alive
       |
       +--> client: Reactor timer + nonblocking connector
       |       or
       +--> server: application accepts a replacement socket
       |
       v
new reactor-owned Connection
       |
       v
tr_channel_replace_connection()
       |
       v
HELLO / HELLO_ACK
       |
       v
new Streams / new RPC Calls
```

This boundary is intentional. Transport does **not** replay an RPC merely
because TCP failed; it cannot know whether an operation is idempotent. Generic
RPC retry and backup-specific resume remain upper-layer policies.

Client automatic reconnect is enabled with:

```c
struct tr_channel_reconnect_config cfg = {
    .ipv4_address = "127.0.0.1",
    .control_port = 9000,
    .bulk_port = 9001,          /* 0 => control_port */
    .initial_delay_ms = 200,
    .max_delay_ms = 10000,
    .connect_timeout_ms = 5000,
};

tr_channel_enable_client_reconnect(channel, &cfg);
```

Client reconnect is fully Reactor-owned: backoff uses a Reactor-local timer and
connect completion uses the bounded auxiliary-fd connector path. No per-Channel
maintenance thread is created. Shared mode reconnects one physical connection
for both lanes; split mode reconnects failed lanes independently.

The server does not automatically accept or trust replacement sockets. Its
listener/authentication layer accepts the socket, adopts it into the Reactor,
and then calls:

```c
tr_channel_replace_connection(channel, lane, new_connection);
```

This keeps listener, TLS/authentication, and service policy outside Channel.

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
