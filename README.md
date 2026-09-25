# trpc_transport_v1

Linux 3.x+ C transport/RPC prototype focused on bounded resources, explicit ownership, high-throughput bulk streams, and a clean separation between generic transport/RPC and backup business semantics.

## Architecture

```text
Application / business layer
             |
             v
       RPC Service / Method
       Call + Executor
  ONE/MANY <-> ONE/MANY
             |
     RAW control message
        or RAW slices
             |
             v
        +--------- Channel ---------+
        |                           |
   CONTROL lane                 BULK lane
        |                           |
        +------ physical map -------+
        |                           |
 control connection          bulk connection
        |                           |
        +------------+--------------+
                     |
                Stream layer
            byte flow control
                     |
              Transport Frame
                     |
             owner Reactor
                     |
             epoll LT + eventfd
              /             \
           recv()          sendmsg()
             |                 |
       incremental       header + slices
         parser              iovec[]
             |                 |
        RX buffer          TX item pools
           pool        (bulk + control reserve)
              \             /
                    TCP
```

A Channel can run in:

- `TR_CHANNEL_SHARED_CONNECTION`: CONTROL and BULK lanes map to one TCP connection.
- `TR_CHANNEL_SPLIT_CONNECTIONS`: CONTROL and BULK use independent TCP connections while sharing one Reactor.

The transport/RPC core performs no normal filesystem I/O and contains no backup-specific commit/storage semantics.


## High-level Client / Server facade

The library now includes a first high-level facade for applications that do not
want to manage Reactor slots, connection generations, Channel construction, or
accepted socket adoption directly:

```text
Application
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

Client lifecycle:

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

Server lifecycle:

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

The facade owns the Reactor, RPC message pool and Channel reassembly pool.
`tr_server_register_method()` / `tr_server_register_stream_method()` are
pre-start operations in V1; every accepted peer receives an RPC endpoint with
the registered method table. `tr_client_call_start*()` exposes Streaming Calls
without exposing Reactor/Connection internals. Call-level send/cancel/metadata
operations continue to use the existing `tr_rpc_call_*()` APIs on the returned
Call handle.

The V1 facade deliberately uses `TR_CHANNEL_SHARED_CONNECTION`. The lower-level
Channel API still supports split CONTROL/BULK connections. A production
multi-client split facade needs a connection-binding identity in the handshake
so the server can prove which independently accepted CONTROL and BULK sockets
belong to the same logical Channel; that pairing protocol is intentionally not
guessed or inferred from accept order.

The server facade reclaims disconnected peer objects at runtime. Reactor
callback teardown uses an explicit quiescence barrier: callbacks are disabled
first, then the owning thread waits until the Reactor has crossed the barrier
before RPC/Channel storage is released. Therefore `max_peers` is a bound on
concurrently retained peer objects rather than cumulative accepts over the
server lifetime.

Two real-process examples are built by default:

```sh
./examples/echo_server 9000
./examples/echo_client 127.0.0.1 9000 hello
```

## Implemented

### Transport wire / framing

- explicit little-endian wire codec; packed C structs are not used as wire ABI
- 40-byte transport frame header
- CRC32C for header and payload
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
mode tracks CONTROL and BULK independently. Low-level standalone Channel keeps
the compatible private maintenance-thread behavior. High-level Client/Server
facades instead register keepalive work on their runtime-owned shared
maintenance scheduler. Actual socket I/O remains Reactor owned.

### Metrics / diagnostics

Runtime snapshots are available without introducing filesystem logging into
Transport Core:

```c
struct tr_connection_stats conn;
struct tr_channel_stats channel_stats;
struct tr_stream_diagnostics stream_diag;
struct tr_rpc_endpoint_stats rpc_stats;

tr_reactor_get_connection_stats(connection, &conn);
tr_channel_get_stats(channel, &channel_stats);
tr_stream_get_diagnostics(stream, &stream_diag);
tr_rpc_endpoint_get_stats(endpoint, &rpc_stats);
```

Connection diagnostics include RX/TX bytes and frames, send/recv `EAGAIN`, RX
pause count, last RX/TX activity, queued TX items and current backpressure
flags. Channel diagnostics expose lane state, active Streams, logical
message/byte counters, window updates, reconnect results, keepalive probes,
timeouts and RTT. Stream diagnostics expose identity, half-close state,
message cursors and byte-flow-control frontiers. RPC endpoint diagnostics show
registered methods, current Call states, executor queue/running counts and
cumulative Call/cancel/deadline counters.

These APIs are snapshots for troubleshooting/metrics export. They do not imply
that Channel liveness is application health; a future Health service belongs
at the RPC/service layer.

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
       +--> client: optional connect/backoff maintenance thread
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

The V1 maintenance thread performs only low-rate connect/backoff work. Once a
socket is adopted, normal RX/TX remains owned by the epoll Reactor. Shared mode
reconnects one physical connection for both lanes; split mode reconnects each
failed lane independently.

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
- `TR_RPC_NONE` is reserved for a future explicit method-open envelope and is not executable in V1
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
There is no per-Endpoint deadline thread and RPC deadlines no longer use the
shared maintenance scheduler.

Initial metadata is a bounded TLV side channel:

- maximum encoded metadata per direction: 512 bytes
- user keys: lowercase ASCII `[a-z0-9_.-]`, maximum 63 bytes
- values: binary bytes
- duplicate keys are rejected in V1
- user keys starting with `:` are reserved for protocol use
- metadata is emitted only with the first outbound RPC message in a direction
- metadata remains separate from the application RAW payload

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

Public metadata helpers:

```c
tr_rpc_call_set_metadata(...);
tr_rpc_call_get_peer_metadata(...);
```

Server handlers can read request metadata and set response metadata before the
first response is encoded.

### RPC executor boundary

- application RPC callbacks never execute on the network Reactor thread
- bounded fixed-capacity executor task-node pool per RPC endpoint
- low-level standalone RPC endpoints retain their own configurable worker pool
- the high-level Server facade creates one `executor_threads` worker pool shared by all peer RPC endpoints
- one FIFO task queue per Call plus an endpoint-local ready-Call queue
- the shared Server executor schedules ready endpoints while preserving endpoint-local Call ordering
- at most one executor task for a Call runs at a time, so callbacks for one Call remain strictly serialized and ordered
- different Calls, including Calls from different peers, may run concurrently on shared workers
- one task is taken per ready-Call scheduling turn, providing fairness without letting thread count scale with accepted peers
- incoming RPC payload ownership is held until executor processing completes
- this naturally delays Stream receive-credit return while application processing is outstanding
- each queued executor task holds a C11 strong reference on its RPC Endpoint
- Call `task_refs` remain only for Call-slot reuse; Endpoint lifetime uses `tr_refcount`

The executor implementation therefore separates network progress from application latency without sacrificing in-Call message ordering. Server worker count is now O(1) with respect to peer count; endpoint task queues remain independently bounded.

### RAW codec and bulk fast path

Small control-path messages may use the copied helper:

```text
application bytes
      |
RPC envelope + copy
      |
Transport DATA
```

Bulk RAW messages can use `tr_rpc_call_send_buffer()`:

```text
32-byte RPC envelope buffer  ----\
                                 +--> tr_stream_sendv()
original application buffer  ----/          |
                                           v
                                       sendmsg(iovec)
```

On `TR_OK`, both the small envelope buffer and original payload are owned by Transport. On failure, only the internally-created envelope is released and the caller retains its payload.

This removes the extra sender-side large-payload copy introduced by a generic contiguous RPC envelope. If the logical RPC message spans multiple Transport DATA frames, TX still reads directly from the original slices while RX performs one bounded reassembly copy into the Channel `reassembly_pool`. Messages that fit in one frame retain the existing zero-copy receive path.

### RPC retained-message ownership

Streaming callbacks receive:

```c
struct tr_rpc_message {
    struct tr_rpc_bytes bytes;
    struct tr_buffer *storage;
    struct tr_stream_handle stream;
};
```

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

架构设计按“总览、Runtime、Ownership、RPC 执行、Backup Pipeline、Durability/Recovery、演进路线、ADR”拆分，入口见
[`docs/architecture/README.md`](docs/architecture/README.md)。

这些文档区分 CURRENT、TARGET V1 与 FUTURE，避免把规划能力误认为当前已经实现。

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
- `tr_rpc_call_send_buffer()` transfers the application payload only on `TR_OK`.
- any non-`TR_OK` send leaves caller-owned input buffers with the caller.
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
- shared maintenance scheduler callback re-arm/unregister semantics
- RPC Endpoint deadlines run on Reactor-local timers without per-Endpoint timer threads
- high-level Client/Server deadline handling no longer consumes shared maintenance scheduler entries
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

Current build validation passes:

- normal build/tests
- AddressSanitizer + UndefinedBehaviorSanitizer
- ThreadSanitizer

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
- hardware-accelerated CRC32C dispatch
- split-connection Client/Server facade pairing/binding protocol
- lock-free command queue / io_uring / kernel zerocopy optimizations

## Current V1 constraints

- one Transport frame remains bounded by `max_payload_len`; one logical Stream/RPC message may span many DATA frames
- fragmented receive requires a configured bounded Channel `reassembly_pool`; `max_message_bytes` must fit one reassembly buffer
- fragmented RX currently becomes one contiguous buffer before RPC dispatch; single-frame RX remains zero-copy
- TX logical-message size is bounded by the current 32-bit message/reassembly sizing policy and configured flow-control limits
- executor workers run different Calls in parallel, but one Call is intentionally serialized and can therefore be delayed by its own slow handler
- generic Streaming writes do not internally queue arbitrary application messages: `TR_AGAIN` is intentional backpressure and the caller retries after `TR_RPC_CALL_EVENT_WRITABLE` / server `on_writable`
- direct destruction must not run from a Reactor callback; RPC/Channel teardown now uses a Reactor quiescence barrier, while normal shutdown should still drain application work first
- reconnect restores Channel connectivity only; all Streams from the failed physical connection are terminal and must be recreated
- V1 automatic client reconnect still uses one low-rate reconnect thread per enabled Client Channel; connect/poll/backoff may block and is intentionally not executed on the shared timer scheduler
- server connection replacement remains explicit so accept/TLS/authentication policy stays outside the generic Channel
- RPC Endpoint deadlines are Reactor-local; standalone Channel keepalive and high-level Channel maintenance still use transitional private/shared maintenance paths until their owner-timer migration
- the high-level Client/Server facade currently uses shared CONTROL/BULK TCP mapping; split mode remains available through the lower-level Channel API

## Build

The project language baseline is ISO C11. GCC/Clang's cleanup attribute is used
only through the typed `TR_AUTO()` ownership helper.

```sh
make test
```

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
7. fuzz/soak/benchmark suites, then optional RX vectored messages and hardware CRC dispatch when profiling justifies them.

Backup remains an application layer above this generic RPC/Transport library.
