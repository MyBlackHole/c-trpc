# 分层、模块职责与 API 边界

**状态：TARGET INVARIANT + CURRENT AUDIT**

本文定义 c-trpc 的长期架构边界。它不是目录规范，而是 dependency、ownership、lifetime 和 API capability 的约束。

核心原则：

> 每一层只通过能力/API 向上一层提供服务；上层不依赖下层内部对象、线程模型、队列结构、slot/generation 或 wire parser 实现。

另一个同等重要的原则：

> 分层是 source/ownership/dependency boundary，不要求每层增加一次 queue、thread hop、copy 或 serialization。

因此模块化与高性能不是对立目标。性能优化必须发生在边界内部，不能以泄露内部状态或打破单 owner 为代价。

---

## 1. c-trpc Core Scope

c-trpc core 只负责通用 RPC 与 Transport 能力。

```text
Application / Business
        |
        | RPC / Transport capability
        v
+---------------------------+
| Public RPC Facade         |
| Client / Server / Call    |
+-------------+-------------+
              |
              v
+---------------------------+
| RPC Core                  |
| Endpoint / Method / Call  |
| Codec / Deadline / Cancel |
+-------------+-------------+
              |
              v
+---------------------------+
| Transport                 |
| Channel / Stream          |
| Connection Group/Pipeline |
| Flow control / Routing    |
+-------------+-------------+
              |
              v
+---------------------------+
| Runtime                   |
| Reactor / Timer / Queue   |
| Socket / Buffer ownership |
+-------------+-------------+
              |
              v
             Linux
```

Backup、数据库同步、对象复制等都属于 c-trpc 之上的业务层。

以下概念不得进入 c-trpc core public contract：

- `backup_id`；
- backup checkpoint / manifest；
- STORED / CHECKPOINTED / COMMITTED；
- backup resume / commit；
- WORM / retention；
- 任何只对某一业务成立的 durable state machine。

业务层可以使用 generic Pipeline/Connection Group，但 core 不知道它承载的是 Backup 还是其他业务。

---

## 2. Layer Responsibilities

### 2.1 Runtime

这里区分**概念 Runtime layer** 与物理 `src/runtime/` module：

- 概念 Runtime layer 包含 Reactor execution substrate；
- 物理 `src/runtime/` 只负责 shard/lifecycle/resource-domain orchestration，
  不接管 Reactor 的 epoll/queue/timer implementation。

**Responsibility**

- shard resource-domain composition；
- Reactor lifecycle create/start/stop/destroy；
- shard-local shared RPC executor lifecycle；
- listener / peer storage / deferred peer-event resource lifecycle；
- shard identity and aggregate resource ownership。

**Owns**

- Runtime / RuntimeShard objects；
- shard-local Reactor and shared executor object lifetime；
- peer slot storage/counters；
- listener fd and peer-event fd lifecycle。

**Delegates to Reactor**

- epoll event loop；
- connection slot execution；
- command/completion admission；
- Reactor timer；
- Reactor queue/pool implementation。

**Does not own**

- RPC Method/Call 业务语义；
- Stream routing policy；
- Connection Group lifecycle policy；
- Reactor internal queue/epoll implementation；
- 业务 identity/durability。

**Capability offered upward**

```text
shard lifecycle
owner Reactor capability
shared executor capability
listener/peer resource domain
owner call
```

Runtime 默认应是 internal orchestration module。

---

### 2.2 Transport

**Responsibility**

- TRP1 framing；
- Channel / Stream；
- connection lifecycle；
- flow control；
- reconnect/drain；
- zero-copy/copy-minimal message transport。

**Owns**

- Transport protocol state；
- Stream state；
- receive credit；
- frame/message ordering。

**Does not own**

- RPC method dispatch；
- business retry semantics；
- durable commit；
- application data model。

**Capability offered upward**

```text
logical connection
message/stream send/receive
transport lifecycle
transport diagnostics
```

---

### 2.3 Connection Group / Pipeline

Pipeline 是 Transport 的通用 multi-connection capability，不是 Backup object。

**Responsibility**

- one CONTROL + DATA[N] membership；
- TRR1 routing；
- DATA reservation/attach；
- Stream -> DATA generation affinity；
- group teardown；
- bounded shard-local registry。

**Owns**

- runtime group identity；
- membership capability generation；
- connection group soft state。

**Does not own**

- backup identity；
- durable checkpoint；
- storage commit；
- RPC Method semantics。

**Capability offered upward**

目标 public abstraction 应表达：

```text
group create/connect/accept
open/close data lane
bind/release logical transfer
group drain/close
stats
```

调用者不应直接操作：

- Pipeline registry；
- TRR1 parser；
- Reactor connection slot；
- Pipeline member generation table；
- preface gate。

这些都属于 implementation components。

---

### 2.4 RPC

**Responsibility**

- Service / Method；
- Call lifecycle；
- Unary / Streaming；
- deadline / cancel；
- metadata；
- codec；
- executor admission and callback scheduling。

**Owns**

- RPC protocol state；
- Method registry；
- Call state；
- RPC status lifecycle。

**Does not own**

- socket/epoll；
- Reactor slot；
- Transport frame parser；
- business transaction semantics。

**Capability offered upward**

```text
register method
start/call/send/finish/cancel
receive callback
RPC stats
```

---

### 2.5 Public Client / Server Facade

Facade 的职责是组合内部能力，而不是把内部对象重新暴露给业务。

**Responsibility**

- create/start/connect/listen/drain/destroy；
- method registration；
- Call API；
- semantic resource limits；
- service-level diagnostics。

**Must hide**

- Reactor；
- connection slot/generation；
- command/completion queue；
- parser；
- socket fd；
- registry；
- internal worker ownership。

---

## 3. Module Contract Template

每个新增模块必须能够回答：

```text
Module:
Responsibility:
Owns:
Does not own:
Public capability:
Internal dependencies:
Thread/owner model:
Lifetime:
Backpressure/resource bound:
```

如果无法明确回答，说明模块边界还没有形成。

模块不是“放相关代码的目录”；模块必须有清晰 capability 和 ownership。

---

## 4. Dependency Rules

允许的依赖方向：

```text
Business
   |
   v
Public RPC/Transport API
   |
   v
RPC
   |
   v
Transport / Connection Group
   |
   v
Runtime
   |
   v
OS
```

规则：

1. 上层可以调用下层 capability，但不能直接修改下层内部状态。
2. 下层不得反向依赖上层业务语义。
3. 同层 implementation component 可以通过 internal API 协作。
4. 如果上层必须读取 lower-layer struct field 才能完成功能，优先认为缺少 capability API。
5. 如果一个 high-level API 需要调用者理解 Reactor slot、generation、queue、epoll 等实现细节，API 抽象层级不正确。
6. internal header 不属于 SDK compatibility contract。
7. public header 必须显式列出；禁止用 glob 把全部内部 header 自动安装。
8. tests 可以直接测试 internal module，但 test-only access 不应因此把 internal header 变成 public SDK。

---

## 5. API Visibility Tiers

### 5.1 Stable Public

面向普通 c-trpc 用户。

目标包括：

- Client / Server lifecycle；
- Method/Call/Streaming；
- generic Transport/Connection Group high-level capability；
- semantic limits；
- stable observability。

特点：

- opaque owner objects；
- 不暴露 internal pointer graph；
- 不暴露 slot/generation；
- 不要求调用者管理 Reactor；
- ABI/API 有兼容承诺。

### 5.2 Advanced Public

只有确实存在 transport-only / framework-integration use case 时才提供。

例如未来：

- raw Transport session；
- Connection Group；
- zero-copy buffer view；
- advanced performance tuning。

要求：

- 仍然不能要求调用者操作 Reactor internals；
- advanced API 是 capability abstraction，不是 internal struct dump；
- 与 Stable Public 分开文档和兼容级别。

### 5.3 Internal

默认包括：

- Reactor；
- Runtime shard；
- command/completion/timer queue；
- parser/frame assembly；
- socket helper；
- wire codec；
- Pipeline registry/route/ingress/control transport；
- owner-only lifecycle helper。

Internal API 可以为了性能暴露更多实现细节，但只能在库内部使用。

---

## 6. Handle / Struct Rules

public handle 优先满足：

```text
opaque
small
generation-safe
implementation-independent
```

不应把以下布局作为 public ABI：

```c
struct public_handle {
    struct internal_object *owner;
    uint32_t slot;
    uint32_t generation;
};
```

因为这会：

- 暴露实现 ownership；
- 固化 slot table 设计；
- 阻碍未来改变 handle encoding；
- 允许上层绕过模块 API。

可以继续在 internal 层采用 slot + generation；public 层应通过 opaque capability 封装。

---

## 7. Configuration Rules

高层配置暴露**语义预算**，而不是默认暴露 implementation knob。

优先：

```text
max_connections
max_inflight_calls
max_message_bytes
memory_budget_bytes
worker_concurrency
```

谨慎暴露：

```text
command_ring_capacity
tx_item_pool_count
parser_buffer_count
internal_queue_batch
```

如果确实需要高性能调优：

- 放入独立 advanced tuning struct；
- 文档标明不是协议/语义 contract；
- 默认值必须足够工作；
- 不能要求普通业务理解内部队列才能正确使用系统。

---

## 8. Observability Rules

高层 stats 可以汇总 lower-layer evidence，但不等于把 lower-layer struct 直接嵌入 public ABI。

推荐：

```text
server stats
  -> semantic RPC/Transport metrics
  -> optional advanced runtime diagnostics
```

不推荐：

```text
high-level Server ABI
  -> embeds exact Reactor internal queue/pool layout
```

这样既保留性能诊断，又不把 Runtime implementation 固化成 facade ABI。

---

## 9. Performance and Layering

### 9.1 不冲突的原因

层边界不等于运行时 hop。

下面都是允许的高性能实现：

```text
RPC API
  -> direct internal C call
  -> Transport owner fast path
  -> sendmsg
```

并不要求：

```text
RPC
  -> queue
  -> Transport thread
  -> queue
  -> Runtime thread
```

同一 owner domain 内可以直接调用下层 internal function。

### 9.2 Hot-path rules

为了性能，允许：

- owner-thread direct fast path；
- static inline 小函数；
- fixed-capacity table/pool；
- scatter/gather；
- ownership transfer；
- zero-copy/copy-minimal；
- shard-local cache；
- batch；
- preallocated state；
- compile-time specialization。

不允许以“性能”为理由默认采用：

- 跨层共享 mutable struct；
- global hot-path mutex；
- 上层直接访问 Reactor slot；
- 每层一次 heap allocation；
- 每层一次 serialization/deserialization；
- 每层一次 buffer copy；
- 每层一个独立线程/queue；
- frame-by-frame generic hash/map lookup；
- 为了避免一个函数调用而破坏 ownership boundary。

### 9.3 Pay for capability, not for layer

原则：

> 未使用的能力不应产生运行时成本；使用某层抽象也不应因为“分层”自动增加 copy/thread hop。

例如：

- RPC 可以直接使用 Transport internal API；
- Transport 可以直接调用 owner Reactor fast path；
- Connection Group 可以在 owner Reactor 内直接做 registry/affinity；
- public opaque handle 只改变 API contract，不要求额外 heap object。

### 9.4 Performance exception gate

任何跨层性能例外都必须同时满足：

1. profile/benchmark 证明现有边界是实际瓶颈；
2. 明确记录优化前后数据；
3. 不破坏 single-owner/lifetime correctness；
4. 优先通过 internal fast path 解决；
5. 如果必须改变 public boundary，新增 ADR；
6. 必须有 fallback/可测试语义。

不能以“可能更快”为理由提前暴露 implementation state。

---

## 10. CURRENT Audit

审查基线为文档修改前的 `main@f2e5eafdad82e4543163e53d4e69d207059c2fb0`。

本次文档变更已经处理 A1（core 文档的 Backup 耦合）：
core overview/ownership/roadmap 已改为业务中立，Backup durability 被标记为
BUSINESS REFERENCE。其余 A2-A10 是当前代码/API 仍然存在的收敛项。

### 10.1 已满足

| 项目 | 状态 | 说明 |
|---|---|---|
| Runtime shard ownership | 满足 | listener、peer state、executor/resource 已 shard-local |
| Reactor single-owner | 满足 | protocol mutable state 基本遵循 owner mutation |
| Worker completion returns to owner | 满足 | completion/event 返回原 owner，不直接修改协议状态 |
| Pipeline implementation components | 满足 | semantic core/registry 已进入 `src/group/`；route/ingress/control/listener 仍为 internal adapter，全部未进入 `include/tr` |
| Pipeline business independence in code | 基本满足 | 当前代码使用 generic pipeline/control/data/stream 名称，没有引入 backup_id/checkpoint 等业务对象 |
| bounded resource model | 满足 | queue/pool/table 大部分都有显式容量 |
| hot-path no cross-shard shared pool | 满足 | 当前 shard resource ownership 与设计方向一致 |

### 10.2 不满足 / 需要收敛

#### A1. Core architecture documents are Backup-coupled — HIGH — RESOLVED BY THIS DOC CHANGE

当前：

- `00-overview.md` 的 System Context 直接写 Backup；
- Ownership Map 使用 `Backup Pipeline`；
- core invariants 包含 durable backup mutation；
- `04-backup-pipeline.md` / `05-durability-recovery.md` 在 core 阅读顺序中；
- `06-evolution.md` 把 Backup Correctness 当作 c-trpc 下一阶段。

问题：

这会把业务正确性自然下沉到 RPC/Transport core。

处理结果：

- core 架构已改用 generic Application / Connection Group / Pipeline；
- Backup durability 已降为 BUSINESS REFERENCE；
- core roadmap 已把 Phase 6 改为 API Boundary Cleanup，不再实现 backup_id/checkpoint/commit。

---

#### A2. SDK header installation has no visibility boundary — HIGH — PARTIALLY RESOLVED

基线问题：

```lua
add_headerfiles("include/(tr/*.h)")
```

会把任何新增 `include/tr/*.h` 自动发布为 SDK contract。

第一阶段已完成：

- `xmake.lua` 改为 explicit installed-header allowlist；
- CI 对安装后的 header 集合做 exact diff；
- 新增 header 不会因为目录位置自动变 public；
- 以下明显 implementation header 已退出安装 SDK：
  - `command_queue.h`；
  - `parser.h`；
  - `rpc_wire.h`；
  - `socket.h`；
  - `endian.h`；
  - `guard.h`；
  - `refcount.h`；
  - `crc32c.h`。

仍未完成：

- `rpc.h` 仍混合 application API 与 Endpoint engine；
- 因此 `buffer.h/channel.h/reactor.h/frame.h/wire.h` 仍作为 transitional
  transitive dependency 被安装；
- 下一阶段必须拆 RPC/Transport engine 才能把这些 Runtime headers 从 stable SDK
  closure 移出。

目标保持不变：

- Stable Public / Advanced Public / Internal 明确分级；
- Runtime/parser/wire/queue/socket implementation 最终不属于默认 stable SDK；
- protocol-extension header 如需公开，单独定义兼容级别。

---

#### A3. Internal data structures are public ABI — HIGH

例：

`command_queue.h` 公开：

- `pthread_mutex_t`；
- ring head/tail/count；
- internal command union；
- Reactor slot/generation。

`parser.h` 公开 parser state machine、payload pool 和 CRC state。

`buffer.h` 公开 pool mutex/free-list/storage layout。

这不是 capability API，而是 implementation layout。

目标：

- internal struct private；
- public API 使用 opaque object / buffer view；
- zero-copy 需求通过 ownership API 解决，不通过暴露 allocator internals 解决。

---

#### A4. Channel public API requires Reactor internals — HIGH — RESOLVED FOR STABLE SDK

基线 `tr_channel_create()` 要求调用者传入 `tr_conn_handle`，把 Reactor
pointer + slot + generation 暴露给 Transport 使用者。

第二阶段处理结果：

- `channel.h` / `reactor.h` 已退出安装的 stable SDK；
- Client/Server facade 仍在库内部组合 Channel/Reactor；
- repository internal tests 可以继续直接测试低层 Channel；
- generic Connection Group/Transport public capability 后续单独设计，不复用
  `tr_conn_handle` 作为业务 contract。

因此该问题对 stable facade 已解除；低层 Channel 现在是 internal engine API。

---

#### A5. rpc.h mixes application RPC API with lower-layer construction — HIGH — RESOLVED

第二阶段已拆分：

```text
include/tr/rpc.h
  -> Method / Call / Streaming application contract

src/rpc/rpc_internal.h
  -> Endpoint config/create/destroy
  -> Channel binding
  -> executor config/stats
  -> internal Buffer fast path
```

`rpc.h` 不再 include Channel/Buffer/Reactor，也不声明 Endpoint engine
construction。Client/Server 用户无需知道 Endpoint 如何绑定 Transport。

---

#### A6. Public handles expose owner pointer + slot/generation — MEDIUM/HIGH

当前：

- `tr_rpc_call_handle`；
- `tr_stream_handle`；
- `tr_conn_handle`。

这些实现是正确的 internal capability 技术，但 public layout 固化内部对象模型。

处理结果：

- stable public `tr_rpc_call_handle` 现在只暴露固定大小 `_private` capability；
- Endpoint pointer、slot、generation 的编码/解析只存在于 RPC engine；
- handle 仍按值复制，不增加 allocation、lock、queue 或 thread hop；
- generation fencing 与 stale-handle 检查语义保持不变。

---

#### A7. High-level facade config leaks queue/pool implementation — MEDIUM — RESOLVED

`tr_facade_limits` 曾直接包含 command/TX/RX/pool/executor worker/node 等实现容量。

当前已完成收敛：这些字段已退出 stable config，统一进入 repository-internal
`tr_facade_tuning`；public create 使用 facade-owned defaults，benchmark/architecture
诊断才通过 tuned create 精确指定布局。

后续若需要用户控制过载行为，应新增 max-inflight/admission 等 semantic policy，
不能重新公开 Reactor allocator、worker count 或 executor node count。

---

#### A8. High-level Server stats embeds Runtime layout — MEDIUM — RESOLVED FOR STABLE SDK

原 `tr_server_stats` 直接嵌入 Reactor/pool implementation。

第二阶段已将：

- `tr_server_stats`；
- `tr_server_get_stats()`；
- `tr_client_get_channel_stats()`；
- `tr_client_get_rpc_stats()`

移到 `facade_diagnostics_internal.h`。现有 benchmark/test 继续使用内部完整诊断，
stable Server/Client header 不再绑定 Runtime stats layout。

后续仍需要设计真正的 stable semantic observability API。

---

#### A9. Umbrella header exposes allocator — MEDIUM — RESOLVED

`tr/trpc.h` 已移除 `buffer.h`。安装 SDK 现在只发布 facade/RPC application
headers；Buffer/Channel/Reactor/Frame/Wire 都不再属于 stable installed surface。

内部 copy-minimal fast path 仍保留，没有因为 API cleanup 增加 copy。未来若公开
zero-copy，将通过 opaque ownership abstraction 设计。

---

#### A10. CI currently treats Reactor as installed external SDK API — HIGH — RESOLVED IN FIRST STAGE

基线安装测试直接编译 external Reactor consumer，使 Runtime API 成为事实上的
compatibility contract。

第一阶段已修改为：

- external consumer 只 include `<tr/trpc.h>`；
- 只使用 Client/Server facade config 与 status API；
- install smoke 对 published header 集合做精确校验；
- Reactor 行为测试继续留在 repository internal tests。

第二阶段已解除 `rpc.h -> channel.h -> reactor.h` 的 transitive dependency，
Reactor/Channel/Buffer/Frame/Wire 已从安装 SDK 退出。

---

### 10.3 不是问题的内容

以下内容本身不构成架构违规：

- `src/` 当前是平铺目录；
- Client/Server implementation 内部向下调用 Runtime/Channel；
- internal test 直接 include `../src/*_internal.h`；
- owner fast path 直接函数调用；
- 为零拷贝使用 buffer ownership transfer；
- internal slot + generation capability。

是否需要物理目录重构应由可维护性决定，不应把目录形式误当分层本身。

---

## 11. Cleanup Priority

建议按以下顺序收敛，而不是一次性大规模改名：

### P0 — Architecture contract

- 本文成为 core architecture invariant；
- Backup durability 从 core roadmap/read-order 移出；
- 新功能评审先检查 layer/module/API contract。

### P1 — Public header boundary — COMPLETE

已完成：

- explicit installed-header allowlist；
- exact install-set CI gate；
- external smoke 改用 public facade；
- command_queue/parser/rpc_wire/socket/endian/guard/refcount/crc32c 退出 SDK。

第二阶段补齐：

- RPC/Channel/Runtime transitive dependency 已拆；
- buffer/channel/reactor/frame/wire 已退出 stable facade closure；
- installed SDK 只保留自包含 facade/RPC headers。

### P2 — RPC public surface — COMPLETE

已完成：

- application RPC API 与 Endpoint engine 分离；
- Client/Server 用户不再看到 Channel/Endpoint construction；
- retained message 用 opaque release token 保持零额外 allocation/copy；
- `tr_rpc_call_handle` 已 opaque 化，不再公开 Endpoint pointer + slot/generation，同时保持原有 stale-generation fencing。

### P3 — Transport / Connection Group public capability — COMPLETE

已完成第一阶段：

- 新增 stable `tr/transport.h`，Server 可显式启用 bounded Connection Group capability；
- `tr_server` 拥有 public group listener 生命周期，并将 group connection capacity
  纳入 Reactor 预留预算；
- CONTROL authorization 只暴露 `group_id/epoch`；
- DATA receive 只暴露 semantic group/stream/message/bytes，不暴露
  Reactor handle、registry、TRR1 或 member generation；
- TAKE_OWNERSHIP 通过 opaque release token 直接保留原 RX buffer，不增加 payload copy；
- 现有 Pipeline/registry/route/control/ingress engine 保持 internal single-owner 实现。

第二阶段已完成：

- Client facade 可通过 `group_id/epoch` 建立/关闭一个 active Group CONTROL connection；
- CONTROL TRR1 route 由 Transport 内部生成，owner shard/member generation 不进入 public API；
- fd adopt 与 CONTROL handler install 在同一个 Reactor owner turn 完成，不存在已接收 frame 落到 NULL/default handler 的窗口；
- Client destroy 在停止 Runtime 前同步回 owner 关闭 Group CONTROL。

第三阶段已完成：

- Client 增加 semantic `max_data_connections` bound；0 保持 CONTROL-only，非 0 才自动建立 DATA lane；
- DATA_OFFER 由 internal Client Group engine 消费，DATA index/generation 不进入 public API；
- DATA connect 使用 nonblocking socket + Reactor-owned auxiliary fd event + Reactor timer，不新增 connector thread；
- 同时最多一个 DATA socket 处于 connect/preface establishment，多个 DATA lane 可以保持 ATTACHED，资源量由 semantic bound 限制；
- Client Reactor connection budget 从该 DATA bound 推导，不再依赖 DATA lane 的隐式 magic capacity；Group engine/pool/timer 仍按首次 Group API 使用惰性创建，普通 RPC Client 不承担未使用能力成本；
- DATA route attach 失败时 Server 只 exact-cancel 仍为 RESERVED 的 capability；已经 ATTACHED、stale 或复用 generation 不受影响；
- peer DATA_CANCEL 对 exact generation 幂等：RESERVED -> FREE，ATTACHED/FREE 为 no-op，generation 已复用才 STALE，因此 preface handoff 不需要新增 attach ACK；
- `max_data_connections == 0` 时继续由 Client 内部 exact DATA_CANCEL 归还 reservation。

第四阶段已完成：

- Client 消费 TRANSFER_READY，并要求 wire 中的 DATA index/generation 精确命中当前 ACTIVE DATA lane；
- Client 使用 `limits.max_streams` 作为 bounded local transfer-affinity capacity，不新增重复的 public capacity knob；
- public READY callback 只暴露 `group_id/epoch + stream_id + CONTROL message_id`，DATA routing capability 保持 internal；
- Client transfer release 回到同一 Reactor owner 删除 affinity；DATA lane 关闭会按 exact generation 失效对应 affinity，slot reuse 不会让旧 Stream 漂移到新连接。

第五阶段已完成：

- Client public `tr_client_connection_group_send()` 只按 READY affinity 发送，不允许重新选择 DATA lane；
- 每次 send 都重新验证 exact DATA membership generation，lane replacement 后旧 Stream 返回 STALE；
- public bytes 仅调用期间 borrowed；TR_OK 前复制到 owner-local owned buffer，应用可立即复用源内存；
- send memory bound 从 `max_data_connections * max_message_bytes` 推导，不新增一个 implementation tuning knob；
- Reactor 原有 DATA path 继续负责 FIRST/LAST、分片、TX fairness 与 EAGAIN；Group 不新增第二发送队列；
- TX 完成/连接关闭/提交失败通过 one-shot Buffer release hook 回收 quota；
- 当前 backpressure contract 为 `TR_AGAIN + caller retry`，不发布可能产生 false-ready 的单一 writable callback。

第六阶段已完成：

- Server `begin_drain` 只关闭 Group accept，保留既有 Group/Data/Transfer；draining 后不再创建新的 DATA_OFFER / TRANSFER_READY；
- drain 前已 accept 但尚未完成 CONTROL preface 的 socket 在 route attach 时仍会被 draining fence 拒绝，避免 late Group creation；
- Client `begin_drain` exact-cancel queued/connecting DATA capability，后续 DATA_OFFER 只 cancel；它同时是本地 READY admission barrier，barrier 后观察到的新 TRANSFER_READY 不安装 affinity，barrier 前已 READY 的 transfer 可继续完成；
- Client `wait_drained` 等 active transfer、owned send payload 与 pending establishment quiesce；Server `wait_drained` 等既有 Group/connection 自然归零；
- force `stop()` 与 graceful drain 语义明确分离；
- stable Client stats 暴露 group/control/data/transfer/send-byte lifecycle；stable Server stats 暴露 group/connection/data/transfer 与 accept/reject counters；
- Server DATA ingress 在 application callback 前验证 `stream_id -> exact DATA generation/connection` affinity；物理 DATA attach 本身不构成 transfer authorization，未经过 TRANSFER_READY 的 DATA frame 会关闭违规 DATA lane；
- DATA attach 时一次性绑定 owner-local Pipeline capability，frame hot path 只做现有 bounded Stream-affinity lookup，不做 frame-by-frame registry routing/hash，也不新增锁或 owner hop；
- Reactor slot/generation、TRR1 identity、connector、queue/pool occupancy 继续保持 internal。

P3 public capability 至此闭环；后续只接受 bugfix、验证与 profile 驱动的扩展。

### P4 — Config / Stats split — IN PROGRESS

已完成：

- detailed Reactor/Channel/Endpoint diagnostics 退出 stable facade header。

已推进：

- Connection Group 已具备 stable semantic stats，且与 internal Reactor/Pipeline diagnostics 分离；
- P4 第一阶段已把 command ring / DATA TX / CONTROL TX / RX buffer /
  RPC message pool count / reassembly pool count 从 stable `tr_facade_limits`
  移到 repository-internal `tr_facade_tuning`；
- P4 第二阶段进一步把 executor worker count / per-Endpoint node capacity /
  continuation reserve 移到 internal tuning；高层 facade 不再承诺当前 worker-pool
  和 task-node 实现布局；
- P4 第三阶段把 timing observability flag 移到 internal tuning，并将
  `observability.h` 从 installed SDK allowlist 移除；固定 histogram 与 queue/pool
  snapshot 类型现在明确属于 engine diagnostics contract；
- P4 第四阶段增加 stable aggregate `tr_rpc_semantic_stats`：started / finished /
  inflight / final status distribution；Client 读取当前 Endpoint，Server 通过
  shard-owner snapshot + retired-peer finalizer 聚合，不暴露 Runtime layout；
- public Client/Server create 只接受 semantic config，并由 facade 内部生成 Runtime/Pool
  defaults；benchmark/architecture tests 通过 internal `*_create_with_tuning()`
  保留精确资源实验能力；
- Server internal tuning 仍是 aggregate budget -> deterministic shard split，但 public
  path 会保证 hidden default 至少支持每个 configured shard 一个必要资源 unit。

已完成：

- RPC encoded-message ownership 使用 bounded on-demand pool：descriptor/slot 数
  保持 internal tuning 有界，单个 slot 按实际 encoded size 增长并复用；
- facade 不再预分配 `pool_count * max_message_bytes` 的完整 storage，也不再需要
  stable `rpc_message_buffer_bytes`；
- Client ownership 保持本地，Server ownership 保持 shard-local；扩容在 pool mutex
  之外执行，不新增跨 shard shared hot state。

待完成：

- 如需应用可配置过载策略，设计 max-inflight/admission 等 semantic policy，不能重新
  暴露 executor node/thread 实现数量；
- per-Service/Method semantic observability 如有需求，必须使用 shard-local
  accounting 再在 control plane 聚合，不能增加 shared hot counter；

### P5 — Physical directory cleanup — IN PROGRESS

capability 边界已经稳定，开始让物理目录反映现有 ownership/module contract，
但每一刀都只做文件归位与 include/build dependency 收敛，不趁目录迁移改变运行语义。

第一、二、三阶段已完成：

```text
src/rpc/
  rpc.c
  rpc_internal.h
  rpc_codec.c
  rpc_wire.c

src/runtime/
  runtime.c
  runtime_internal.h

src/group/
  pipeline.c
  pipeline_internal.h
  pipeline_registry.c
  pipeline_registry_internal.h
```

规则：

- stable/public headers 继续留在 `include/tr/`，不因源码目录移动扩大或缩小 SDK ABI；
- RPC 内部实现只通过 `src/rpc/rpc_internal.h` 向 facade/runtime 暴露 engine contract；
- Runtime 物理 module 只包含 shard/lifecycle/resource-domain orchestration；
- Group 物理 module 第一阶段只包含 Pipeline membership/affinity semantic core 与
  shard-local registry；TRR1 route、CONTROL、ingress/listener、Client adapter 暂不并入；
- Reactor、command/completion/timer queue 不因 Runtime layer 名称被机械搬入
  `src/runtime/`；它们仍是独立 execution substrate；
- module 对 sibling internal dependency 使用显式跨目录 include；
- tests 直接引用 internal contract 时也使用新的物理路径；
- 本阶段不改变线程、owner、锁、队列、resource bound、wire 或 hot path。

后续再按相同规则评估：

```text
src/transport/
```

目录结构服务于已经确定的职责，而不是反过来决定架构。

---

## 12. Review Gate

以后新增功能/PR 必须至少回答：

1. 属于哪一层？
2. 哪个模块拥有状态？
3. 对上一层提供什么 capability？
4. 是否要求上一层理解 internal state？
5. 是否引入新的跨 owner mutable sharing？
6. resource bound 在哪里？
7. failure/teardown ownership 如何闭环？
8. hot path 是否新增 allocation/copy/queue/thread hop？
9. 如果跨层是为了性能，benchmark 证据是什么？
10. public API 是否扩大了 compatibility contract？

只要第 4、5、8、9 项无法解释清楚，就不应直接合入 core architecture。
