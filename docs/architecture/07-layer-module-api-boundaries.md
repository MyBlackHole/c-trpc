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

**Responsibility**

- Reactor event loop；
- socket ownership；
- timer；
- command/completion admission；
- bounded runtime resources；
- owner-thread execution。

**Owns**

- epoll/eventfd；
- connection slot；
- Reactor timer；
- runtime queue/pool lifecycle。

**Does not own**

- RPC Method/Call 业务语义；
- Stream routing policy；
- Connection Group lifecycle policy；
- 业务 identity/durability。

**Capability offered upward**

```text
connection execution
timer execution
owner call/completion
bounded I/O resource
```

Runtime 默认应是 internal implementation layer。

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
| Pipeline implementation components | 满足 | registry/route/ingress/control/listener 当前都在 `src/*_internal.h`，未进入 `include/tr` |
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

#### A2. SDK header installation has no visibility boundary — HIGH

当前 `xmake.lua`：

```lua
add_headerfiles("include/(tr/*.h)")
```

因此所有 `include/tr/*.h` 都被安装为 SDK header。

其中包含明显 internal 实现：

- `command_queue.h`；
- `parser.h`；
- `wire.h`；
- `rpc_wire.h`；
- `socket.h`；
- `reactor.h`。

目标：

- 显式 public-header allowlist；
- internal header 移入 `src/` 或 private include；
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

#### A4. Channel public API requires Reactor internals — HIGH

当前 `tr_channel_create()` 要求调用者传入：

```c
struct tr_conn_handle
```

而 `tr_conn_handle` 公开：

```text
Reactor pointer
slot
generation
```

这意味着使用 Channel capability 必须理解并管理 Runtime implementation。

目标：

- generic Transport/Connection API 自己拥有 connect/adopt lifecycle；
- Reactor handle 留在 internal；
- 如果保留 low-level Channel API，应降为 advanced/internal，不作为普通 public facade。

---

#### A5. rpc.h mixes application RPC API with lower-layer construction — HIGH

当前同一个 `rpc.h` 同时包含：

- Method/Call/Streaming public semantics；
- `tr_rpc_endpoint_create(struct tr_channel *)`；
- buffer pool pointer；
- Stream handle；
- low-level executor capacity。

结果是 RPC application API 与 RPC engine integration API 混在一起。

目标：

拆分概念：

```text
public RPC types/call API
internal RPC endpoint engine
optional advanced integration API
```

普通 Client/Server 用户不需要知道 Channel/Endpoint construction。

---

#### A6. Public handles expose owner pointer + slot/generation — MEDIUM/HIGH

当前：

- `tr_rpc_call_handle`；
- `tr_stream_handle`；
- `tr_conn_handle`。

这些实现是正确的 internal capability 技术，但 public layout 固化内部对象模型。

目标：

- stable public handle opaque；
- slot/generation 继续保留在 internal implementation。

---

#### A7. High-level facade config leaks queue/pool implementation — MEDIUM

`tr_facade_limits` 当前直接包含：

- command capacity；
- tx item capacity；
- control tx item capacity；
- rx buffer count；
- RPC message pool count；
- reassembly pool count；
- executor queue capacity。

这些参数对于性能调优有价值，但不是高层业务语义。

目标：

- stable config 暴露 semantic limits；
- implementation tuning 移到 optional advanced tuning；
- 默认配置不要求用户理解 Reactor allocator。

---

#### A8. High-level Server stats embeds Runtime layout — MEDIUM

`tr_server_stats` 直接嵌入：

```c
struct tr_reactor_stats
struct tr_pool_observation
```

这把高层 Server ABI 与 Runtime queue/pool implementation 绑定。

目标：

- stable semantic stats；
- advanced runtime diagnostics 单独 API。

---

#### A9. Umbrella header exposes allocator — MEDIUM

`tr/trpc.h` 当前直接 include `tr/buffer.h`。

这使应用默认进入 memory-resource implementation surface。

目标：

- umbrella header 只包含 stable public capability；
- zero-copy buffer API 如果需要，应提供 public buffer/view abstraction，而不是 allocator internals。

---

#### A10. CI currently treats Reactor as installed external SDK API — HIGH

安装测试明确：

```c
#include <tr/reactor.h>
```

并从安装目录编译 external Reactor consumer。

这会把 Runtime internal API 变成事实上的 compatibility contract。

目标：

- install smoke 改测 stable public Client/Server/RPC API；
- Reactor tests 留在 repository internal test。

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

### P1 — Public header boundary

- 建立 explicit public-header allowlist；
- 将 command_queue/parser/wire/socket/reactor 等从默认 SDK contract 移出；
- 修改 install smoke。

### P2 — RPC public surface

- 分离 application RPC API 与 endpoint engine API；
- stable public handle opaque 化；
- Client/Server 用户不再看到 Channel construction。

### P3 — Transport / Connection Group public capability

- 设计 generic Transport/Connection Group facade；
- 不暴露 Reactor/registry/TRR1 implementation；
- 保留 zero-copy/ownership semantics。

### P4 — Config / Stats split

- stable semantic limits/stats；
- advanced performance tuning/runtime diagnostics 独立。

### P5 — Physical directory cleanup

只有前面 capability 边界稳定后，再决定是否迁移为：

```text
src/runtime/
src/transport/
src/group/
src/rpc/
```

目录结构应服务于已经确定的职责，而不是反过来决定架构。

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
