# 分层、模块职责与 API 边界

**状态：目标不变量 + 当前审查**

本文定义 c-trpc 的长期架构边界。它不是目录规范，而是对依赖、所有权、生命周期和 API 能力的约束。

核心原则：

> 每一层只通过能力/API 向上一层提供服务；上层不依赖下层内部对象、线程模型、队列结构、槽位/代次或线协议解析器实现。

另一个同等重要的原则：

> 分层是源码边界、所有权边界和依赖边界，不要求每层额外增加一次队列、线程跳转、复制或序列化。

因此模块化与高性能不是对立目标。性能优化必须发生在边界内部，不能以泄露内部状态或打破单所有者模型为代价。

---

## 1. c-trpc 核心范围

c-trpc 核心只负责通用 RPC 与传输能力。

```text
应用 / 业务
        |
        | RPC / 传输能力
        v
+---------------------------+
| 公开 RPC 门面             |
| Client / Server / Call    |
+-------------+-------------+
              |
              v
+---------------------------+
| RPC 核心                  |
| Endpoint / Method / Call  |
| 编解码 / 截止时间 / 取消 |
+-------------+-------------+
              |
              v
+---------------------------+
| 传输层                    |
| Channel / Stream          |
| 连接组 / Pipeline         |
| 流量控制 / 路由           |
+-------------+-------------+
              |
              v
+---------------------------+
| 运行时                    |
| Reactor / 定时器 / 队列   |
| 套接字 / Buffer 所有权    |
+-------------+-------------+
              |
              v
             Linux
```

备份、数据库同步、对象复制等都属于 c-trpc 之上的业务层。

以下概念不得进入 c-trpc 核心公开契约：

- `backup_id`；
- 备份检查点 / 清单；
- `STORED / CHECKPOINTED / COMMITTED`；
- 备份续传 / 提交；
- WORM / 保留策略；
- 任何只对某一业务成立的持久状态机。

业务层可以使用通用 Pipeline/连接组，但核心不需要知道它承载的是备份还是其他业务。

---

## 2. 分层职责

### 2.1 运行时

这里区分**概念上的运行时层**与物理 `src/runtime/` 模块：

- 概念运行时层包含 Reactor 执行基础设施；
- 物理 `src/runtime/` 只负责分片/生命周期/资源域编排，
  不接管 Reactor 的 epoll/队列/定时器实现。

**职责**

- 分片资源域组合；
- Reactor 生命周期创建/启动/停止/销毁；
- 分片本地共享 RPC 执行器生命周期；
- 监听器 / 对端存储 / 延迟对端事件资源生命周期；
- 分片标识和聚合资源所有权。

**拥有**

- Runtime / RuntimeShard 对象；
- 分片本地 Reactor 与共享执行器对象生命周期；
- 对端槽位存储/计数器；
- 监听 fd 和对端事件 fd 生命周期。

**委托给 Reactor**

- epoll 事件循环；
- 连接槽位执行；
- 命令/完成事件准入；
- Reactor 定时器；
- Reactor 队列/资源池实现。

**不拥有**

- RPC Method/Call 业务语义；
- Stream 路由策略；
- 连接组生命周期策略；
- Reactor 内部队列/epoll 实现；
- 业务标识/持久化语义。

**向上提供的能力**

```text
分片生命周期
所有者 Reactor 能力
共享执行器能力
监听器/对端资源域
所有者调用
```

Runtime 默认应是内部编排模块。

---

### 2.2 传输层

**职责**

- TRP1 分帧；
- Channel / Stream；
- 连接生命周期；
- 流量控制；
- 重连/排空；
- 零复制/尽量少复制的消息传输。

**拥有**

- 传输协议状态；
- Stream 状态；
- 接收额度；
- 帧/消息顺序。

**不拥有**

- RPC 方法分发；
- 业务重试语义；
- 持久化提交；
- 应用数据模型。

**向上提供的能力**

```text
逻辑连接
消息/流发送与接收
传输生命周期
传输诊断
```

---

### 2.3 连接组 / Pipeline

Pipeline 是传输层的通用多连接能力，不是备份对象。

**职责**

- 一个 CONTROL + DATA[N] 成员关系；
- TRR1 路由；
- DATA 预留/附着；
- Stream -> DATA 代次亲和关系；
- 组销毁；
- 有界分片本地注册表。

**拥有**

- 运行时组标识；
- 成员能力代次；
- 连接组易失状态。

**不拥有**

- 备份标识；
- 持久化检查点；
- 存储提交；
- RPC Method 语义。

**向上提供的能力**

目标公开抽象应表达：

```text
组创建/连接/接收
打开/关闭 DATA 通道
绑定/释放逻辑传输
组排空/关闭
统计
```

调用者不应直接操作：

- Pipeline 注册表；
- TRR1 解析器；
- Reactor 连接槽位；
- Pipeline 成员代次表；
- 前导信息门控。

这些都属于实现组件。

---

### 2.4 RPC

**职责**

- Service / Method；
- Call 生命周期；
- 一元调用 / 流式调用；
- 截止时间 / 取消；
- 元数据；
- 编解码；
- 执行器准入与回调调度。

**拥有**

- RPC 协议状态；
- Method 注册表；
- Call 状态；
- RPC 状态生命周期。

**不拥有**

- 套接字/epoll；
- Reactor 槽位；
- 传输帧解析器；
- 业务事务语义。

**向上提供的能力**

```text
注册方法
开始/调用/发送/结束/取消
接收回调
RPC 统计
```

---

### 2.5 公开客户端 / 服务端门面

门面的职责是组合内部能力，而不是把内部对象重新暴露给业务。

**职责**

- 创建/启动/连接/监听/排空/销毁；
- 方法注册；
- Call API；
- 语义资源限制；
- 服务级诊断。

**必须隐藏**

- Reactor；
- 连接槽位/代次；
- 命令/完成队列；
- 解析器；
- 套接字 fd；
- 注册表；
- 内部工作线程所有权。

---

## 3. 模块契约模板

每个新增模块必须能够回答：

```text
模块：
职责：
拥有：
不拥有：
公开能力：
内部依赖：
线程/所有者模型：
生命周期：
背压/资源上限：
```

如果无法明确回答，说明模块边界还没有形成。

模块不是“放相关代码的目录”；模块必须有清晰的能力和所有权。

---

## 4. 依赖规则

允许的依赖方向：

```text
业务
   |
   v
公开 RPC/传输 API
   |
   v
RPC
   |
   v
传输 / 连接组
   |
   v
运行时
   |
   v
操作系统
```

规则：

1. 上层可以调用下层能力，但不能直接修改下层内部状态。
2. 下层不得反向依赖上层业务语义。
3. 同层实现组件可以通过内部 API 协作。
4. 如果上层必须读取下层结构体字段才能完成功能，应优先认为缺少能力 API。
5. 如果一个高层 API 需要调用者理解 Reactor 槽位、代次、队列、epoll 等实现细节，说明 API 抽象层级不正确。
6. 内部头文件不属于 SDK 兼容性契约。
7. 公开头文件必须显式列出；禁止用通配方式把全部内部头文件自动安装。
8. 测试可以直接测试内部模块，但仅测试用途的访问不应因此把内部头文件变成公开 SDK。

---

## 5. API 可见性分级

### 5.1 稳定公开接口

面向普通 c-trpc 用户。

目标包括：

- 客户端 / 服务端生命周期；
- Method/Call/Streaming；
- 通用传输/连接组高层能力；
- 语义限制；
- 稳定可观测性。

特点：

- 不透明所有者对象；
- 不暴露内部指针图；
- 不暴露槽位/代次；
- 不要求调用者管理 Reactor；
- ABI/API 具有兼容承诺。

### 5.2 高级公开接口

只有确实存在纯传输或框架集成场景时才提供。

例如未来：

- 原始传输会话；
- 连接组；
- 零复制缓冲区视图；
- 高级性能调优。

要求：

- 仍然不能要求调用者操作 Reactor 内部实现；
- 高级 API 是能力抽象，不是内部结构体转储；
- 与稳定公开接口分开文档和兼容级别。

### 5.3 内部接口

默认包括：

- Reactor；
- Runtime 分片；
- 命令/完成/定时器队列；
- 解析器/帧组装；
- 套接字辅助接口；
- 线协议编解码；
- Pipeline 注册表/路由/入口/CONTROL 传输；
- 仅所有者可用的生命周期辅助接口。

内部 API 可以为了性能暴露更多实现细节，但只能在库内部使用。

---

## 6. 句柄 / 结构体规则

公开句柄优先满足：

```text
不透明
小尺寸
代次安全
与实现无关
```

不应把以下布局作为公开 ABI：

```c
struct public_handle {
    struct internal_object *owner;
    uint32_t slot;
    uint32_t generation;
};
```

因为这会：

- 暴露实现所有权；
- 固化槽位表设计；
- 阻碍未来改变句柄编码；
- 允许上层绕过模块 API。

内部层可以继续采用槽位 + 代次；
公开层应通过不透明能力封装。

---

## 7. 配置规则

高层配置暴露**语义预算**，而不是默认暴露实现参数。

优先：

```text
max_connections
max_calls
max_active_transfers
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

- 放入独立高级调优结构体；
- 文档明确说明它不是协议/语义契约；
- 默认值必须能够正常工作；
- 不能要求普通业务理解内部队列才能正确使用系统。

`memory_budget_bytes` 只有在所有主要分片本地分配路径都参加同一个
预留/归还能力后，才能进入稳定语义配置。
部分覆盖时只能作为内部记账/证据，不能对外宣称进程或分片内存已经被完整限制。

---

## 8. 可观测性规则

高层统计可以汇总下层证据，但不等于把下层结构体直接嵌入公开 ABI。

推荐：

```text
服务端统计
  -> 语义 RPC/传输指标
  -> 可选高级运行时诊断
```

不推荐：

```text
高层服务端 ABI
  -> 嵌入精确 Reactor 内部队列/资源池布局
```

这样既保留性能诊断能力，又不会把运行时实现固化成门面 ABI。

---

## 9. 性能与分层

### 9.1 为什么两者不冲突

层边界不等于运行时跳转。

下面都是允许的高性能实现：

```text
RPC API
  -> 直接内部 C 调用
  -> 传输层所有者快速路径
  -> sendmsg
```

并不要求：

```text
RPC
  -> 队列
  -> 传输线程
  -> 队列
  -> 运行时线程
```

在同一个所有者域内可以直接调用下层内部函数。

### 9.2 热路径规则

为了性能，允许：

- 所有者线程直接快速路径；
- `static inline` 小函数；
- 固定容量表/资源池；
- 分散/聚集；
- 所有权转移；
- 零复制/尽量少复制；
- 分片本地缓存；
- 批处理；
- 预分配状态；
- 编译期特化。

不允许默认以“性能”为理由采用：

- 跨层共享可变结构体；
- 全局热路径互斥锁；
- 上层直接访问 Reactor 槽位；
- 每层一次堆分配；
- 每层一次序列化/反序列化；
- 每层一次缓冲区复制；
- 每层一个独立线程/队列；
- 每帧执行通用哈希/映射查找；
- 为了避免一个函数调用而破坏所有权边界。

### 9.3 为能力付出成本，而不是为分层付出成本

原则：

> 未使用的能力不应产生运行时成本；使用某一层抽象也不应因为“分层”自动增加复制或线程跳转。

例如：

- RPC 可以直接使用传输层内部 API；
- 传输层可以直接调用所有者 Reactor 快速路径；
- 连接组可以在所有者 Reactor 内直接操作注册表/亲和关系；
- 公开不透明句柄只改变 API 契约，不要求额外堆对象。

### 9.4 性能例外门槛

任何跨层性能例外都必须同时满足：

1. 性能分析/基准测试证明现有边界是实际瓶颈；
2. 明确记录优化前后数据；
3. 不破坏单所有者/生命周期正确性；
4. 优先通过内部快速路径解决；
5. 如果必须改变公开边界，新增 ADR；
6. 必须有回退方案和可测试语义。

不能以“可能更快”为理由提前暴露实现状态。

---

## 10. 当前审查

审查基线为文档修改前的 `main@f2e5eafdad82e4543163e53d4e69d207059c2fb0`。

本文最初的文档变更已经处理 A1（核心文档与备份业务耦合）：
核心总览/所有权/演进路线已经改为业务中立，
备份持久性被标记为**业务参考**。
其余 A2-A10 记录当前代码/API 的收敛过程。

### 10.1 已满足

| 项目 | 状态 | 说明 |
|---|---|---|
| Runtime 分片所有权 | 满足 | 监听器、对端状态、执行器/资源已经分片本地化 |
| Reactor 单所有者 | 满足 | 协议可变状态基本遵循所有者修改 |
| 工作线程完成事件返回所有者 | 满足 | 完成事件返回原所有者，不直接修改协议状态 |
| Pipeline 实现组件 | 满足 | 语义核心/协议/CONTROL 协调器位于 `src/group/`；服务端/客户端组传输端点与入口/CONTROL 适配器位于 `src/transport/group/` |
| Pipeline 代码层业务独立 | 基本满足 | 当前代码使用通用 pipeline/control/data/stream 名称，没有引入 `backup_id`/检查点等业务对象 |
| 有界资源模型 | 满足 | 队列/资源池/表大部分都有显式容量 |
| 热路径无跨分片共享资源池 | 满足 | 当前分片资源所有权与设计方向一致 |

### 10.2 历史审查项 / 当前收敛状态

#### A1. 核心架构文档与备份业务耦合 — 高 — 已通过文档调整解决

原状态：

- `00-overview.md` 的系统上下文直接写备份；
- 所有权关系图使用“备份 Pipeline”；
- 核心不变量包含持久备份状态修改；
- `04-backup-pipeline.md` / `05-durability-recovery.md` 位于核心阅读顺序；
- `06-evolution.md` 把备份正确性当作 c-trpc 下一阶段。

问题：

这会把业务正确性自然下沉到 RPC/传输核心。

处理结果：

- 核心架构改用通用应用 / 连接组 / Pipeline；
- 备份持久性已经降为业务参考；
- 核心演进路线把阶段 6 改为 API 边界清理，不再实现 `backup_id`/检查点/提交。

---

#### A2. SDK 头文件安装没有可见性边界 — 高 — 已解决

基线问题：

```lua
add_headerfiles("include/(tr/*.h)")
```

曾经会把新增 `include/tr/*.h` 自动发布为 SDK 契约。

处理结果：

- `xmake.lua` 使用显式安装头文件白名单；
- CI 对安装后的头文件集合执行精确差异检查，并显式断言历史内部头文件不得安装；
- `include/tr/` 的物理内容现在与稳定安装 SDK 闭包一致，只保留 8 个公开头文件；
- Reactor/Channel/Buffer/解析器/线协议/套接字、CRC/endian、生命周期与诊断头文件都已经回收到 `src/`；
- `rpc.h` 只保留应用 RPC 契约，Endpoint 引擎构造位于 `src/rpc/rpc_internal.h`。

以后新增高级公开能力必须显式加入白名单并定义兼容级别，不能依靠目录位置自动发布。

---

#### A3. 内部数据结构成为公开 ABI — 高 — 稳定 SDK 已解决

基线中命令队列、解析器、Buffer 资源池、Reactor/Channel 等实现布局
曾位于公开包含目录，容易被误认为 SDK ABI。

处理结果：

- 命令/完成/定时器队列、解析器/帧/线协议、Buffer、Reactor、Channel 等结构都只位于 `src/`；
- 稳定安装头文件不暴露 pthread 互斥锁、环形队列布局、资源池空闲链表、解析器状态或 Reactor 槽位/代次；
- 仓库内部测试仍可以直接测试这些引擎契约，但不会因此扩大 SDK 兼容契约；
- 未来零复制/高级传输能力必须通过不透明所有权 API 公开，不能重新暴露分配器/运行时布局。

---

#### A4. Channel 公开 API 要求 Reactor 内部信息 — 高 — 稳定 SDK 已解决

基线 `tr_channel_create()` 要求调用者传入 `tr_conn_handle`，
把 Reactor 指针 + 槽位 + 代次暴露给传输层使用者。

第二阶段处理结果：

- `channel.h` / `reactor.h` 已退出安装的稳定 SDK；
- 客户端/服务端门面仍在库内部组合 Channel/Reactor；
- 仓库内部测试可以继续直接测试低层 Channel；
- 通用连接组/传输公开能力单独设计，不复用 `tr_conn_handle` 作为业务契约。

因此该问题对稳定门面已经解除；低层 Channel 现在是内部引擎 API。

---

#### A5. rpc.h 混合应用 RPC API 与下层构造接口 — 高 — 已解决

第二阶段已经拆分：

```text
include/tr/rpc.h
  -> Method / Call / Streaming 应用契约

src/rpc/rpc_internal.h
  -> Endpoint 配置/创建/销毁
  -> Channel 绑定
  -> 执行器配置/统计
  -> 内部 Buffer 快速路径
```

`rpc.h` 不再包含 Channel/Buffer/Reactor，也不声明 Endpoint 引擎构造。
客户端/服务端用户无需知道 Endpoint 如何绑定传输层。

---

#### A6. 公开句柄暴露所有者指针 + 槽位/代次 — 中/高

基线对象：

- `tr_rpc_call_handle`；
- `tr_stream_handle`；
- `tr_conn_handle`。

这些实现作为内部能力技术是正确的，但公开布局会固化内部对象模型。

处理结果：

- 稳定公开 `tr_rpc_call_handle` 现在只暴露固定大小 `_private` 能力；
- Endpoint 指针、槽位、代次的编码/解析只存在于 RPC 引擎；
- 句柄仍按值复制，不增加分配、锁、队列或线程跳转；
- 代次隔离和陈旧句柄检查语义保持不变。

---

#### A7. 高层门面配置泄露队列/资源池实现 — 中 — 已解决

`tr_facade_limits` 曾直接包含命令/TX/RX/资源池/执行器工作线程/节点等实现容量。

当前已经完成收敛：
这些字段已经退出稳定配置，统一进入仓库内部 `tr_facade_tuning`；
公开创建使用门面内部默认值，只有基准/架构诊断通过带调优参数的创建接口精确指定布局。

后续如果需要用户控制过载行为，应新增最大进行中工作量/准入等语义策略，
不能重新公开 Reactor 分配器、工作线程数量或执行器节点数量。

---

#### A8. 高层服务端统计嵌入运行时布局 — 中 — 稳定 SDK 已解决

原 `tr_server_stats` 直接嵌入 Reactor/资源池实现。

第二阶段已经把：

- `tr_server_stats`；
- `tr_server_get_stats()`；
- `tr_client_get_channel_stats()`；
- `tr_client_get_rpc_stats()`

移动到 `facade_diagnostics_internal.h`。
现有基准/测试继续使用内部完整诊断，
稳定服务端/客户端头文件不再绑定运行时统计布局。

稳定聚合 RPC 语义可观测性已经由 `tr_rpc_semantic_stats`
与客户端/服务端门面访问接口提供；
引擎级诊断继续保持内部化。

---

#### A9. 汇总头文件暴露分配器 — 中 — 已解决

`tr/trpc.h` 已移除 `buffer.h`。
安装 SDK 现在只发布门面/RPC 应用头文件；
Buffer/Channel/Reactor/Frame/Wire 都不再属于稳定安装表面。

内部尽量少复制的快速路径仍然保留，
不会因为 API 清理增加复制。
未来如果公开零复制，将通过不透明所有权抽象设计。

---

#### A10. CI 把 Reactor 当作安装后的外部 SDK API — 高 — 第一阶段已解决

基线安装测试直接编译外部 Reactor 使用程序，
使 Runtime API 成为事实上的兼容契约。

第一阶段已经改为：

- 外部使用程序只包含 `<tr/trpc.h>`；
- 只使用客户端/服务端门面配置与状态 API；
- 安装冒烟测试对已发布头文件集合执行精确校验；
- Reactor 行为测试继续保留在仓库内部测试中。

第二阶段已经解除 `rpc.h -> channel.h -> reactor.h` 的传递依赖，
Reactor/Channel/Buffer/Frame/Wire 已经退出安装 SDK。

---

### 10.3 不构成问题的内容

以下内容本身不构成架构违规：

- `src/` 当前采用平铺或混合目录形式；
- 客户端/服务端实现内部向下调用 Runtime/Channel；
- 内部测试直接包含 `../src/*_internal.h`；
- 所有者快速路径直接函数调用；
- 为零复制使用 Buffer 所有权转移；
- 内部槽位 + 代次能力。

是否需要物理目录重构应由可维护性决定，
不能把目录形式误当成分层本身。

---

## 11. 清理优先级

建议按以下顺序收敛，而不是一次性大规模改名。

### P0 — 架构契约

- 本文成为核心架构不变量；
- 备份持久性从核心演进路线/阅读顺序移出；
- 新功能评审先检查分层/模块/API 契约。

### P1 — 公开头文件边界 — 已完成

已完成：

- 显式安装头文件白名单；
- 精确安装集合 CI 门禁；
- 外部冒烟测试改用公开门面；
- `command_queue/parser/rpc_wire/socket/endian/guard/refcount/crc32c` 退出 SDK。

第二阶段补齐：

- RPC/Channel/Runtime 传递依赖已经拆除；
- Buffer/Channel/Reactor/Frame/Wire 已退出稳定门面闭包；
- 安装 SDK 只保留自包含门面/RPC 头文件。

### P2 — RPC 公开表面 — 已完成

已完成：

- 应用 RPC API 与 Endpoint 引擎分离；
- 客户端/服务端用户不再看到 Channel/Endpoint 构造；
- 保留消息使用不透明释放令牌保持零额外分配/复制；
- `tr_rpc_call_handle` 已经不透明化，不再公开 Endpoint 指针 + 槽位/代次，
  同时保持原有陈旧代次隔离。

### P3 — 传输 / 连接组公开能力 — 已完成

第一阶段已完成：

- 新增稳定 `tr/transport.h`，服务端可以显式启用有界连接组能力；
- `tr_server` 拥有公开组监听器生命周期，并把组连接容量纳入 Reactor 预留预算；
- CONTROL 授权只暴露 `group_id/epoch`；
- DATA 接收只暴露语义组/Stream/消息/字节，不暴露
  Reactor 句柄、注册表、TRR1 或成员代次；
- `TAKE_OWNERSHIP` 通过不透明释放令牌直接保留原 RX Buffer，不增加载荷复制；
- 现有 Pipeline/注册表/路由/CONTROL/入口引擎保持内部单所有者实现。

第二阶段已完成：

- 客户端门面可以通过 `group_id/epoch` 建立/关闭一个活动组 CONTROL 连接；
- CONTROL TRR1 路由由传输层内部生成，所有者分片/成员代次不进入公开 API；
- fd 接管与 CONTROL 处理器安装在同一个 Reactor 所有者轮次完成，
  不存在已接收帧进入空处理器/默认处理器的窗口；
- 客户端销毁在停止 Runtime 前同步回所有者关闭组 CONTROL。

第三阶段已完成：

- 客户端增加语义 `max_data_connections` 上限；
  0 保持仅 CONTROL，非 0 才自动建立 DATA 通道；
- `DATA_OFFER` 由内部客户端组引擎消费，DATA 索引/代次不进入公开 API；
- DATA 连接使用非阻塞套接字 + Reactor 所有的辅助 fd 事件 + Reactor 定时器，
  不新增连接线程；
- 同时最多一个 DATA 套接字处于连接/前导信息建立阶段，
  多个 DATA 通道可以保持 `ATTACHED`，资源量由语义上限约束；
- 客户端 Reactor 连接预算从该 DATA 上限推导，
  不再依赖 DATA 通道的隐式魔数容量；
  组引擎/资源池/定时器仍按首次组 API 使用惰性创建，
  普通 RPC 客户端不承担未使用能力成本；
- DATA 路由附着失败时，服务端只精确取消仍为 `RESERVED` 的能力；
  已经 `ATTACHED`、陈旧或复用代次不受影响；
- 对端 `DATA_CANCEL` 对精确代次幂等：
  `RESERVED -> FREE`，`ATTACHED/FREE` 为无操作，代次已经复用才 `STALE`，
  因此前导信息移交不需要新增附着确认；
- `max_data_connections == 0` 时继续由客户端内部精确 `DATA_CANCEL`
  归还预留。

第四阶段已完成：

- 客户端消费 `TRANSFER_READY`，并要求线协议中的 DATA 索引/代次
  精确命中当前 `ACTIVE` DATA 通道；
- 客户端使用 `limits.max_streams` 作为有界本地传输亲和关系容量，
  不新增重复公开容量配置项；
- 公开 READY 回调只暴露 `group_id/epoch + stream_id + CONTROL message_id`，
  DATA 路由能力保持内部化；
- 客户端传输释放回到同一个 Reactor 所有者删除亲和关系；
  DATA 通道关闭会按精确代次失效对应亲和关系，
  槽位复用不会让旧 Stream 漂移到新连接。

第五阶段已完成：

- 客户端公开 `tr_client_connection_group_send()` 只按 READY 亲和关系发送，
  不允许重新选择 DATA 通道；
- 每次发送都重新验证精确 DATA 成员代次，
  通道替换后旧 Stream 返回 `STALE`；
- 公开字节只在调用期间借用；返回 `TR_OK` 前复制到所有者本地拥有的 Buffer，
  应用可以立即复用源内存；
- 发送内存上限从 `max_data_connections * max_message_bytes` 推导，
  不新增实现调优配置项；
- Reactor 原有 DATA 路径继续负责 `FIRST/LAST`、分片、TX 公平性和 `EAGAIN`；
  组层不新增第二发送队列；
- TX 完成/连接关闭/提交失败通过一次性 Buffer 释放钩子回收额度；
- 当前背压契约为 `TR_AGAIN + 调用方重试`，
  不发布可能产生虚假就绪的单一可写回调。

第六阶段已完成：

- 服务端 `begin_drain` 只关闭组连接接收，保留既有组/DATA/传输；
  进入排空后不再创建新的 `DATA_OFFER / TRANSFER_READY`；
- 排空前已经接收但尚未完成 CONTROL 前导信息的套接字，
  在路由附着时仍会被排空屏障拒绝，避免延迟创建新组；
- 客户端 `begin_drain` 精确取消已经排队/连接中的 DATA 能力，
  后续 `DATA_OFFER` 只执行取消；
  它同时是本地 READY 准入屏障，屏障后观察到的新 `TRANSFER_READY`
  不安装亲和关系，屏障前已经 READY 的传输可以继续完成；
- 客户端 `wait_drained` 等待活动传输、拥有的发送载荷与等待中的建立操作静默；
  服务端 `wait_drained` 等待既有组/连接自然归零；
- 强制 `stop()` 与优雅排空语义明确分离；
- 稳定客户端统计暴露组/CONTROL/DATA/传输/发送字节生命周期；
  稳定服务端统计暴露组/连接/DATA/传输与接收/拒绝计数；
- 服务端 DATA 入口在应用回调前验证
  `stream_id -> 精确 DATA 代次/连接` 亲和关系；
  物理 DATA 附着本身不构成传输授权，
  未经过 `TRANSFER_READY` 的 DATA 帧会关闭违规 DATA 通道；
- DATA 附着时一次性绑定所有者本地 Pipeline 能力，
  帧热路径只执行现有有界 Stream 亲和关系查找，
  不执行逐帧注册表路由/哈希，也不新增锁或所有者跳转；
- Reactor 槽位/代次、TRR1 标识、连接器、队列/资源池占用继续保持内部化。

P3 公开能力至此闭环；
后续只接受缺陷修复、验证和性能分析驱动的扩展。

### P4 — 配置 / 统计分离 — 进行中

已完成：

- 详细 Reactor/Channel/Endpoint 诊断退出稳定门面头文件。

已经推进：

- 连接组已经具备稳定语义统计，并与内部 Reactor/Pipeline 诊断分离；
- P4 第一阶段已经把命令环、DATA TX、CONTROL TX、RX Buffer、
  RPC 消息资源池数量、重组资源池数量从稳定 `tr_facade_limits`
  移到仓库内部 `tr_facade_tuning`；
- P4 第二阶段进一步把执行器工作线程数量、每 Endpoint 节点容量、
  续处理预留移到内部调优；
  高层门面不再承诺当前工作线程池和任务节点实现布局；
- P4 第三阶段把计时可观测性标志移到内部调优，
  并将 `observability.h` 从安装 SDK 白名单移除；
  固定直方图与队列/资源池快照类型现在明确属于引擎诊断契约；
- P4 第四阶段增加稳定聚合 `tr_rpc_semantic_stats`：
  已开始、已完成、进行中和最终状态分布；
  客户端读取当前 Endpoint，服务端通过分片所有者快照 + 退役对端最终清理器聚合，
  不暴露 Runtime 布局；
- 公开客户端/服务端创建只接受语义配置，
  并由门面内部生成 Runtime/资源池默认值；
  基准/架构测试通过内部 `*_create_with_tuning()` 保留精确资源实验能力；
- 服务端内部调优仍采用“聚合预算 -> 确定性分片拆分”，
  但公开路径会保证隐藏默认值至少支持每个已配置分片一个必要资源单元。

已完成：

- RPC 编码消息所有权使用有界按需资源池：
  描述符/槽位数量继续由内部调优限制，
  单个槽位按实际编码大小增长并复用；
- 门面不再预分配 `pool_count * max_message_bytes` 的完整存储，
  也不再需要稳定 `rpc_message_buffer_bytes`；
- 客户端所有权保持本地，服务端所有权保持分片本地；
  扩容发生在资源池互斥锁之外，不新增跨分片共享热状态。

待完成：

- 如果需要应用可配置过载策略，设计最大进行中工作量/准入等语义策略，
  不能重新暴露执行器节点/线程实现数量；
- 每服务/方法语义可观测性如果有需求，
  必须使用分片本地记账后再在控制面聚合，
  不能增加共享热计数器。

### P5 — 物理目录清理 — 进行中

能力边界已经稳定，开始让物理目录反映现有所有权/模块契约，
但每一步都只做文件归位与包含/构建依赖收敛，
不借目录迁移改变运行语义。

第一至第十五阶段已完成：

```text
src/rpc/
  rpc.c
  rpc_internal.h
  rpc_codec.c
  rpc_wire.c
  rpc_wire.h

src/runtime/
  runtime.c
  runtime_internal.h

src/group/
  pipeline.c
  pipeline_internal.h
  pipeline_registry.c
  pipeline_registry_internal.h
  pipeline_route.c
  pipeline_route_internal.h
  pipeline_control_wire.c
  pipeline_control_wire_internal.h
  pipeline_control.c
  pipeline_control_internal.h

src/transport/group/
  pipeline_ingress.c
  pipeline_ingress_internal.h
  pipeline_control_transport.c
  pipeline_control_transport_internal.h
  pipeline_listener.c
  pipeline_listener_internal.h
  client_group.c
  client_group_internal.h

src/transport/protocol/
  wire.c
  wire.h
  frame.c
  frame.h
  parser.c
  parser.h

src/transport/channel/
  channel.c
  channel.h
  channel_internal.h

src/io/
  socket.c
  socket.h
  socket_internal.h
  connector.c
  connector_internal.h

src/execution/
  reactor.c
  reactor.h
  reactor_internal.h
  command_queue.c
  command_queue.h
  completion_queue.c
  completion_queue.h
  timer_queue.c
  timer_queue.h
  buffer.c
  buffer.h
  buffer_internal.h
```

规则：

- 稳定/公开头文件继续留在 `include/tr/`，
  不因源码目录移动扩大或缩小 SDK ABI；
- RPC 内部实现只通过 `src/rpc/rpc_internal.h`
  向门面/运行时暴露引擎契约；
- Runtime 物理模块只包含分片/生命周期/资源域编排；
- Group 物理模块已经包含 Pipeline 成员关系/亲和关系语义核心、
  分片本地注册表、TRR1/TRC1 协议编解码，以及 CONTROL 生命周期协调器；
- CONTROL 协调器可以在 Reactor 所有者轮次内请求精确 DATA 连接销毁，
  但不拥有监听器/fd 注册、TRP1 TX 队列或套接字会话实现；
- 组专用入口与 CONTROL 传输适配器已经进入
  `src/transport/group/`：
  负责 TRR1 前导信息门控、DATA 附着/解除、CONTROL 帧 RX/TX
  与 Reactor 连接处理器绑定；
- 入口/CONTROL 传输适配器不成为组语义状态所有者；
  Pipeline/注册表/CONTROL 生命周期仍由 `src/group/` 定义；
- Pipeline 监听器已经进入 `src/transport/group/`，
  作为服务端组传输端点：
  拥有监听 fd、Reactor 注册、已接收连接/会话槽位、
  准入和有界 CONTROL 消息资源池，
  但不依赖服务端门面对象；
- 客户端组端点也已经进入 `src/transport/group/`：
  拥有 CONTROL 连接、DATA 连接器/通道状态、READY 亲和关系消费、
  有界发送所有权和 Reactor 处理器/定时器编排；
  它不依赖 `struct tr_client`；
- 稳定 `tr_client_connection_group_*` 门面包装继续留在 `client.c`，
  负责把客户端配置/Runtime 能力组合到该传输端点；
- TRP1 线协议/帧/解析器实现已经进入 `src/transport/protocol/`；
  这一层只负责固定头部编解码、帧所有权辅助和增量解析器，
  不拥有套接字、Reactor 事件循环、连接/Stream 状态；
- TRP1 线协议/帧/解析器能力头文件已经收敛到 `src/transport/protocol/`；
- Channel 状态引擎已经进入 `src/transport/channel/`：
  拥有 HELLO/GOAWAY、Stream 槽位/索引、消息顺序、流量控制、
  排空、保活和重连策略；
- Channel 只通过 Reactor/连接器/套接字能力执行网络动作；
  这些模块仍属于执行/资源基础设施，
  不因为 Channel 位于传输层就被机械搬入；
- Channel 能力头文件已经收敛到 `src/transport/channel/`；
- Linux 套接字原语与 Reactor 所有的非阻塞连接器已经进入 `src/io/`；
  Socket 只负责 fd/TCP 原语，
  Connector 只负责连接/前导信息/定时器/辅助 fd 执行，
  不拥有 Channel/Group 语义状态；
- `socket.h` 已从 `include/tr/` 回收到 `src/io/`，
  作为仓库内部 I/O 能力头文件；
  生产代码与内部测试不再从公开包含树获取套接字原语；
- `crc32c.h` 与 `endian.h` 已从 `include/tr/` 回收到 `src/` 根部；
  前者是跨模块校验和能力，后者是无状态小端编解码辅助接口，
  两者都不属于稳定 SDK 契约；
- `cleanup.h`、`guard.h`、`refcount.h`
  已从 `include/tr/` 回收到 `src/` 根部；
  它们定义编译期清理、互斥锁作用域保护和强引用原语，
  不属于稳定 SDK；
- `observability.h` 已回收到 `src/observability.h`；
  直方图、队列/资源池快照与计时标志属于引擎诊断契约，
  稳定语义可观测性继续由 `tr/rpc.h` 定义；
- `socket_internal.h` 不再包含门面策略；
  `TCP_NODELAY` 策略校验/映射已经回收到 `facade_policy_internal.h`，
  保持 `src/io/` 对门面无反向依赖；
- Reactor 实现与命令/完成/定时器队列已经进入 `src/execution/`；
  Reactor 是事件执行所有者，
  命令/完成队列是有界跨上下文移交，
  定时器队列是仅所有者访问的有界调度器；
- `command_queue.h` 已从 `include/tr/` 下沉到执行层本地头文件；
  完成/定时器队列头文件同样保持本地；
- Reactor 能力/内部头文件已经收敛到 `src/execution/`；
- Buffer 实现/资源池已经进入 `src/execution/`：
  只定义有界 Buffer 描述符所有权、固定/动态资源池获取/释放
  与资源压力统计，
  不拥有 RPC/Channel/Group 语义状态；
- 执行/传输引擎能力头文件已经回收到各自源码模块：
  `reactor.h/reactor_internal.h/buffer.h` 位于 `src/execution/`，
  `wire.h/frame.h/parser.h` 位于 `src/transport/protocol/`，
  `channel.h` 位于 `src/transport/channel/`；
- 生产代码不再通过 `include/tr/` 获取这些内部引擎类型；
  `include/tr/` 物理目录与稳定安装头文件闭包完全一致，
  只包含既有 8 个 SDK 头文件，
  且不存在 `include/tr -> src/` 反向包含；
- RPC 线协议头文件已经从 `include/tr/rpc_wire.h`
  回收到 `src/rpc/rpc_wire.h`；
  TRPC 请求/响应/取消/状态线协议布局与元数据分帧
  明确属于 RPC 引擎内部契约，不进入稳定 SDK；
- Reactor、命令/完成/定时器队列不会因为概念上的 Runtime 层名称
  被机械搬入 `src/runtime/`；
  它们仍然是独立执行基础设施；
- 模块对同级内部依赖使用显式跨目录包含；
- 测试直接引用内部契约时也使用新的物理路径；
- 本阶段不改变线程、所有者、锁、队列、资源上限、线协议或热路径。

P5 源码树头文件清理已经完成：

```text
include/tr/ == 稳定安装 SDK 头文件闭包
内部引擎 / 工具 / 诊断头文件 -> src/
```

目录结构服务于已经确定的职责，而不是反过来决定架构。

---

## 12. 评审门禁

以后新增功能/PR 必须至少回答：

1. 属于哪一层？
2. 哪个模块拥有状态？
3. 对上一层提供什么能力？
4. 是否要求上一层理解内部状态？
5. 是否引入新的跨所有者可变共享？
6. 资源上限在哪里？
7. 故障/销毁的所有权如何闭环？
8. 热路径是否新增分配/复制/队列/线程跳转？
9. 如果跨层是为了性能，基准证据是什么？
10. 公开 API 是否扩大了兼容性契约？

只要第 4、5、8、9 项无法解释清楚，就不应直接合入核心架构。
