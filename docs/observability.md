# 运行时可观测性

c-trpc 在运行时内部收集结构化快照，而不是把某个指标后端嵌入核心库。
完成 API 边界清理后，低层 Reactor/Channel/Endpoint 快照结构属于**内部诊断契约**，
供仓库内基准测试/测试使用；稳定语义可观测性通过另一套独立 API 提供。

## 成本模型

低成本计数器和有界队列高水位始终采集。

计时指标属于按需启用的**仓库诊断能力**，不是稳定门面配置。
基准测试/架构测试通过内部调优入口启用：

```c
struct tr_facade_tuning tuning;

tr_facade_tuning_init(&tuning);
tuning.observability_flags = TR_OBSERVABILITY_TIMING;
tr_server_create_with_tuning(&config, &tuning, &server);
```

该标志会在内部传播到 Reactor 和 RPC Endpoint 引擎配置。
关闭计时时，调度热路径不会执行延迟直方图所需的额外单调时钟读取。

`src/observability.h` 是仓库内部诊断契约。Reactor/RPC 引擎诊断使用其中固定的
直方图和队列/资源池快照类型；稳定 RPC 语义可观测性仍由已安装的 `tr/rpc.h` API 定义。

计时直方图属于诊断采样，不是事务式记账计数器。
只有相关单调时钟读取成功时才记录计时样本，因此 `samples` 可能小于对应任务/轮次计数。
非计时计数器仍是精确工作量记账的事实来源。

`tr_latency_histogram` 是固定 64 桶、以 2 为底按纳秒分桶的直方图。
它不执行分配；聚合快照时可以对对应桶、样本数和总和求和，并对 `max_ns` 取最大值。

## 稳定 RPC 语义可观测性

已安装 SDK 暴露一份有意保持精简的聚合快照：

```c
struct tr_rpc_semantic_stats {
    uint64_t calls_started;
    uint64_t calls_finished;
    uint64_t calls_inflight;
    uint64_t final_status[TR_RPC_STATUS_COUNT];
};
```

门面访问接口：

```c
tr_client_get_rpc_semantic_stats(client, &stats);
tr_server_get_rpc_semantic_stats(server, &stats);
```

该契约不依赖 Endpoint 槽位、执行器工作线程和传输连接：

- 客户端 `calls_started`：Call API 返回 `TR_OK` 并发布一个 Call 能力句柄；
- 服务端 `calls_started`：已注册 Method 接受第一条有效 `REQUEST`，包括随后被拦截器/准入拒绝的 Call；
- `calls_finished`：一次最终应用可见 RPC 结果已经提交；
- `calls_inflight`：快照值 `started - finished`；
- `final_status[status]`：终止状态恰好一次分类；所有桶之和等于 `calls_finished`。

因此取消和截止时间不是单独的计数来源，而分别落入
`TR_RPC_STATUS_CANCELLED` 和 `TR_RPC_STATUS_DEADLINE_EXCEEDED`。
同样，过载和连接丢失分别表现为
`TR_RPC_STATUS_RESOURCE_EXHAUSTED` 和 `TR_RPC_STATUS_UNAVAILABLE`。

服务端快照通过现有所有者/最终清理路径聚合存活分片和已经退役的对端，
不会新增全局热路径原子变量。服务端跨分片快照是结构化一致而不是全局原子一致，
因此采集期间计数仍可能继续变化；排空完成后快照稳定。

V1 仅提供聚合 RPC 可观测性。未来可以使用分片本地记账增加每服务/方法语义拆分，
但不能引入跨分片共享热计数器。

## Reactor 瓶颈归因

`tr_reactor_get_stats()` 报告：

- 命令队列和完成队列的 `capacity/current/peak/full_events`；
  命令 `full_events` 表示生产者第一次遇到满环形队列的压力：
  `SEND/RESUME` 等异步 API 仍立即返回 `TR_AGAIN`，
  `CALL/QUIESCE/SET_HANDLER/STOP` 这类同步或生命周期请求会进入队列本地容量等待；
  完成队列 `full_events` 表示工作线程移交时遇到满环形队列并进入容量等待；
- 每轮有界工作量与预算耗尽计数；
- 启用计时时的 `busy_ns` 和 `poll_ns`；
- 每轮忙碌时间直方图。

一个有用的饱和度信号是：

```text
reactor_busy_ratio = busy_ns / (busy_ns + poll_ns)
```

Reactor 忙碌比例较高且 RPC 队列等待较低时，更可能是 Reactor 成为受限执行资源。
队列高水位与预算耗尽计数可以进一步区分压力来自命令/完成调度还是 RX/TX 工作。
尤其是完成队列 `full_events` 增长，表示工作线程曾被 Reactor 的完成事件消费速度反压，
而不是完成事件被丢弃；需要结合 Reactor 忙碌比例、完成预算耗尽和执行器队列等待判断瓶颈位置。

## Reactor 命令归因

Reactor 快照也暴露已有的每轮命令公平性数据：

- `limits.commands`：每个事件循环轮次的命令预算；
- `total.commands`：已处理命令总数；
- `max_per_turn.commands`：观察到的单轮最大命令数；
- `budget_hits.commands`：耗尽完整命令预算的轮次数。

命令队列生产者侧压力按 `SEND`、`RESUME_RX`、`CALL` 和其他类别分类，
每类都记录成功入队数与第一次遇满次数。

对于异步 `SEND/RESUME`，遇满计数表示立即背压；
对于同步 `CALL` 以及其他类别中的 `QUIESCE/SET_HANDLER/STOP`，
则表示进入容量等待，而不是命令被丢弃。
这些计数都复用命令队列现有互斥锁更新，不增加分配、时钟读取或额外指标锁。

当发送发生在所属 Reactor 上时，只有在不存在待处理命令，且 Reactor 当前没有分发
已经从环形队列复制出的命令批次时，`SEND` 才能直接附着到对应连接的 TX 队列。
这样可以在所有者快速路径周围保持命令 FIFO 顺序。
直接所有者本地发送不会增加 `command_send.enqueued`；
如果所有者发送必须保持此前命令顺序边界，则回退到环形队列并正常计数。
因此该计数器衡量的是真实排队 `SEND` 压力，而不是传输帧总数。

当队列已满但 Reactor 忙碌时间较低时，这一区分尤其重要：
命令预算耗尽率高，说明可能受到每轮有界公平调度或突发排空行为影响；
某个生产者类别占据大部分队列满事件，则指向特定上游压力来源。

## RPC 执行器归因

内部 `tr_rpc_endpoint_get_stats()` 快照报告：

- 执行器队列 `capacity/current/peak/full_events`；
- 当前和峰值就绪 Call 数；
- 已接受任务总数和工作线程已取走任务总数；
- `executor_admission_limit_hits`：新 Call 在物理节点池真正耗尽前，
  因续处理预留而停止准入的次数；
- `executor_hard_full_events`：物理任务节点池耗尽次数；
- 启用计时时的队列等待和回调/任务执行直方图。

准入限制命中和物理池耗尽计数有意分离。
预留命中表示运行时正在为已经接受的流式 Call 保护容量，
并不意味着执行器节点池已经物理耗尽。

## 服务端门面聚合

内部 `tr_server_get_stats()` 聚合为仓库基准测试提供一份完整引擎级视图，包括：

- 服务端拥有的 Reactor 快照，包括 RX/TX/CONTROL TX 资源池压力；
- RPC 消息和重组资源池 `capacity/current/peak/exhausted_events`；
- 当前对端/Call/Stream 数量；
- 存活对端和已经回收对端的 RPC 与 Channel 生命周期累计计数；
- 跨对端 Endpoint 合并的执行器队列等待和处理器直方图；
- 每 Endpoint 执行器队列/就绪 Call 最大高水位。

一个已经回收的对端在最终统计合并到服务端退役累计值前，
会先完成同步静默，并确保其 RPC 执行器完全排空。
这样可以避免断连/回收截断最后一批工作线程完成事件。

跨组件服务端快照是结构化一致，而不是全局原子一致：
单个组件快照内部保持一致，但采集期间计数可能继续推进。
`tr_server_drain()` 停止新连接接收/回收并排空活动工作后，
该快照足够稳定，可用于运行结束后的基准归因。

## 解释方式

典型模式：

- Reactor 忙碌比例高 + 执行器队列等待低：增加工作线程前先检查 Reactor 可扩展性；
- Reactor 忙碌比例低 + 执行器队列等待高：检查工作线程数量、回调成本或执行器分片；
- 准入限制命中高但物理池耗尽低：续处理预留正在主动保护已经接受的 Stream；
- 物理池耗尽事件增加：有界执行器容量已经耗尽，应先与队列等待和回调时长关联分析，再决定是否修改容量。

可观测性计数器提供的是证据，不是自动策略。
CI 应继续以确定性不变量作为门槛，而不是固定 QPS 或延迟阈值。
