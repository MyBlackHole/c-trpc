# 架构演进路线

**状态：实施计划**

不会一次性同时引入多 Reactor、多套接字、Pipeline 恢复和新的 RPC 执行模型。

## 阶段 0：当前基线

```text
服务端：
  1 个 Reactor
  Reactor 所有者接收连接
  分片本地执行器
  Reactor 本地定时器

Channel：
  CONTROL + 1 个 BULK

RPC：
  不可变任务
  工作线程回调修改状态 -> Reactor 所有者命令
  每 Reactor 独立完成队列
```

## 阶段 1：RPC 完成事件所有权

先处理一元调用：

```text
Reactor
 -> 任务
 -> 工作线程
 -> 完成事件
 -> 原 Reactor
```

目标：工作线程不直接修改 Call/Endpoint/Stream 协议状态。

一元调用完成事件、流式调用的发送/结束/取消、元数据/取消状态查询，
以及工作线程 RX 额度归还/关闭路径都已经回到 Reactor 所有者。

## 阶段 2：完成队列与调度器

**状态：进行中**

已完成：

- 每个 Reactor 一个有界完成队列；
- 与命令队列独立的唤醒合并；
- 完成事件批量排空；
- 完成事件积压时的非阻塞事件循环续处理；
- `STOP` 前已经接受的完成事件具有排空屏障；
- 完成事件准入已经下沉到完成队列自身锁域，工作线程热路径
  不再经过 Reactor `ctl_lock`；
- 每轮只处理一个命令批次，唤醒路径不重复消费命令预算；
- 命令积压的保守非阻塞续处理，以及公平性/退出回归测试；
- 命令、完成事件、定时器、RX、TX 共享整轮额度；
- RX/TX 就绪轮转、单连接处理量子与严格线协议字节预算；
- 所有者一致快照的累计工作量、单轮峰值、额度耗尽、epoll 等待和定时器迟到采样；
- 无载荷 PING/PONG/WINDOW_UPDATE 的有界帧边界优先级、控制顺序屏障
  与跨轮 DATA 防饥饿，见 [控制帧调度](../tx_priority.md)。

命令预算与唤醒推导见 [Reactor 命令调度公平性](../reactor_fairness.md)。
整轮限额、配置语义与统计口径见 [Reactor 整轮预算与调度诊断](../reactor_budget.md)。

仍待完成：

- 逻辑 CONTROL 通道业务 DATA 的独立优先策略（目前仍按 DATA FIFO）；
- 根据真实性能分析决定是否增加队列高水位、排队延迟与直方图等指标，
  不能把最小调度统计误认为完整性能观测体系。

已经提供 CRC32C 组件微基准；它不是端到端 RPC/BULK 性能证明。
生产化与后续测量门槛见 [性能与生产化评估](../performance_readiness.md)。

## 阶段 3：运行时分片抽象

**状态：已完成**

第一阶段已经完成：

- 引入内部 `tr_runtime` / `tr_runtime_shard`；
- 客户端/服务端不再直接拥有 Reactor 生命周期；
- Runtime 统一负责 Reactor 创建/启动/停止/销毁；
- 建立稳定分片标识；阶段 3 的门面使用 `shard_id = 0`；
- 公开 API、线协议、线程数量与启动时机保持不变；
- 独立测试验证单分片标识与完整生命周期语义；
- 服务端 RPC 执行器组已经从服务端全局所有者下沉到 `shard[0]`；
- 服务端 Endpoint 只进入所属分片的工作线程池，不再依赖服务端全局执行器所有者；
- 服务端监听 fd / 绑定端口已经从服务端下沉到 `shard[0]` 所有权；
- 监听器已经注册进 `shard[0]` Reactor epoll，独立接收线程已经删除；
- 接收回调有固定批次上限，监听积压由后续 Reactor 轮次继续处理；
- 对端槽位存储/容量/高水位/回收中/就绪/拒绝计数器已经下沉到 `shard[0]`；
- 服务端仍负责对端 Channel/RPC 构造；断连销毁已经拆成 Reactor 所有者解除关联 + 外部最终清理；
- 所有者解除关联会移除 Channel/RPC 回调、截止时间/保活定时器源，并关闭执行器准入；
- 只有所有者解除关联成功后对端才离开分片表；回收流程只等待工作线程引用、采集统计并释放；
- Reactor 接收连接、对端预留/发布/表解除关联/存活快照已经全部变为仅所有者访问，
  不再通过服务端全局状态转换锁串行化；
- `shard[0]` 对端生命周期 eventfd 已经注册进 Reactor epoll；Channel DOWN/回滚/
  发布只发送延迟所有者事件；
- 独立回收线程已经删除；所有者解除关联后对端槽位立即复用，旧
  Channel/服务端回收信息由已解除关联最终清理上下文持有；
- Endpoint 所有者引用转交最后引用清理器，已有工作线程引用作为旧对象生命周期
  隔离，不需要清理线程阻塞等待；
- 工作线程数量、RPC 调度算法和公开接口/线协议行为保持不变。

阶段 3 结束时服务端门面仍是：

```text
shard_count = 1
接收连接由 Reactor 所有者执行
对端生命周期由 Reactor 事件驱动
不存在独立接收/回收/最终清理线程
监听器/执行器/对端资源由分片拥有
对端表由 Reactor 单所有者管理
```

完成事件发布、监听器所有权、对端资源所有权、接收执行、对端生命周期事件、
所有者解除关联、最后引用清理以及对端表修改/快照均已经分片所有者化，
独立接收/回收线程和服务端全局对端状态转换锁都已经删除。剩余的
`finalizer_lock` 只属于退役统计/关闭控制面，不在热路径。阶段 3 的所有权边界至此收口。

## 阶段 4：多 Reactor 监听器

**状态：已完成**

已完成：

- `tr_runtime` 支持 N 个分片与每分片配置；
- 服务端公开配置暴露 `shard_count`，默认值为 1；
- 服务端全局的对端/工作线程/队列/缓冲区/监听积压保持总预算语义；
- 总预算通过确定性的“基础值 + 余数”拆分到各分片；
- 每个分片独立拥有 Reactor、对端表/事件源、RPC 执行器、
  RPC 消息资源池与重组资源池；
- `tr_server_listen()` 创建同地址同端口的 N 个 `SO_REUSEPORT` 监听器；
- 每个监听器只由所属 Reactor 接收连接，不做跨分片 fd 转移；
- 启动/排空/销毁遍历全部分片；
- 服务端统计聚合全部 Reactor、资源池、对端、Channel 与 RPC 数据；
- 不存在独立接收/回收线程，也不存在服务端全局热路径锁。

每个 Reactor：

```text
SO_REUSEPORT 监听器
自己的连接表
自己的对端状态
```

中央接收/回收流程与对端状态转换锁都已经删除；阶段 3 的单分片所有权边界
已经基本收口，可以进入阶段 4 的多分片启用。

## 阶段 5：Pipeline / 连接组

**状态：进行中**

已经完成连接组基础能力：

- 内部有界 `tr_pipeline` 易失状态对象；
- 一个 Pipeline 固定由一个 Reactor 所有者管理；
- CONTROL 与 DATA 连接成员关系进行所有者校验；
- CONTROL/DATA 物理连接成员关系唯一；
- DATA 槽位代次防止 ABA；
- 有界 DATA 轮询选择；
- 有界 Stream -> DATA 亲和关系；
- DATA 移除时自动使对应代次的 Stream 亲和关系失效；
- 所有可变成员关系/亲和关系修改都通过所有者 Reactor 串行化。

已经完成路由标识基础能力：

- 固定 48 字节 `TRR1` 路由前导信息；
- 小端编码 + CRC32C；
- `pipeline_id` / `epoch` / `owner_shard` / `role` / 成员索引 / 成员代次；
- CONTROL 与 DATA 的成员索引语义校验；
- 增量解析器支持任意 TCP 分片；
- 解析器精确停在前导信息边界，保留同一次读取中后续的 TRP1 字节；
- 完整但非法的前导信息直接终止连接，禁止对任意字节重新同步；
- `member_generation` 明确定义为 Pipeline 成员关系代次，不复用 Reactor 槽位代次。

已经完成注册表/能力基础能力：

- 有界分片本地 Pipeline 注册表；
- 注册表以 `pipeline_id` 为唯一键，旧 `epoch` 未注销时新 `epoch` 不能并存；
- Pipeline 标识增加 `owner_shard_id + epoch`；
- DATA 槽位状态拆分为 `FREE / RESERVED / ATTACHED`；
- CONTROL 控制面可以先预留 `(data_index, generation)`；
- 预留/附着都要求 CONTROL 处于活动状态；CONTROL 清除时撤销所有尚未附着的预留；
- 预留占用容量，但不参与 DATA 选择/Stream 亲和关系；
- 路由附着时再次校验角色/分片/`pipeline_id`/`epoch`/索引/代次/所有者 Reactor；
- 路由不匹配不会消耗预留；
- 只有精确匹配的附着才把 `RESERVED` 转成 `ATTACHED`；
- 注册表不拥有 Pipeline 生命周期；注销前强制 CONTROL/DATA/预留/亲和关系全部静默；
- 注册表不向所有者域外返回裸 Pipeline 指针，按标识操作必须在所有者内部完成查找与操作。

已经完成已接收 DATA 入口基础能力：

- Reactor 连接支持可选启用的固定长度前导信息门控；
- 门控在 TRP1 解析器之前读取且只读取精确前导长度，不会过读后续帧；
- Pipeline 入口使用真实已接收 fd 执行 TRR1 解析 + 注册表预留附着；
- 路由不匹配会使连接致命关闭，但不会消耗预留；
- 相同能力可以随后精确重试并成功附着；
- 附着后安装正常 TRP1 下游处理器；
- 已路由 DATA 连接发生 `CLOSED/ERROR` 时自动精确解除成员关系；
- 普通 RPC 接管路径不启用该门控，现有协议行为不变。

已经完成内部 CONTROL 控制面基础能力：

- `tr_pipeline_control` 负责 Pipeline 的创建/绑定/注册生命周期；
- CONTROL 预留 DATA 时生成可以直接编码为 TRR1 的 DATA 提供信息；
- 预留本身不代表 `READY`；
- `prepare_transfer(stream_id)` 只从 `ATTACHED` DATA 中选择，跳过 `RESERVED`；
- DATA 选择 + Stream 亲和关系在同一个所有者操作内原子完成；
- 成功返回内部 `TRANSFER_READY` 令牌 `(stream_id, data_index, generation)`；
- 重复的 Stream 准备被拒绝，Stream 生命周期保持单 DATA 亲和关系；
- CONTROL 关闭在已经附着的 DATA/Stream 未静默时拒绝；
- 关闭时自动撤销仍未附着的 `RESERVED` 能力，再注销/销毁 Pipeline。

已经完成内部 CONTROL 线协议基础能力：

- 固定 48 字节 `TRC1` 载荷，小端编码；
- `DATA_OFFER / DATA_CANCEL / TRANSFER_READY` 三种消息；
- 线协议标识固定包含 `owner_shard_id / pipeline_id / epoch /
  data_index / data_generation`，`TRANSFER_READY` 额外包含 `stream_id`；
- 线协议不暴露 Reactor 槽位/代次；
- `DATA_OFFER` 可以唯一转换为现有 TRR1 DATA 路由能力；
- 预留后如果 `DATA_OFFER` 编码失败，则精确回滚对应预留；
- `DATA_CANCEL` 解码后只取消完全匹配的 `RESERVED` 能力；
- `prepare_transfer -> TRANSFER_READY` 保持“先精确附着 DATA，再进入 READY”；
- `READY` 编码失败时回滚刚建立的 Stream 亲和关系。

已经完成分片本地 Pipeline 传输/控制面集成：

- 新增 `TR_FRAME_PIPELINE_CONTROL`，CONTROL 载荷使用 Reactor CONTROL TX 资源池；
- 一个内部 Pipeline 监听器固定属于一个 Reactor/分片所有者；
- 监听器自己拥有有界 Pipeline 注册表、连接/会话槽位与
  固定大小 CONTROL 消息资源池；
- CONTROL/DATA 共用同一个精确 TRR1 已接收套接字门控；
- CONTROL 路由必须先通过应用授权钩子，不能把远端路由信息本身当作授权；
- CONTROL 路由成功后创建/绑定/注册 Pipeline 并安装真实帧处理器；
- DATA 路由复用注册表精确预留附着，再进入普通 TRP1 解析器；
- 服务端所有者 API 可以通过真实套接字发送 `DATA_OFFER / TRANSFER_READY`；
- `DATA_CANCEL` 在 CONTROL 帧回调中精确校验并消费预留；
- CONTROL 断开时先把会话标记为 `CLOSING`，再原子失效成员关系/亲和关系；
- 组销毁时注销 Pipeline 后由所有者立即关闭 DATA 套接字；
- 监听器停止时同步关闭等待中/CONTROL/DATA 已接收套接字，不依赖后台线程；
- 真实回环测试覆盖：授权 -> OFFER -> DATA 附着 -> READY -> CANCEL ->
  CONTROL 故障组销毁 -> 相同 `pipeline_id` 使用新 `epoch` 复用。

下一步：

- 先完成层/模块/API 边界清理，禁止业务直接访问内部 Reactor 句柄；
- 把通用连接组/Pipeline 能力封装成不依赖 Reactor 的公开/高级传输能力；
- 分离应用 RPC API 与内部 Endpoint 引擎 API；
- 建立稳定公开头文件白名单；
- 只有真实性能分析/部署拓扑证明需要时，才增加跨分片 fd 转移。

当前仍不实现单个 Pipeline 跨 Reactor 的共享可变状态。

备份标识、检查点、提交、续传等持久化语义属于业务层，不再作为
c-trpc 核心阶段目标。

## 阶段 6：API 边界清理

**状态：进行中**

已完成：

- SDK 头文件从通配发布改为显式白名单；
- CI 精确校验安装头文件集合；
- 外部安装后消费程序只使用稳定 `tr/trpc.h` 门面；
- `rpc.h` 已经分离应用 Method/Call/Streaming 契约与内部 Endpoint 引擎；
- 已解除 `rpc.h -> channel.h -> reactor.h` 的公开依赖；
- buffer/channel/reactor/frame/wire 以及 queue/parser/socket/wire codec 辅助接口均退出安装 SDK；
- 详细 Reactor/Channel/Endpoint 诊断已经移入内部诊断接口；
- 保留的 RPC 消息使用固定不透明释放令牌，没有增加分配/复制；
- `tr_rpc_call_handle` 已经改为 16 字节不透明能力，不再公开 Endpoint 指针 + 槽位/代次，陈旧代次隔离语义保持不变；
- P3 第一阶段已经提供服务端通用连接组公开门面，复用内部 Pipeline 监听器，并以不透明 RX 所有权令牌保持 DATA 接收不增加额外载荷复制；
- P3 第二阶段已经提供客户端组 CONTROL 连接/关闭；CONTROL 路由标识保持内部实现；
- P3 第三阶段已经提供有界客户端 DATA 通道自动建立：`DATA_OFFER` 由内部组引擎消费，非阻塞连接使用 Reactor 辅助 fd 事件 + 定时器，不新增连接线程；服务端对失败路由只精确取消仍为 `RESERVED` 的代次；对端 `DATA_CANCEL` 对精确代次幂等，因此无需新增 `DATA_ATTACH_ACK`；
- P3 第四阶段已经提供客户端 `TRANSFER_READY` 消费与有界 Stream 亲和关系：`READY` 必须精确命中 `ACTIVE` DATA 代次，公开回调不暴露 DATA 路由标识，客户端释放与 DATA 丢失失效都在原 Reactor 所有者上完成；
- P3 第五阶段已经提供客户端逻辑 DATA 发送：调用期间借用的应用字节在所有者内复制到有界内部所有权，发送字节额度从 DATA/消息语义限制推导，Reactor 继续负责分片/TX，临时资源压力以 `TR_AGAIN` 反馈；
- P3 第六阶段已经完成稳定的优雅排空与组语义统计：排空与强制停止分离，客户端/服务端都能等待语义工作静默，稳定统计不暴露 Reactor/路由/队列/资源池内部诊断。
- 运行时收敛阶段进一步抽出通用的 Reactor 所有非阻塞连接器：Channel 自动重连与连接组 DATA 建立共用连接/超时所有权；Reactor 辅助 fd 从单槽升级为有界多源 + 代次隔离，因此同一客户端 Reactor 可以并行观察多个连接器，不再为 Channel 创建重连线程。
- 完成事件移交收敛阶段把工作线程满载路径从 `sched_yield()` 改为有界队列本地条件等待：Reactor 批量弹出释放容量后唤醒生产者；停止时关闭准入、广播等待者并推进准入代次，防止旧完成事件在 Reactor 重启后跨 `epoch` 提交。
- 命令移交收敛阶段进一步删除 Reactor 剩余 `sched_yield()`：异步 `SEND/RESUME` 继续以 `TR_AGAIN` 表达有界背压，只有同步 `CALL/QUIESCE/SET_HANDLER` 使用代次隔离的条件等待；`STOP` 关闭普通等待者准入后使用强制等待获得环形队列槽位，保持 FIFO 关闭屏障。
- 控制面锁收敛阶段开始拆分过渡锁职责：Channel 生命周期观察者与重连套接字策略改为 Reactor 所有者发布；RPC Endpoint 将强引用等待/已解除关联最终清理生命周期从 `endpoint->lock` 拆到独立 `ref_lock`，并统一所有强引用释放时的等待者唤醒。
- 客户端销毁/处理器收敛阶段把 RPC Endpoint 与 Channel 的销毁移动到 Runtime 停止之前，并将上层 Channel 处理器发布改为 Reactor 所有者屏障；完全停止后仅保留“仅用于销毁”的直接发布，因此回调发布不再占用 `channel->lock`。
- RPC 控制面收敛阶段把 Method 注册迁到 Reactor 所有者命令；客户端 Call 创建/启动已经确认原本就是所有者调用，因此应用线程不再直接写 Method 注册表或创建 Call 协议状态。
- RPC 流式一致性阶段冻结四种基数关系的 V1 生命周期：客户端请求半关闭、服务端最终 `STATUS`、`STATUS(OK)+ONE` 必须恰好一个响应、`FINISHED` 终止屏障、合法状态域，以及 MANY 请求 1..N 的当前 Method 打开限制；正常/取消/异常终止进一步共用一次性通知与完成统计语义。
- RPC 上下文/元数据阶段把元数据拆成有界的本地/对端初始与尾部四个范围：首个 `REQUEST/RESPONSE` 携带初始元数据，最终 `STATUS` 携带尾部元数据；新增所有者一致的 `tr_rpc_context` 快照，作为后续认证/链路追踪/指标拦截器的稳定输入边界，而不暴露 Endpoint/Stream/槽位。
- RPC V1 拦截器阶段增加客户端前置/服务端前置/服务端后置/客户端后置四个固定 Call 级阶段：钩子同步运行于 Reactor 所有者，调用时不持有 Endpoint 协议锁；服务端前置阶段可以返回 RPC 状态在处理器之前拒绝，其余阶段用于上下文/元数据/链路追踪/指标；钩子内对同一 Call 的发送/结束/取消等协议修改由运行时拒绝。

当前稳定安装头文件：

```text
trpc.h
client.h
server.h
rpc.h
rpc_codec.h
facade.h
status.h
transport.h
```

P4 第一、二、三阶段：

- 稳定 `tr_facade_limits` 已移除命令/TX/CONTROL-TX/RX/RPC 资源池/重组
  等 Reactor/资源池实现容量；
- 执行器工作线程数量/每 Endpoint 节点容量/流式续处理预留
  也已经迁入仓库内部调优，不再把当前线程池布局固化为 SDK ABI；
- 时间观测标志已经迁入仓库内部调优；
  `observability.h` 同时退出安装 SDK，Reactor/RPC 直方图/队列/资源池
  诊断明确保持内部实现；
- 新增仓库内部 `tr_facade_tuning` 与带调优参数的创建接口，供基准/
  架构诊断使用，不进入安装 SDK；
- 编码 RPC 消息存储仍作为最后一个过渡期稳定配置项，留待下一阶段收敛。

P4 第四阶段：

- 新增稳定 `tr_rpc_semantic_stats`；
- 客户端/服务端门面提供聚合 RPC 生命周期快照；
- 指标只包含已开始/已完成/进行中调用以及最终 RPC 状态分布；
- 服务端复用分片所有者 + 退役对端最终清理器汇总，不新增全局热路径原子变量；
- 每服务/方法拆分暂缓，后续必须采用分片本地记账。

P4 第五阶段：

- RPC 编码消息资源池改为有界按需所有权：槽位数量继续有界，
  每个槽位只在实际使用时增长，并在稳态复用容量；
- 增长发生在槽位独占后、资源池互斥锁之外，不把分配器延迟放进共享临界区；
- 稳定 `tr_facade_limits` 删除最后一个实现存储配置项
  `rpc_message_buffer_bytes`，RPC 大消息能力只由语义 `max_message_bytes`
  与 Method 限制约束；
- 客户端资源池为所有者本地，服务端资源池继续保持分片本地，不新增跨分片热路径共享状态。

P5 第一至第十九阶段：

- RPC 引擎已经迁到 `src/rpc/`；
- Runtime 编排器已经迁到 `src/runtime/`，只包含
  Runtime/RuntimeShard 生命周期、分片资源域、监听器/对端存储
  与共享执行器组合；
- 组语义核心已经迁到 `src/group/`：包含 Pipeline 成员关系/亲和关系
  与分片本地注册表；
- 组协议编解码也已经迁到 `src/group/`：TRR1 路由与 TRC1 CONTROL 线协议
  只负责标识/编码/校验，不拥有套接字、会话或 Reactor 可变状态；
- 组 CONTROL 生命周期协调器已经迁到 `src/group/`：负责创建/绑定/注册、
  DATA 预留/取消、传输亲和关系、优雅关闭/致命中止；致命中止通过
  Reactor 所有者能力关闭精确 DATA 连接，但不拥有监听器/TX/套接字会话；
- 组 DATA 入口与 CONTROL 传输适配器已经迁到 `src/transport/group/`：
  负责 Reactor 前导信息/处理器绑定、DATA 附着/解除、CONTROL TRP1 RX/TX；
- Pipeline 监听器已经迁到 `src/transport/group/`，明确作为服务端组传输
  端点；它拥有监听 fd、Reactor 注册、已接收连接/会话槽位、
  准入与 CONTROL 消息资源池，但通过回调与服务端门面解耦；
- 客户端组端点已经迁到 `src/transport/group/`：拥有 CONTROL 连接/接管、
  DATA 连接器/通道、`TRANSFER_READY` 亲和关系、有界 DATA 发送所有权；真正的
  `tr_client_connection_group_*` 门面包装仍保留在 `client.c`；
- TRP1 分帧实现已经迁到 `src/transport/protocol/`：
  `wire.c` 负责固定头部编解码/校验，`frame.c` 负责帧载荷所有权，
  `parser.c` 负责有界增量接收解析；
- TRP1 线协议/帧/解析器能力头文件已经与实现一起收敛到 `src/transport/protocol/`；
- Channel 状态引擎已经迁到 `src/transport/channel/`：Channel 自己拥有 HELLO/
  GOAWAY、Stream 状态/索引、消息顺序、流量控制、排空、保活与重连
  策略；连接器/套接字/Reactor 仍只作为执行能力被调用，不随 Channel 迁移；
- Channel 能力头文件已经与状态引擎一起收敛到 `src/transport/channel/`；
- Linux 套接字原语与 Reactor 所有非阻塞连接器已经迁到 `src/io/`：
  Socket 负责非阻塞/`cloexec`/监听/连接/接收/`TCP_NODELAY` 等 fd 原语，
  Connector 负责 Reactor 辅助 fd + 定时器驱动的有界连接/前导信息尝试；
- `TCP_NODELAY` 门面枚举的有效性/启用辅助函数已经从 Socket 内部契约移到
  `facade_policy_internal.h`，因此 I/O 原语不再反向依赖稳定门面类型；
- Reactor 实现与命令/完成/定时器队列已经迁到 `src/execution/`：
  Reactor 持有 epoll/eventfd、连接执行与队列/定时器生命周期；
  命令/完成队列继续提供有界跨执行上下文移交，定时器队列保持仅所有者访问；
- `command_queue.h` 已经从 `include/tr/` 下沉为执行层本地头文件；
- Reactor 能力/内部头文件已经与执行基础层一起收敛到 `src/execution/`；
- Buffer 实现已经迁到 `src/execution/`：固定/动态有界资源池、
  描述符所有权、获取/释放与资源池压力统计作为执行资源
  原语管理；客户端/服务端只通过执行层本地 `buffer_internal.h` 使用
  按需资源池初始化；
- 执行/传输引擎能力头文件已经回收到源码模块：
  `src/execution/{reactor.h,reactor_internal.h,buffer.h}`、
  `src/transport/protocol/{wire.h,frame.h,parser.h}` 与
  `src/transport/channel/channel.h`；生产代码不再依赖对应的
  `include/tr/` 引擎头文件；
- RPC 线协议头文件已经迁到 `src/rpc/rpc_wire.h`；客户端/服务端仅作为门面
  实现消费该内部线协议契约，稳定 `rpc.h/rpc_codec.h` 不依赖它；
- Socket 能力头文件已经从 `include/tr/socket.h` 回收到 `src/io/socket.h`；
  生产/测试代码统一通过 I/O 模块本地头文件使用 fd/TCP 原语，
  不改变套接字所有权、非阻塞策略、清理语义或调用路径；
- `crc32c.h` 与 `endian.h` 已经从 `include/tr/` 回收到 `src/` 根部，作为跨模块
  仓库内部共享工具；调用方只修改包含路径，不改变 CRC 后端、
  线协议字节序、内联编解码或热路径行为；
- `cleanup.h`、`guard.h` 与 `refcount.h` 已经从 `include/tr/` 回收到 `src/` 根部，
  作为仓库内部生命周期/并发原语；所有权转移、作用域解锁、
  强引用内存顺序与错误语义保持不变；
- `observability.h` 已经从 `include/tr/` 回收到 `src/observability.h`；固定直方图、队列/资源池
  快照与时间标志保持引擎诊断契约，稳定 `tr_rpc_semantic_stats`
  继续只由已安装的 `tr/rpc.h` 暴露；
- Reactor/命令/完成/定时器队列继续作为独立执行基础层，
  不因概念上的 Runtime 层而机械并入物理 Runtime 模块；
- `include/tr/` 物理目录只保留实际安装的稳定 SDK 头文件；所有内部头文件均位于 `src/`；
- 不改变所有权、线程、锁、执行器、资源上限、线协议或热路径。

下一阶段：

- P5 源码/头文件物理边界收敛已经完成：`include/tr/` 只保留实际安装的 8 个稳定 SDK 头文件，所有引擎/工具/诊断头文件均位于 `src/`；下一步进入阶段 7 的资源驱动流量控制；
- 每服务/方法语义观测暂不因目录重构顺带引入；
- 连接组 P3 能力进入维护/验证阶段，不再扩大路由/内部契约。

详细审查见 [分层、模块职责与 API 边界](07-layer-module-api-boundaries.md)。

## 阶段 7：资源驱动流量控制

**状态：进行中**

第一阶段已经完成连接组进行中工作语义预算：

- 服务端继续使用既有 `max_streams_per_group` 作为每组传输亲和关系上限；
- 客户端新增 `connection_groups.max_active_transfers`，不再把组传输并发
  隐式绑定到全局 `limits.max_streams`；
- 客户端值为 0 时继承 `limits.max_streams`，因此现有配置的默认行为不变；
- 直接复用现有内部 `max_transfers` 强制逻辑，不新增队列/线程/复制；
- 客户端语义统计新增 `active_transfer_limit`，与 `active_transfers` 配对；
- RPC `max_calls` 已经是 Call 生命周期语义上限，本阶段不再增加重复的
  `max_inflight_calls` 配置项。

第二阶段已经建立分片内存预算基础：

- RuntimeShard 持有线程安全的字节预算，提供预留/归还/当前值/峰值/拒绝证据；
- `limit_bytes == 0` 在迁移期表示只记账、不限制，不提前形成不完整的稳定契约；
- 运行时对端表作为第一个真实使用方：分配前先预留，所有失败/销毁路径精确归还；
- 预算拒绝返回 `TR_AGAIN`，与可恢复资源准入压力语义一致；
- 稳定 `memory_budget_bytes` 暂不开放：Reactor/Buffer/RPC/Channel 等主要分片本地
  使用方尚未全部接入，避免形成“配置了预算但仍能从旁路超出”的虚假保证。

第三阶段扩大分片内存记账覆盖：

- 内存预算原语从 `src/runtime/` 提升到 `src/memory_budget.h`，RuntimeShard
  仍是所有者，执行层/Buffer 只消费该能力，不形成反向模块依赖；
- Reactor 创建时一次性精确预留生命周期固定的用户态堆内存请求，任一构造失败
  或销毁都精确归还；内核 fd/epoll 内存不计入该字节契约；
- 固定 Buffer 资源池对描述符/存储分别预留，动态资源池对描述符 +
  实际 `realloc` 增长预留，保留容量在资源池销毁时归还；
- 服务端分片 RPC 消息/重组资源池接入所属 RuntimeShard 预算；
- 服务端销毁顺序调整为最终清理器静默 -> 分片资源池销毁 -> Runtime 销毁，
  防止预算使用方晚于所有者销毁；
- 客户端门面消息/重组资源池、RPC Endpoint/Channel 生命周期分配
  仍未覆盖，因此稳定 `memory_budget_bytes` 继续不开放。

后续：

- 将客户端门面资源池、RPC/Channel 生命周期分配接入同一个分片预算；
- 覆盖闭环后再公开稳定 `memory_budget_bytes`；
- 工作线程准入；
- 应用/后端准入钩子（优先复用 `SERVER_PRE_HANDLER`，而不是新增第二套钩子）；
- 自适应 DATA 并行度。

## 阶段 8：增加复杂度前先做性能分析

重点测量：

- Reactor CPU；
- 完成队列；
- 跨分片路由速率；
- 内存；
- 工作队列；
- 后端队列/准入；
- P99 延迟；
- 单个超大 Pipeline 与多个小 Pipeline 的对比。

只有出现单 Pipeline 单核瓶颈，再进入未来的 DataShard。

## 非目标

当前阶段不做：

- `SCM_RIGHTS`；
- Nginx 主/工作进程模型；
- io_uring 基线；
- reuseport eBPF 导流；
- 全面无锁化；
- 单 Pipeline 跨 Reactor 共享可变状态。
