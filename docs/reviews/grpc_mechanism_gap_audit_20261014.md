# c-trpc 对标 gRPC 的机制差距审查

**审查基线：** `main@130835414f3e00cf97d49ca8af21d7a83c25146d`（2026-10-14）。
**审查性质：** 静态源码、稳定头文件与现有测试/架构契约的定向核对；
不是协议互通测试、正式安全审计或完整故障覆盖证明。

## 一、判定标准

- **已实现**：当前稳定接口或内部模块可确认已有对应机制；不表示全部 gRPC 行为兼容。
- **部分实现**：有基础能力，但 gRPC 所述高级策略或跨层语义尚未形成完整契约。
- **缺失**：未找到可确认的当前实现，且已列为可评估能力；不能仅靠关键字缺失断言。
- **不适用/暂缓**：与当前 TRP1、连接组或资源目标不匹配，不应直接照搬。

## 二、证据矩阵

| 机制 | 状态 | 当前 c-trpc 证据 | 仍需审查/决策 |
| --- | --- | --- | --- |
| 调用截止时间与传播 | 已实现（V1） | `include/tr/rpc.h` 截止时间选项、`docs/architecture/09-rpc-context-metadata.md` 的 `:timeout-ms` 相对传播、`tests/test_rpc_deadline_*.c` | 取消/断连/执行器积压的极端交错，非绝对时钟跨机器传递 |
| 取消与终止一次性 | 已实现（V1） | `docs/architecture/08-rpc-streaming-conformance.md` 的 `FINISHED` 终止屏障与四种基数关系，`src/rpc/rpc.c` | 已提交但未确认的副作用、取消后的迟到完成 |
| 自动重试安全性 | 部分实现 | `include/tr/client.h` 明确连接自动重连不透明重放 Call；Channel 控制发送及 RPC 最终关闭有内部局部重试 | **尚无统一逻辑调用/多尝试、提交点、幂等声明、重试预算和退避策略**；不把内部排队重试误称 RPC 自动重试 |
| 连接状态与 Wait-for-Ready | 部分实现 | `tr_client_wait_ready()`、`tests/test_channel_ready_deadline.c`、`tests/test_client_connect_deadline.c` | 就绪等待属于显式门面 API；未确认具有 gRPC 的每 Call Wait-for-Ready 排队与原始截止时间契约 |
| 服务健康检查 | 缺失（需评估） | 目前核对的稳定客户端/服务端 API、架构路线未提供独立健康协议 | 需区分 TCP/TLS/Channel 就绪、服务健康和方法级准入；不要混用连接存活 |
| 本地背压/资源预算 | 已实现（基础） | `TR_AGAIN`、`docs/architecture/08-rpc-streaming-conformance.md` 的 `RESOURCE_EXHAUSTED`、`tests/test_rpc_stream_backpressure.c`、`tests/test_rpc_overload.c` | 远端过载建议、可恢复性和重试提示未形成统一策略 |
| 远端过载/重试推迟 | 部分实现 | 已有标准 RPC 状态域及尾部元数据，`docs/architecture/10-rpc-interceptor-v1.md` 给出 `retry-after` 示例 | 示例不是正式协议；需版本化状态/延迟语义、限额与恶意对端约束 |
| Keepalive | 已实现 | `include/tr/client.h`、`include/tr/server.h` 的间隔/超时配置；`src/transport/channel/channel.c` 的 PING/PONG、超时统计 | 长空闲与高频保活的策略、连接组多通道协同 |
| 优雅排空/强制销毁 | 已实现（V1） | `tr_client_begin_drain()/wait_drained()`、`tr_server_drain()`、`tests/test_server_drain_deadline.c` | TLS 密文待发送/close_notify、连接组 DATA 进行中组合场景 |
| RPC 上下文/元数据 | 已实现（V1） | `docs/architecture/09-rpc-context-metadata.md`：四块有界元数据与上下文快照 | 跨服务追踪规范、可传播身份与敏感数据隔离 |
| 四阶段拦截器 | 已实现（V1） | `docs/architecture/10-rpc-interceptor-v1.md`、`include/tr/rpc.h` | 异步认证、复杂插件链不是当前 V1 目标 |
| TLS/mTLS | 缺失（已有正式要求） | `docs/architecture/11-compression-tls-requirements.md`、#234 | 非阻塞 TLS 1.3、证书校验、CONTROL/DATA 身份绑定与故障收敛 |
| 消息级压缩 | 缺失（已有正式要求） | `docs/architecture/11-compression-tls-requirements.md`、#235 | **归属 Channel/Stream 传输层**，默认 `none`、压缩前分片/重组后解压、双资源预算 |
| 服务发现与客户端负载均衡 | 缺失/暂缓 | 当前稳定门面主要提供显式连接与多分片服务端 `SO_REUSEPORT` | 服务端多分片不是跨服务节点发现；先明确 Resolver/Picker 边界，不急于做复杂算法 |
| 服务配置策略 | 部分实现 | 门面存在静态超时、保活、连接策略；Method/Call 也有配置 | 尚无统一的按服务/方法解析的重试、等待就绪、健康/负载均衡策略模型 |
| 语义指标 | 部分实现 | `tr_rpc_semantic_stats`、运行时内部诊断和 `docs/performance_readiness.md` | 方法级统计、尝试级统计、OpenTelemetry 映射和延迟直方图需成本证据 |
| HTTP/2、Protobuf 强绑定 | 不适用 | 当前使用 TRP1/TRR1、可替换编解码与连接组 | 不为追求 gRPC 形态替换现有线协议；互通如有业务需求另立网关 ADR |
| 请求对冲（Hedging） | 暂缓 | 当前没有通用 RPC 多尝试模型 | 在幂等性、取消、资源预算和最终结果一次性证明前禁止引入 |

## 三、P0：优先补齐的语义缺口

### G1：逻辑调用与物理尝试

目前客户端的自动重连明确不透明重放 Call，这是一项正确的安全约束。
在增加自动 RPC 重试前，必须先形成**独立逻辑 Call 与 Attempt** 模型：

- 逻辑调用拥有唯一截止时间、最终状态和应用回调；
- Attempt 拥有自己的连接/Stream 代次、发送准入和完成/取消结果；
- 不允许仅依据 TCP 发送成功/失败推断服务端是否执行；
- 需要精确的提交点、方法幂等声明、允许重试状态、指数退避与抖动、
  全局/每服务/每调用重试预算，防止重试风暴；
- 超时、取消、断连和服务端已经执行但响应丢失时，不得默认重放非幂等请求；
- 旧 Attempt 的迟到结果必须按代次释放资源，不能完成新 Attempt 的逻辑 Call；
- 在此模型未证明前继续保持**不透明重试关闭**，不要直接增加自动重试代码。

**交付**：单独 ADR + 故障矩阵；只有需求成立才实现。

### G2：连接就绪与服务健康

区分 `TCP_CONNECTED`、未来 `TLS_AUTHENTICATED`、
`CHANNEL_READY`、`SERVICE_SERVING` 和 `RPC_ADMISSION_ALLOWED`。
当前 `tr_client_wait_ready()` 不能直接等同 gRPC 的每 Call Wait-for-Ready。
需要验证连接组 CONTROL/DATA、健康探测、等待队列有界容量、原始截止时间、
停机排空和认证失败不被错误归类为“稍后可用”。

**交付**：先定义状态契约和不变量，确认业务需要后再引入健康服务/等待就绪 API。

### G3：远端过载与本地背压

`TR_AGAIN` 表示本地准入/资源压力；RPC `RESOURCE_EXHAUSTED`
表示应用可观察的远端调用结果，两者不能混为一谈。
如引入重试延迟建议，必须规定状态码、合法元数据、最长期限、
无效/恶意数值处理、重试预算和取消/截止时间优先级。
`retry-after` 当前仅是拦截器文档示例，**不是已经实现的线协议保证**。

**交付**：现有背压边界审查 + 可选协议/策略 ADR。

## 四、已有能力的回归审查，而非重写

1. 截止时间、取消、最终 `STATUS`、迟到完成与代次失效；
2. Reactor/Channel 背压、队列满载、停止与连接组重连；
3. 保活与自动重连、显式等待就绪和排空期限；
4. 拦截器同步非阻塞规则、元数据修改窗口、强引用收敛；
5. 多分片资源预算与公开 SDK 不透明边界。

对这些已实现项，只在发现可重现缺陷或缺少重要语义契约时创建实现任务。

## 五、已有独立任务与暂缓项

- TLS 1.3/mTLS：#234；压缩归属传输层：#235。
- HTTP/2 替换、Protobuf 强制依赖、每 RPC 对应 HTTP/2 Stream、
  Hedging、通用插件链和复杂客户端负载均衡**不作为本阶段实现目标**。
- 与 gRPC 比较是借鉴语义，不是宣称协议兼容；不得以文档审查替代
  精确 CI、跨进程互通、安全评估或真实性能测试。

## 六、下一阶段验收顺序

1. G1：先证明逻辑 Call/Attempt 语义边界，再讨论是否允许自动重试；
2. G2：梳理 TCP/TLS/Channel/Service 状态与有界等待就绪；
3. G3：审查本地背压与远端过载/重试提示；
4. 与 #234/#235 的安全传输和压缩组合验收；
5. 健康检查、服务配置和可观测性按需求与性能证据逐步扩展。

参考：gRPC 的重试、Wait-for-Ready、健康检查、压缩与服务配置机制，
但不要求与其 HTTP/2 线协议或公开 API 兼容。
