# 性能预期与生产化判断（2026-09-26）

## 结论与证据等级

单 owner Reactor、有界队列、worker 分离与显式资源转移的架构可以演进到生产级。
当前源码和测试证据不足以把项目认定为通用生产就绪 RPC 库，更不能保证万兆吞吐。
单 Reactor 本身不是正确性缺陷；多 Reactor 也不是生产级的充分或必要条件。

以下区分源码事实、组件实测和待验证的推导。没有端到端 RPC QPS、TCP 吞吐、
混合 BULK/RPC P99、故障恢复时间或长稳运行结果，不将组件数字包装成这些指标。

## 已实测的校验成本

基线：main `1589de607c7784aa93834a9d8e37947a7743e4d1`；本轮没有修改 CRC 算法。
`src/crc32c.c` blob 为 `500b3ecd2335580f0ffa180476f9ed0e102d8916`，每字节 8 次
位迭代，没有当前源码中的硬件加速分派或查表实现。

新增可复现入口：

```sh
xmake f -c -y -m release --toolchain=gcc
xmake build bench_crc32c
xmake run bench_crc32c
```

本地使用 GCC 14.2.0 / Clang 17.0.0、`-O2`，独立编译 benchmark 与实际 CRC
翻译单元，不使用 LTO。Linux x86_64 共享执行容器，报告的 CPU 为 AMD EPYC
9V74，允许 CPU 0-4，cgroup CPU 配额为 4 核等价，没有绑定 CPU。不能据此
推断独占物理主机、跨 NUMA、其他编译器或用户部署硬件的性能。

每种长度测试 5 个样本，每样本累计 16 MiB，使用 CLOCK_MONOTONIC 墙钟计时；
表中为样本中位数，数据缓冲重复访问，属于缓存友好的单线程组件微基准。

| 每次输入 | GCC MiB/s | Clang MiB/s |
|---|---:|---:|
| 64 B | 103.159 | 212.111 |
| 4 KiB | 101.307 | 211.409 |
| 64 KiB | 101.397 | 209.758 |
| 1 MiB | 101.735 | 207.117 |

同一算法仅编译器不同就存在明显差异，因此不能把“C 实现”当成性能保证。
1 MiB 样本对应约 107 / 217 MB/s 的纯 CRC 处理能力；真实接收还有解析、复制、
系统调用及调度成本。这里不是声称这些数字是所有平台上的严格上限。

按本机微基准线性折算，一个 256 KiB payload 的单次 CRC 约 2.46 / 1.21 ms。
这只是校验 CPU 路径成本估计，不是测得的 RPC 延迟，也不是 P99。
Facade 默认 frame 上限为 256 KiB，并把 RX/TX 整轮预算设为 4 倍 frame 上限，
即默认各 1 MiB；直接创建低层 Reactor 的零值默认预算则为各 4 MiB，二者不能混淆。
帧 CRC/解析与 callback 不能被字节预算抢占，所以已有公平性不意味着硬实时。

## 架构性能预期

- 大 payload：当前逐位 CRC 很可能先于内存带宽和万兆网卡饱和。1 Gb/s 原始
  线速是 125 MB/s，10 Gb/s 是 1250 MB/s，均还未扣除协议开销。本机 GCC
  单线程 CRC 已低于前者；Clang 结果也远低于万兆需求。必须实测端到端，
  不能宣称当前必定跑满千兆或万兆。
- 短 RPC：成本更可能由系统调用、跨线程队列、同步 owner-call、业务 handler
  和可变表查询决定。尚无实际 QPS/P99 测量，不给缺乏依据的固定范围。
- 混合负载：整轮预算限制独占工作量；本轮 header-only 控制优先级改善保活/
  credit 消息的发送机会，但不会提升 CONTROL lane 上业务 DATA 的优先级。
- 多连接：当前一个 Server 的协议 I/O 仍由一个 Reactor 驱动。worker 可以并发
  执行业务，却不能自动把这一 Reactor 的接收 CRC/协议处理变成多核并行。
- 内存：默认 facade 三个共享 payload pool 的 storage 为 RX 64×256 KiB、
  RPC 64×256 KiB、reassembly 8×4 MiB，共约 64 MiB/实例，不含结构体、线程栈、
  socket buffer 与任务元数据。Server 共享这些池，不是每个 peer 各 64 MiB；
  多 Client 实例仍会重复分配。固定容量有利于上界，但还需全局/租户级容量规划。

发送首帧的 CRC 在提交路径准备，后续分帧的 CRC 在 owner 上准备；接收 CRC
在 Reactor 上验证。不能简单地把所有 TX 校验成本都归到 Reactor，也不能因为
增加 producer/worker 就假定接收瓶颈消失。

TCP 有序字节流的限制参见 RFC 9293 §2.2：
https://www.rfc-editor.org/rfc/rfc9293.html#section-2.2
网卡/内核多队列并行与用户态 owner 并行不是同一层，参见 Linux networking scaling：
https://docs.kernel.org/networking/scaling.html

## 生产化门槛（建议验收要求，非已完成项目）

| 方面 | 当前基础 | 投产前仍需验证/实现 |
|---|---|---|
| 协议正确性 | CRC、分帧、generation、队列所有权与确定性回归 | parser/Channel/RPC fuzz、畸形/截断/超长输入、状态机与边界审计 |
| 生命周期 | STOP Completion drain、quiesce、部分启动失败测试 | 并发 stop/destroy、owner callback 调用约束、异常 poll/wake、超时和销毁竞态契约 |
| 背压与隔离 | 有界 buffer/command/completion/task 资源 | 全局与每 peer admission、超限策略、慢读慢写、恶意连接和公平性验证 |
| 安全 | checksum 检测意外损坏，不是认证机制 | 明确认证/授权、传输保护与密钥策略；raw TCP fd 不能被当成已经集成 TLS |
| 性能 | 工作预算和 owner 统计，新增 CRC 微基准 | TCP/多进程真实 RPC 与 BULK 基准、负载拐点、P99、CPU/RSS、CRC 优化与回归 |
| 故障与运维 | 基本重连、deadline、诊断统计与 CI | 丢包/断网/半开/重连风暴、FD/内存耗尽、长稳、指标告警、版本兼容与回滚 |
| 业务语义 | Transport/RPC 的收发与完成 | 备份落盘、durable ACK、幂等与重放由上层实现，不能把 RPC 成功当持久化提交 |

不把删除所有锁或增加 Reactor 数量当作验收标准。建议先把受控、可信网络中的
小规模试点与“面向不可信网络、稳定 SLA 的通用生产部署”分开。前者仍需应用
回滚与资源限制；后者在上述安全、故障及负载验证完成前不应承诺。

建议下一轮优先 CRC32C 优化（可移植 fallback + 能力检测 + 等价性测试）及真实
RPC/BULK 基准，再按 profile 决定 owner 控制面与 Runtime shard 的优化顺序。
不在本轮同时更改 CRC 算法、多 Reactor 或备份恢复协议。
