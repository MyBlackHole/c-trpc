# 连接组压缩接收容量实施计划

**目标：**让服务端能独立于活动 Stream 数量，配置同时执行/排队的压缩 DATA 解码任务数，并保持容量有界。

**架构：**新增公开连接组服务端配置，用于限制在途压缩消息数。该值驱动 MessageService 任务槽和内存预算；部分消息重组槽仍按活动传输路由数配置。无可用任务槽时，按有界路由错误处理；调用方须根据消息突发量和内存预算选择容量。

**技术栈：**C11、公开连接组门面、Runtime MessageService、Xmake 测试、Python 基准运行器。

**设计依据：**`docs/superpowers/specs/2026-10-24-message-layer-compression-design.md`

## 全局约束

- 压缩仍为显式选择，NONE 行为保持不变。
- 接收任务数量必须有明确有限上限，并计入现有每分片消息内存预算。
- 部分 DATA 消息仍按路由代次、stream ID 和 message ID 关联。
- 所有测试临时路径和生成结果均保留在当前 worktree 的 `build/` 下，不写入 `/tmp`。
- 不提交、不推送，也不使用后台子 agent。

## 文件范围

- 修改 `include/tr/transport.h`：公开压缩接收任务容量并说明零值/非零规则。
- 修改 `src/server.c`：校验容量，并将精确配置用于 MessageService 容量和预算计算。
- 修改 `src/transport/group/pipeline_listener.c` 及内部配置：允许任务槽数量独立于部分消息重组槽数量。
- 修改 `tests/test_connection_group_facade.c` 和 `tests/test_connection_group_tls.c`：配置容量并覆盖单 Stream 压缩突发。
- 修改 `bench/bench_connection_group.c`：采用与最大生成消息接近的编码/解码上限，并配置有限突发容量。
- 更新设计 spec 和本计划：记录容量语义、内存预算及测试证据。

## 任务与证据

### 任务 1：在门面测试中复现容量耗尽

- [x] 扩展公开连接组压缩测试：在同一 transfer 上连续提交多条压缩消息，不等待逐条确认，并要求全部消息以准确 ID 和正文交付。
- [x] 通过 worktree 本地 Xmake 临时路径运行 facade 测试，确认旧实现因任务槽被活动 Stream 槽限制而无法承接突发；回归用例随后通过。

### 任务 2：公开并校验有界接收容量

- [x] 在 `tr_connection_group_server_config` 增加 `max_inflight_compressed_messages`。未启用压缩接收时必须为零；启用 Zstandard 接收时必须大于零。
- [x] 拒绝超过归一化 Runtime MessageService 队列容量的配置。
- [x] 用配置值驱动 Runtime 任务容量、分派上限、Pipeline Listener 任务槽和 `tr_group_message_budget_required()`。
- [x] 部分消息重组槽继续由 `max_groups * max_streams_per_group` 决定；移除任务槽不得超过重组槽的错误限制。
- [x] 为现有连接组压缩 fixture 明确设置小容量。
- [x] facade 与 TLS 聚焦测试通过；突发回归和既有 TLS 压缩覆盖均通过。

### 任务 3：让基准覆盖有界突发

- [x] 基准服务端编码/解码上限设为足以容纳最大生成消息的有限值。
- [x] 设置有限接收任务容量，容纳单 Stream 突发而不按请求数无限预留资源。当前基准使用 1024 槽，对应归一化 Runtime 执行器队列上限。
- [x] NONE 与 Zstandard 在明文及 mTLS 下的冒烟通过；消息计数、正文散列、TCP 字节和分配计数均有效。
- [x] Python 结果契约测试通过；失败 case 会在 `build/` 下保留协议和 TCP 诊断。

### 任务 4：验证整体实现并记录限制

- [x] 最近一次完整 `xmake test -P . -j1`：88/88 通过，耗时 80.567 秒；Xmake 临时目录位于当前 worktree `build/`。
- [x] 连接组压缩矩阵 96/96 case 通过，144,000 条测量消息全部接收并校验；独立核对计数、延迟、分配字段与 TCP 字节通过。
- [x] 设计 spec 已记录公开容量语义、内存预算依据、饱和行为、基准配置和测量结果。
- [x] 保留真实业务负载、生产网络、长期运行和项目性能阈值为待验收项；不据此宣称 V2 整体完成。

## 容量边界

基准使用 128 KiB 编码/解码上限、1024 个接收任务槽。每个任务按解码消息、编码消息、Zstandard 工作区、任务开销及任务元数据计入预算，任务项理论预算约 1,409,548,288 字节（1.313 GiB）；此外还要预留活动传输重组槽、Reactor 和 peer 资源。因此该配置用于覆盖基准突发负载，不是生产建议。生产部署应结合实际并发量、消息上限和可用内存选择较小的明确值。
