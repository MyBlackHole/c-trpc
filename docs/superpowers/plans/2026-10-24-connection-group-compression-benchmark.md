# 连接组压缩基准实施计划

> **执行约束：**在当前 worktree 内执行，不使用后台子 agent。保留所有已有未提交更改。测试文件、日志、缓存和结果不得写入 `/tmp`。

**目标：**为公开连接组 DATA 增加可复现的 TLS × NONE/Zstandard 基准，记录逐条校验交付、延迟、吞吐、CPU、累计分配、RSS 和 TCP 字节。

**架构：**增加一个小型 C 基准程序，分别以服务端和客户端模式运行，只调用公开传输门面。Python 运行器在结果目录内生成 mTLS 身份，通过代理转发并统计 CONTROL 与 DATA TCP 字节，在多个客户端进程间同步预热和测量阶段，校验每条结果，并将 JSONL 与子进程日志写入指定目录。不设置绝对耗时门槛。

**技术栈：**C11、公开 c-trpc 传输门面、pthread、Xmake、Python 标准库、OpenSSL 命令行工具。

**设计依据：**`docs/superpowers/specs/2026-10-24-message-layer-compression-design.md`

## 全局约束

- 压缩保持显式启用；NONE 使用 `tr_client_connection_group_send()`，并保持既有行为。
- 仅通过 `tr_client_connection_group_send_with_compression()` 和真实 `tr_server_connection_group_listen()` 路由测试压缩。
- TLS 模式使用 TLS 1.3 双向证书认证；其他模式使用 loopback TCP。
- 只有逐条消息和应用正文校验全部成功，才计入成功数。
- TCP 字节统计的是 TCP 端点间的流字节，包含 TLS 记录和连接建立流量，不含 IP/TCP 头与重传。
- Xmake 临时路径、生成证书、结果和日志都留在当前 worktree 的 `build/` 下。
- 基准为可选工具，不作为 CI 耗时门槛。

## 文件范围

- 新增 `bench/bench_connection_group.c`：服务端/客户端模式、公开门面初始化、就绪门闩、small/可压缩/不可压缩/mixed 确定性负载、回调校验、延迟摘要、CPU/RSS/分配计数和有界退出。
- 新增 `bench/run_connection_group_compression_matrix.py`：证书生成、多连接 TCP 字节代理、进程协调、矩阵运行、JSONL 元数据/结果及失败清理。
- 新增 `bench/test_connection_group_compression_matrix.py`：测试 case 统计、结果契约拒绝、矩阵覆盖和 TCP 代理双向字节计数，不启动 c-trpc。
- 修改 `xmake.lua`：注册可选 `bench_connection_group` 二进制及分配器包装。
- 更新本计划和设计 spec：记录方法、环境、结果、限制和待完成验收。

## 任务与证据

### 任务 1：定义并测试结果契约

- [x] 编写 Python 单元测试，检查矩阵覆盖完整，并拒绝消息数不符、校验/错误/deadline 非零、指标非有限及分位数无效的结果。
- [x] 先运行新测试确认 runner 契约尚未实现时失败，再添加纯结果校验/摘要函数并运行到通过。
- [x] Python 测试共 8 项，全部通过：`python3 -m unittest discover -s bench -p 'test_connection_group_compression_matrix.py' -v`。

### 任务 2：建立公开 API 连接组基准程序

- [x] 新增 C 二进制，包含 `server` 和 `client` 模式；服务端等待每个 CONTROL 授权、发出 DATA offer、等待 DATA 授权并为 stream 1 发送 `TRANSFER_READY`；客户端等待就绪消息和 stdin 阶段令牌。
- [x] 服务端回调校验序号、时间戳、负载类别和确定性正文散列；区分预热/测量样本，汇总延迟分位数与准确完成/错误数。
- [x] 客户端在 stream 1 提交有界消息序列；仅在 `TR_AGAIN` 时以相同消息身份重试；记录重试、测量时间区间、排空结果和进程资源。
- [x] 注册可选 Xmake 目标，并使用 worktree 内临时目录构建：`xmake build -P . bench_connection_group`。
- [x] NONE/无 TLS 小消息与 Zstandard/mTLS bulk 冒烟均通过，身份校验、消息计数、散列、排空和销毁成功。

### 任务 3：增加 TLS 矩阵运行器与 TCP 字节统计

- [x] 在输出目录内生成 CA、带 `server.example.test` DNS SAN 的服务端证书和带 URI SAN 的客户端证书；TLS 矩阵要求客户端认证。
- [x] 增加 loopback TCP 转发器，同时支持客户端 CONTROL/DATA 套接字并加锁统计双向字节。
- [x] 编排一个或两个独立客户端；先等待预热完成，再取得测量字节计数基线，并在双客户端场景验证测量区间重叠。
- [x] 覆盖 TLS 关/开、NONE/Zstandard、small/bulk-compressible/bulk-incompressible/mixed；默认每种组合 3 次，每客户端测量 1000 条请求。
- [x] 以 JSONL 保存元数据与 case 结果，在同目录保存子进程日志；丢失、重复、损坏、非零退出均失败，并仅停止运行器启动的子进程。
- [x] Python 契约测试、4 组合小型冒烟和额外 1000 条/客户端容量冒烟均通过。

### 任务 4：记录证据和剩余限制

- [x] 完整矩阵通过：96/96 case、144,000 条测量消息全部精确接收和校验，无 route 错误或 deadline；双客户端测量区间重叠。
- [x] 从原始 JSONL 独立重算覆盖、消息计数、中位数、TCP 字节/消息、延迟顺序和资源字段，均通过；结果在 `build/connection-group-compression-matrix/final.jsonl`，日志在 `build/connection-group-compression-matrix/final-logs/`。
- [x] 设计 spec 已记录软硬件环境、负载分布、样本数、全部模式的性能表、累计分配开销和 TCP 统计边界。
- [x] 明确保留真实业务正文、生产网络、长期运行及发布阈值为未完成验收；本基准不代表 Message compression 或 V2 整体完成。

## 自检

- 范围是测量工具，不改变公开 API 或运行时行为。
- 单元测试验证 Python 结果契约和代理计数；端到端冒烟及完整矩阵覆盖 C 公开门面和 TLS × 压缩组合。
- 测量值作为证据，不作为固定通过/失败门槛。
- 基准服务端配置 1024 个压缩接收任务槽是为容纳本基准突发流量；该有限容量并非通用生产建议，容量预算见设计 spec。
