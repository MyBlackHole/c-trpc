# TLS-C：普通 V2 Reactor 接线设计

**状态：** TLS-C 实施基线；沿用已批准 ADR-009 与 TLS-B 配置 ABI。

**日期：** 2026-10-23

**基线：** `75f76674b60dbe9637c12e8e3ab34d3497fb8435`，`2.0.0-dev.10`

**对应：** #234、#239、#241、#242、#243；TLS-D/E 仍未完成。

## 目标

让普通 V2 Client/Server 在 `tls.mode == TR_TLS_REQUIRED` 时，经现有 Reactor
连接上的 TLS 1.3 字节端点传输完整 TRP 字节流。TCP 连接或接受后先完成 TLS
认证；只有 `AUTHENTICATED` 之后才允许发送 V2 HELLO、读入 Parser、派发帧或
触发 Channel 就绪。默认 `TR_TLS_DISABLED` 的现有行为保持不变。

TLS-C 交付可由真实普通 V2 RPC 往返、跨方向 WANT、分片转发、RPC FINAL 顺序与
明文快速路径回归来证明。它不代表 TLS-D 的 mTLS、连接组 CONTROL/DATA 主体绑定、
凭证轮换或迟到凭证已经实现，也不满足 TLS-E 资源/公网暴露门槛。

## 方案选择

1. **在现有 `tr_connection` 中增加可选 TLS 字节端点（采用）：** 明文仍执行
   原有 `recv()` 与头/载荷 `sendmsg()`；TLS 连接改为端点读写，使用现存 TX 项和
   Parser 写入目标，不增加密文队列或载荷拼接缓冲。一个 TLS 会话只由所属 Reactor
   所有者推进。
2. **把明文也改走统一端点：** 代码路径较统一，但会改变明文 sendmsg/iovec 语义、
   调用次数与复制边界，无法把 TLS 行为和既有吞吐回归隔离验证。
3. **TLS 代理 fd/socketpair：** 将增加额外事件源、两向缓冲和关闭屏障，且会让
   OpenSSL 缓冲之外再复制一次；与现有连接槽所有权不符。

## 集成边界与状态顺序

新增 Reactor 所有者接管入口接收 TLS profile 和一条既有绝对握手期限。连接
创建独立 OpenSSL session 与 `TR_BYTE_ENDPOINT_TLS_REQUIRED` endpoint；在 epoll
注册前绑定事件源并执行一次非阻塞握手推进，以取得初始 WANT 兴趣。已有
`tr_reactor_adopt_fd()` 继续作为明文入口，所有当前调用点和低层 API 行为不变。

```text
TCP 已连接 / 已接受
  -> TLS NEGOTIATING（只允许握手，不读写 TRP）
  -> TLS AUTHENTICATED
  -> 普通 V2 HELLO / ACK
  -> RPC READY
  -> RPC FINAL / 应用层排空
  -> close_notify（有界）
  -> 事件解绑、端点与 session 回收、fd 关闭
```

TLS 握手沿用外层已有期限：Client 从 TCP connect 和 HELLO 共用的
`connect_timeout_ms` 单调绝对期限；Server 使用接受连接时建立的
`handshake_timeout_ms` 单调绝对期限。重复 WANT 不刷新期限。普通 V2 Server
在握手期间即占用已预留 peer 槽。任何 TLS 必需配置、握手、链或 SAN 验证失败
均关闭精确物理连接，不回退明文。

本阶段只开放普通 V2 `tr_client_create()` / `tr_server_create()`。Transport-only
连接组工厂在 TRR1 前导及 CONTROL/DATA 身份绑定尚未接入之前，对 TLS 必需模式
仍明确返回 `TR_ERR_UNSUPPORTED`；不得让未认证 TRR1 先行进入现有路由器。

## Reactor 收发规则

- `tr_connection_interest()` 在 TLS 协商期只加入 endpoint 当前 WANT 的 READ/WRITE
  兴趣及关闭观察；认证后将 endpoint 的握手/读/写/关闭等待，与应用 RX/TX
  背压兴趣求并集。`SSL_write_ex` 可 WANT_READ、`SSL_read_ex` 可 WANT_WRITE，
  因而 epoll 的方向不能由应用操作名称推断。
- epoll 事件被映射为端点 readiness，并按现有有界 RX/TX 调度处理。OpenSSL
  pending 明文仅在当前 Reactor 轮次的 RX 字节/dispatch 预算内继续推进；
  `SSL_has_pending()` 不作为可进展证明。预算耗尽时用既有 ready 队列续跑，
  不等待内核重复报告已在 OpenSSL 中缓存的字节。
- Parser 路径把稳定的 Parser 写入目标和容量传给 endpoint；遇到 WANT 时保持
  地址与容量一致。端点成功读到字节后仍经 `tr_parser_produce()`，TLS 不改变
  帧校验、缓冲池和帧回调所有权。
- TX 路径每次用现有头部/载荷 iovec 中一个连续片段调用 endpoint，不构造完整帧
  副本。WANT 期间保留 TX item、帧代次、片段地址和请求长度；只按成功返回的明文
  字节推进 `wire_pos`。只有完整帧结束后才释放或复用 TX item。明文路径保持
  原 `sendmsg()` 与多片 iovec 代码，不经 endpoint。
- TLS peer `close_notify` 作为有序 RX EOF 处理：不再接收应用字节，先让已排队
  的 V2 控制/终局 TX 到达端点，再完成本端有界 TLS 关闭。若仍有已派发但尚未
  产生 TX 的 RPC 工作，传输关闭会按既有连接关闭策略终止这些未完成流；不能把
  TLS `close_notify` 当作 V2 请求 `HALF_CLOSE`。普通客户端先等待 RPC FINAL 和
  Channel 排空，再发起本端 TLS 优雅关闭。TCP EOF 而未收到 `close_notify` 是截断
  错误，不能作为成功终局。
- 错误/取消路径立即 abort，不等待对端。关闭时先从 epoll 删除连接事件，再解绑
  endpoint，释放 OpenSSL session，最后关闭 BIO_NOCLOSE 所借用的 fd；任何失败
  接管回滚都执行同一单次所有权收尾。

TLS 读写仍只在 Reactor owner 上调用。一次 TLS 调用无明文进度或返回 WANT 后
结束当前操作等待；不得通过反复调用非阻塞 OpenSSL 函数忙等。所有续跑受现有
单轮预算约束。profile 在 Client/Server 创建时构造，在所有所属 Reactor/peer
停止与销毁完成后释放；每条物理连接创建新 session。

## 测试与验收

新集成目标 `test_rpc_v2_tls_reactor` 使用公开普通 V2 SDK、测试证书和真实 TCP，
覆盖：

1. 正确 TLS 1.3 身份下的真实 V2 ONE→ONE 请求/响应和 FINAL；请求、响应正文及
   尾部元数据按 RPC 顺序交付。
2. 透明 TCP 转发器对双向 TLS 字节按很小且变化的片段转发，验证拆分 ClientHello、
   证书/握手记录、DATA 和 FINAL 不依赖一次 recv 对应一个 TLS 记录。
3. 小 socket 缓冲下双向大消息及多条 DATA，触发非阻塞 WANT；持续使用原 TX
   item/地址/长度并完成传输。服务端收到完整 RPC FINAL 后才能观测干净关闭。
4. 明文端口发 TLS 必需配置时失败关闭；错误 CA/SAN 和 TLS 失败不得触发任何
   V2 方法或 READY 回调。TLS 1.2 不得协商。
5. 同一套公开 V2 正常往返在 `TR_TLS_DISABLED` 下通过，并保留明文 `sendmsg()`
   快速路径；测试记录发送系统调用/散布聚集路径未被 TLS 分支取代。

所有证书、代理状态、日志和构建产物都位于仓库的 `build/test-tmp/` 或现有
Xmake 输出目录；禁止使用 `/tmp`。TDD 顺序为：先加入 TLS-required 普通门面
往返测试并观察其在当前 `TR_ERR_UNSUPPORTED` 边界失败，再接入配置与 Reactor；
每个后续负例和分片场景单独证明其失败原因并在实现后通过。

TLS-C 通过条件：新增集成目标及完整既有功能测试在 GCC debug/release、Clang
release、ASan/UBSan、TSan 下通过；安装 SDK 不暴露 OpenSSL 内部类型；静态分析、
格式和真实普通 V2 TLS/明文场景均有实际输出。TLS-B OpenSSL 3.0 CI 基线继续由
堆叠 PR 的检查负责；本阶段本地 OpenSSL 较新版结果不能代替该证据。

## 依据和限制

- OpenSSL 3.0 [`SSL_write`](https://docs.openssl.org/3.0/man3/SSL_write/)：非阻塞
  WANT 重试须使用同一参数；本设计的 endpoint 对 TX item 稳定地址和长度。
- OpenSSL 3.0 [`SSL_get_error`](https://docs.openssl.org/3.0/man3/SSL_get_error/)：
  读写都可能交叉 WANT，WANT_WRITE 的待写操作应在同线程优先原样重试。
- OpenSSL 3.0 [`SSL_pending`](https://docs.openssl.org/3.0/man3/SSL_pending/)：仅计入
  已处理且可立即读出的明文；尚未处理的缓存记录不能作为持续忙循环的理由。
- 当前仅验证项目所列 Ubuntu 22.04/x86_64 SDK 环境；连接组 TLS、mTLS、主体绑定、
  证书轮换、异步凭证、TLS 全部内部内存硬计量及外部 TLS 实现互通继续属于
  TLS-D/E，不得据 TLS-C 单独宣布 #234 完成或可暴露公网。
