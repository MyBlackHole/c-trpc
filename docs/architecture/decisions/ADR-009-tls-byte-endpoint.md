# ADR-009：在 Reactor 字节端点接入 TLS 1.3

- 状态：提议中，须通过下述原型与安全验收后接受
- 日期：2026-10-23
- 基线：`75d91a03b249ece2e83e6aded4a8d387d27ee1cf`，`2.0.0-dev.8`
- 对应：#234、#239、#241、#242、#243；前置需求见
  [压缩与 TLS 要求](../11-compression-tls-requirements.md)

## 问题与边界

目前 Reactor 直接对 fd 调用 `recv` 和 `sendmsg`。入站 TRR1 前导、TRP1
HELLO、DATA 与终止帧都走同一连接；CRC32C 不提供认证或机密性。
TLS 必须保护这些字节的全部，而不能只在 RPC 载荷外包一层加密。
这一决策定义接入点、配置语义、信任身份和上线顺序；提交 ADR 本身
不使现有 SDK 具备 TLS，也不改变当前明文配置。

## 决策

每个 Reactor 连接拥有一个内部字节端点，选择显式明文或 **TLS 必需**
模式。端点在连接所属 Reactor 线程上接收 epoll 事件并推进状态；
上层 Parser、TRR1 路由和 Channel 只见认证后交付的明文字节。
外部应用和 RPC 不取得 `SSL *`、BIO、fd、Reactor 槽位或密钥指针。
TLS 必需模式的任何配置、握手或验证失败都关闭该连接，不尝试明文。

首版后端候选为 OpenSSL 3.x 的非阻塞 socket BIO。选择理由是现有
TX 项已经持有稳定的帧头与载荷，Reactor 已以 LT epoll 管理 fd，
可在同一所有者内替换字节读写而不增加密文队列。memory BIO 是备选：
它便于明确管理密文输入/输出，但需增加两条有界缓冲、复制、队列
背压和关闭责任。**这个候选不是实现已验收的结论**：TLS-A 原型必须
用相同 socketpair、分片、EAGAIN、取消和关闭矩阵比较两者。
若 socket BIO 的稳定重试或库内缓存不能满足预算与前进证明，
改选 memory BIO 并记录新增复制及内存成本，不保留不成立的选择。
实现只使用 OpenSSL 3.0 已提供的接口，并在目标 Ubuntu 环境链接
系统维护的共享 `libssl`/`libcrypto`；TLS-B 的构建与安装消费端须验证
实际版本和运行时依赖。文末 3.5 版 API 文档用于说明接口语义，
不把 3.5 当作未经验证的发行依赖。

TLS 只允许 TLS 1.3；显式设置最低与最高协议版本，禁用 0-RTT
和隐式协议降级。服务端使用预先验证的证书/私钥及信任配置快照；
客户端启用证书链、有效期、服务器用途与 SAN DNS/IP 验证。
预期服务名与 SNI 是两个输入，发送 SNI 不能代替身份验证。
配置语义只包含 `disabled`、`required`，以及服务端 `none`、
`optional`、`required` 客户端证书策略；未实现的组合明确拒绝。
首次公开配置的字段布局需在 TLS-B 提交时单独冻结并验证安装消费端，
本 ADR 不预先占用 SDK 结构字段。

### 连接状态与线协议顺序

```text
TCP 已连接/已接受
  -> TLS_NEGOTIATING
  -> AUTHENTICATED
  -> TRR1 前导（连接组场景）
  -> TRP1 HELLO/ACK
  -> READY
  -> DRAINING/CLOSING
  -> DETACHED
  -> RELEASED
```

显式明文模式从 TCP 状态直接进入现有前导/HELLO 路径。
TLS 必需模式在 `AUTHENTICATED` 之前不得调用前导回调、Parser、
Channel 就绪回调或 RPC 准入。每个 CONTROL/DATA 物理连接独立
握手和认证；组内 DATA 除现有组代次/成员检查外，还须按授权策略
匹配 CONTROL 的已验证主体及信任域。同 CA 签发或自报 TRR1 标识
不足以证明同一主体。重连创建新 TLS 会话和身份结果，不自动重放
结果未知的非幂等 RPC。

### 非阻塞读写与资源所有权

一个连接的 `SSL` 状态只由其 Reactor 所有者推进。握手、读、写、
关闭的 `WANT_READ` / `WANT_WRITE` 更新为 fd 的 LT epoll 兴趣并集；
读操作可能需要可写事件，写操作也可能需要可读事件。没有明文或
密文进度时退出本轮，等待新事件，不能根据 `SSL_has_pending`
无限自旋。`SSL_pending` 的已处理明文在每轮字节/帧预算内续跑；
预算用尽后以有界就绪队列继续，不依赖内核再次报告可读。

TLS 发送沿用 TX 项的所有权：`SSL_write_ex` 在成功报告的明文字节数
内推进 `wire_pos`，直到此时才允许归还相应 TX 项；`WANT` 时保持
同一缓冲地址、长度和项代次，重试原操作。将 OpenSSL 已接受的
明文字节与真正写出的密文字节分开统计；任何一种都不等于远端
业务执行。TLS 模式不声称保留明文 `sendmsg` 的零复制或系统调用数。

握手使用从 TCP 接受/连接完成起算的单个 `CLOCK_MONOTONIC`
绝对期限，重复 WANT、TCP 分片或重连不重置本次期限。连接在
握手期间即占对端槽位及自管内存额度。TLS 证书配置解析和轮换
在非 Reactor 执行域完成，成功后原子发布不可变快照；已有连接
持有旧快照至最终回收。外部凭证获取需要 #243 的有界可取消操作，
不能阻塞 Reactor 回调。

自管的明文、密文、TX 稳定缓冲、任务和握手状态须接入 #241 预算；
OpenSSL 内部分配与内核 socket 缓冲不自动纳入当前逐 Server 原子
账本。实现须给出实际分配范围与可测上界；没有证据前不能把
`memory_budget_bytes` 或进程 RSS 宣称为包含 TLS 的硬上限。
并发握手数、证书输入大小和每轮 CPU 工作分别设限，不能用
墙钟期限冒充 CPU 硬配额。

### 失败与关闭

验证失败、明文输入、协议版本不符、TLS 完整性错误和截断都结束
精确物理连接；不得把未收到 `close_notify` 的中途 EOF 解释为
成功的 RPC 终局。正常排空尝试有界发送 `close_notify`；
致命 SSL 错误不机械继续正常 shutdown，销毁也不无限等待对端。
连接只有在事件源解绑、所有者回调静默、TX/Parser/前导引用
归还后才能释放 TLS 会话和配置快照。

## 原型与交付门槛

1. **TLS-A：** 定义内部字节端点和可替换状态驱动，以真实非阻塞
   socketpair 对比 socket/memory BIO 的交叉 WANT、短写、待处理
   明文、无进度、关闭和回滚。通过后锁定 BIO 选择与稳定写契约。
2. **TLS-B：** 单连接 TLS 1.3 与服务端身份验证。正确信任、未知
   CA、错误 SAN/IP、过期/用途不符、TLS 1.2、明文和 0-RTT 负例
   必须在真实跨进程路径拒绝。公开配置与构建/安装消费端在此冻结。
3. **TLS-C：** TLS 字节端点接入真实 TRP/普通 V2；任意拆包、
   双向 DATA、写缓冲稳定性、库内 pending、半关闭与 RPC FINAL
   顺序通过，明文路径的复制/分配回归另测。
4. **TLS-D：** mTLS、CONTROL/DATA 身份绑定、重连、证书轮换及
   迟到凭证取消。旧/新配置快照并存与失效回滚必须验证。
5. **TLS-E：** 分配和资源总账、慢握手/取消洪峰、长期负载、
   GCC/Clang、ASan/UBSan、TSan、静态分析及外部 TLS 实现互通。
   未通过这一门槛不关闭 #234，也不宣称可暴露于不可信公网。

本 ADR 若与 [较早需求文档](../11-compression-tls-requirements.md) 中
“压缩属于 Channel/Stream 内部算法”的描述冲突，以后续已接受的
独立 Message 层决策为准；TLS 始终位于完整 TCP 字节流与前导之间。

## 依据

- [OpenSSL `SSL_get_error`：交叉 WANT 与原操作重试](https://docs.openssl.org/3.5/man3/SSL_get_error/)
- [OpenSSL `SSL_write`：非阻塞写的参数与缓冲约束](https://docs.openssl.org/3.5/man3/SSL_write/)
- [OpenSSL `SSL_pending`：已处理与未处理数据](https://docs.openssl.org/3.5/man3/SSL_pending/)
- [OpenSSL 协议版本上下限](https://docs.openssl.org/3.5/man3/SSL_CTX_set_min_proto_version/)
- [#234 TLS 研究](https://github.com/MyBlackHole/c-trpc/blob/d89bd301556b566db6d79d55dc8a37ea9dee8b85/docs/design-studies/20261015/issue-234.md)
