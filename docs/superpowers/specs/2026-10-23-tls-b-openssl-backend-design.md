# TLS-B：OpenSSL 单连接后端与公开配置设计

**状态：** TLS-B 本地实现和验证通过；Ubuntu 22.04/OpenSSL 3.0 CI、PR 与合入待完成；#234 保持开放。

**日期：** 2026-10-23

**基线：** `951ee1d5aec02b3c99b3d4b7fa3727cc6f83f334`，`2.0.0-dev.9`

**实现版本：** `2.0.0-dev.10`

**依据：** [ADR-009](../../architecture/decisions/ADR-009-tls-byte-endpoint.md)、
[#234](https://github.com/MyBlackHole/c-trpc/issues/234)。

## 目标

建立一个可由内部字节端点驱动的 OpenSSL 3.x TLS 1.3 后端，冻结
Client/Server 的高层 TLS 配置形状，并用独立进程上的真实套接字验证
服务端证书链、有效期、用途和 DNS/IP 身份校验。配置和凭证在创建阶段
解析，TLS 会话只借用 fd，WANT 与 `close_notify` 遵循字节端点合同。

TLS-B 不接入真实 Reactor 的收发路径。开发版的公开 Client/Server
工厂在 `TLS_REQUIRED` 时返回 `TR_ERR_UNSUPPORTED`，连接仍不会以明文
继续。真实 V2 握手、Parser/Channel 就绪顺序和 RPC 通信属于 TLS-C。

## 方案选择

1. **冻结公开配置并交付独立后端（采用）：** 现在固定高层配置结构，构造
   不可变 OpenSSL 上下文和每连接 TLS 会话，通过字节端点与跨进程测试验证；
   Reactor 接线独立在 TLS-C。阶段边界与 ADR-009 一致，并保证未接线时
   不会伪装成 TLS 可用。
2. **推迟公开配置到 TLS-C：** TLS-B 只测试内部 OpenSSL 后端。改动较少，
   但配置 ABI 在真实连接接入时才确定，违反 ADR-009 对 TLS-B 的冻结要求。
3. **TLS-B/C 同批接线：** 一次完成配置、后端和 Reactor。可立即端到端使用，
   但把证书、事件兴趣、Parser 顺序和 TX 生命周期合在一个更难定位故障的改动中，
   也偏离已批准的分阶段验收。

## 公开配置

新增 `include/tr/tls.h`，由 `tr/client.h` 和 `tr/server.h` 引用，并在
`tr_client_config`、`tr_server_config` 中各增加尾部 `tls` 子结构。
`tr_client_config_init()` / `tr_server_config_init()` 将模式初始化为
`TR_TLS_DISABLED`，其他 TLS 字段置空；现有零初始化默认行为不变。
这会扩大公开配置结构，调用者须重新编译；当前仍是开发版，TLS-B 在稳定
2.0.0 发布前冻结该布局。

```c
enum tr_tls_mode {
	TR_TLS_DISABLED = 0,
	TR_TLS_REQUIRED = 1
};

enum tr_tls_client_auth_policy {
	TR_TLS_CLIENT_AUTH_NONE = 0,
	TR_TLS_CLIENT_AUTH_OPTIONAL = 1,
	TR_TLS_CLIENT_AUTH_REQUIRED = 2
};

struct tr_tls_client_config {
	enum tr_tls_mode mode;
	const char *ca_file;
	const char *ca_directory;
	const char *expected_peer_name;
	const char *sni_name;
	const char *client_certificate_file;
	const char *client_private_key_file;
};

struct tr_tls_server_config {
	enum tr_tls_mode mode;
	const char *certificate_file;
	const char *private_key_file;
	const char *client_ca_file;
	enum tr_tls_client_auth_policy client_auth;
};
```

字段语义如下：

- Client 的 `expected_peer_name` 必填，接受 DNS 名或 IP 字面量；TLS 验证
  必须匹配此值。DNS 名用 A-label 表示，只匹配 dNSName SAN；IP 字面量只匹配
  iPAddress SAN，不接受 Common Name 回退。`sni_name` 是独立的可选 DNS 名，只控制 SNI，不改变证书
  验证目标。省略 SNI 时不从 IP 地址或验证名隐式推导。
- Client 的 `ca_file` 与 `ca_directory` 都为空时使用 OpenSSL 系统默认信任库；
  任意一个非空时只使用显式配置的信任源，不悄悄并入系统信任库。空字符串
  无效。服务端证书与私钥路径必须同时提供，并在创建时验证匹配。
- Server 的 `certificate_file` 与 `private_key_file` 在 TLS 必需模式下必填。
  `client_auth` 的 none/optional/required 枚举现纳入 ABI；TLS-B 只支持 none，
  optional/required 在 TLS-D 实现前明确返回 `TR_ERR_UNSUPPORTED`。none 时
  `client_ca_file` 必须为空。
- Client 端证书与私钥字段现纳入 ABI；TLS-B 不执行客户端证书认证，任一字段
  被设置都明确返回 `TR_ERR_UNSUPPORTED`，mTLS 留至 TLS-D。
- `disabled` 模式要求其他 TLS 字段均为空且 Server 的 `client_auth` 为 none；
  非空 TLS 字段不能被静默忽略。`required` 模式从不允许明文回退。
- 配置字符串由调用方借用到创建函数返回。真实上下文创建时同步读取证书、私钥、
  CA 和名称，解析后不保留调用方的路径指针。私钥内容、密码和 TLS 内部指针
  不进入公开结构或普通日志。

TLS-B 的内部测试入口使用相同配置结构创建 Client/Server TLS profile；这些
内部函数不安装到 SDK。公开 Client/Server 工厂遇到 `TR_TLS_REQUIRED` 时在
任何 TCP 或 RPC 准入之前返回 `TR_ERR_UNSUPPORTED`。这条显式拒绝边界在 TLS-C
完成真实 Reactor 接线后才变为可用。

## OpenSSL 后端与所有权

- 生产后端位于 `src/io/tls_openssl.c/.h`，使用 OpenSSL 3.0 已有 API 和非阻塞
  socket BIO。构造端设置 TLS 最低和最高版本均为 TLS 1.3，验证服务端用途、
  链、有效期与 DNS/IP 身份，并显式设置 SSL server 验证用途、关闭 early data。
  服务器只加载证书、私钥和
  TLS-B 支持的 none 客户端证书策略。
- 每个 profile 拥有不可变 `SSL_CTX`。每个物理连接有独立 `SSL` 和字节端点；
  `SSL_new` 保持上下文所需引用。每条 BIO 使用 `BIO_NOCLOSE`，TLS session
  的 release 只释放 `SSL`，不关闭由 Runtime/Reactor 拥有的 fd。
- OpenSSL 驱动实现已有 `tr_byte_endpoint_driver`：握手、读取、写入和关闭各自
  单次推进；`SSL_ERROR_WANT_READ/WRITE` 转为端点等待位，close_notify 映射为
  对端有序关闭，未认证 EOF、协议错误和证书失败均失败关闭。`WANT` 写重试
  保持原始操作、地址和长度。每次 SSL I/O 前清空当前线程错误队列，失败后
  立即在同一线程调用 `SSL_get_error()`，中间不调用其他 OpenSSL 函数。致命错误由
  abort 路径终止，不循环调用正常 shutdown。
- profile 与 `SSL` 只在所属测试驱动或未来 Reactor 所有者线程推进。证书文件读取
  与 `SSL_CTX` 构造同步发生在连接执行域创建期间，不在 Reactor 回调中读磁盘。
  TLS-D 再引入异步凭证、轮换和跨配置快照引用。
- 私钥输入大小、并发握手数和 OpenSSL 全部内部内存尚无可证明硬上界；TLS-E
  前不得声称 TLS 已计入 `memory_budget_bytes` 或进程 RSS 的硬预算。

## 构建与安装

`trcore` 的生产静态归档加入 OpenSSL 后端对象，并通过 Xmake 公共系统链接项
传播 `libssl`/`libcrypto`。目标平台为 Ubuntu 22.04 x86_64，CI 安装并验证
系统 OpenSSL 3.0 头文件、链接与运行时依赖。公开头文件不包含 OpenSSL 头文件、
类型或宏；手工链接静态 SDK 的命令明确列出 `-lssl -lcrypto`。

新增 `tr/tls.h` 进入安装头白名单与 C11/C++11 消费端检查。Make 入口沿用 Xmake
构建图；不重命名既有必需检查。Xmake、GCC、Clang、ASan/UBSan、TSan、安装消费端
和源码格式工作流均覆盖 TLS-B。

## 验证设计

测试证书和私钥由测试代码生成，文件只写到仓库 `build/test-tmp/` 并在退出时
清理；测试不回退到 `/tmp`。跨进程测试通过 `fork` 后 `exec` 同一测试程序，
使用 `127.0.0.1` 临时端口上的非阻塞 TCP；服务端与客户端进程分别创建 OpenSSL
上下文并驱动各自字节端点。

正例要求：可信 CA、正确 DNS SAN、正确 IP SAN、有效期与 serverAuth 用途通过；
TLS 1.3 双端可发送并接收确定字节，写操作经历 WANT 后保持原缓冲；SNI 值与
验证名可以不同；TLS 正常关闭后 fd 仍由调用方持有。

负例要求：未知 CA、错误 DNS/IP SAN、过期证书、缺少 serverAuth 用途、TLS 1.2
对端、明文首字节和 0-RTT 均失败关闭，且不会交付早期或未认证应用字节、不会回退
明文。0-RTT 场景先取得 TLS 会话票据，再由测试客户端显式尝试早期数据；测试服务端
在进入 `SSL_do_handshake()` 前先推进 `SSL_read_early_data()`；接收配置将 early data
上限设为零，任何成功读到的早期字节都失败关闭。测试先在完整握手中取得会话票据，
再由独立客户端进程尝试早期数据；服务端必须证明没有接受或交付早期应用标记，测试
不要求故意越权的尝试继续握手。仅检查配置值不作为唯一证据。

另验证证书/私钥不匹配、非法配置、构造失败回滚、端点取消/绝对期限、fd 非拥有
关闭、错误 close_notify 与重复 release。测试覆盖 OpenSSL 3.0 CI 基线；本机
OpenSSL 更新版本不能替代最低版本构建证据。

## 明确不在 TLS-B 的内容

- Reactor 的 epoll 兴趣更新、连接注册、客户端/服务器实际握手与 TRR1/TRP1 顺序；
- TLS 保护后的普通 V2 Client/Server 通信、Parser 的 TLS pending 前进队列；
- mTLS、CONTROL/DATA 身份绑定、重连、证书轮换和凭证提供器；
- OpenSSL 全内存/CPU 上界、连接洪峰、长稳和性能发布门槛。

TLS-B 完成后 #234 仍开放。只有 TLS-C/D/E 及 issue 中全部安全、资源、兼容和互通
验收均有真实证据，才可关闭 #234 或称 V2 TLS 已完成。

## TLS-B 本地实现与验证记录

`2.0.0-dev.10` 已将 TLS 配置校验和 OpenSSL 3.0 API 后端编入生产静态库，并通过
Xmake 公开链接项传递 `libssl`/`libcrypto`。Client/Server 公共配置默认关闭 TLS；
TLS 必需模式在工厂创建阶段明确拒绝，Reactor 尚未接入 TLS。

本机 OpenSSL 3.6.5 下，GCC 16.2.1 与 Clang 23.1.3 的完整构建/测试各通过 84/84；
GCC ASan/UBSan 与 TSan 的完整测试也各通过 84/84。安装 SDK 的 C11、C++11 消费端
已编译并运行。新增源码通过 clang-format 18.1.8 改动行检查，源码格式门禁自测
22 项通过。真实 TCP 跨进程测试覆盖证书链与用途、DNS/IP SAN、SNI 分离、错误身份、
过期/错误用途、TLS 1.2、明文、截断、WANT/稳定写缓冲、fd 所有权和 0-RTT 负例。

这些结果没有验证 Ubuntu 22.04 系统 OpenSSL 3.0 的实际构建与运行；PR CI 尚未执行。
也没有声称普通 V2 Client/Server 已启用 TLS、TLS 全量内存已纳入硬预算或 #234
整体验收通过。

## OpenSSL 3.0 接口依据

- [协议版本上下限](https://docs.openssl.org/3.0/man3/SSL_CTX_set_min_proto_version/)
- [TLS 1.3 early data 收发与拒绝语义](https://docs.openssl.org/3.0/man3/SSL_read_early_data/)
- [DNS 主机名验证](https://docs.openssl.org/3.0/man3/SSL_set1_host/)
- [SAN 主机名匹配标志](https://docs.openssl.org/3.0/man3/X509_VERIFY_PARAM_set_flags/)
- [IP SAN 验证参数](https://docs.openssl.org/3.0/man3/SSL_CTX_get0_param/)
- [非拥有 socket BIO](https://docs.openssl.org/3.0/man3/BIO_s_socket/)
- [非阻塞错误与 WANT 映射](https://docs.openssl.org/3.0/man3/SSL_get_error/)
- [非阻塞关闭状态](https://docs.openssl.org/3.0/man3/SSL_shutdown/)
- [Xmake 公共系统链接项](https://xmake.io/api/description/project-target.html#add_syslinks)
