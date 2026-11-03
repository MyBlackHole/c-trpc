# TLS-B OpenSSL 后端实施计划

> **实施方式：** 在当前会话内按任务顺序执行 `superpowers:executing-plans`，每项先写失败测试、确认失败，再实现并验证；本工作不派发子 agent。

**目标：** 交付可由字节端点驱动的 OpenSSL 3.x TLS 1.3 单连接后端、冻结的公开 Client/Server TLS 配置，以及真实跨进程套接字认证证据。

**架构：** 为公开门面增加默认关闭的 TLS 配置；TLS-B 的公开工厂在 TLS 必需模式下明确拒绝，避免误报为已启用。内部 OpenSSL profile 创建并验证证书身份，逐连接 SSL 会话实现现有 `tr_byte_endpoint_driver`，而 Reactor 收发接线留给 TLS-C。

**技术栈：** ISO C11、Xmake 3.1.1、OpenSSL 3.0+、Linux 非阻塞 TCP、GCC/Clang。

**设计依据：** [TLS-B OpenSSL 单连接后端设计](../specs/2026-10-23-tls-b-openssl-backend-design.md)、[ADR-009](../../architecture/decisions/ADR-009-tls-byte-endpoint.md)、[#234](https://github.com/MyBlackHole/c-trpc/issues/234)。

## 全局约束

- OpenSSL 最低版本为 3.0；客户端与服务端的 TLS 最低、最高协议版本均为 TLS 1.3。
- TLS 必需模式不得明文回退；关闭 TLS 1.3 early data；DNS 与 IP 身份按各自 SAN 类型校验，DNS 不回退到 Common Name。
- 配置字符串仅在工厂/profile 创建期间借用；解析完成后不保存调用方路径指针。
- TLS-B 不把后端接入 Reactor，不宣称普通 V2 Client/Server 已启用 TLS；该接线属于 TLS-C。
- 新增的公开配置字段会改变结构体大小；本项目仍处于开发预览期，使用者需重新编译，版本由 `2.0.0-dev.9` 增至 `2.0.0-dev.10`。
- 构建入口仍为 Xmake；保留所有现有 CI 检查名称。静态 SDK 消费端显式链接 `-lssl -lcrypto`。
- 测试生成文件只写入工作区 `build/test-tmp/` 并由测试清理；运行测试前设置 `TMPDIR="$PWD/build/test-tmp" XMAKE_TMPDIR="$PWD/build/test-tmp" PYTHONDONTWRITEBYTECODE=1`，不使用 `/tmp`。

---

### 任务 1：公开 TLS 配置与早期拒绝边界

**文件：**
- 新建：`include/tr/tls.h`
- 修改：`include/tr/client.h`、`include/tr/server.h`、`src/client.c`、`src/server.c`
- 新建：`src/io/tls_config.h`、`src/io/tls_config.c`
- 新建：`tests/test_tls_config.c`
- 修改：`xmake.lua`、`.github/workflows/ci.yml`

**接口：**
- `include/tr/tls.h` 定义 `enum tr_tls_mode`、`enum tr_tls_client_auth_policy`、`struct tr_tls_client_config` 和 `struct tr_tls_server_config`，字段与设计 spec 中的 C 声明逐项一致。
- `tr_client_config` 与 `tr_server_config` 在尾部各增加一个 `tls` 成员；初始化器将其清零，保证 `TR_TLS_DISABLED` 是零值。
- 内部验证函数为 `int tr_tls_client_config_validate(const struct tr_tls_client_config *config, int backend_enabled)` 与 `int tr_tls_server_config_validate(const struct tr_tls_server_config *config, int backend_enabled)`。
- 本任务 `backend_enabled` 固定为 `0`：合法 TLS 必需配置由公开工厂返回 `TR_ERR_UNSUPPORTED`；TLS-B 的 profile 测试下一任务使用 `backend_enabled=1` 进行结构检查。

- [x] **步骤 1：先写公开配置和拒绝语义测试**

在 `tests/test_tls_config.c` 覆盖初始化为 disabled、disabled 带任一 TLS 字段返回 `TR_ERR_INVALID`、非法枚举返回 `TR_ERR_INVALID`、TLS 必需模式缺少客户端验证名或服务端证书/私钥返回 `TR_ERR_INVALID`、结构完整的客户端/服务端 TLS 必需配置返回 `TR_ERR_UNSUPPORTED`。另断言 TLS 必需模式调用 `tr_client_create()`、`tr_client_create_transport()`、`tr_server_create()` 和 `tr_server_create_transport()` 时返回 unsupported 且 `*out == NULL`，不建立连接或监听。

- [x] **步骤 2：注册并运行目标，确认失败原因**

在 `xmake.lua` 将 `src/io/tls_config.c` 加入 `trcore`，注册 `test_tls_config`，链接 `trcore` 并加入 `default` 测试。服务端 V2 工厂用例复用 `tests/test_rpc_v2_sdk.c` 中有效的方法表样例。运行：

```sh
mkdir -p build/test-tmp
TMPDIR="$PWD/build/test-tmp" XMAKE_TMPDIR="$PWD/build/test-tmp" \
    PYTHONDONTWRITEBYTECODE=1 xmake test -v -j1 'test_tls_config/*'
```

预期：因 TLS 公共类型、字段或验证入口尚不存在而编译失败。

- [x] **步骤 3：新增公开配置类型并接入默认初始化**

创建 `include/tr/tls.h`，声明 spec 中列出的模式、认证策略和配置字段；在 `include/tr/client.h`、`include/tr/server.h` 引入它，在 Client/Server 配置末尾添加 `struct tr_tls_client_config tls` 与 `struct tr_tls_server_config tls`，在两个初始化器中明确设置 `tls.mode = TR_TLS_DISABLED`、其他字段清零。

- [x] **步骤 4：实现纯结构校验和工厂拒绝**

在 `src/io/tls_config.c` 实现内部校验：disabled 模式拒绝任何非空附加字段；TLS 必需模式检查必需字段、空字符串和枚举；TLS-B 禁止客户端证书字段及服务端 optional/required client auth，返回 unsupported。先在 `tr_client_create_internal()` 与 `tr_server_create_internal()` 调用校验，再在有效 TLS 必需配置路径返回 unsupported，确保网络/Runtime 对象创建前 `*out` 仍为 NULL。

- [x] **步骤 5：安装公开头并验证 C/C++ 头文件消费**

在 `xmake.lua` 的显式 SDK 头白名单增加 `include/(tr/tls.h)`，在 CI 的 `expected-headers.txt` 加入 `tls.h`，并让安装消费端断言两个默认配置的 `tls.mode == TR_TLS_DISABLED`。运行 `test_tls_config` 和 `test_rpc_v2_sdk`。

预期：配置与早期拒绝测试通过；安装头文件清单差异只包含新增 `tls.h`。

### 任务 2：OpenSSL profile 与证书身份验证

**文件：**
- 新建：`src/io/tls_openssl.h`、`src/io/tls_openssl.c`
- 新建：`tests/tls_test_identity.h`、`tests/tls_test_identity.c`
- 修改：`tests/test_tls_config.c`、`xmake.lua`

**接口：**
- 内部不透明类型 `struct tr_tls_openssl_profile`。
- `int tr_tls_openssl_client_profile_create(const struct tr_tls_client_config *, struct tr_tls_openssl_profile **out)`。
- `int tr_tls_openssl_server_profile_create(const struct tr_tls_server_config *, struct tr_tls_openssl_profile **out)`。
- `void tr_tls_openssl_profile_release(struct tr_tls_openssl_profile *)`。
- Client profile 从 CA 文件/目录或系统信任库加载信任；Server profile 加载并匹配证书与私钥。profile 不保留配置路径。

- [x] **步骤 1：添加 profile 输入失败测试**

在 `tests/test_tls_config.c` 为缺失 CA/证书、空路径、无法读取或解析 CA 文件、证书与私钥不匹配、`client_auth != TR_TLS_CLIENT_AUTH_NONE`、任一 mTLS 客户端凭证字段分别增加断言；profile 创建失败时 `*out` 必须为 NULL。测试身份工具将生成 CA 与 serverAuth 证书并只写入 `build/test-tmp/`。对端证书的链、SAN、有效期与 EKU 验证发生在握手，完整正负例归入任务 4。

- [x] **步骤 2：配置 profile 测试编译与链接**

让 `test_tls_config` 直接编译 `src/io/tls_openssl.c`，链接 `ssl`、`crypto`，继续依赖 `trcore` 获取公共配置与状态实现；OpenSSL 生产链接传播等到任务 5 再接入 `trcore`。

- [x] **步骤 3：运行新测试确认 profile API 尚未实现**

```sh
mkdir -p build/test-tmp
TMPDIR="$PWD/build/test-tmp" XMAKE_TMPDIR="$PWD/build/test-tmp" \
    PYTHONDONTWRITEBYTECODE=1 xmake test -v -j1 'test_tls_config/*'
```

预期：编译/链接因 profile API 缺失而失败。

- [x] **步骤 4：实现 OpenSSL 版本与配置校验**

在 `tls_openssl.c` 使用 OpenSSL 3.0 API；创建 `SSL_CTX` 后将最小和最大协议版本都设为 `TLS1_3_VERSION`，Client profile 设为 `X509_PURPOSE_SSL_SERVER` 并关闭 Common Name 回退，Client 与 Server 都关闭 early data，将错误路径集中收敛至释放 `SSL_CTX` 并返回负状态码。

- [x] **步骤 5：实现 CA、证书与 SAN-only 身份策略**

Client 未显式配置 `ca_file` 与 `ca_directory` 时调用 `SSL_CTX_set_default_verify_paths()`；只要任一信任源显式设置，则只加载显式来源。Client 启用 peer 验证；DNS 名设置 `X509_CHECK_FLAG_NEVER_CHECK_SUBJECT | X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS` 并使用 host API，IP 字面量使用 IP 验证 API，SNI 仅对显式 DNS `sni_name` 调用设置。Server 加载 certificate/key 并调用 `SSL_CTX_check_private_key()`。profile 创建时同步读取完毕，不保存路径。

- [x] **步骤 6：补齐证书构造器与 profile 正负例**

测试证书工具生成 CA、serverAuth 服务端证书、无效 CA 文件和不匹配私钥；断言有效 Client/Server profile 创建成功、坏 CA 不能加载、证书私钥不匹配失败。对端证书的可信链、错误 DNS/IP SAN、Common Name-only、过期与错误 EKU 均通过任务 4 的真实握手结果验证。

- [x] **步骤 7：运行 profile 目标并整理所有权**

```sh
TMPDIR="$PWD/build/test-tmp" XMAKE_TMPDIR="$PWD/build/test-tmp" \
    PYTHONDONTWRITEBYTECODE=1 xmake test -v -j1 'test_tls_config/*'
```

预期：有效证书和身份条件通过，所有负例失败且无 profile/证书/私钥对象泄漏；测试退出时删除工作区生成文件。

### 任务 3：非阻塞 SSL 会话与字节端点驱动

**文件：**
- 修改：`src/io/tls_openssl.h`、`src/io/tls_openssl.c`
- 新建：`tests/test_tls_openssl.c`
- 修改：`xmake.lua`

**接口：**
- `int tr_tls_openssl_session_create(struct tr_tls_openssl_profile *, int fd, struct tr_tls_openssl_session **out)`。
- `const struct tr_byte_endpoint_driver *tr_tls_openssl_driver(void)`。
- session 通过 driver 的 `arg` 携带；driver release 释放 `SSL` 和 session，但不关闭借用 fd。`SSL_new()` 持有 profile context 所需引用，因此 profile 可在 session 创建后单独释放。

- [x] **步骤 1：为 driver 的 WANT 与缓冲约束写失败测试**

在 socketpair 上创建 client/server sessions 和 `tr_byte_endpoint`。测试非阻塞握手会分别产生 WANT_READ/WANT_WRITE，读写只有匹配方向就绪时推进；将同一写缓冲与长度重试后完成，替换缓冲则由 byte endpoint 拒绝；close_notify 只由 read 返回 `TR_ERR_CLOSED`；fd 在 endpoint release 后仍保持打开。

- [x] **步骤 2：注册 TLS OpenSSL 测试目标**

在 `xmake.lua` 新建 `test_tls_openssl` binary，加入 `tests/test_tls_openssl.c`、`tests/tls_test_identity.c` 与 `src/io/tls_openssl.c`，依赖 `trcore`，链接 `ssl`、`crypto`，启用 `default` 测试和 120 秒上限。

- [x] **步骤 3：运行目标确认 session/driver 尚未实现**

```sh
TMPDIR="$PWD/build/test-tmp" XMAKE_TMPDIR="$PWD/build/test-tmp" \
    PYTHONDONTWRITEBYTECODE=1 xmake test -v -j1 'test_tls_openssl/*'
```

预期：因 session/driver 接口未定义而失败。

- [x] **步骤 4：实现会话构造、fd 非拥有 BIO 和角色状态**

会话创建时调用 `SSL_new()`、`BIO_new_socket(fd, BIO_NOCLOSE)`，分别将 Client 设为 connect state、Server 设为 accept state，并启用已构造 profile 的验证策略；任何 BIO/SSL 失败都释放已分配对象且不关闭 fd。

- [x] **步骤 5：实现握手、读写 WANT 映射**

driver 每次只调用一次 SSL 操作；每次 SSL I/O 前清空当前线程的 OpenSSL error queue，返回后不调用其他 OpenSSL 函数，立即在同一线程用结果调用 `SSL_get_error()`。WANT_READ 和 WANT_WRITE 分别映射端点事件位；写遇到 WANT 时缓存/校验原始地址与长度，成功后清除；`SSL_ERROR_ZERO_RETURN` 映射为有序关闭，其余 EOF、SSL 或系统错误 fail closed。

- [x] **步骤 6：实现有界 close_notify 与 release**

`shutdown` 通过一次 `SSL_shutdown()` 推进：返回 1 表示完成，返回 0 按 socket BIO 等待对端关闭通知的方向返回 `TR_AGAIN`，负值按 `SSL_get_error()` 映射 WANT 或失败。driver 不循环关闭；endpoint 的绝对 shutdown deadline 仍负责超时退出。release 只释放 SSL/BIO/session。

- [x] **步骤 7：运行 socketpair driver 测试**

```sh
TMPDIR="$PWD/build/test-tmp" XMAKE_TMPDIR="$PWD/build/test-tmp" \
    PYTHONDONTWRITEBYTECODE=1 xmake test -v -j1 'test_tls_openssl/*'
```

预期：双端只在 TLS 1.3 握手完成后读写确定字节；方向交叉、稳定写缓冲、关闭期限、close_notify、突发 EOF、重复 release 和 fd 所有权断言全部通过。

### 任务 4：跨进程 TCP 认证与降级负例

**文件：**
- 修改：`tests/test_tls_openssl.c`、`tests/tls_test_identity.h`、`tests/tls_test_identity.c`
- 修改：`xmake.lua`

**接口：**
- 测试二进制以 `--tls-peer <role> <case> <port> <listener-fd> <cert-dir>` 作为子进程入口；父进程通过 `fork()`/`exec()` 启动独立 server/client 进程，使用回环地址与临时端口。
- 证书、密钥和子进程交换的标记文件只位于 `build/test-tmp/tls-b/`，每个 case 唯一命名并在父进程收尾时删除。

- [x] **步骤 1：先写跨进程成功与负例断言**

覆盖可信 CA + 正确 DNS SAN 成功、正确 IP SAN 成功、SNI 与期望验证名不同仍成功；未知 CA、错 DNS SAN、错 IP SAN、过期证书、仅含 clientAuth 的证书、TLS 1.2-only 对端、明文首字节、异常 EOF 均失败。失败用例断言服务端/客户端没有收到应用层测试标记字节。

- [x] **步骤 2：用 0-RTT 恢复测试建立失败基线**

```sh
mkdir -p build/test-tmp
TMPDIR="$PWD/build/test-tmp" XMAKE_TMPDIR="$PWD/build/test-tmp" \
    PYTHONDONTWRITEBYTECODE=1 xmake test -v -j1 'test_tls_openssl/*'
```

观察：跨进程真实会话票据恢复尝试建立了 0-RTT 负例。客户端在负例中显式提高本地票据 early-data 上限并发送应用标记；服务端必须先调用 `SSL_read_early_data()`，并在收到上限为零的服务端配置无法接受的早期字节时失败关闭。测试断言服务端没有交付应用标记，不要求越权尝试继续握手。该用例在真实 TCP 上运行，不以 socketpair 替代。

- [x] **步骤 3：实现 fork/exec 与端口分配**

父进程创建 loopback listener 并绑定端口 0，读取实际端口后分别启动 exec 子进程；listener fd 不设 `FD_CLOEXEC` 并作为参数传给 server child，client child 使用该端口主动连接。每个子进程在自己的 OpenSSL 上下文中初始化 TLS profile/session。父进程关闭自己的 listener 副本并在有界期限内 `waitpid()`；超时先终止再回收所有 child，exec 失败、信号退出或未收敛均使测试失败。

- [x] **步骤 4：实现认证、协议和降级负例**

客户端必须把握手结果作为应用读写的前置条件；TLS 1.2-only 对端、明文首字节、证书身份错误和未认证 EOF 均在任何标记字节交付前终止。服务端负例检查端点/SSL 错误并确认接收缓冲未写入应用标记。

- [x] **步骤 5：验证 0-RTT 票据恢复拒绝**

先执行一次完整握手并取得 session ticket，再在同一客户端/服务端进程各自保留 profile 的情况下新建 TCP 连接恢复该 session；测试客户端调用 OpenSSL 3.0 的 early-data API 尝试发送标记字节。服务端 profile 的最大 early data 为 0，测试断言客户端报告 early data 被拒绝、恢复连接未认证/不交付应用字节；会话票据文件位于测试工作区并由测试清理。

- [x] **步骤 6：运行跨进程场景并检查清理**

```sh
TMPDIR="$PWD/build/test-tmp" XMAKE_TMPDIR="$PWD/build/test-tmp" \
    PYTHONDONTWRITEBYTECODE=1 xmake test -v -j1 'test_tls_openssl/*'
find build/test-tmp/tls-b -maxdepth 1 -type f -print
```

预期：所有正负例按预期返回，测试结束目录无证书、私钥、票据或标记文件；失败退出路径也收割所有子进程。

### 任务 5：生产构建、SDK 安装、CI 与阶段文档

**文件：**
- 修改：`xmake.lua`、`.github/workflows/ci.yml`
- 修改：`README.md`、`docs/build.md`、`docs/architecture/decisions/ADR-009-tls-byte-endpoint.md`
- 修改：`docs/superpowers/specs/2026-10-23-tls-b-openssl-backend-design.md`

**接口：**
- `trcore` 编入 `src/io/tls_openssl.c` 与 `src/io/tls_config.c`，并公开传递 `ssl`、`crypto` 系统链接库。
- CI 安装 `libssl-dev`；已安装静态 SDK 的 C/C++ consumer 在 `-ltrcore` 后显式增加 `-lssl -lcrypto`。
- README、ADR 与 spec 明确标注 TLS-B 已交付、TLS-C/D/E 与 issue #234 未完成。

- [x] **步骤 1：将生产 OpenSSL 依赖接入 Xmake**

把 `src/io/tls_openssl.c` 加入 `trcore` 源文件表，并从 `test_tls_config` 与 `test_tls_openssl` 的直接源列表移除，两个目标改为只依赖 `trcore`；使用 `add_syslinks("ssl", "crypto", {public = true})`。OpenSSL 头文件只出现在内部源/头，不添加到 `include/tr` 公共头依赖。

- [x] **步骤 2：扩展安装 consumer 链接和依赖安装**

CI 编译 job 明确安装 `libssl-dev`；SDK consumer 的 GCC/Clang C11 和 G++/Clang++ C++11 链接命令增加 `-lssl -lcrypto`，并运行安装后的消费程序。保留当前 xmake、Make、头文件逐项检查和检查标识。

- [x] **步骤 3：更新 TLS-B 阶段状态和 SDK 文档**

在 README 与 `docs/build.md` 说明 OpenSSL 3.0 依赖、静态 SDK 链接顺序、TLS-B 仅提供内部后端且公共门面仍不启用 TLS；在 ADR/spec 更新实现状态与精确测试证据边界。不要把 TLS-A/B 描述为 #234 整体验收。

- [x] **步骤 4：递增开发版本并执行完整 GCC 测试**

将 `xmake.lua` 版本从 `2.0.0-dev.9` 改为 `2.0.0-dev.10`，运行：

```sh
mkdir -p build/test-tmp
xmake f -c -y -m release --toolchain=gcc
TMPDIR="$PWD/build/test-tmp" XMAKE_TMPDIR="$PWD/build/test-tmp" \
    PYTHONDONTWRITEBYTECODE=1 xmake test -v -j1
```

预期：全部既有与 TLS 测试通过，安装消费端只因静态 OpenSSL 链接参数变化而扩展。

- [x] **步骤 5：执行 Clang、ASan/UBSan、TSan 和格式门禁**

按 `docs/build.md` 使用 Xmake 分别切换到 Clang release、GCC ASan/UBSan、GCC TSan，运行 TLS 专项和完整注册测试。GCC/Clang release、ASan/UBSan、TSan 均已运行完整注册测试并通过 84/84。仓库内 clang-format 18.1.8 的改动行检查与格式门禁 22 项自测已通过；未调用会把临时目录写入 `/tmp` 的 `scripts/check_compile_commands.py`。

- [x] **步骤 6：审查最终差异与工作区产物**

运行 `git diff --check`、改动行格式检查、源码格式门禁自测、SDK 安装 consumer 和 `git status --short`；确认没有生成文件进入 Git、用户已有的 R16/R17 两个未跟踪计划仍未暂存，TLS 测试输出均位于 `build/`。

预期：本地验证证据完整；推送 PR 后仍须等待 Ubuntu 22.04 CI 的 GCC/Clang、消毒器、静态分析、安装和格式检查结果。TLS-B 合入后 #234 继续开放，TLS-C 才开始真实 Reactor/V2 接线。
