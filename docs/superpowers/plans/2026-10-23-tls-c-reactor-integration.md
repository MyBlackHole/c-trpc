# TLS-C Reactor 接线实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让普通 V2 Client/Server 在 TLS-required 模式下通过 Reactor 字节端点完成认证后的 TRP/RPC 通信。

**Architecture:** 每条 `tr_connection` 可选拥有一个字节端点和 OpenSSL session；TLS 握手与 TLS I/O 只在 Reactor owner 上驱动，认证前禁止 Parser、帧回调及 HELLO TX。明文连接继续走原 `recv()` / `sendmsg()` 快速路径。Client/Server 创建并拥有不可变 TLS profile，生命周期长于所连接 Reactor。

**Tech Stack:** C11、Xmake、Linux epoll LT、`tr_byte_endpoint`、OpenSSL 3 TLS 1.3、普通 V2 Client/Server SDK。

**Spec:** `docs/superpowers/specs/2026-10-23-tls-c-reactor-integration-design.md`

**状态：** TLS-C 本地实现与验证已完成；候选版本为 `2.0.0-dev.11`，GitHub 固定版本 CI、PR 与合并验收待完成。公网/资源门槛以及 TLS-D/E 不在本阶段完成范围内。

## Global Constraints

- 仅读写 TLS 1.3，TLS 必需模式的错误/验证失败关闭连接且不回退明文。
- Client TLS 握手与 V2 HELLO 共用 TCP connect 开始时的 `CLOCK_MONOTONIC` 绝对期限；Server 沿用接受连接时的 `handshake_timeout_ms` 绝对期限。
- 只让普通 V2 工厂开放 TLS；`transport-only` 连接组工厂在 TLS-D 身份绑定前，对 `TR_TLS_REQUIRED` 返回 `TR_ERR_UNSUPPORTED`。
- TLS 发送的 WANT 重试保持 TX item 代次、片段地址、片段长度；只有成功返回字节才推进 `wire_pos`。
- 明文发送维持当前 `sendmsg()` + 多 iovec 实现和调度语义。
- 每轮 TLS 握手/收发服从现有 Reactor IO quantum、ready dispatch 和 parser/RX 背压限制；不忙等 OpenSSL WANT。
- 所有测试身份、日志、代理状态、Xmake 输出放在仓库 `build/test-tmp/` 或现有构建目录；不得访问 `/tmp`。
- 不把 TLS-C 结果称为 #234 或完整 V2 完成；mTLS、CONTROL/DATA 身份绑定及 TLS-E 仍是未完成门槛。

---

## 文件与职责

- `tests/test_rpc_v2_tls_reactor.c`：公开普通 V2 SDK 的真实 TLS/RPC 集成、分片转发、认证失败及明文快速路径回归。
- `xmake.lua`：新增单个 TLS Reactor 集成测试目标，沿用完整功能测试集入口。
- `src/execution/reactor.h`、`src/execution/reactor.c`：TLS owner 接管 API、连接拥有的 endpoint 生命周期、认证门禁、WANT 到 epoll 的事件闭环、TLS RX/TX。
- `src/client.c`：普通 V2 Client profile 构造/回收、把原连接期限传给 TLS adopt；拒绝 transport-only TLS。
- `src/server.c`：普通 V2 Server profile 构造/回收、把 peer 绝对期限传给 TLS adopt；拒绝 transport-only TLS。
- `docs/superpowers/specs/2026-10-23-tls-c-reactor-integration-design.md`：状态顺序、接口边界、测试验收和限制。
- `docs/superpowers/plans/2026-10-23-tls-c-reactor-integration.md`：本执行清单和验证命令。
- `README.md`：TLS-C 合入后更新当前开发版 TLS 能力与限制。
- `docs/reviews/rpc_v2_tls_c_20261023.md`：最终记录实际证据、未覆盖边界和本地 OpenSSL 版本限制。

## 测试环境命令

每个测试命令都设置项目内临时目录，不让 Xmake 或测试代码写入 `/tmp`：

```bash
mkdir -p build/test-tmp
TMPDIR="$PWD/build/test-tmp" XMAKE_TMPDIR="$PWD/build/test-tmp" PYTHONDONTWRITEBYTECODE=1 xmake f -c -y -m release --toolchain=gcc --crc32c_portable=n
TMPDIR="$PWD/build/test-tmp" XMAKE_TMPDIR="$PWD/build/test-tmp" PYTHONDONTWRITEBYTECODE=1 xmake build test_rpc_v2_tls_reactor
TMPDIR="$PWD/build/test-tmp" XMAKE_TMPDIR="$PWD/build/test-tmp" PYTHONDONTWRITEBYTECODE=1 xmake run test_rpc_v2_tls_reactor
```

完整门禁按 CI 的入口执行：先分别 `xmake f -c -y -m <mode> --toolchain=<cc> --crc32c_portable=n`，再 `xmake build --all -v` 和 `xmake test -v -j1`。模式为 GCC debug、GCC release、Clang release、GCC ASan/UBSan 和 GCC TSan；不把完整矩阵作为每个小步骤的重复验证。

### Task 1: 先写真实 V2 TLS 集成矩阵并确认 RED

**Files:**
- Create: `tests/test_rpc_v2_tls_reactor.c`
- Modify: `xmake.lua`
- Reuse: `tests/tls_test_identity.c`, `tests/tls_test_identity.h`

**Interfaces:**
- 测试只用 `tr/trpc.h` 中公开的 Client/Server V2 门面和 `tr_tls_*_config`；证书由 `tls_test_identity_init()` 建立于 `build/test-tmp`。
- 正例走 `tr_server_listen(..., 0, &port)`、`tr_server_start()`、`tr_client_connect()`、`tr_client_unary_call()`，校验请求处理、响应正文和唯一 OK FINAL。
- 同一测试目标还预先包含错误 CA、错误 SAN、明文连 TLS listener、1–7 字节透明转发、关闭顺序与 disabled 模式 `sendmsg()` 路径检查；各子场景可单独运行，避免首个断言遮住其余红灯。

- [x] **Step 1: 写真实 V2 TLS、失败关闭、分片和明文快速路径测试**

构造含 serverAuth、DNS SAN `localhost` 的测试身份；设置 Server `certificate_file`、`private_key_file` 和 Client CA/`expected_peer_name = "localhost"`。测试公开普通 V2 的 TLS RPC 往返和唯一 FINAL；错误 CA/SAN 与明文输入不得调用业务 work/terminal；透明本地代理逐方向拆分 TLS 字节；disabled 配置完成同样的 V2 往返并计数 `sendmsg()` 调用。每项用独立子测试选择参数。

- [x] **Step 2: 注册测试目标并验证 RED**

在 `xmake.lua` 注册 `test_rpc_v2_tls_reactor`，源文件包含 `tests/test_rpc_v2_tls_reactor.c` 与 `tests/tls_test_identity.c`，依赖 `trcore`，启用断言且 `run_timeout = 120000`。先独立运行 TLS-required RPC 用例，确认失败原因是普通 V2 TLS-required 工厂当前返回 `TR_ERR_UNSUPPORTED`，不是证书生成或测试构建错误；再对 disabled 用例运行一次，确认既有明文基线在生产代码改动前通过。

### Task 2: 接入 Reactor TLS connection 所有权与认证门禁

**Files:**
- Modify: `src/execution/reactor.h`
- Modify: `src/execution/reactor.c`

**Interfaces:**
- 新增 owner-only `tr_reactor_adopt_tls_fd_on_owner(reactor, fd, profile, handshake_deadline_ns, out)`，其成功时接管 fd，失败时不接管 fd。
- `struct tr_connection` 只为 TLS 实例持有 endpoint；endpoint driver arg 拥有该连接的 OpenSSL session。旧 `tr_reactor_adopt_fd()` 不变。
- endpoint 在 epoll 注册前执行首个握手 step；只有 endpoint 为 `AUTHENTICATED` 后才允许运行 RX Parser 或 TX 队列。

- [x] **Step 1: 增加连接内 endpoint 字段和 TLS adopt 声明**

在 `reactor.h` 前置声明 `struct tr_tls_openssl_profile` 并声明 owner-only adopt。`reactor.c` 引入 `byte_endpoint.h` 和 `tls_openssl.h`；连接对象新增可空 endpoint 指针、TLS readiness 累积位及关闭阶段状态。

- [x] **Step 2: 实现 owner TLS 接管与单一路径回滚**

TLS adopt 分配 endpoint、创建 session、`tr_byte_endpoint_init(TR_BYTE_ENDPOINT_TLS_REQUIRED, ...)`、绑定事件源、调用一次 `tr_byte_endpoint_step(..., 0, now_ns)`，然后按当前 WANT 注册 epoll。任一步失败都解绑/abort/release 已创建 endpoint，并保留 fd 给调用方关闭。

- [x] **Step 3: 实现连接关闭中的 endpoint 回收**

在 `tr_connection_close_internal()` 移除 epoll 事件后 abort endpoint、event-unbind、release、free，随后关闭 fd；确保连接回调最多一次。对普通连接，endpoint 分支不执行，原有关闭行为保持。

- [x] **Step 4: 运行 Reactor 生命周期目标与 TLS 测试构建**

运行 `test_reactor_source_detach`、`test_reactor_tokens`、`test_byte_endpoint` 和 `test_rpc_v2_tls_reactor`。此时端到端 TLS 测试仍可在公开工厂处失败；不能为推进暂时开放一个未接线的成功配置。

### Task 3: 实现 TLS 握手兴趣、RX/TX、跨方向 WANT 与预算续跑

**Files:**
- Modify: `src/execution/reactor.c`
- Modify: `tests/test_rpc_v2_tls_reactor.c`

**Interfaces:**
- 连接 readiness 将 `EPOLLIN`/`EPOLLOUT` 映射为 `TR_BYTE_ENDPOINT_EVENT_READ`/`WRITE`；握手、read、write 各自只在对应 ready 到达时重试。
- TLS RX 继续使用 `tr_parser_prepare()` 提供的同一写入地址和容量；TLS TX 对现有 iovec 片段调用端点写入，WANT 时使用 endpoint 保存的原地址和长度。
- 明文仍用原 `recv()` 和 `sendmsg()` 实现；TLS 分支不组装帧副本。

- [x] **Step 1: 接入认证前握手和事件兴趣更新**

在连接事件处理和 interest 计算中优先处理 NEGOTIATING endpoint；只注册 endpoint 报告的握手 WANT。握手成功后更新兴趣、允许已排队的 HELLO TX，并按当前内核可读条件安排 RX；握手错误直接关闭。

- [x] **Step 2: 接入 TLS RX**

在 `tr_connection_on_readable()` 中增加 TLS 分支：用 Parser 当前稳定写目标调用 endpoint read，`TR_AGAIN` 更新兴趣并返回；`TR_OK` 的明文字节继续走现有 Parser produce/dispatch；`TR_ERR_CLOSED` 标记有序 TLS peer half-close 并停读；其余负状态错误关闭。预算用完时只经现有 RX ready 队列续跑。

- [x] **Step 3: 接入 TLS TX**

在 `tr_connection_flush_tx()` 中保留原明文 `sendmsg()` 分支原文/语义；TLS 分支从当前 TX item 的一个连续 iovec 片段建立稳定写入。WANT 保存 item 与原片段长度且不改变 `wire_pos`；成功按返回明文数更新 `wire_pos`、turn TX 字节数与完成帧状态。完成后才允许 item 释放或切换片段。

- [x] **Step 4: 覆盖跨方向 WANT 与 pending 明文公平性**

让 TLS 写等待 READ 时由 EPOLLIN 重试发送，读等待 WRITE 时由 EPOLLOUT 重试读取；未匹配方向只更新兴趣、不调用 SSL 函数。`SSL_pending() > 0` 时通过 RX ready queue 续跑，且每次严格扣减现有字节/dispatch 预算；不以 `SSL_has_pending()` 自旋。

- [x] **Step 5: 运行新增集成与调度测试**

运行 `xmake run test_rpc_v2_tls_reactor`、`xmake run test_reactor_budget`、`xmake run test_reactor_fairness`、`xmake run test_reactor_tx_drain` 和 `xmake run test_byte_endpoint`；记录每项真实输出和退出码。

### Task 4: 普通 V2 Client/Server profile、期限与配置接入

**Files:**
- Modify: `src/client.c`
- Modify: `src/server.c`
- Modify: `tests/test_rpc_v2_tls_reactor.c`

**Interfaces:**
- `tr_client`/`tr_server` 在创建期存储可空 `tr_tls_openssl_profile *`，TLS-required V2 工厂验证配置并创建 profile。
- Client owner assemble request 携带由既有 connect deadline 转换的绝对 ns 值，TLS adopt 使用该值；Server accept 传入 peer 已登记的 `handshake_deadline`。
- Transport-only 工厂若 TLS-required 返回 `TR_ERR_UNSUPPORTED`；默认 disabled 和普通 V2 TLS-required 是两条明确配置路径。

- [x] **Step 1: 保存并回收 Client profile**

在 `tr_client_create_internal()` 对普通 V2 required 模式校验并调用 `tr_tls_openssl_client_profile_create()`；transport-only 模式 required 显式 unsupported。销毁只在 Runtime/Reactor 与连接停止后释放 profile；构造失败通过既有 rollback 收敛。

- [x] **Step 2: 将 Client 绝对 deadline 传入 owner adopt**

将 `tr_deadline_to_ns(&connect_deadline)` 的结果放入 assemble request；owner 回调按 `client->tls_profile` 选择 TLS/明文 adopt。先有 TLS handshake success，Reactor 才发送 deferred Channel start 排入的 HELLO。

- [x] **Step 3: 保存并回收 Server profile**

在 `tr_server_create_internal()` 中仅普通 V2 required 模式创建 server profile；transport-only required 返回 unsupported。清理 profile 时确保失败构造和正常销毁都只释放一次，且任何 peer session 已释放或持有独立 SSL_CTX 引用。

- [x] **Step 4: 将 Server peer deadline 传入 TLS adopt**

在 `tr_server_adopt_peer()` 将已建立的 `peer->handshake_deadline` 转为单调 ns，并用 `server->tls_profile` 接管。原有 Server handshake timer 持续在期限到达时 abort negotiating peer，不续期。

- [x] **Step 5: 运行 RED/GREEN 正例与回滚验证**

先单独运行 `test_rpc_v2_tls_reactor`，确认原 unsupported 失败变为真实 RPC 正例通过；再运行 `test_client_connect_deadline`、`test_client_connect_rollback`、`test_server_peer_rollback`、`test_server_record_deadline` 与 `test_rpc_v2_sdk`。

### Task 5: 验证认证顺序、FINAL/关闭与明文零回归并记录阶段证据

**Files:**
- Modify: `tests/test_rpc_v2_tls_reactor.c`
- Modify: `src/execution/reactor.c`（仅按失败测试补齐）
- Modify: `README.md`
- Create: `docs/reviews/rpc_v2_tls_c_20261023.md`

**Interfaces:**
- 普通 V2 TLS connection 的应用读写在 endpoint 完成握手前始终不可见。
- TLS close_notify 只发生在所有已接受的 V2 控制/终局 TX 已由 endpoint 接收后；TCP 截断不成为成功 FINAL。
- 明文验证仅针对原 V2 路径的 `sendmsg()` 证据和功能结果，不声称新的全面性能结果。

- [x] **Step 1: 依据已有失败证据补齐认证、半关闭和关闭逻辑**

peer 收到 TLS close_notify 后停止新 RX、继续排空现有 TX，完成本端有界 shutdown；正常 Client/Server 排空仅在 V2 FINAL/GOAWAY 等 TX 前驱完成后进入 TLS shutdown。TLS 协议错误、超时和取消走 abort，不等待 close_notify。

- [x] **Step 2: 运行 TLS、明文和生命周期回归**

运行完整 `test_rpc_v2_tls_reactor`、`test_rpc_v2_sdk`、`test_reactor_source_detach`、`test_server_destroy_retry` 与 `test_rpc_v2_server_destroy_drain`，并确认明文/ TLS 两种 fixture 都销毁成功、无 peer 或 fd 泄漏。

- [x] **Step 3: 更新 README 与证据记录**

README 将当前 TLS 说明更新为普通 V2 TLS-required 已接入、默认仍明文，并标出连接组 TLS/mTLS/身份绑定和 TLS-E 暂未完成。review 记录逐条列出测试矩阵、命令、OpenSSL 版本、失败负例和未通过的公网/资源门槛；不写未运行结果。

- [x] **Step 4: 完成完整功能验证和差异检查**

按全局验证命令运行 GCC debug、GCC release、Clang release、ASan/UBSan、TSan 测试；实际可运行的源码格式、生产静态分析也执行并存证。`git diff --check` 与 `git status --short` 确认无空白错误且两份既有未跟踪计划仍原样存在。

## 自审

- spec 中普通 V2 接线、认证前置、握手 deadline、任意分片、双向 DATA、稳定 TX、pending 明文、半关闭、FINAL 顺序、明文快速路径与失败回滚分别落入 Task 1–5。
- TLS-D 的 mTLS、CONTROL/DATA 绑定、凭证轮换/迟到凭证和 TLS-E 的资源总账及公网门槛没有被混入 TLS-C 验收。
- API 名称统一为 `tr_reactor_adopt_tls_fd_on_owner`；TLS endpoint 与 profile 所有权边界在 Task 2/4/5 一致。
- 每项生产接线之前都有测试行为；第一轮红灯明确落在 TLS-required 工厂的现有 unsupported 边界。
- 所有生成测试身份路径由仓库 helper 指向 `build/test-tmp`；测试命令显式重定向 `TMPDIR` 与 `XMAKE_TMPDIR`，不写 `/tmp`。
