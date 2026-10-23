# TLS-D1 mTLS 与连接组身份绑定实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` and perform all tasks inline. The user explicitly forbids background subagents.

**Goal:** 为普通 V2 RPC 和连接组接入 TLS 1.3 mTLS、可传递的验证主体、CONTROL/DATA 身份授权绑定与安全配置轮换。

**Architecture:** OpenSSL TLS session 在 Reactor owner 上完成双向证书验证；TLS endpoint 创建有界、不可变、引用计数的 URI SAN peer identity。普通 RPC work 和连接组 authorizer 只接收不含 OpenSSL 类型的借用 identity view。CONTROL 与每条 DATA 各自完成 mTLS，按稳定主体、group/epoch/member 代次和角色策略准入；profile 更新通过所有 Reactor owner 的暂停/安装/回滚屏障发布。

**Tech Stack:** C11、OpenSSL 3 TLS 1.3、Linux epoll LT、Xmake、现有 Reactor/byte endpoint、V2 RPC Record 和 Pipeline listener。

**Spec:** `docs/superpowers/specs/2026-10-23-tls-d1-mtls-group-identity-design.md`

## Global Constraints

- TLS-required 失败时关闭连接，绝不回退明文；TLS 身份只来自成功验证的证书链和用途。
- mTLS 主体必须且只能有一个 URI SAN，长度为 1–512 个 IA5 字节，精确比较，不回退到 Common Name。
- 连接组必须使用 client-auth REQUIRED；CONTROL/DATA 在 route 代次检查之外还要匹配经验证主体并分别授权。
- TLS 握手、TRR1 前导和原握手期限共享一个单调绝对期限；收到部分字节不得续期。
- TLS profile 更新只能从外部控制线程进行；多 owner 更新须全有或全无，失败回滚不释放未收敛对象。
- 普通 RPC peer identity 由 Call 引用至所有业务工作和 terminal 责任静默；callback 借用视图不能逃逸回调。
- 外部异步凭证和远端授权属于 TLS-D2/#243；TLS-E 承担 OpenSSL/内核完整资源预算与长期安全验收。
- 所有本地测试文件、身份、日志和临时目录写入仓库 `build/`；测试时设置 `TMPDIR="$PWD/build/test-tmp"` 与 `XMAKE_TMPDIR="$PWD/build/test-tmp"`，不得写入 `/tmp`。
- 不使用后台子 agent；各公开交付阶段分别更新开发版本，TLS-D1 结束时 #234 仍保持开放。

## 文件与职责

- `include/tr/peer_identity.h`：公共、借用的对端身份与连接组角色/授权上下文，不暴露 OpenSSL 类型。
- `include/tr/tls.h`、`src/io/tls_config.c`：现有 client cert/server client-auth 字段的验证合同。
- `src/io/tls_openssl.c/.h`、`src/io/peer_identity_internal.c/.h`：mTLS profile/session、唯一 URI SAN 提取、identity 引用与 profile 代次。
- `src/io/byte_endpoint.h/.c`、`src/execution/reactor.h/.c`：握手后身份发布、TLS+preface 顺序及 TLS session 身份生命周期。
- `include/tr/rpc_v2.h`、`src/rpc/rpc_record_dispatch.c/.h`：把 Server 连接身份绑定到每个 RPC work event 的借用 request view。
- `include/tr/transport.h`、`src/transport/group/pipeline_listener*.c/.h`：Server CONTROL/DATA 主体准入和 route 回收。
- `src/transport/group/client_group*.c/.h`、`src/client.c`：Client group TLS 握手、延后 TRR1 发送与服务端主体一致性。
- `src/client.c`、`src/server.c`、Reactor owner command 代码：profile 多 owner 安全更新、旧代次排空/撤销。
- `tests/tls_test_identity.c/.h`：在 `build/test-tmp/` 生成有效、错误、轮换中的 client/server 证书。
- `tests/test_rpc_v2_tls_identity.c`、`tests/test_pipeline_listener_tls.c`、`tests/test_connection_group_tls.c`：分别验证普通 RPC、Server listener 和公开 Client/Server group 路径。
- `xmake.lua`：注册新增测试目标并保留全量测试入口。
- `README.md`、`docs/rpc-v2-sdk.md`、`docs/reviews/rpc_v2_tls_c_20261023.md`、`docs/reviews/v2_remaining_work.md`：更新 mTLS、身份 view、版本、CI 与 TLS-D2/E 边界。

## 测试命令模板

每个测试命令都使用项目内临时目录：

```bash
mkdir -p build/test-tmp
TMPDIR="$PWD/build/test-tmp" XMAKE_TMPDIR="$PWD/build/test-tmp" PYTHONDONTWRITEBYTECODE=1 xmake build test_tls_config
TMPDIR="$PWD/build/test-tmp" XMAKE_TMPDIR="$PWD/build/test-tmp" PYTHONDONTWRITEBYTECODE=1 xmake run test_tls_config
```

运行多个目标时，逐个替换目标名并检查每条命令的退出码；集成修改后还要运行
`xmake build --all` 与 `xmake test -v -j1`。测试身份 helper 必须指向
`build/test-tmp`，不得调用会在 `/tmp` 创建目录的脚本。

---

### Task 1: 普通 V2 mTLS 与 RPC 身份生命周期

**Files:**
- Create: `include/tr/peer_identity.h`
- Create: `src/io/peer_identity_internal.h`, `src/io/peer_identity_internal.c`
- Modify: `include/tr/rpc_v2.h`, `include/tr/tls.h`
- Modify: `src/io/tls_config.c`, `src/io/tls_openssl.h`, `src/io/tls_openssl.c`
- Modify: `src/execution/reactor_internal.h`, `src/execution/reactor.c`
- Modify: `src/transport/channel/channel_internal.h`, `src/transport/channel/channel.c`
- Modify: `src/rpc/rpc_record_dispatch.c`
- Modify: `tests/tls_test_identity.h`, `tests/tls_test_identity.c`, `tests/test_tls_config.c`, `tests/test_tls_openssl.c`
- Create: `tests/test_rpc_v2_tls_identity.c`
- Modify: `tests/test_rpc_v2_server.c`, `xmake.lua`, `README.md`, `docs/rpc-v2-sdk.md`, `docs/reviews/v2_remaining_work.md`

**Interfaces:**
- Public view: `struct tr_peer_identity_view { enum tr_peer_identity_authentication authentication; const uint8_t *uri_san; uint32_t uri_san_len; }`。
- Internal identity API: `tr_peer_identity_snapshot_create(uri, len, &out)`、`tr_peer_identity_snapshot_retain(identity)`、`tr_peer_identity_snapshot_release(identity)`、`tr_peer_identity_snapshot_view(identity, &view)`。
- `struct tr_rpc_v2_request` 新增 `struct tr_peer_identity_view peer_identity`；Server 所有 OPEN/MESSAGE/HALF_CLOSE work event 都填入同一 Call 身份。
- TLS OpenSSL session 在验证后提供 `tr_tls_openssl_session_peer_identity(session, require_uri, &out)`；无证书的 OPTIONAL 情况返回空身份，required 或 group 身份缺失返回错误。

- [x] **Step 1: 先写 TLS 配置和 X.509 身份正负例**

在 `test_tls_config.c` 增加 REQUIRED/OPTIONAL/NONE 与缺失 CA、孤立证书/私钥、错误枚举组合断言。扩充 `tls_test_identity` 生成 clientAuth、wrong-client-CA、无 URI、重复 URI、空 URI、超 512 字节 URI 和换钥但相同 URI SAN 的证书。

- [x] **Step 2: 运行 RED 并记录现有 unsupported 边界**

构建并运行 `test_tls_config`、`test_tls_openssl`；新断言应因 mTLS 字段当前返回 `TR_ERR_UNSUPPORTED` 或缺少 identity API 而失败。保留既有 TLS-C 服务端认证测试的绿色行为。

- [x] **Step 3: 实现 mTLS profile 和唯一 URI SAN 提取**

服务端 `client_auth=REQUIRED/OPTIONAL` 加载 client CA 并启用 `SSL_VERIFY_PEER`；REQUIRED 再启用 `SSL_VERIFY_FAIL_IF_NO_PEER_CERT`。Client 提供 client cert/key 时加载并检查私钥匹配。握手认证后提取唯一 1–512 字节 URI SAN；clientAuth/serverAuth EKU、证书链和证书期仍由 OpenSSL 验证。没有身份的 OPTIONAL 连接发布 `TR_PEER_IDENTITY_NONE`。

- [x] **Step 4: 实现引用计数 identity 和 RPC OPEN 生命周期**

Reactor connection 在 TLS AUTHENTICATED 后持有 identity snapshot；V2 Call 在 OPEN 准入时 retain，同一 identity view 进入所有 work event；所有 worker 和 terminal 责任静默后 release。明文与无证书 OPTIONAL 的视图为 `NONE`/空 URI。metadata 不得写入或覆盖 identity。

- [x] **Step 5: 增加真实 RPC mTLS 集成用例并运行 GREEN**

`test_rpc_v2_tls_identity` 使用公开 `tr_client_create`/`tr_server_create` 和真实 loopback TLS；验证双向有效证书、Server OPEN/MESSAGE/HALF_CLOSE 收到同一主体、错误 CA/缺失证书/错 EKU/无 URI 不进入 work、metadata 同名键不伪装认证身份、断开连接后仍在工作的 Call 可以安全读取 identity、OPTIONAL 无客户端证书与普通明文路径均返回 `NONE`。`test_rpc_v2_server` 也断言明文身份为 `NONE`。运行 `test_tls_config`、`test_tls_openssl`、`test_rpc_v2_tls_identity`、`test_rpc_v2_tls_reactor`、`test_rpc_v2_server`、`test_rpc_v2_sdk` 与 `test_client_connect_rollback`。

- [x] **Step 6: 提交普通 V2 mTLS 垂直切片**

将 `xmake.lua` 从 `2.0.0-dev.12` bump 到 `.13`；按 `feature-commit-format` 记录需求、背景、实现、影响、验证、性能、风险和回滚。提交只包含 Task 1 文件。

### Task 2: TLS 后的 Server CONTROL/DATA 路由与主体授权

**Files:**
- Modify: `include/tr/transport.h`
- Modify: `src/io/tls_openssl.c`
- Modify: `src/execution/reactor_internal.h`, `src/execution/reactor.c`
- Modify: `src/group/pipeline_internal.h`, `src/group/pipeline.c`
- Modify: `src/group/pipeline_registry_internal.h`, `src/group/pipeline_registry.c`
- Modify: `src/transport/group/pipeline_listener_internal.h`, `src/transport/group/pipeline_listener.c`
- Modify: `src/server.c`
- Modify: `tests/test_pipeline_listener.c`, `tests/test_connection_group_facade.c`
- Modify: `tests/test_inbound_handshake_deadline.c`, `tests/test_internal_constructor_ownership.c`
- Existing regression: `tests/test_tls_openssl.c::test_unexpected_eof_is_not_close_notify`
- Create: `tests/test_pipeline_listener_tls.c`
- Modify: `xmake.lua`, `README.md`

**Interfaces:**
- `enum tr_connection_group_role { TR_CONNECTION_GROUP_ROLE_CONTROL=1, TR_CONNECTION_GROUP_ROLE_DATA=2 }`。
- `struct tr_connection_group_authorization` 包含 `group`、`role`、`peer_identity`、`control_identity`；CONTROL 的 control identity 为空，DATA 的值来自活动 CONTROL session。
- `tr_connection_group_authorize_cb` 改为接收 `const struct tr_connection_group_authorization *` 与 `arg`。
- 新 owner API `tr_reactor_adopt_tls_fd_prefaced_on_owner(reactor, fd, profile, deadline_ns, preface, out)` 在 endpoint 认证后才把明文字节交给 preface parser。

- [x] **Step 1: 写认证前 TRR1 不可见及主体绑定失败测试**

在 `test_pipeline_listener_tls.c` 使用测试 OpenSSL Client 通过回环 socket 向 TLS listener 先发未经加密的 TRR1，并断言 route/authorize/control/message callback 都为零。增加有效 CONTROL、同 CA 不同 URI 主体 DATA、错 group/epoch/member generation DATA 与同主体合法 DATA 场景。

- [x] **Step 2: 运行 RED**

运行 `test_pipeline_listener_tls`；当前 group TLS factory 仍拒绝或以明文解析，测试应在握手/主体门禁断言处失败。确认当前 `test_pipeline_listener` 的明文基线仍通过。

- [x] **Step 3: 实现 TLS-preface Reactor adopt**

在 TLS endpoint AUTHENTICATED 后才调用 preface feed；复用同一 `handshake_deadline_ns`，不重置 TRR1 期限。preface feed 接收到的内容只来自 endpoint 解密的明文字节。TLS 错误、EOF、期限和回滚路径沿用现有 endpoint detach/rollback 屏障。

- [x] **Step 4: 实现 Server CONTROL/DATA mTLS 与 authorizer**

pipeline listener 仅在 server TLS REQUIRED 且 client auth REQUIRED 时允许 group listen。CONTROL TLS 身份进入 authorize 上下文并存入 session；DATA 先校验现有 route/member 代次，再比较 URI 主体与 CONTROL 并调用 DATA authorizer。失败只撤销自己的 route reservation，不修改 CONTROL 或其他 DATA。
TLS OpenSSL socket BIO 的写入使用 `MSG_NOSIGNAL`，避免异常 EOF 时 OpenSSL 发告警触发进程级 SIGPIPE。

- [x] **Step 5: 运行 Server group GREEN 与构造回滚测试**

运行 `test_tls_config`、`test_tls_openssl`、`test_rpc_v2_tls_identity`、`test_rpc_v2_tls_reactor`、`test_pipeline_listener_tls`、`test_rpc_v2_server`、`test_rpc_v2_sdk`、`test_client_connect_rollback`、`test_pipeline_listener`、`test_pipeline_route`、`test_pipeline_registry`、`test_connection_group_facade`、`test_inbound_handshake_deadline`、`test_server_peer_rollback` 和 `test_internal_constructor_ownership`；本地 15 个目标均构建成功且二进制退出码为 0。

- [x] **Step 6: 提交 Server group 安全切片**

版本从 `.13` bump 到 `.14`；Task 2 已按 `feature-commit-format` 提交为 `2e24c32` 并推送到 `feat/v2-tls-d-234`。Server group 正负测试、构造回滚和源码格式门禁均通过。

### Task 3: Client CONTROL/DATA mTLS 与服务器主体连续性

**Files:**
- Modify: `src/client.c`
- Modify: `src/transport/group/client_group_internal.h`, `src/transport/group/client_group.c`
- Modify: `src/execution/reactor_internal.h`, `src/execution/reactor.c`
- Modify: `src/io/connector_internal.h`, `src/io/connector.c`, `src/io/tls_openssl.h`, `src/io/tls_openssl.c`
- Modify: `src/transport/channel/channel.c`
- Modify: `tests/test_connection_group_facade.c`
- Modify: `tests/test_connector.c`, `tests/test_connector_deadline.c`, `tests/test_rpc_v2_tls_reactor.c`
- Create: `tests/test_connection_group_tls.c`
- Modify: `xmake.lua`, `README.md`, `docs/rpc-v2-sdk.md`

**Interfaces:**
- `tr_client_create_transport` 接受 `TR_TLS_REQUIRED` 并要求成对 client certificate/private key；无证书或无 expected server name 返回配置错误。
- Client group 保存 CONTROL 的 server URI identity；同一 group 中每条 DATA 使用同一 client TLS snapshot，并在发布 transfer READY 前要求 Server URI 与 CONTROL 完全相同。

- [x] **Step 1: 写公开 Client group 真实端到端测试**

`test_connection_group_tls` 用公开 Client/Server transport API 和真实 loopback mTLS 完成 CONTROL/DATA 消息。TCP 转发夹具将 CONTROL 与第一条 DATA 分别送到同 CA、相同 DNS SAN、不同 URI SAN 的服务端；验证不准入 DATA、不能建立 READY、拒绝 DATA_OFFER 后只撤销该路由，并证明原 CONTROL 随后仍可建立同主体 DATA。另覆盖客户端错误 CA、错误客户端 URI、服务端先关闭和握手静默超时。

- [x] **Step 2: 运行 RED 并保留 transport-only 明文基线**

初始 `test_connection_group_tls` 在 `tr_client_create_transport()` 的 `TR_ERR_UNSUPPORTED` 断言处 RED；`test_connection_group_facade` 明文路径保持通过。随后更新 Reactor API 时，同步调整 Connector 回调携带 TCP 阶段原绝对期限。

- [x] **Step 3: TLS 握手后发送 CONTROL/DATA preface**

Client 在每个 CONTROL/DATA fd 上通过新的 outbound-prefaced Reactor API，在 TLS 认证回调成功后才发送加密 TRR1。Client group 保留 CONTROL 服务端 URI；DATA 必须匹配且 TRR1 写完后才成为 READY 可用状态。TCP、TLS 握手与 TRR1 使用同一单调绝对期限；握手完成后仍保留该期限直到前导写完，过期或 URI 不匹配都关闭该连接并撤销对应 route。

- [x] **Step 4: 运行双向身份和失败回滚测试**

`test_connection_group_tls` 覆盖错误 client CA、错误 client URI、服务端 CONTROL/DATA URI 不同、失败 DATA route 撤销后继续复用 CONTROL、服务端先关闭连接组和客户端 TLS 握手超时。`test_connection_group_facade` 覆盖明文基线、迟到 offer/取消、CONTROL 远端关闭及销毁回滚。构建并运行连接组/TLS/RPC 重点回归目标均通过。

- [x] **Step 5: 更新 SDK 使用文档并提交 Client group 切片**

版本从 `.14` bump 到 `.15`；README 与 SDK 合同说明 Client/Server 连接组 mTLS、每条 DATA 独立握手、主体连续性和期限语义。Client group 端到端覆盖及全量 88 项测试通过，本切片按 `feature-commit-format` 提交并推送到 `feat/v2-tls-d-234`。Task 4 的 profile 轮换尚未开始。

### Task 4: Profile 多 owner 原子发布、优雅轮换与旧代次撤销

**Files:**
- Modify: `include/tr/tls.h`, `include/tr/client.h`, `include/tr/server.h`
- Modify: `src/io/tls_openssl.h`, `src/io/tls_openssl.c`
- Modify: `src/client.c`, `src/server.c`
- Modify: `src/execution/reactor_internal.h`, `src/execution/reactor.c`
- Modify: `src/rpc/rpc_record_client.c`, `src/rpc/rpc_record_client.h`
- Modify: `src/transport/group/client_group.c`, `src/transport/group/pipeline_listener.c`
- Modify: `tests/test_rpc_v2_tls_identity.c`, `tests/test_connection_group_tls.c`, `tests/test_tls_openssl.c`
- Modify: `tests/tls_test_identity.c`, `tests/tls_test_identity.h`, `README.md`, `xmake.lua`
- Modify: this plan's Task 4 evidence and status

**Interfaces:**
- `enum tr_tls_rotation_policy { TR_TLS_ROTATE_GRACEFUL=0, TR_TLS_ROTATE_REVOKE_OLD=1 }`。
- `tr_client_tls_update_profile(client, config, policy, out_generation)` 和 `tr_server_tls_update_profile(server, config, policy, out_generation)`：只用于已启用 TLS_REQUIRED 的实例；成功才写 generation。

- [x] **Step 1: 写多分片 Server、单 owner Client 轮换及故障测试**

公开 Client 只有一个 Reactor owner；RPC 与连接组 Server 使用两个 shard。测试验证 candidate 构造失败不改变 generation/out 参数，Server 两 shard 的 TLS generation 一致，CONTROL/DATA generation 不一致时拒绝路由，graceful 轮换后关闭并重连的组使用新证书。安装失败注入通过正在 drain 的连接组 listener 触发：候选 profile 已安装到后续 shard，listener 拒绝安装；若恢复命令也被 listener 生命周期拒绝，实例进入 fail-closed 状态，保留资源并只允许 destroy 重试。

- [x] **Step 2: 运行 RED**

在 generation 辅助函数实现前，`test_tls_openssl` 因缺少 `tr_tls_openssl_profile_next_generation` 声明而编译失败；接入新服务端证书夹具前，连接组测试因缺少 `rotated_server_certificate_file` 字段而编译失败。补齐实现后，相关 TLS 和连接组目标转绿。

- [x] **Step 3: 让 profile 与 session 持有同一引用计数快照**

profile snapshot 持有 SSL_CTX 与 generation；Reactor TLS connection 和已接管的 Server CONTROL session retain profile。Client group 在 owner 线程切换到 pending profile。最后引用归还时释放 profile 与身份对象。generation 达 `UINT64_MAX` 或 profile 已共享时拒绝修改，不能回绕。

- [x] **Step 4: 实现更新 gate、安装屏障和 rollback 屏障**

profile 在调用线程构建。Server 各 owner 先暂停 TLS 准入，所有 gate 成功后安装 candidate；全部安装成功才开放 gate。可恢复的安装错误在 gate 关闭期间回装旧指针；若 gate、install 或 rollback 生命周期命令失败，保留实例和 snapshots、保持 fail-closed，并仅允许 destroy 重试。Client 对单 owner 使用相同准入和恢复规则。

- [x] **Step 5: 实现 graceful 与 revoke-old 语义**

Graceful update 使新 TLS 接管用新 generation，已接管 session 保持旧 snapshot；Client group 在重连时使用新 snapshot，Server 旧 generation CONTROL 不接受新 generation DATA，需 drain/reconnect。REVOKE_OLD 通过所有 owner 关闭旧 generation 连接；连接仍持有 snapshot 直至 close callback/quiescence 后释放。

- [x] **Step 6: 运行轮换 GREEN 和生命周期回归**

`test_connection_group_tls` 覆盖同主体新旧服务端/客户端证书轮换、旧 CONTROL 拒绝新代次 DATA、重连成功、generation 输出保持和 listener drain 安装失败；`test_rpc_v2_tls_identity` 覆盖普通 RPC graceful/revoke-old 和新连接恢复；`test_tls_openssl` 覆盖最大 generation 与共享 profile 不可变。全量 88 个测试通过，包含既有 Client/Server 回滚、destroy retry 和 Reactor 生命周期用例。未覆盖 TLS-D1 的跨工具链/消毒器矩阵，留给 Task 5。

- [x] **Step 7: 提交快照轮换交付**

版本从 `.15` bump 到 `.16`；本提交按 `feature-commit-format` 记录跨 owner 原子性、旧连接撤销影响、明文路径不变的性能影响及回滚方式，并推送到 `feat/v2-tls-d-234`。提交只包含 Task 4 与本计划文件，不包含其他未跟踪计划。

### Task 5: TLS-D1 全矩阵、记录与 GitHub 验收

**Files:**
- Modify: `.github/workflows/ci.yml`, `xmake.lua` (SDK header installation boundary)
- Modify: `README.md`, `docs/rpc-v2-sdk.md`, `docs/build.md`, `docs/platform-support.md`
- Modify: `docs/superpowers/plans/2026-10-23-tls-c-reactor-integration.md`
- Modify: `docs/reviews/rpc_v2_tls_c_20261023.md`, `docs/reviews/v2_remaining_work.md`
- Create: `docs/reviews/rpc_v2_tls_d1_20261023.md`

- [x] **Step 1: 更新 TLS-C 已合入状态与 TLS-D1 已交付边界**

TLS-C review/plan 已记录 PR #310 merge commit `0fa90817c15286485ee2e1cfe8372bb8ab8a8e5a` 和成功的 GitHub 检查；Remaining-work、SDK/build/platform 文档已记录 TLS-D1 当前实现边界以及 TLS-D2/E 未完成。TLS-D1 review 补充本地实测命令、工具版本、identity 约束和未验证的 OpenSSL/RSS 预算。

- [x] **Step 2: 运行 GCC Debug、GCC Release、Clang Release、ASan/UBSan 与 TSan 全量矩阵**

五种配置均执行配置、`xmake build --all -v` 和 `xmake test -v -j1`，各 88/88 通过；矩阵与复测日志保存在 `build/tls-d1/`。本机为 Arch Linux、GCC 16.2.1、Clang 23.1.3、OpenSSL 3.6.5、Xmake 3.1.2；不把本机结果称为 Ubuntu 22.04 / OpenSSL 3.0 的等价验证。SDK 白名单修正后的 dev.17 又通过 GCC Release 88/88。

- [ ] **Step 3: 运行源码格式、安装消费端和固定静态分析**

本地修改行格式检查、22 项格式脚本自测、Make all/test/test-timer/clean/compiler-switch、安装头文件边界、GCC/Clang C11 与 G++/Clang++ C++11 公共头及 V2 消费端、portable CRC32C 和 QEMU 隐藏 SSE4.2 均通过。发现并修复 `peer_identity.h` 未安装问题。固定 GCC 11/Clang 14 分析器未安装在本机，需由本分支 PR CI 确认；Xmake 3.1.2 下 `test_compile_commands.py` 的临时副本 project-dir 自测失败，但当前 checkout 的 GCC/Clang 编译数据库各 47 个生产单元校验通过。完整 OpenSSL/RSS 硬预算留给 TLS-E。

- [ ] **Step 4: 创建 TLS-D1 PR 并等待 GitHub CI 全绿**

`feat/v2-tls-d-234` 已推送至 `f8ce985`，TLS-D1 PR #311 已创建并等待固定版本 CI。PR 描述包含 D1 scope、版本序列、本地证据及 TLS-D2/E 未完成项。任何新静态告警必须解释并保持精确 baseline，不得无依据删除或把 `unverified` 改成已审。

- [ ] **Step 5: 合并、更新 #234 并清理已合入远程分支**

仅在 PR 全部必需检查通过后按仓库 merge commit 方式合并；验证合并提交的 main CI，再更新 #234 说明 TLS-D1 部分交付，保持 #234 开放，并删除仅供本 PR 使用的远程分支。

## 自审覆盖

- URI SAN 与证书链/EKU、optional/required 语义：Task 1、验收矩阵 1–2。
- RPC identity 在连接关闭后仍有 Call 引用：Task 1 和 RPC 生命周期测试。
- TLS-before-TRR1、CONTROL/DATA route 与主体绑定、准确撤销 route：Tasks 2–3。
- multi-owner profile generation、全成全败屏障、rollback 和 revoke：Task 4。
- TLS-D2/#243 和 TLS-E 资源边界、全量平台和静态门禁：Task 5。
- 默认 disabled 明文、普通 TLS-C、deadline、construct/destroy retry 全有回归项。
- 所有本地临时文件都写仓库 `build/`；没有委派子任务。
