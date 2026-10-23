# TLS-D2 异步凭证与连接组授权实施计划

> **执行要求：** 本计划按 `superpowers:executing-plans` 在当前会话逐项执行；用户已禁止子代理，步骤使用 `- [ ]` 跟踪。

**Goal:** 为 V2 Client/Server 提供有界异步 TLS 凭证获取/轮换与 CONTROL/DATA 远端授权，并保留 TLS_REQUIRED 失败关闭和 D1 profile 原子发布语义。

**Architecture:** 使用值类型操作句柄与 facade 预算内的固定槽位。凭证 provider 只负责外部异步 I/O；`complete` 复制到预留缓冲，外部控制线程的 `poll` 构造 profile 并走 D1 发布屏障。连接组授权通过 Reactor 辅助 eventfd 回到原 owner；路由前导支持 defer/resume，授权完成前不读 TRR1 后续业务。

**Tech Stack:** C11、OpenSSL 3、Xmake 3.1.1、POSIX `eventfd`/pthread、现有 Reactor timer 和 D1 TLS profile。

**Spec:** `docs/superpowers/specs/2026-10-24-tls-d2-async-credentials-authorization-design.md`

## Global Constraints

- TLS_REQUIRED 的任何凭证、授权、期限或容量失败都关闭连接，不回退明文。
- 异步操作槽默认 64；凭证 bundle 默认上限 4 MiB；二者由 `tr_facade_limits` 配置并受现有 memory budget 约束。
- provider start/cancel 回调必须短小、非阻塞；取消是合作式通知，未确认操作阻止 facade 成功销毁。
- 不增加 TLS 专用线程池；凭证 profile 解析只在外部控制线程执行，不在 Reactor、RPC worker 或 provider completion 中执行。
- 新增测试的 `TMPDIR` 和 `XMAKE_TMPDIR` 均指向当前工作区 `build/`；不得向 `/tmp` 写测试文件、日志、缓存或产物。
- 保留既有 Xmake 目标、CI 必需检查标识和 TLS-D1 静态路径；文档、注释、提交与 PR 说明使用简体中文。
- 本计划只实现 TLS-D2；不宣称完成 #243 的通用 Call、父子取消、跨 owner 环路或 TLS-E 发布验收。

---

## 文件结构与职责

- `include/tr/status.h`、`src/status.c`：新增可观察取消终态 `TR_ERR_CANCELLED`。
- `include/tr/facade.h`、`src/facade.c`：新增有界 TLS 操作数与凭证字节上限的公共配置及默认值。
- `include/tr/tls.h`：凭证 bundle、provider 请求、操作句柄和操作结果值类型。
- `include/tr/client.h`、`include/tr/server.h`：Client/Server 凭证 provider 回调与 start/complete/poll/cancel API。
- `include/tr/transport.h`：连接组异步授权与取消回调，以及 ALLOW/DENY 结果。
- `src/io/tls_provider_operation_internal.h`、`src/io/tls_provider_operation.c`：有界槽位、句柄代次、一次性 complete/cancel 线性化和结果容量记账。
- `src/io/tls_config.h`、`src/io/tls_config.c`：静态配置与 provider-only 初始配置组合校验。
- `src/io/tls_openssl.h`、`src/io/tls_openssl.c`：从带长度 PEM bundle 构造 Client/Server profile；固定信任策略，清理私钥临时副本。
- `src/client.c`、`src/server.c`：保留 TLS 策略字符串、provider 生命周期、初始 profile 等待、异步 refresh 和 destroy 重试。
- `src/execution/reactor_internal.h`、`src/execution/reactor.c`：支持 TLS/路由前导读完后的 defer/resume。
- `src/transport/group/pipeline_listener_internal.h`、`src/transport/group/pipeline_listener.c`：每连接有界 pending 授权、辅助 eventfd、CONTROL/DATA 提交与取消。
- `tests/test_tls_provider_operation.c`：操作槽、代次、容量、取消/完成竞争单元测试。
- `tests/test_tls_openssl.c`、`tests/test_tls_config.c`：内存 PEM profile 与配置边界回归。
- `tests/test_pipeline_listener_tls.c`、`tests/test_connection_group_tls.c`：真实 loopback TLS defer/resume、远端授权、迟到结果和关闭场景。
- `tests/test_rpc_v2_sdk.c`：已安装 C11/C++11 消费端公开符号检查。
- `xmake.lua`：新内部单测目标、生产源码登记；不重命名现有测试/CI 目标。
- `docs/rpc-v2-sdk.md`、`README.md`：provider 使用合同、期限、取消和限制。

### Task 1：公共状态、配置和 provider 类型

**Files:**
- Modify: `include/tr/status.h`
- Modify: `src/status.c`
- Modify: `include/tr/facade.h`
- Modify: `src/facade.c`
- Modify: `include/tr/tls.h`
- Modify: `include/tr/client.h`
- Modify: `include/tr/server.h`
- Modify: `include/tr/transport.h`
- Test: `tests/test_tls_config.c`

**Interfaces:**
- `tr_tls_provider_operation` 包含 `instance`、`generation`、`slot`、`kind`，只作为值句柄，不授予对象所有权。
- `tr_tls_credential_bundle` 用三组 `(const uint8_t *, size_t)` 表示 PEM 链、私钥和可选口令。
- `include/tr/tls.h` 定义 `tr_tls_provider_operation`、`tr_tls_credential_bundle`、`tr_tls_credential_request`、`tr_tls_credential_result` 与 `tr_tls_credentials_provider`。provider 的 `fetch(request, operation, arg)` 和 `cancel(operation, arg)` 都返回 `void`；请求含单调时钟绝对期限和轮换策略，bundle 的三段字节视图均带长度。
- Client/Server 配置各自内嵌 `tr_tls_credentials_provider`；公开 `tr_client_tls_credentials_start/complete/poll/cancel` 与 Server 对应入口。`complete` 接受 facade、值句柄、状态和 bundle；`poll` 通过结果结构返回终态及 generation。
- 连接组授权配置新增 `authorize_async(request, operation, arg)` 与 `authorize_cancel(operation, arg)`，均返回 `void`；请求含组/角色/身份视图、TLS generation 和单调时钟绝对期限。`tr_server_connection_group_authorize_complete(server, operation, status, decision)` 跨线程提交一次结果，decision 仅允许 ALLOW/DENY。同步 authorize 同时配置时先 veto，再等待异步 ALLOW。
- `tr_facade_limits` 新增 `max_pending_tls_operations` 与 `max_tls_credential_bundle_bytes`；默认分别为 64 和 4 MiB，0 按现有 `tr_facade_limits_init()` 规则归一化为默认值。

- [x] **Step 1: 先写失败的默认值与状态字符串测试**

在 `tests/test_tls_config.c` 增加测试：`tr_client_config_init()` 与 `tr_server_config_init()` 默认得到 64 和 4 MiB；容量显式设为 1 时保留 1；新增 `tr_status_str(TR_ERR_CANCELLED)` 返回固定标记 `cancelled`。provider/cancel 配对与连接组授权入口组合在 Task 4/6 的真实 facade 构造测试中验证。

- [x] **Step 2: 运行专测确认失败**

运行：

```sh
mkdir -p build/tmp build/xmake-tmp
TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" \
  xmake test -v -j1 'test_tls_config/*'
```

预期：新增字段/API 尚不存在，测试编译失败。

- [x] **Step 3: 实现公共类型与校验**

在 `tr_facade_limits_init()` 中增加两个默认值，在 `tr_status` 末尾新增 `TR_ERR_CANCELLED = -17` 并扩展 `tr_status_str()`。provider callback 与 cancel 必须成对；provider-only Server 仍须有固定 `client_auth` 和静态信任根。所有公开注释说明借用数据、句柄失效点、线程上下文和 `TR_OK`/失败输出行为。

- [x] **Step 4: 验证并提交本任务**

运行上面的专测与 `git diff --check`；预期 `test_tls_config` 通过，已有配置回归不变。提交只包含本任务文件，提交说明写明新增状态/配置和验证命令。

### Task 2：有界 provider 操作槽

**Files:**
- Create: `src/io/tls_provider_operation_internal.h`
- Create: `src/io/tls_provider_operation.c`
- Modify: `xmake.lua`
- Create: `tests/test_tls_provider_operation.c`

**Interfaces:**
- `tr_tls_provider_operation_table_init(table, capacity, budget)` 预分配固定槽位并记账，内部生成不复用的实例 ID。
- `tr_tls_provider_operation_reserve(table, kind, result_capacity, out_handle)` 在调用 provider 前取得槽位、代次和结果容量；失败不修改输出。
- `tr_tls_provider_operation_complete(table, handle, status, result)` 对首次有效完成线性化并写入预留槽；重复/跨实例句柄返回 `TR_ERR_STALE`。
- `tr_tls_provider_operation_request_cancel(table, handle, reason, out_notify)` 只请求一次合作式取消；先胜者决定结果，未确认槽位不回收。
- `tr_tls_provider_operation_retire(table, handle)` 仅在 provider 已确认且 owner/调用方已消费结果后归还槽位和 budget。
- 每个 facade 只有一张操作表；Server 的连接组 listener 借用 Server 操作表，credential refresh 与 auth 共用总槽位上限。

- [x] **Step 1: 写状态机负例与容量测试**

在 `tests/test_tls_provider_operation.c` 覆盖容量满返回 `TR_AGAIN` 且输出 handle 不变、跨实例/旧代次句柄 `TR_ERR_STALE`、句柄代次最大值不回绕、重复 complete 只有一次成功、cancel 与 complete 两种先后顺序只保留一个终态、未确认取消无法 retire。

- [x] **Step 2: 注册专用 Xmake 测试目标并确认红灯**

将 `src/io/tls_provider_operation.c` 加入 `trcore`，新增 `test_tls_provider_operation` 目标并仅链接 `trcore`；运行：

```sh
TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" \
  xmake test -v -j1 'test_tls_provider_operation/*'
```

预期：因内部接口缺失而编译失败。

- [x] **Step 3: 实现固定槽与一次性线性化**

槽位数组和句柄实例在初始化时建立；所有跨线程状态在同一 mutex 下更新。credential 结果槽在准入时按 bundle 上限预留并计入 budget；授权结果固定大小。完成路径不分配、不等待队列容量，取消不释放 provider 仍可能读写的 `arg` 或 payload。

- [x] **Step 4: 跑操作内核专测与 sanitizer 专测**

运行目标专测，然后用 `xmake f -c -y -m tsan --toolchain=gcc` 配置并运行同一目标。预期并发竞态测试通过且 TSan 无报告；所有临时与缓存路径保持在 `build/`。

### Task 3：内存 PEM 到不可变 OpenSSL profile

**Files:**
- Modify: `src/io/tls_openssl.h`
- Modify: `src/io/tls_openssl.c`
- Modify: `src/io/tls_config.h`
- Modify: `src/io/tls_config.c`
- Modify: `tests/test_tls_openssl.c`

**Interfaces:**
- 新增 `tr_tls_openssl_client_profile_create_from_pem(config, bundle, max_bundle_bytes, out)` 与保留 peer identity 策略的对应构造器。
- 新增 `tr_tls_openssl_server_profile_create_from_pem(config, bundle, max_bundle_bytes, out)`；最大 bundle 字节数来自门面 limits。
- 两个构造器只替换本地身份 cert/key；Client trust store、expected peer/SNI、Server client-auth 与 client CA 完全由固定配置提供。

- [x] **Step 1: 先写内存构造测试**

用 `tls_test_identity_init()` 在 `build/test-tmp` 生成证书，将 PEM 读入有长度的内存 buffer；验证正确链/匹配私钥生成 profile，错私钥、空链、私钥口令错误和超长 bundle 均失败且 `out == NULL`。TLS 1.3 loopback 握手分别验证 Client 与 Server profile。

- [x] **Step 2: 运行 TLS backend 专测确认红灯**

```sh
TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" \
  xmake test -v -j1 'test_tls_openssl/*'
```

预期：新构造入口不存在，新增测试未通过。

- [x] **Step 3: 用 OpenSSL BIO 解析链与私钥**

用 `BIO_new_mem_buf()` 解析证书链和私钥，拒绝空字段、尾随非法 PEM、长度超过 `max_tls_credential_bundle_bytes` 和不匹配私钥。口令回调只借用 bundle 字节；私钥/口令临时副本用 `OPENSSL_cleanse()` 清理。所有失败路径释放 BIO、X509、EVP_PKEY、SSL_CTX 和 profile。

- [x] **Step 4: 验证 file 与 memory 路径一致**

运行 TLS backend、配置和既有 TLS identity 专测；对有效身份比较两种 profile 的 TLS 版本、peer identity 策略和握手结果。用 `git diff --check` 检查源码格式。

### Task 4：Client/Server 异步凭证获取与轮换

**Files:**
- Modify: `include/tr/client.h`
- Modify: `include/tr/server.h`
- Modify: `src/client.c`
- Modify: `src/server.c`
- Modify: `src/io/tls_config.c`
- Create: `tests/test_tls_provider.c`
- Modify: `xmake.lua`

**Interfaces:**
- `tr_client_tls_credentials_start(client, timeout_ms, policy, out_handle)` 和 Server 对应函数预留槽后调用 typed provider start。
- `tr_client_tls_credentials_complete(client, handle, status, bundle)` 与 Server 对应函数只复制结果并标记 ready。
- `tr_client_tls_credentials_poll(client, handle, out_result)` 与 Server 对应函数在外部控制线程执行 PEM profile 构造和 D1 全 owner 安装；pending 时返回 `TR_AGAIN` 且不改输出。
- `tr_client_tls_credentials_cancel(client, handle)` 与 Server 对应函数发起单次 cancel；profile publish、cancel、deadline 的线性化点由操作表决定。

- [x] **Step 1: 测试 provider-only 创建及未就绪拒绝**

在 `tests/test_tls_provider.c` 创建缺少静态身份文件但配置 provider 的 TLS_REQUIRED Client/Server；验证 create 成功，未取到凭证前 Client connect、Server listen/start 返回 `TR_AGAIN`，输出端口/fd 状态不变。provider 配置缺 cancel 时 create 返回 `TR_ERR_INVALID`。

- [x] **Step 2: 测试异步首取、refresh、取消与结果 poll**

添加确定性 fake provider：start 只保存 request/handle；测试线程显式调用 complete。覆盖 complete 前 poll `TR_AGAIN` 且 output 不变、首取 generation 1、静态 profile refresh generation 加一、坏凭证保留旧 generation、取消终态 `TR_ERR_CANCELLED`、deadline `TR_ERR_TIMEOUT`、重复/迟到 complete `TR_ERR_STALE`、destroy 在 provider 未确认时返回 `TR_AGAIN`。

- [x] **Step 3: 实现 facade 配置复制与 provider-only 状态**

创建时深拷贝后续 profile 构造需要的固定 trust/name/client-auth 字符串。provider-only Server 不创建可接收连接的空身份 profile；未发布首代前 `listen/start` 返回 `TR_AGAIN`。Client 同样阻止 TLS_REQUIRED connect，不能将空 profile 解释为明文。

- [x] **Step 4: 实现 start/complete/poll/cancel 与 D1 发布**

start 先 reserve 凭证 bundle 容量再调用 provider；complete 只复制到预留结果槽；poll 在外部控制线程构造候选并调用现有 TLS rotation 屏障。publish 成功才更新 generation；rollback 失败沿用 D1 fail-closed/destroy retry 规则。deadline 用 Reactor timer 触发 provider cancel，但不在 timer 回调解析 PEM。

- [x] **Step 5: 验证公开 API 与旧静态路径**

运行 `test_tls_provider`、`test_tls_openssl`、`test_rpc_v2_tls_identity` 和 `test_connection_group_tls`。预期静态文件 TLS、不带 provider 的创建与轮换行为完全保留；provider-only 初始与更新场景通过。

### Task 5：Reactor 路由前导 defer/resume

**Files:**
- Modify: `src/execution/reactor_internal.h`
- Modify: `src/execution/reactor.c`
- Modify: `tests/test_pipeline_listener_tls.c`

**Interfaces:**
- preface feed 可在已消费全部固定前导后返回 `TR_IN_PROGRESS`，表示连接保留前导状态并暂停普通读取。
- 新增 owner-only `tr_reactor_preface_resume_on_owner(connection, status)`；`TR_OK` 释放前导并恢复读取，错误状态关闭连接并执行既有 release。

- [ ] **Step 1: 写 defer/resume 行为测试**

扩展 TLS listener 测试，让 preface callback 在完整 TRR1 后返回 `TR_IN_PROGRESS`；客户端立即发送后续帧。断言 resume 前无 frame callback，resume 成功后恰好交付一次，resume 失败关闭连接并恰好 release 一次。

- [ ] **Step 2: 运行专测确认当前实现失败**

```sh
TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" \
  xmake test -v -j1 'test_pipeline_listener_tls/*'
```

预期：现有 preface 路径把 `TR_IN_PROGRESS` 当作连接错误处理。

- [ ] **Step 3: 增加 deferred 状态和 owner 恢复 API**

连接槽增加 `preface_deferred`；deferred 时只保留 ERR/HUP 事件，不再读取 socket。resume 在 Reactor owner 验证精确 connection generation，成功时释放 preface handler、清除 read pause 并更新 epoll interest；失败时通过 `tr_reactor_abort_on_owner()` 收敛。

- [ ] **Step 4: 验证关闭、EOF 和槽位复用**

补测 deferred 连接 EOF/close、错误 resume、重复 resume 和连接槽复用后的旧 handle；运行 `test_pipeline_listener_tls` 与 `test_reactor_timer_deadline`，确认旧代次句柄不能恢复新连接。

### Task 6：CONTROL/DATA 远端异步授权

**Files:**
- Modify: `include/tr/transport.h`
- Modify: `include/tr/server.h`
- Modify: `src/server.c`
- Modify: `src/transport/group/pipeline_listener_internal.h`
- Modify: `src/transport/group/pipeline_listener.c`
- Modify: `tests/test_connection_group_tls.c`
- Modify: `tests/test_pipeline_listener_tls.c`

**Interfaces:**
- listener 配置接收 Server async authorize/cancel 回调、操作容量和 Server completion eventfd。
- `tr_server_connection_group_authorize_complete(server, handle, status, decision)` 可跨线程提交一次结果；owner eventfd handler 决定 commit/deny。
- `tr_pipeline_listener_accept_control/data()` 在同步授权时保持原路径；异步授权时保留 route、精确 connection generation、TLS/profile/identity 引用并返回 `TR_IN_PROGRESS`。
- listener 借用 `tr_server` 的同一操作表；auth 和 server credential refresh 共用 `max_pending_tls_operations`，不双重预留容量。

- [ ] **Step 1: 写远端 ALLOW/DENY/HOLD 测试**

在 `tests/test_connection_group_tls.c` 增加可控 async provider：CONTROL/DATA 分别 HOLD、ALLOW、DENY。HOLD 时发送 CONTROL/DATA 后续帧并断言业务回调未执行；ALLOW 后只交付对应帧；DENY/provider error 关闭连接且永不交付帧；HOLD 一条连接时另一条已授权连接仍能推进。

- [ ] **Step 2: 写取消与 ABA 回归**

覆盖 deadline、TCP EOF、facade drain、profile revoke、DATA reservation cancel，以及 cancel-before-complete / complete-before-cancel。旧句柄不能影响复用连接槽；late ALLOW 不得创建 session、激活 route 或调用业务 callback。

- [ ] **Step 3: 运行连接组专测确认红灯**

```sh
TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" \
  xmake test -v -j1 'test_connection_group_tls/*'
```

预期：async provider 类型或 completion API 尚不存在，新增场景编译失败。

- [ ] **Step 4: 为 listener 预留操作槽与 eventfd**

listener create 预分配不超过 `min(max_pending_tls_operations, max_connections)` 的授权槽，创建并注册 eventfd；每槽保留 connection/route/session/profile/identity generation。complete 只写结果和 eventfd；owner 批量 drain eventfd 后消费 ready 槽。

- [ ] **Step 5: defer CONTROL/DATA 前导并按 owner commit**

同步 authorize 保留兼容逻辑；若 async provider 存在，先运行同步本地 veto，再 reserve 操作槽并调用 async start。route parser 收到 `TR_IN_PROGRESS` 后 defer Reactor 前导。owner 收到 ALLOW 后重新校验连接、deadline、group epoch、CONTROL session、TLS generation，再创建 CONTROL transport 或 attach DATA route，最后 resume preface。DENY/error/失效则取消精确 reservation 并 abort connection。

- [ ] **Step 6: 验证真实 TLS 矩阵**

运行 `test_connection_group_tls`、`test_pipeline_listener_tls`、`test_tls_provider_operation`。预期同步授权测试全部不变；异步 HOLD 不阻塞其他连接，迟到 ALLOW 不进入应用层。

### Task 7：排空、取消与销毁重试

**Files:**
- Modify: `src/client.c`
- Modify: `src/server.c`
- Modify: `src/transport/group/pipeline_listener.c`
- Modify: `tests/test_tls_provider.c`
- Modify: `tests/test_connection_group_tls.c`

- [ ] **Step 1: 增加 destroy/drain 未确认 provider 测试**

让凭证与授权 provider HOLD cancel acknowledgement；验证 Client/Server/listener destroy 返回 `TR_AGAIN` 且对象、eventfd、操作参数仍有效。provider complete 后再次 destroy 成功；重复 destroy 不重复执行已成功的拆除步骤。

- [ ] **Step 2: 实现取消的单次通知与 retirement 顺序**

连接关闭、deadline、Server drain、TLS revoke-old 先把槽线性化到 cancel requested，再在锁外至多调用一次 provider cancel。provider ack 后由 owner 丢弃结果、释放精确 reservation/ref 并 retire；已取消 operation 的迟到 ALLOW 只作 ack。

- [ ] **Step 3: 验证 deadline、eventfd 和 rollback 失败路径**

故障注入 eventfd write 的 `EINTR`/饱和 `EAGAIN`；结果槽必须保留且可由 owner maintenance 轮次处理。凭证 rotation 的 install/rollback 故障沿用 D1 原测试，确认 generation 不提前发布且失败对象可重试销毁。

### Task 8：SDK 文档和消费端

**Files:**
- Modify: `tests/test_rpc_v2_sdk.c`
- Modify: `docs/rpc-v2-sdk.md`
- Modify: `README.md`

- [ ] **Step 1: 扩展安装 SDK 消费端**

在 `tests/test_rpc_v2_sdk.c` 引用新增的 credentials start/complete/poll/cancel 函数地址和 async authorization callback 类型；普通静态配置运行路径不启用 provider。确保这些符号从 `trcore` 可链接。

- [ ] **Step 2: 文档化生命周期与安全限制**

在 `docs/rpc-v2-sdk.md` 说明 provider callback 的非阻塞限制、bundle 长度和 4 MiB 默认值、外部控制线程 poll、合作式 cancel、句柄过期、静态 trust roots、TLS_REQUIRED 失败关闭与 SDK 链接方式。README 更新 TLS-D2 当前状态和 #234/TLS-E 未完成边界。

- [ ] **Step 3: 验证公共 C/C++ 头文件边界**

运行 `xmake test -v -j1 'test_rpc_v2_sdk/*'`。SDK 安装消费端没有独立本地脚本；由 `.github/workflows/ci.yml` 的 `build-smoke` job 执行安装、公开头文件边界检查及 GCC/Clang C11、G++/Clang++ C++11 链接运行验证。C11/C++11 消费端只包含已安装 `include/tr/*.h`，不得依赖 `src/` 私有头。该 job 的临时文件在 CI runner 临时目录；本地不复制该步骤到 `/tmp`。

### Task 9：全量验证与 PR 验收

**Files:**
- Modify only files required by failures discovered in Tasks 1–8.

- [ ] **Step 1: Release 全量测试**

```sh
TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" \
  xmake f -c -y -m release --toolchain=gcc
TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" \
  xmake test -v -j1
```

- [ ] **Step 2: GCC debug、Clang release、ASan/UBSan 和 TSan**

每种配置均设置工作区 `TMPDIR`/`XMAKE_TMPDIR`，执行：

```sh
TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" xmake f -c -y -m debug --toolchain=gcc
TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" xmake test -v -j1
TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" xmake f -c -y -m release --toolchain=clang
TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" xmake test -v -j1
TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" xmake f -c -y -m asan --toolchain=gcc
TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" xmake build --all
TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 xmake test -v -j1
TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" xmake f -c -y -m tsan --toolchain=gcc
TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" xmake build --all
TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" TSAN_OPTIONS=halt_on_error=1:second_deadlock_stack=1 xmake test -v -j1
```

- [ ] **Step 3: 格式、编译数据库与公开 SDK 验收**

运行 `python3 scripts/check_source_format.py --base origin/main --head HEAD`。对 GCC release 配置执行 `xmake f -c -y -m release --toolchain=gcc --crc32c_portable=n`，再运行 `python3 scripts/check_compile_commands.py --output-dir build/compdb/gcc-release --mode release --toolchain gcc --crc32c-portable n --arch x86_64`；Clang release 对应使用 `build/compdb/clang-release` 与 `--toolchain clang`。随后将分支推至 PR #312，等待 `.github/workflows/ci.yml` 中原有 GCC/Clang、ASan/UBSan、TSan、SDK C11/C++11 和格式检查对实际 head SHA 全部通过；不得改动必需检查名称。

- [ ] **Step 4: 更新 PR #312 并检查实际 head CI**

将计划与实现提交推到 `feat/v2-tls-d2-234`，PR #312 继续保持 draft 直到产品矩阵全部通过；记录各 CI 实际 head SHA、测试结果和未覆盖项。随后再处理 TLS-E 与 #243 剩余范围，不把 D2 PR 通过说成完整 V2 完成。

## 自审覆盖

- 凭证 fetch/complete/poll/cancel、provider-only 初始配置、固定策略、in-memory PEM 与 D1 发布：Tasks 1–4、8。
- CONTROL/DATA HOLD/ALLOW/DENY、前导 defer、精确 route/connection 代次、期限和迟到结果：Tasks 5–6。
- 有界容量、eventfd、取消确认、destroy retry、rollback：Tasks 2、6–7。
- C11/C++11 SDK、C/C++ 头边界、格式和五种既有 CI 模式：Tasks 8–9。
- #243 通用 Call/父子取消/跨 owner 环路和 TLS-E 发布证据仍是显式非目标；本计划不把它们标为完成。
