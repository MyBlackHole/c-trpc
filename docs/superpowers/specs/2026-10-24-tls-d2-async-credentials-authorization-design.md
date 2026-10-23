# TLS-D2：异步凭证与连接组远端授权设计

日期：2026-10-24
状态：设计稿，待用户审阅
关联：[TLS 总任务 #234](https://github.com/MyBlackHole/c-trpc/issues/234)、
[异步操作与取消 #243](https://github.com/MyBlackHole/c-trpc/issues/243)、
[TLS-D1 设计](2026-10-23-tls-d1-mtls-group-identity-design.md)、
[ADR-009](../../architecture/decisions/ADR-009-tls-byte-endpoint.md)。

## 1. 目标与阶段边界

TLS-D1 已提供 TLS 1.3 mTLS、URI SAN 身份、CONTROL/DATA 主体绑定、不可变
profile generation 和显式轮换。D1 的连接组授权钩子在 Server 所属 I/O 域同步执行，
profile 只从文件创建；它们适用于本地快速策略与静态部署，不能等待外部凭证服务或
远端授权服务。

TLS-D2 为这些外部操作提供应用可实现的通用异步 provider：

1. 从 Client/Server 控制流程显式启动凭证获取或刷新；初次凭证也可以由 provider 提供，
   不要求落盘。成功材料在非 Reactor 执行域校验并构造为不可变 TLS profile，再沿用
   D1 的全 owner 屏障发布。
2. 在连接组 CONTROL/DATA 完成 TLS 身份认证、但会话或 route 尚未提交时异步请求授权。
   未收到许可之前不得发送/接收 TRR1 后续业务、提交 CONTROL session 或激活 DATA route。
3. 对每个异步操作提供有限槽位、代次句柄、一次性完成、合作式取消和迟到结果丢弃。
   这实现 #243 为 TLS 所需的最小操作能力，不宣称 #243 的通用 Call 持有、父子取消、
   跨 owner 环路和全部回收矩阵已经完成。

D2 不新增凭证厂商适配，不在每个 accept 上访问外部凭证服务，不增加 TLS 专用线程池，
不改变线协议、自动重试或明文降级策略。OpenSSL/内核完整 RSS 上界、握手洪峰、长期
负载、外部 TLS 实现互通和公网发布门槛仍属于 TLS-E。

## 2. 现状与约束

- `tr_tls_client_config`、`tr_tls_server_config` 目前通过文件路径加载身份材料；
  `src/io/tls_openssl.c` 在 profile 构造期间调用文件加载 API。
- `tr_client_tls_update_profile()`、`tr_server_tls_update_profile()` 是外部控制线程上的
  同步操作，已经提供暂停准入、跨 owner 安装、回滚和 generation 递增。
- `tr_connection_group_authorize_cb` 在单个 Pipeline listener 的 Reactor owner 上同步执行；
  CONTROL/DATA 的身份视图只在回调期间借用。
- Reactor 有跨线程唤醒与辅助事件入口，服务端现有工作执行器有有限槽位、取消令牌和
  owner 完成交付；它只覆盖当前记录 RPC 用途，没有公开的 provider 操作能力。
- `tr_task_executor` 的取消是合作式；取消不释放仍在工作的参数，owner 静默和完成交付
  是分开的生命周期屏障。D2 必须保留这些语义，不强杀应用代码。
- 已接受的远端操作不能依赖普通有界 command 队列里“以后再试”才得到终局；完成槽位、
  operation 状态和唤醒责任必须在调用 provider 前保留。
- 身份校验、TLS_REQUIRED 不降级、CONTROL/DATA 的主体与 generation 绑定，以及旧 profile
  在 session/Call 引用归还前保留，沿用 D1 合同。

凭证材料使用带长度的内存视图。OpenSSL PEM/BIO API 可从内存输入构造证书和私钥；
私钥输入副本在解析后清理，`SSL_CTX`/OpenSSL 内部对象仍按 profile 引用寿命释放。
这不代表 OpenSSL 内部或内核缓冲已经纳入硬 RSS 预算。
参考：[OpenSSL `PEM_read_bio_PrivateKey`](https://docs.openssl.org/3.6/man3/PEM_read_bio_PrivateKey/)、
[OpenSSL `BIO_s_mem`](https://docs.openssl.org/3.6/man3/BIO_s_mem/)。

## 3. 方案比较与选择

### 方案 A：应用通用异步 provider + 有界操作槽（采用）

provider 启动外部 I/O 后立即返回；外部线程以一个带 instance/slot/generation 的值句柄
提交结果。库先预留 operation 与 completion 容量，再调用 provider；完成只写入该操作的
预留结果槽并唤醒 owner，不在 provider 线程上运行 Reactor、RPC 或应用回调。凭证 profile
构建交给 #243 的共享有限执行域，完成后通过 D1 多 owner 屏障发布。现有 `task_executor`
作为基础能力扩展/复用，不再为 TLS 建一套线程生命周期。

优点是支持事件循环式 secret store 和授权服务、没有外部等待占用 Reactor、凭证和授权
共享同一取消/代次/完成模型。代价是需要把 #243 的操作完成能力补到足够支撑 Client、
Server 和连接组；D2 不扩展为一次性完成全部 #243。

### 方案 B：库在工作线程执行应用同步 provider

把阻塞式取凭证/授权函数提交到库线程池。实现入口直观，但同步外部调用可占满所有工作者，
协作式取消无法终止不合作的网络调用，Client 与 Server 还可能各自引入线程/预算域。
这既难与已有执行器和 #243 的统一操作责任保持一致，也不能让原生异步 provider 直接
接入，因此不采用。

### 方案 C：应用自行取凭证，继续同步更新和同步授权

应用在库外取得证书后调用现有同步 profile 更新。它避免新操作 API，却不能覆盖 D2 的
异步远端授权，也无法由库确认连接关闭、期限和 profile 代次是否取消了准入，因此不采用。

## 4. 组件与操作状态

### 4.1 通用有界操作能力

在 #243 已有取消/完成语义上增加可外部完成的有限 operation slot。公开句柄是值类型，
包含 facade instance、slot index 和单调 slot generation；不暴露 Reactor、fd、裸 session
或可脱离 Client/Server 生命周期的指针。句柄代次耗尽时拒绝新操作，不回绕复用。

操作状态至少区分：

```text
FREE -> RESERVED -> ACCEPTED -> COMPLETION_READY -> OWNER_RETIRED -> FREE
                                \-> CANCEL_REQUESTED -> COMPLETION_READY
```

- `reserve` 在回调前取得一个操作槽、完成记录、结果容量和 owner 唤醒责任。容量满或所需预算
  无法预留时返回 `TR_AGAIN`，不调用 provider、不借走输入、不创建半准入 session/route。
- 操作在 provider `start` 被调用前转为 `ACCEPTED`。start 是 `void` 通知；它不能拒绝一个已接收
  的操作。若无法启动外部工作，provider 必须立刻以错误结果调用 `complete`。这样公共发起 API
  的 `TR_OK` 表示操作已接受，`TR_AGAIN` 表示尚未接受，且不存在“provider 已启动、调用方却以
  未接受重试”的歧义。provider 可在其事件循环或工作线程上继续运行；
  start/cancel 回调只启动/通知，不得阻塞、分配无界工作、重入同一 Client/Server 生命周期
  操作或直接修改 Reactor/OpenSSL/route。
- `complete` 可跨线程调用；它只校验句柄、线性化一次完成、将结果复制到准入时预留并计费的
  有界结果槽、唤醒 owner，不内联执行应用回调或协议状态转换。成功复制后 provider 的输入
  缓冲即可由 provider 清理；对重复/过期句柄返回 `TR_ERR_STALE`。同步 complete 与 cancel
  竞争由同一操作锁/原子状态确定唯一先胜者；合法的首次 completion 不会因队列满或后置分配
  失败而丢失。
- cancel 是合作式通知，不证明 provider 已停止。若 provider 尚未 complete 且取消与 `start`
  并发，库先记录取消请求，待 `start` 返回后至多调用一次 cancel；provider 随后必须 complete
  一次作为收敛确认，操作槽及其 `arg`、身份/profile 引用才可退休。若 provider 已 complete，
  取消只等待库侧 owner/build/rollback 责任收敛，不再要求 provider 二次确认。不能强杀线程或因
  cancel 请求提前释放 provider 仍使用的数据。
- cancel 先胜时，任何之后的成功凭证/授权结果只作为 provider 收敛确认，不能发布 profile、
  放行连接或复活旧 generation。completion 先胜也仍须在 owner 重新验证 deadline、连接代次、
  operation generation 与 TLS profile generation。
- 重复、已退休或跨 facade 句柄返回 `TR_ERR_STALE`；句柄格式错误返回 `TR_ERR_INVALID`。
  过期操作不会写入任何 output 或共享状态。
- owner 关闭期间保留已接受操作和 wake fd；销毁遇到未完成 provider 返回 `TR_AGAIN` 并保留
  对象，provider 确认后才能重试销毁。不能把未确认取消等同为完成。

操作表由 facade 的显式 `max_pending_tls_operations` 上限和运行时 memory budget 限制；
连接组授权不能超过 listener 的 `max_connections`。每个 Client/Server 同时只允许一个
profile refresh；并发 refresh 返回 `TR_AGAIN`。凭证操作准入时按配置的 bundle 字节上限预留
结果容量，授权操作只预留固定大小结果；所有容量均按 facade budget 计费，预算不足时在调用
provider 前返回 `TR_AGAIN`。超过 bundle 上限或结构不合法的首次 completion 被记为该操作的终态
错误，并释放预留容量；它不能要求 provider 再猜测是否需要补发。完成写入每个已接受操作预留的
槽位，因此普通命令队列满不丢终局；
eventfd 唤醒失败时仍保留结果，并用既有 owner 维护/恢复路径有界重试。

D2 在 facade 配置中新增操作槽上限，建议默认 64；凭证 bundle 字节上限建议默认 4 MiB。两个值
都可显式调整，但不能突破 facade memory budget；Server 授权操作的实际并发数还受连接组
`max_connections` 限制。设操作槽上限为 0 表示不启用异步 provider，不影响既有静态 profile 和
同步授权。

凭证刷新是公开异步控制操作：调用方提供有限、非零 `timeout_ms` 与 D1 轮换策略，并取得
operation handle；
成功准入后可用对应 Client/Server 的 poll API 查询状态，也可请求取消。超时值转为单调时钟
绝对 deadline，溢出时拒绝操作。未完成时 poll 返回 `TR_AGAIN` 且不改输出；完成时一次 poll
本身返回 `TR_OK`，并在结果结构中给出终态错误码和已发布 generation，然后退休该刷新槽。终态
取消使用新增的 `TR_ERR_CANCELLED`，超期使用 `TR_ERR_TIMEOUT`。请求取消后，poll 继续返回
`TR_AGAIN`，直到 provider 确认且库侧 profile build/rollback 责任收敛；如果 provider 已 complete，
则只等待库侧责任收敛。成功 destroy 可丢弃尚未读取的终态，但不得丢弃仍未由 provider 确认的
操作。连接授权操作没有应用侧结果 poll；其结果只在对应连接 owner 中消费。具体 C 符号名属于
实施计划，以上线程、状态和输出语义是本设计合同。

### 4.2 provider 合同

配置包含两个互不混淆的 provider 入口：

- `fetch_credentials(request, operation_handle, arg)`：`void` 通知应用异步取得一个身份材料快照；
- `authorize_group(request, operation_handle, arg)`：`void` 通知应用异步许可/拒绝一次 CONTROL 或 DATA
  加入。

两个 provider 都必须提供 `cancel(operation_handle, arg)`；它是返回 `void` 的合作式通知。
provider start 也是返回 `void` 的一次性通知；
一次 start 被调用就代表操作已接受，若外部工作无法启动，provider 仍通过一次带错误状态的
completion 结束操作。start 可在返回前
同步 complete；取消不能在 start 返回前调用 provider cancel。provider 可使用应用自己的事件循环
或受限线程池；库不规定 vendor、网络协议或密钥托管方式。start/cancel 不得阻塞、访问或保存
借用视图地址；需要后续使用的 URI、group id、generation 等由 provider 自行复制。start 与
cancel 之间可并发发生 complete，provider 参数须并发安全，并在 Client/Server 成功销毁前保持
有效。provider completion 对凭证操作提交凭证 bundle 或错误状态；对授权操作提交 ALLOW、DENY
或错误状态。有效句柄的首次 completion 总会终结 provider 一侧，即使其 payload 超限/非法也会
以错误状态结束；completion 返回后 provider 不得再读取输入 payload。异步请求只接受带长度的
数据，禁止依赖 NUL 终止或隐式大小。若操作在成功发布/授权前由库取消，provider cancel 后
仍须用一次 complete 确认其不再访问请求/arg；这时成功 payload 被丢弃，用户侧终态采用库的
取消原因，而不是 provider 给出的授权/凭证结果。

凭证结果是单一 bundle：服务身份的完整 PEM 证书链、PEM 私钥和可选私钥口令。信任根仍由现有
静态 Client CA 配置、Server `client_ca_file` 或系统信任库提供；D2 provider 不得替换信任根。
bundle 总字节数受配置的有限上限约束，初始建议 4 MiB；该上限也作为准入时预留结果容量的
计费上限。Client 的 expected peer name/SNI、Server 的 client-auth policy、信任根、算法策略等
继续由 facade 配置控制，不能被 provider result 改成 TLS_DISABLED 或放宽认证。provider 结果
只能给既定策略提供本地身份材料。

授权结果只有 `ALLOW` 或 `DENY`；超时、provider 错误、格式错误、资源拒绝、句柄过期均按拒绝
关闭处理，不把异常映射成 allow。已有同步 `authorize` 回调若同时配置，先作为本地 veto 执行：
它必须返回 `TR_OK`，随后仍须等异步 provider 的 ALLOW；同步拒绝时不调用远端 provider。只配置
一种授权入口时执行该入口；Server 至少须配置一种授权入口。现有 Server 校验“连接组启用时
authorize 非空”的规则相应改为“同步 authorize 或异步 authorize provider 至少配置一个”。
若配置异步 provider 但操作槽上限为 0，则配置校验失败。授权请求携带 group id/epoch、
CONTROL/DATA role、已验证的 peer URI 身份、DATA 对应的 CONTROL 身份、TLS generation 和绝对
deadline。身份数据只在
provider start 回调借用有效；库自己在操作期间 retain 所需快照。

### 4.3 初次凭证、更新与轮换

静态文件配置继续可用。应用也可以在 TLS_REQUIRED 下不配置初始身份文件、配置 provider，
创建处于 `WAITING_CREDENTIALS` 的 Client/Server，再显式启动首次凭证获取。没有可用 profile
时，Client connect、Server listen/start 和连接组 listener listen 均返回 `TR_AGAIN`，且不改变
连接、监听或启动状态，也不先打开 socket。
首次 bundle 校验和 profile 安装成功后，generation 从 0 发布为 1，facade 才可开始 I/O。

后续 refresh 使用同一流程：

1. 在预留槽后调用 fetch provider；deadline、facade 销毁或显式 cancel 都请求取消；同一 facade
   同时只允许一个刷新，因此不会接受替换/并发请求。
2. provider completion 送入非 Reactor profile-build 域。验证材料长度、PEM/链、用途、私钥
   匹配和配置策略；构造失败只丢弃候选，不改变当前 snapshot。
3. 候选通过 D1 全 owner pause/install/rollback 屏障。全部 owner 接受后一次性发布 generation
   并恢复准入；任一失败则在 gate 关闭期间恢复旧 profile，旧 generation 保持不变。
4. 成功/失败对发起者可观察；输出 generation 只在全局发布成功后写入。generation 发布是
   refresh 是否成功的线性化点；取消/deadline 先赢时不得发布。旧 session 和 Call 按 D1 引用
   规则继续持有旧 snapshot，`REVOKE_OLD` 仍使用 D1 的 owner close/quiescence 屏障。

provider-only 初次创建和 profile refresh 是显式操作，不会在每个 TCP accept 或每次普通
RPC 上暗中访问外部服务。调用方可按自己的密钥轮换计划主动刷新；D2 不增加自动定时轮换。

## 5. CONTROL/DATA 异步授权流程

1. TLS endpoint 完成证书链/EKU/有效期/URI SAN 校验后，Reactors 才读取 TRR1；明文或无身份
   连接仍不能进入 TLS_REQUIRED 组 listener。
2. CONTROL route 已通过现有结构和 generation 校验后，owner 预留精确连接/session/operation
   槽，再调用 `authorize_group`。在 ALLOW completion 前，session 不进入可加入的活动表。
3. DATA 首先校验当前 CONTROL、route/member、server TLS generation 和主体连续性；再预留它
   自己的 route/operation 槽并发出异步授权。ALLOW 后 owner 重新校验所有版本，再提交 DATA
   route；不能只凭早先检查的结果提交。
4. owner 完成前，连接、peer identity snapshot、TLS profile snapshot、group/epoch 与精确 route
   reservation 全部保持引用。provider 只看到 request view，不获得内部结构指针。
5. DENY 或 provider 错误拒绝该连接。CONTROL 释放自己的候选 session；DATA 只取消自己的
   精确 reservation，不改变 CONTROL 或其他 DATA。失败不得调用数据回调或暴露后续业务帧。
6. 操作完成时重新检查：连接句柄/代次仍活动、绝对握手 deadline 未过、route reservation
   未取消、CONTROL session 仍相同、profile generation 仍符合策略、facade 未 drain/destroy。
   任一不匹配时丢弃结果并释放本操作责任。

对旧 generation 的 `REVOKE_OLD`、连接关闭、TLS/route deadline、Server drain/destroy 都取消
其挂起的认证。`GRACEFUL` 不取消已绑定 generation 的现有 session，但不允许 completion
把旧 profile 的请求误提交到新 profile。异步授权只延长准入前的等待，不延长原始单调 deadline；
不重置 deadline，不重连重试，不重放 RPC 或 CONTROL/DATA 请求。

## 6. 错误、关闭与安全边界

- 增加 `TR_ERR_CANCELLED` 作为可观察的终态状态码；`TR_ERR_TIMEOUT` 表示原始 deadline 已过。
  poll 函数自身成功读取终态时仍返回 `TR_OK`，详细操作状态写入结果结构；准入失败直接返回
  `TR_AGAIN` 或 `TR_ERR_INVALID`，不会创建 operation handle。
- async provider 缺失时沿用已有同步本地授权与静态 profile 语义；TLS_REQUIRED 不能因 provider
  错误、超时、容量满、PEM 无效或异步取消切到明文。
- 对发起异步操作的 API，`TR_AGAIN` 只表示尚未接受：操作槽未保留、provider 未调用、调用方
  继续持有输入。对 poll API，`TR_AGAIN` 表示已接受但尚无终态。操作一旦 ACCEPTED，必须最终走
  一次成功/拒绝/取消确认；不能静默丢失 completion。
- deadline、连接先关、Server drain 先取消时，取消通知恰一次。迟到成功只释放/清理结果，不
  改变 protocol state、不创建 CONTROL、不激活 DATA、不向 RPC work/Message callback 交付。
- profile build 的输入/候选归操作所有；失败 rollback 不释放仍被 owner 使用的 profile。服务
  对象在 provider 确认和 owner 完成槽 retirement 前不得成功 destroy。
- TLS profile 更新参数不得改变 `mode=TR_TLS_REQUIRED`、客户端期望主机、客户端证书验证用途、
  Server `client_auth` 必需性或 D1 generation 不回绕规则。
- 私钥 PEM/口令的库内临时副本在解析/失败清理时清理；provider 原始缓冲区由 provider 拥有，
  到 `complete` 返回前必须保持有效，随后由 provider 自己清理。OpenSSL 派生对象照常由 profile
  引用管理；D2 不声称完整清零 OpenSSL 私有副本或硬性限制进程 RSS。
- 完整 SSL_CTX 解析、证书链建立、profile 安装不能运行在 Reactor 回调里；远端 I/O 不能运行在
  Reactor callback 或 OpenSSL lock 里。

## 7. 验收矩阵

### 有界操作内核（#243 的 TLS 所需部分）

- 启动前 capacity 满、operation generation 最大值、重复 start/complete/cancel、跨 facade
  句柄、cancel 与 complete 两种先后顺序、complete 与 owner close 并发。
- provider completion queue/普通命令队列满、eventfd 写失败、operation slot 重用、provider
  不确认 cancel 时 destroy 返回 `TR_AGAIN` 并保留对象；确认后 destroy 重试成功。
- completion 只发布一次；已取消操作永不恢复准入；接受的操作没有静默丢失终局；句柄不回绕。

### 凭证与 snapshot

- provider-only Client/Server 首次启动成功；无凭证时 connect/listen/start 不变更状态。
- 正确链与匹配私钥可安装；错链、错私钥、格式错误、超长材料、预算失败保留旧 profile 与
  generation，且无文件落地。
- 多 owner install 中途失败完整 rollback；rollback 失败 fail-closed 并可重试销毁；并发/迟到
  refresh 不覆盖较新 generation。
- graceful 与 revoke-old 与 D1 已有 session 引用组合验证；旧代次 completion 不能给新 group
  供凭证或授权。
- C11/C++11 SDK consumer 编译并运行新 public API；旧文件配置与静态 D1 路径继续通过回归。

### 远端授权与真实连接

- CONTROL/DATA 异步 ALLOW、DENY、provider error、操作队列满、deadline、TCP EOF、facade drain、
  profile revoke、DATA 单条 reservation 取消。
- 每种关闭/超期/代次变化分别安排 completion-before-cancel 和 completion-after-cancel；迟到
  ALLOW 不能调用任何数据/RPC callback。
- HOLD 住 provider 时验证同 Server 上其他已授权连接和既有流仍能推进；释放后只处理精确
  connection/route generation，不能跨连接唤醒或串线。
- 真实 loopback TLS 1.3/mTLS 测试覆盖证书链/EKU/SAN、CONTROL/DATA 身份、错误授权和 no-downgrade。
- 保留 D1 全量矩阵，并为新操作增加 GCC/Clang、ASan/UBSan、TSan、固定 GCC 11/Clang 14 静态
  分析、格式、SDK 安装和实际 PR/main CI。所有本地临时路径设到工作区 `build/`。

## 8. 非目标与后续

- 不提供具体 Vault/KMS/cloud-secret-manager 插件；应用实现 provider。
- 不在每个 accept 自动刷新证书；自动 refresh cadence、缓存和预取由后续运维契约决定。
- 不提供跨连接重放、自动重试、服务选路或 TLS 0-RTT。
- 不关闭 #243：通用可持有 Call、父子 deadline、跨 owner A→B→A、自等待证明和全部组件回收仍按
  #243 独立验收。
- 不关闭 #234：TLS-E 的完整资源账本、并发握手/取消洪峰、长期负载、最低目标 OpenSSL 环境、
  外部实现互通、目标平台和公网安全门槛仍未验收。

## 9. 设计自审

- **完整性：** 凭证、授权、取消、期限、owner completion、generation、回滚、销毁重试均有明确定义和测试要求。
- **安全：** 所有负面结果 fail-closed；认证/授权先于 route/session 与业务交付；metadata 不能替代 TLS 身份。
- **资源：** 外部操作有有限槽位，凭证字节按配置上限和现有 budget 计费；未退休 provider 操作阻止销毁。
- **边界：** 只实现 TLS-D2 所需 #243 操作能力；不把 `task_executor` 自测、TLS-D1 CI 或 R17 历史性能报告当成 D2 证据。
- **需审阅的产品参数：** 异步凭证 bundle 默认总上限暂提议 4 MiB；配置仍须受 facade budget 限制。此上限如不适合真实 CA/链大小，应在实施计划前由用户审阅调整。
