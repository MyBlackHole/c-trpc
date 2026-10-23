# TLS-D1：mTLS 身份与连接组绑定设计

日期：2026-10-23  
状态：按 #234 与 ADR-009 的已接受安全目标制定；TLS-D1 是 TLS-D 的第一切片。

## 1. 目标与阶段边界

TLS-C 已把普通 V2 Client/Server 的服务端认证 TLS 1.3 接入 Reactor。TLS-D1
补齐双向证书认证、经过验证的应用身份、普通 RPC 的身份上下文，以及
CONTROL/DATA 连接组的主体绑定和配置快照轮换。它不得把未认证连接标成可信，
也不得在 TLS、路由或授权失败后继续进入 Parser、RPC 或数据回调。

TLS-D 按依赖拆成：

1. **TLS-D1（本设计）：** 静态 mTLS 配置、对端 URI SAN 身份、普通 V2 身份
   上下文、CONTROL/DATA 授权绑定、不可变 TLS 配置代次、显式轮换与失败回滚。
2. **TLS-D2（后续）：** 外部异步凭证获取和远端授权，依赖 #243 的有界、可取消
   操作内核；迟到完成不得复活已取消/超期的准入或使用旧配置代次。

TLS-E 的 OpenSSL/内核完整资源总账、握手洪峰、长期负载、外部实现互通和公网
安全门槛仍在 TLS-D 之后。完成 TLS-D1 不关闭 #234。

## 2. 现状与必须跨越的边界

- `tr_tls_client_config` 已有客户端证书/私钥字段，`tr_tls_server_config` 已有
  `client_ca_file` 与 `client_auth`；TLS-B 目前明确拒绝这些 mTLS 配置。
- TLS-C 的 Reactor TLS adopt 目前拒绝同时带 preface 的接管；连接组仍由明文
  TRR1 preface 进入 pipeline listener。
- pipeline listener 的 CONTROL 授权回调当前只收到自报 route；DATA 校验现有
  group/epoch/member 代次，但没有经过 TLS 验证的主体。
- 普通 RPC 的 OPEN work 收到 metadata 和 body，但 `tr_rpc_v2_request` 当前没有
  连接级认证身份。
- group authorize 与 message/event 回调运行在所属 I/O 域；同步等待外部 I/O 会
  阻塞 Reactor，TLS-D1 的回调只允许有界、非阻塞的本地策略判断。

## 3. 方案选择

### 方案 A：比较 CONTROL 与 DATA 的叶证书是否完全相同

实现简单，但正常证书轮换会使同一服务的新 DATA 证书与旧 CONTROL 不等；它也
把证书密钥误当成稳定业务主体。此方案不采用。

### 方案 B：验证稳定主体 URI，并按连接角色执行组策略（采用）

TLS 后端先验证证书链和证书用途，再从证书中读取稳定主体 URI。服务策略分别
授权 CONTROL 与 DATA；DATA 还必须与活动 CONTROL 的主体完全相等，并通过已有
group/epoch/member 代次校验。证书可以换钥，只要受信配置和稳定 URI 主体仍有效。
该方案可通过异步策略阶段扩展，而不把阻塞工作放进 Reactor。

### 方案 C：CONTROL 下发签名 DATA 一次性票据

可以独立授权成员和短期有效期，但需要新增票据格式、签名密钥管理、重放缓存、
跨版本处理和票据撤销机制。当前已有 CONTROL 路由成员代次可复用，因此暂不引入
第二套凭据协议。

## 4. 对端身份模型

### 4.1 验证来源

连接身份只能来自 TLS 握手后由本地配置的 trust store 验证成功的叶证书。TRR1
中的 group、epoch、member、地址和任何 RPC metadata 都是对端输入，单独不能
证明身份。相同 CA 签发也不代表相同服务主体。

需要在普通 mTLS 及连接组中使用的证书必须且只能含一个 URI SAN。该值是应用
定义的稳定主体标识，按 ASN.1 IA5 字节精确比较，长度为 1–512 字节，不做
大小写折叠、URI 重写或 Common Name 回退。证书主体/URI 的含义由应用授权策略
管理；库只验证链、用途和 TLS 名称规则，不推断租户、组或权限。

客户端仍按现有 `expected_peer_name` 验证服务端 DNS/IP SAN；连接组额外记录
经验证的服务端 URI 主体，并要求 CONTROL 与每条 DATA TLS 会话的 URI 主体一致。
服务端对客户端证书执行 clientAuth 用途验证；连接组要求 `client_auth` 为
`TR_TLS_CLIENT_AUTH_REQUIRED`，并要求客户端 URI SAN 主体存在。

普通 Server 的 `client_auth` 语义：`NONE` 不请求客户端证书；`OPTIONAL` 接受
未提供证书的连接（认证等级为 none），但若对端提供证书就必须验证链、用途和
唯一 URI SAN；`REQUIRED` 缺证书、无 URI SAN、重复 URI SAN 或验证失败时关闭
连接。连接组 listener 只能使用 `REQUIRED`。

### 4.2 RPC 身份可见性和生命周期

新增公共 `include/tr/peer_identity.h`，定义借用值类型：

```c
enum tr_peer_identity_authentication {
        TR_PEER_IDENTITY_NONE = 0,
        TR_PEER_IDENTITY_TLS_CERTIFICATE = 1
};

struct tr_peer_identity_view {
        enum tr_peer_identity_authentication authentication;
        const uint8_t *uri_san;
        uint32_t uri_san_len;
};
```

内部 `peer_identity_internal.h` 定义不可变 refcount 对象及接口：
`tr_peer_identity_snapshot_create(uri, len, &out)`、
`tr_peer_identity_snapshot_retain(identity)`、
`tr_peer_identity_snapshot_release(identity)`、
`tr_peer_identity_snapshot_view(identity, &view)`。只有 `uri` 长度 1–512 且
内容不含 NUL/ASCII 控制字符时允许创建；retain 溢出返回 `TR_ERR_STATE`。

无证书连接必须给出 `NONE` 和空 URI；经过证书链与用途校验后给出
`TLS_CERTIFICATE` 和已解析 URI。此视图不含 OpenSSL 类型，不代表调用凭证，也不
延长对象寿命。

给 `tr_rpc_v2_request` 增加连接级 peer identity 视图。Server 的每个 OPEN、MESSAGE
和 HALF_CLOSE work event 都收到同一 Call 所持身份；明文连接和无证书的 optional
mTLS 连接显式给出 `NONE` 与空 URI。视图地址仅借用至当前 work 返回；RPC metadata
仍是调用方提供的数据，不能覆盖或提升连接身份。Server 将 identity view 放在
work callback 的栈上，底层不可变 identity snapshot 由 Call 引用到所有 work、
producer 和 terminal 责任静默；应用若需在回调后保存主体，应在回调内自行复制。

peer identity 使用有界、不可变、引用计数对象。Server 在 TLS 握手后创建身份，
连接持有一份引用；每个已接受的 Call 在 OPEN 准入时持有引用，直到该 Call 的
业务工作与终局回调静默后释放。因此 TCP/TLS 连接关闭后，已接受的异步业务仍能
读取其 OPEN identity 视图，不会解引用已释放的 TLS session。引用和 URI 字节计入
对应 Server/Call 资源预算；超过 512 字节直接拒绝准入。

## 5. 握手、路由和 RPC 的顺序

### 5.1 Server CONTROL/DATA

```text
TCP accept
  -> TLS 1.3 handshake + 双向证书验证
  -> 提取唯一 peer URI SAN
  -> 读取并验证 TRR1
  -> CONTROL：本地 authorize(peer, group, CONTROL) -> 建立 session
     DATA：校验 group/epoch/member 代次
           -> peer URI 必须等于活动 CONTROL URI
           -> 本地 authorize(peer, group, DATA) -> 交接 DATA
  -> 只在上述步骤成功后启用后续 Channel/数据处理
```

TLS 认证前不得读取或调用 TRR1 preface callback。Reactor 必须支持一个连接先由
TLS endpoint 完成握手、再将解密字节交给既有定长 preface parser；不能先读明文
TRR1 再升级 TLS。CONTROL 准入成功后，session 保存 peer identity 引用和本地
TLS 配置代次。DATA 认证或绑定失败只拒绝该物理 DATA 连接并撤销该 route reservation，
不能污染已建立的 CONTROL session 或错误释放其他 member。

在 `transport.h` 定义 `enum tr_connection_group_role`，取值为 CONTROL=1、DATA=2；
以及 `struct tr_connection_group_authorization`，字段为 `struct
tr_connection_group_id group`、`role`、`peer_identity` 和 `control_identity`。
CONTROL 回调中的 control identity 必须为空；DATA 回调中的 control identity 是
该活动组已经授权的主体。把公开配置中的 authorize 回调改为
`int (*)(const struct tr_connection_group_authorization *, void *)`。回调成功不替代
listener 自己做的 route/member/代次校验。回调不得访问未验证的证书字段或阻塞
等待其他 I/O 域。

Client/Server TLS profile 更新函数只能从外部控制线程调用；owner、RPC work 或
message/event callback 中调用返回 `TR_ERR_STATE`。另一个更新正在进行时返回
`TR_AGAIN` 且不启动第二个代次。若在任何 owner gate 关闭前因队列满而退出，候选
snapshot 由 API 内部释放，调用方可重新调用；若 gate 已建立，API 必须完成成功
发布或完全回滚后才返回。回滚屏障本身失败时 API 保留 Client/Server 所有权并返回
生命周期错误；调用方只能重试 destroy 收敛，不得继续连接/监听或启动新的轮换。

### 5.2 Client CONTROL/DATA

连接组 Client 必须在 mTLS 认证完成后才发送 TRR1。CONTROL 成功后缓存其已验证的
Server URI 主体。每条 DATA 在加入组前完成独立 TLS 握手，并要求 Server URI 主体
与 CONTROL 完全相等；不等时关闭 DATA 并撤销对应 route，不发布 READY 或发送业务
数据。Client 持有连接组所用的本地 TLS 配置快照，使同一活动组不会将新旧客户端
证书混在一个 group epoch 内。

### 5.3 握手期限和关闭

从 TCP accept/connect 完成开始的原单调绝对期限覆盖 TLS 握手、URI 提取及 TRR1
路由前导，不因 WANT、分片、profile 轮换或部分输入重置。期限到达时由原 Reactor
owner 关闭精确连接并归还 route、peer identity 和 profile 引用。TLS `close_notify`
仍受 TLS-C 的有界关闭规则约束，不代替 RPC FINAL 或连接组排空。

## 6. 不可变配置快照与轮换

TLS profile 升级为不可变、带本地递增代次的 refcount snapshot，至少持有 SSL_CTX、
信任材料的自管副本、身份提取规则和代次。每个 session 保留其 profile snapshot，
不只保留 SSL_CTX 的隐式 OpenSSL 引用；任何已发布 identity 和授权决策引用都不得
提前释放。

`tr_client` / `tr_server` 增加显式 TLS profile 更新入口：

```c
enum tr_tls_rotation_policy {
        TR_TLS_ROTATE_GRACEFUL = 0,
        TR_TLS_ROTATE_REVOKE_OLD = 1
};

int tr_client_tls_update_profile(
        struct tr_client *client,
        const struct tr_tls_client_config *config,
        enum tr_tls_rotation_policy policy,
        uint64_t *out_generation);
int tr_server_tls_update_profile(
        struct tr_server *server,
        const struct tr_tls_server_config *config,
        enum tr_tls_rotation_policy policy,
        uint64_t *out_generation);
```

更新入口只能用于已经以 TLS_REQUIRED 创建的门面，不能运行时切换回明文；
`out_generation` 只在整个更新屏障成功时写入。

1. 在调用方线程读取、校验、加载 CA/证书/私钥并构建完整新 snapshot；禁止在
   Reactor callback 中读取文件或解析证书。
2. 任一构造或校验失败时释放候选 snapshot，现行 snapshot 和代次保持不变。
3. Client/Server 可能拥有多个 Reactor owner。更新流程先在所有 owner 建立“暂停
   新 TLS 连接准入/接管”的屏障，再向每个 owner 安装候选 snapshot。所有 owner
   安装成功后一次性开放新连接准入；任一 owner 失败时，在屏障仍关闭期间将已
   安装的 owner 全部恢复到旧 snapshot，再开放准入。这样不会出现半数分片使用
   新配置、半数使用旧配置的成功发布，也不会让候选配置下建立的 session 在失败
   回滚后继续留存。已建立的 SSL session 保持旧代次直至其所有者引用全部释放。
   暂停、安装或回滚命令自身失败时，必须保留旧/候选 snapshot 及未收敛 owner 的
   所有权，保持新连接 gate 关闭并返回可重试的生命周期错误；不得报告成功或释放
   仍可能被 owner 使用的对象。
4. Client group 在整个 CONTROL/DATA 生命周期固定其本地 snapshot；轮换只影响新
   group。服务端 Control session 保存接受时的 server snapshot generation；新 DATA
   必须在同一 server generation 下完成认证并匹配相同主体。若 listener 已轮换，
   旧 group 不再新增 DATA，必须排空并以新 generation 重建；已有 DATA 可正常收敛。
5. 提供显式 revoke-old-generation 方式：暂停新 TLS 接管后，各 owner 关闭被撤销
   代次上的活动连接，再开放新连接；旧连接通过既有 connection-close 与 Call
   quiescence 规则收敛。graceful rotation 则允许旧连接自然排空。两个模式都不得
   释放仍被 session/Call 引用的 identity 或 TLS profile。Profile 代次单调递增，
   达到 `UINT64_MAX` 时拒绝后续轮换，不能回绕复用。

回调函数及其 `arg` 在 Server/listener 生命周期内仍由应用拥有并保持不变；本切片
不提供动态替换授权回调。调用方若需要远程策略或可轮换授权快照，使用 TLS-D2。

## 7. 失败和错误边界

- 缺失/错误 client certificate、错误 CA、错误 EKU、URI SAN 缺失/重复/超长：TLS
  握手或认证步骤失败，关闭该连接，不调用 preface、OPEN、work 或 group 数据回调。
- CONTROL 未授权：释放其候选 session 和连接资源；不得创建可被 DATA 加入的组。
- DATA 的主体、group、epoch、member index/generation 任一不匹配：只拒绝该 DATA
  连接，精确取消其预留，不改变另一个有效 DATA 的亲和或 CONTROL 身份。
- 配置加载失败：保持当前 generation；不会让新连接临时使用半成品证书/CA。
- 强制撤销期间发生的新 accept 使用新配置；旧 generation 连接在 owner close
  和 callback/quiescence 屏障完成前保留全部引用。
- TLS/身份错误不做明文降级；当前通用传输错误码沿用既有 `TR_ERR_SYS`、
  `TR_ERR_CLOSED`、`TR_ERR_TIMEOUT` 等边界，业务最终认证状态由 OPEN work 决定。

## 8. 资源与并发约束

- 每个主体 URI 最大 512 字节；身份快照每连接最多一个；Call 引用只增引用计数，
  不复制 URI 字节。连接组 CONTROL identity 只保存一个引用，DATA 成功后不再
  按每条 frame 复制身份。
- TLS/证书/profile 创建和更新在 I/O owner 以外执行。owner command 队列满时更新
  返回可重试错误，候选 snapshot 由调用方按合同释放或继续提交。
- TLS handshake、preface、RPC OPEN 准入共享原绝对期限；identity callback 是本地
  同步判断，不能把 deadline 伪称为可抢占 callback。
- OpenSSL 内部堆、内核 socket 缓冲和证书解析峰值的完整硬预算不在 TLS-D1 的
  通过声明中；其测量/上界属于 TLS-E，不因引入 refcount snapshot 而宣称 RSS 硬限额。

## 9. 验收矩阵

1. 普通 V2 mTLS：双向有效证书、clientAuth/serverAuth EKU、错误/缺失 client CA、
   未提供必需客户端证书、错误 key、客户端证书身份未由允许 CA 签发。
2. 身份解析：唯一合法 URI SAN；无 URI、两个 URI、空 URI、控制字符、超过 512
   字节；无 Common Name 或 DNS SAN 回退；身份在 RPC OPEN work 可见，metadata
   同名键不改变身份。
3. 普通 RPC 生命周期：连接关闭与异步 work、producer、terminal 交错；work 在
   connection gone 后仍可读 identity，终局静默后 identity ref 恰好释放；预算拒绝
   不留下 Call 或 session。
4. Group 顺序：认证前发送 TRR1 不得调用任何 route/authorize callback；有效
   mTLS 后的 CONTROL/DATA 才能进入 preface parser。
5. CONTROL/DATA 绑定：同 principal/有效 group epoch/member 成功；同 CA 不同
   principal、同 principal 错 group/epoch/member generation、不同 server URI、无
   client cert、重放旧 DATA route 均拒绝；CONTROL 和其他 DATA 的状态不被误清理。
6. 授权策略：CONTROL 与 DATA 两种角色分别由本地 authorize callback 决策；
   拒绝回调不创建业务/transfer；回调不得在 Reactor 上作阻塞等待。
7. 轮换：加载有效新证书后新 group 使用新 generation；旧连接持有旧 snapshot
   继续收敛；一个活动 group 不混用 client generations；server generation 变更后
   旧 group 新 DATA 被拒并要求重建；构造失败保持旧 generation；强制撤销只关闭
   目标 generation，且销毁屏障完成前 profile/identity 不释放。
8. 回归：现有 TLS disabled、普通服务端认证 TLS、错误 SAN/CA、普通明文、Reactor
   deadline/rollback、连接组 route/resource/drain 和 RPC V2 全量矩阵不回退。
9. 工具门槛：固定 Ubuntu 22.04/OpenSSL 3.0；GCC/Clang、ASan/UBSan、TSan、
   GCC 11/Clang 14 静态分析、Make/install、格式与 CRC32C 可移植路径均通过。

所有本地测试身份、构建临时目录、日志和工件写入仓库 `build/`；不得向 `/tmp`
写入测试文件、日志或缓存。测试通过后只可宣称 TLS-D1 已交付；TLS-D2、TLS-E
及 #234 的其余验收继续开放。
