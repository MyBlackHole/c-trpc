# ADR-006：Split facade 使用显式 connection-group binding identity

**Status: Proposed**

## Context

低层 `Channel` 已支持 `CONTROL` / `BULK` 两条独立 physical connection，
但高层 Client/Server facade 当前只建立 shared connection。

Server 进入 split 模式后，两条 TCP socket 会被独立 accept。不能使用 accept 顺序、
源 IP/端口或“第一条 CONTROL、第二条 BULK”这类启发式进行配对；并发 Client、NAT
和 reconnect 都会造成歧义。错误配对会把不同 Client 的 protocol state 拼到同一个
Channel 中。

## Decision

split facade 的 binding handshake **完整发生在 Reactor ownership transfer 之前**：

```text
Client logical session
  -> generate 128-bit unpredictable connection_group_id
  -> connect CONTROL fd + BULK fd
  -> raw nonblocking BIND(group_id, CONTROL/BULK)
  -> wait matching raw BIND_ACK on both fds
  -> Reactor adopts both fds
  -> create split Channel
  -> existing HELLO negotiation

Server
  -> accept raw nonblocking fd
  -> bounded pending-binding table
  -> incrementally read exactly one BIND frame per fd
  -> pair one CONTROL + one BULK with the same group_id
  -> incrementally write BIND_ACK on both raw fds
  -> only after both ACK frames are fully written:
       Reactor adopts both fds
       create deferred split Channel + RPC
       start existing HELLO negotiation
```

accept/binding loop 把 listener 与 pending sockets 一起 `poll()`。每个 fd 只推进当前
ready 的 read/write bytes；禁止在单个 peer 上 blocking read/write。因此慢连接不能
阻塞其他 accept、binding 或 ACK。

`BIND` / `BIND_ACK` 都是普通 Transport wire frame，继续复用 header CRC、payload
CRC 和 wire validation。payload 固定 32 bytes：

```text
magic       4   "TRB1"
version     2   little-endian, V1 = 1
lane        2   CONTROL=0, BULK=1
identity   16   cryptographically strong random bytes
reserved    8   must be zero
```

`BIND_ACK` 回显相同 identity/lane。Client 只有在两个 socket 都收到匹配 ACK 后才把
fd ownership 转移给 Reactor。Server 只有在两个 ACK 都完全发送后才转移 ownership，
因此 Reactor 接管时不存在 binding frame 的 partial write，也不需要临时 handler。

identity 通过 Linux `getrandom()` 生成。随机源失败时连接建立失败，不回退到时间戳、
PID、counter 或 `rand()`。

Server pending-binding state 必须满足：

- fixed/bounded capacity；
- monotonic timeout；
- 每个 fd 独立的 incremental header/payload/read/write offset；
- 同一 identity 最多一个 CONTROL 和一个 BULK；
- duplicate lane、第三条 connection、非法或全零 identity 立即拒绝；
- timeout/close/error 关闭仍由 binding stage 拥有的 fd；
- pair 完成前不得创建 ready peer，不计入 `peers_ready`。

## Why pre-Reactor

temporary Reactor BIND handler 会额外引入 handler handoff、双 lane phase 同步和
partial ACK/HELLO ordering 问题。把 routing/binding 放在 fd ownership transfer
之前，边界变成：

```text
raw accepted fd
  -> identify logical connection group / lane
  -> complete BIND ACK
  -> choose protocol owner
  -> transfer fd ownership
```

这也直接兼容未来 multi-Reactor routing preface：先根据 identity 选择 owner，再将 fd
交给目标 Reactor，而不是接管后再迁移。

## Security boundary

128-bit unpredictable identity 用于 **connection confusion / off-path hijack resistance**，
不是 peer authentication。

明文 TCP 上的 on-path attacker 仍可观察或修改 BIND。因此：

- BIND identity 不是 credential；
- identity 相同不代表 peer 已认证；
- 面向不可信网络时，TLS/mTLS 必须在 BIND 之前建立，或未来使用已认证会话密钥保护
  binding proof。

TLS 决定“对端是谁”；BIND 决定“两条已建立 transport 属于哪个逻辑 connection
group”。

## Consequences

优点：

- 不依赖源地址或时序启发式；
- accept/binding 保持 nonblocking；
- binding stage 对 raw fd ownership 完全封闭；
- Reactor 无临时 handler、无 binding partial state；
- 与未来 multi-Reactor owner selection 一致。

代价：

- Server accept 层增加有界 pending read/pair/write/timeout state；
- Client split connect 增加 bounded BIND/BIND_ACK handshake；
- wire protocol增加 `BIND` / `BIND_ACK` frame type；
- TLS 未接入前只解决配对身份，不解决 peer authentication。
