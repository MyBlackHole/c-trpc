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

Client 在 Reactor ownership transfer 前完成 BIND/BIND_ACK；Server 在识别出完整
connection group 后才把两条 fd 交给 Reactor：

```text
Client
  connect CONTROL + BULK raw nonblocking fds
  -> BIND(group_id, CONTROL/BULK)
  -> wait matching BIND_ACK on both
  -> Reactor adopts both
  -> create split Channel
  -> existing HELLO

Server
  accept raw nonblocking fd
  -> bounded pending-binding parser
  -> pair one CONTROL + one BULK with same group_id
  -> Reactor adopts both
  -> install final deferred Channel handlers + RPC methods
  -> Reactor enqueue BIND_ACK on CONTROL and BULK
  -> start Channel, enqueue existing HELLO
```

Server 不能在 raw ACK 完成后才 adopt：Client 收到 ACK 后可能立即发送 HELLO，而 Server
此时还没有 Reactor handler，会存在首帧被 NULL handler 丢弃的窗口。

Server 也不使用 temporary BIND handler。配对完成后直接安装最终 Channel handler，
然后把两个 `BIND_ACK` command 先于 `tr_channel_start()` 的 HELLO command 入队。
同一 Reactor command FIFO 与每条 TCP connection 的 TX FIFO 保证该 lane 上
`BIND_ACK -> HELLO` 的 wire 顺序。

Client 仍在 Reactor 外读取固定长度 ACK；如果 ACK 后的 HELLO 已经进入 socket receive
queue，Client 只消费 ACK，HELLO 留在内核缓冲，fd adopt 后由正常 Reactor parser 处理。

accept/binding loop 把 listener 与 pending raw sockets 一起 `poll()`，每个 fd 只推进
ready 的 read bytes；慢/partial peer 不得阻塞其他 accept 或配对。

`BIND` / `BIND_ACK` 都是普通 Transport frame。payload 固定 32 bytes：

```text
magic       4   "TRB1"
version     2   little-endian, V1 = 1
lane        2   CONTROL=0, BULK=1
identity   16   cryptographically strong random bytes
reserved    8   must be zero
```

identity 由 Linux `getrandom()` 生成；失败即连接失败，没有弱随机回退。

Server pending-binding state 必须满足 fixed capacity、monotonic timeout、每 fd 独立
incremental read offset；同 identity 最多一个 CONTROL 和一个 BULK。duplicate lane、
第三条连接、非法/全零 identity、timeout/close/error 都必须被拒绝/释放。pair 完成前
不得创建 ready peer。

## Security boundary

128-bit unpredictable identity 用于 **connection confusion / off-path hijack resistance**，
不是 peer authentication。明文 TCP 上的 on-path attacker 仍可观察/修改 BIND。
面向不可信网络时，TLS/mTLS 必须先建立，或未来使用已认证会话密钥保护 binding proof。

TLS 决定“对端是谁”；BIND 决定“两条 transport 属于哪个逻辑 connection group”。

## Consequences

优点：

- 不依赖源地址或时序启发式；
- accept/binding 保持 nonblocking；
- Server Reactor 从第一帧开始就使用最终 Channel handler；
- Client 不需要 temporary Reactor handler；
- 与未来 multi-Reactor routing/owner selection 兼容。

代价：

- Server 增加有界 pending-binding 生命周期；
- split Client connect 增加 BIND/BIND_ACK handshake；
- Server 需要极小的专用 ACK payload pool；
- wire protocol增加 `BIND` / `BIND_ACK`；
- TLS 未接入前只解决配对身份，不解决 peer authentication。
