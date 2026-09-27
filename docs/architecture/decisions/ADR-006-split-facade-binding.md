# ADR-006：Split facade 使用显式 connection-group binding identity

**Status: Proposed**

## Context

低层 `Channel` 已支持 `CONTROL` / `BULK` 两条独立 physical connection，
但高层 Client/Server facade 当前只建立 shared connection。

Server 进入 split 模式后，两条 TCP socket 会被独立 accept。不能使用以下信息把它们
配成一个逻辑 peer：

- accept 先后顺序：并发 Client 会交错；
- 源 IP/端口：NAT、同主机并发和 reconnect 都会造成歧义；
- “第一条是 CONTROL、第二条是 BULK”：连接建立/重试顺序不稳定。

错误配对会把两个不同 Client 的 CONTROL/BULK 状态拼到同一个 Channel 中，属于协议
隔离错误，不能依赖概率或部署习惯规避。

## Decision

split facade 在 Reactor/Channel ownership 之前完成显式 binding：

```text
Client logical session
  -> generate 128-bit unpredictable connection_group_id
  -> connect CONTROL fd
  -> connect BULK fd
  -> BIND(group_id, CONTROL)
  -> BIND(group_id, BULK)
  -> wait matching BIND_ACK on both fds
  -> Reactor adopts both fds
  -> create split Channel
  -> existing HELLO negotiation

Server
  -> accept fd, but do not adopt it into Reactor yet
  -> bounded nonblocking pending-binding state machine
  -> incrementally read exactly one BIND frame per accepted fd
  -> pair exactly one CONTROL + one BULK with the same group_id
  -> Reactor adopts both fds
  -> create deferred split Channel + RPC state
  -> enqueue BIND_ACK on both physical connections
  -> start existing HELLO negotiation
```

accept thread 不对某一 socket 做 blocking preface read。它必须把 listener 与 pending
binding sockets 一起 poll，并对每个 fd 只推进当前可读的 header/payload bytes。这样
一个慢连接不会阻塞其他 accept 或配对。

`BIND` / `BIND_ACK` 使用普通 Transport frame，因此继续复用 frame header CRC、
payload CRC 和既有 wire validation。binding payload 固定 32 bytes：

```text
magic       4   "TRB1"
version     2   little-endian, V1 = 1
lane        2   CONTROL=0, BULK=1
identity   16   cryptographically strong random bytes
reserved    8   must be zero
```

`BIND_ACK` 回显相同 binding payload。Client 只有在两个 fd 都收到匹配 identity/lane 的
ACK 后才把 fd ownership 转移给 Reactor。

Server 在 pair 完成后先 enqueue `BIND_ACK`，再启动 Channel HELLO。对同一 TCP
connection，wire FIFO 保证 ACK 位于该 lane 的 HELLO 之前。Client 在 Reactor adopt
之前只消费 ACK；如果 HELLO 已经紧随 ACK 到达，它仍留在 socket receive queue 中，
Reactor 接管后由正常 parser 消费，不需要临时 handler 切换。

identity 通过 Linux `getrandom()` 生成。随机源失败时连接建立失败，不回退到时间戳、
PID、counter 或 `rand()`。

Server pending-binding table 必须同时满足：

- fixed/bounded capacity；
- monotonic timeout；
- 每个 accepted fd 有独立增量 parser state；
- 同一 identity 最多一个 CONTROL 和一个 BULK；
- duplicate lane、第三条 connection、非法或全零 identity 立即拒绝；
- timeout/close/error 必须关闭并释放仍由 binding stage 拥有的 fd；
- 完成 pair 前不得创建 ready peer，不计入 `peers_ready`。

## Why not a temporary Reactor BIND handler

让 Reactor 先接管 fd，再通过临时 handler 等待 BIND 会引入额外的 handler handoff
协议。尤其在两条独立 TCP connection 上，Server 可能已经在一条 lane enqueue
ACK/HELLO，而 Client 尚未完成另一条 lane 的 ACK；此时需要额外同步才能保证两边在同一
阶段切换 handler。

pre-Reactor binding 把边界固定为“fd ownership transfer 之前”，同时与未来
multi-Reactor 的 routing preface 一致：先读 routing/binding identity 决定逻辑 owner，
再把 fd 交给目标 Reactor。

## Security boundary

128-bit unpredictable identity 用于 **connection confusion / off-path hijack resistance**，
不是 peer authentication。

在明文 TCP 上，能够观察或修改流量的 on-path attacker 仍可能看到/替换 BIND。因此：

- 不把 BIND identity 当 credential；
- 不把“identity 相同”解释为已经认证；
- 面向不可信网络时，TLS/mTLS 必须在 BIND 之前建立，或者未来给 BIND 增加由已认证
  会话密钥保护的 proof。

这与后续 TLS provider 集成不冲突：TLS 决定“对端是谁”，BIND 决定“两条已经建立的
transport 属于哪个逻辑 connection group”。

## Consequences

优点：

- 不依赖源地址或时序启发式；
- accept loop 保持 nonblocking；
- 无临时 Reactor handler 生命周期与双-lane handler 切换竞态；
- physical connection 进入协议阶段后仍保持 Reactor single-owner；
- 与未来 multi-Reactor routing preface / owner selection 方向一致。

代价：

- Server accept/binding 层需要有界 pending state 和 timeout；
- Client split connect 在 Reactor adopt 前多一个 bounded BIND/ACK handshake；
- wire protocol新增 `BIND` / `BIND_ACK` transport frame；
- TLS 未接入前只能提供配对身份，不提供端到端认证。
