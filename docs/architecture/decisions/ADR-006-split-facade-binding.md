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

split facade 在 Channel HELLO 之前增加显式 binding 阶段：

```text
Client logical session
  -> generate 128-bit unpredictable connection_group_id
  -> CONTROL socket: BIND(group_id, CONTROL)
  -> BULK socket:    BIND(group_id, BULK)

Server
  -> Reactor adopts each accepted socket
  -> temporary bind handler accepts only BIND
  -> pair exactly one CONTROL + one BULK with the same group_id
  -> install the normal Channel handlers
  -> start the existing per-lane HELLO negotiation
```

`BIND` 使用普通 Transport frame，因此继续复用 frame header CRC、payload CRC、bounded
parser 和 Reactor nonblocking I/O；accept thread 不同步读取自定义 preface。

binding payload 固定 32 bytes：

```text
magic       4   "TRB1"
version     2   little-endian, V1 = 1
lane        2   CONTROL=0, BULK=1
identity   16   cryptographically strong random bytes
reserved    8   must be zero
```

identity 通过 Linux `getrandom()` 生成。随机源失败时连接建立失败，不回退到时间戳、
PID、counter 或 `rand()`。

Server pending-binding table 必须有容量和超时上限；同一 identity 的重复 lane、第三条
连接、非法/全零 identity 都拒绝。完成配对前不得创建 RPC Endpoint，也不得把 socket
暴露为 ready peer。

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
- 不阻塞 accept thread；
- 继续保持 physical connection 由 Reactor 单 owner 管理；
- 后续 reconnect 可以复用同一个显式 lane/group 语义。

代价：

- Server 增加有界 pending-binding 生命周期；
- Client split connect 需要在 Channel create 前完成两次 BIND；
- wire protocol 新增一个 transport frame type；
- TLS 未接入前只能提供配对身份，不提供端到端认证。
