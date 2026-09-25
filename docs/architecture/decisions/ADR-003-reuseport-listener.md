# ADR-003：每 Reactor 使用独立 SO_REUSEPORT listener

**Status: Accepted**

## Context

中央 accept thread 会让所有连接先经过一次用户态分发和 fd handoff。

Linux 最低版本为 3.10，已经具备 SO_REUSEPORT。

## Decision

TARGET V1 每个 Reactor 创建自己的 listener：

```text
R0 -> listen_fd0
R1 -> listen_fd1
...
RN -> listen_fdN

all bind same IP:port with SO_REUSEPORT
```

每个 Reactor 自己 accept。

## Consequences

普通连接：

```text
kernel -> owner Reactor
```

无需中央 accept thread。

Backup DATA socket 仍可能被错误 shard accept，因此保留一个固定长度 pre-connection routing stage；需要 affinity 时在同一进程内将 fd ownership 转给目标 Reactor。

不依赖 EPOLLEXCLUSIVE、reuseport eBPF 或 io_uring，因此保持 Linux 3.10 基线。
