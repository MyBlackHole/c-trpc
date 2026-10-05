# ADR-003：每个 Reactor 使用独立 SO_REUSEPORT 监听器

**状态：已接受**

## 背景

中央接收线程会让所有连接先经过一次用户态分发和 fd 移交。

Linux 最低版本为 3.10，已经具备 `SO_REUSEPORT`。

## 决策

V1 目标是每个 Reactor 创建自己的监听器：

```text
R0 -> listen_fd0
R1 -> listen_fd1
...
RN -> listen_fdN

全部使用 SO_REUSEPORT 绑定相同 IP:端口
```

每个 Reactor 自己执行 `accept`。

## 影响

普通连接：

```text
内核 -> 所有者 Reactor
```

无需中央接收线程。

备份 DATA 套接字仍可能被错误分片接收，因此保留固定长度的连接前路由阶段；
需要亲和关系时，在同一进程内把 fd 所有权转给目标 Reactor。

不依赖 `EPOLLEXCLUSIVE`、reuseport eBPF 或 io_uring，因此保持 Linux 3.10 基线。
