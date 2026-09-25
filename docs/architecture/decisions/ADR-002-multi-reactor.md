# ADR-002：Server V1 采用单进程多 Reactor

**Status: Accepted**

## Context

备份场景存在大 buffer、Pipeline soft state、hash/compression/crypto、storage queue 和多个 DATA connection。

候选方案：

1. Nginx 风格 master + worker process；
2. 单进程 + N Reactor threads。

## Decision

Server V1 采用：

```text
one process
+
N Reactor shards
+
shared blocking worker pool
```

多进程只作为未来部署/故障隔离扩展，不作为 core runtime 基础。

## Rationale

同进程可以：

- pointer/buffer ownership 直接转移；
- DATA socket 跨 shard 不需要 SCM_RIGHTS；
- cache/pool 可按 shard + shared 两层组织；
- Server 与 Client 复用同一 runtime abstraction。

## Consequences

主要代价是 fault isolation 较弱：一个致命进程错误会影响所有 shard。

因此 durable correctness 必须假定整个 Server process 随时可以消失，通过 supervisor/systemd/container 重启并 resume。
