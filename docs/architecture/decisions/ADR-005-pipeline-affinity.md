# ADR-005：Pipeline V1 固定一个 Reactor owner

**Status: Accepted**

## Context

Backup Pipeline 可能包含一个 CONTROL 和多个 DATA connections，并维护 inflight、credit、ACK batch、cache 等大量 soft state。

如果同一个 Pipeline 被多个 Reactor 直接共享，将重新引入跨 Reactor locking。

## Decision

TARGET V1：

```text
one Pipeline
=
one Reactor owner

CONTROL + DATA[N]
all ultimately owned by that Reactor
```

DATA socket 若被其他 shard accept，通过 routing preface 找到 owner，再进行同进程 fd ownership transfer。

## Consequences

优点是 Pipeline hot state 无需跨 Reactor lock。

风险是 elephant Pipeline 可能达到单 Reactor protocol CPU 上限。

只有 benchmark 证明：

```text
one Pipeline saturates owner Reactor
while other shards are idle
```

才引入 FUTURE DataShard；DataShard 也应采用独立 owner + message passing，而不是共享同一个 mutable Pipeline。
