# ADR-005：Pipeline V1 固定一个 Reactor 所有者

**状态：已接受**

## 背景

通用连接组/Pipeline 可以包含一个 CONTROL 和多个 DATA 连接，并维护成员关系、
亲和关系、额度、路由等传输层易失状态。

如果同一个 Pipeline 被多个 Reactor 直接共享，将重新引入跨 Reactor 锁。

## 决策

V1 目标：

```text
一个 Pipeline
=
一个 Reactor 所有者

CONTROL + DATA[N]
最终全部由该 Reactor 拥有
```

V1 不允许同一个可变 Pipeline 被多个 Reactor 共同拥有。
已接收套接字必须在加入 Pipeline 前确定所有者；如果未来部署拓扑要求接收分片与
Pipeline 所有者不同，应显式选择准入拒绝、reuseport 导流或 fd 所有权转移，
而不是共享 Pipeline 状态。

## 影响

优点是 Pipeline 热状态不需要跨 Reactor 加锁。

风险是超大 Pipeline 可能达到单 Reactor 协议 CPU 上限。

只有基准测试证明：

```text
一个 Pipeline 已经耗尽所有者 Reactor
同时其他分片处于空闲状态
```

才引入未来 DataShard；DataShard 也应采用独立所有者 + 消息传递，
而不是共享同一个可变 Pipeline。
