# 架构决策记录

架构决策记录只说明“为什么做这个选择”，不重复实现细节。

- [ADR-001：协议状态采用 Reactor 单所有者](ADR-001-single-owner.md)
- [ADR-002：服务端 V1 采用单进程多 Reactor](ADR-002-multi-reactor.md)
- [ADR-003：每个 Reactor 使用独立 SO_REUSEPORT 监听器](ADR-003-reuseport-listener.md)
- [ADR-004：RPC 工作线程通过完成事件返回所有者](ADR-004-worker-completion.md)
- [ADR-005：Pipeline V1 固定一个 Reactor 所有者](ADR-005-pipeline-affinity.md)
- [ADR-006：拆分门面使用显式连接组绑定标识](ADR-006-split-facade-binding.md)
- [ADR-007：分层/API 边界不能通过额外运行时跳转实现](ADR-007-layering-performance.md)

状态约定：

- **已接受**：当前目标架构正式采用；
- **已替代**：已经被后续架构决策记录替代；
- **提议中**：尚未定稿。
