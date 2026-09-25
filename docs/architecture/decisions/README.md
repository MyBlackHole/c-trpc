# Architecture Decision Records

ADR 只记录“为什么做这个选择”，不重复实现细节。

- [ADR-001：Protocol state 采用 Reactor single-owner](ADR-001-single-owner.md)
- [ADR-002：Server V1 采用单进程多 Reactor](ADR-002-multi-reactor.md)
- [ADR-003：每 Reactor 使用独立 SO_REUSEPORT listener](ADR-003-reuseport-listener.md)
- [ADR-004：RPC worker 通过 completion 回 owner](ADR-004-worker-completion.md)
- [ADR-005：Pipeline V1 固定一个 Reactor owner](ADR-005-pipeline-affinity.md)

状态约定：

- Accepted：当前目标架构正式采用。
- Superseded：已被后续 ADR 替代。
- Proposed：尚未定稿。
