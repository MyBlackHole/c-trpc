# RPC V2 压缩配额压力期间控制帧进展回归

日期：2026-10-25。关联 #242/ERR-A、ERR-C。版本：`2.0.0-dev.67 -> 2.0.0-dev.68`。

## 覆盖范围

本切片只扩展 `test_rpc_record_stream` 中已有的 `test_compressed_workspace_exhaustion_preserves_peer`，不改生产实现、公开
API/ABI 或线协议。真实 TCP 双 Reactor、共享 Channel 场景中并行建立两条 RPC 调用；服务端预算仅保留 512 KiB 时，第一条调用的
Zstandard 解码工作区预约失败，以 `RESOURCE_EXHAUSTED` 本地终结并通过 RESET 通知客户端。

预算预留仍保持、失败流已退役且同伴调用仍未终结时，客户端 Channel 的 CONTROL keepalive 必须完成新的 PING/PONG 往返。
确认控制面进展后停止 keepalive、释放预算，再由同一 Channel 上的压缩同伴完成消息、半关闭和唯一 `OK` 终局。

## 验证

- GCC Release、TSan、ASan/UBSan 下完整 `test_rpc_record_stream` 均通过。
- 远端 PR CI 与 RPC smoke 在本记录提交后运行；结果以 PR #324 对应 head 的检查为准。
- `git diff --check` 在提交前执行。

## 边界、风险与回滚

该回归证明一种解码工作区不足状态下，CONTROL keepalive 仍有往返路径，并在预算释放后同伴可完成；不证明连接组 CONTROL/DATA
所有故障分类、持续竞争下的控制面延迟上界或资源公平矩阵。只增加测试；库运行时无性能影响，测试增加一次约 10ms 间隔的 keepalive 等待。

回滚可撤销 dev.68 提交，将 `xmake.lua` 与 README 恢复为 dev.67，移除新断言和记录，并恢复总账描述；随后在 GCC Release 下重跑
`test_rpc_record_stream`。

## 验收标准

1. 解码工作区不足只终结目标调用，RESET 到达对端且同伴保持活动。
2. 预算预留未释放期间，CONTROL PING/PONG 计数继续前进。
3. 释放预算后，同 Channel 压缩同伴成功完成，终局只发生一次且缓存归零。
4. Release、TSan、ASan/UBSan 下完整目标通过。
