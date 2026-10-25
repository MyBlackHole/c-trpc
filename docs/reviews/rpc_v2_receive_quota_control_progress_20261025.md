# RPC V2 重组与解码配额期间控制帧进展回归

日期：2026-10-25。关联 #242/ERR-A、ERR-C。版本：`2.0.0-dev.68 -> 2.0.0-dev.69`。

## 覆盖范围

本切片在 `test_rpc_record_stream` 的 RAW 重组配额失败场景中复用 dev.68 增加的 Channel keepalive 进展断言。真实 TCP 双 Reactor、
共享 Channel 上建立两条 RPC 调用，服务端预留完剩余消息预算。第一条调用仅发送 MESSAGE 头，迫使服务端预约 RAW 正文重组缓存；
预算不足后该调用以 `RESOURCE_EXHAUSTED` 终结，由上层发送 RESET 通知对端。

当预算预留仍存在、失败调用已终结且同伴仍活动时，客户端 Channel 必须完成新的 CONTROL keepalive PING/PONG。PONG 往返后释放
预算，同 Channel 上的另一条调用继续发送 RAW 消息、半关闭并正常完成。dev.68 已为 Zstandard 解码工作区配额失败覆盖同一控制面
进展和同伴恢复；本切片将断言抽为共享 helper，使两类资源压力都显式覆盖。

## 验证

- GCC Release、TSan、ASan/UBSan 下完整 `test_rpc_record_stream` 均通过。
- `git diff --check` 在提交前执行。
- 远端 PR CI 与 RPC smoke 在本记录提交后运行；结果以 PR #324 对应 head 的检查为准。

## 边界、风险与回滚

该回归验证预算全部占用时 RAW 重组失败路径与 CONTROL keepalive 的可进展性，以及释放后的同伴恢复；它不证明持续高压下的控制帧
延迟上界、连接组 CONTROL/DATA 错误矩阵，或全部信用/预算组合。只增加测试，库运行时没有性能影响；测试增加一次约 10ms 的
keepalive 往返等待。

回滚可撤销 dev.69 提交，将 `xmake.lua` 与 README 恢复为 dev.68，移除 RAW 场景调用和记录、恢复总账，并在 GCC Release 下重跑
`test_rpc_record_stream`。

## 验收标准

1. RAW 重组预算不足只终结目标调用并通过 RESET 通知对端，同伴保持活动。
2. 预算仍全部预留时，CONTROL PING/PONG 计数增长。
3. 释放预算后，同 Channel 同伴调用正常完成，终局唯一且缓存归零。
4. RAW 重组与 Zstandard 解码两种场景在 Release、TSan、ASan/UBSan 下均通过。
