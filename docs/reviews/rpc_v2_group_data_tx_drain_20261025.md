# RPC V2 连接组 DATA 在途发送排空记录

## 范围

本记录对应 `2.0.0-dev.54` 与 GitHub #242/ERR-D。它扩展双 DATA 成员公开门面场景，验证已经接受但尚未写入 socket 的未压缩 DATA 字节会阻止客户端排空。

## 场景与断言

1. 两个 DATA 成员各承载一个活动 transfer，测试链接期 `sendmsg` wrapper 只对 DATA 帧返回 `EAGAIN`，将客户端已接受的第三条 DATA 消息留在 Reactor TX 队列。
2. 开始客户端和服务端 drain 后，新 Offer/Ready 被拒绝。只释放一个客户端 transfer 时仍有另一项活动传输，两端等待继续超时。
3. 两个客户端 transfer 都释放后，客户端统计为零活动传输、一个在途发送字节；Client drain 仍超时，证明等待由已接受的 TX 字节单独阻止，而非 transfer 计数。
4. 恢复 DATA socket 写入后，服务端实际收到第三条消息，客户端 `send_bytes_inflight` 归零，Client drain 完成；DATA 成员仍活动。
5. 服务端 transfer 在消息交付后释放，但 Server drain 继续等待连接成员；客户端关闭组后两条 DATA 连接退出，Server drain 完成且最终统计归零。

## 验证

- GCC Release：完整 `test_connection_group_facade` 通过。
- GCC TSan：完整 `test_connection_group_facade` 通过，无数据竞争报告。
- `git diff --check` 通过。

## 未覆盖范围

该测试覆盖未压缩 DATA 的发送队列持有；Zstandard 压缩任务/编码缓冲在途排空、成员故障或重连、多组并行竞争仍待验证。
