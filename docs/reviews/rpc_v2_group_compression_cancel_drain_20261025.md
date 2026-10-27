# RPC V2 连接组 Zstandard 任务取消与排空记录

## 范围

本记录对应 `2.0.0-dev.55` 与 GitHub #242/ERR-D。它验证连接组在 Zstandard 编码任务尚未交付完成回调时开始排空，并由应用释放逻辑 transfer 的路径。

## 场景与断言

1. 建立一个 CONTROL 和一个 DATA 成员，协商客户端 Zstandard 发送与服务端 Zstandard 接收，并接纳 Stream 5001。
2. 测试 wrapper 在 worker 调用真实 `ZSTD_compressStream2()` 后暂停其完成回调。公开发送 API 已返回 `TR_OK`，Client 统计显示一个活动 transfer，`send_bytes_inflight` 等于原始输入长度。
3. Client 与 Server 开始排空；新 DATA Offer/Transfer Ready 被拒绝，两个等待都未完成。
4. 释放客户端与服务端的 Stream transfer 后，客户端活动 transfer 数归零，但 `send_bytes_inflight` 仍等于原始输入长度；Client drain 继续超时。
5. 放开 worker 后，已撤销 transfer 的压缩工作安全取消并完成回收，不得向服务端交付 DATA；输入字节归还后 Client drain 成功。Server 仍等待连接成员关闭。
6. 客户端关闭组，DATA 成员退出，Server drain 完成，最终组、连接与活动 transfer 数归零。

## 验证

- GCC Release：完整 `test_connection_group_facade` 通过。
- GCC TSan：完整 `test_connection_group_facade` 通过，无数据竞争报告。
- `git diff --check` 通过。

## 未覆盖范围

该测试覆盖 Zstandard worker 被接纳后、编码结果交付前撤销 transfer 的取消/回收路径；编码完成后压缩 wire 已被 Reactor 接受并处于 TX 队列时的 drain、成员故障或重连、多组并行竞争仍待验证。
