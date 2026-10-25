# RPC V2 真实 TCP CONTROL 故障与 epoch 切换回归

## 目的

为 #242/ERR-D 补充公开 Server 连接组的真实 TCP 故障证据：CONTROL socket 关闭后，服务端应回收活动 DATA、transfer affinity 和组资源；相同 group ID 由新 epoch 接管后，旧 epoch 的 DATA route 不得附着到新组。

## 场景与断言

- 创建真实公开连接组 Server，使用 loopback TCP 和原始 CONTROL/DATA route 前导。先以 epoch 71 建立 CONTROL，接收 DATA offer 并附着一个 DATA socket。
- 经 CONTROL 发布 stream 3001 的 `TRANSFER_READY`，从 DATA socket 发送 message 2001，确认服务端回调收到正文。测试夹具按公开回调契约接管消息后立即释放，再确认服务端统计为 1 个 group、1 个 DATA connection 和 1 个 active transfer。
- 关闭 epoch 71 的真实 CONTROL socket。等待 DATA close 回调，确认旧 DATA socket 收到 EOF/reset，并等待 group、DATA connection、active transfer 以及总连接计数全部归零。
- 使用同一 group ID 建立 epoch 72 的 CONTROL，接收新 DATA offer，并断言成员 generation 与 epoch 71 不同。提交旧 epoch 71 的 DATA route 后，旧 socket 收到 EOF/reset，服务端仍只有新 group 且没有附着 DATA。
- 提交 epoch 72 的有效 DATA route，确认其附着成功。再次关闭 CONTROL，确认 DATA 被回收，最后停止并 drain Server，等待 destroy 完成。

## 验证

- GCC Release：定向 `test_connection_group_facade/*` 通过。
- GCC TSan（`halt_on_error=1`）：同一目标通过。
- GCC ASan/UBSan（开启泄漏检测并配置 `halt_on_error=1`）：同一目标通过。
- clang-format 18.1.8 对本次新增和修改行的检查通过；提交后再运行仓库 changed-lines 格式门禁。
- 测试、临时目录和构建产物均位于该工作树的 `build/err-d-highfrequency/`。

## 范围与限制

本次增加公开连接组测试、证据记录和开发版本号，不更改生产代码、公开 API/ABI 或 wire。该场景验证一个 CONTROL、一个 DATA、一个 transfer 和一次 71→72 epoch 接管的真实 TCP 故障路径；不代替长时间循环压力、并发资源争用、TLS 连接组故障矩阵或 #242 完整验收。相关的 CONTROL abort 组件回归见[CONTROL 致命拆除与 epoch 路由回归](rpc_v2_group_control_abort_epoch_20261026.md)，双组高频成员替代回归见[双组高频成员故障替代回归](rpc_v2_group_repeated_failure_stress_20261026.md)。
