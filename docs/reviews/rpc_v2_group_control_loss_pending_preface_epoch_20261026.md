# CONTROL 故障期间 pending DATA route 的 epoch 隔离回归

## 目的

补充 #242/ERR-D 的真实 TCP 连接组证据：DATA TCP 已进入 listener，但客户端尚未发送 route 前导；旧 CONTROL 故障并由新 epoch 接管后，延迟到达的旧 route 不能附着到新组，也不能消耗新组的 DATA 预留。

## 场景与断言

- 通过公开 transport-only Server 建立 epoch 71 的 CONTROL，并取得一条 DATA route 预留。
- 建立 DATA TCP socket，确认 listener 已接受连接，但暂不发送 route 前导。
- 关闭旧 CONTROL 并等待旧组状态清零，同时确认 route-less socket 仍作为 listener pending 连接存在。
- 使用同一 group ID 建立 epoch 72 的 CONTROL 和新的 DATA route 预留。
- 在已接受的 pending socket 上提交 epoch 71 的 route；服务端关闭该 socket，DATA attach 计数仍为零。
- 提交 epoch 72 的有效 route，确认 DATA attach、TRANSFER_READY 和资源清理正常完成。

## 验证

- GCC Release：`test_connection_group_facade/*` 通过。
- GCC TSan（`halt_on_error=1`）：`test_connection_group_facade/*` 通过。
- GCC ASan/UBSan（泄漏检测开启且错误立即终止）：`test_connection_group_facade/*` 通过。
- clang-format 23.1.3 新增和修改代码行检查、`git diff --check` 通过；当前环境未提供项目固定的 18.1.8。
- 测试临时目录及构建产物位于工作树 `build/err-d-highfrequency/`。

## 范围与限制

本次只增加公开 Server 回归、进度记录和开发版本号，不更改生产代码、公开 API/ABI 或 wire。该回归检查 route 前导跨 epoch 接管的时序，不测量 pending 握手期限本身，也不代表持续压力、完整连接组错误矩阵或 #242/ERR-D 整体验收完成。
