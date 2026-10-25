# 跨组 CONTROL 故障隔离回归

## 目的

补充 #242/ERR-D 的组间隔离证据：两个独立连接组都持有 CONTROL、DATA 和活动 transfer 时，真实 TCP 关闭其中一组的 CONTROL，只能拆除所属组；另一组必须继续使用原 CONTROL 和 DATA。

## 场景与断言

- 创建一个公开 Server，接纳两个独立 group ID/epoch，各自通过 loopback TCP 建立 CONTROL 和一条 DATA，并分别建立活动 transfer。
- 关闭第一组的 CONTROL socket。等待第一组 DATA close 回调并确认该 DATA 收到 EOF/reset；公开统计收敛到仅有第二组的 1 个 group、1 条 DATA 和 1 个活动 transfer。
- 确认总连接数仍为第二组的 CONTROL 与 DATA 两条连接，且第二组没有收到 DATA close 事件。
- 在第一组失败后，通过第二组原 CONTROL 发布新的 `TRANSFER_READY` 并从第二组原 DATA 发送消息；确认服务端回调验证 message、stream、group ID 和正文。
- 关闭第二组 CONTROL，确认其 DATA 随之关闭，最终 group、DATA、transfer 与连接计数全部归零，再停止、drain 并销毁 Server。

## 验证

- GCC Release：定向 `test_connection_group_facade/*` 通过。
- GCC TSan（`halt_on_error=1`）：同一目标通过。
- GCC ASan/UBSan（开启泄漏检测并配置 `halt_on_error=1`）：同一目标通过。
- clang-format 18.1.8 对新增回归代码行的检查通过；`git diff --check` 通过。
- 测试、临时目录和构建产物均位于该工作树的 `build/err-d-highfrequency/`。

## 范围与限制

本次只增加公开 Server 的测试、证据、台账和开发版本号，不更改生产代码、公开 API/ABI 或 wire。测试验证两个组中 A 组 CONTROL 单次真实断连后 B 组继续运行；不代替多个 CONTROL 同时故障、双组 epoch 并发切换、长时间压力、TLS 连接组故障矩阵或 #242/ERR-D 完整验收。
