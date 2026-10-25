# 多状态 TCP CONTROL 故障与 epoch 路由隔离回归

## 目的

扩展 dev.122 的真实公开 Server 回归，覆盖同一连接组中 DATA 状态不一致时 CONTROL 真实断连的回收行为。验证活动 DATA、空闲 DATA 和尚未连接的 RESERVED route 都随 epoch 71 CONTROL 退役，并且都不能在 epoch 72 接管后重新附着。

## 场景与断言

- 建立真实 loopback TCP 连接组，配置一个 CONTROL、最多三条 DATA。epoch 71 下附着第一条 DATA，为 stream 3001 建立 transfer，并发送和验证一条消息；此时统计为 1 个 group、1 条 DATA 和 1 个 active transfer。
- 附着第二条 DATA，但不给它分配 transfer；再发出第三条 DATA offer，读取并保留其 route 前导，不连接 socket。断连前统计为 1 个 group、2 条 DATA 和 1 个 active transfer。
- 关闭 epoch 71 的 CONTROL。等待两个 DATA close 回调，确认活动和空闲 DATA socket 均收到 EOF/reset，且 group、DATA、active transfer 和连接数归零。未附着的 RESERVED route 由旧 Pipeline 一并撤销。
- 同一 group ID 以 epoch 72 建立新 CONTROL 并取得新 DATA route。依次提交 epoch 71 的活动 DATA route、空闲 DATA route 和 RESERVED route；三条 socket 均收到 EOF/reset，新组仍保持 0 条 DATA、0 个 active transfer。
- 提交 epoch 72 的有效 route，确认 DATA 成功附着。再次关闭新 CONTROL，验证 DATA close 回调、socket EOF 和统计归零，最后完成 Server drain/destroy。

## 验证

- GCC Release：定向 `test_connection_group_facade/*` 通过。
- GCC TSan（`halt_on_error=1`）：同一目标通过。
- GCC ASan/UBSan（开启泄漏检测并配置 `halt_on_error=1`）：同一目标通过。
- `git diff --check` 通过；提交后运行仓库 clang-format 18.1.8 changed-lines 门禁。
- 测试、临时目录和构建产物均位于该工作树的 `build/err-d-highfrequency/`。

## 范围与限制

本次只扩展测试并更新证据、进度台账和开发版本号，不更改生产代码、公开 API/ABI 或 wire。该回归覆盖一个组中两条已附着 DATA 和一个 RESERVED route 的单次 CONTROL 故障及 epoch 切换；不代替双组控制故障交错、长期循环压力、TLS 故障矩阵或 #242/ERR-D 完整验收。前序单 DATA 真实 TCP 回归见[真实 TCP CONTROL 故障与 epoch 切换回归](rpc_v2_group_control_loss_tcp_epoch_20261026.md)。
