# 32 轮真实 TCP CONTROL epoch 轮换回归

## 目的

扩展 #242/ERR-D 的 epoch 切换证据，检查同一 group ID 经多轮真实 CONTROL 故障、DATA 退役和新 epoch 重建后，资源与成员代次持续收敛，较早签发的 DATA route 不会在新 epoch 复活。

## 场景与断言

- 创建公开连接组 Server，固定 group ID，在 epoch 71 起连续执行 32 轮 CONTROL/DATA 建立与拆除。
- 每轮接收新的 DATA offer 并附着真实 TCP socket，验证 route epoch；与上一代比较，成员 generation 必须变化。
- 每轮为 DATA 建立一个 transfer，再关闭 CONTROL；等待 DATA close 回调、确认 DATA socket 收到 EOF/reset，并验证 group、DATA、active transfer 和总连接计数均回到零。
- 第 33 个 CONTROL 使用 epoch 103 并取得新 DATA offer。依次重放 epoch 71 的最早 route 与 epoch 102 的最近 route，确认两条 TCP socket 均关闭且新组仍没有附着 DATA。
- 使用 epoch 103 的有效 route 附着 DATA、建立 transfer，再关闭 CONTROL；确认最终 DATA/组/transfer/连接状态全部归零并完成 Server drain/destroy。

## 验证

- GCC Release：定向 `test_connection_group_facade/*` 通过。
- GCC TSan（`halt_on_error=1`）：同一目标通过。
- GCC ASan/UBSan（开启泄漏检测并配置 `halt_on_error=1`）：同一目标通过。
- clang-format 18.1.8 对新增和修改代码行的检查通过；`git diff --check` 通过。
- 测试、临时目录和构建产物均位于该工作树的 `build/err-d-highfrequency/`。

## 范围与限制

本次只增加测试、证据、进度台账和开发版本号，不更改生产代码、公开 API/ABI 或 wire。32 轮固定序列验证的是有限 teardown/recreate 周转，不是长时间压力或 64 位代次回绕；不代替多组并发 epoch 切换、TLS 故障矩阵或 #242/ERR-D 完整验收。
