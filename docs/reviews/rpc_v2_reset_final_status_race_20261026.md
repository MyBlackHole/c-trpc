# RESET 与已排队 FINAL_STATUS 竞态回归

日期：2026-10-26。关联 [#242 / ARCH-04](https://github.com/MyBlackHole/c-trpc/issues/242)，验证“RESET 与 DATA/STATUS/半关闭并发”中的 RPC Record FINAL_STATUS 时序。

## 场景和观察

测试在真实 TCP、Reactor、Channel 和 RPC Record Endpoint 上同时打开目标调用与健康同伴。目标业务成功后，Endpoint 已将 `FINAL_STATUS(OK)` 交给 Channel 并报告发送接受；测试链接包装只让服务端对应 fd 的真实 `sendmsg` 返回 `EAGAIN`，因此终局记录尚未到达客户端。确认客户端仍未终结后，客户端对目标流发送 `TR_ERR_TIMEOUT` RESET。

测试等待服务端 Channel 处理 RESET，再解除发送阻塞，检查：

- 客户端以 `DEADLINE_EXCEEDED` 终结且终局计数恰为 1；
- 服务端保留先前已接受的 `OK` 结果，终局计数恰为 1；
- 释放发送阻塞并完成 RESET 后，目标调用的已交付消息数不增加，FINAL_STATUS 未被重试；
- 两端没有 Channel down 事件，预先打开的健康同伴在同一 TCP 连接成功完成。

这条时序表明：服务端接受 FINAL 只证明记录已进入本地传输发送队列；客户端在 RESET 前未收到该记录时，客户端结果仍是超时/未知。测试没有把这解释成业务持久化提交、exactly-once 或可安全重放的证明。

## 验证

- GCC Release：完整 `test_rpc_record_endpoint_tcp` 通过；
- GCC ThreadSanitizer：完整目标通过；
- GCC AddressSanitizer/UndefinedBehaviorSanitizer（泄漏检测开启）：完整目标通过；
- `git diff --check` 与改动行 clang-format 检查通过。

本切片仅增加 Endpoint 回归、审查记录、README 和版本号，不修改生产行为、公开 API/ABI 或 wire。它补充 #242 一项交错证据，不关闭 GOAWAY/连接组接受边界、完整 RESET/FIFO 矩阵、公平恢复或统一资源回收验收。
