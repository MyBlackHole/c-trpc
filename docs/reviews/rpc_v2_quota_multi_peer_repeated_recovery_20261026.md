# 压缩配额四轮多流恢复回归

日期：2026-10-26。关联 [#242 / ARCH-04](https://github.com/MyBlackHole/c-trpc/issues/242) 与 [#241 / RES-A](https://github.com/MyBlackHole/c-trpc/issues/241)，在 [dev.152 单轮多流恢复](rpc_v2_compressed_quota_multi_peer_recovery_20261026.md)基础上检查释放配额后的重复进展。

## 场景和观察

测试使用真实 TCP、Reactor、Channel、RPC Record Stream 和 Zstandard 解码，在同一 Channel 上打开五条调用。双方测试预算设为 16 MiB；服务器预留可用预算至仅剩 512 KiB 后，一条 4 KiB Zstandard MESSAGE 因无法预约解码工作区，以 `RESOURCE_EXHAUSTED` 终结。该调用不交付 MESSAGE、请求消息计数不增加且缓存归零；映射为 RESET 后客户端也只终结一次。预算仍保留时 CONTROL PING/PONG 成功，CONTROL 与 BULK lane 保持 UP。

释放预算后，四个幸存调用以 `TR_RPC_MANY` 请求基数运行四轮。每轮四个发送线程在屏障后同时提交 4 KiB Zstandard MESSAGE；测试等待每个服务端调用都观察到本轮消息，再通过客户端和服务端 Reactor owner 屏障排空本轮完成回调后才开始下一轮。每条流四轮的压缩类型、解码长度和正文校验和均核对，避免首轮成功掩盖后续任务槽或预算回收停滞。四轮结束后四条流各收到 HALF_CLOSE，服务端响应初始元数据、MESSAGE 和 OK `FINAL_STATUS`；两端终局各一次，缓存均归零。

红灯阶段将每条流的消息期望提高为四条，但保留单次发送；Release 测试稳定在流 1 观察到 `current=1 expected=4` 后超时。这确认断言会发现单轮发送无法满足多轮进展。加入每轮并发发送和 owner 排空后测试转绿。

## 验证

- GCC Release：完整 `test_rpc_record_stream` 通过；修正后的回归在最终断言补充前另连续 5/5 通过；
- GCC ThreadSanitizer：最终版本完整目标通过，无数据竞争报告；
- GCC AddressSanitizer/UndefinedBehaviorSanitizer，泄漏检测开启：最终版本完整目标通过；
- `git diff --check` 与改动行 clang-format 检查通过。

本切片只修改测试夹具、审查记录、README 和候选版本号，不修改生产逻辑、公开 API/ABI 或 wire。它验证单 Channel、四条存活流、四轮的有界进展，不证明无限运行期的最大延迟、吞吐阈值、跨机器基线或完整预算/取消组合；#242/#241 整体验收仍未完成。
