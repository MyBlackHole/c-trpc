# 压缩配额耗尽后的多流并发恢复回归

日期：2026-10-26。关联 [#242 / ARCH-04](https://github.com/MyBlackHole/c-trpc/issues/242) 与 [#241 / RES-A](https://github.com/MyBlackHole/c-trpc/issues/241)，补充真实 TCP 上 Zstandard 接收预算失败后的同连接恢复证据。

## 场景和观察

测试使用真实 TCP、Reactor、Channel、RPC Record Stream 和 Zstandard 解码，在同一 Channel 上打开五条调用。测试将双方测试预算提高到 16 MiB，以容纳四个并发 Zstandard 任务各自最多 1 MiB 的工作区预约；随后服务器预留除 512 KiB 外的可用预算。

第一条调用发送 4 KiB Zstandard MESSAGE。剩余预算不足以预约解码工作区，服务端以 `RESOURCE_EXHAUSTED` 终结该调用，未交付 MESSAGE、请求消息计数不增加且缓存归零；上层将该终局映射为 RESET 后，客户端也只终结一次。预算仍被预留期间，CONTROL PING/PONG 成功，CONTROL 与 BULK lane 均保持 UP。释放预算后，四个幸存调用通过屏障并发发送各自的 4 KiB Zstandard MESSAGE 和 HALF_CLOSE。

四条幸存流都由服务端按 Zstandard 解码，正文长度与校验和匹配；随后每条服务端调用发送初始元数据、响应 MESSAGE 和 OK `FINAL_STATUS`。测试确认两端调用均只终结一次、状态均为 OK、消息缓存均归零。建流阶段显式等待客户端和服务端都打开全部五条流；测试预算也为四个最大工作区及必要消息/调用记账留足容量，避免把夹具自身配置不足误判为恢复不公平。

## 验证

- GCC Release：完整 `test_rpc_record_stream` 通过；
- GCC ThreadSanitizer：完整目标通过，无数据竞争报告；
- GCC AddressSanitizer/UndefinedBehaviorSanitizer，泄漏检测开启：完整目标连续 10/10 通过；
- `git diff --check` 与改动行 clang-format 检查通过。

本切片只修改测试夹具、审查记录、README 和候选版本号，不修改生产逻辑、公开 API/ABI 或 wire。它只证明该单 Channel、该预算配置下的一次恢复场景；长期公平性、每条消息延迟界、跨机器性能基线、分域预算矩阵及 #242/#241 整体验收仍待完成。
