# RPC V2 双组高频成员故障替代回归

## 目的

为 #242/ERR-D 补充真实 TCP 连接组在同一轮内发生双组 DATA 成员并发故障、随后分别替代的重复回归，扩展既有双组两轮替代场景。

## 场景与断言

- 两个独立 Client Reactor 分别承载一个连接组；两组由同一 Server shard 管理，且每组最多一个 DATA 连接。
- 两条 DATA 物理连接通过 pthread 屏障并发中止；每轮都等待两组收到故障事件，核对客户端活动连接/transfer 计数归零，并验证旧 transfer 发送返回 `TR_ERR_STALE`。
- Server 分别重新接纳两条 DATA 连接并发送 `TRANSFER_READY`。两组各发一条消息，校验组 ID、消息 ID、stream ID、正文和新连接代次；之后进入下一轮。
- 在原有两次替代之后额外执行 32 轮；收尾验证关闭一组不影响另一组交付，释放 transfer 并排空后 Server 的 group/connection/transfer 计数归零。

## 验证

- GCC Release：定向 `test_connection_group_facade/*` 连续 3 次通过；最终按 clang-format 18.1.8 格式化后追加运行 1 次并通过。
- GCC ThreadSanitizer：定向 `test_connection_group_facade/*` 通过，`TSAN_OPTIONS=halt_on_error=1:second_deadlock_stack=1`。
- GCC AddressSanitizer/UndefinedBehaviorSanitizer：定向 `test_connection_group_facade/*` 通过，泄漏检测开启。
- 所有 xmake 临时目录与构建产物位于本工作树 `build/err-d-highfrequency/`。

## 范围与限制

本次只增加测试夹具、证据记录和开发版本号，不更改生产运行时代码、公开 API/ABI 或 wire 格式。回归覆盖固定的 32 轮双组并发故障替代序列，不构成持续时长压力、任意线程竞态覆盖、资源耗尽、延迟/吞吐阈值或 ERR-D 全矩阵验收；这些事项仍保留在 #242 的剩余工作中。
