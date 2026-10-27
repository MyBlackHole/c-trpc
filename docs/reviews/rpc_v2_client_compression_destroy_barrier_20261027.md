# RPC V2 Client 压缩取消与销毁屏障回归

日期：2026-10-27。关联 #243。版本：`2.0.0-dev.171 -> 2.0.0-dev.172`。

## 需求与背景

补齐普通 RPC V2 Client 的压缩请求生命周期证据：CallRef 发出取消后，已受理的异步请求变换仍在 worker 中运行；即使 CallRef 已释放，Client destroy 也必须保留所有者并返回可重试状态。重试销毁应兑现唯一调用终态，并回收变换与请求资源。本回归只覆盖这条 #243 调用方路径，不表示整体生命周期矩阵或 V2 验收完成。

## 实现与影响

- `test_rpc_v2_sdk` 使用真实 TCP Client/Server，向支持 Zstandard 请求压缩的方法提交正文，并在消息服务 worker 进入变换运行入口时通过测试链接包装确定性暂停任务。
- 暂停期间取得 CallRef、请求取消并确认活动结果仍为 `TR_AGAIN`，失败读取不会修改输出；随后释放 CallRef。此时 `tr_client_destroy()` 仍返回 `TR_AGAIN`，证明销毁等待并非由 CallRef 引用单独造成。
- 解除 worker 门控并重试 destroy 后，stopped-reap 保留先前选定的 `CANCELLED`，只交付一次 `SHUTDOWN` 终态。服务端收到 OPEN 但没有收到请求 MESSAGE；客户端连接销毁后服务端以唯一 `UNAVAILABLE` 终结。
- 门控与链接包装仅在 `TR_RPC_V2_TEST_FAULTS` 测试构建中存在；同一源码的 SDK 消费构建不定义该宏，也不引用私有头或包装符号。不改生产运行时、公开 API/ABI、TLS、压缩 wire 或协议行为。
- 无产品运行时性能影响。测试新增一次短期暂停的 TCP/Zstandard 回归。

## 验证

- GCC Release：全量测试 94/94 通过，包含 `test_rpc_v2_sdk/default`。
- GCC ASan/UBSan：`ASAN_OPTIONS=detect_leaks=1:abort_on_error=1 UBSAN_OPTIONS=halt_on_error=1` 下定向目标通过。
- GCC TSan：`TSAN_OPTIONS=halt_on_error=1` 下定向目标通过。
- 同一 SDK 源码在无测试故障宏的构建下，通过 GCC/Clang C11 与 G++/Clang++ C++11 编译、链接并运行。
- 测试期间所有 Xmake 临时文件均使用当前 worktree 的 `build/test-tmp` 与 `build/xmake-tmp`，没有写入 `/tmp`。

## 边界、风险与回滚

门控位于 `tr_message_transform_run_checked()` worker 入口，验证已接受的 Zstandard 变换任务、取消、终态和销毁重试之间的屏障；它不把 Zstandard 算法内部某一条指令停住。因此本用例不替代长时间 CPU 变换、消息服务队列饱和或 TLS/压缩共享调度矩阵。测试若门控未被变换 worker 命中，会在有界等待后失败。回滚时撤销 dev.172 提交即可移除测试、记录、链接包装与版本号；无需数据或协议迁移。

## 验收标准

1. 在真实 Client/Server 连接上受理一个 Zstandard 压缩请求，并确定性暂停其变换 worker。
2. 活动 CallRef 取消后，活动结果查询返回 `TR_AGAIN` 且保留输出；CallRef 释放后销毁仍返回 `TR_AGAIN`。
3. 变换 worker 解除门控后，重复销毁最终成功，客户端只收到一次 `CANCELLED/SHUTDOWN` 终态。
4. Release、ASan/UBSan、TSan 与安装 SDK 的 C11/C++11 消费验证通过；测试钩子不进入安装 SDK。
5. 记录本切片的范围边界，不关闭 #243 或宣称 V2 整体验收完成。
