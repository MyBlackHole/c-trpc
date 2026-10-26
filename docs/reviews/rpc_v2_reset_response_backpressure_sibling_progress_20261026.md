# RESET 背压期间健康同伴流前进回归

日期：2026-10-26。关联 #242/ERR-B、Draft PR #327。版本：`2.0.0-dev.147 -> 2.0.0-dev.148`。

## 覆盖范围

扩展 `test_peer_reset_response_reserved_fifo_under_pressure()`。在普通控制 TX 池已满、RESET 回调填满 Reactor 命令环的场景中，对端 RESET 的响应先写出 7 字节并真实遇到 `EAGAIN`；此时从同一物理连接向仍健康的同伴流发送 DATA，确认应用收到消息、连接无错误且仍有一条活动流。随后注入 PING，验证其 PONG 只能排在完整 RESET 后。解除发送阻塞后，线上顺序为四个既有 PING、完整 RESET、PONG；连接保持 UP，健康流随后完成 RESET。

测试让应用暂持收到的载荷，避免自动释放触发 `WINDOW_UPDATE` 干扰 RESET/PONG 队列断言。该夹具的初始窗口与最大消息大小均为 256 字节，信用更新阈值会被压到 1 字节；因此测试在完成背压期间的先进先出检查后释放载荷，再正常 RESET 健康流。

## 验证

- GCC Release：干净构建的完整 `test_stream_reset` 通过，真实 Channel 墓碑容量 40,000。
- GCC ASan/UBSan：启用泄漏检测的完整 `test_stream_reset` 通过，墓碑容量 40,000。
- GCC ThreadSanitizer：完整 `test_stream_reset` 通过，墓碑容量 5,000，无数据竞争报告。
- `git clang-format HEAD --diff -- tests/test_stream_reset.c` 无格式差异；`git diff --check` 通过。
- Xmake 构建输出、临时文件及测试均使用仓库内 `build/test-tmp/`。

## 影响与限制

只修改 `tests/test_stream_reset.c`、本报告、README、V2 剩余工作记录和 Xmake 版本号；不改生产运行时、公开 API/ABI 或协议线格式。运行时性能无影响，仅增加测试执行时间。

本回归证明同一连接的 TX 响应受阻时 RX 仍可处理健康流 DATA，同时保留 RESET/PONG 先进先出证据；不证明任意负载下的无限期双向公平性，也不代表 #242 或 V2 整体验收完成。

## 回滚方案

回滚 dev.148 提交，移除此测试扩展和报告，将 README、剩余工作记录及 Xmake 版本恢复至 dev.147，并重跑完整 GCC Release `test_stream_reset`。

## 验收标准

1. RESET 响应部分写出 7 字节并在真实 fd 上遇到 `EAGAIN`。
2. 发送阻塞期间，同连接健康流 DATA 交付应用，连接无错误且健康流仍活动。
3. 后到 PING 的 PONG 排在完整 RESET 后，恢复后连接保持 UP。
4. 健康流能在释放 payload 后完成 RESET。
5. GCC Release、ASan/UBSan、TSan 下完整 `test_stream_reset` 均通过，格式与差异检查通过。
