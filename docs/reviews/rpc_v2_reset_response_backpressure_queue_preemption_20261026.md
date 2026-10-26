# RESET 响应队列满载与 PONG 抢占边界回归

日期：2026-10-26。关联 #242/ERR-B。版本：`2.0.0-dev.144 -> 2.0.0-dev.145`。

## 覆盖范围

通过真实 Channel/Reactor 验证对端 RESET 响应在队列容量耗尽时仍可获得固定终止预留，并维持命令和 wire FIFO：四个先行 PING 占满普通控制 TX 池；RESET 事件回调在 owner 线程中向 Reactor 命令环加入 8 个前驱命令，下一次提交确认返回 `TR_AGAIN`。RESET 响应等待这些前驱处理后仍成功排在四个 PING 后。

恢复发送后，测试确认 RESET 响应先写出 7 字节并在目标 fd 上真实遇到 `EAGAIN`。此时从对端发送 PING，等 PONG 已入队后再允许发送恢复；最终 wire 顺序为四个先行 PING、完整 RESET、PONG。该顺序证明 PONG 没有插入已部分写出的 RESET。RESET 后同一物理连接上的健康 Stream 仍能交付 DATA 并完成 RESET。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，真实 Channel 墓碑容量 40,000。
- GCC ThreadSanitizer（`halt_on_error=1:second_deadlock_stack=1`）：完整 `test_stream_reset` 通过，墓碑容量 5,000，无数据竞争报告。
- GCC ASan/UBSan（启用泄漏检测并在错误时停止）：完整 `test_stream_reset` 通过，墓碑容量 40,000，无 sanitizer 报告。
- clang-format 23.1.4 改动行检查及 `git diff --check` 通过。Xmake 构建目录和测试运行环境位于仓库内 `build/test-tmp/`。

## 影响与限制

只修改 `tests/test_stream_reset.c`、本报告、README、剩余工作记录和 Xmake 版本号；不改生产运行时、公开 API/ABI 或协议线格式。运行时性能无影响。

该回归验证普通控制 TX 池满载、命令环满载、固定 RESET 预留、部分写入 EAGAIN 与同连接 PONG 排序的组合；不覆盖多个并行物理连接共享 Reactor 时的全局终止预留竞争，也不代表 #242 或 V2 整体验收完成。

## 回滚方案

撤销 dev.145 提交，移除队列满载场景和本报告，将 README、剩余工作记录与 Xmake 版本恢复至 dev.144，并在 GCC Release 下重跑完整 `test_stream_reset`。

## 验收标准

1. 四个普通控制项保持排队时，目标 stream RESET 响应仍能取得每连接固定预留；命令环已满时其接受顺序保持固定。
2. 观测到 RESET 响应实际部分写出 7 字节并遇到 `EAGAIN`。
3. 后到 PONG 不插入 RESET 帧，完整 wire 顺序为四个 PING、RESET、PONG。
4. 原连接保持 UP，健康 Stream 交付 DATA 并完成 RESET。
5. GCC Release、TSan、ASan/UBSan 下完整 `test_stream_reset` 均通过。
