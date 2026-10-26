# 多连接 RESET 固定预留隔离回归

日期：2026-10-26。关联 #242/ERR-B。版本：`2.0.0-dev.146 -> 2.0.0-dev.147`。

## 覆盖范围

通过真实 Split Channel 与同一个 Reactor 验证两个独立物理连接同时承受控制队列压力时，RESET 固定预留不会互相阻塞：共享的普通控制 TX 池容量为四项，CONTROL 与 BULK 各排入两条 PING；第一个对端 RESET 的回调再填满 Reactor 命令环，随后两个连接各自收到 RESET 并取得每连接预留。

恢复发送后，CONTROL 响应先写出 7 字节并真实遇到 `EAGAIN`。此期间 BULK 连接仍按两条 PING、RESET、后到 PONG 的顺序排空；CONTROL 上后到 PING 产生的 PONG 只能排在 RESET 完整帧之后。最后两条连接上的健康流都能交付 DATA 并完成 RESET。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，真实 Channel 墓碑容量 40,000。
- GCC ASan/UBSan：启用泄漏检测，完整 `test_stream_reset` 通过，墓碑容量 40,000。
- GCC ThreadSanitizer：完整 `test_stream_reset` 通过，墓碑容量 5,000，无数据竞争报告。
- `git clang-format HEAD --diff -- tests/test_stream_reset.c` 无格式差异；`git diff --check` 通过。
- Xmake 输出、临时文件及测试均使用仓库内 `build/test-tmp/`。

## 影响与限制

只修改 `tests/test_stream_reset.c`、本报告、README、V2 剩余工作记录和 Xmake 版本号；不改生产运行时、公开 API/ABI 或协议线格式。运行时性能无影响，仅增加测试执行时间。

用例覆盖当前 Split Channel 的两条物理连接与共享 Reactor；它验证并行连接下各自的 RESET 预留以及一侧部分写入受阻时另一侧前进，不代表任意数量连接、公平性长压或 #242/V2 整体验收完成。

## 回滚方案

回滚 dev.147 提交，移除双连接压力回归和本报告，将 README、剩余工作记录与 Xmake 版本恢复至 dev.146，并重跑完整 GCC Release `test_stream_reset`。

## 验收标准

1. 两条 Split Channel 物理连接共享的普通控制 TX 池达到容量上限，且 RESET 回调期间 Reactor 命令环满载。
2. 两条连接分别将 RESET 响应放入队列，CONTROL RESET 发生真实部分写入和 `EAGAIN`。
3. CONTROL 被阻塞时 BULK 按 PING、RESET、PONG 顺序前进；CONTROL 恢复后按 RESET、PONG 顺序完成。
4. 两条连接保持可用，各自健康流成功交付 DATA 并完成 RESET。
5. GCC Release、ASan/UBSan、TSan 下完整 `test_stream_reset` 均通过，格式和差异检查通过。
