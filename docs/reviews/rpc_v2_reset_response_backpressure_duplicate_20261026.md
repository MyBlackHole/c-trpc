# Split Channel RESET 响应背压期间重复 RESET 回归

日期：2026-10-26。关联 #242/ERR-B。版本：`2.0.0-dev.143 -> 2.0.0-dev.144`。

## 覆盖范围

扩展真实 Split Channel/Reactor 发送背压夹具。CONTROL、BULK 两种目标 lane 分别覆盖相同原因与冲突原因的重复 RESET，共四个组合。每种组合都先使对端 RESET 的本地响应在目标 Reactor fd 上真实遇到 `EAGAIN`，并确认目标流已退役。

相同原因的重复 RESET 应由驻留墓碑吸收；同 lane PING 屏障确认输入已处理，恢复发送后只观察到最初的一份 RESET 响应，目标 lane 保持 UP。不同原因的重复 RESET 应以 `TR_ERR_STATE` 关闭目标 lane。四个组合均验证另一 lane 保持 UP，健康 Stream 仍能交付 DATA 并完成 RESET。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，真实 Channel 墓碑容量 40,000。
- GCC ThreadSanitizer（`halt_on_error=1:second_deadlock_stack=1`）：完整 `test_stream_reset` 通过，墓碑容量 5,000，无数据竞争报告。
- GCC ASan/UBSan（启用泄漏检测并在错误时停止）：完整 `test_stream_reset` 通过，墓碑容量 40,000，无 sanitizer 报告。
- clang-format 23.1.4 改动行检查及 `git diff --check` 通过。Xmake 构建目录和测试运行环境位于仓库内 `build/test-tmp/`。

## 影响与限制

只修改 `tests/test_stream_reset.c`、本报告、README、剩余工作记录和 Xmake 版本号；不改生产运行时、公开 API/ABI 或协议线格式。运行时性能无影响。

该回归只覆盖 RESET 响应已入队、实际写出受 `EAGAIN` 阻塞时到达的同原因与冲突原因重复 RESET，以及 CONTROL/BULK lane 故障隔离；不覆盖 RESET 重试队列容量耗尽、其他帧抢占或 ERR-A/B/C/D/E 完整矩阵，也不代表 #242 或 V2 整体验收完成。

## 回滚方案

撤销 dev.144 提交，移除四个重复 RESET 变体和本报告，将 README、剩余工作记录与 Xmake 版本恢复至 dev.143，并在 GCC Release 下重跑完整 `test_stream_reset`。

## 验收标准

1. 四个 lane/原因组合均确认 RESET 响应实际遇到 `EAGAIN`，且重复 RESET 到达时尚未写出。
2. 同原因重复 RESET 被墓碑吸收，只写出一份 RESET 响应且目标 lane 保持 UP；冲突原因以 `TR_ERR_STATE` 关闭对应 lane。
3. CONTROL、BULK 两种目标 lane 下，另一 lane 的健康 Stream 均能继续交付 DATA 并完成 RESET。
4. GCC Release、TSan、ASan/UBSan 下完整 `test_stream_reset` 均通过。
