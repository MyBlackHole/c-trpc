# 压缩解码窗口超限隔离回归

日期：2026-10-25。版本：`2.0.0-dev.111 -> 2.0.0-dev.112`。关联 #241、#235、PR #324。

## 目标与场景

验证真实 Channel 在收到有效压缩体、但 Zstandard 帧要求的解码窗口超过本地上限时，会在应用交付前隔离该流。Channel 配置 `max_compression_window_log=20`，允许窗口最大为 1 MiB。

## 输入与断言

- 对 4,096 字节重复正文生成一个 Zstandard 帧，关闭帧内 content size；外层 envelope 正确声明 4,096 字节解码长度。
- 测试确认帧头没有 Single Segment 标志后，将 Window Descriptor 指数设为 11、mantissa 设为 0，要求 2 MiB 窗口。独立 `ZSTD_decompress` 仍可解出完整正文，证明压缩体有效；只有窗口声明超过 Channel 的 1 MiB 上限。
- 经真实 TCP Shared Channel 提交压缩 DATA；服务端以 `TR_ERR_NOMEM` 复位当前流，应用已交付消息计数保持 8，已有同伴流继续活动，接收缓冲池完全归还。
- 后续 trailing-data 与 DATA 标志负向检查继续执行。

## 验证结果

- GCC Release `test_transport/default` 连续 3/3 通过。
- 变更行 `git clang-format --diff HEAD -- tests/test_transport.c` 未发现格式改动；`git diff --check` 通过。
- 测试命令将 `TMPDIR` 与 `XMAKE_TMPDIR` 指向工作区 `build/test-tmp`；未在 `/tmp` 创建测试文件。

本次只增加集成测试、记录和开发版本，不改生产运行时、公开 API/ABI 或 wire。它证明一个有效未知长度帧在窗口上限外时被 Channel 隔离，不量化窗口检查的 CPU 成本，不覆盖所有窗口 descriptor、复杂帧组合或全局 RSS 上界。#241/#235 与 V2 整体验收继续开放。
