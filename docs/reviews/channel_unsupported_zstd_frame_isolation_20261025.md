# 不支持的 Zstandard 帧隔离回归

日期：2026-10-25。版本：`2.0.0-dev.118 -> 2.0.0-dev.119`。关联 #241、#235、PR #324。

## 目标与场景

验证真实 Channel 对两类不属于单条压缩消息正文的 Zstandard 帧进行流级拒绝。独立 message transform 单测已经覆盖这些帧格式；本次补齐 Reactor、Channel 和错误映射之间的集成证据。

## 输入与断言

- 发送带标准 skippable magic、零长度跳过区的合法 skippable frame，envelope 声明零字节解码正文。
- 发送 dictionary ID 为 1 的 Zstandard 帧，并断言 `ZSTD_getDictID_fromFrame()` 识别出非零 ID。
- 两种输入均经真实 TCP Shared Channel 接收后，以 `TR_ERR_STATE` 复位目标流；服务端应用交付数保持不变，活动流和接收缓冲池恢复到基线。

## 验证结果

- GCC Release `test_transport/default` 在最终版本下连续 3/3 通过。
- `git clang-format --diff HEAD -- tests/test_transport.c` 未发现格式改动；`git diff --check` 通过。
- 测试命令将 `TMPDIR` 与 `XMAKE_TMPDIR` 指向工作区 `build/test-tmp`；未在 `/tmp` 创建测试文件。

本次仅增加集成测试、记录和开发版本，不改生产运行时、公开 API/ABI 或 wire。该切片只覆盖 skippable frame 与非零 dictionary ID，不覆盖其他帧扩展和更广预算/取消组合。#241/#235 与 V2 整体验收继续开放。
