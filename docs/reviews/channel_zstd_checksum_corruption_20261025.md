# Zstandard 帧校验和损坏隔离回归

日期：2026-10-25。版本：`2.0.0-dev.117 -> 2.0.0-dev.118`。关联 #241、#235、PR #324。

## 目标与场景

验证启用了帧内容校验的 Zstandard 消息在校验失败时不交付部分或未校验正文，并将故障限制在目标流。既有压缩接收回归主要验证帧结构、声明长度和窗口边界，没有明确覆盖有效帧的内容校验字节损坏。

## 输入与断言

- 编码带 Zstandard 内容校验和的有效帧，确认帧内声明的原始长度与 envelope 一致，并用 `ZSTD_decompress()` 验证正文。
- 翻转帧末尾校验和中的 1 bit，并确认独立解码器报告错误。
- 通过真实 TCP Shared Channel 接收后，Channel 以 `TR_ERR_STATE` 复位当前流；服务端应用交付数保持不变，活动流与接收缓冲池恢复到基线。

## 验证结果

- GCC Release `test_transport/default` 在最终版本下连续 3/3 通过。
- `git clang-format --diff HEAD -- tests/test_transport.c` 未发现格式改动；`git diff --check` 通过。
- 测试命令将 `TMPDIR` 与 `XMAKE_TMPDIR` 指向工作区 `build/test-tmp`；未在 `/tmp` 创建测试文件。

本次仅增加测试、记录和开发版本，不改生产运行时、公开 API/ABI 或 wire。该用例验证帧校验和损坏被流级隔离，不覆盖其他 Zstandard 校验模式和更广压缩/预算组合。#241/#235 与 V2 整体验收继续开放。
