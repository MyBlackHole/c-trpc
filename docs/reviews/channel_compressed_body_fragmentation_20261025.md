# 压缩 body 跨 DATA 帧接收回归

日期：2026-10-25。版本：`2.0.0-dev.114 -> 2.0.0-dev.115`。关联 #241、#235、PR #324。

## 目标与场景

修正既有真实 TCP Channel 多帧压缩接收用例的切分点。此前首个 DATA 帧的压缩 body 长度为 0，测试只覆盖 envelope 与压缩 body 分属不同帧，没有覆盖压缩 body 自身跨帧。

## 输入与断言

- 将有效 Zstandard body 在中点切开，首个 `FIRST|LAST|COMPRESSED` DATA 帧承载完整 message envelope 和前半 body；后续 `COMPRESSED` DATA 帧承载剩余 body。
- 明确断言切分长度大于 0 且小于编码体长度，防止回归为空切片或整段切片。
- 通过真实 TCP Shared Channel 重组并解压；服务端收到两条消息，消息 ID、原始长度和正文逐字节相符。
- 检查 wire 接收/消费计数相等，并确认缓冲池归还。

## 验证结果

- 修改前新增的非空切片断言按预期失败，确认原用例的切分长度为 0。
- GCC Release `test_transport/default` 在最终版本下连续 3/3 通过。
- `git clang-format --diff HEAD -- tests/test_transport.c` 未发现格式改动；`git diff --check` 通过。
- 测试命令将 `TMPDIR` 与 `XMAKE_TMPDIR` 指向工作区 `build/test-tmp`；未在 `/tmp` 创建测试文件。

本次仅修正测试、记录和开发版本，不改生产运行时、公开 API/ABI 或 wire。该用例验证单条压缩消息的 body 跨两个 DATA 帧，不覆盖更多切片、乱序或更广预算/取消组合。#241/#235 与 V2 整体验收继续开放。
