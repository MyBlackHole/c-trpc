# 未知长度帧超出 envelope 解码容量回归

日期：2026-10-25。版本：`2.0.0-dev.112 -> 2.0.0-dev.113`。关联 #241、#235、PR #324。

## 目标与场景

验证未知帧 content size 时，真实 Channel 的 envelope 解码长度是输出硬边界；实际解码多产生一个字节时必须拒绝，不能截断后交付或写出已预约容量。

## 输入与断言

- 使用 `ZSTD_c_contentSizeFlag=0` 生成对 4,096 字节正文可完整解码的有效帧，并通过 `ZSTD_getFrameContentSize` 确认帧头为 `ZSTD_CONTENTSIZE_UNKNOWN`。
- 外层 message envelope 声明 4,095 字节解码容量，编码体长度仍匹配真实压缩帧。
- 通过真实 TCP Shared Channel 提交；解码器在输出容量恰满时用一字节探针发现真实输出多 1 字节，变换返回 LIMIT，Channel 将当前流 RESET 为 `TR_ERR_NOMEM`。
- 应用交付数保持 8，活动流数回到 3，接收缓冲池恢复完整；独立解码校验也确认该输入的真实正文仍为完整 4,096 字节。

## 验证结果

- GCC Release `test_transport/default` 连续 3/3 通过。
- 变更行 `git clang-format --diff HEAD -- tests/test_transport.c` 未发现格式改动；`git diff --check` 通过。
- 测试命令将 `TMPDIR` 与 `XMAKE_TMPDIR` 指向工作区 `build/test-tmp`；未在 `/tmp` 创建测试文件。

本次只增加集成测试、记录和开发版本，不改生产运行时、公开 API/ABI 或 wire。它验证一个未知长度帧实际输出超出 envelope 声明容量时被隔离；不覆盖短输出、所有输出边界/校验和组合或大规模 RSS 上界。#241/#235 与 V2 整体验收继续开放。
