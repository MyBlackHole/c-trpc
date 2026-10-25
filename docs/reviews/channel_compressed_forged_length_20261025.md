# 压缩 envelope 虚报原始长度隔离回归

日期：2026-10-25。版本：`2.0.0-dev.110 -> 2.0.0-dev.111`。关联 #241、#235、PR #324。

## 目标与场景

验证真实 Channel 会隔离 envelope 与有效压缩帧不一致的消息，避免把压缩帧的实际输出截短或交付成另一种正文。现有 Channel 负向用例的若干输入在 Zstandard 帧解析前就已无效；本回归使用可正常解压的帧，并只伪造外层声明的原始长度。

## 输入与断言

- 使用 Zstandard 正常压缩既有 `source` 正文，确认编码本身成功；帧头携带真实原始长度。
- 外层 message envelope 声明的解码长度比真实正文少 1 字节，编码长度仍与有效压缩体一致。
- 通过真实 TCP Shared Channel 提交该消息；服务端隔离该流并发送 RESET，RESET 状态为 `TR_ERR_STATE`。
- helper 校验应用交付数保持 8，活动流数恢复到既有基线，接收缓冲池完全归还；随后 trailing-data 和 DATA lane 标志负向用例继续通过。

## 验证结果

- GCC Release `test_transport/default` 连续 3/3 通过。
- 变更行 `git clang-format --diff HEAD -- tests/test_transport.c` 未发现格式改动；`git diff --check` 通过。
- 测试命令将 `TMPDIR` 与 `XMAKE_TMPDIR` 指向工作区 `build/test-tmp`；未在 `/tmp` 创建测试文件。

本次只增加集成测试、记录和开发版本，不改生产运行时、公开 API/ABI 或 wire。该回归覆盖已知帧长度与 envelope 声明不一致且实际压缩体有效的拒绝路径；未知帧长度下的短输出/超长输出、恶意输入安全和更广资源组合仍需单独验收。#241/#235 与 V2 整体验收继续开放。
