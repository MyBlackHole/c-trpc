# 未知帧内容长度压缩消息接收回归

日期：2026-10-25。版本：`2.0.0-dev.109 -> 2.0.0-dev.110`。关联 #241、#235、PR #324。

## 目标与场景

验证 Zstandard 帧头不携带解码长度时，真实 TCP Channel 仍能依据 message envelope 中已校验、已预约的解码长度完成有限解码；测试输入同时采用小编码体和 4 KiB 解码正文，与 dev.109 已有帧头包含内容长度的高扩张比用例形成对照。

## 输入与断言

- 使用 `ZSTD_CCtx_setParameter(..., ZSTD_c_contentSizeFlag, 0)` 生成帧，并通过 `ZSTD_getFrameContentSize` 断言其值为 `ZSTD_CONTENTSIZE_UNKNOWN`。
- 外层 message envelope 声明编码长度和 4,096 字节解码长度；Zstandard 帧本身不提供该原始长度。
- 将完整 envelope 与压缩帧作为压缩 DATA 消息送入真实 TCP Shared Channel。消息 wire 小于 128 字节初始窗口。
- 服务端完成解码和回调交付，核对 stream ID、message ID、4,096 字节长度及正文逐字节内容；并确认接收流累计收到和消费的 wire 字节正好增加该消息长度。
- 后续 L=W 边界消息、非法压缩输入和 RESET 计数仍逐项断言，避免新增成功路径掩盖原负向覆盖。

## 验证结果

- GCC Release `test_transport/default` 连续 3/3 通过。
- 变更行 `git clang-format --diff HEAD -- tests/test_transport.c` 未发现格式改动；`git diff --check` 通过。
- 测试命令将 `TMPDIR` 与 `XMAKE_TMPDIR` 指向工作区 `build/test-tmp`；未在 `/tmp` 创建测试文件。

本次只增加集成测试、记录与开发版本，不改生产运行时、公开 API/ABI 或 wire。该回归证明当前 decoder 支持未知帧 content size，并将真实输出限制为 envelope 声明的已预约容量；不覆盖伪造长度的所有组合、超过配置的窗口拒绝、恶意压缩流安全或大规模 RSS。#241/#235 和 V2 整体验收仍保持开放。
