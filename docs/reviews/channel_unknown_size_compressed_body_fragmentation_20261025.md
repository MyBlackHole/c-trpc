# 未知长度压缩 body 跨 DATA 帧接收回归

日期：2026-10-25。版本：`2.0.0-dev.115 -> 2.0.0-dev.116`。关联 #241、#235、PR #324。

## 目标与场景

组合未知帧内容长度与压缩 body 跨 DATA 帧接收，避免两项各自覆盖、组合路径却没有真实 Channel 回归。接收端只从 envelope 获得解码长度，Zstandard 帧头不提供 content size。

## 输入与断言

- 使用 `ZSTD_c_contentSizeFlag=0` 编码有效帧，并断言 `ZSTD_getFrameContentSize()` 返回 `ZSTD_CONTENTSIZE_UNKNOWN`。
- 在压缩 body 中点切分：首个 DATA 帧携带完整 message envelope 与部分 body，后续 DATA 帧携带其余 body。
- 通过真实 TCP Shared Channel 完成重组和流式解码，服务端交付两条正文；消息 ID、解码长度与正文逐字节匹配。
- 检查 wire 接收/消费计数相等，并确认缓冲池归还。

## 验证结果

- 新增的未知 content size 断言在修改前按预期失败，确认该分片用例原先使用已知帧长度。
- GCC Release `test_transport/default` 在最终版本下连续 3/3 通过。
- `git clang-format --diff HEAD -- tests/test_transport.c` 未发现格式改动；`git diff --check` 通过。
- 测试命令将 `TMPDIR` 与 `XMAKE_TMPDIR` 指向工作区 `build/test-tmp`；未在 `/tmp` 创建测试文件。

本次仅扩展测试、记录和开发版本，不改生产运行时、公开 API/ABI 或 wire。该回归覆盖未知长度帧与两段 body 分片的组合，不覆盖更多 DATA 分片、取消交错或更广预算矩阵。#241/#235 与 V2 整体验收继续开放。
