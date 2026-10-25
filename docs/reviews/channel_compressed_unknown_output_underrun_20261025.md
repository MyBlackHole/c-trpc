# 未知长度帧短于 envelope 声明长度回归

日期：2026-10-25。版本：`2.0.0-dev.113 -> 2.0.0-dev.114`。关联 #241、#235、PR #324。

## 目标与场景

补齐未知帧 content size 时实际输出短于 envelope 声明长度的负向路径。它与 dev.113 的超长输出用例形成两侧边界，确认只有完整、精确匹配声明容量的帧能交付。

## 输入与断言

- 使用 `ZSTD_c_contentSizeFlag=0` 将既有 `source` 的前 `source_len-1` 字节压缩成有效帧，并确认帧头为 `ZSTD_CONTENTSIZE_UNKNOWN`。
- 独立 Zstandard 解码验证帧的真实输出恰为 `source_len-1`；envelope 则声明 `source_len`，比实际输出多 1 字节。
- 通过真实 TCP Shared Channel 接收后，帧正常结束但未填满 envelope 声明容量，变换以 BAD_DATA 结束；Channel 将当前流 RESET 为 `TR_ERR_STATE`。
- 服务端应用交付数保持 8，活动流回到 3，接收缓冲池恢复完整；此前未知长度帧的精确匹配、超长输出和窗口超限场景仍继续执行。

## 验证结果

- GCC Release `test_transport/default` 连续 3/3 通过。
- 变更行 `git clang-format --diff HEAD -- tests/test_transport.c` 未发现格式改动；`git diff --check` 通过。
- 测试命令将 `TMPDIR` 与 `XMAKE_TMPDIR` 指向工作区 `build/test-tmp`；未在 `/tmp` 创建测试文件。

本次只增加集成测试、记录和开发版本，不改生产运行时、公开 API/ABI 或 wire。该回归验证当前解码器拒绝 envelope 未填满的未知长度帧；不覆盖任意短缺量、字节级分片和更广预算/取消组合。#241/#235 与 V2 整体验收继续开放。
