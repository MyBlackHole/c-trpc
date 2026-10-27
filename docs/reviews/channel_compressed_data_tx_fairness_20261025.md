# Shared Channel 多帧压缩 DATA 池恢复公平轮转回归

日期：2026-10-25。版本：`2.0.0-dev.96 -> 2.0.0-dev.97`。关联 #241、#235、PR #324。

## 问题描述

Shared Channel 的 DATA TX 池只有一个项目时，BULK RAW Stream 占住该项目；CONTROL 与 BULK
各有压缩消息等待池容量。解除 socket 阻塞后，同一存活 Stream 可能连续发送多个压缩 DATA
分片，使另一个仍在等待的 Stream 无法及时取得发送机会。复现条件是压缩 wire 大于单帧上限、
共享池容量为 1，且至少两个存活压缩发送者在池耗尽期间等待。

## 原因分析

`tr_channel_compressed_tx_progress_on_owner` 成功发送一个分片后将
`tx_schedule_cursor` 移到下一个 Stream。下一个 Stream 因 DATA 池满返回 `TR_AGAIN` 时，原
逻辑仍将游标移到它之后。池项归还后，下一轮于是从先前成功发送的 Stream 开始，连续选中它。

## 解决方案

当发送返回 `TR_AGAIN` 且明确因为 DATA 池耗尽时，将游标设回当前未取得发送机会的 Stream
槽位；成功发送和其他错误路径维持原有游标推进行为。测试将三条 Zstandard 正文改为固定种子
生成的高熵数据，并将 DATA 最大帧长设为 128 字节，确保 surviving CONTROL/BULK 消息均为多帧；
帧追踪检查池恢复后的前两个成功分片来自不同 Stream，且 RESET Stream 没有 DATA 残留。

## 影响范围

影响 Channel 内部压缩 DATA 调度，仅覆盖共享 DATA TX 池耗尽并通过容量就绪通知恢复的路径；
公开 API、ABI、协议帧格式和 lane 编码不变。风险等级：低。

## 性能影响

不新增分配、锁或全表扫描；仅在既有发送选择后针对池耗尽多执行一次条件分支并保留游标位置。
未建立吞吐或尾延迟基线，因此不据此宣称性能提升。

## 复现步骤

1. 配置 Shared Channel 和唯一 DATA TX 池项。
2. 令 BULK RAW 发送遇到 `EAGAIN` 并占用该项。
3. CONTROL 与 BULK 提交大于 128 字节压缩 wire 的 Zstandard 消息，再 RESET 第三条等待流。
4. 解除 socket 阻塞，观察池恢复后的前两个成功压缩 DATA 帧。
5. 修复前回归在 `first_stream != second_stream` 断言处失败；修复后前两帧由两个存活 Stream 分别发送。

执行命令：

```sh
TMPDIR="$PWD/build/test-tmp" XMAKE_TMPDIR="$PWD/build/test-tmp" \
  xmake test -P . -r test_transport/default
```

## 测试验证

- GCC Release `test_transport/default`：修复后连续 2/2 通过。
- GCC Release 全量测试：91/91 通过，耗时 53.237 秒。
- clang-format 18.1.8 修改行检查通过，`git diff --check` 通过。
- 帧追踪还确认 RESET Stream 未发送 DATA，DATA TX 池归零，预算最终归还。

## 相关 Issue

GitHub #241（压缩 DATA 资源与活性），#235（V2 网络压缩），PR #324（当前持续开发验证）。

## 风险评估

低。游标只在 DATA 池明确耗尽时回到尚未发送的候选槽，减少该路径的重复选择；其他重试、
错误处理和成功推进保持原状。多于两个存活等待者的长期公平性仍需后续压力场景验证。

## 回滚方案

执行 `git revert <本次修复提交>` 回滚生产游标和对应回归；版本号按项目后续版本策略另行递增，
不得直接复用已发布 dev.97。

## 版本变更

- `xmake.lua`：`2.0.0-dev.96 -> 2.0.0-dev.97`
- `README.md`：候选版本及 dev.97 证据记录同步更新。
