# 公开 Server 慢消费者期间 RAW 突发回归

日期：2026-10-25。版本：`2.0.0-dev.82 -> 2.0.0-dev.83`。关联 #241、#235。

## 验收目标

扩展公开 Server 的真实慢消费者场景，验证多个 Zstandard 解码结果由应用持有时，另一条 DATA 物理成员可连续处理一组 RAW 消息，并确保这些消息没有被重新路由到已承载压缩流的成员。

## 场景与断言

- 沿用真实 TCP `tr_server`/`tr_client` 连接组及两个 DATA 连接。已有路由探针确认压缩 transfer 和 RAW transfer 落在不同物理连接。
- Server 回调通过公开 `TAKE_OWNERSHIP` API 同时持有六条真实 Zstandard 解码输出，正文合计 5,308,416 字节。
- 持有期间，客户端向第二 transfer 连续提交 64 条单字节 RAW 消息；每条使用不同消息 ID 和递增正文值。Server 回调逐条检查 ID 范围、Stream、DATA 标志、长度和字节，并释放消息。
- 测试链接探针记录首条 RAW 消息的物理连接，并逐条断言后续突发仍使用同一 Reactor、slot 和 generation。该连接与压缩流所在连接不同。
- 等待完整 64 条回调后，再逐字节检查所有六条仍由应用持有的 Zstandard 输出；随后释放正文并排空、销毁连接组。

## 验证

- GCC Release 下 `test_connection_group_facade` 通过，完整二进制连续运行 5/5。
- GCC ASan/UBSan（`detect_leaks=1`）下完整 `test_connection_group_facade` 通过。
- GCC TSan 下完整 `test_connection_group_facade` 通过。
- GCC Release 完整测试套件 91/91 通过，耗时 141.601 秒。
- `git diff --check` 通过；系统 clang-format 23.1.3 的改动行差异检查无格式改动。项目固定要求 clang-format 18.1.8，该精确版本未执行。

## 结论边界

该回归证明有限 64 条 RAW 突发在多条大解码正文仍被应用持有时可通过另一 DATA 物理连接完成；它不模拟无限/持续饱和源，不测量公平延迟或吞吐，不观察 Server 私有预算账本，因此不能宣称精确自然耗尽或持续饱和公平性已通过。这是测试、文档与开发版本改动，无生产 API/ABI、wire 或运行时行为变化。
