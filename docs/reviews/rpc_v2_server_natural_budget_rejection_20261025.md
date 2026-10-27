# 公开 Server 自然预算拒绝期间 RAW 同伴进展

日期：2026-10-25。版本：`2.0.0-dev.83 -> 2.0.0-dev.84`。关联 #241、#235。

## 验收目标

在公开 Server 按配置派生的真实共享消息预算下，不通过测试预约额度或制造分配，验证应用持有压缩输出会自然导致下一项完整解码任务因预算不足被拒绝；拒绝关闭承载压缩流的 DATA 物理连接后，另一成员上的 RAW 同伴仍能进展。

## 场景与证据

- 测试通过真实公开 `tr_server`/`tr_client` 建立两条 DATA 物理连接和两个 transfer。Server 启用 Zstandard，最大解码正文 1 MiB、任务容量 8。
- 仅在 `test_connection_group_facade` 链接中包装 `tr_pipeline_listener_create`，捕获公开 Server 实际传给 listener 的 `message_budget` 指针。测试直接读取该预算的 limit、current 和 rejection 计数；生产代码没有测试钩子或行为变更。
- 压缩流逐条提交真实 1 MiB Zstandard 输出并通过公开消息所有权回调持有。36 条输出（36 MiB）都被保留后，下一项解码预算申请被拒绝，Server 关闭该压缩 DATA 连接。
- 同一预算观察显示 `limit_bytes=46,908,160`、`current_bytes=42,147,072`、`rejection_events=1`；剩余 4,761,088 字节，小于下一任务至少需要的 1 MiB 输出与 4 MiB Zstandard 工作区之和（5,242,880 字节）。测试明确验证真实预算请求拒绝，而不要求 current 数值等于 limit。
- 压缩成员关闭且 36 条输出仍被持有期间，第二 DATA 成员继续交付并校验 64 条 RAW 消息。链接期路由探针确认压缩流和整个 RAW 突发位于不同物理连接；突发之后再次验证 36 条正文完整，再全部释放并排空 Server/Client。

## 验证

- GCC Release 下完整 `test_connection_group_facade` 连续 5/5 通过，重复测得 36 条持有输出、一次预算拒绝和相同预算值。
- GCC ASan/UBSan（`detect_leaks=1`）及 TSan 下完整 `test_connection_group_facade` 均通过。
- GCC Release 完整测试套件 91/91 通过，耗时 142.154 秒。
- `git diff --check` 和系统 clang-format 23.1.3 改动行复核通过；项目固定 clang-format 18.1.8 未执行。

## 结论边界

该测试证明自然产生的应用持有输出会使下一项真实任务预约因可用预算不足被拒绝，并验证拒绝后另一个 DATA 成员仍交付 64 条 RAW 消息。拒绝发生时 current 仍低于 limit，因为剩余空间小于下一任务的一次完整预留；因此不宣称 current 数值精确等于 limit。有限突发也不测量持续公平延迟/吞吐，不代表持续饱和公平性已通过。本次仅测试、文档、版本与测试链接包装改动，无生产 API/ABI、wire 或运行时行为变化。
