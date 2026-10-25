# 大正文压缩持有期间 RAW 同伴进展回归

日期：2026-10-25。关联 #241、#235。版本：`2.0.0-dev.79 -> 2.0.0-dev.80`。

## 目的

扩大 #241 慢消费者隔离回归中的解压正文规模，并检验只有一个服务端压缩任务槽时，已交给应用持有的 Zstandard 输出不会阻塞另一物理 DATA 成员上的 RAW 交付。

## 场景与断言

- 使用真实 TCP 连接组、两个 DATA 成员及两个独立传输流；服务端 `max_inflight_compressed_messages` 设为 `1`。
- 第一 DATA 成员发送 1 MiB、可压缩的 Zstandard 正文。测试链接探针确认实际 DATA 提交带压缩标志；服务端逐字节校验解压内容并返回 `TAKE_OWNERSHIP`。
- 第一条解压消息继续被应用持有时，第二 DATA 成员发送 RAW 小消息。服务端校验正文并释放；探针确认两条 DATA 消息来自不同物理连接代次。
- RAW 同伴完成后再次逐字节校验仍被持有的 1 MiB 缓冲，再释放它并完成两侧 transfer drain 和关闭流程。

## 工作区预算修正

该回归在原 1 MiB Zstandard 工作区上限下观察到编码任务返回 `TR_MESSAGE_TRANSFORM_LIMIT`，DATA 帧未能提交。Zstandard 1 MiB 窗口的上下文及压缩表空间本身会超过 1 MiB。现把 `TR_GROUP_MESSAGE_WORKSPACE_BYTES` 提高到 4 MiB；`tr_group_message_budget_required` 已按任务容量乘入该工作区上限，因此压缩任务并发数仍直接限定总预算。每个配置任务槽的预算上限相应增加 3 MiB。低层监听器测试使用手工消息预算，也同步扩大以覆盖新工作区、输出与监听器常驻预算。

该调整保留固定的每任务工作区上限，没有改变公开 API/ABI、wire 格式或任务槽语义。实际变换仍在预算内运行，最大允许工作区由预算统一核算。

## 验证与边界

- GCC Release：完整 `test_connection_group_facade` 连续运行 5/5 通过。
- GCC TSan：完整 `test_connection_group_facade` 通过，无数据竞争报告。
- GCC ASan/UBSan：完整 `test_connection_group_facade` 通过，启用 leak detection 且无诊断。
- 项目完整 Release 测试集：91/91 通过。
- `git diff --check` 与 `git clang-format --diff HEAD` 检查通过。
- 用例证明 1 MiB 解压正文由应用持有时另一 DATA 成员的 RAW 消息可进展；它没有耗尽 Server 全局预算，不覆盖多个应用消息并发持有、压缩预算耗尽后的恢复或持续饱和公平性。
