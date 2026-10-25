# 压缩持有期间 RAW 同伴进展回归

日期：2026-10-25。关联 #241。版本：`2.0.0-dev.78 -> 2.0.0-dev.79`。

## 目的

dev.78 验证应用持有一条 Zstandard 解压消息时，另一 DATA 成员仍能解压并交付自己的 Zstandard 消息。本切片在首消息仍被持有期间，再验证该同伴还能交付 RAW 消息，补上混合编码路径的边界。

## 场景与断言

- 使用 dev.78 的真实 TCP 双 DATA 连接组：stream `5001` 和 `5002` 分别绑定不同物理 DATA 连接。
- message `6611` 在第一成员上经 Zstandard 解压后由应用 `TAKE_OWNERSHIP`；测试保留其 128 字节缓冲。
- message `6612` 在第二成员上经 Zstandard 解压、逐字节校验并由服务端回调释放。
- 首消息仍被持有时，第二成员再接收 RAW message `6613`；服务端校验正文并释放。
- 两条同伴消息完成后再次校验首消息缓冲，再释放首消息并继续原有的 transfer drain、TX 排空与连接关闭验证。

该场景若把某成员的应用持有状态错误扩散为组级 RAW 阻塞，`6613` 等待会超时；若任何解压正文或 RAW 正文错误，固定内容断言失败。Zstandard 帧标志和不同物理 DATA 连接由 dev.78 的既有测试链接探针继续核验。

## 验证与边界

- GCC Release：完整 `test_connection_group_facade` 连续运行 5/5 通过。
- GCC TSan：完整目标通过，无数据竞争报告。
- GCC ASan/UBSan：完整目标通过，启用 leak 检测且无诊断。
- `git diff --check` 与 `git clang-format --diff HEAD -- tests/test_connection_group_facade.c` 通过。
- 只增加测试与记录、开发版本，不改生产实现、公开 API/ABI 或 wire 协议。
- 测试没有耗尽 Server 全局预算，也没有同时持有多条应用消息；全局预算耗尽、压缩预算失败后恢复和持续饱和公平性仍属于 #241/#235 后续验收。
