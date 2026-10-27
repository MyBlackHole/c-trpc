# 压缩与多 DATA 慢消费者隔离回归

日期：2026-10-25。关联 #241。版本：`2.0.0-dev.77 -> 2.0.0-dev.78`。

## 目的

dev.77 已证明第一条 DATA 成员上的应用持有一条 RAW 消息时，另一物理 DATA 成员仍可进展。本切片将同一真实 TCP 连接组场景扩展到 Zstandard：首成员交付解压后的消息并由应用持有，第二成员仍需独立完成压缩 DATA 接收、解压、正文校验、服务端交付和消息释放。

## 场景与断言

- 服务端为两条 DATA 连接启用 Zstandard 接收，客户端为两条连接启用显式 Zstandard 发送。
- stream `5001` 和 `5002` 分别绑定不同 DATA 物理连接；两个 128 字节正文使用固定模式，并由服务端回调逐字节校验。
- message `6611` 完成解压后由回调返回 `TR_CONNECTION_GROUP_MESSAGE_TAKE_OWNERSHIP`。测试确认缓冲内容仍有效，再发送 message `6612`。
- message `6612` 必须在首条消息仍被持有时完成解压、进入回调并由回调释放。首条缓冲在第二条处理后再次校验，并显式释放。
- 测试链接的 `tr_reactor_sendv_limited` 包装器确认目标两次 DATA 提交均设置 `TR_FRAME_F_COMPRESSED`；路由探针确认两个 message 的 Reactor 连接代次不同。
- 场景随后继续原有双 DATA transfer drain、已接纳 TX 排空与连接关闭验证。

若压缩选择在客户端被忽略，新增压缩提交计数断言失败；若解压后正文错误，固定正文断言失败；若一条已解压消息被持有会阻塞另一成员，第二消息等待失败；若错误路由到同一连接，物理连接代次断言失败。

## 夹具诊断

- 初始尝试将客户端帧 payload 上限设为 32 字节，以强制压缩数据分帧；真实组在 DATA route 前导阶段超时，未进入目标场景。该限制与待验证不变量无关，已移除，并用测试链接中的帧标志观察确认选定消息实际走压缩路径。
- 一次完整运行在最终单次 `tr_server_destroy()` 断言未即时收敛（当时未记录返回枚举）；改用项目已有的服务端销毁重试屏障，该屏障只对 `TR_ERR_STATE`/`TR_AGAIN` 重试并要求最终销毁成功。采用后完整 Release 目标连续 5/5 通过。
- 临时将客户端对 `6611/6612` 的压缩选择改为 NONE 时，新增压缩提交断言按预期失败；突变代码已恢复，生产实现没有改动。

## 验证与边界

- GCC Release：完整 `test_connection_group_facade` 连续 5/5 通过。
- GCC TSan：完整目标通过，无数据竞争报告。
- GCC ASan/UBSan：完整目标通过，启用 leak 检测且无诊断。
- `git diff --check` 与 `git clang-format --diff HEAD -- tests/test_connection_group_facade.c` 通过。
- 仅修改测试、README、V2 剩余工作台账、本记录与开发版本；不改运行时实现、公开 API/ABI 或 wire 协议。
- 测试只持有一条解压消息，没有耗尽 Server 全局预算；不覆盖 RAW/Zstandard 混合、多个应用持有者、压缩预算耗尽或持续饱和公平性。#241/#235 仍未完成。
