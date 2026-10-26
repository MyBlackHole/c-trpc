# RPC V2 RESETTING 压缩解码正文信用隔离回归

日期：2026-10-26。关联 #242/ERR-C。版本：`2.0.0-dev.128 -> 2.0.0-dev.129`。

## 覆盖范围

扩展真实 TCP、Channel 与消息服务集成测试。客户端发送有效 Zstandard 压缩消息，服务端完成真实解码并由应用回调持有解压后的 payload。客户端随后发起 RESET；测试阻塞服务端 RESET 事件回调，使目标流已经进入 RESETTING、但流槽位尚未退役。此时由应用线程释放解压后的 payload。

压缩 DATA 的接收信用已在读取 wire 字节时记账，解压正文长度不能再次作为 wire 消费量返还。测试在释放前后比较目标流的 `rx_received_bytes`、`rx_consumed_bytes` 和 `rx_advertised_limit`，并比较 Channel 的 `window_updates_tx` 计数，要求所有值保持不变。释放完成后解除 RESET 回调闸门并等待双向 RESET 收敛；同 Channel 上仍活动的压缩同伴继续发送第二条消息并被完整解码交付。

## 验证

- GCC Release：完整 `test_transport` 通过，8.664 秒。
- GCC TSan（`halt_on_error=1:second_deadlock_stack=1`）：完整 `test_transport` 通过，8.954 秒，无数据竞争报告。
- GCC ASan/UBSan（启用泄漏检测并在错误时停止）：完整 `test_transport` 通过，8.919 秒。
- 敏感性验证：临时修改消费字节上限，使 RESETTING 中释放的解压正文长度进入消费计数；新用例在 `rx_consumed_bytes` 不变量处失败。生产代码已恢复，Release、TSan 与 ASan/UBSan 均在恢复后通过。
- 当前环境 Xmake 为 3.1.2+20261016。构建和测试临时目录位于工作区 `build/test-tmp/`，未使用 `/tmp`。
- `git diff --check` 通过。

## 影响与限制

本切片只修改 `test_transport` 测试辅助状态、测试回归、版本和文档；生产运行时代码、公开 API/ABI、协议线格式均未改变。运行时性能无影响。测试额外执行一次 Zstandard 编解码、一次 RESET 和同伴流的一次后续发送。

该场景覆盖单条压缩消息、一个持有载荷、一次 RESETTING 窗口释放和一个存活压缩同伴；不覆盖发送队列阻塞期间累计的压缩信用债务、多个并发 RESET、split 连接或长期压力。ERR-C 其他信用边界以及 #242/V2 全部验收仍未完成。

## 回滚方案

撤销 dev.129 提交，移除本测试扩展与报告，并将 `xmake.lua` 和 README 版本恢复为 dev.128；随后在 Release 下重跑完整 `test_transport`。

## 验收标准

1. RESETTING 窗口释放已交付的解压正文只释放内存，不能重复增加 wire 消费量。
2. 释放前后目标流的接收量、消费量、通告信用和 WINDOW_UPDATE 发送计数保持不变。
3. RESET 完成后，同 Channel 存活压缩流仍能收发并正确交付正文。
4. Release、TSan、ASan/UBSan 完整 `test_transport` 均通过，敏感性变异由消费量断言捕获。
