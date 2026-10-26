# RPC V2 RESET 时跨消息信用债务隔离回归

日期：2026-10-26。关联 #242/ERR-C。版本：`2.0.0-dev.127 -> 2.0.0-dev.128`。

## 覆盖范围

新增 `test_reset_release_preserves_cross_message_credit_debt`。Channel 初始窗口为 256 字节，协商的最大消息
长度为 128 字节，WINDOW_UPDATE 阈值为 128 字节。目标流依次接收并立即释放两条 64 字节消息：首条只留下
64 字节待通告债务，不得提前发 WINDOW_UPDATE；第二条累积到阈值后，通告上限必须恰好推进到 384。

随后目标流收到一条 128 字节消息，由应用持有。对端发起 RESET，测试阻塞 Reactor owner 的 RESET 回调，
在流已进入 RESETTING、槽位尚未退役时由应用线程释放该载荷。累计接收与消费量均为 256 字节，但通告上限
必须保持在 384，不得将跨消息债务再次变成新的信用。解除闸门后用 owner 命令栅栏和 PING/PONG 检查 wire，
并验证同 Channel 的同伴流仍能交付 DATA 和完成 RESET。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，真实 Channel tombstone 容量 40,000。
- GCC TSan（`halt_on_error=1:second_deadlock_stack=1`）：完整 `test_stream_reset` 通过，容量 5,000，无数据竞争报告。
- GCC ASan/UBSan（启用泄漏检测并在错误时停止）：完整 `test_stream_reset` 通过，容量 40,000。
- 敏感性验证：临时移除 `rx_credit_stopped` 保护后，本新增用例自身在 advertised limit 断言处失败；生产文件已恢复。
- 新增测试行经当前环境 clang-format 23.1.3 格式化，`git diff --check` 通过；项目固定版 clang-format 18.1.8 不在此环境。
- 测试临时目录、缓存和构建产物均位于工作区 `build/err-c-cross-message-credit/`。

## 范围与限制

本切片覆盖单条流的三条 RAW 消息、一次阈值累计、一次 RESETTING 窗口释放和一个健康同伴流；不覆盖压缩工作区
释放、多个同时持有的载荷、多个流同步 RESET 或长期压力，也不代表 ERR-C 全矩阵、#242 或 V2 整体验收完成。
生产运行时代码不变，无性能影响；测试增加三条消息、一次 RESET 同步闸门、一次 owner 栅栏和同伴流验证。

回滚可撤销 dev.128 提交，移除本测试与报告并将 `xmake.lua`、README 版本恢复为 dev.127，然后重跑 Release
`test_stream_reset`。

## 验收标准

1. 两条小消息累计到既定更新阈值后仅通告一次正确的窗口上限。
2. RESETTING 窗口释放下一条已交付载荷，不增加通告上限或发送迟到 WINDOW_UPDATE。
3. 同 Channel 同伴流保持可用；Release、TSan 与 ASan/UBSan 下完整 `test_stream_reset` 均通过。
