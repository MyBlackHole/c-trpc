# RPC V2 RESETTING 期间释放载荷的信用隔离回归

日期：2026-10-26。关联 #242/ERR-C。版本：`2.0.0-dev.126 -> 2.0.0-dev.127`。

## 覆盖范围

新增 `test_reset_begun_held_payload_release_without_credit`。测试建立两条共享 Channel 的流，目标流已向
应用交付 128 字节 DATA 并由应用持有。对端发起 RESET 后，测试在 Reactor owner 的 RESET 回调中设置同步
闸门；该回调发生在流进入 RESETTING 后、RESET 进度处理和槽位退役之前。应用线程在此窗口释放载荷，验证
接收字节只记为已消费、不增加 advertised limit。恢复 owner 回调并完成 RESET 后，通过 Reactor 命令栅栏和
PING/PONG 观察 wire，确认目标流没有迟到 WINDOW_UPDATE；同 Channel 的同伴流仍可交付 DATA 并完成 RESET。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，真实 Channel tombstone 容量 40,000。
- GCC TSan（`halt_on_error=1:second_deadlock_stack=1`）：完整 `test_stream_reset` 通过，容量 5,000，无数据竞争报告。
- GCC ASan/UBSan（开启泄漏检测并在错误时停止）：完整 `test_stream_reset` 通过，容量 40,000。
- 敏感性验证：临时移除生产代码对 `rx_credit_stopped` 的保护后，新断言按预期失败；生产文件随即恢复，当前切片不修改生产 C 逻辑。
- 新增代码使用当前环境 clang-format 23.1.3 格式化，`git diff --check` 通过；项目固定 18.1.8 不在此环境中。
- 测试临时目录、缓存及构建产物均位于工作区 `build/err-c-reset-release-race/`。

## 范围与限制

本切片只覆盖单条流的一次 RESETTING 窗口和一份已交付载荷，并检查一个健康同伴流；不覆盖多批次信用债务、压缩预算释放或多流同时 RESET，也不代表 ERR-C 全矩阵、#242 或 V2 整体验收完成。运行时无性能影响；测试增加同步闸门、一次载荷释放、一个 wire 屏障和同伴流验证。

回滚可撤销 dev.127 提交，移除本测试和报告并将 `xmake.lua`、README 版本恢复为 dev.126，然后重新运行 Release `test_stream_reset`。

## 验收标准

1. 应用在 RESETTING 窗口释放已交付载荷时不增加该流接收信用，也不产生迟到 WINDOW_UPDATE。
2. 目标流 RESET 完成后，同 Channel 同伴仍能收发并正常终止。
3. Release、TSan 与 ASan/UBSan 下完整 `test_stream_reset` 均通过。
