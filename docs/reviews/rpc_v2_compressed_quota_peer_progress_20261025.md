# RPC V2 压缩解码配额失败与同伴恢复回归

日期：2026-10-25。关联 #242/ERR-A、ERR-C。版本：`2.0.0-dev.48 -> 2.0.0-dev.49`。

## 覆盖范围

`test_compressed_workspace_exhaustion_preserves_peer` 使用真实 TCP、双 Reactor、共享 Channel 和 RPC Record Stream。
测试同时建立两条调用并让两条 OPEN 都进入服务端处理，然后将服务端预算压到只剩 512 KiB。第一条调用收到完整
Zstandard MESSAGE，解码工作区预约失败后只以 `RESOURCE_EXHAUSTED` 本地终结，不交付 MESSAGE；测试通过
`TR_ERR_NOMEM` RESET 通知客户端，客户端映射到相同 RPC 状态并只终结一次。

配额仍被占用时，双方共享 Channel 的 CONTROL 与 BULK lane 都保持 UP，失败调用以外的同伴调用仍活动。释放预算后，
第二条调用发送并成功解码压缩正文，完成 HALF_CLOSE、响应和 `OK` 终局。测试检查压缩标志、解码长度、正文校验和、
四个终局回调唯一及缓存归零。

## 验证

- GCC Release：完整 `test_rpc_record_stream` 通过，包含 `compressed workspace exhaustion preserves peer passed`。
- GCC TSan：完整 `test_rpc_record_stream` 通过，未报告数据竞争。
- `git diff --check` 通过。
- 只增加真实网络回归、开发版本和记录；不修改生产 C、公开 API/ABI 或线协议。

## 边界与风险

本用例验证一种 Zstandard 解码工作区预算不足、上层 RESET 通知及恢复后同伴完成的组合。它不覆盖压缩流重组预算、
多解码任务同时耗尽、信用压力、连接组 CONTROL/DATA 不一致或 ERR-A 的完整错误分类矩阵。生产运行时性能无影响；
测试在既有目标中多运行一个双 Reactor 场景。

回滚可撤销 dev.49 提交，恢复 `xmake.lua` 与 README 版本并移除此回归和记录，再运行 Release
`test_rpc_record_stream`。

## 验收标准

1. 解码工作区预算不足时，仅受影响调用以 `RESOURCE_EXHAUSTED` 终结且不交付正文。
2. RESET 成功到达对端，双方终局唯一，CONTROL/BULK lane 仍处于 UP。
3. 释放预算后，同 Channel 压缩同伴调用正常完成并校验正文。
4. Release 与 TSan 下完整 `test_rpc_record_stream` 均通过。
