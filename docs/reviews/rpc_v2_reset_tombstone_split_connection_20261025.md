# RPC V2 RESET 墓碑 split 连接身份回归

日期：2026-10-25。关联 #242/ERR-B。版本：`2.0.0-dev.44 -> 2.0.0-dev.45`。

## 覆盖范围

`test_tombstone_split_connection_identity` 使用真实 split Channel、Reactor 和两组 Unix socketpair。
测试先在 CONTROL 连接上建立流 ID 1，完成双向 RESET 并保留墓碑，再在独立 BULK 连接上发送
相同流 ID 和原因的 RESET。BULK 上不存在该流；CONTROL 的历史墓碑不能跨物理连接匹配该帧，
所以 BULK 必须按未知流协议错误关闭。

回归继续检查错误作用域：BULK 进入 DOWN 后，CONTROL 保持 UP，并能接纳新流、交付 DATA、完成
正常双向 RESET。用例因此覆盖墓碑身份与 split lane 物理连接之间的组合，并证明坏帧不会关闭
另一条 lane。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，输出 `stream_reset: wire, feature, FIFO, pressure, retry, retirement, ID exhaustion passed`。
- GCC TSan：完整 `test_stream_reset` 通过，同一输出且无数据竞争报告。
- `git diff --check` 通过。
- 仅增加测试与开发版本/记录，不修改生产 C 逻辑、公开 API/ABI 或线协议。

## 边界与风险

该场景使用固定的 CONTROL 墓碑和一条 BULK 错误帧，不覆盖多次快速重连、多个同时存在的墓碑、
连接组成员代次或 ERR-C 信用组合。运行时性能无影响；测试新增 split Channel 双连接的 RESET、
错误隔离及新流交付步骤。

回滚可撤销 dev.45 提交，恢复 `xmake.lua` 与 README 版本，移除此用例和记录，然后重新运行
Release `test_stream_reset`。

## 验收标准

1. CONTROL 墓碑不匹配 BULK 物理连接上相同 ID/原因的 RESET。
2. BULK 因未知流协议错误退役后，CONTROL 保持可用并完成新流。
3. Release 与 TSan 下完整 `test_stream_reset` 均通过。
