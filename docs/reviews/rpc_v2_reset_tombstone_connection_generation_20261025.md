# RPC V2 RESET 墓碑连接代次隔离回归

日期：2026-10-25。关联 #242/ERR-B。版本：`2.0.0-dev.43 -> 2.0.0-dev.44`。

## 覆盖范围

`test_tombstone_connection_generation` 使用真实 Reactor 与 Channel、Unix socketpair 驱动帧。
测试先在旧连接上完成流 ID 1 的双向 RESET，形成精确连接句柄、流 ID 和对端原因组成的
墓碑；关闭连接并等待旧连接槽变为空闲后，再将同一个 Reactor 槽位分配给新连接代次，替换
Channel 并完成 HELLO 握手。

新连接此时尚无流 ID 1。测试发送与旧墓碑相同的 RESET 原因，要求 Channel 将其视为新连接上的
未知流并报告协议错误。旧连接墓碑不能因流 ID 和状态相同而吞掉新代次上的帧。该结果同时证明
新连接的错误仍限定在当前连接；逻辑 Channel 的历史记录不会跨物理连接代次放宽协议校验。

既有 `test_invalid(EVICTED_RESET)` 覆盖墓碑容量上限和 FIFO 淘汰，`test_late_and_reuse` 覆盖
Stream 槽复用后的旧句柄陈旧。本切片将连接代次加入这两者之间的 RESET 边界矩阵。

## 验证

- Release：`xmake f -P . -m release`、`xmake build -P . -j 4 test_stream_reset`，运行输出
  `stream_reset: wire, feature, FIFO, pressure, retry, retirement, ID exhaustion passed`。
- TSan：`xmake f -P . -m tsan`、构建并运行 `test_stream_reset`，同一目标通过。
- `git diff --check` 通过。
- 只修改测试、开发版本和记录；不改变生产实现、公开 API/ABI 或协议。

## 边界与风险

回归覆盖共享 Channel 的一次连接替换、同一 Reactor 槽位的新连接代次和旧 tombstone 同 ID/原因
碰撞。它不覆盖拆分 CONTROL/BULK 连接、多个快速替换、连接组成员代次及 ERR-C 信用组合。运行时
性能无影响；测试新增一轮本地连接关闭、重建和握手。

回滚可撤销 dev.44 提交，恢复 `xmake.lua` 与 README 版本并移除此测试及记录；之后重新构建和运行
Release `test_stream_reset`。

## 验收标准

1. 旧连接的 RESET 墓碑在同 Reactor 槽位分配新代次后保留精确连接身份。
2. 新连接上的未知 RESET 不命中旧代次墓碑，并触发协议错误。
3. 既有 tombstone FIFO 淘汰与 Stream 槽代次回归继续通过。
4. Release 与 TSan `test_stream_reset` 均通过；#242 其余 ERR-B/C/D/E 不误标为完成。
