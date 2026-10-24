# RPC V2 RESET 后迟到载荷释放与信用隔离回归

日期：2026-10-25。关联 #242/ERR-C。版本：`2.0.0-dev.45 -> 2.0.0-dev.46`。

## 覆盖范围

`test_reset_releases_held_payload_without_credit` 在真实 Reactor/Channel 上建立两条共享连接流。第一条
流交付的 DATA 载荷由上层持有，随后本地与对端以不同原因完成双向 RESET，Channel 退役旧流。此后才由
上层释放原载荷；该释放必须归还内存，但不能为已退役流产生新的 WINDOW_UPDATE。

测试在迟到释放后发送 PING，并要求收到的下一帧是对应 PONG，形成同一物理连接上的 TX FIFO 观察点。
如果旧流释放错误地产生窗口更新，它会先于 PONG 暴露。随后测试在同 Channel 的另一条活动流交付 DATA，
并成功完成 RESET，确认信用清理没有扩大到同伴流或整条连接。

## 验证

- GCC Release：完整 `test_stream_reset` 通过，输出 `stream_reset: wire, feature, FIFO, pressure, retry, retirement, ID exhaustion passed`。
- GCC TSan：完整 `test_stream_reset` 通过，同一输出且无数据竞争报告。
- `git diff --check` 通过。
- 仅增加测试与开发版本/记录，不修改生产 C 逻辑、公开 API/ABI 或线协议。

## 边界与风险

回归覆盖单条有界载荷、双向 RESET 完成后的应用释放和一个同 Channel 健康流。它不覆盖尚在
RESETTING 状态时的多批次消费、压缩工作区释放、多个接收窗口债务或多流同时 RESET。运行时性能无影响；
测试增加一条流的 RESET、延后释放、PING/PONG 屏障和同伴流交付步骤。

回滚可撤销 dev.46 提交，恢复 `xmake.lua` 与 README 版本并移除此测试及记录，然后重新运行 Release
`test_stream_reset`。

## 验收标准

1. 已退役流的迟到载荷释放返回陈旧状态并释放持有内存，不产生 WINDOW_UPDATE。
2. RESET 影响范围限制在目标流，同 Channel 同伴仍能传输并正常终止。
3. Release 与 TSan 下完整 `test_stream_reset` 均通过。
