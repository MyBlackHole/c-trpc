# RPC V2 DATA TX 容量恢复通知实施计划

> 执行约束：当前主会话顺序实施，不使用子 agent。测试临时文件、日志、缓存及构建产物全部放在本 worktree `build/`。

**Goal:** DATA TX item pool 归还后，在有界 Reactor owner 调度内唤醒等待的压缩分帧器，避免仅依赖 25ms timer。

**Architecture:** Reactor 为普通 DATA TX pool 添加每 Connection 一个 intrusive waiter 和 FIFO 调度队列，容量释放只 eventfd 唤醒。Channel 仅在 DATA send 返回 pool-exhausted `TR_AGAIN` 时 watch；timer 保留为兜底。

**Spec:** `docs/superpowers/specs/2026-10-24-rpc-v2-data-tx-ready-design.md`

## 工作项

- [x] Reactor DATA pool watcher 的库内合同与测试先行。
- [x] 实现队列、跨线程唤醒、FIFO、公平预算、撤销/代次回收。
- [x] Channel 压缩 DATA `TR_AGAIN` 接入 watcher，并保留 timer fallback。
- [x] 增加真实 Channel 压力解除集成回归。
- [x] 版本 `2.0.0-dev.26 -> 2.0.0-dev.27`，文档与变更记录。
- [x] 聚焦、全量 Release、ASan/UBSan、TSan 和修改行格式验收。
- [ ] 提交、推送并创建依赖 PR Draft，完成提交级格式门禁。

## 验证命令约束

Xmake `TMPDIR`、`XMAKE_TMPDIR` 指向本 worktree 的 `build/data-tx-ready-247/tmp` 与
`build/data-tx-ready-247/xmake-tmp`；Release 构建目录使用标准 `build/`，以保留
TLS identity 测试寻找仓库根目录的约定。
