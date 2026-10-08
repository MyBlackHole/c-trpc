# Connector R3 测试证据

## 代码范围

- 基线：`944e22d0eb9221717d47da40e3c102f02b222083`
- 修复提交：`b2977a03f8bcce863b1495f4b8b629c1a33f67c7`
- 验证日期：2026-10-08

## TDD 红灯复现

- 命令：`xmake run test_connector`
- 执行时点：新增 socketpair 回归已编译，生产修复尚未修改。
- 退出码：`255`。
- 关键输出：`test_timeout_status_survives_writable_event` 中 `ctx.status == TR_ERR_TIMEOUT` 断言失败。旧实现的可写事件继续推进 preface 并以 `TR_OK` 完成，证明超时结果会被覆盖。

## 定向测试

- 命令：`xmake test -v -j1 'test_connector/*'`
- 退出码：`0`。
- 结果：`test_connector/default passed`，新增超时优先、完成优先取消、取消优先三种状态顺序均通过。

## 时序重复测试

- 命令：连续执行 20 次 `xmake run test_connector`。
- 退出码：20 次均为 `0`。
- 结果：`20/20` 通过。测试使用真实 Reactor 和 socketpair；发送端填充至 `poll()` 不再报告可写后，注入一次 aux unregister 失败，再按场景放行可写事件。

## 全量测试

- 命令：`xmake test -v -j1`
- 退出码：`0`。
- 关键输出：`100% tests passed, 0 test(s) failed out of 38, spent 13.347s`。
- 结果：38 项全部通过，包含 Connector、客户端连接回滚、Reactor、RPC 与运行时测试。

## GitHub Actions

- PR：[#175](https://github.com/MyBlackHole/c-trpc/pull/175)
- 结果：17 项检查全部通过，覆盖 gcc/clang 构建测试、ASan+UBSan、ThreadSanitizer、Make 兼容安装、CRC32C 和 RPC smoke。

## 补充

- `git diff --check` 退出码为 `0`。
- 此记录只覆盖 Issue #154 的 R3 子项，不代表总审查 Issue 已关闭。
