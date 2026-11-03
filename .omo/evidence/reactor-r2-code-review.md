# Reactor R2 代码审查记录

## 审查基线

- 审查范围：基线 `3d4699f28e5433a5b7c4bb059f85716180b76517` 至 R2 初始修复 `0bedac6f0e4365f340b9487d87e80ec70bdfe0d4`。
- 结论：请求修改。
- 阻断级别：高。

## 发现

`tr_pipeline_listener_listen_ipv4()` 在 `tr_reactor_listener_register_publish()` 返回错误时无条件关闭 listener fd。若 epoll 添加成功、状态发布失败，而回滚 `EPOLL_CTL_DEL` 也失败，Reactor 会保留 source 发布；此时关闭 fd 会导致后续注销无法跨过解绑屏障，并可能阻塞 listener 的注册、停止和销毁。

审查指出，已有 Reactor 单元测试会在 fd 仍打开时重试注销，因此没有覆盖 Pipeline listener 的生产调用方。

## 修复和回归

后续修复为注册事务增加保留回调和必填结果参数。回滚仍保留 source 时，Reactor 在同一串行化区间通知调用方；Pipeline listener 记录 fd、屏蔽 accept，并在后续 drain 注销成功后关闭 fd。

新增 `test_listener_publish_rollback_keeps_fd_until_drain`，经生产入口注入发布失败和一次 `EIO`。未修复时测试因 fd 已被提前关闭而失败；修复后确认 fd 一直有效到 drain 成功。定向测试和全量 `xmake test -v -j1` 均通过，详见 [R2 测试证据](reactor-r2-test-evidence.md)。

## 复审状态

代码及测试已按发现修复，仍需对最终提交进行独立复审并等待 PR 最新检查完成后，才能关闭审查阻断。
