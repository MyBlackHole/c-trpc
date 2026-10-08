# Reactor R2 测试证据

## 审查范围

- Issue：[#154 R2](https://github.com/MyBlackHole/c-trpc/issues/154)
- 基线：`3d4699f28e5433a5b7c4bb059f85716180b76517`
- 修复提交：`8f833aa`
- 验证日期：2026-10-08
- 范围：listener、peer event 和 aux event 的 `EPOLL_CTL_DEL` 失败语义及 fd 身份契约。

## TDD 红灯复现

- 命令：`xmake run test_reactor_source_detach`
- 执行时点：新增真实 fd 复用回归已编译，生产修复尚未修改。
- 退出码：`255`。
- 关键断言：`tr_reactor_aux_event_unregister(reactor, registered_fd) == TR_ERR_SYS` 失败。
- 原因：旧实现把 `EBADF` 当作解绑成功。测试已保留原 eventfd 的 `dup`，关闭注册 fd 后旧 epoll 事件仍可读。

## 回归覆盖

测试对同一个注册 fd 数值覆盖三种情形：

| fd 当前状态 | `EPOLL_CTL_DEL` 结果 | 检查内容 |
| --- | --- | --- |
| 已关闭，旧 open file description 仍由 dup 保留 | `EBADF` | 旧 token 仍在 epoll 中；注销报错；回调发布保留 |
| 复用为 `/dev/null` | `EPERM` | 旧 token 仍在 epoll 中；注销报错；回调发布保留 |
| 复用为另一条管道 | `ENOENT` | 旧 token 仍在 epoll 中；注销报错；回调发布保留 |

测试还启动真实 Reactor，确认失败的注销不会清除 callback arg；回调通过保留的 dup 消费旧 eventfd，再在 Reactor 销毁后释放测试上下文。

## 修复后验证

- 目标测试：`xmake build test_reactor_source_detach` 与 `xmake run test_reactor_source_detach`，均退出码 `0`。
- 全量测试：`xmake test -v -j1`，退出码 `0`。
- 全量结果：`38/38` 项通过，耗时 `13.371s`。
- 差异检查：`git diff --check` 退出码 `0`。
- 当前记录不包含 GitHub Actions 结果；PR CI 通过前不将跨架构或 sanitizer 门禁记为完成。

## 结论边界

该证据只覆盖 #154 的 R2。修复规定外部事件源借用 fd 必须保持注册时的数值和 open file description，直到注销成功；dup 可以存在，但不能提前关闭或复用注册用 fd。任何 `EPOLL_CTL_DEL` 错误均不再充当生命周期屏障。

## 调用方回滚缺口回归

代码审查发现 Pipeline listener 在 `register_publish()` 发布失败、回滚 `EPOLL_CTL_DEL` 也失败时，会提前关闭仍由 Reactor 发布的 fd。新增生产入口回归通过链接包装器注入发布失败和一次 `EIO`，覆盖真实 `tr_pipeline_listener_listen_ipv4()` 调用路径。

### TDD 红灯复现

- 命令：`xmake run test_pipeline_listener`
- 执行时点：回归测试已加入，所有权修复尚未修改。
- 退出码：`255`。
- 关键断言：`fcntl(injected_listener_fd, F_GETFD) >= 0` 失败。
- 原因：撤销屏障失败后，生产调用方关闭了仍在 Reactor 中发布的 listener fd。

### 修复后验证

- Reactor 注册事务在回滚仍保留 source 时，于同一串行化区间调用 `on_retained`，并通过必填的 `out_retained` 告知调用方。Pipeline listener 记录该 fd，屏蔽其 accept 回调；后续 `begin_drain()` 只有在注销成功后才关闭保留 fd。
- 定向构建：`xmake build test_pipeline_listener test_reactor_source_detach`，退出码 `0`。
- 定向测试：`xmake run test_pipeline_listener` 与 `xmake run test_reactor_source_detach`，均退出码 `0`。
- 全量测试：`xmake test -v -j1`，退出码 `0`；`38/38` 项通过，耗时 `13.343s`。
- 差异检查：`git diff --check` 退出码 `0`。
- GitHub Actions 仍以该 PR 最新提交上的检查结果为准；此前 R2 提交的 CI 通过结果不能替代本次修复后的 CI。
