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
