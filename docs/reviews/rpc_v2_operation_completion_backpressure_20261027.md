# 异步操作与 Reactor 完成队列背压回归

## 范围

dev.167 只扩展 `test_reactor_fairness`，把真实 `tr_async_operation_pool` 状态与 Reactor 有界 completion queue 满载、关闭组合起来；不修改生产代码、公开 API/ABI 或 wire。

## 覆盖路径

1. 操作在 Reactor owner 上 reserve/accept，worker publish 结果并持有 completion ref。
2. 测试先阻塞 owner，再用已排队完成项填满真实 completion queue。异步操作完成 callback 的生产者进入 `push_wait` 后，payload 仍由已发布操作持有、用户 callback 尚未运行。
3. owner 恢复后，队列释放容量并唤醒生产者。completion callback 在 owner 消费结果、执行 payload destroy、retire 操作；验证结果值、callback 次数、payload 析构次数及队列总完成数均精确一次。
4. stop 场景在同样的满载条件下关闭 completion admission。producer 的交接返回 `TR_ERR_CLOSED`，completion ref 和已发布结果没有被队列接管，业务 completion callback 不运行。owner 与 producer 完全停止后，`tr_async_operation_reap_stopped()` 接管 stopped cleanup 并恰好析构一次；池销毁后预算归零。

## 验证

- GCC Release：`test_reactor_fairness/default` 通过，覆盖队列恢复与 stop 两条新组合。
- GCC TSan：同一目标通过；首次运行发现测试在 `teardown()` 销毁 mutex 后仍检查计数，已将断言移到 teardown 前并复跑通过。
- GCC ASan/UBSan：同一目标通过。
- `git diff --check` 通过；最终 clang-format 18.1.8 改动行检查待提交后运行。
- 临时目录均使用当前 worktree `build/test-tmp`。

## 边界

该回归证明一个已接受异步操作通过真实 Reactor completion 队列的满载恢复与停止拒绝路径不丢失结果，并验证 stopped cleanup 屏障。它不证明 RPC authorize、TLS provider、MessageService 各自所有队列满载/关闭组合，也不关闭 #243/#239 或 V2 总体验收；跨 TLS/压缩共享调度、更多回调与资源屏障仍待验证。
