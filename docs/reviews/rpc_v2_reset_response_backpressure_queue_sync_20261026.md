# RESET 响应队列压力回归的异步观察时序修复

日期：2026-10-26。关联 #242/ERR-B、Draft PR #327。版本：`2.0.0-dev.145 -> 2.0.0-dev.146`。

## 现象与复现

dev.145 的完整 GitHub TSan 工作流在 `test_peer_reset_response_reserved_fifo_under_pressure()` 的首个队列断言失败：

```text
tests/test_stream_reset.c:1852:
Assertion `stats.tx_queued_items == CONTROL' failed.
```

在独立干净 Release 构建中，收到末尾 PONG 后立即读取 `tx_queued_items` 也能观察到非零值，尽管完整帧已到达对端。

## 根因

压力建立函数经 `tr_reactor_call()` 执行；该 owner 回调内部的 `tr_reactor_send()` 由于处于命令分发阶段，会把 SEND 项排入命令环。`tr_reactor_call()` 只等待回调完成，不等待后续 SEND 命令被分发到连接 TX 队列，因此调用返回后的即时统计不是压力建立屏障。

帧字节进入 socket 后，对端可以先读完整 PONG，而 Reactor 随后才完成该 TX 项并更新队列计数。wire 可见性与队列完成统计不是同一同步点。

## 修复

- 发送四个 PING 后轮询真实连接统计，直到四个项都进入 TX 队列，再检查固定池已满。
- 收到完整 RESET 和 PONG 后轮询统计，直到 TX 队列归零，再确认连接仍 UP 和健康流状态。
- 两个循环都有 5 秒有界等待；未满足条件仍由原断言明确失败。

测试继续经过真实 Channel/Reactor/socketpair/sendmsg 路径；RESET 部分写出 7 字节并遇到真实 `EAGAIN`，队列压力与 wire FIFO 断言保持不变。

## 验证

- GCC Release：干净构建的完整 `test_stream_reset` 通过，墓碑容量 40,000。
- GCC ASan/UBSan：干净构建的完整 `test_stream_reset` 通过，墓碑容量 40,000，泄漏检测开启。
- GCC TSan：干净构建的完整 `test_stream_reset` 通过，墓碑容量 5,000，无数据竞争报告。
- clang-format 23.1.4 改动行检查及 `git diff --check` 通过。
- 构建目录与 `TMPDIR` / `XMAKE_TMPDIR` 均位于仓库 `build/test-tmp/`。

GitHub CI dev.145 的失败及当前修复后 CI 状态以 Draft PR #327 的 checks 为准。

## 影响、性能与风险

仅修改测试、README、剩余工作记录、本报告和 Xmake 版本号；不改生产运行时、公开 API/ABI 或 wire。无运行时性能影响；测试最多增加两个有界统计等待，通常在一个调度周期内结束。

风险为 Reactor 环境严重停顿时，等待到期后测试失败；5 秒上限避免挂起。等待仅用于测试观察，不改变被测组件的队列语义。

## 回滚方案

撤销 dev.146 提交，移除两个队列状态等待和本报告，将 README、剩余工作记录及 Xmake 版本恢复到 dev.145，并重跑 Release、TSan、ASan/UBSan 完整 `test_stream_reset`。

## 验收标准

1. SEND 命令提交后，测试确认普通控制 TX 队列实际达到 `CONTROL` 再继续。
2. 完整 RESET 与 PONG 到达后，测试确认队列最终归零。
3. 既有队列压力、RESET 部分写入 EAGAIN、PING/RESET/PONG FIFO 及健康流场景继续通过。
4. Release、TSan、ASan/UBSan 完整 `test_stream_reset` 均通过。
