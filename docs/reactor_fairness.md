# Reactor 命令调度公平性

## 命令阶段边界

正常事件循环每轮只处理一个 `TR_COMMAND_BATCH`，目前最多 64 条已入队命令。
命令只在循环开头消费；wake 事件不再额外执行一批命令。
同一轮随后继续运行 Completion、Timer、RX/TX 与 epoll I/O 阶段。

这限制的是命令数量，不是执行时间：不能抢占正在执行的 owner callback，
也不限制 callback 内嵌套的直接 owner 调用。生产 callback 仍必须短小、非阻塞。
命令阶段没有新增公开预算参数。后续已完成其他阶段的整轮预算统一与最小诊断，
配置语义和统计口径见 [Reactor 整轮预算与调度诊断](reactor_budget.md)。
CONTROL 优先级仍留待独立改造。

## 为什么不能只删掉 drain 循环

Command Queue 合并唤醒。队列仍有积压时，`wake_pending` 可能一直为 1，
eventfd 通知却已经被消费；后续 producer 不一定再次写 eventfd。
若处理一批就进入阻塞等待，剩余命令可能永久停留在队列中。

因此 `tr_process_commands()` 返回“可能还有工作”：

- 取满一批：保守地将下一次 `epoll_wait` timeout 设为 0；
- 未取满：`pop_batch` 已在队列锁内观察到清空并复位 `wake_pending`，
  之后入队的 producer 会重新发送通知。

即使恰好取满并清空，也只额外执行一次非阻塞轮转，下一次空取后恢复正常等待。
没有无锁读取队列 `count`，没有新增队列 API，也没有改变队列容量或唤醒协议。
TX 因 `EAGAIN` 等待可写的原路径保持不变，不因存在待发送数据就强制空转。

## 退出与同步语义

命令仍按 FIFO 顺序取出和执行。STOP 不插队，之前已接受的命令先得到处理。
`stop()` 仍在控制锁内关闭 producer acceptance，然后排入 STOP。
观测到 STOP 后，Reactor 保留原有路径，完整处理此前已接受的 Completion，
不将正常运行的命令预算套到退出清理上，也不补跑业务 Timer callback。

同步 owner call 和 quiesce 仍使用原有队列与完成通知；队列满时等待容量，
停止接受工作后尚未入队的等待者返回关闭错误，不提前报告成功。

## 回归验证

```sh
xmake f -c -y -m debug --toolchain=gcc
xmake test -v -j1 'test_reactor_fairness/*'
```

`tests/test_reactor_fairness.c` 使用真实 Reactor、socketpair 和公开命令提交路径。
仅该测试目标使用链接包装观察 queue push/pop 和 epoll 边界，不向库或 SDK 增加钩子。
测试 gate 有意暂时阻塞 owner，用于确定性构造积压；它不是生产回调用法示例。

覆盖以下场景：

1. 固定积压：Timer、Completion、可读 socket 在命令清空前运行；
   消费初始唤醒后，无新 producer 也能继续排空，并最终恢复阻塞等待。
2. 持续补入：每批出队后通过公开 API 补入命令，检查轮转预算仍然有效。
3. 队列满：同步 call 与 quiesce 确实遇到满队列，释放 gate 后保持 FIFO 与返回值。
4. 队列满时停止：STOP 也经历满队列，未被接受的同步等待者返回关闭错误。
5. STOP 清理：验证此前接受的 200 个 Completion 全部完成，且部分在 STOP 出队后完成。

使用计数器、条件变量和实际执行顺序断言，不使用短时延性能门槛。
10 秒条件等待与 60 秒进程 alarm 只用于检测挂死。新目标自动进入现有
GCC/Clang debug/release、ASan/UBSan、TSan 和 Make 兼容 CI 测试入口。
