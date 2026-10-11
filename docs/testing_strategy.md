# 测试与 CI 覆盖策略

## 目标

测试矩阵按“不变量证据”组织，不按编译器、优化级别、CRC 后端和内存/线程检查器
做机械笛卡尔积。

每个测试/任务必须能回答一个独立问题：

- 功能语义是否正确；
- 生命周期/所有权是否收敛；
- 并发访问是否存在数据竞争或自等待；
- 内存/未定义行为是否安全；
- 编译器/公开 SDK 边界是否可移植；
- CPU 特性回退是否正确；
- 真实 RPC 负载下是否仍满足上述不变量。

## 功能测试分层

<a id="primitive-单元测试"></a>

### 基础原语单元测试

命令队列、完成队列、定时器队列、CRC32C 等基础原语由各自
`test_*.c` 独立验证。

不要在 `test_transport.c` 再复制基础 FIFO、最小堆或测试向量验证。
Transport/RPC 集成测试只验证跨模块契约。

### 集成测试

`test_transport.c` 关注：

- Reactor、套接字与解析器集成；
- Channel/Stream 生命周期；
- 重连、保活与连接替换；
- RPC 线协议、Call、流式调用与门面端到端语义；
- 所有者发布与拆除屏障。

专门的并发/调度测试继续独立：

- `test_reactor_fairness`：命令/完成队列公平性与停止屏障；
- `test_reactor_budget`：单轮 RX/TX/命令/完成处理预算；
- `test_tx_priority`：CONTROL/DATA 调度顺序；
- RPC 过载/背压/预留：执行器准入和流式背压；
- TSan：数据竞争；
- ASan/UBSan：生命周期、越界和未定义行为。

这些测试即使触达同一模块，也不是重复覆盖。

## CI 矩阵

保留完整功能测试集：

```text
GCC debug
GCC release
Clang release
ASan + UBSan
TSan
```

原因：

- GCC 的 `debug` 模式验证低优化/调试构建；
- GCC 的 `release` 模式验证生产优化路径；
- Clang 的 `release` 模式提供独立编译器证据；
- ASan/UBSan 与 TSan 验证不同错误类别。

不再为 `crc32c_portable={n,y}` 重复整个测试集。

CRC 后端单独验证：

```text
强制使用可移植后端运行 test_crc32c
自动选择后端 + QEMU 隐藏 SSE4.2
```

这样直接验证后端选择，而不是把无关 RPC/Channel 测试翻倍。

## Echo 独立进程冒烟测试

安装与构建检查保留原有的服务端就绪探测、客户端 20 秒超时及响应内容断言。
请求成功后，正常路径显式发送 `SIGTERM`，最多等待 10 秒，并检查服务端退出状态
必须为 0；退出失败或超时均使该步骤失败并输出服务端日志。`EXIT` trap 仅在失败
路径强制清理剩余服务端，不再承担正常退出验证。

`scripts/echo_smoke_shutdown.sh` 使用 Ubuntu 既有的 Bash、GNU `timeout` 与
`tail --pid`，最后由父 shell 的 `wait` 取得真实退出状态。
`python3 scripts/test_echo_smoke_shutdown.py` 通过独立替身进程验证正常退出、
非零退出和忽略 `SIGTERM` 的挂起，检查失败能传递给调用 shell，且不会遗留替身。
这项自测只覆盖 CI 判定逻辑，不替代库的功能测试。

<a id="rpc-benchmark-smoke"></a>

## RPC 基准冒烟测试

基准测试负载不是第二套完整 CI。

保留三种有独立证据价值的运行：

```text
GCC release  -> 正常负载 / 基准测试脚本
ASan         -> 负载内存安全
TSan         -> 负载并发安全
```

Clang 与 `debug` 基准任务仍保留原检查标识，但只做对应的
`bench_rpc` 构建；真实冒烟负载只在 `release`/ASan/TSan 三种有独立
证据价值的模式执行。CRC 可移植/回退路径由 CRC 专测覆盖，不在基准测试
中重复。

PR 必需的冒烟检查不承担容量曲线、扩展性矩阵、资源余量、40k 重复性、
扇出和生成器槽位扫描。这些属于架构/性能诊断，运行时间和波动来源都明显
高于正确性冒烟测试，放入独立的 `RPC benchmark diagnostics` 工作流：

```text
拉取请求
    -> RPC benchmark smoke（RPC 基准冒烟测试）
       -> 构建 / 计数核算单元测试
       -> release 冒烟测试
       -> ASan 负载
       -> TSan 负载

main 推送 / workflow_dispatch
    -> RPC benchmark diagnostics（RPC 基准诊断）
       -> 容量曲线
       -> 扩展性矩阵
       -> 资源余量
       -> 重复性
       -> 扇出
       -> 生成器槽位扫描
```

这样 PR 门禁只回答“这次改动是否破坏真实 RPC 正确性/生命周期负载”，
重型性能诊断仍保留证据，但不再增加每个生命周期修复的合并延迟。

<a id="ci-check-identity"></a>

## CI 检查标识

GitHub 必需检查/合并队列可能按任务展示名称绑定。优化矩阵时，
原有稳定检查名称视为外部自动化 ABI：

- `build-test (gcc)`
- `build-test (clang)`
- 既有 `rpc-smoke (...)` 名称
- 内存/线程检查器、安装与 CRC 检查名称

在无法读取分支保护配置时，默认保持这些名字不变。可以减少任务内
重复负载，但不能通过随意改名让必需检查永远无法满足。

## 新增测试的门槛

新增回归测试前先写出它保护的不变量。如果现有测试已经覆盖同一判定条件、
同一故障注入点和同一生命周期边界，应增强现有专测，而不是再复制一份场景。

优先测试：

1. 状态机非法转移；
2. 准入/拆除的线性化；
3. 释放后仍发生回调；
4. 取消与完成的竞争；
5. 队列已满、分配失败或陈旧代次；
6. 停止/重启代次；
7. 所有者回调/工作线程回调自等待；
8. 故障回滚与资源归还。

不以“代码行覆盖率更高”为理由保留重复测试。
