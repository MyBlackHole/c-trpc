# 测试与 CI 覆盖策略

## 目标

测试矩阵按“不变量证据”组织，不按编译器、优化级别、CRC 后端和 sanitizer
做机械笛卡尔积。

每个测试/job 必须能回答一个独立问题：

- 功能语义是否正确；
- 生命周期/所有权是否收敛；
- 并发访问是否存在数据竞争或 self-wait；
- 内存/未定义行为是否安全；
- 编译器/公开 SDK 边界是否可移植；
- CPU feature fallback 是否正确；
- 真实 RPC workload 下是否仍满足上述不变量。

## 功能测试分层

### Primitive 单元测试

Command queue、completion queue、timer queue、CRC32C 等基础原语由各自
`test_*.c` 独立验证。

不要在 `test_transport.c` 再复制 basic FIFO/min-heap/test-vector 测试。
Transport/RPC 集成测试只验证跨模块 contract。

### 集成测试

`test_transport.c` 关注：

- Reactor + socket + parser 集成；
- Channel/Stream 生命周期；
- reconnect/keepalive/replace connection；
- RPC wire/Call/streaming 与 facade 端到端语义；
- owner publication / teardown barrier。

专门并发/调度测试继续独立：

- `test_reactor_fairness`：命令/完成队列公平与 stop barrier；
- `test_reactor_budget`：单轮 RX/TX/command/completion 预算；
- `test_tx_priority`：CONTROL/DATA 调度顺序；
- RPC overload/backpressure/reserve：执行器准入和流式背压；
- TSan：数据竞争；
- ASan/UBSan：生命周期、越界和未定义行为。

这些测试即使触达同一模块，也不是重复覆盖。

## CI 矩阵

完整功能 suite 保留：

```text
GCC debug
GCC release
Clang release
ASan + UBSan
TSan
```

原因：

- GCC debug 验证低优化/调试构建；
- GCC release 验证生产优化路径；
- Clang release 提供独立编译器证据；
- ASan/UBSan 与 TSan 验证不同错误类别。

不再为 `crc32c_portable={n,y}` 重复整个 suite。

CRC 后端单独验证：

```text
forced portable test_crc32c
automatic dispatch + QEMU hide SSE4.2
```

这样直接验证 backend selection，而不是把无关 RPC/Channel 测试翻倍。

## RPC benchmark smoke

benchmark workload 不是第二套完整 CI。

保留三种有独立证据价值的运行：

```text
GCC release  -> 正常 workload / benchmark scripts
ASan         -> workload 内存安全
TSan         -> workload 并发安全
```

Clang 编译由普通 CI 的 `xmake build --all` 覆盖；debug 行为由正常 suite
覆盖。CRC portable/fallback 由 CRC 专测覆盖，不在 benchmark 中重复。

## 新增测试的门槛

新增回归测试前先写出它保护的不变量。如果现有测试已经覆盖同一 predicate、
同一故障注入点和同一生命周期边界，应增强现有专测，而不是再复制一份场景。

优先测试：

1. 状态机非法转移；
2. admission/teardown linearization；
3. callback-after-free；
4. cancellation vs completion；
5. queue full / allocation failure / stale generation；
6. stop/restart epoch；
7. owner callback / worker callback self-wait；
8. fault rollback 与资源归还。

不以“代码行覆盖率更高”为理由保留重复测试。
