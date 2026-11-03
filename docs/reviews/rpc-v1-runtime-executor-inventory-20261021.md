# R13 Runtime 旧 RPC Executor 依赖盘点（2026-10-21）

## 基线与范围

- 审计基线：R12 head `2b4c360f6f620f4bc0360114030f29b34504b1f9`。
- 当前分支：`refactor/rpc-v1-retirement-r13-20261021`。
- 目标：确认 Runtime 对旧 V1 Executor Group 的依赖能否独立移除；本记录只做盘点，不改实现。
- 归档：`build/linux/x86_64/release/libtrcore.a`，由 R12 release 构建生成。

## 扫描证据

运行：

```sh
python3 scripts/rpc_v1_inventory.py \
  --root . \
  --archive build/linux/x86_64/release/libtrcore.a \
  --output build/r13-inventory-rpc-v1.json
```

结果：扫描 211 个 C 家族文件，识别 104 个候选名称；分类为 facade 1、production-other 41、runtime 7、tests 47、v1-core 92。JSON 清单留在忽略的 `build/` 输出目录。扫描器是去注释/字面量后的词法清单，不是 C AST 调用图。

归档对象关系：

| 对象 | 定义或引用 | 结论 |
| --- | --- | --- |
| `legacy_rpc_execution.c.o` | 定义 `tr_runtime_legacy_rpc_execution_{create,can_start,start,stop,destroy,group}`；未解析引用 `tr_rpc_executor_group_{create,can_start,start,stop,destroy_checked}` | Runtime 适配器依赖旧 V1 Executor Group。 |
| `runtime.c.o` | 定义 `tr_runtime_shard_rpc_executor` | Runtime 内部仍暴露旧组 getter；实现调用仅在 Runtime 内。 |
| `rpc.c.o` | 定义 `tr_rpc_executor_group_*`，并同时实现 V1 Endpoint 的执行调度 | 不能在 R13 全局删除/拒绝该组符号。 |
| `rpc.c.o` | 未解析引用 `tr_rpc_raw_encode`、`tr_rpc_wire_{decode_ex,encode}` | V1 Endpoint 与旧 Codec/Wire 保持闭包，归后续 R14。 |

生产源码中，`src/runtime/runtime.c` 保存旧组、验证旧组配置、构造/启动/停止/回收旧组；`src/runtime/legacy_rpc_execution.c/.h` 是适配层。R12 后 `src/client.c`、`src/server.c` 均无旧组构造调用；`tr_runtime_shard_rpc_executor()` 的剩余源码消费者是内部测试。

## 测试所有权

- `test_runtime.c` 的 `test_runtime_shard_rpc_executor_ownership` 专测 Runtime 私有旧组配置/getter，应随 Runtime 适配层删除。
- `test_runtime_threads.c` 的 Runtime 多分片计数、启动回滚与 `test_runtime_executor_group_join_failure_is_retryable` 仍假设 Runtime 会创建旧组线程；需改为 Reactor/消息服务真实线程数，并移除 Runtime 旧组 join 用例。
- `test_runtime_threads.c` 的 `test_standalone_rpc_executor_join_failure_is_retryable` 与 `test_standalone_rpc_start_rollback_keeps_owner` 直接创建 V1 Endpoint，继续保留到 R14。
- `test_rpc_executor_group_drain.c` 的 raw group 生命周期和 worker-context 用例仍验证 `rpc.c` 中 V1 Endpoint 所用的组实现。由于 R13 保留 Endpoint 核心，这些用例改归 R14，不能在 R13 先删掉实现仍在用的覆盖。
- `test_complex_constructor_faults.c` 的旧组构造注入仍针对 `rpc.c` 实现，留到 R14；`test_task_executor` 继续作为 V2 执行器生命周期覆盖。

## 重要命名边界

不能把 `tr_rpc_endpoint_*` 作为整体 deny 前缀：V2 记录端点也使用 `tr_rpc_endpoint_result`、`tr_rpc_endpoint_terminal_fn`、`tr_rpc_endpoint_progress_fn` 等名称。R13 门禁应精准拒绝 Runtime 适配器符号及配置类型，同时保留 V1 Endpoint/Executor/Wire 名称供 R14 退役。

## 结论

建议 R13 只移除 Runtime 对旧 V1 Executor Group 的配置与生命周期所有权，保留 `rpc.c` 中供 raw V1 Endpoint 使用的执行组实现。当前 Runtime 配置是仓库内部接口；此改动不删除安装 SDK API，也不声称整个 V1 Executor 已退役。执行组和 Endpoint 一起在 R14 清理，可避免为了一个短暂中间阶段拆分或重写旧引擎。
