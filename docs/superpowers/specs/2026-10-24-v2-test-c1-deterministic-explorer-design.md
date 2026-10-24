# RPC V2 TEST-C1 固定种子模型探索与轨迹缩减设计

日期：2026-10-24。关联 GitHub #247 TEST-C，前置为 TEST-A L1 状态模型和 TEST-B1–B4 组件回放。

## 目标与边界

为现有有限状态模型增加一个可重复的事件序列探索器和确定性缩减器。探索器使用固定算法和种子生成符合 schema v1 的事件序列，逐事件运行 `ModelState` 并检查账本、终局、代次和失败事件不变性；发现模型不变量异常时，缩减到仍触发相同异常类别的最短轨迹，并输出可复现 JSON。

这是 TEST-C 的第一个工具验证切片：它验证生成、失败捕获、缩减和输出链路，不修改生产 C，也不宣称已经注入或发现 Endpoint/Channel/Reactor 的生产故障。真实对象故障点与组件映射留给后续 TEST-C2；真实 TCP、跨平台及综合诊断仍属于 TEST-D/E。

## 方案

1. **扩展固定 TEST-B 例子**：继续手写更多终局与 RESET 场景。它有利于定向回归，但不能检验种子复现和失败缩减。
2. **增加标准库固定种子探索器和 delta-debug 缩减器（采用）**：复用既有状态模型和 JSON schema v1，用自有 xorshift32 PRNG，避免依赖升级和 Python `random` 实现细节；通过测试专用已知故障注入验证失败能被找到并缩减。
3. **直接对任意生产 C 调用做随机注入**：可更早触达实现缺陷，但当前 Endpoint 适配器只允许固定事件序列，任意组件调度接口、C 故障语义及跨层 oracle 尚未定义。将它作为后续 TEST-C2 的设计工作，不在本切片伪装完成。

采用方案 2 是为了先把可复现测试工具链建立起来，同时明确 L1 的能力边界。不会把由参考模型生成并以同一模型计算期望值的轨迹，描述成独立于模型的语义证明。

## 组成与数据流

- 新模块 `scripts/rpc_v2_trace_explorer.py` 提供稳定 PRNG、有限事件生成、模型探索、失败分类、轨迹缩减和稳定 JSON 序列化；不修改 `rpc_v2_model.py` 的语义。
- `scripts/rpc_v2_model.py` 的已知 task ID 增加 `#247/TEST-C`；生成轨迹同时声明 `#247/TEST-A` 与 `#247/TEST-C`，既有 B1–B4 组件轨迹仍声明 TEST-A/TEST-B，不改变固定组件映射规则。
- 新测试 `scripts/test_rpc_v2_trace_explorer.py` 验证同 seed 字节稳定、不同 seed 可分化、事件/种子上限、生成轨迹 schema 合法、模型不变量逐事件通过，以及已知测试故障可被缩减。
- Python API 固定为 `generate_trace(seed, max_events) -> Trace`、`shrink_trace(trace, probe, signature) -> Trace`、`explore(seed, cases, max_events, failure_probe=None) -> ExplorationSummary | FailureReport` 和 `main(argv=None, *, failure_probe=None) -> int`。`failure_probe` 只允许单元测试传入；普通 CLI 无对应参数，也不注入故障。
- 生成器只产生 schema v1 已知操作、字段和取值。默认每条轨迹最多 48 事件；一次 CLI 最多探索 64 个 seed。初始种子为无符号 32 位整数；seed 为 0 时 PRNG 内部状态固定替换为 `0x6d2b79f5`，轨迹仍记录输入 seed 0。
- 事件从受限语法中选择：优先使用当前槽状态可执行的操作，并混入陈旧代次、重复终局、预算重复释放等有界非法操作。生成器调用现有模型取得每个事件的 `expect`；之后将序列再次交给 `run_trace` 逐事件校验。拒绝或资源失败事件必须保持状态不变。
- 缩减器按确定的 `ddmin` 顺序删除连续事件块，再逐个删除事件；每个候选均由参考模型重新计算 `expect`，只保留能重现相同异常类别的候选。保留原 seed 和限制字段，事件最大数收紧到实际长度。相同输入必须得到字节相同的最小轨迹。
- 故障链路的自测使用仅存在于单元测试中的 `duplicate-terminal-callback` 已知 mutant：首次成功终局后故意把回调数改为 2。它不得进入 CLI 默认路径、生产源、组件适配器或 schema。缩减器须把带填充事件的输入缩到 `OPEN`、`CANCEL` 两个事件，并保留“终局回调至多一次”异常。
- 发现真实模型异常时，CLI 返回非零并在 stdout 输出包含 `seed`、异常类别、原始轨迹和最小轨迹的严格 JSON 报告；可选 `--failure-dir` 将两份独立 schema v1 轨迹写入指定目录。默认及 CI 目录固定为 worktree `build/test-tmp/rpc-v2-trace-explorer/`。不得写 `/tmp`。
- CI 在既有 `build-test (gcc)` 的 RPC V2 自测步骤运行固定 64-seed 探索；同一 job 增加 `if: failure()` 的 7 天 artifact 上传，保留工作区故障报告。不新增 job/check 或更名稳定检查。

## 命令与输入约束

```sh
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/build/test-tmp" \
  python3 scripts/rpc_v2_trace_explorer.py \
  --seed 33047 --cases 64 --max-events 48 \
  --failure-dir build/test-tmp/rpc-v2-trace-explorer
```

`--seed` 必须是 0 到 `4294967295` 的十进制整数；`--cases` 为 1 到 64；`--max-events` 为 1 到 48。缺失、重复、未知参数、越界值或轨迹序列化后超出现有 64 KiB 限制时明确失败。固定探索不得使用墙上时钟、线程、网络、外部随机库或环境熵。

## 版本、影响与限制

开发版本从 `2.0.0-dev.32` 升为 `2.0.0-dev.33`。只修改 Python 测试工具、单元测试、CI 既有 GCC job、README 与评估记录；不修改 C 生产源码、公开头、ABI、协议、Xmake 构建目标或依赖。运行时性能影响为零；CI 增加最多 64 条、每条最多 48 事件的模型回放。

模型及生成器共享同一个语义实现，所以随机探索能加强状态/资源组合不变量压力，但不能独立证明模型语义正确，更不能代表生产 Endpoint 正确。测试 mutant 只验证缩减链路，不算生产故障注入。遇到模型异常时，报告中的最小 JSON 必须和原 seed、工具版本一起保留；只有审查并确定真实缺陷后，才将其作为固定回归提交。

## 验收

1. 相同 seed/参数重复运行产生完全相同的探索摘要和失败报告；另一个 seed 产生可观察的序列差异。
2. 默认 64-seed、48-event 探索运行的每条序列均能通过严格 schema v1 解析，且 `run_trace` 完成逐事件期望、状态不变和预算/终局不变量检查。
3. 缩减器在已知 `duplicate-terminal-callback` mutant 上确定性地产生恰含 `OPEN`、`CANCEL` 的两事件最小复现，保留同一异常类别；无故障候选不得误报失败。
4. 负向测试覆盖无效 seed、case 数、事件上限、未知 CLI 字段、无故障时伪失败报告，以及输出目录写入错误；所有临时文件都位于工作区 `build/test-tmp/`。
5. 现有模型 28 项、组件映射 11 项及完整 GCC Debug/Release 测试继续通过；既有 `build-test (gcc)` 内运行探索器并在失败时归档最小轨迹。
6. README 与 `v2_remaining_work.md` 明确记录这是 TEST-C1 工具链切片，保留 TEST-C 生产对象故障注入、TEST-D/E、#248 和 V2 整体验收未完成状态。
