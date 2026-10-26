# RPC V2 TEST-C1 固定种子模型探索与轨迹缩减实施计划

> **执行约定：** 在当前会话逐项执行，使用 `superpowers:executing-plans` 检查点；用户禁止后台子 agent。

**目标：** 为 RPC V2 L1 状态模型增加固定种子事件探索、同类失败的确定性缩减和 JSON 失败报告。

**架构：** 新 Python 标准库模块复用 `rpc_v2_model.py` 和 schema v1，使用自有 xorshift32 生成不超过 48 事件的轨迹。发现模型不变量异常后，稳定地删除无关事件并保留同一异常类别；测试专用 mutant 证明生成/缩减/报告链路能工作。

**技术栈：** Python 3.10 标准库、`unittest`、现有 Xmake/GitHub Actions。

**设计：** [TEST-C1 固定种子探索与轨迹缩减设计](../specs/2026-10-24-v2-test-c1-deterministic-explorer-design.md)

## 全局约束

- 不修改生产 C、公开头、ABI、线协议或 Xmake 生产目标。
- 不增加 Python 第三方依赖、CI job 或 GitHub 检查名称。
- JSON schema 保持 v1；已知 task IDs 增加 `#247/TEST-C`，TEST-C 轨迹同时关联 TEST-A/TEST-C。
- PRNG 固定为本计划实现的 xorshift32；seed 取 0–4294967295；case 数 1–64；每条最多 48 事件。
- 故障 mutant 只存在于 Python 测试，不进入 CLI 默认执行、生产代码或测试适配器。
- 所有测试临时目录、错误轨迹、日志、构建产物与缓存均位于 worktree `build/`，禁止写 `/tmp`。
- 开发版本从 `2.0.0-dev.32` 提升为 `2.0.0-dev.33`。
- CI 仅扩展既有 `build-test (gcc)` job；模型异常时归档 `build/test-tmp/rpc-v2-trace-explorer/` 七天。

---

### 任务 1：先为固定种子生成与稳定输出写测试

**文件：**

- 新建：`scripts/test_rpc_v2_trace_explorer.py`
- 新建：`scripts/rpc_v2_trace_explorer.py`
- 修改：`scripts/rpc_v2_model.py`

**接口：**

- `generate_trace(seed: int, max_events: int) -> Trace`：返回带原 seed、TEST-A/TEST-C task IDs 和 schema v1 字段的部分轨迹。
- `serialize_trace(trace: Trace) -> str`：返回键排序、无空格的 UTF-8 JSON 文本。
- `explore(seed: int, cases: int, max_events: int, failure_probe=None) -> ExplorationSummary | FailureReport`：运行有界 seed 序列；`failure_probe` 仅供 Python 单元测试注入已知故障，默认使用 `run_trace`。
- `ExplorationSummary` 包含 `seed`、`cases`、`max_events`、`events`、按字典序排序的 `operation_counts` 和 `result_counts`。

- [x] **步骤 1：写最小失败测试。** 测试导入尚不存在的 `rpc_v2_trace_explorer`，断言相同 seed 产生相同序列化文本，生成轨迹能由 `parse_trace` 接收且 `run_trace` 完成。
- [x] **步骤 2：确认 RED。** 运行：

```sh
mkdir -p build/test-tmp
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/build/test-tmp" \
  python3 scripts/test_rpc_v2_trace_explorer.py -v
```

预期：因 `rpc_v2_trace_explorer` 模块尚不存在而失败；不得创建 `/tmp` 文件。

- [x] **步骤 3：实现 PRNG 和轨迹生成。** 使用无符号 32 位 xorshift32：`x ^= x << 13; x ^= x >> 17; x ^= x << 5`，每一步以 `& 0xffffffff` 截断；seed 0 的内部状态固定改用 `0x6d2b79f5`，轨迹仍记录 0。操作字段只从模型已有枚举和限制中选择；`expect` 由 `ModelState.apply` 记录。
- [x] **步骤 4：扩展 TEST-C task ID。** 在 `rpc_v2_model.TASK_IDS` 增加 `#247/TEST-C`，生成轨迹使用 `task_ids=["#247/TEST-A", "#247/TEST-C"]`；添加未知 ID 仍失败的断言，确认现有 TEST-B 组件检查不变。
- [x] **步骤 5：确认 GREEN。** 运行相同测试命令，并运行 `scripts/test_rpc_v2_model.py -v`；新生成轨迹逐条通过 `parse_trace`、`run_trace` 和模型不变量。

### 任务 2：扩展事件覆盖并验证探索边界

**文件：**

- 修改：`scripts/rpc_v2_trace_explorer.py`
- 修改：`scripts/test_rpc_v2_trace_explorer.py`

**接口：** `explore(seed, cases, max_events, failure_probe=None)` 除边界验证外，还汇总实际生成事件数、事件操作和结果种类，输出排序稳定；仅测试调用可传 `failure_probe`。

- [x] **步骤 1：添加 RED 覆盖与边界测试。** 固定 seed 覆盖 `OPEN`、`ACCEPT`、消息、半关闭、工作开始/结束、终局、`RESET`、预算预留/转移/释放、陈旧代次和拒绝；case=0/65、事件数=0/49、负 seed 与大于 `2^32-1` 均拒绝。
- [x] **步骤 2：运行测试确认新增断言失败。** 使用任务 1 的 Python 命令；失败必须是缺少接口、覆盖或限制断言，不得由导入路径/临时目录造成。
- [x] **步骤 3：实现受限选择策略。** 每步从当前槽生命周期可执行操作及固定无效/陈旧事件池中选择；两者按固定权重 4:1 取样。所有数值字段由同一 xorshift 状态派生。若当前无活动槽则优先 OPEN；若已达上限则终止为 `PARTIAL`，不合成非模型事件。
- [x] **步骤 4：确认 GREEN。** 重跑 Python 测试并运行 64 个 seed、每条最多 48 事件；断言生成计数不越界、summary 可重复且每种目标事件至少被固定 seed 集合覆盖一次。

### 任务 3：添加同类失败缩减与已知 mutant 自测

**文件：**

- 修改：`scripts/rpc_v2_trace_explorer.py`
- 修改：`scripts/test_rpc_v2_trace_explorer.py`

**接口：**

- `FailureSignature(kind: str, message: str)`：异常类别使用稳定短名，message 保留异常文本。
- `failure_signature(trace: Trace, probe) -> FailureSignature | None`：对不失败轨迹返回 `None`。
- `shrink_trace(trace: Trace, probe, signature: FailureSignature) -> Trace`：按 ddmin 块删除，再逐事件删除；只有同一 signature 的候选才保留。
- `FailureReport(seed: int, signature: FailureSignature, original: Trace, minimized: Trace)`：JSON 序列化用于 CLI 和工作区报告。

- [x] **步骤 1：写 RED 测试。** 构造带有无关 MESSAGE/工作事件的合法调用轨迹；测试专用 `duplicate_terminal_callback` probe 在第一个成功终局后将回调计数置 2，并调用 `check_invariants()`。断言缩减仍保持同一 `TraceInvariantError`，结果恰为 `OPEN`、`CANCEL` 两事件；重复缩减产生相同字节。
- [x] **步骤 2：确认 RED。** 运行 Python explorer 测试；预期 `FailureSignature` 或 `shrink_trace` 未定义。
- [x] **步骤 3：实现规范化候选与 ddmin。** 对每个候选事件子序列从相同限制和初始模型重放，重新计算每个 `expect`；遇到异常时在失败事件停止并返回含异常事件的前缀。先用连续块删除（块数从 2 开始递增到事件数），再按原顺序单事件删除；以首个相同 signature 作为判定，不使用随机选择。
- [x] **步骤 4：确认 RED 自测变绿。** 运行 Python explorer 测试，确认测试 mutant 可稳定触发并最小化为 `OPEN`,`CANCEL`，基线模型不带 mutant 时不产生失败报告。
- [x] **步骤 5：测试失败报告结构。** 断言 JSON 含原 seed、signature、原轨迹和最小轨迹；每份轨迹可独立由 schema v1 解析，最小轨迹的事件数严格小于原轨迹。

### 任务 4：接入 CLI、失败工件与文档版本

**文件：**

- 修改：`scripts/rpc_v2_trace_explorer.py`
- 修改：`.github/workflows/ci.yml`
- 修改：`README.md`
- 修改：`docs/reviews/v2_remaining_work.md`
- 新建：`docs/reviews/rpc_v2_test_c1_explorer_20261024.md`
- 修改：`xmake.lua`

- [x] **步骤 1：写 CLI 契约测试。** 使用临时目录 `ROOT/build/test-tmp/rpc-v2-trace-explorer-test/`，覆盖 help、成功摘要、非法参数退出码、输出目录不可写错误，以及经测试 probe 注入故障时 stdout 报告与原始/最小 JSON 文件均出现。
- [x] **步骤 2：运行测试确认 RED。** 使用 `PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/build/test-tmp" python3 scripts/test_rpc_v2_trace_explorer.py -v`；不得调用会写系统 `/tmp` 的 tempfile 默认路径。
- [x] **步骤 3：实现 CLI。** 增加 `--seed`、`--cases`、`--max-events`、`--failure-dir`；成功输出固定 JSON 并返回 0；模型异常输出失败 JSON、按安全的 seed 文件名写 `original.json` 与 `minimized.json` 并返回 1；路径/I/O/参数错误返回 2。默认报告目录为 `build/test-tmp/rpc-v2-trace-explorer/`。
- [x] **步骤 4：更新 CI。** 在现有 `运行 RPC V2 小状态模型与组件回放自测` 步骤运行 `scripts/test_rpc_v2_trace_explorer.py -v` 与 `rpc_v2_trace_explorer.py --seed 33047 --cases 64 --max-events 48 --failure-dir build/test-tmp/rpc-v2-trace-explorer`；后续添加同 job `if: failure()` artifact step，仅上传 `build/test-tmp/rpc-v2-trace-explorer/`，保留 7 天；不新增 job/check。
- [x] **步骤 5：更新版本和维护文档。** 将 Xmake 与 README 从 dev.32 升至 dev.33；README 说明 TEST-C1 的命令和“模型探索不等于生产 C 注入”；剩余工作登记 TEST-C1 证据并保留 TEST-C2、TEST-D/E、#248 和 V2 未完成状态；评估记录说明方法、seed/case/event 上限、summary、失败工件、测试 mutant 边界和历史 CI 责任。
- [x] **步骤 6：确认脚本与文档检查。** 运行 Python explorer、模型与组件映射自测，`git diff --check`；检查 README 和新评估记录不声称 TEST-C 或 V2 整体完成。

### 任务 5：提交、Draft PR 与远端验收

**文件：** 上述全部改动。

- [x] **步骤 1：本地验证。** 运行 TEST-A 28/28、TEST-B 映射 11/11、TEST-C1 全部自测与 64-seed 探索；当前分支对比 B4 运行 changed-source clang-format 18.1.8 门禁；不为纯 Python/文档变更重复完整 90 项 C 测试。
- [x] **步骤 2：检查改动范围。** 确认生产 `src/`、公开 `include/`、依赖列表及既有 CI 检查名无变化；`git diff --check` 通过且 tracked worktree 干净。
- [ ] **步骤 3：按 feature commit 规范提交。** 标题从 `2.0.0-dev.32` 升至 `2.0.0-dev.33`，提交正文包含需求描述、背景、实现、影响、测试、相关需求、性能、风险、回滚和验收标准十项。
- [ ] **步骤 4：推送并建 Draft PR。** 新 PR 以 TEST-B4 PR #321 head 分支为 base；PR 说明列出本地验证和 L1-only 限制；不合并、不删除开放 PR 使用的远程分支。
- [ ] **步骤 5：运行干净树源码格式门禁与远程检查。** 运行 `scripts/check_source_format.py --base origin/feat/v2-test-b4-final-reset-race-247 --head HEAD --clang-format build/tooling/clang-format-18.1.8/clang_format/data/bin/clang-format`；等待新 PR 的 14 项检查全部结束。失败时保存首次失败输出，不以重跑通过覆盖。

## 计划自审

- spec 的 PRNG、schema task ID、事件上限、同类异常缩减、已知 mutant、CLI、失败报告、CI 归档、版本文档与边界均分别分配任务。
- 全部命令将 Python 临时目录固定到 `build/test-tmp`；计划没有 `/tmp` 测试写入步骤。
- 计划不新增生产对象、外部依赖、Xmake 构建目标或 GitHub 检查名称。
- `FailureSignature`、`FailureReport`、生成器与缩减器接口在任务之间一致；已知 mutant 仅由测试 probe 实现。
- TEST-C1 明确只验证 L1 探索工具；实际 C 故障注入留给后续 TEST-C2，不提前关闭 #247。
