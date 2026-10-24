# RPC V2 小状态模型与可重放轨迹实施计划

> 由当前会话直接逐项执行；不启动子 agent。每个任务先写失败测试，再实现并验证。

**目标：** 建立一个不依赖生产代码的有限 RPC V2 语义模型、严格 JSON 轨迹校验器和确定性回放工具。

**架构：** Python 标准库负责解析版本化轨迹、执行独立 Call/方向/代次/预算状态机，并输出稳定结果。人工维护的 JSON 轨迹同时保存预期事件结果；既有 GCC 功能测试作业运行模型自测。

**技术栈：** Python 3.10 标准库、`unittest`、JSON、现有 GitHub Actions 与 Xmake 项目。

**设计：** [RPC V2 小状态模型与可重放轨迹设计](../specs/2026-10-24-v2-state-model-replay-design.md)

## 全局约束

- 本期只实现 TEST-A 的 L1 语义 oracle，不修改生产 C、SDK API/ABI、线协议或运行时代码。
- 模型最多两个槽、两位代次、两个方向、每方向两条消息、八个租约、16 字节预算、256 个事件。
- 轨迹严格校验字段；未知字段、重复 JSON 键、非法代次和超限输入必须失败。
- 每事件检查终局、工作生命周期、代次和预算守恒；失败事件不得变更状态。
- `seed` 只作为回放元数据，不在本期生成随机事件。
- 不添加第三方依赖，不新增 CI 作业或改名既有检查。
- 测试、临时目录、缓存、构建产物和日志不得写入 `/tmp`；Python 测试设置工作区 `TMPDIR` 并关闭字节码缓存。
- 仅测试/维护工具变更也遵循当前开发版本连续性：`2.0.0-dev.27 -> 2.0.0-dev.28`；不改公开 API/ABI。

---

### 任务 1：为 JSON 轨迹格式建立严格校验

**文件：**

- 新建：`scripts/test_rpc_v2_model.py`
- 新建：`scripts/rpc_v2_model.py`
- 新建：`tests/fixtures/rpc_v2_model/empty-quiescent-v1.json`

**接口：**

- `parse_trace(raw: bytes) -> Trace`：校验 UTF-8、文件大小、重复键、顶层结构、task/invariant ID、限制值、事件字段和 schema 版本。
- 轨迹对象只包含经过校验的整数、固定枚举、列表和映射；不保留任意 JSON 扩展字段。

`Trace` 与 `Event` 的首期内部表示固定如下：

```python
@dataclass(frozen=True)
class Event:
    op: str
    slot: int
    generation: int
    expect: str
    fields: tuple[tuple[str, int | str], ...]

@dataclass(frozen=True)
class Trace:
    schema_version: int
    trace_id: str
    task_ids: tuple[str, ...]
    invariant_ids: tuple[str, ...]
    seed: int | None
    memory_bytes: int
    max_events: int
    finish: str
    events: tuple[Event, ...]
```

- [x] **步骤 1：先写解析器测试。** 在测试文件中构造一个最小合法空轨迹，并断言 `schema_version`、默认预算和事件数；增加重复键、未知字段、布尔值充当整数、未知版本与事件字段多余的负例。

测试必须先导入尚不存在的接口并验证 RED：

```python
from rpc_v2_model import TraceFormatError, parse_trace

raw = (b'{"schema_version":1,"trace_id":"empty-quiescent-v1",'
       b'"task_ids":["#247/TEST-A"],"invariant_ids":["CALL-01"],'
       b'"seed":null,"limits":{"memory_bytes":0,"max_events":1},'
       b'"finish":"QUIESCENT","events":[]}')
trace = parse_trace(raw)
self.assertEqual(trace.schema_version, 1)
self.assertEqual(trace.events, ())
```

最小有效输入用实际完整字段填充，不保留省略号；fixture 使用 `memory_bytes=0`、`max_events=1`、`finish=QUIESCENT` 和空事件表。

- [x] **步骤 2：确认 RED。** 运行：

```sh
mkdir -p build/test-tmp
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/build/test-tmp" python3 scripts/test_rpc_v2_model.py -v
```

预期：测试因 `rpc_v2_model` 尚不存在而失败，不应触碰 `/tmp`。

- [x] **步骤 3：实现最小解析器。** 使用 `json.loads(text, object_pairs_hook=unique_object, parse_constant=reject_constant)` 检出重复键和 NaN/Infinity；拒绝超过 64 KiB 的输入、未知/缺失键、未知 task/invariant ID、超界整数、`bool`、浮点、未知操作、事件数量超限和非精确字段集合。轨迹解析失败抛出带字段路径的 `TraceFormatError`。

重复键检测器须在构造字典前失败：

```python
def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise TraceFormatError(f"重复字段: {key}")
        result[key] = value
    return result
```

- [x] **步骤 4：确认 GREEN。** 使用同一命令；预期合法轨迹通过，所有非法输入均由对应格式错误拒绝。

### 任务 2：实现 Call、方向、代次与预算状态机

**文件：**

- 修改：`scripts/rpc_v2_model.py`
- 修改：`scripts/test_rpc_v2_model.py`

**接口：**

- `ModelState.apply(event: Event) -> str`：按单事件执行；`ModelState.snapshot() -> tuple` 返回槽、Call、工作、租约和预算状态的不可变快照。
- `run_trace(trace: Trace) -> ReplayResult`：按顺序执行事件、逐事件核对 `expect`、验证状态快照不变规则和全局不变量。
- `ReplayMismatch(trace_id: str, event_index: int, operation: str, expected: str, actual: str)`：记录完整轨迹差异。
- `ReplayResult`：包含 `trace_id`、事件结果元组、Call 历史元组、两个槽的终态、预算 `limit/available/reserved/held` 和 `quiescent` 布尔值。

- [x] **步骤 1：先写状态机测试。** 分别覆盖 ONE/MANY 方向、受理前请求消息、受理后响应消息、重复终局拒绝、取消后工作迟到完成、旧代次返回 `STALE`、代次耗尽后槽退役、资源满时 `RESOURCE_LIMIT`、预留转持有再释放及双重释放拒绝。每个负例比较事件前后快照。

- [x] **步骤 2：确认 RED。** 运行同一 `python3 scripts/test_rpc_v2_model.py -v` 命令；预期新增状态机断言因 `run_trace` 未实现而失败。

- [x] **步骤 3：实现纯参考状态。** 用独立 Python 数据类表达两槽、Call 终局回调计数、请求/响应计数与半关闭、单个活动工作、单调不回绕的两位代次、租约状态和 `available/reserved/held`。逐事件按设计表改变状态，之后检查预算守恒、回调不超过一次、终局不复活和租约唯一归属。事件执行采用复制后提交，任何拒绝保持原状态：

```python
before = state.snapshot()
result = state.apply(event)
if result in {"STALE", "REJECTED", "RESOURCE_LIMIT"}:
    assert state.snapshot() == before
state.check_invariants()
if result != event.expect:
    raise ReplayMismatch(
        trace_id=trace.trace_id,
        event_index=index,
        operation=event.op,
        expected=event.expect,
        actual=result,
    )
```

代码实现须将操作结果映射为唯一枚举；预算守恒检查按每事件执行：

```python
assert budget.available + budget.reserved + budget.held == budget.limit
```

- [x] **步骤 4：确认 GREEN。** 运行同一测试命令；预期所有事件转换和不变量检查通过，错误预期产生 `ReplayMismatch`。

### 任务 3：加入代表性 JSON 轨迹及确定性 CLI

**文件：**

- 修改：`scripts/rpc_v2_model.py`
- 修改：`scripts/test_rpc_v2_model.py`
- 新建：`tests/fixtures/rpc_v2_model/unaccepted-reject-v1.json`
- 新建：`tests/fixtures/rpc_v2_model/cancel-late-work-v1.json`
- 新建：`tests/fixtures/rpc_v2_model/budget-transfer-v1.json`
- 新建：`tests/fixtures/rpc_v2_model/stale-generation-v1.json`
- 新建：`tests/fixtures/rpc_v2_model/invalid-double-terminal-v1.json`

**接口：**

- `python3 scripts/rpc_v2_model.py --trace tests/fixtures/rpc_v2_model/cancel-late-work-v1.json`：成功时向 stdout 输出稳定、排序固定的 JSON 回放摘要；格式错误或预期不符时返回非零，并输出轨迹 ID/事件序号。

CLI 序列化使用 `json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"))`，最后恰有一个换行；stdout 只写成功 JSON，失败原因写 stderr。

- [x] **步骤 1：先写 fixture 测试。** 让 unittest 遍历上述轨迹；比较每个 trace 的终局结果、回调次数、工作状态、槽代次、预算总账和事件结果。另断言同一轨迹重复运行得到完全相同的序列化字节。

- [ ] **步骤 2：确认 RED。** 运行模型测试；预期因 fixture 缺失或 CLI 未实现而失败。

执行记录：fixture/CLI RED 阶段未单独保留；该组 fixture 测试是在回放器和 CLI 已实现后加入，当前完整回归通过。

- [x] **步骤 3：添加手写轨迹与 CLI。** 每份 fixture 的 `task_ids`/`invariant_ids` 与测试目标对应；失败案例明确标注期望的 `REJECTED`/`STALE`。CLI 只读取指定轨迹、输出确定性 JSON，不创建临时文件。入口按以下顺序处理：

```python
raw = args.trace.read_bytes()
trace = parse_trace(raw)
result = run_trace(trace)
sys.stdout.write(serialize_result(result) + "\n")
```

- [x] **步骤 4：确认 GREEN。** 运行：

```sh
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/build/test-tmp" python3 scripts/test_rpc_v2_model.py -v
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/build/test-tmp" python3 scripts/rpc_v2_model.py --trace tests/fixtures/rpc_v2_model/cancel-late-work-v1.json
```

预期自测通过，CLI 两次输出字节一致；故意修改一个 `expect` 后退出码非零且报告精确事件序号。

### 任务 4：接入现有 CI 并更新开发记录

**文件：**

- 修改：`.github/workflows/ci.yml`
- 修改：`xmake.lua`
- 修改：`README.md`
- 修改：`docs/reviews/v2_remaining_work.md`
- 新建：`docs/reviews/rpc_v2_state_model_replay_20261024.md`

**接口：** 无生产接口。CI 在既有 `build-test (gcc)` 作业运行 Python 自测，保持所有既有 job/check 名称不变。

- [x] **步骤 1：先验证版本与 CI 放置点。** 确认当前版本为 `dev.27`，只在 GCC 功能测试作业的全部 Xmake 测试成功后增加 `python3 scripts/test_rpc_v2_model.py -v`，不要添加新 job。

新增 CI 步骤保持既有作业名：

```yaml
- name: 运行 RPC V2 小状态模型自测
  run: PYTHONDONTWRITEBYTECODE=1 python3 scripts/test_rpc_v2_model.py -v
```

将步骤插在 GCC job 的 `xmake test -v -j1` 所在脚本成功之后、库存审计之前。
- [x] **步骤 2：先写文档与版本检查。** 验证开发版本唯一更新为 `2.0.0-dev.28`，README 明确这是测试能力交付，不暗示 #247 或 V2 已完成。
- [ ] **步骤 3：更新记录。** 评估记录列出模型覆盖、命令输出、CI SHA 和限制；剩余工作将 TEST-A 标为完成，并保留 TEST-B/C/D/E 与 #247/#241/#242 未完成状态。
- [x] **步骤 4：检查改动。** 运行 `git diff --check`、JSON 解析、Python 测试和两个 CLI 正反例；确认生产目录和公开头没有变化。

### 任务 5：完整验证、提交并登记 Draft PR

**文件：** 已列出的模型、轨迹、CI、版本和文档文件。

- [x] 本地运行 `scripts/test_rpc_v2_model.py`，`git diff --check`，Python 3.10 语法执行路径；所有输出和临时目录留在 worktree `build/`。
- [x] 确认没有生产 C/API/ABI 变更、第三方依赖或必需检查重命名，工作树干净。
- [ ] 使用包含需求、背景、实现、影响、测试、关联需求、性能、风险、回滚和验收说明的中文提交信息，版本字段为 `2.0.0-dev.27 -> 2.0.0-dev.28`。

提交正文逐项填写，测试结果必须使用实际命令输出：

```text
需求描述: 增加 RPC V2 有限状态参考模型和版本化轨迹回放
需求背景: #247 TEST-A 要求独立的语义 oracle，供后续组件级确定性执行复用
实现方案: 使用 Python 标准库校验严格 JSON 轨迹，执行双槽、双方向、预算和迟到完成状态模型
影响范围: 仅 tests/scripts/CI/维护文档，不改变生产二进制或公开合同
测试验证: python3 scripts/test_rpc_v2_model.py -v、正常与预期不符轨迹 CLI 检查、GitHub PR CI
相关需求: #247 TEST-A；后续 TEST-B/C/D/E 未包含
性能影响: 只增加 GCC 功能 CI 的 Python 自测，无生产热路径开销
风险评估: 参考模型仍是有限状态；以手写轨迹预期、严格边界和后续真实组件对照缓解
回滚方案: 回退本提交，移除维护脚本/轨迹/CI 步骤并恢复开发版本
验收标准: 精确轨迹重放、终局/所有权/预算不变量通过，新增 CI 检查全绿
```
- [ ] 推送 `feat/v2-test-a-model-247`，创建以 `fix/v2-data-tx-ready-247` 为 base 的 Draft PR；等待新 head 所有 CI 检查完成，失败时修复并重跑。
- [ ] 保留首轮 CI 失败证据，不因重跑通过覆盖原因；更新 #247 TEST-A 记录和 `v2_remaining_work.md`。

## 计划自审

- Spec 的方向、终局、迟到工作、代次、租约、严格格式和静默要求均映射到任务 1–3 的实现/测试步骤。
- CI、版本、记录、提交和远程 Draft PR 均由任务 4–5 覆盖。
- 本计划没有要求接入真实组件、生成随机序列或宣称完整 RPC V2 完成；这些仍由后续独立任务完成。
