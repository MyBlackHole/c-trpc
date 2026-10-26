# RPC V2 小状态模型与可重放轨迹设计

日期：2026-10-24。关联：GitHub #247 的 TEST-A，前置研究见 TEST-A/TEST-B 切片与验证分层说明。

## 目标

为 RPC V2 提供一个独立、有限、可重复执行的语义参考模型，以及可检查、可提交、可重放的 JSON 轨迹格式。模型用于明确 Call 终局、双向消息、工作延迟完成、代次复用和预算责任之间的关系；以后组件级测试可将同一轨迹输入真实代码并与模型输出对照。

TEST-A 只建立 L1 参考模型和轨迹契约，不声称模型已经验证 Reactor、Channel、RPC 或任意并发实现。它是 #247 后续 TEST-B/C/D/E 的 oracle 输入基础，不关闭 #247、#241 或 #242。

## 方案与边界

采用 Python 标准库实现独立模型，轨迹使用版本化 JSON。Python `json` 解析器易于严格限制字段、拒绝重复键，并能直接被现有 `scripts/test_*.py` 方式验证；可读 JSON 便于把失败序列保存在仓库中。C 模型会要求本期自建轨迹解析器；只给集成测试增加日志则没有独立语义预期，均不采用。

模型不得导入、调用或复制生产 C 的状态转换函数。它只定义本期明确的参考语义，不通过生产测试桩反向生成预期值。真实时钟、线程、网络 I/O、Reactor 调度、压缩、TLS、随机轨迹生成、自动缩减和性能指标均属于后续独立阶段。

## 有限状态和不变量

模型固定最多两个 Stream 槽、两种方向（请求和响应）、两位代次、每方向最多两条消息、最多八个资源租约、单轨迹最多 256 个事件和最多 16 字节的抽象预算。代次达到最大值后槽永久退役，不回绕到旧句柄可再次有效的状态。

每个槽依次经历 `FREE -> OPEN -> ACCEPTED -> TERMINAL -> FREE/RETIRED`；拒绝、取消、期限、RESET 或 owner 停止可以从 `OPEN` 或 `ACCEPTED` 到 `TERMINAL`。Call 的终局结果与实际工作完成分开记录：终局回调最多一次，已启动的工作可以在取消/期限/RESET/owner 停止后迟到完成，但迟到完成不得重开 Call、追加回调或复活代次。

请求方向可在受理前开始发送；响应方向只在受理后可发送。方向关闭后不能再接收该方向消息。`ONE` 方向至多一条消息，并须在成功关闭时恰有一条；`MANY` 最多两条且可为零条。成功状态须有已关闭的请求方向；响应 `ONE` 恰有一条响应消息。错误终局允许提前结束未完成方向。

每份抽象存储以唯一租约 ID 在 `reserved` 与 `held` 间转移或释放。任一事件后均满足 `available + reserved + held = limit`。资源不足返回 `RESOURCE_LIMIT` 且账本不变；只有 `ACCEPTED` Call 能预约；终局后可归还已有租约，但不能再开新预约。双重释放、错误代次、非法状态转换均不得改变模型状态。

完成轨迹必须满足：每个已打开 Call 已进入一个终局且终局回调数恰为 1；无活动工作、预约租约或持有存储；预算全部回到 `available`。槽尚未复用不影响静默判断。显式标记为部分轨迹时，只允许用于重放模型检查点，不能报告为完成或 quiescent。

轨迹引用的稳定不变量 ID 为：`CALL-01`（终局至多一次）、`CALL-02`（迟到工作不复活终局）、`OWN-01`（未受理前不取得工作/预算所有权）、`DIR-01`（方向顺序、半关闭和基数）、`RES-01`（预算总账守恒）、`RES-02`（租约只转移/释放一次）、`GEN-01`（旧代次不可访问新 Call）。`task_ids` 必须包含 `#247/TEST-A`，已知切片还包括 `#247/TEST-B`；TEST-B 轨迹列出两者。`invariant_ids` 非空且只能使用以上 ID。未知 task/invariant ID 在 TEST-A/TEST-B 中拒绝，避免轨迹与验收目标脱节。

## 轨迹格式 v1

每个轨迹是一个 UTF-8 JSON 对象，必需顶层字段为 `schema_version`、`trace_id`、`task_ids`、`invariant_ids`、`seed`、`limits`、`finish` 和 `events`。`schema_version` 固定为整数 `1`；`seed` 为 `null` 或无符号 32 位整数；本期不据此生成随机事件。`trace_id` 仅允许有限 ASCII 小写字母、数字、点、下划线、斜线和短横线。`finish` 只能为 `QUIESCENT` 或 `PARTIAL`；后者只跳过末尾静默断言，仍须执行所有单步状态与预算不变量。轨迹不得携带载荷、任意用户字符串、方法名、认证信息或调用 ID 标签。

`limits` 仅包含 `memory_bytes` 与 `max_events`，分别限制在 0–16 与 1–256；槽数、代次位宽、方向数、每方向消息数与租约数为格式常量，不能由输入扩大。严格拒绝重复 JSON 键、未知字段、非整数数值（包括布尔值）、超限文件和未知 schema 版本。

每个事件的共同字段为 `op`、`slot`、`generation`、`expect`；`slot` 只能为 0 或 1，`generation` 只能为 0–3。事件操作及附加字段如下：

| 操作 | 附加字段 |
|---|---|
| `OPEN` | `request_mode`、`response_mode`，值为 `ONE` 或 `MANY` |
| `MESSAGE` | `direction`（`REQUEST` 或 `RESPONSE`）、`bytes`（0–16 的整数，只是消息长度，不代表预算分配） |
| `HALF_CLOSE` | `direction`（`REQUEST` 或 `RESPONSE`） |
| `FINAL_STATUS` | `status`（`OK` 或 `ERROR`） |
| `RESERVE` | `lease`（0–7）、`bytes`（1–16 的整数） |
| `TRANSFER`、`DROP_RESERVATION`、`RELEASE_STORAGE` | `lease`（0–7） |
| 其他操作 | 无 |

操作集合为 `OPEN`、`ACCEPT`、`REJECT`、`MESSAGE`、`HALF_CLOSE`、`WORK_START`、`WORK_DONE`、`FINAL_STATUS`、`CANCEL`、`DEADLINE`、`RESET`、`OWNER_STOP`、`RESERVE`、`TRANSFER`、`DROP_RESERVATION`、`RELEASE_STORAGE` 和 `SLOT_RELEASE`。每种操作有固定字段集合；校验器拒绝缺失字段和多余字段。`expect` 只能是 `APPLIED`、`STALE`、`REJECTED` 或 `RESOURCE_LIMIT`。

轨迹按文件顺序执行。每个操作的实际模型结果必须与该事件的 `expect` 相等；否则回放以轨迹 ID、事件序号和差异终止。预期拒绝的事件必须保持状态不变，随后仍可继续执行轨迹。回放结果按固定字段、稳定排序输出最终 Call 历史、各事件结果、代次槽状态和预算账本，不输出时间戳或随机数据。

## 文件与运行入口

- `scripts/rpc_v2_model.py`：严格格式校验、参考状态机、账本不变量检查、确定性 JSON 回放 CLI。
- `scripts/test_rpc_v2_model.py`：标准库 `unittest`，覆盖成功、预受理拒绝、取消后迟到完成、重复终局、消息方向/基数、预算失败与归还、旧代次、代次耗尽、非法轨迹字段及状态回滚。
- `tests/fixtures/rpc_v2_model/*.json`：人工审查的有效与无效回放轨迹，task/invariant ID 与测试目标一一对应。
- `.github/workflows/ci.yml`：在现有 `build-test (gcc)` 作业执行模型自测，不新增 CI 作业，不改稳定检查名称。
- `docs/reviews/v2_remaining_work.md` 与完成记录：记录 TEST-A 交付证据及仍未覆盖的后续阶段。

轨迹和回放输出为测试材料，不进入安装 SDK、生产库、公开 API/ABI 或线协议。只使用 Python 标准库，不增加第三方依赖。自测不创建 `/tmp` 文件。

## 验收

1. 每个有效 fixture 都可由 CLI 和 `unittest` 稳定重放，且模型逐事件核对手写预期结果。
2. 每个非法事件的状态快照保持不变；重复终局、取消后迟到完成和旧代次事件的结果符合不变量。
3. 任一步预算变更后都满足账本守恒；资源失败与资源释放路径分别覆盖。
4. 重复 JSON 键、未知字段、越界值、缺失事件结果、超大轨迹和未知 schema 版本均明确失败，不跳过、不猜测。
5. 全轨迹完成检查拒绝尚未终局的 Call、活动工作、未释放租约和未归还预算。
6. 同一输入在重复运行中产生字节稳定的 JSON 结果；失败输出包含可重放的轨迹 ID 和事件序号。
7. 新脚本在现有 GCC 功能测试作业内运行；原有全部检查标识保持不变。

TEST-A 不声称覆盖无限状态空间、真实 TCP 字节顺序、异步线程交错、活性公平前提、实际内存分配或生产 C 实现正确性。这些需要后续 TEST-B/C/D 与相关功能验收共同证明。
