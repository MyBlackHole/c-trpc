# RPC V2 TEST-B1 组件轨迹回放实施计划

> 由当前会话直接逐项执行，不启动子 agent。测试先写并观察 RED；临时输出仅放在 worktree `build/`。

**目标：** 用同一严格 JSON 轨迹驱动 TEST-A 参考模型和真实 RPC Record Endpoint 测试夹具，确定性核对取消后的迟到工作。

**设计：** [TEST-B1 组件回放设计](../specs/2026-10-24-v2-component-replay-design.md)

## 约束

- 只改测试夹具、轨迹驱动脚本、CI 和维护文档；不改生产 C/ABI/协议，不加依赖/CI job。
- 复用 `test_rpc_record_endpoint_contract` 与真实 `rpc_record_endpoint.c`/memory-budget 对象。
- Python 校验完整 JSON；C 只接收有限操作名，不实现第二套 JSON parser。
- 只支持明确的一条 TEST-B1 轨迹及固定事件映射；未知字段/顺序/结果失败关闭。
- 不使用 `/tmp`；本地设置 `PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/build/test-tmp"`。
- 当前开发版本为 dev.28；交付时升级为 dev.29，仍不改公开版本契约。

## 任务 1：先扩展 task ID 并为组件映射写 Python 测试

- [x] 将 schema v1 的已知 task IDs 扩展为 TEST-A 与 TEST-B，仍要求包含 TEST-A；TEST-B fixture 列两者，空 ID/未知 ID 继续失败。
- [x] 新建 `component-cancel-late-work-v1.json`，包含固定八个事件，并断言 L1 结果为单一 CANCELLED 终局、工作静止、槽代次前进、预算归还。
- [x] 新建 `test_rpc_v2_component_replay.py` 的映射单测；先导入不存在的适配器 API，运行测试确认 RED。

## 任务 2：给既有 Endpoint 测试增加受限事件入口

- [x] 扩展 `tests/test_rpc_record_endpoint_contract.c` 的可选 CLI 路径；无参数时既有全部回归不变。
- [x] 只接受唯一完整操作序列；按 fixture 操作真实 Endpoint callback、替身 Dispatch 句柄和确定 tick。
- [x] 输出稳定 JSON 观察值：状态、终局回调数、工作完成、FINAL 发送数、退役数和 budget bytes。
- [x] 单独构建/运行 `test_rpc_record_endpoint_contract`，确认原 35 组与新组件轨迹均通过。

## 任务 3：实现 Python L1/L2 对照驱动

- [x] 组件驱动加载 fixture、调用 `parse_trace`/`run_trace`，仅接受一个固定映射。
- [x] 调用 `--binary` 指定的现有 C 测试二进制；将 C 输出解析为固定 JSON，逐字段对照 L1 结果。
- [x] 增加不支持轨迹/操作、非零 C 退出、坏 JSON 输出和字段偏差的负向测试。
- [x] 在工作区 `build/` 中用伪造可执行文件验证驱动成功和失败路径；真实 Endpoint 回放另行验证。

## 任务 4：接 CI、版本和记录

- [x] GCC job 在既有 Debug/Release 全量 Xmake 测试及 TEST-A 自测成功后，调用组件回放脚本，指向 Release 已构建的 Endpoint 测试目标。
- [x] README 与 `v2_remaining_work.md` 将 TEST-B1 明确登记为单组件交付，并保留未覆盖范围。
- [x] 开发版本提升到 dev.29；评估记录列出真实对象、替身边界、本地测试和待运行 GitHub CI。

## 任务 5：验证并登记 Draft PR

- [x] Python 单测、TEST-A 28 项、C Endpoint 夹具、GCC Debug/Release 全量测试与 CLI 对照通过。
- [ ] `git diff --check`；确认没有 `/tmp`、产品源、公开头、依赖、稳定 check 名称变更。
- [ ] 使用十项中文 feature commit 格式记录 dev.28 -> dev.29。
- [ ] 推送 `feat/v2-test-b-deterministic-replay-247`，创建 base 为 TEST-A PR 的 Draft PR；检查通过后更新记录。

## 计划自审

该测试只把确定事件驱动到真实 Endpoint，对 Dispatch/Reactor/网络继续依赖清楚标注的替身；L1/L2 对照不是并发公平性或全栈 TCP 证明。之后仍需扩大组件字母表、加入受控持续故障、L3 实际网络/平台以及 #248 模块边界检查。
