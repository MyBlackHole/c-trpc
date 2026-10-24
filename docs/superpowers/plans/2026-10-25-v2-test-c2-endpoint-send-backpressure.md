# RPC V2 TEST-C2 Endpoint 发送背压回放实施计划

> **执行约定：** 在当前会话逐项实施；用户禁止后台子 agent。

**目标：** 以一条固定 L1 schema v1 错误终局轨迹验证真实 RPC Record Endpoint 在一次最终状态发送 `TR_AGAIN` 后会成功重试并正确完成终局。模型 `ERROR` 映射到 C 夹具的 `UNIMPLEMENTED` 状态。

**设计：** [TEST-C2 Endpoint 发送背压回放设计](../specs/2026-10-25-v2-test-c2-endpoint-send-backpressure-design.md)

## 全局约束

- 仅修改测试夹具、Python 测试工具、CI 既有 GCC job、文档与开发版本号。
- 不修改生产 C、公开头、ABI、协议、依赖、Xmake targets 或 CI check 名称。
- 故障配置唯一且固定为 `send_rejects = 1`；适配器和 C CLI 都拒绝任意事件/故障值。
- 所有测试临时文件与输出位于 worktree `build/test-tmp/`，不得访问 `/tmp`。
- 开发版本从 `2.0.0-dev.33` 提升为 `2.0.0-dev.34`。

### 任务 1：为轨迹映射和固定故障入口添加失败测试

**文件：** `scripts/test_rpc_v2_component_replay.py`、新建固定轨迹 JSON、`scripts/rpc_v2_component_replay.py`。

- [x] 新建 9 事件固定轨迹，task IDs 同时包含 TEST-A/B/C。
- [x] 单测精确校验映射操作序列、固定背压配置、L1 期望结果；删除/重排事件、修改 `expect`、缺失 TEST-C 时均失败关闭。
- [x] 单测用工作区内 fake binary 验证只调用明确的背压入口，并可严格比较其观察值。
- [x] 运行 Python 组件映射测试确认新增断言因映射/入口不存在而 RED，设置 `TMPDIR="$PWD/build/test-tmp"` 与 `PYTHONDONTWRITEBYTECODE=1`。

### 任务 2：实现 Endpoint 固定回放入口

**文件：** `tests/test_rpc_record_endpoint_contract.c`。

- [x] 增加专用命令入口，只接受固定的错误终局事件序列。
- [x] 在初始化 fixture 后设置一次 `send_rejects`；按工作完成路径产生 `UNIMPLEMENTED` 最终状态并执行真实 Endpoint。
- [x] 断言发送尝试恰为两次且注入已消费，最终状态帧仅接受一次，发送状态为 `UNIMPLEMENTED`，credit stop/close 顺序正确，用户终局回调恰为一次。
- [x] RESET 后验证调用槽退役并在 `close_all` 后预算归零；输出既有严格 observation schema。
- [x] 编译并运行 Debug/Release C Endpoint 契约目标和专用 replay；新增 Python 映射测试先 RED 再 GREEN。

### 任务 3：接入适配器、CI 与维护文档

**文件：** `scripts/rpc_v2_component_replay.py`、`.github/workflows/ci.yml`、`README.md`、`docs/reviews/v2_remaining_work.md`、`xmake.lua`、新建评估记录。

- [x] 适配器将新 trace ID 映射到固定操作序列和专用夹具入口，再比较 L1/C observation。
- [x] 在既有 GCC 小模型自测步骤添加新轨迹回放；不新增 job/check。
- [x] 开发版本提升到 dev.34；README 和剩余工作记录写明只覆盖一次真实 Endpoint 最终发送背压。
- [x] 新评估记录列出轨迹、夹具故障断言、测试结果和仍未覆盖的 TEST-C/D/E 范围。

### 任务 4：验证并交付

- [x] TEST-A 模型、TEST-B 映射和 TEST-C1/C2 Python 测试通过。
- [x] GCC Debug/Release Endpoint 契约各 35/35，B1–B4/C2 Release 回放 5/5 匹配。
- [x] 64-seed TEST-C1 回归通过；全部测试临时路径指向 worktree `build/test-tmp/`。
- [x] `git diff --check`、clang-format 18.1.8 全文件检查通过；审查生产 `src/`、公开 `include/`、依赖、targets 和 check names 均未改变。

### 任务 5：提交、推送与远端验收

- [ ] 从 TEST-C1 分支切出独立 TEST-C2 分支，保留 PR #322 的提交不变。
- [ ] 按 feature commit 规范提交 dev.33→dev.34，正文完整记录需求、背景、实现、影响、验证、关联、性能、风险、回滚和验收。
- [ ] 推送分支并以 TEST-C1 分支为 base 建 Draft PR；不合并 PR。
- [ ] 记录 PR 首轮每项 CI 结果；不覆盖失败证据，逐项调查新增失败或已有失败。
