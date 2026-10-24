# RPC V2 连接组成员代次防重放实施计划

> 本计划由当前主会话内联执行；按步骤逐项 RED/GREEN，不使用子 agent。

**目标：** 让同一注册表生命周期中的 DATA 能力代次全局单调且不复用，拒绝旧 Pipeline 的迟到 DATA 前导。

**架构：** Pipeline 注册表持有一个有界的 32 位代次计数器；Pipeline Control 在 Reactor 所有者事务中先验证容量，再从注册表取得代次并写入精确 DATA 槽。Wire 格式不变，耗尽时显式拒绝，不回绕。

**技术栈：** C11、Xmake、现有 TRR1/TRC1、Reactor 所有者与真实 loopback TCP 测试。

**设计：** [连接组成员代次防重放设计](../specs/2026-10-24-rpc-v2-group-member-generation-design.md)

## 全局约束

- 中文文档和注释使用简体中文。
- 测试仅使用仓库 `build/`；禁止在 `/tmp` 创建测试输出、临时文件、日志或缓存。
- 所有 Pipeline/注册表状态转换都在所属 Reactor 所有者中执行。
- 保留公开 API、TRR1/TRC1 长度和既有 CI 检查名称。
- 仅对本注册表生命周期提供代次不复用保证；服务重启后由调用方更换 epoch。

---

### 任务 1：复现旧 DATA 能力跨 Pipeline 生命周期附着

**文件：**
- 修改：`tests/test_pipeline_listener.c`
- 依赖：现有 loopback listener、route 编解码、CONTROL 和 DATA_OFFER 测试辅助函数。

**步骤：**

- [x] 新增一个独立测试：创建 listener 和旧 CONTROL，签发 DATA_OFFER；编码前导，只向 DATA 套接字发送前 47 字节。
- [x] 在该旧 CONTROL 下附着另一个 DATA；CONTROL 丢失时确认已附着 DATA 关闭，而 pending 前导仍保留至独立路由校验。
- [x] 关闭旧 CONTROL，等待旧 Pipeline 计数归零，同时确认 pending DATA 连接仍占一个连接槽。
- [x] 使用完全相同的 `pipeline_id/epoch` 建立新 CONTROL 并签发新 DATA_OFFER；断言成员索引相同而成员代次不同。
- [x] 补齐旧 DATA 前导；等待路由拒绝计数增加，并断言 `data_accepts` 未增加、新预留仍可用。
- [x] 使用新前导附着第二条 DATA，发送 PING，确认新能力正常工作；关闭所有连接并销毁 listener/Reactor。
- [x] 运行 RED：`env TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" PYTHONDONTWRITEBYTECODE=1 xmake test -P . -v -j1 'test_pipeline_listener/*'`。
- [x] 预期旧实现因两个 Pipeline 都从代次 1 开始而在“新旧成员代次不同”断言失败；保留该失败日志于 `build/v2-group-drain-242/logs/red-member-generation.log`。

### 任务 2：在注册表增加不回绕成员代次分配器

**文件：**
- 修改：`src/group/pipeline_registry.c`
- 修改：`src/group/pipeline_registry_internal.h`
- 测试：`tests/test_pipeline_registry.c`

**步骤：**

- [x] 在注册表结构增加 `uint32_t next_member_generation`，由 `calloc` 初始化为 0。
- [x] 新增所有者专用分配函数：普通值递增并写入输出；到 `UINT32_MAX` 返回 `TR_ERR_STATE`，输出保持原值且计数器不变。
- [x] 通过纯辅助函数单测 0→1、`UINT32_MAX-1`→`UINT32_MAX`、`UINT32_MAX` 拒绝三种边界；每个拒绝断言输出哨兵不变。
- [x] 通过 Control 单测验证 DATA 槽已满时返回 `TR_AGAIN` 且不消耗代次；实现对超界容量统计返回 `TR_ERR_STATE`。
- [x] 运行聚焦 RED/GREEN 单测：`env TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" PYTHONDONTWRITEBYTECODE=1 xmake test -P . -v -j1 'test_pipeline_registry/*'`。

### 任务 3：将连接组 DATA 预留绑定到注册表代次

**文件：**
- 修改：`src/group/pipeline.c`
- 修改：`src/group/pipeline_internal.h`
- 修改：`src/group/pipeline_control.c`
- 修改：`tests/test_pipeline_listener.c`

**步骤：**

- [x] 增加一个只允许 Reactor 所有者调用的 Pipeline 预留入口，显式接收非零成员代次；先验证 CONTROL 和空闲槽，再写入 generation 并发布 RESERVED。
- [x] 保留原 `tr_pipeline_reserve_data()`，供现有独立 Pipeline 单测使用原槽位局部代次。
- [x] 改造 `tr_pipeline_control_reserve_data()`：在一次 owner 回调内先读取 Pipeline 统计并确认空槽，再申请注册表代次，随后以该代次创建预留并构造 route；任何失败都不留下槽位。
- [x] 更新 RED 测试，使新旧 Pipeline 的代次断言通过；补齐旧前导时确认它被拒绝且未取消新代次预留。
- [x] 运行 `test_pipeline_listener/*`、`test_pipeline_control/*`、`test_pipeline_registry/*`，确认 DATA_CANCEL、传输 READY 和槽位复用既有语义保持。

### 任务 4：记录契约并完成版本、格式和回归验收

**文件：**
- 修改：`docs/architecture/04-backup-pipeline.md`
- 修改：`include/tr/transport.h`
- 修改：`xmake.lua`
- 新增审查记录：`docs/reviews/rpc_v2_group_member_generation_20261024.md`

**步骤：**

- [x] 文档说明成员 generation 在单个分片注册表生命周期内全局唯一、不回绕；组 epoch 在服务进程重启后必须更新。
- [x] 按开发版本规则把 `xmake.lua` 从 `2.0.0-dev.17` 递增到 `.18`；修复发现的预留请求未初始化问题后再递增至 `.19`。若后续 TLS-D2 先合入，集成时重新基于实际 main 处理唯一版本行，不改 PR #312 的 draft 状态。
- [x] 对本次修改的 C/H 文件执行 clang-format 18.1.8 修改行检查。
- [x] 运行连接组、Pipeline、TLS 连接组、Channel 和 RPC 目标聚焦回归，再运行 GCC release 全量测试。
- [x] 记录精确 HEAD、RED/GREEN、测试目标、格式结果和边界；不得把本切片登记为 #242 或整体 V2 完成。
