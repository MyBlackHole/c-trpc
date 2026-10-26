# RPC V2 TEST-B1 确定性组件轨迹回放设计

日期：2026-10-24。关联 GitHub #247 TEST-B，前置为 TEST-A L1 状态模型与 JSON schema v1。

## 目标

将一条人工审核的 L1 `cancel-late-work` JSON 轨迹同时送入参考模型和真实
`tr_rpc_record_endpoint` 组件测试夹具，比较 Call 终局、终局回调、迟到工作和资源回收。
首个 TEST-B 切片只证明取消后工作迟到完成这一条有限交错可在确定事件顺序下重放。

## 方案

复用 `tests/test_rpc_record_endpoint_contract.c` 现有 Xmake 测试目标。该夹具通过
`on_config` 唯一链接真实 `src/rpc/rpc_record_endpoint.c` 和真实 memory-budget 原语；
下层 Reactor、Channel、Dispatch 与 Executor 保持测试替身。现有 `clock_gettime` 替身和
手动 `tick` 驱动提供确定时间与事件顺序。本期不新增构建目标、不增加 JSON C 解析器。

新增一条 TEST-B 轨迹，只包含 `OPEN`、`ACCEPT`、请求 `MESSAGE`、`WORK_START`、`CANCEL`、
迟到 `WORK_DONE`、被拒绝的重复 `FINAL_STATUS` 和 `SLOT_RELEASE`。Python 驱动器先使用
TEST-A parser 与模型完整校验轨迹，再确认它符合组件适配器支持的固定事件/字段集合，
最后把有序操作名传给既有 C 测试二进制。C 侧按事件执行实际 Endpoint 回调与手动 tick，
输出稳定 JSON 观察值；Python 比较观察值与模型的最终 Call/预算摘要。

组件适配是显式的窄映射：`ACCEPT` 观察 Endpoint 已建立真实 Dispatch 句柄；`WORK_START`
观察已送入的请求和被夹具门控的待完成工作；`CANCEL` 对真实 Endpoint 可见的
Dispatch 结果施加取消；`WORK_DONE` 释放门控并手动驱动 Endpoint 消费迟到完成；
`FINAL_STATUS` 将同一调用的重复终局回调重新送入真实 Endpoint，并断言终局结果不变且
没有 wire FINAL；`SLOT_RELEASE` 断言唯一终局已归还组件槽。
适配器拒绝不支持的轨迹 ID、事件顺序、槽代次、方向、基数、字段值和预期结果。

## 边界

- 不修改产品 C、安装 SDK、API/ABI、线协议、生产时钟/IO/scheduler 或第三方依赖。
- C 夹具不创建真实线程、TCP 连接或 sleep；它验证真实 Endpoint 对确定替身事件的反应，
  不验证真实 Channel/Dispatch/Reactor 的并发调度。
- C 事件参数从已严格校验的 fixture 派生，不在 C 端重新实现 JSON parser，也不复制生产
  Endpoint 状态转换作为预期 oracle。
- JSON `task_ids` 必须继续包含 `#247/TEST-A`，并可添加 `#247/TEST-B`；TEST-B fixture
  同时列出两者，未知 ID 仍拒绝。既有 TEST-A fixture 与 schema v1 均保持兼容。
- 本切片不包含随机调度、模糊测试、自动缩减、持续故障矩阵、TCP 集成、平台矩阵、指标或
  性能验证；#247 与 V2 继续开放。

## 验收

1. 相同 TEST-B fixture 可分别由 L1 模型和实际 Endpoint 测试夹具重放。
2. 固定序列得到唯一终局回调；取消后迟到工作不发送或接纳第二次 FINAL。
3. 终局回调后槽/调用可回收，Endpoint 关闭后真实 memory-budget 账本归零。
4. 轨迹或组件观察值不匹配时，Python 驱动非零退出并报告 trace ID 与差异字段。
5. 既有 `build-test (gcc)` 作业内运行，不新增 job、重命名 check 或将替身描述为真实网络。

该实现是 #247 TEST-B 的单组件交付；它不覆盖 issue 评论新增的 #248 模块依赖图与负向门禁要求。
