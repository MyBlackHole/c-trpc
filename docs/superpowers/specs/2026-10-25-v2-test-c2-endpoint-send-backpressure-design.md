# RPC V2 TEST-C2 Endpoint 发送背压回放设计

日期：2026-10-25。关联 GitHub #247 TEST-C；前置为 TEST-A、TEST-B1–B4 与 TEST-C1。

## 目标与边界

将一条固定 schema v1 错误终局轨迹接入真实 `rpc_record_endpoint.c` 测试夹具，并让夹具链接层的 `tr_stream_sendv` 在首次发送最终状态帧时返回一次 `TR_AGAIN`，随后接受重试。Endpoint 仅对非 OK 终局发送最终状态帧；轨迹的抽象 `ERROR` 在夹具中由 Dispatch 映射为具体 `UNIMPLEMENTED`。Python 适配器先验证轨迹并运行 L1 状态模型，再运行 Endpoint 夹具并比较终局、一次回调、最终帧接受、退役和预算静默结果。

这是 TEST-C2 的单一故障配置切片：只覆盖一次最终状态发送背压；不修改生产 C、公开头、ABI、协议或调度实现，不声称已覆盖任意故障序列、真实 TCP/Channel/Dispatch、随机注入或 TEST-C 全部要求。

## 方案

1. **仅扩展 L1 模型轨迹**：可校验模型终局，但无法证明真实 Endpoint 遇到 `TR_AGAIN` 后会重试。
2. **固定一条真实 Endpoint 发送背压回放（采用）**：复用现有组件夹具的 `send_rejects` 注入点和 `--wrap` 链接方式；新增一个严格登记的终局轨迹与明确命名的夹具入口，仅将第一次最终帧发送拒绝一次。实现小、可重复，并直接覆盖一次重试行为。
3. **通用事件前缀和故障时序驱动**：需要定义独立于 L1 操作的故障调度 schema、C 夹具事件解释器和更广泛的状态组合，超出本切片边界。

## 轨迹与执行

- 新固定轨迹 `component-final-send-backpressure-v1.json` 使用现有 schema v1，事件为 `OPEN, ACCEPT, MESSAGE, HALF_CLOSE, WORK_START, WORK_DONE, FINAL_STATUS(ERROR), RESET(REJECTED), SLOT_RELEASE`。
- 轨迹关联 `#247/TEST-A`、`#247/TEST-B`、`#247/TEST-C`，并保持 `CALL-01`、`CALL-02`、`DIR-01`、`OWN-01`。
- Python 适配器只接受该 trace ID 的精确事件字段、顺序、限制与 task/invariant IDs；它将其映射至既有 Endpoint 成功终局操作序列，并选用固定 `--replay-send-backpressure` 夹具入口。
- C 夹具对该入口硬编码一次 `MESSAGE` 错误状态并设置 `send_rejects = 1`，最终帧状态为 `UNIMPLEMENTED`；要求 `tr_stream_sendv` 实际被调用两次、唯一拒绝已消费、最终状态帧只接受一次、credit stop/close 顺序正确、用户终局回调一次、Endpoint 槽退役且预算归零。任意其他事件序列或故障计数均失败关闭。
- 组件 JSON observation schema 不变：模型与夹具比较状态、终局回调数、工作完成、成功最终帧数、退役数、预算余量、静默状态及旧消息计数。
- 既有 GCC job 增加该固定回放命令；不新增 CI job/check、Xmake target、依赖或生产 C 变更。
- 版本由 `2.0.0-dev.33` 升至 `2.0.0-dev.34`；更新 README、剩余工作记录和本切片验证记录。

## 验收

1. 新 Python 单测验证固定 trace 的严格映射、错误事件/字段拒绝、TEST-C ID 要求，并区分普通回放入口和固定背压入口；L1 的 `ERROR` 与真实夹具的 `UNIMPLEMENTED` 映射有显式断言。
2. 真实 Release Endpoint 夹具回放成功，C 侧断言一次 `TR_AGAIN` 被消费、第二次发送成功、最终帧仅一个、终局回调仅一次并静默释放预算。
3. 既有 TEST-A、TEST-B 映射和 TEST-C1 自测继续通过；新增回放由 CI 现有 GCC job 调用。
4. `git diff --check`、源码格式门禁通过；文档明确这是单一故障配置，不宣称 TEST-C 或 V2 完成。
5. 所有临时目录、日志、构建产物和缓存留在 worktree `build/` 下，不写入 `/tmp`。
