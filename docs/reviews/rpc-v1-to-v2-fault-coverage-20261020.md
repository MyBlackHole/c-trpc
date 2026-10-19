# RPC V1 → V2 故障验证迁移（阶段 3.3.2，第一切片）

固定基线：PR #287 `426804f9b0b5fa2e89adc2875eeb878e6f57b973`。本切片**不删除旧测试**、不改生产实现或公开协议；仅以确定性故障注入增加 V2 的真实覆盖。完成与否只对新增场景负责，不代表 V1 所有测试已迁移。

## 已迁移的两个组合场景

| 遗留不变量 | V2 验证 | 失败/退役的明确结果 |
| --- | --- | --- |
| 执行域部分启动失败，随后部分 `pthread_join` 失败，重试从精确游标继续 | `test_task_executor::test_partial_start_join_retry` | 第 3 次 create 失败，两条线程已建立；第 2 次 join 失败，第一条已成功回收；下一轮只 join 剩余线程。全程拒绝任务准入、禁止 restart，不修改拒绝的输出能力、不提前 free、不泄漏预算 |
| 已接受且正在执行的两条 V2 请求，Server 启动终止后在 worker join 时失败 | `test_server_record::test_stopped_work(false,false,true)` | 真实 TCP 两对端受理并阻塞业务，开始 destroy 时先返回 AGAIN；打开门闩后 worker 看见取消并仍构造迟到回复；第一次真实 join 注入 EINVAL 导致 destroy 返回 SYS 但保留 Server/Endpoint；重试收敛，客户端最终状态不为 OK，正文不得交付，任务/worker 计数平衡 |

### 验证边界

- 首项测试的是 **V2 通用 task executor**：与 V1 执行器组的错误语义对应，但不提供 V1 那种 stop 后重新 start 的能力；不可据此断言生命周期 API 完全等价。
- 第二项使用现有 V2 Server 的真实业务工作线程、Reactor、Channel、双连接和单次链接期故障注入，**不是**新造的生产故障开关。
- 既有 V1 `test_rpc_executor_group_drain`、`test_rpc_deadline_failure`、`test_rpc_terminal_backpressure` 等全部保留。
- 尚未完成严格等价迁移：V1 生命周期并发控制、所有异常队列回滚、deadline 时钟失效、终局背压控制信用、跨 Connection 资源预算等。需在后续批次补充独立证据后再评估删除旧测试与旧代码。
- 新增场景不得降低超时、资源容量或跳过 Sanitizer；每次新提交必须取得自身完整 CI、源码格式、双静态分析、安装消费及 RPC smoke 结果。
