# RPC V2 连接组成员代次防重放验证记录

核对日期：2026-10-24

基线：`f73d8bb4d214fe71393eb8fa5e1cb0d8fbf01741`（main，2.0.0-dev.17）。实现提交：
[`5adc946`](https://github.com/MyBlackHole/c-trpc/commit/5adc946)，版本 `2.0.0-dev.18`。
关联 [#242](https://github.com/MyBlackHole/c-trpc/issues/242)，仅覆盖连接组 epoch 与迟到
DATA 路由能力隔离。

## 实现范围

Pipeline 注册表现在分配注册表生命周期内全局唯一、单调递增的非零 DATA
`member_generation`。CONTROL 预留在所属 Reactor 的单次 owner 事务内先确认容量，再分配
代次并写入精确 RESERVED 槽位。容量恰好用尽时返回 `TR_AGAIN` 且不消耗代次；容量统计
违反不变量时返回 `TR_ERR_STATE`。`UINT32_MAX` 后拒绝新分配，不回绕。失败或取消允许留下
代次空洞，保证已暴露过的路由能力不复用。

真实 loopback listener 回归覆盖：旧 CONTROL 的 DATA 套接字只发送 47/48 字节路由前导；
关闭 CONTROL 并销毁 Pipeline；用同一 `pipeline_id/epoch` 重建 CONTROL；补齐旧前导后必须被
拒绝，不能消耗新预留；新 DATA 能力仍能附着并处理 PING。TRR1/TRC1 字段和长度没有改变。

代次唯一性只覆盖单个注册表生命周期。它不跨服务进程重启持久化，调用方必须在服务重启后
为新连接组使用新 `epoch`。计数器耗尽后，该注册表不再签发 DATA 能力，不能复用旧代次。

## RED/GREEN 与回归证据

- Listener RED：旧、新 Pipeline 都签发代次 1，测试在
  `new_data_route.member_generation != old_data_route.member_generation` 断言失败。
- 容量 RED：槽位已满时第二次预留返回 `TR_AGAIN`，但原实现提前消耗代次；取消首个预留后，
  下次应为 2、实得 3，`second.data.generation == 2` 断言失败。
- GREEN：重新格式化后的连接组、Pipeline、TLS、Channel 与 RPC 聚焦矩阵 10/10，通过；
  GCC Release 全量测试 88/88，通过。
- 代次边界单测覆盖 0→1、`UINT32_MAX-1`→`UINT32_MAX`、耗尽拒绝且输出哨兵不变；Control
  单测覆盖满容量拒绝不消耗代次。
- 仓库正式源码格式门禁：clang-format 18.1.8 检查 9 个修改源码文件，0 失败；
  `git diff --check` 通过。
- 所有测试临时目录、Xmake 临时目录与原始日志使用仓库 `build/`；原始日志位于忽略目录
  `build/v2-group-drain-242/logs/`。

## 影响、风险与未覆盖项

成功 DATA 预留仅增加一次 owner 本地 32 位计数器递增和常数次容量检查；没有新增锁、动态
分配或按成员保存的历史表，预期性能影响可忽略。代次唯一性避免旧 Pipeline 的未完成路由
前导在同一注册表内跨重建复活。

该提交**不表示 #242 或 V2 完成**。GOAWAY 接受/未接受边界、RESET 与 DATA 的完整错误分类、
连接组 CONTROL/DATA 全部交错故障矩阵、资源回收证明仍待后续实施与验收。回滚可使用
`git revert 5adc946`；回滚也会恢复本记录描述的代次复用问题。
