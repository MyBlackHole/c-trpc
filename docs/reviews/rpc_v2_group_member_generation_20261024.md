# RPC V2 连接组成员代次防重放验证记录

核对日期：2026-10-24

基线：`f73d8bb4d214fe71393eb8fa5e1cb0d8fbf01741`（main，2.0.0-dev.17）。实现提交
[`5adc946`](https://github.com/MyBlackHole/c-trpc/commit/5adc946) 将版本升至 `.18`；后续排查
发现并修复同一变更引入的请求字段未初始化问题，修复提交为
[`2306ae1`](https://github.com/MyBlackHole/c-trpc/commit/2306ae1)，当前分支版本为 `2.0.0-dev.19`。
关联 [#242](https://github.com/MyBlackHole/c-trpc/issues/242)，仅覆盖连接组 epoch 与迟到
DATA 路由能力隔离。

## 实现范围

Pipeline 注册表现在分配注册表生命周期内全局唯一、单调递增的非零 DATA
`member_generation`。CONTROL 预留在所属 Reactor 的单次 owner 事务内先确认容量，再分配
代次并写入精确 RESERVED 槽位。容量恰好用尽时返回 `TR_AGAIN` 且不消耗代次；容量统计
违反不变量时返回 `TR_ERR_STATE`。`UINT32_MAX` 后拒绝新分配，不回绕。失败或取消允许留下
代次空洞，保证已暴露过的路由能力不复用。

真实 loopback listener 回归覆盖：旧 CONTROL 的 DATA 套接字只发送 47/48 字节路由前导；
另一个 DATA 已附着；关闭 CONTROL 后已附着 DATA 必须关闭，pending 前导仍保留；用同一
`pipeline_id/epoch` 重建 CONTROL；补齐旧前导后必须被拒绝且不能消耗新预留；新 DATA 能力仍能
附着并处理 PING。TRR1/TRC1 字段和长度没有改变。

代次唯一性只覆盖单个注册表生命周期。它不跨服务进程重启持久化，调用方必须在服务重启后
为新连接组使用新 `epoch`。计数器耗尽后，该注册表不再签发 DATA 能力，不能复用旧代次。

## RED/GREEN 与回归证据

- Listener RED：旧、新 Pipeline 都签发代次 1，测试在
  `new_data_route.member_generation != old_data_route.member_generation` 断言失败。
- CI/Debug RED：预留请求增加 `use_external_generation` 后，旧 `tr_pipeline_add_data_on_owner()`
  只赋值 Pipeline 和输出指针；其栈上请求的新增字段未初始化。CI 的 Clang 14 静态分析报告
  `src/group/pipeline.c:328` 分支条件读取垃圾值；GCC Debug 的既有 Pipeline 测试在成员槽复用
  后发现 generation 未递增。本机复现相同失败。将该请求零初始化后，旧 Pipeline 路径继续使用
  槽位局部代次，静态分析分支条件也有确定值。
- 容量 RED：槽位已满时第二次预留返回 `TR_AGAIN`，但原实现提前消耗代次；取消首个预留后，
  下次应为 2、实得 3，`second.data.generation == 2` 断言失败。
- GREEN：初始化修复后的 Debug 连接组、Pipeline、TLS、Channel 与 RPC 聚焦矩阵 10/10，通过；
  GCC Release 全量测试 88/88 通过。Control 丢失时混合 DATA 状态回归也通过，附着 DATA 关闭，
  旧 pending 路由被拒绝且 `data_accepts` 计数不变。
- 代次边界单测覆盖 0→1、`UINT32_MAX-1`→`UINT32_MAX`、耗尽拒绝且输出哨兵不变；Control
  单测覆盖满容量拒绝不消耗代次。
- 仓库正式源码格式门禁：clang-format 18.1.8 检查 9 个修改源码文件，0 失败；
  `git diff --check` 通过。
- 首轮 GitHub CI 的 GCC Debug 与 Clang 14 门禁复现同一未初始化问题；本记录对应修复提交
  `2306ae1`，其后续 CI 结果以 PR #313 当前 head 的检查状态为准。
- 所有测试临时目录、Xmake 临时目录与原始日志使用仓库 `build/`；原始日志位于忽略目录
  `build/v2-group-drain-242/logs/`。

## 影响、风险与未覆盖项

成功 DATA 预留仅增加一次 owner 本地 32 位计数器递增和常数次容量检查；没有新增锁、动态
分配或按成员保存的历史表，预期性能影响可忽略。旧请求的零初始化只作用于栈上小结构，
不改变独立 Pipeline 的本地代次语义。代次唯一性避免旧 Pipeline 的未完成路由前导在同一
注册表内跨重建复活。

该提交**不表示 #242 或 V2 完成**。GOAWAY 接受/未接受边界、RESET 与 DATA 的完整错误分类、
连接组 CONTROL/DATA 全部交错故障矩阵、资源回收证明仍待后续实施与验收。完整回滚需撤销
`2306ae1`、文档记录提交及 `5adc946`；回滚也会恢复本记录描述的迟到路由复用问题。
