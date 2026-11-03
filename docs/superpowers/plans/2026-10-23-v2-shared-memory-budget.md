# V2 服务端共享内存预算实施计划

**目标：** 让普通 V2 Server 的所有 shard 共用一个可核对的内存硬预算，
保留分片本地账目并避免旧等分份额造成的提前拒绝。

**架构：** 内部预算原语支持一个根父预算。Server 持有父对象，Runtime
配置把它传给各 shard；直接 Runtime 和 Client 保持无父预算。

**技术栈：** C11 原子、Xmake、GCC/Clang、ASan/UBSan。

**设计：** `docs/superpowers/specs/2026-10-23-v2-shared-memory-budget-design.md`

**执行约束：** 全部由主会话内联执行，不使用子 agent；测试临时目录、
日志、缓存和构建产物只放在工作区 `build/`，不写 `/tmp`。

## 任务 1：预算原语

文件：`src/memory_budget.h`、`tests/test_runtime.c`。

- [x] 在 `test_runtime_shard_memory_budget` 附近增加双子预算行为测试：
  根限额 16，两个子限额 16，A 预留 12、B 预留 4、B 再预留 1
  必须 `TR_AGAIN`；A 释放 12 后 B 可继续，三份账目最终为零。
- [x] 先运行 `xmake test 'test_runtime/*'` 确认因缺少父预算能力而失败。
- [x] 增加只在初始化时设置的 `parent`，实现先父后子预留与先子后父释放，
  子预留失败时精确撤销父预留。保留无父预算的原路径。
- [x] 再测本地限额拒绝时父账本回滚、零字节、父总额溢出和释放下溢。

## 任务 2：Runtime 与 Server 绑定

文件：`src/runtime/runtime_internal.h`、`src/runtime/runtime.c`、
`src/server.c`、`tests/test_runtime.c`、`tests/test_server_record.c`。

- [x] 增加双 RuntimeShard 测试：构造后父当前值等于两子之和；
  将父限额设为当前值加 12，A 可用 12，B 的下一字节拒绝；
  A 释放后 B 可用，Runtime 销毁后父归零。
- [x] 先运行目标测试确认旧实现不能满足共享账本。
- [x] Runtime 分片配置增加可选父预算；创建时冻结到子预算。
  Server 在 Runtime 创建前初始化自身父预算，所有 V2 分片本地
  限额改用同一总额并绑定父预算；Transport-only 不绑定。
- [x] 运行构造失败、Server Record、Client Record 和 Runtime 定向测试，
  检查回滚、持有视图与销毁重试。

## 任务 3：证据与集成

文件：`xmake.lua`、`README.md`、`docs/reviews/v2_remaining_work.md`、
`docs/reviews/rpc_v2_shared_memory_budget_20261023.md`；必要时更新精确
静态分析告警坐标。

- [x] 将开发版本升为 `2.0.0-dev.7`，记录 RED/GREEN、双层账本、
  性能成本及 #241 未完成的压缩、慢消费者、多 DATA、租户等范围。
- [x] 运行 GCC release 全量和受影响 ASan/UBSan；使用工作区临时目录。
- [ ] 由主会话按代码审查清单核对并提交，创建 PR，等待 CI，合入后
  在实际主线复核，更新 #241，清理本次远程分支。
