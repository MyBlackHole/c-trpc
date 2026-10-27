# RESET 墓碑索引独立测试链接修复

日期：2026-10-25。关联 #242/ERR-E、PR #324。版本：`2.0.0-dev.62 -> 2.0.0-dev.63`。

## 问题与根因

dev.62 把 RESET 墓碑查询实现拆入 `reset_tombstone_index.c`，并加入 `trcore`。普通测试和库消费者使用完整目标，链接正常；
但 `test_rpc_record_endpoint_tcp` 为隔离真实 Endpoint 依赖而关闭对 `trcore` 的对象继承，在 `xmake.lua` 中手动从 `trcore`
抽取生产对象。该对象闭包漏掉新索引模块，导致完整测试构建在链接时出现
`tr_reset_tombstone_index_{insert,remove,find}` 未定义引用。此前局部验证只构建了索引、RESET、Channel 构造和排空目标，
没有触发此独立 TCP 目标。

## 修复

将 `reset_tombstone_index.c` 加入 `test_rpc_record_endpoint_tcp` 的生产对象清单。目标仍按唯一对象身份从 `trcore`
编译批次抽取，避免单独重复编译或改变运行时依赖边界。

GCC 11 精确告警基线中的 `channel.c` 已知 `analyzer-use-after-free` 诊断从 1408 行移动到 1402 行，原因为 dev.62
将墓碑结构移出 `channel.c` 后文件前段净减少 6 行；告警类别、列号和消息保持不变。只调整基线位置，没有屏蔽诊断或
改变 `review_state`。

## 验证

- 修复前 `TMPDIR="$PWD/build/test-tmp" xmake build -P . test_rpc_record_endpoint_tcp` 可稳定复现链接失败，错误为三个墓碑索引符号未定义。
- 修复后重新构建并运行 `test_rpc_record_endpoint_tcp`，确认独立对象闭包完整。
- dev.62 失败 CI 的 ASan/UBSan、TSan、GCC/Clang 完整构建和 Make 安装检查均在链接 `test_rpc_record_endpoint_tcp` 时失败；GCC 静态分析的唯一区别是上述已知告警行号漂移，Clang 14 静态分析通过。dev.63 完整 CI 复验结果以 PR 检查为准。
- 本地 dev.62 索引、RESET、Channel 构造事务和排空目标在 GCC Release、TSan、ASan/UBSan 下均通过。

## 风险与回滚

修复只补全测试目标的对象抽取清单并更新精确行号，不改变生产链接、Channel 行为、公开 API/ABI 或线协议。回滚可撤销 dev.63，
恢复 dev.62 构建配置、GCC 告警旧行号及本记录；这会重新暴露独立 Endpoint 测试目标的未定义引用。

验收要求：`test_rpc_record_endpoint_tcp` 独立构建/运行成功，完整 CI 构建与静态分析通过，相关告警基线仍只有原诊断。
