# RPC V2 连接组双 DATA 成员 pending route 排空回归

日期：2026-10-25。关联 #242/ERR-A。版本：`2.0.0-dev.72 -> 2.0.0-dev.73`。

## 需求与背景

连接组 listener draining 接受边界已覆盖一个 pending DATA route 和一个 pending CONTROL route。此切片验证同一 CONTROL 下两个不同 DATA member reservation 同时处于 pending 时，drain 屏障后补齐前导不会只拒绝其中一个，也不会让某个成员附接。

## 实现

真实 listener 为同一 group/epoch 依次签发两个 DATA_OFFER，验证两条 route 使用不同 `member_index`，并连接两个 TCP socket，各发送 route 前导的前 N-1 字节。再建立一个有效的 CONTROL route，也保持前导未完整。三条连接都进入 pending 后调用 `tr_pipeline_listener_begin_drain()`，随后补齐三条前导。

回归断言 CONTROL/DATA 接受计数不增长，route rejection 恰好增加三次，三个 pending socket 全部关闭，排空前已连接的 CONTROL 会话保留。该切片只扩展测试、开发记录和版本号，不改生产代码。

## 验证

GCC Release、ThreadSanitizer、AddressSanitizer/UndefinedBehaviorSanitizer 下均重新配置并构建运行 `test_pipeline_listener`，三次均退出码 0，输出 `pipeline listener/control transport: ok`。构建临时目录使用仓库内 `build/test-tmp`，未在 `/tmp` 写测试文件。`git diff --check` 在提交前执行。

dev.72 的 push 和 PR 全量检查已完成的项目中，ASan/UBSan、TSan、Clang 构建、静态分析及 RPC smoke 均通过；截至记录时 push 的 GCC build/test 和 PR 的 GCC build/test、Make/install 尚在运行。新 dev.73 提交的 GitHub 检查需推送后复核。

## 影响、性能与风险

只增加一条真实 TCP 场景和对应文档，不改运行时、公开 API/ABI、线协议或授权逻辑，因此没有运行时性能影响。授权 fixture 已针对 `TEST_EPOCH_2` 限定第二个 pipeline ID；新增 CONTROL route 在 drain 前有效。测试覆盖同一 listener 同一 CONTROL 的两个 DATA member reservation，不等同于连接组完整跨成员错误分类与故障矩阵验收。

回滚可撤销 dev.73，移除双 DATA pending route 回归和记录，并把版本从 `2.0.0-dev.73` 恢复到 `2.0.0-dev.72`。

## 验收标准

1. 两个不同 DATA member route 与一个有效 CONTROL route 在 drain 后补齐前导时均被拒绝。
2. CONTROL/DATA 接受计数不变，route rejection 增加三次。
3. 三个 pending socket 关闭，原有 CONTROL 会话保留。
4. `test_pipeline_listener` 在 Release、TSan、ASan/UBSan 下通过。

版本变更：`xmake.lua` 与 README 从 `2.0.0-dev.72` 更新到 `2.0.0-dev.73`。
