# RPC V2 连接组 CONTROL/DATA pending route 排空回归

日期：2026-10-25。关联 #242/ERR-A。版本：`2.0.0-dev.71 -> 2.0.0-dev.72`。

## 需求与背景

确认连接组 listener 开始排空后，不完整 route 前导不能在随后补齐时建立新的 CONTROL 或 DATA 连接。dev.71 已修复同步 DATA 接受入口的 draining 检查；本切片把真实网络回归扩展到 CONTROL，并在同一时序中验证两种 route。

## 实现

测试保留一个已连接的 CONTROL 会话，同时创建一个有效的新 CONTROL route 和一个通过 DATA_OFFER 预留的 DATA route。两个新 socket 都只发送 route 前导的前 N-1 字节；`tr_pipeline_listener_begin_drain()` 返回后，再分别发送最后一个字节。

断言确认 CONTROL/DATA 接受计数都不增长、route rejection 恰好增加两次、两个 pending socket 都关闭，原有 CONTROL 会话仍在 listener 中。测试授权 fixture 仅为此场景允许 `TEST_EPOCH_2` 下的第二个 pipeline ID；生产逻辑没有变化。

## 验证

以下命令均针对 `test_pipeline_listener`，使用仓库内 `build/test-tmp` 作为 `TMPDIR` 和 `XMAKE_TMPDIR`，结果均为退出码 0，输出 `pipeline listener/control transport: ok`：

- GCC Release：`xmake f -P . -c -y -m release --toolchain=gcc --crc32c_portable=n`，随后构建并运行 `test_pipeline_listener`。
- GCC ThreadSanitizer：以 `-m tsan` 配置后构建并运行 `test_pipeline_listener`；同次构建还运行 `test_connection_group_tls`，退出码 0。
- GCC AddressSanitizer/UndefinedBehaviorSanitizer：以 `-m asan` 配置后构建并运行 `test_pipeline_listener`。
- `git diff --check` 在提交前执行。

dev.71 的 GitHub 全量 TSan 检查 `40152560851` 有一条 `test_connection_group_tls` 以 `-1` 退出，日志没有给出断言或 sanitizer 诊断；该检查中的 `test_pipeline_listener` 通过。dev.71 的另一条 TSan 全量检查 `40152563622` 通过。本机重新运行 `test_connection_group_tls` 的 TSan 目标通过。dev.72 推送后的 GitHub 检查仍需等待并单独记录。

## 影响、性能与风险

仅修改 `tests/test_pipeline_listener.c`、版本号和开发记录；不改生产代码、公开 API/ABI 或 wire 格式，因此没有运行时性能影响。测试覆盖同步 listener route 前导与 drain 的边界，不代表连接组跨成员接受和 ERR-A/B/C/D/E 全部矩阵已经完成。

测试 fixture 的第二个 pipeline ID 仅在 `TEST_EPOCH_2` 下授权；该条件使新增 CONTROL route 在 drain 前有效，避免用无效路由造成误通过。回滚可撤销 dev.72，移除此扩展测试与记录，并将版本恢复为 dev.71。

## 验收标准

1. drain 后补齐的有效 CONTROL 与已预留 DATA route 均被拒绝。
2. CONTROL/DATA 接受计数不变，route rejection 增加两次，两个 pending socket 关闭。
3. drain 前已连接的 CONTROL 会话仍保留。
4. `test_pipeline_listener` 在 Release、TSan、ASan/UBSan 下通过。

版本变更：`xmake.lua` 与 README 从 `2.0.0-dev.71` 更新到 `2.0.0-dev.72`。
