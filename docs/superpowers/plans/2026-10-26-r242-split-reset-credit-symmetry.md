# Split Channel RESET 信用超限对称性实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. 本任务按用户要求仅在当前会话 inline 执行，不使用子 agent。

**Goal:** 验证 Split Channel 中 CONTROL 与 BULK 任一物理连接上的 RESETTING 流信用超限，都只关闭发生错误的物理连接并保留另一 lane 的健康流。

**Architecture:** 扩展现有 `test_frozen_discard_credit_split_connection_isolation`，参数化发生超限的 lane，并分别在 CONTROL 与 BULK 上运行同一 256 字节冻结窗口加 1 字节超限序列。每次都在另一 lane 上交付 DATA、通过 PING/PONG 屏障并完成 RESET；本计划只增加测试与记录，不改变 Channel 生产逻辑。

**Tech Stack:** C11、Xmake、GCC Release、GCC ThreadSanitizer、GCC ASan/UBSan、clang-format 18.1.8、GitHub Actions。

**Spec:** GitHub [Issue #242](https://github.com/MyBlackHole/c-trpc/issues/242) 的错误隔离/RESET 验收要求；`docs/reviews/v2_remaining_work.md`；`docs/reviews/rpc_v2_reset_split_overflow_connection_isolation_20261026.md`。

## Global Constraints

- 当前分支为 `test/v2-reset-cross-message-credit-242`，继续使用 Draft PR #327。
- 本切片只修改 `tests/test_stream_reset.c`、README、剩余工作记录、审查报告、此计划和 `xmake.lua` 版本号；若测试揭示生产缺陷，先按系统化调试流程定位，再单独记录最小生产修复。
- 版本由 `2.0.0-dev.135` 升至 `2.0.0-dev.136`。
- 所有 Xmake 输出和测试临时路径使用仓库内 `build/test-tmp/`，不得在 `/tmp` 创建临时内容。
- 在 dev.135 当前 head 的 GitHub CI 全部结束并记录结果前，不推送 dev.136；可以先做本地实现和验证。
- 维持 PR 为 Draft，不合并、不删除远程分支。

---

### Task 1: 完成 dev.135 远程验证登记

**Files:**
- Read: `docs/reviews/rpc_v2_reset_split_overflow_connection_isolation_20261026.md`
- Remote: [Draft PR #327](https://github.com/MyBlackHole/c-trpc/pull/327)、[Issue #242](https://github.com/MyBlackHole/c-trpc/issues/242)

- [ ] **Step 1: 读取当前 PR 检查**

Run: `gh pr checks 327`

Expected: head 为 `2f701d2f71552a2b37e067662bd832db2ef7d96d`，所有适用检查均已完成；若有失败，先读取失败 job 日志并修复或说明根因，不进入推送步骤。

- [ ] **Step 2: 将 dev.135 结果登记到 Issue 与 PR**

仅在全部检查通过时，把 13/13 结果记入 Issue #242 和 PR #327。若 CI 失败，保留实际结果并记录失败项，不写“全绿”。

### Task 2: 覆盖 BULK 超限与 CONTROL 同伴恢复

**Files:**
- Modify: `tests/test_stream_reset.c`

**Interfaces:**
- Modify `test_frozen_discard_credit_split_connection_isolation(void)` to accept `unsigned reset_lane`.
- For each invocation, set `healthy_lane = 1U - reset_lane`; open the RESETTING target on `reset_lane` and the healthy Stream on `healthy_lane`.

- [x] **Step 1: 参数化现有 split 超限测试**

将现有测试中目标连接的硬编码 lane `0` 替换为 `reset_lane`，把健康 Stream、DATA、PING/PONG、RESET 收敛操作使用 `healthy_lane`。保留目标 Stream 的固定窗口断言：接收量与消费量为 `WINDOW`，通告上限为 `WINDOW`，超限 1 字节后 down 状态为 `TR_ERR_BAD_LENGTH`，应用交付数和 `window_updates_tx` 不增加。

在 `main()` 中调用两次：`test_frozen_discard_credit_split_connection_isolation(0U)` 和 `test_frozen_discard_credit_split_connection_isolation(1U)`。第一档验证 CONTROL 错误隔离到 CONTROL，第二档验证 BULK 错误隔离到 BULK。

- [x] **Step 2: 跑完整 Release 目标**

Run:

```sh
ulimit -c 0
TMPDIR="$PWD/build/test-tmp" xmake f -P . -c -y -m release --toolchain=gcc --crc32c_portable=n -o build/test-tmp/xmake-release
TMPDIR="$PWD/build/test-tmp" xmake run -P . test_stream_reset
```

Expected: 完整 `test_stream_reset` 通过；两个 lane 方向均在超限连接 down 后保留并完成健康同伴。

- [x] **Step 3: 跑 TSan 和 ASan/UBSan**

TSan:

```sh
ulimit -c 0
TMPDIR="$PWD/build/test-tmp" xmake f -P . -c -y -m tsan --toolchain=gcc --crc32c_portable=n -o build/test-tmp/xmake-tsan
TMPDIR="$PWD/build/test-tmp" TSAN_OPTIONS="halt_on_error=1:second_deadlock_stack=1" xmake run -P . test_stream_reset
```

ASan/UBSan:

```sh
ulimit -c 0
TMPDIR="$PWD/build/test-tmp" xmake f -P . -c -y -m asan --toolchain=gcc --crc32c_portable=n -o build/test-tmp/xmake-asan
TMPDIR="$PWD/build/test-tmp" ASAN_OPTIONS="detect_leaks=1:halt_on_error=1" UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1" xmake run -P . test_stream_reset
```

Expected: 两个完整目标均通过，无 TSan、ASan、UBSan 报告。

### Task 3: 记录 dev.136 并推送验收

**Files:**
- Modify: `README.md`
- Modify: `docs/reviews/v2_remaining_work.md`
- Create: `docs/reviews/rpc_v2_reset_split_overflow_bidirectional_20261026.md`
- Modify: `docs/superpowers/plans/2026-10-26-r242-split-reset-credit-symmetry.md`
- Modify: `xmake.lua`
- Modify: `tests/test_stream_reset.c`

- [x] **Step 1: 更新测试记录和版本**

报告写明 CONTROL 与 BULK 两个方向各自超限均只关闭本 lane，另一 lane 仍交付 DATA、通过 PING/PONG 并完成 RESET；记录 Release/TSan/ASan/UBSan 实际结果。README 与剩余工作记录标记为 dev.136，并继续声明 ERR-C/#242/V2 的其余验收未完成；把 Xmake 版本改为 `2.0.0-dev.136`。

- [ ] **Step 2: 检查格式和差异**

Run: `PYTHONPATH="$PWD/build/test-tmp/clang-format-18.1.8" python3 scripts/check_source_format.py --base origin/test/v2-reset-release-race-242 --head HEAD --clang-format "$PWD/build/test-tmp/clang-format-18.1.8/bin/clang-format"`

Expected: changed source files 格式失败数为 0；`git diff --check` 通过。清除脚本在仓库内生成的 `scripts/__pycache__/`。

- [ ] **Step 3: 按 F-0242 提交规范提交、推送并更新 GitHub**

提交标题从 `2.0.0-dev.135` 升至 `2.0.0-dev.136`，提交正文包含需求描述、背景、方案、影响、验证、相关需求、性能、风险、回滚和验收标准十项。推送到当前分支，更新 PR #327 与 Issue #242，等待新 head CI；仅在 13/13 检查通过后登记全绿结果。

## 本计划之后的未完成项

本计划只补齐 Split Channel RESET 超限的两个 lane 方向，不关闭 ERR-C 全部信用/组合矩阵，也不完成 ERR-A/B/D/E 或 V2 总验收。完成后继续按 `docs/reviews/v2_remaining_work.md` 对照 Issue #242 未覆盖的 RESET/FIFO、DATA/STATUS/半关闭、错误分类和长时间压力证据安排后续切片。
