# Reactor 跨所有者同步等待图实施计划

> **For agentic workers:** 本计划由主会话按顺序执行；用户要求不使用子 agent。Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在当前 V2 链尾 #327（`2.0.0-dev.153`）上增加无环同步跨 Reactor owner 调用支持，并在闭环等待被接受前返回 `TR_ERR_STATE`，推进至 `2.0.0-dev.154`。

**Architecture:** 在每个 `tr_reactor` 记录唯一出边、scope depth 和 inbound waiter 数，由一个静态 mutex 保护整张活动等待图。所有阻塞入口在 queue wait/condition wait/join 前建立 scope，并在所有出口清理；graceful close 的外层 scope 跨越命令完成和 TLS 关闭完成。

**Tech Stack:** C11、pthread、Reactor 内部结构、Xmake、真实 Reactor/socketpair 测试、现有 OpenSSL TLS 测试工具。

**Spec:** `docs/superpowers/specs/2026-10-26-reactor-cross-owner-wait-graph-design.md`

## Global Constraints

- 不改变公共 API/ABI 或 TRP/RPC wire。
- 无环 owner→other Reactor 同步调用保持支持。
- 形成环的最后一条等待边返回 `TR_ERR_STATE`，且不得把命令交给目标。
- 销毁中仍有等待者时保留 Reactor 对象并返回 `TR_AGAIN`。
- 测试临时目录、日志、缓存和构建产物只能写入该 worktree 的 `build/reactor-wait-graph/`，不得写入 `/tmp`。
- 按用户要求不启动子 agent；主会话内按本计划串行执行。

---

### Task 1：建立确定性环等待回归

**Files:**
- Create: `tests/test_reactor_owner_wait_graph.c`
- Modify: `xmake.lua`

**Interfaces:**
- 新测试只使用内部 `tr_reactor.h` API，不增加生产测试 hook。
- 测试通过 Reactor owner callback 实际调用目标 API；watchdog 在子进程超时时将死锁转为失败状态。

- [x] **Step 1：加入无环、二节点环、三节点环测试**

测试构造两到三个小容量 Reactor。无环场景中 A owner 调用 B owner 的函数并检查双方计数；二节点场景中 A owner 同步调用 B，而 B callback 同步调用 A；三节点场景中 A→B→C，C 再同步调用 A。对环场景，最深层调用记录必须得到 `TR_ERR_STATE`，外层无环命令必须收敛。

- [x] **Step 2：把测试接入 Xmake 并在生产改动前运行 RED**

初始 RED 阶段在 xmake 主测试目标列表加入 `test_reactor_owner_wait_graph`，不设置额外 linker wrap；destroy waiter 生命周期测试在 Task 2 Step 6 再增加测试专用 wrapper。构建与运行命令：

```sh
mkdir -p build/reactor-wait-graph/tmp build/reactor-wait-graph/xmake-tmp
env TMPDIR="$PWD/build/reactor-wait-graph/tmp" \
    XMAKE_TMPDIR="$PWD/build/reactor-wait-graph/xmake-tmp" \
    PYTHONDONTWRITEBYTECODE=1 \
    xmake test -P . -v -j1 'test_reactor_owner_wait_graph/*'
```

预期：A→B 通过；二节点和三节点 ring 的 watchdog 子进程非零退出，父测试报告 cycle 场景失败。这证明现有代码在目标行为下会自等待，不接受仅编译失败作为 RED。

- [x] **Step 3：加 quiesce、set_handler 与 stop 的竞争环测试**

用两个 owner callback 和条件变量屏障同时发起 A→B 的被测同步 API 与 B→A 的 `tr_reactor_call`。每轮都必须在 5 秒测试看门狗内完成，并恰有闭环边返回 `TR_ERR_STATE`；其它一侧完成后清理 Reactor。set_handler 使用一个已经 adopt 到 B 的 socketpair connection。stop 场景覆盖 `pthread_join` 等待并在结束时保证两 Reactor 都停止或可被显式停止。

- [x] **Step 4：重新运行测试验证失败仍局限在未实现的等待图行为**

运行同 Step 2 的命令。预期新增 wait API 的 cycle 子进程触发 watchdog；无环调用仍为 GREEN。

---

### Task 2：实现图 scope 并保护 Reactor 目标生命周期

**Files:**
- Modify: `src/execution/reactor.c`
- Test: `tests/test_reactor_owner_wait_graph.c`

**Interfaces:**
- 文件内私有 helper:
  ```c
  struct tr_reactor_wait_scope {
      struct tr_reactor *source;
      struct tr_reactor *target;
      int active;
  };
  static int tr_reactor_wait_scope_begin(
      struct tr_reactor *target,
      struct tr_reactor_wait_scope *scope);
  static void tr_reactor_wait_scope_end(
      struct tr_reactor_wait_scope *scope);
  ```
- helper 在 owner 上验证 source/target 链，在相同 source-target 的 nested scope 上递增 depth；外层退出时减少 target inbound count。

- [x] **Step 1：添加私有图状态与 cycle admission helper**

在 `struct tr_reactor` 增加由全局 wait-graph mutex 保护的 `sync_wait_target`、`sync_wait_depth`、`sync_wait_inbound` 和 `sync_wait_destroying`。在 `reactor.c` 定义静态初始化的图 mutex。begin helper 从 target 沿 `sync_wait_target` 走到 NULL；若遇到 source，返回 `TR_ERR_STATE` 且不写 scope。否则递增 inbound 与 depth 并标记 scope active。end helper 递减 depth；降至零时清出边并递减 inbound。

- [x] **Step 2：为 tr_reactor_call 加 RED/GREEN 覆盖**

先确认 Task 1 二节点和三节点测试当前失败。只将 `tr_reactor_call` 的阻塞路径包在 begin/end scope 中：同 owner 的内联调用不建边；非 owner context 使用 inactive scope；cycle reject 在 `tr_reactor_push_command_wait` 前返回。保证 sync init、queue push、sync wait 的所有错误与成功出口都调用 end。运行同 Step 2 命令，二、三节点环应通过且无环链仍通过。

- [x] **Step 3：将 scope 接入 set_handler、quiesce 和 stop**

在 `tr_reactor_set_handler`、`tr_reactor_quiesce` 和 `tr_reactor_stop` 实际阻塞前建立 scope，覆盖 queue capacity wait、condition wait 和 join。确保每个 early return 都结束 scope。运行 Task 1 的竞争环场景，各 API 都须拒绝形成环的最后一条边。

- [x] **Step 4：让 graceful close 外层持有 scope**

`tr_reactor_close_graceful` 在调用 `tr_reactor_call` 前建立 outer scope；内层 call 命中相同 source-target 后只增加 depth。close wrapper 只有在 TLS 关闭完成或同步错误退出后才结束 outer scope。用真实 TLS fixture 建立关闭待完成窗口，在 A owner 等待 B graceful close 时让 B owner 同步回调 A，确认该反向边得到 `TR_ERR_STATE`，然后 TLS close 完成且 A 返回。

- [x] **Step 5：保护停止中/销毁中的 target waiters**

`tr_reactor_destroy` 成功 stop 后通过图 mutex 标记 `sync_wait_destroying`，禁止新 scopes；若 `sync_wait_inbound` 非零则保留对象并返回 `TR_AGAIN`。现有 scopes 仍可 end 并归还 inbound 计数。destroy retry 在计数归零后继续拆资源成功。对 `tr_reactor_start` 增加 destroy-pending 检查，返回 `TR_ERR_STATE`。

- [x] **Step 6：用 linker-wrapped cond wait 强制覆盖 destroy 的 inbound 竞态**

在测试目标启用 `-Wl,--wrap=pthread_cond_wait`。测试 wrapper 只在 A owner 的同步等待被 B 唤醒后暂停其返回，期间另一测试线程 stop/destroy B。destroy 必须返回 `TR_AGAIN` 且 B 对象仍有效；释放 A waiter 后 retry destroy 返回 `TR_OK`。wrapper 通过 test-local gate 和 `__real_pthread_cond_wait` 实现，禁止加入生产 hook。

- [x] **Step 7：运行所有 wait-graph 测试并检查无泄漏**

运行目标测试 Release、ASan/UBSan 与 TSan；重复执行 100 次 cycle 场景。所有 Reactor、socketpair、pthread 同步对象均在父子进程正常路径释放；watchdog 子进程失败时由父进程回收。

---

### Task 3：验证其余阻塞路径、格式与回归面

**Files:**
- Test: `tests/test_reactor_owner_wait_graph.c`
- Test: `tests/test_reactor_fatal_exit.c`
- Test: `tests/test_reactor_stop_full.c`
- Test: `tests/test_rpc_v2_tls_reactor.c`
- Modify: `xmake.lua`

- [x] **Step 1：审查所有 Reactor 内部阻塞点的覆盖**

使用 `rg -n 'tr_reactor_sync_wait|pthread_cond_wait|pthread_join|push_command_wait' src/execution/reactor.c`。逐个映射到 `call`、`set_handler`、`quiesce`、`close_graceful`、`stop` 或明确不等待其它 Reactor 的 TLS/queue 内部唤醒。对通过 `tr_reactor_call` 委派的 API 不重复建边。

- [x] **Step 2：运行聚焦及生命周期回归**

```sh
mkdir -p build/reactor-wait-graph/tmp build/reactor-wait-graph/xmake-tmp
env TMPDIR="$PWD/build/reactor-wait-graph/tmp" \
    XMAKE_TMPDIR="$PWD/build/reactor-wait-graph/xmake-tmp" \
    PYTHONDONTWRITEBYTECODE=1 \
    xmake test -P . -v -j1 'test_reactor_owner_wait_graph/*' \
      'test_reactor_fatal_exit/*' 'test_reactor_stop_full/*'
```

预期所有测试退出 0；wait graph watchdog 未触发，fatal-exit 对已接受调用的完成契约不变，stop-full 的 FIFO/join 重试计数不变。

- [x] **Step 3：运行 TLS/格式/SDK 验收**

运行 `test_rpc_v2_tls_reactor/*` 与 SDK C11/C++11 消费端；用仓库格式检查只检查改动文件；运行 `git diff --check`。真实 graceful-close cycle 必须在 TLS close completion 返回前拒绝 B→A。

- [x] **Step 4：构建并运行 Release、ASan/UBSan 和 TSan**

每种 sanitizer 使用自己的 `build/reactor-wait-graph/<mode>/` 输出目录，所有 `TMPDIR` 和 `XMAKE_TMPDIR` 指向该 worktree 下目录。记录完整命令、退出码和目标计数；不将该切片测试外推成 #243 或整体 V2 完成。

- [x] **Step 5：更新版本与说明**

更新 `README.md`、`docs/rpc-v2-sdk.md` 或内部 Reactor 文档中的 owner 等待错误语义，并根据 PR #315 及其后继 #316–#327 的 dev 版本链，确定无冲突的候选版本号；只在所有测试通过后提交。保留 #243 其他未完成条目，不将其整体关闭。

- [ ] **Step 6：提交变更并推送 #328 链尾 PR**

运行 `git diff --check`、格式检查、目标测试与 `git status --short --branch`。提交正文按功能提交格式说明需求、根因、方案、影响、测试、性能、风险与回滚，再创建基于 PR #327 的 Draft PR #328 并附上验证证据。此版本落点保留 #315–#327 的原有历史，不改写其逐次版本推进。
