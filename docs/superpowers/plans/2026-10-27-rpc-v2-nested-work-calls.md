# RPC V2 Server work 发起嵌套 Client Call 实施计划

> **执行方式：**使用 `superpowers:executing-plans` 在当前会话内逐项执行。用户要求不使用子 agent；步骤用复选框跟踪。

**目标：**允许 Server V2 `work` 回调短暂同步提交一个子 Client Call，并用真实 TCP A→B→A 回归证明父上下文、异步回复和所有终局均收敛。

**架构：**区分运行在有界工作线程上的 Server `work` 回调与运行在 Reactor owner/Client 回调中的同步禁入上下文。仅允许 `work` 回调同步等待目标 Reactor 接受一次新 Call；它必须立即返回并用 `tr_rpc_v2_reply_acquire_producer()` 延后父回复，不能等待子 Call 终局。子 Call 通过 `tr_rpc_call_options.parent_context` 继承取消和 deadline。现有 Reactor owner 等待图与 Client 回调禁入规则保持有效。

**技术栈：**ISO C11、pthread、Xmake、公开 V2 Client/Server API、真实回环 TCP。

**设计依据：**`docs/superpowers/specs/2026-10-27-rpc-v2-operation-kernel-design.md` 的 owner/worker 隔离、父子取消和回调/终局/资源三屏障；任务 #243 的嵌套/跨 Reactor 验收。

## 全局约束

- 不改变公开函数签名、ABI 或 TRP/RPC wire。
- 可变协议状态继续只由各自 Reactor owner 修改；Server `work` 只能提交 Client Call，不能直接访问 Client/Server 内部状态。
- Server `work` 运行于有界 TaskExecutor；不得等待子 Call 终局、同一执行器任务或依赖该 worker 返回的资源。
- 子 Call 只在父上下文仍有效且期限未过时受理；父取消和 deadline 传播到后代。
- Client 回调和 Server owner hook 仍不得同步跨到另一个 I/O owner。
- 使用 Xmake；临时目录、日志、缓存和构建产物均放在当前 worktree `build/` 内，不写入 `/tmp`。
- 不新增或恢复源码改动行格式 CI 门禁，不更名现有必需检查。

---

### 任务 1：建立真实 A→B→A RED 回归

**文件：**
- 新建：`tests/test_rpc_v2_nested_calls.c`
- 修改：`xmake.lua`

**接口与场景：**
- 测试只使用 `include/tr` 公开 API；建立两个独立 Client/Server 门面和三条真实 TCP 调用：A Server `work` 提交 B Client 子调用，B Server `work` 再提交 A Client 子调用，第二次到达 A 的 Server 作为叶节点返回。
- 两个中间 Server `work` 都持有父上下文并取得回复 producer；调用子 Client 后立即返回，不等待子终局。子 Client terminal 结束相应父 producer。
- 请求方向为 ONE，响应方向为 MANY，使中间调用可用空响应 terminal 完成；叶节点交付一条响应正文。
- 记录每次 `tr_client_call_start()` 的返回值。测试要求两个 Server `work` 内的提交均为 `TR_OK`，Client 回调中跨到另一 Client 的提交仍为 `TR_ERR_STATE` 且不改输出。
- 根调用、子调用、孙调用均必须只终局一次；Server A 终局两次、Server B 终局一次；三个 Client/Server 销毁和排空成功，所有 held context 与 producer 引用归还。

- [x] **步骤 1：先新增两个独立门面和固定容量测试夹具。**
- [x] **步骤 2：加入 A→B→A work/producer/parent-context 回调链及精确终局断言。**
- [x] **步骤 3：在 Client `writable`、Server owner observer、authorize 和 terminal 回调中尝试跨 Client `call_start`，断言仍拒绝且哨兵输出不变。**
- [x] **步骤 4：注册 `test_rpc_v2_nested_calls` 并运行 RED。** 旧门禁稳定返回 `TR_ERR_STATE`；修复后目标通过。

运行：

```sh
mkdir -p build/test-tmp/tmp build/test-tmp/xmake-tmp
env TMPDIR="$PWD/build/test-tmp/tmp" \
    XMAKE_TMPDIR="$PWD/build/test-tmp/xmake-tmp" \
    xmake test -P . -v -j1 'test_rpc_v2_nested_calls/*'
```

预期：A Server `work` 中首次嵌套 `tr_client_call_start()` 被现有通用 callback gate 返回 `TR_ERR_STATE`；根调用以错误终局结束，测试在预期的返回码断言失败，而非超时或构造失败。

### 任务 2：区分 Server work 与同步禁入回调

**文件：**
- 修改：`src/rpc/rpc_record_dispatch.c`
- 修改：`src/rpc/rpc_record_dispatch.h`
- 修改：`src/client.c`
- 修改：`include/tr/rpc_v2.h`

- [x] **步骤 1：为 `tr_dispatch_v2_work()` 增加独立的线程局部 work-callback 标记。** 只在实际 `job->v2_work()` 调用范围设置并恢复；authorize、observe、admit、terminal 不设置。
- [x] **步骤 2：增加内部查询函数 `tr_rpc_record_dispatch_in_work_callback()`。** 保留现有 `tr_rpc_record_dispatch_in_callback()` 的广义语义供其他调用点使用。
- [x] **步骤 3：调整 `tr_client_record_sync()` 禁入条件。** 其他同步操作继续使用严格门禁；只对 Client Call 提交入口开放 Server V2 `work` worker。
- [x] **步骤 4：更新 V2 API 注释，明确 work callback 只允许提交子 Call，不得等待其 terminal；要异步完成父回复时必须使用 reply producer 与 parent context。**
- [x] **步骤 5：重跑 RED 目标，确认 A→B→A 与 Client callback 负向断言通过。**

### 任务 3：生命周期与全量验证

- [x] **步骤 1：Release 下连续运行 A→B→A 目标 25 次，确认无死锁、重复终局、引用泄漏或 slot 残留。**
- [x] **步骤 2：运行该目标的 TSan 与 ASan/UBSan。**
- [x] **步骤 3：运行 GCC Release 全量 Xmake 测试。** 94/94。
- [x] **步骤 4：检查公开头文件 C11/C++11 编译与 `git diff --check`；改动行格式待提交后以仓库固定版本做本地可选检查，不修改格式 CI 门禁。**
- [x] **步骤 5：更新 README、#243/#239 验收记录和 V2 剩余工作清单。** 只将 A→B→A 提交/父子上下文/异步回复这一场景标为已验证；队列压力、TLS/压缩共享调度与整体 V2 验收继续保持未完成。
- [ ] **步骤 6：按功能提交格式更新开发版本、提交并推送到 PR #332；等待新 head 的全部远端检查通过后更新 GitHub 记录。** 不合并 PR。

## 自审

- 本计划没有放宽 Reactor owner callback、Client callback、authorize 或 Server owner hook 的同步等待限制。
- 测试验证跨 facade 的真实 RPC，而不是仅调用 wait-graph 原语；所有异步回复都由 producer 完成。
- 测试中任何一个 parent context 或 producer 未释放都必须使 drain/destroy 无法通过，避免仅以 terminal 数量代替生命周期证据。
- 此切片不能关闭 #243/#239，也不能替代 command/completion 满载、TLS/压缩接入和最终验收矩阵。
