# RPC V2 父子调用上下文实施计划

> **执行方式：**本计划在当前会话内逐项执行；不启动子代理。每个行为切片先写失败测试，再实现最小逻辑并复验。

**目标：**让 Server V2 工作能够将父 Call 的合作取消和绝对期限安全地传给下游 Client Call，且不因子期限延长父期限或在父终局后继续交付迟到工作。

**架构：**在 `tr_rpc_call_options` 中增加可选的父上下文借用句柄。Server Dispatch 使用与 Call 容量相同的固定状态池承载跨回调上下文；父 Call 终局原子发布取消原因，子 Client 保留状态引用并通过原有进度泵取消。父期限与子本地期限取较早者，且 Server 传输槽可在其网络终局后正常回收。

**技术栈：**ISO C11、POSIX 单调时钟、C11 原子、Xmake、现有 `test_rpc_v2_sdk` 真实 TCP 测试。

**设计：**`docs/superpowers/specs/2026-10-26-rpc-v2-parent-context-design.md`

## 全局约束

- 公共说明、代码注释和提交说明使用简体中文；标识符与机器接口保持原样。
- 测试临时目录、日志、缓存和构建产物只放在当前 worktree 的 `build/`，不得写入 `/tmp`。
- 不新增动态线程、无界队列或每个父上下文的堆分配；固定上下文状态池容量等于 Dispatch Call 容量。
- 已受理 Call 仍恰好一个终局；未受理的拒绝不写输出、不回调、不接管正文。
- `tr_rpc_v2_call_context` 是唯一拥有型句柄；成功受理的子 Call 各持一个内部引用，所有引用归零前 Server 不得释放状态池。
- 不修改既有 CI 检查名称，不以格式门禁或仅编译成功代替功能验证。

---

### 任务一：用公共 SDK 测试锁定上下文接口和未受理错误合同

**文件：**
- 修改：`tests/test_rpc_v2_sdk.c`
- 后续实现：`include/tr/rpc.h`、`include/tr/rpc_v2.h`

**接口：**
- Server `work` 回调通过 `tr_rpc_v2_cancel_context_hold(cancel, &context)` 获得唯一拥有型句柄。
- Client `options.parent_context = &context` 借用句柄；调用成功受理后 Client 内部持有引用。
- `tr_rpc_v2_call_context_release(&context)` 释放应用引用。

- [x] **步骤 1：先写未声明 API 的消费测试。**在 `test_rpc_v2_sdk.c` 增加父工作回调：成功取得上下文并发布给控制线程；控制线程传入子 Client `call_start`。同时检查传入已终止上下文时返回正确错误、`out` 哨兵值不变、回调计数为零。
- [x] **步骤 2：运行单目标并确认 RED。**配置目录使用 `build/linux/x86_64/debug`，命令为 `TMPDIR="$PWD/build/tmp" xmake test -v -j1 'test_rpc_v2_sdk/*'`；初始失败应是新公共上下文类型/函数尚不存在，不得因语法错误或其他既有测试失败误判。
- [x] **步骤 3：增加公共声明。**在 `rpc.h` 前向声明上下文类型，并在 `tr_rpc_call_options` 末尾加入 `parent_context`；在 `rpc_v2.h` 定义私有槽句柄和 hold/release 声明及所有权注释。再次运行目标，预期编译成功、链接因实现尚缺而失败。
- [x] **步骤 4：为句柄校验和上下文状态池写最小实现。**在 `rpc_record_dispatch.c` 为 Dispatch 预留 `capacity` 个状态；首次 hold 从池中取得一个状态，写入父期限和代次，输出句柄仅在成功时修改。池耗尽返回 `TR_AGAIN`，无动态分配。
- [x] **步骤 5：验证 RED 转 GREEN。**再次运行同一目标，检查正常 hold/release、陈旧代次拒绝、池耗尽和失败不改输出断言通过；确认不带 `parent_context` 的既有 SDK 用例行为不变。

### 任务二：传播父取消并抑制迟到应用回调

**文件：**
- 修改：`src/rpc/rpc_record_dispatch.c`、`src/rpc/rpc_record_dispatch.h`
- 修改：`src/rpc/rpc_record_client.c`、`src/rpc/rpc_record_client.h`
- 测试：`tests/test_rpc_v2_sdk.c`

**接口：**
- Dispatch 终局把 `DEADLINE_EXCEEDED` 原因保留；其他父终局对仍活动子 Call 传播为 `CANCELLED`。
- Client 固定 Call 槽保存状态引用；上下文状态在 owner 终局前后使用 acquire/release 原子语义。

- [x] **步骤 1：增加父取消 RED 场景。**真实 TCP 下父工作保存上下文，子 Client 以该上下文启动下游 Call；测试通过父 Client cancel 触发父 Server 任务取消，断言子端唯一终局为 `CANCELLED`，父上下文取消可见后不再交付子 MESSAGE/metadata。
- [x] **步骤 2：确认失败原因。**在未实现传播时运行 `test_rpc_v2_sdk`；测试应因子 Call 未取消或收到迟到应用回调而失败，并打印父/子终局计数与状态，不接受超时挂起作为有效 RED。
- [x] **步骤 3：实现跨 owner 的非阻塞观察。**父 Dispatch 原子发布上下文终局状态；Client 每轮既有 `tr_record_client_progress` 检查状态并经原绑定选择终局，不同步调用父 Reactor、不新增轮询线程。
- [x] **步骤 4：在消息回调入口再次核验。**`tr_record_client_received` 在交付 message/metadata 前读取父上下文状态；若上下文取消已发布则提交对应 abort 并抑制本条应用回调。已有先行 FINAL 仍服从绑定的 first-final-wins。
- [x] **步骤 5：运行 RED 场景并复验既有 SDK 目标。**确认父取消时子端恰好一次 `CANCELLED`，取消后消息计数不增；无父上下文的原有 cancel/deadline 与 unary/many 测试继续通过。

### 任务三：父期限继承、子期限取最小值

**文件：**
- 修改：`src/rpc/rpc_record_client.c`
- 测试：`tests/test_rpc_v2_sdk.c`
- 文档：`include/tr/rpc.h`、`include/tr/rpc_v2.h`

- [x] **步骤 1：增加期限继承 RED 场景。**父 Client Call 设置 200 ms 期限，子 Call 设置 5 s 期限；子端必须收到 `DEADLINE_EXCEEDED`。另测子 `timeout_ms == 0` 仍继承父期限，以及 25 ms 子期限先于较长父期限终结。
- [x] **步骤 2：运行并确认失败。**运行 `TMPDIR="$PWD/build/tmp" xmake test -v -j1 'test_rpc_v2_sdk/*'`；若子 Call 错误地等到 5 s、返回非期限状态或父/兄弟 Call 被取消，作为预期 RED。
- [x] **步骤 3：计算有效绝对期限。**在同一次 Client 受理时间采样中，将父绝对期限与 `child_accept_time + timeout_ms` 取最小值；父无期限且子期限为零时保持零。向 OPEN 写入该有效期限的剩余时间。
- [x] **步骤 4：保证已过期上下文不受理。**在 `call_start` 写 `out` 和设置 accepted 前，检测父上下文取消/期限；取消返回 `TR_ERR_CANCELLED`，期限返回 `TR_ERR_TIMEOUT`，释放临时引用与请求副本，不调用回调。
- [x] **步骤 5：验证边界。**确认父期限不被 5 s 子期限延长；25 ms 子期限只终结该子 Call；同时运行已有 deadline 以及 GOAWAY 未知结果用例。

### 任务四：引用归还、池复用和销毁重试

**文件：**
- 修改：`src/rpc/rpc_record_dispatch.c`、`src/rpc/rpc_record_endpoint.c`
- 修改：`src/rpc/rpc_record_client.c`
- 测试：`tests/test_rpc_v2_sdk.c`

- [x] **步骤 1：增加生命周期 RED 场景。**父 work 取得上下文，子 Call 成功受理后应用先释放父句柄；Server destroy 必须返回 `TR_AGAIN`，而子 Call 终结并退役后再次 destroy 必须成功。
- [x] **步骤 2：验证池耗尽与代次复用。**通过固定小容量 Server 同时保留多个上下文，耗尽时 hold 返回 `TR_AGAIN` 且不写 out；释放旧上下文、复用状态槽后，旧代次句柄返回 `TR_ERR_STALE`。
- [x] **步骤 3：实现外部引用总账。**每个 public hold 和被接受的 Client Call 各加/减一次外部引用；Client 槽只有在绑定退役后释放自己的上下文引用；Server normal destroy/reap 在外部引用非零时取消上下文并返回 `TR_AGAIN`，不释放 Dispatch/池内存。
- [x] **步骤 4：保持网络槽独立回收。**父 Stream 终局后 Endpoint 按既有关闭/RESET 屏障退役网络槽；上下文状态存于独立 Dispatch 固定池，不以子 Call 生命周期占用已关闭的 Stream 槽。
- [x] **步骤 5：复验完整生命周期目标。**运行 `test_rpc_v2_sdk`、`test_rpc_v2_server_destroy_drain`、`test_server_record`、`test_client_record`，确认 Server/Client 多次 destroy 重试、停止域回收、上下文释放和池归还无泄漏或双重释放。

### 任务五：完整构建与并发验证

**文件：**
- 修改：`docs/reviews/v2_remaining_work.md`
- 新增：`docs/reviews/rpc_v2_parent_context_20261026.md`

- [x] **步骤 1：GCC Debug 全套。**将 `TMPDIR` 指向当前 worktree `build/tmp`，运行 `xmake f -c -y -m debug --toolchain=gcc --crc32c_portable=n`、`xmake build --all`、`xmake test -v -j1`。
- [x] **步骤 2：GCC Release 全套。**用独立 Release 产物目录重新配置和运行全部已注册目标，避免复用 Debug 二进制。
- [x] **步骤 3：ASan/UBSan 与 TSan。**分别以 GCC 配置对应模式运行全套测试；上下文跨 owner 引用重点检查 use-after-free、数据竞争、重复 release 和代次复用。
- [x] **步骤 4：Clang Release 与 SDK 消费。**运行全套 Clang Release；安装后 C11/C++11 公共 SDK consumer 检查新字段、新符号及零初始化兼容。
- [x] **步骤 5：记录并审查证据。**新增中文审查记录说明父/子行为、固定状态池上限、测试命令和结果；更新剩余工作台账只核销 #243 的父子上下文子项，不关闭 #243 或宣称 V2 完成；运行 `git diff --check`。
- [ ] **步骤 6：提交并推送。**使用中文功能提交格式描述需求、背景、实现、影响范围、性能和测试；推送到基于 `feat/reactor-owner-wait-graph-243` 的新分支并在 PR 说明中明确其 stacked base。
