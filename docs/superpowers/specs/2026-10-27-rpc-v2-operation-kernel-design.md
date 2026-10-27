# RPC V2 通用异步操作内核设计

**状态：** 第一阶段核心实现与真实调用方迁移已完成；PR #332 的 dev.164 head 检查 13/13 通过。dev.165 增补 terminal callback 内 hold CallRef、跨回调读取结果并阻止 Client destroy 的真实 TCP 回归，GCC Release 全量测试 93/93 通过；dev.165 新 head 的远端检查待复验。后续跨能力与全量验收仍未完成。

**关联：** #239 ARCH-07、#243；依赖现有 TaskExecutor、CompletionQueue、CallRef、父子上下文与 TLS-D2 操作契约。

**基线：** PR #330 head `a8b0f8aa31c4569005e6a404015a81cee2c55074`，`2.0.0-dev.158`。

**边界：** 先统一库内异步操作的身份、所有权和完成生命周期；不改变 wire，也不在本设计中调整普通 V2 的公开 ABI。

## 问题与现有证据

已批准的 #239 ARCH-07 要求明确区分提交拒绝、操作受理、完成发布和最终释放。现有组件分别解决了相邻问题，但没有共同的生命周期对象：

- `src/execution/task_executor.c` 管理工作任务及其完成回调。Task 是一次工作执行；其 done 返回后任务槽才归还，不能单独表达 RPC/凭证/压缩操作在 owner 消费结果前仍然存活。
- `src/execution/completion_queue.c` 管理有界完成通知交接。队列解决事件所有权和关闭代次，不拥有调用终局或业务结果。
- `src/io/tls_provider_operation.c` 已有 reserve/accept/complete/cancel/result/retire 状态，但结果种类、凭证缓冲、预算和生命周期都绑定在 TLS provider 表内。
- `src/rpc/rpc_record_dispatch.c` 管理每调用事件 FIFO、业务 Task、回复和终局；异步授权路径仍按 dispatch 槽位自行关联任务完成。
- V2 `CallRef` 延长 Call 结果槽生命周期；它不代表一个独立后台操作，也不允许完成方在 Call 终局后恢复协议状态。

因此各能力虽然分别有局部代次与取消检查，却难以共同证明“调用已终局、最后用户回调结束、所有异步工作和缓冲回收”这三个屏障。单靠给 TaskExecutor 增加字段也不能解决：执行任务的结束时点不等于 owner 已消费结果或异步操作已退役。

## 目标

1. 提供库内可复用、有固定容量的异步操作状态机；每个 owner 域各自创建操作表，不建立全局线程、队列或单例。
2. 保持 #239 的四个关键阶段可观察：未受理拒绝、已受理、完成已发布、操作已退役。
3. 使用实例号、槽位和不回绕代次标识操作；旧代次完成、取消、读取或释放不能触及复用槽。
4. 明确负载、执行方、owner 和显式操作引用分别持有的责任；每个成功受理的操作最终只有一次结果发布或一个明确的 owner 停机清理终态。
5. 取消仅表示合作式停止请求。取消与完成可以任意竞争；迟到结果必须被消费并清理，但不能通过内核本身恢复 Call、Attempt、profile 或 route 状态。
6. 调度继续由现有 TaskExecutor 或异步 provider 执行；完成唤醒继续由现有 CompletionQueue、命令队列和 eventfd 负责。
7. 第一批使用者为 RPC Server 异步 `authorize` 与 TLS-D2 provider operation；后续以相同接口接入压缩异步工作。每次迁移都保留调用侧的错误、期限与数据所有权语义。

## 非目标

- 不以通用内核替代 TaskExecutor、CompletionQueue、Reactor owner、Call/Attempt、TLS profile 或压缩预算域。
- 不在内核内创建 pthread、eventfd、队列、定时器、等待循环或无界堆对象。
- 不把所有异步动作改成同一个公开 SDK API；首期内核为内部接口，不改变公共 ABI、线协议或默认并发数。
- 不提供强制终止不合作的用户工作线程；已受理操作仍需等待工作函数退出或进入经证明的 stopped-owner 清理路径。
- 不把操作结果成功等同于 FINAL 已写出或远端业务已执行。

## 方案比较与选择

| 方案 | 优点 | 代价 | 结论 |
| --- | --- | --- | --- |
| 扩展 TaskExecutor，让每个任务同时成为 RPC/凭证/压缩操作 | 复用现有代次句柄和容量 | 把调度、工作线程退出、owner 结果消费和 Call 终局绑在同一状态；非 Task 的异步 provider 也需伪装成 Task | 不采用 |
| 保留每个子系统自己的状态表，只补相似测试与文档 | 变更较少，短期迁移风险低 | TLS、RPC、压缩持续重复实现受理/取消/完成/退役协议，不能满足通用内核验收 | 不采用 |
| 增加内部通用操作池，调度和唤醒继续由现有执行层提供 | 生命周期统一而保留调度边界；可逐个接入真实使用者 | 新增一个固定槽状态机，需严格定义结果所有权与关闭竞态 | **采用** |

选定方案使“操作身份与寿命”成为可复用层，TaskExecutor 仍表示“在哪里执行一次工作”。操作池由调用域提供容量、预算和结果处置函数；它不替调用域决定业务语义。

## 内部接口与状态

新增内部模块 `src/execution/async_operation.{h,c}`。具体函数命名在实施计划中冻结；最低能力如下：

- `tr_async_operation` 是弱身份值，由 pool instance、slot 和 generation 组成；它不延长池或槽位寿命。`tr_async_operation_ref` 是唯一、move-only 的强能力，另含不可复用的 ref token。每个操作同一时间只允许一个强 ref，沿 producer→完成队列→owner 的路径转移；不提供 clone。Call 的长期结果仍由独立 `tr_rpc_v2_call_ref` 持有。
- `reserve(pool, spec, out_operation, out_producer_ref)`：检查容量与代次，建立尚未对外受理的 RESERVED 槽，并发给提交路径唯一的 provisional producer ref。`spec` 只含按值复制的种类、owner/domain 身份和关联代次，不保留调用方指针，也不转移输入或结果 payload。失败不改输出、不接管任何资源；容量满返回 `TR_AGAIN`。
- `accept(pool, operation, out_completion_ref)`：确认 TaskExecutor/provider 已接受本次操作。有效 RESERVED 槽在下游明确接受后必须可成功 accept；接受后即使调用方关闭或发起取消，操作池仍承担一次完成发布/清理责任。通常 `out_completion_ref` 保持空；若 producer 在 accept 线性化前已经发布完成，操作池内部暂存结果并在 accept 时返回 completion ref。这样结果不会先于受理交给 owner。accept 不接管工作输入；输入生命周期由调用域的 Task/provider 参数单独持有。
- `reject(pool, &producer_ref)`：只终止尚未受理且没有完成回调的 RESERVED 槽；拒绝后 producer 不得再访问或完成该代次。成功时消费并清零 producer ref。若 inline completion 已发生，reject 失败且不改变 ref/输出；调用方必须按已接受的操作清理，不能把它报告成可安全重试的未受理。
- `request_cancel(pool, operation, reason, out_notify)`：最多线性化一次取消请求。返回 `out_notify=true` 的调用方负责恰好一次地通知执行方；重复取消不产生额外通知。
- `publish(pool, &producer_ref, completion, out_completion_ref)`：完成方以同一实例/代次发布不可变结果；completion 携带状态、结果 payload 指针及一次性 destroy 函数。若 operation 已 ACCEPTED，成功将唯一强 ref 转为 completion ref；若仍 RESERVED，先把结果与强 ref 暂存于操作池，成功时消费并清零 producer ref，`out_completion_ref` 保持空，等 accept 线性化后再由 accept 返回 completion ref。失败不转移 completion/ref 所有权，也不修改输出。成功只表示结果责任转入操作池；不表示 owner 已应用结果或网络已发送。重复、旧代次和已退休句柄均不改变槽位或结果。
- `consume(pool, completion_ref, out_completion)`：只由绑定 owner 调用，移交或丢弃唯一结果。owner 必须在产生副作用前检查保存的 domain/Call/Attempt/profile/route 代次与截止时间。结果 payload 移交后由 owner 负责使用或析构；如选择由池销毁，则只调用注册的 destroy 函数一次。`consume` 不提前 retire 操作。
- `retire(pool, &completion_ref)`：只有生产者已无法再访问操作、完成已由 owner 消费或丢弃、结果销毁/移交已完成时才成功；成功时原子消费最后的强 ref 并清零调用方 token。owner pump 随后才可复用槽位。
- `get_stats(pool, out_stats)`：读取固定 pool 状态计数，输出只在成功时更新；不返回内部指针、不采样时钟。
- `close/reap_stopped`：关闭准入后拒绝新 reserve。已接受操作保持在池中；普通销毁有未退役操作时返回 `TR_AGAIN`。只有外层先证明旧 owner 和生产者永久静默后，清理 owner 才能接管清理；接管路径不得假装原 owner 成功应用了结果。

核心状态如下。取消是 ACCEPTED 后的竞争标志，不是跳过完成和回收的捷径。

```text
FREE → RESERVED → ACCEPTED → PUBLISHED → CONSUMED → RETIRED → FREE
       ├─ inline publish → READY_RESERVED ── accept ──→ PUBLISHED
       └─ reject → RETIRED → FREE

ACCEPTED -- cancel_requested (一次通知) --> ACCEPTED
```

`RESERVED` 期间发生的同步完成会先缓存 payload 与唯一强 ref，状态成为 READY_RESERVED；`accept` 将其直接推进为 PUBLISHED 并返回 completion ref。此交错不允许调用方在 provider 已完成后把本次操作报告为“未受理”。`PUBLISHED` 的结果仅可被消费一次。强 ref 在线性转移时更新 token，旧副本即使被误用也不能消费、释放或退役新阶段；`RETIRED` 是资源回收阶段，槽位在强 ref 释放前不能回到 FREE。

### 输入、结果和内存所有权

- `spec` 是无指针的按值描述，包含 operation kind、owner domain 身份、可选调用/Attempt/profile/route 代次与绝对 deadline。任务输入由调用域的 Task/provider 参数持有；操作池不保存或销毁输入 payload。
- `publish` 失败时 result payload 仍由 producer 所有；publish 成功时结果及其 destroy 函数一并交给操作池。结果消费后由 owner 接管并负责析构，或明确留给池析构；不得两方同时拥有。
- payload/result 字节仍由具体使用者按实际容量预约其既有 `tr_memory_budget`；操作池只记账固定 slot/ref storage 和不透明对象指针，不推断 TLS、RPC 或压缩对象大小。
- destroy 函数恰好运行一次且不得重入同一操作池。对 TaskExecutor 的接入顺序是 reserve → executor submit → accept：提交前将 provisional producer ref 与任务输入分别放入 task arg；submit 拒绝时 task arg 和输入仍归调用方，reject 消耗 ref；submit 接受后调用 accept。若 task 已在线性化的 accept 前完成，publish 将结果暂存于 RESERVED，accept 返回时把 completion ref 交给提交路径以续交。对 provider 同样先 reserve，再把 producer ref 与 provider 所需输入分别交给 provider；provider 拒绝且未回调时 reject，provider 已同步回调后不能再报告未受理。
- 某个阶段容量不足时不得先消耗输入再返回 `TR_AGAIN`。已受理后若完成队列满，生产者保留 completion ref 和结果所有权，通过现有队列的等待交接或外层容量恢复通知续交；绝不丢弃完成通知。

### 身份、取消与结果竞争

- 每个池有不复用的 instance；每槽 generation 递增，到最大值时永久停用该槽，不回绕；每次 ref 转移使用新的非零 token，不回绕。
- 弱 operation identity、强 `tr_async_operation_ref`、borrowed `tr_rpc_v2_call`、强持有 `tr_rpc_v2_call_ref` 和 Task handle 是不同能力。operation ref 仅在一个操作内部跨异步边界转移，不公开给 SDK，也不让用户在回调外访问可变协议对象。
- `request_cancel`、provider completion 和 owner close 通过池锁/原子状态选择唯一操作结果。内核只记录操作层是否取消以及 completion 到达，不替代调用方的 Call 终局策略。取消发生在 PUBLISHED 之后返回 `TR_ERR_STATE`，不能改写结果或重发 provider 通知。
- completion 携带提交时捕获的域身份；调用方在 owner 消费前验证至少 Call slot generation 和 Attempt generation。TLS 使用 profile/connection/route generation；压缩使用 Stream/变换任务 generation。任一失配时只运行结果清理路径。
- deadline 仍由调用域的单调绝对期限判断。内核不重置期限，不因为引用存活延长父子期限。
- 取消通知只负责告诉 provider/work。接受后的工作必须退出并发布结果，或由外层在证明生产者永远静默后进入 stopped-owner 清理；“已经请求取消”本身不等于可释放内存。

### 并发域与完成队列

- 每个操作池绑定一个 completion owner。单个 pool mutex 线性化状态、generation、ref token、cancel flag 和结果指针；不得在锁内调用 provider、应用回调、payload destructor、TaskExecutor 或 CompletionQueue。
- reserve/accept/consume/retire/close 由绑定 owner 调用并在短临界区内完成；producer 可从其他线程 publish。取消先在锁内设置一次性标志，再于锁外通知执行方；只有赢得该状态转换的一方通知。
- 操作池不替换 CompletionQueue。producer 在非 owner 线程 publish 后把 completion ref 放入现有 CompletionQueue；队列项携带受控槽位身份和完整 instance/generation/token。队列 push 成功才转移该 ref；队列 push 失败时调用方仍持有 ref 与结果。其他 push 所有权规则仍以 `completion_queue.h` 为准。`push_wait()` 满载阻塞只允许非 owner producer 使用；owner 上完成的操作直接 consume，或由所属模块的有界 owner pump 重试，不得阻塞 owner 等待自己腾出队列空间。
- 队列暂满时非 owner producer 保留 completion ref 与结果，并在 `not_full` 上等待；关闭或 admission generation 变化返回后，producer 保留所有权，必须转入外层 stopped-owner 清理责任。队列准入不是完成终局，队列关闭不能释放仍由 producer 使用的操作池。
- 操作池 stats 是固定容量快照，至少报告 capacity、各状态计数、generation-exhausted slots 和 reserve-full 次数；不添加方法名/远端字符串标签。该快照用于 owner 诊断和确定性容量断言，不在热路径采样时钟。
- owner 不可同步等待依赖自身 pump、completion dispatch 或其回调继续推进的操作。现有跨 Reactor wait graph 约束继续有效；操作引用不能绕过循环等待拒绝。
- operation ref 的强能力只允许单线性转移，不允许复制成多个持有者。结果读取、取消、转移和释放均校验 instance、slot、generation、token；失败不改输出、不改变当前代次预算。

## 接入顺序

1. 先实现内部操作池本身及单元测试，覆盖状态、身份、代次、取消/完成竞争、引用、预算和停止回收；此阶段不得改变任何已发布业务行为。
2. 把 Server V2 `authorize` 的执行与 owner 应用分开，由 operation ref 关联 Call generation 和 deadline。现有 dev.156–.158 的观察/admit/auth、FIFO、满执行器延期、迟到 ALLOW 不复活等外部合同必须保持。
3. 将 TLS-D2 provider operation 表改为薄类型适配层，保留现有同步 provider、异步凭证、授权、eventfd、profile generation、固定容量和 destroy 重试语义。迁移后 D2 全部既有检查仍需通过。
4. 将压缩异步完成接到同一生命周期协议，保留其消息预算、Stream owner 和公平调度；不得将压缩 CPU 任务槽与网络 byte credit 混为一项。
5. 对照 #243 与 #234/#235 把通用模型落实到证据矩阵；只有各域完成引用、终局、回调结束与资源回收屏障后，才更新对应 Issue 验收。

不得为了先证明 API 而新增永远无人使用的“通用”对象：步骤 2 与步骤 3 都必须实现并验证真实调用方，随后才可以把 TLS/压缩共享调度列为已具备。

## 错误与停机语义

| 情形 | 返回/结果 | 所有权与后续动作 |
| --- | --- | --- |
| pool 满或预算拒绝，尚未 accept | `TR_AGAIN` 或预算错误 | 无 slot 泄漏；调用方仍拥有输入和输出结构 |
| 过期、错误 instance/generation、重复消费 | `TR_ERR_STALE` / `TR_ERR_STATE` | 不影响当前槽，不修改输出，不释放新代次资源 |
| cancel 请求先于 provider 完成 | cancel reason 锁存一次，完成仍须到达 | 迟到 payload 被消费/销毁；owner 不应用业务副作用 |
| 成功结果先发布、随后 cancel | 发布结果保持不可变 | owner 按 Call/Attempt/deadline 决定是否使用；cancel 不改写已发布结果 |
| owner 正常 close 时仍有已受理操作 | `TR_AGAIN` | 不释放池、facade、provider 参数或 completion owner |
| completion queue 暂满 | 保留已接受的操作与 completion 所有权 | producer 等待有界队列准入或在准入关闭后交给已证明的 stopped cleanup |
| owner 已永久停止且生产者静默已证明 | 清理接管入口 | 只丢弃/析构结果、归还预算；不运行应用成功回调、不复活终局 |

## 验收

### 内核状态测试

1. 容量满 reserve 返回 `TR_AGAIN`，输出保持哨兵且调用方输入、结果与预算未转移；释放一个槽后重试成功。
2. generation、instance 和 operation ref 在槽复用、pool 重建、generation 耗尽、重复 publish/consume/release 下不回绕、不误释放。
3. 覆盖 publish-before-accept、accept-before-publish、provider reject、cancel-before-publish、publish-before-cancel；暂存完成只能由成功 accept 取回，拒绝和完成回调不能都获胜，每次恰好一个完成消费/销毁。
4. work 未退出、completion 未交付、owner 未消费、引用未释放中的任一条件存在时 retire/destroy 均不得释放对象；最后一个屏障结束后重试归零。
5. CompletionQueue 暂满、close/reopen generation 变化及 notify 失败时，accepted completion 不丢失且仅发布/消费一次。
6. owner stopped cleanup 必须先有旧 owner 和 producer 永久静默证明；清理不调用业务成功回调，所有预算归零。
7. GCC/Clang、ASan/UBSan、TSan 覆盖共享状态；固定格式按 CI clang-format 版本检查。测试输出、缓存与临时目录在当前 worktree `build/` 下，禁止写入 `/tmp`。

### 真实调用方

- RPC authorize：单 worker/共享队列满、取消与 deadline 先后、迟到 ALLOW、Call 槽复用、结果回调返回屏障、destroy 时在途 completion。
- TLS provider：异步请求被接受/拒绝边界、provider cancel ack、completion 先于/晚于 close、profile/connection/route generation 失配、eventfd/CompletionQueue 背压、destroy 重试。
- 压缩：输入复制、编码完成晚到/取消、Stream reset/槽复用、压缩预算和 TX 等待者公平推进；CONTROL/TLS 同伴仍进展。
- 安装 SDK 仍通过 C11/C++11 公开头文件/符号消费矩阵；本 spec 首期不增加新公开符号。

### #243 完成屏障

对每个已接入调用方分别提供证据，不能由本操作池单测替代：

1. `final selected`：Call 的网络/协议终局已唯一选择。
2. `last callback returned`：用户可观察回调已经返回，不能再访问借用对象。
3. `all async operations retired`：producer、owner completion、payload、operation refs 和 CallRef/父上下文引用均按其各自合同归还。

三者不是同一事件；只有所有相关责任均完成后，Client/Server/Dispatch/Provider 才可成功销毁。仍有引用或生产者工作时须保留所有者并返回可重试状态。

## 风险、回滚与不变量

- **主要风险：** 通用模块抽象太宽导致各 owner 语义被含糊化。缓解方式是只统一操作身份、完成所有权和退役屏障；deadline、Call 终局、TLS profile 与压缩预算仍由各领域解释。
- **内存风险：** accepted 操作或显式引用长期不退役会占满有限池。容量与预算必须可观察；释放后同池可重试，不能为缓解池满而丢操作。
- **并发风险：** cancel/publish/close 竞争和 owner 停止可能造成悬空访问。每条发布/释放路径都按 operation generation 复核，并在 TSan 和停止矩阵中验证。
- **回滚：** 可以逐使用者回退适配层，暂时恢复 TLS 专用池和 dispatch 本地关联；在所有调用方切换前，不删除 TLS/dispatch 旧状态表。公开 ABI 和 wire 首期不变，因此无需协议迁移。

## 设计自审

- 明确区分 Task 执行生命周期与 RPC/TLS/压缩 operation 生命周期。
- 说明拒绝不转移所有权、受理之后必须收敛、completion 单次发布、结果单次消费、所有引用归零后退役。
- 将取消视为合作式请求，不用取消请求替代 producer 静默证明。
- 将完成队列、Reactor owner、Call generation、Attempt generation 和领域预算保持在各自责任边界。
- 将 #243 的三个终局/回调/资源屏障分别列出；单纯 13/13 CI 或内核单测不构成整体验收。

本设计仅在 #239/#243 的异步操作子项获得实现与验收证据后才可关闭相应项目，不代表 #240–#248 或 V2 整体完成。
