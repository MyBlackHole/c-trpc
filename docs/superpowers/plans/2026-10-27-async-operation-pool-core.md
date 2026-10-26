# RPC V2 异步操作池核心实施计划

> **执行方式：** REQUIRED SUB-SKILL: 使用 `superpowers:executing-plans` 按任务内联执行。按用户要求不派发子 agent。

**目标：** 为 RPC V2 提供固定容量、可取消、可证明所有权转移的内部异步操作池，并先交付状态机和直接单元证据。

**架构：** 新池只统一操作身份、接受/发布/消费/退役、取消标记和停机清理；TaskExecutor、CompletionQueue、owner 和领域预算继续负责调度、唤醒与业务语义。每个池绑定一个 memory budget，槽位按池容量预分配；结果 payload 的领域字节仍由调用方记账。

**技术栈：** ISO C11、pthread mutex、现有 `tr_memory_budget`、Xmake。

**设计依据：** `docs/superpowers/specs/2026-10-27-rpc-v2-operation-kernel-design.md`。

## 全局约束

- 生产代码和测试只能通过 `xmake.lua` 构建；目标名及现有 CI 标识保持不变。
- 使用 C11 与项目 `-Wall -Wextra -Werror -pedantic -pthread` 编译约束。
- 文档、注释使用简体中文；内部标识符保留英文。
- 测试临时目录固定使用当前 worktree 的 `build/test-tmp`，不在 `/tmp` 写入。
- 本计划只完成通用操作池核心，不据此关闭 #243；RPC authorize、TLS provider 和 MessageService 三个真实调用方接入及其验收仍是结项条件。

---

### 任务 1：定义池、身份与固定容量准入

**文件：**
- 新建：`src/execution/async_operation_internal.h`
- 新建：`src/execution/async_operation.c`
- 修改：`xmake.lua`，把生产源加入 `trcore` 并注册 `test_async_operation`
- 新建：`tests/test_async_operation.c`

**本任务接口：**

~~~c
struct tr_async_operation_pool;
struct tr_async_operation { uint64_t instance, generation; uint32_t slot; };
struct tr_async_operation_ref {
    uint64_t instance, generation, token;
    uint32_t slot;
};
struct tr_async_operation_spec {
    uint32_t kind;
    uint64_t domain_generation, object_generation, deadline_ns;
};
struct tr_async_operation_stats {
    uint32_t capacity, reserved, accepted, published, consumed;
    uint32_t generation_exhausted;
    uint64_t reserve_full;
    int accepting;
};
int tr_async_operation_pool_create(uint32_t capacity,
        struct tr_memory_budget *budget,
        struct tr_async_operation_pool **out);
int tr_async_operation_pool_bind_owner(struct tr_async_operation_pool *pool);
int tr_async_operation_reserve(struct tr_async_operation_pool *pool,
        const struct tr_async_operation_spec *spec,
        struct tr_async_operation *out_operation,
        struct tr_async_operation_ref *out_producer_ref);
int tr_async_operation_reject(struct tr_async_operation_pool *pool,
        struct tr_async_operation operation,
        struct tr_async_operation_ref *producer_ref);
int tr_async_operation_pool_close(struct tr_async_operation_pool *pool);
int tr_async_operation_get_stats(struct tr_async_operation_pool *pool,
        struct tr_async_operation_stats *out);
int tr_async_operation_pool_destroy(struct tr_async_operation_pool **pool);
~~~

- [x] **步骤 1：先写准入和拒绝测试。** 测试容量 1：首次 reserve 返回实例/代次/槽位及强 ref；第二次 reserve 返回 `TR_AGAIN` 且两个输出哨兵不变；reject 释放槽；复用后 generation 增加，旧 handle 返回 `TR_ERR_STALE`。
- [x] **步骤 2：运行红测。** 测试首次因 `async_operation_internal.h` 不存在而编译失败，定位为预期缺失功能。
- [x] **步骤 3：实现最小池。** create 先检查容量/预算溢出并为 pool、slots 预约预算；失败回滚且不改 `*out`。reserve 在锁内扫描 FREE 槽，代次递增但不回绕；满载返回 `TR_AGAIN` 并计数；reject 仅消费匹配的 RESERVED producer ref。任何失败均不改输出或转移所有权。
- [x] **步骤 4：注册目标并重跑测试。** `test_async_operation/default` GCC release 通过；`-Werror` 初次发现 size 比较警告，修为可移植的乘法溢出检查后通过。
- [x] owner 必须显式绑定；reserve/reject/accept/consume/retire/close/stats 只允许绑定线程，publish 与 cancel 可跨线程。停机清理在静默证明后把销毁责任交给清理线程。

### 任务 2：实现 accept、publish、consume 与 retire

**文件：** `src/execution/async_operation_internal.h`、`src/execution/async_operation.c`、`tests/test_async_operation.c`。

**新增接口：**

~~~c
struct tr_async_operation_completion {
    int status;
    void *payload;
    void (*destroy)(void *payload);
};
int tr_async_operation_accept(struct tr_async_operation_pool *pool,
        struct tr_async_operation operation,
        struct tr_async_operation_ref *out_completion_ref, int *out_ready);
int tr_async_operation_publish(struct tr_async_operation_pool *pool,
        struct tr_async_operation_ref *producer_ref,
        struct tr_async_operation_completion completion,
        struct tr_async_operation_ref *out_completion_ref, int *out_ready);
int tr_async_operation_consume(struct tr_async_operation_pool *pool,
        const struct tr_async_operation_ref *completion_ref,
        struct tr_async_operation_spec *out_spec,
        struct tr_async_operation_completion *out_completion,
        int *out_cancelled, int *out_cancel_reason);
int tr_async_operation_retire(struct tr_async_operation_pool *pool,
        struct tr_async_operation_ref *completion_ref);
~~~

- [x] **步骤 1：写 accept-before-publish 测试。** reserve 后 accept 返回 `out_ready=0`；publish 转移 payload 和 destroy callback、清空 producer ref 并返回 completion ref；consume 恰好移交一次结果；retire 清空 ref 并释放容量；旧 ref 不影响复用槽。
- [x] **步骤 2：运行红测。** 将后续接口实现暂时剥离后，Xmake 测试按预期在链接阶段因 accept/publish/consume/retire 未定义而失败。
- [x] **步骤 3：写 publish-before-accept 测试。** RESERVED 上 publish 成功缓存结果且 `out_ready=0`；此时 reject 返回 `TR_ERR_STATE` 且 ref 不变；accept 返回唯一 completion ref，随后 consume/retire 成功。
- [x] **步骤 4：实现线性状态迁移。** producer→completion 每次更新非零 token；duplicate/stale token 被拒绝；consume 一并移交提交时捕获的 spec，结果由 owner 单次消费；retire 只接受 CONSUMED。
- [x] **步骤 5：运行两种顺序测试。** 两种接受/完成先后顺序、旧 token 拒绝、一次性 consume 通过 `test_async_operation/default`。

### 任务 3：实现取消竞争、关闭、停机回收与统计

**文件：** `src/execution/async_operation_internal.h`、`src/execution/async_operation.c`、`tests/test_async_operation.c`。

**新增接口：**

~~~c
int tr_async_operation_request_cancel(struct tr_async_operation_pool *pool,
        struct tr_async_operation operation, int reason, int *out_notify);
int tr_async_operation_reap_stopped(struct tr_async_operation_pool *pool,
        int owner_and_producers_permanently_stopped,
        uint32_t *out_reaped);
~~~

- [x] **步骤 1：写取消与发布并发测试。** 由两个 pthread 通过 barrier 同时执行 cancel 和 publish；两个 API 必须各自只线性化一次，consume 得到完整结果，并且取消是否获胜与锁序一致。
- [x] **步骤 2：写停机和统计测试。** close 后 reserve 返回 `TR_ERR_CLOSED` 且输出不变；未传入永久静默证明时 reap 返回 `TR_ERR_STATE`；有证明时未消费 payload 恰好析构一次，统计归零；活动操作时 destroy 返回 `TR_AGAIN`。
- [x] **步骤 3：运行红测并实现。** cancel 只在 ACCEPTED 阶段首个调用返回 `out_notify=1`，publish 先赢后 cancel 返回 `TR_ERR_STATE`。reap 要求 pool 已关闭、调用者提供 owner 与 producer 永久静默证明，并且外层排除其它 API 并发；它只做清理，不调用用户成功回调。
- [x] **步骤 4：覆盖代次耗尽。** 仅在测试可见的内部 hook 将空闲槽 generation 置为 `UINT64_MAX`；该槽永久停用，generation 不回绕，输出保持不变。
- [x] **步骤 5：运行专测及 GCC release 全套现有测试。** `xmake test -P . -v -j1` 全量 93/93 通过；所有测试临时目录都在 `build/test-tmp`。

### 任务 4：接入 Server V2 异步 authorize

**文件：** `src/rpc/rpc_record_dispatch.c`、`tests/test_rpc_record_dispatch.c`；更新对应 xmake 依赖仅在需要时进行。

- [x] **步骤 1：先验证操作池容量、统计、实例/代次耗尽与两种完成顺序。** 这些状态断言放在 `test_async_operation` 直接覆盖；RPC 真实派发测试继续覆盖满 worker、迟到 ALLOW 和取消，不改变其 TCP/TaskExecutor 设置。
- [x] **步骤 2：每个 dispatch 域创建容量与 dispatch slot 同界的 operation pool；authorize 提交前 reserve，TaskExecutor 拒绝时 reject，接受后 accept；worker 在 `tr_dispatch_v2_authorize` 返回后 publish，owner 的 `tr_dispatch_done` consume、校验 route/Call generation 与 deadline，再应用结果并 retire。** application work 路径保持现有实现。
- [x] **步骤 3：运行 RPC 派发专测与全量回归。** GCC release、Clang release、ASan/UBSan、TSan 的 93 项测试均通过；满载延期、迟到 ALLOW 和终局回收测试通过。

### 任务 5：接入 TLS-D2 provider 与 MessageService

**TLS 文件：** `src/io/tls_provider_operation.c`、`src/io/tls_provider_operation_internal.h`、`tests/test_tls_provider_operation.c`、现有 `server.c`、`client.c`、`pipeline_listener.c` 调用点。

**压缩文件：** `src/runtime/message_service.c`、`src/runtime/message_service.h`、`tests/test_message_service.c`、`tests/test_message_service_partial_destroy.c`。

- [x] **步骤 1：TLS 公共操作顺序兼容测试。** 保持公开 `tr_tls_provider_operation` 字段不变；原有拒绝/接受、cancel/complete race、credential 复制和 destroy 重试测试通过，并新增“保留态取消后可读取并退役”回归用例。
- [x] **步骤 2：以通用操作池实现 TLS 状态迁移。** TLS adapter 继续拥有 credential/result 缓冲和其领域 byte budget；通用池负责 instance/generation/ref/cancel/publish/consume/retire/close。
- [x] **步骤 3：MessageService 每个已接收 Task 关联 operation ref。** reserve 在 executor submit 前；submit 拒绝执行 reject 并保留 transform 回滚语义；成功后 accept；worker/done 通过 publish/consume/retire 交接 transform 结果；压缩输入、输出和 CPU 预算仍由 MessageService/transform 管理。
- [x] **步骤 4：运行 TLS 与 MessageService 目标。** GCC release、Clang release、ASan/UBSan、TSan 的 93 项全量测试均通过；格式调整后再运行 6 个直接相关 TSan 目标，6/6 通过。

### 任务 6：完成跨域验证与结项证据

**文件：** 新增 `docs/architecture` 下的操作池所有权契约（如实现行为需要补充）；测试/生产文件仅按前序任务修改。

- [x] **步骤 1：对照设计验收表核对操作池与调用方证据。** `test_async_operation` 覆盖容量、instance/generation、两种 publish 顺序、取消竞争、结果析构、close/reap；`test_runtime_message_recovery` 覆盖活动压缩任务停机回收，MessageService 与 TLS 专测覆盖销毁重试。
- [x] **步骤 2：运行多编译器与检查器全量测试，检查格式及公开 SDK 头。** GCC release 最终格式化后复跑 93/93；Clang release、ASan/UBSan、TSan 各 93/93 通过。格式后直接相关的 TSan 目标 6/6 通过；clang-format 18.1.8 自测 22/22、改动行无格式差异、新文件格式检查通过；`include/tr` 无差异。所有测试临时目录位于当前 worktree 的 `build/test-tmp`。
- [x] **步骤 3：把本切片的实际结果记录到 #243 与 #239。** 已分别更新 [#243 进度记录](https://github.com/MyBlackHole/c-trpc/issues/243#issuecomment-6427798919) 和 [#239 总账记录](https://github.com/MyBlackHole/c-trpc/issues/239#issuecomment-6427799570)。本计划只验收异步操作池及三个真实调用方的迁移；#243/#239 保持 OPEN，不合并 PR。

## 结项边界

本计划的实现与本地验证已完成。dev.162 提交 `d245e0b4bf0fec52e8d4e1db69613a597e35cb89` 已推送，Draft PR #332 对应的检查为 13/13 全部通过；dev.163 的 CallRef × 异步 authorize 真实 TCP 屏障回归远端检查 13/13 通过；dev.164 的 held CallRef destroy/release 回归远端检查 13/13 通过；dev.165 补 terminal callback 内 hold 路径，GCC Release 全量测试 93/93 通过，新 head 的远端检查待推送复验。#243 和 #239 仍保持 OPEN：它们的完整验收还包括跨 Reactor/嵌套调用语义、队列满载契约、更多回调与资源回收屏障及其他 V2 架构项。

## 自审

- 覆盖设计状态机、实例/代次/token、inline publish、取消线性化、结果所有权、锁外析构、固定统计、关闭/停机回收和三类真实调用者。
- 明确 generic pool 不是调度器，TLS/压缩领域预算不混入 pool，owner 仍做 domain generation/deadline 检查。
- 任何调用方迁移都必须保留其原有外部错误、回调、队列背压和 destroy retry 合同。
- 本计划实施完并通过真实调用者与检查器之前，#243 和 #239 均保持未完成。
