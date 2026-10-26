# 公共 V2 SDK 使用合同

`tr/rpc_v2.h` 定义普通 RPC 门面使用的 V2 业务契约，`tr/trpc.h` 同样包含它。
应用使用安装后的头文件及 `libtrcore.a`，不需要记录层、Buffer、Reactor 或执行器头。
`tr_client_create(config, methods, count, out)`、`tr_server_create(config, methods,
count, out)` 只构造 V2。旧两参数构造、旧注册及旧一元回调接口已从普通门面移除；
调用者必须重新编译，不提供 `_create_v2` 别名或 V1 回退。
连接组传输使用 `tr_client_create_transport()` / `tr_server_create_transport()`，
它们不允许 RPC `connect/listen`，不能借传输入口重新激活旧 RPC。

## 接入顺序

1. 准备 `tr_rpc_method_desc`。请求和响应分别选择 `TR_RPC_ONE` 或 `TR_RPC_MANY`，
   指定双方约定的非零 codec 编号与正文上限。当前 SDK 传递原始字节，不自动编解码。
2. 服务端用 `tr_rpc_v2_method` 绑定工作函数、可选终局函数和业务参数，调用
   `tr_server_create()`，再执行 `listen()`、`start()`。
3. 客户端用相同描述创建 `tr_client_create()`，执行 `connect()`、`wait_ready()`。
   两端在构造时复制冻结方法表，调用方随后可以释放或改写原数组。
4. 在调用开始前准备独立的回调参数和输出句柄存储，再调用
   `tr_client_call_start()`。只有 `TR_OK` 修改输出句柄并承担唯一终局责任。
5. 首次可写提示表示 OPEN 已被本地传输接受。按方法基数发送请求，最后半关闭。
   `TR_AGAIN` 时返回事件循环并等待后续提示，不能在回调中忙等。
6. 以终局状态判断调用结果。单独收到响应正文或尾部元数据不等于调用成功。
7. 需要优雅退出时先发布排空意图并等待静默，再销毁。销毁本身是终止拆除，
   不会偷偷增加另一个优雅等待期限。

完整的同步、错误处理和四种消息形态可直接参考
[`test_rpc_v2_sdk.c`](../tests/test_rpc_v2_sdk.c)。它同时用于仓库测试和安装目录外的
C11/C++11 消费验证，因此没有私有头或额外链接依赖。

简单的服务端回声工作函数如下。这里仅展示回调本身，实际程序还须检查创建、
监听、启动和销毁的返回值。

```c
static enum tr_rpc_status echo_work(
        struct tr_rpc_v2_call_id call,
        const struct tr_rpc_v2_request *request,
        const struct tr_rpc_v2_cancel *cancel,
        struct tr_rpc_v2_reply *reply, void *arg)
{
        (void)call;
        (void)arg;
        if (tr_rpc_v2_cancel_requested(cancel) != 0)
                return TR_RPC_STATUS_CANCELLED;
        if (request->kind == TR_RPC_V2_REQUEST_MESSAGE &&
            tr_rpc_v2_reply_append(reply, &request->body) != TR_OK)
                return TR_RPC_STATUS_RESOURCE_EXHAUSTED;
        return TR_RPC_STATUS_OK;
}
```

这个函数可服务 ONE→ONE 和 MANY→MANY 方法。同步模式下，HALF_CLOSE 未显式选择
终局时默认以 OK 完成，仍由记录状态机检查响应基数。ONE→MANY 或 MANY→ONE 的业务须在
对应工作事件中按自己的业务规则生成多条或聚合后的单条响应。

## 一元请求的自动发送

ONE→ONE 方法可以改用 `tr_client_unary_call(client, service, method,
&request, options, &callbacks, &call)`。它一次承担 OPEN、一条 MESSAGE 和
HALF_CLOSE 的发送责任，不要求应用编写发送重试回调。`request` 必须非 NULL；
`{NULL, 0}` 表示一条零字节请求，不是零消息流。其他基数组合仍使用 `call_start()`。

受理前先校验方法、元数据和正文长度，再从原 Client 有限复制池取得请求副本。
`TR_OK` 后可以立即复用正文、元数据数组及其字节。返回 AGAIN 表示调用槽或复制池
不足，此次没有受理，也不会后台排队；应用负责保留原请求并稍后重试。其他失败同样
不修改输出句柄、不交付业务回调。成功受理后的网络背压则由既有调用槽保存剩余
发送责任；MESSAGE 只在传输接管后标记完成，HALF_CLOSE 重试不会重发已接受正文。
取消、期限、断连或停机选定终局时，尚未接管的副本先归还，然后交付终局回调。

该入口复用同一 `message`、`metadata`、`terminal` 回调契约，忽略 `writable`。
返回的能力允许取消，不允许业务再发送或半关闭。响应正文仍先交付临时借用视图，
**不能在 message 回调把调用记为成功**：后续 FINAL 仍可能是错误；需要保留响应到
终局的应用应在回调内复制到自己拥有的有界存储。合法错误也可以直接返回 FINAL，
没有任何响应正文。唯一终局、回调不得重入销毁、早终局可能先于入口返回、静默
不等于终局等规则都不改变。

此便捷入口没有另一张调用表、执行器或终局状态机；它使用当前唯一的普通 V2
门面，也不承诺 OPEN/MESSAGE/HALF_CLOSE 一定合并成一个包或一次系统调用。

## 将 Server Call 的取消和期限传给子 Call

Server V2 的 `work` 回调可调用 `tr_rpc_v2_cancel_context_hold(cancel, &context)`
取得父 Call 上下文，并在回调返回后继续使用。把 `&context` 赋给子调用的
`tr_rpc_call_options.parent_context`。`call_start` 只有返回 `TR_OK` 才为子 Call
固定保留上下文；调用者可在所有并发 `call_start` 已返回后释放自己的句柄。
句柄是唯一拥有型值，不得复制成多个所有者；`release` 不得与尚未返回的
`call_start` 并发。

父取消会让尚未终结的子 Call 以 `CANCELLED` 终局；父 deadline 以
`DEADLINE_EXCEEDED` 终局。子 `timeout_ms` 与父绝对期限取较早值，零子期限仍继承
父期限，较短子期限只终止该子 Call。已取消或过期的父上下文在受理前拒绝调用，
不会写输出句柄或调用业务回调。父 Server 有未释放的上下文或子调用引用时，销毁
返回 `TR_AGAIN`；释放引用后重试销毁。

## 跨回调持有 Call 结果

`tr_rpc_v2_call` 是可复制的弱能力，不延长固定调用槽或 Client 的寿命。
需要跨过回调边界保存终局结果时，在调用仍活动或其 terminal callback 内调用
`tr_rpc_v2_call_hold(call, &ref)`。每个 Call 同时最多一个 `tr_rpc_v2_call_ref`；
它是唯一所有权值，只能由应用串行转交给另一个所有者，不能克隆或由多个线程并发
操作。失败不会修改 `ref`。

```c
static void terminal(struct tr_rpc_v2_call call, enum tr_rpc_status status,
                     enum tr_rpc_v2_delivery delivery, void *arg)
{
        struct tr_rpc_v2_call_ref ref = {0};
        struct tr_rpc_v2_call_result result;
        (void)status;
        (void)delivery;
        (void)arg;
        if (tr_rpc_v2_call_hold(call, &ref) != TR_OK)
                return;
        if (tr_rpc_v2_call_ref_result(ref, &result) == TR_OK) {
                /* 使用 result.status 和 result.delivery。 */
        }
        (void)tr_rpc_v2_call_ref_release(&ref);
}
```

最终 `status` 与 `delivery` 在 terminal callback 即将运行前发布，并在引用有效期间
保持不变。`tr_rpc_v2_call_ref_result()` 可从普通线程非阻塞读取；尚未发布时返回
`TR_AGAIN` 且不改输出。终局回调中取得的引用可立即读到快照。
`tr_rpc_v2_call_ref_cancel()` 只在终局前请求合作式取消，成功表示本地接受取消，
不表示远端业务已停止。引用没有 send 或 half-close 能力，也不改变调用期限。

引用会保留原有固定槽，直到 `tr_rpc_v2_call_ref_release(&ref)` 成功；成功后清零
传入值。长期不释放会占用 Call 容量，使新调用返回 `TR_AGAIN`，并让
`tr_client_wait_drained()` 保持未完成。若 `tr_client_destroy()` 时仍有引用，Client
会停止运行时并报告残留终局，然后返回 `TR_AGAIN`；此时引用仍可读取结果和释放，
释放后重试 destroy。应用必须在 destroy 成功前保留该唯一引用及 Client 生命周期。

## 消息、元数据与所有权

| 对象或操作 | 应用获得的保证 |
|---|---|
| Client `send()` 的正文 | 只借用到函数返回；`TR_OK` 后已复制并接受，源内存可立即复用 |
| 手动 `send()` 返回 `TR_AGAIN` | 本条正文没有被保存，消息计数不因本次失败增加；应用保留数据并重试 |
| Client 消息、元数据回调 | 所有字节只借用至回调返回，零字节消息仍交付一次 |
| Server 请求事件 | 正文及 OPEN 元数据是当前工作事件的有界副本，只借用至 work 返回 |
| `reply_append()` | 成功后已复制；任一追加失败锁存整批错误，不能忽略错误交付部分成功 |
| `reply_set_metadata()` | 成功后已复制键值；每批每阶段只能设置一次，失败锁存整批错误 |
| `reply_acquire_producer()` | 当前空 work 成功取得一份应用拥有的独立引用，之后该调用只用 producer 回复 |
| producer 的三种提交入口 | `TR_OK` 后已复制；失败不保存输入、不锁存调用错误，也不消费元数据阶段或结束发送 |
| producer 引用 | 每个并发使用者在发布前取得自己的引用；关闭后的引用可以长于 Server，最后必须 release |
| Server 方法参数 | 指针被复制，所指对象由业务拥有并保持到 Server 销毁成功 |
| Client 调用参数 | start 受理后借用到唯一 terminal；终局函数可释放它 |
| Client 调用句柄 | 可复制的弱能力，保留完整调用代次，不延长 Client 寿命 |
| Client CallRef | 每个 Call 唯一、必须 release 的拥有型引用；保留固定槽和终局快照，不提供发送能力 |
| Server 调用身份 | 可比较的不透明值，在同一进程中跨连接与 Server 不重复，不是发送能力 |

`tr_rpc_v2_metadata_count()`、`metadata_at()`、`metadata_find()` 提供按数量、索引和
键访问。键没有 NUL 终止符，值可含零字节。返回值继续借用原回调视图，不能把其中
的地址保留到回调之后。按索引越界或查找不存在的键返回 `TR_ERR_STALE`，失败不改
输出对象。请求元数据仍通过 `tr_rpc_call_options` 输入；初始和尾部响应元数据由
Client 回调读取。

### 设置响应元数据

Server 在当前 work 内通过 `tr_rpc_v2_reply_set_metadata(reply, phase, entries, count)`
复制设置响应元数据。输入复用 `tr_rpc_metadata` 数组；键为 NUL 终止字符串，
长度 1～63，只允许小写 ASCII `[a-z0-9_.-]`，同一列表不得重复键。值是二进制字节，
零长度可用 NULL；每阶段编码后的总长度最多为 `TR_RPC_METADATA_MAX_BYTES`（512）。
`TR_OK` 后可以立即复用数组、键和值，库只持有按实际编码大小预约的有界副本。

| 阶段 | 设置条件与发送时机 |
|---|---|
| INITIAL | 尚未发送初始元数据时可在当前 work 设置；始终先于本批正文发送，与 setter 和 append 的调用先后无关 |
| TRAILING | 必须属于同一批的终局；在 finish 之前设置，或由 HALF_CLOSE 的默认终局携带；只随 FINAL 发送 |

每批每阶段只能设置一次，空列表同样消耗这一次设置。尾部元数据不跨工作事件保存：
普通非终局批次设置 TRAILING 后返回 OK 会使该批以 `FAILED_PRECONDITION` 失败。
finish 之后不能再设置元数据或追加正文。有效的初始和尾部元数据可以随显式业务
错误终局发送，包括 `reply_finish(non_OK)` 和 work 返回合法非 OK 状态；本批正文丢弃。

没有既有错误时，无效键、重复键、参数或长度产生 `INVALID_ARGUMENT`；重复设置阶段、
阶段顺序错误产生 `FAILED_PRECONDITION`；资源分配失败产生 `RESOURCE_EXHAUSTED`。
这些错误与 append 失败一样锁存，已有非 OK 状态保持不变，丢弃本批全部正文和
元数据，不能忽略返回值产生截断成功。NULL 或回调外无效能力不会触及任何批次。
已经由前一批传输接受的内容不能撤销；Client 仍须用终局状态判断调用结果。

网络背压由库保存当前批次和发送进度，不重复执行 work。取消或期限先取得终局时，
仍在 work 中生成的结果会在完成交付时丢弃；setter 成功不承诺该元数据一定被发送
或被对端收到。借用的回复能力仍只能在当前 work 中使用；回调外发送使用下面的独立生产者。

## 回调外异步生产者

需要从应用线程持续生成响应时，在当前空工作批次调用
`tr_rpc_v2_reply_acquire_producer(reply, &producer)`。OPEN、MESSAGE、HALF_CLOSE 均可取得，
但同一调用只能成功一次；当前批次必须尚无正文、元数据、finish 或已锁存错误。
只有 `TR_OK` 修改输出并授予一个应用引用，失败不会改变当前批次。
成功后不可恢复同步回复：当前及后续 work 的 `reply_append()`、`reply_set_metadata()`、
`reply_finish()` 返回 `TR_ERR_STATE`，并锁存当前工作批次的 `FAILED_PRECONDITION`。

下例在 HALF_CLOSE 将所有权交给应用自己的有界工作队列。`submit_owned_producer()` 是
应用提供的函数：成功接管这一份引用，失败不保存它。队列线程只能保留 producer 及
应用自己拥有的数据；此前收到的请求字节、元数据视图和取消令牌仍须在借用期内处理或复制。

```c
static enum tr_rpc_status defer_response(
        struct tr_rpc_v2_call_id call,
        const struct tr_rpc_v2_request *request,
        const struct tr_rpc_v2_cancel *cancel,
        struct tr_rpc_v2_reply *reply, void *arg)
{
        struct tr_rpc_v2_producer *producer;
        int ret;
        if (tr_rpc_v2_cancel_requested(cancel) != 0)
                return TR_RPC_STATUS_CANCELLED;
        if (request->kind != TR_RPC_V2_REQUEST_HALF_CLOSE)
                return TR_RPC_STATUS_OK;
        ret = tr_rpc_v2_reply_acquire_producer(reply, &producer);
        if (ret != TR_OK)
                return TR_RPC_STATUS_RESOURCE_EXHAUSTED;
        ret = submit_owned_producer(arg, call, producer);
        if (ret != TR_OK) {
                tr_rpc_v2_producer_release(producer);
                return TR_RPC_STATUS_RESOURCE_EXHAUSTED;
        }
        return TR_RPC_STATUS_OK;
}
```

取得生产者后，HALF_CLOSE 不再自动完成响应。应用必须成功调用 `producer_finish()`，
或归还最后一个尚未 finish 的应用引用以请求取消。后续请求事件仍按 FIFO 执行；
异步输出的背压不会阻塞后续请求工作，应用负责这些工作与生产线程之间的数据同步。
work 返回非 OK 仍可以选择错误终局，因此切换之后也不能忽略请求处理错误。

### 单槽提交与有限等待

三个提交入口共享一个有界记录槽，分别保存 INITIAL、一条 MESSAGE 或 FINAL。
正在复制的输入和因传输背压尚未接受的记录也占用这个槽。正文、初始元数据和尾部
元数据都在 `TR_OK` 返回前复制；源数组、键和值随后可以立即复用。

| 入口 | 接受条件与结果 |
|---|---|
| `producer_send_initial_metadata(p, entries, count)` | 初始阶段尚未关闭；空列表也显式发送一次 INITIAL；键值限制与同步 setter 相同 |
| `producer_send(p, body)` | 复制一条不超过方法响应上限的正文；零字节仍是一条消息；必要时先生成空 INITIAL |
| `producer_finish(p, status, trailing, count)` | 原子复制尾部并接受一个 FINAL；成功后发送入口封闭；非 OK 不撤销前面已接受的记录 |

第一条 INITIAL 或 MESSAGE 受理就关闭初始元数据阶段。之前同步批次已经发送过
INITIAL 时也不能再设置；槽忙时先返回 `TR_AGAIN`，槽空后重复阶段返回
`TR_ERR_STATE`。尾部只属于当前这次 finish，
不存在跨调用暂存尾部再结束的隐含状态。元数据值可以包含零字节，空值可用 NULL；
每阶段的键名、重复项及 512 字节编码上限与同步接口一致。

槽忙返回 `TR_AGAIN`，本次输入没有被保留，元数据阶段和结束状态都不改变。应用保留
待发送内容，调用 `tr_rpc_v2_producer_wait_writable(p, timeout_ms)` 等待后再重试。
等待采用单次有限的 `CLOCK_MONOTONIC` 总期限；0 毫秒只探测，不可写时返回
`TR_ERR_TIMEOUT`。应用应为整项业务另设总期限，避免每次重试重新获得无限等待时间。
成功提示不预约槽或字节预算；其他持有引用的发送者可能让下一次提交再次返回 AGAIN。
分配或共享字节预算不足返回 `TR_ERR_NOMEM`，资源恢复由应用自己的策略决定，
可写等待不承诺共享预算已经恢复。参数、长度或重复键错误可修正后重试，不会锁存
整个调用的错误。每次返回都必须检查，不能把一次失败当成消息已发送。

应用可以在创建 producer 的 work 返回前发布引用，并预先复制一条记录。库在
work 返回和 done 完成交接后才真正发送，`wait_writable()` 的首次成功也只能发生在
这次交接之后。创建 work 不得等待依赖生产者被消费、激活或关闭的外部线程；
否则工作与生产线程会相互等待。等待接口只供普通外部线程使用，任何 c-trpc
I/O 回调、业务 work 或终局回调中调用都会返回 `TR_ERR_STATE`。

finish 成功只是接受终局记录：此时提交和等待返回 `TR_ERR_STATE`，
`producer_is_closed()` 仍可能返回 0。调用真正关闭后，提交及等待返回
`TR_ERR_CLOSED`，查询返回 1；关闭会唤醒等待者。`is_closed()` 既不证明业务终局
已经交付，也不证明对端已经收到 FINAL。取消发生在创建 work 交接之前时，仍由
原 work 的借用取消令牌及时合作观察；producer 不承诺在这段时间立即报告关闭，
已经复制的暂存结果可以在交接时丢弃。

### 应用线程与引用回收

每个并发使用者须在引用发布之前调用 `producer_retain()` 取得自己的引用。
不能在另一个线程正在释放同一份引用时，才尝试借用指针或恢复其所有权。
每份引用最后归还一次；`producer_release(NULL)` 是空操作。最后一个应用引用在
finish 尚未受理时归还，会请求 `CANCELLED`；finish 已受理时归还不会中断这次回复。
取消、期限、工作错误和网络终局仍按先到者选择最终结果。

Server 业务 terminal 等待库提交的 work/done 收敛以及生产者解除运行时依赖，
不等待应用自己的线程或引用归零。方法参数若包含生产线程仍在使用的内存，不能
仅凭 terminal 就释放；应用须自行通知退出、join 线程，再释放相应上下文。
已经持有的 producer 引用允许长于 Server；成功销毁 Server 后仍可安全查询关闭、
收到提交拒绝并 release，不能在最后 release 后继续查询指针。禁止用
`pthread_cancel` 中断正在调用库接口的生产线程。

活跃生产者尚未 finish 或取消时，调用仍计入 inflight 并阻止正常排空；调用关闭后
保留的应用引用不再阻止业务 terminal、统计完成、排空或 Server 销毁。
Server 存活期间，这些尚未 release 的关闭句柄仍占分片内跨对端共享的有限句柄配额，
不是可无限积累的历史记录。完整的 pthread 交接、复制、背压、取消、期限及销毁后
回收样例见公共消费端中的 `test_async_producer()`；它使用安装后的同一公共接口。

## 稳定调用统计

`tr_client_get_rpc_semantic_stats()`、`tr_server_get_rpc_semantic_stats()` 同样支持 V2，
读取开始、结束、进行中调用数及最终状态分布。Client 自身 LIVE 回调内可读本地
快照；Server 聚合快照必须从外部控制线程读取，所有 c-trpc 回调中返回 `TR_ERR_STATE`。
失败保留输出，累计溢出返回 `TR_ERR_BAD_LENGTH`，不影响正常调用。

Client 在调用受理时记开始，在 terminal 调用前记结束；Server 在完整合法 OPEN
准入后记开始，等待工作、完成交付及库内生产者解绑收敛后记结束；retain/release
本身不增加或重复完成调用。Client 取消已结束而 Server 工作
仍未退出时，两端 `calls_inflight` 可以不同。断连和对端回收不清空 Server 累计值。
计数不替代回调返回、发送完成或排空屏障；详细口径见[运行时可观测性](observability.md)。

## 回调与终局

Client 的正常回调位于其 I/O 执行域，必须短小、不阻塞。回调内可发送、半关闭、
取消和发布客户端排空意图；递归 start、阻塞等待及销毁被拒绝。回调可能在外部
start 返回前发生，不能等返回后才初始化参数或把输出句柄放进可能被早终局释放的对象。

服务端同一调用的 OPEN、MESSAGE、HALF_CLOSE 顺序执行。同步模式下，前一事件产生的
回复尚未被传输接受时，不执行下一工作事件；异步模式的输出背压不阻塞下一请求
事件。网络背压不会使业务函数重复执行。不同调用
可以同时进入同一个方法参数，业务负责共享数据的同步。
当前工作及终局回调也不能同步调用 Client V2 的 start、发送、半关闭或取消，
跨 I/O 执行域的同步入口会返回 `TR_ERR_STATE`；阻塞生命周期操作同样被拒绝。

工作回调中的取消令牌和回复能力不能保留、交给其他线程或在回调外使用。取消是
合作式，非合作业务不会被强制抢占。完整合法 OPEN 匹配方法并通过期限准入后，
服务端承担一次业务终局责任，即使后续工作对象分配失败、OPEN 工作尚未开始也不
丢失通知。未知方法、无效或不完整 OPEN 不进入业务方法。

业务终局必须等待已提交工作退出和完成交付，网络 RESET 已接受不构成提前释放
业务状态的许可。异步 FINAL 可在后续 work 仍执行时受理并请求取消；已选终局不被
迟到结果改写，但 terminal 仍等待该 work 返回。应用自己的生产线程由应用单独回收。
正常终局使用 `TR_RPC_V2_DELIVERY_LIVE`；父执行域停止后，由
外部销毁线程交付的残留终局使用 `TR_RPC_V2_DELIVERY_SHUTDOWN`。两者都禁止递归
生命周期操作。终局状态不证明对端已经收到响应，单个 Client 终局也不证明所有
网络槽和 TX 已回收；完整静默由门面排空或成功销毁保证。
销毁时仍可能先在尚未退出的 I/O 域关闭连接并交付 LIVE 终局；SHUTDOWN 只描述
停止后仍未报告的残留交付，不能将所有 destroy 结果都断言为 CANCELLED/SHUTDOWN。

首次 destroy 尝试开始前，应用必须停止并排除其他 Client/Server API 和弱能力的并发使用。
销毁失败时对象所有权仍在调用方，但只能继续销毁，不能复活旧调用能力。构造失败
若返回非空对象同样必须持有并重试销毁，不能把错误返回视作无需清理。
已经拥有独立引用的 producer 按上述关闭合同继续安全收敛；它不授予重新访问
正在销毁或已经释放的 Client/Server 对象的能力。

## TLS 凭证异步获取与连接组授权

静态 TLS 配置仍是默认路径；provider 不配置时，既有 Client/Server 构造和连接行为不变。
`TR_TLS_REQUIRED` 始终失败关闭，任何凭证、期限、授权或容量错误都不会回退到明文。
异步 provider 只提供本地证书链、私钥及可选口令；Client 的服务端信任根和名称验证、
Server 的客户端 CA 与认证策略仍来自固定 TLS 配置，provider 不能替换这些信任策略。

`tr_client_tls_credentials_start()` / `tr_server_tls_credentials_start()` 显式启动一次取证或刷新。
`fetch` 收到带单调时钟绝对期限的请求并快速返回；provider 在自己的 I/O 域完成读取后，
调用对应的 `*_credentials_complete()`。`complete()` 在返回前复制 bundle；随后由外部控制线程
调用 `*_credentials_poll()` 执行 PEM 校验、构造不可变 profile 并走 TLS-D1 发布屏障。
poll 返回 `TR_AGAIN` 时结果对象不变；poll 消费终态后值句柄过期。`*_credentials_cancel()` 只发起合作式
取消，provider 停止访问原始数据后仍须 complete 一次以确认。不能在 Reactor、RPC work 或其他
c-trpc 回调里阻塞等待 provider、轮换或生命周期操作。

bundle 的证书链、私钥和口令按字节计入单次总上限，默认 4 MiB；可由
`tr_facade_limits.max_tls_credential_bundle_bytes` 配置。`max_pending_tls_operations` 默认 64；
同一个 Server 上的凭证操作和连接组授权共用该有界槽位数，Client 则仅用于自己的凭证操作。
取消确认前槽位、复制结果及所需对象引用不会回收。未确认的 provider 操作会使 destroy 返回
`TR_AGAIN`；对象仍归调用方，只能继续等待确认并重试 destroy。

连接组 `authorize_async` 用于远端 CONTROL/DATA 身份策略。可同时设置同步 `authorize` 作为
本地快速 veto；通过后异步回调才会启动。授权请求和身份视图只借用到 provider 回调返回，
需要异步使用时先复制。provider 可从任意线程调用
`tr_server_connection_group_authorize_complete()` 一次提交 ALLOW、DENY 或负状态；只有 Server
所属 Reactor owner 会重验连接代次、期限、路由预留、TLS generation 和身份，再提交 CONTROL
会话或 DATA 路由。挂起期间连接不会进入业务层。收到 cancel 后 provider 必须停止读取原始请求，
再以 complete 确认；迟到 ALLOW 会丢弃。连接关闭、期限到达、旧 TLS generation 撤销或 Server
drain 都会请求取消；未确认操作会保留有界资源并要求调用方重试相应生命周期操作。

## 有界资源与大小配置

V2 构造中的 `limits.max_message_bytes` 指业务正文，`initial_window_bytes` 指正文
信用窗口，方法的最大请求和响应须同时不超过这两个值。库内部补入记录封装和
控制元数据空间，并约束信用更新阈值；应用无需知道固定头长度。帧载荷上限仍由
`max_frame_payload_bytes` 控制，整条消息可以跨帧。公共上限溢出或不能满足方法
合同会使创建明确失败，不采用截断或回绕。

双方的初始接收窗口和方法上限可以不同。当前协议按整条记录准入，不能适配
对端初始窗口或协商消息上限的发送返回 `TR_ERR_BAD_LENGTH`，不会持续返回
`TR_AGAIN` 等待不可能恢复的信用。客户端手动发送失败不接管正文；服务端
已经复制受理的同步回复或 producer 消息若在发送时发现该限制，会以
`INVALID_ARGUMENT` 结束调用并释放待发送责任，同连接其他调用继续工作。
后续累计信用增长不扩大单条消息上限。底层保留接收 payload 时，实际释放后
才归还网络信用；普通 V2 在有界副本接管后可归还网络信用，副本的内存预算
直到实际释放才归还。应用仍须以唯一终局判断调用结果。

每连接的并发调用上限为 `min(max_calls, max_streams)`。Client 复制发送使用有界
动态池，池满时保留精确调用的可写等待责任，容量恢复后由既有进度机制通知。
调用退役不会提前销毁池；已经接受的传输数据可以比调用活得更久。

Server 当前每调用最多保留 16 个未收敛输入事件，包含排队、运行及待发回复的事件；
同步模式一个工作事件最多追加 16 条响应，正文总额受最大方法响应上限的 16 倍约束。
异步生产者每次保留一条 INITIAL/MESSAGE/FINAL，复制中的记录同样计入单槽限制；
句柄和字节存储分别受共享有限配额约束，不能通过增加对端无限扩大保留量。
所有调用还共同受有限的实例内存和任务容量约束，过载可选择
`RESOURCE_EXHAUSTED`。这些限额不承诺每个调用都能同时占满全部额度。
运行时布局、内部计费字节、线程数和任务槽不会成为公共结构成员。

内部预算通过已规范化语义上限和分片数派生，并在分配前预约；这是一项内部计费
硬上限，不是 RSS 或线程栈上限。配额推导和溢出检查集中在
[`rpc_v2.c`](../src/rpc/rpc_v2.c)，不会按对端数、调用数和每调用最大正文简单相乘
形成无界存储承诺。具体内部实现可演进，公开的失败、复制和静默合同保持明确。

## 当前边界

本接口覆盖 ONE/MANY 四种组合、请求与响应元数据、有界同步回复、回调外异步生产者、
可写通知与有限等待、取消、期限、分层排空及调用语义统计。它尚未提供 NONE 基数、
自动重连、或网络压缩。transport-only 连接组通过 `tr_client_create_transport()` /
`tr_server_create_transport()` 提供独立于 RPC 的字节传输能力。普通 V2 Client/Server
可通过配置中的 `tls.mode =
TR_TLS_REQUIRED` 启用 TLS 1.3；Client 必须配置 CA 信任来源或使用系统信任库，并设置
`expected_peer_name`。证书身份不匹配和 TLS 握手失败会关闭连接，不回退到明文。
Client 可同时提供成对的客户端证书与私钥。Server 可设置
`client_auth = TR_TLS_CLIENT_AUTH_OPTIONAL` 或 `TR_TLS_CLIENT_AUTH_REQUIRED`，并提供
`client_ca_file`；`NONE` 不请求客户端证书。OPTIONAL 允许无证书连接，但若对端提供证书，
仍须通过证书链、clientAuth 用途和唯一 URI SAN 校验。REQUIRED 还会拒绝无客户端证书的连接。
证书 URI SAN 必须是唯一、1–512 字节的 IA5 值。

Server 每个 OPEN、MESSAGE 和 HALF_CLOSE 的 work 回调都收到连接级
`request.peer_identity`。已验证证书返回 `TR_PEER_IDENTITY_TLS_CERTIFICATE` 和 URI SAN；
明文连接及没有客户端证书的 OPTIONAL TLS 连接返回 `TR_PEER_IDENTITY_NONE` 与空 URI。
身份视图只借用至当前 work 回调返回，来源不受 RPC metadata 影响。

默认 `TR_TLS_DISABLED` 仍使用明文。transport-only Client 启用 TLS 时必须配置成对的
客户端证书/私钥、CA 信任和 `expected_peer_name`；Server 连接组必须启用 TLS，并将
`client_auth` 设为 `TR_TLS_CLIENT_AUTH_REQUIRED`。CONTROL 与每条 DATA 都独立完成 mTLS，
TLS 握手成功后才发送/解析 TRR1。TCP 连接、TLS 握手和 TRR1 前导共用
`connect_timeout_ms` 的单调绝对期限。Client 的每条 DATA 复用 CONTROL 的 TLS profile，
并要求服务端唯一 URI SAN 与 CONTROL 主体逐字节相同；主体不匹配时该 DATA 被关闭并
撤销对应 offer，不发布 `TRANSFER_READY`，其余 CONTROL 会话仍可工作。服务端对每条
CONTROL/DATA 路由都运行授权回调，应用可进一步限制客户端主体。配置不支持的其他功能
会明确返回 `TR_ERR_UNSUPPORTED`，不会悄悄运行另一种协议。

已启用 `TR_TLS_REQUIRED` 的 Client 和 Server 可分别调用
`tr_client_tls_update_profile()`、`tr_server_tls_update_profile()` 更新 TLS profile。
调用方须在外部控制线程串行化实例生命周期操作；成功时 generation 单调递增，失败时
不修改 `out_generation`。`TR_TLS_ROTATE_GRACEFUL` 让已接管连接继续持有旧快照，新连接
使用新快照；连接组需关闭并重连 CONTROL 才切换到新代次，Server 会拒绝旧 CONTROL 上的
新代次 DATA。`TR_TLS_ROTATE_REVOKE_OLD` 立即关闭旧代次连接，在途 RPC 可能以连接错误结束。
TLS 更新不能把明文实例切换为 TLS，也不能关闭 TLS 降级为明文。

完整阶段范围、限制和测试记录见 [TLS-C 实现记录](reviews/rpc_v2_tls_c_20261023.md)、
[TLS-D1 验证记录](reviews/rpc_v2_tls_d1_20261023.md)和
[TLS-D1 设计与实施计划](superpowers/specs/2026-10-23-tls-d1-mtls-group-identity-design.md)。

普通 `tr_server_create()` 只构造 V2 RPC Server；需要连接组或纯传输能力时使用
`tr_server_create_transport()`。transport-only 保留 Runtime、Reactor、Channel
和 ConnectionGroup listener，不创建 RPC 方法表、Endpoint、任务执行器或重组池；
`tr_server_listen()` 对该模式返回 `TR_ERR_UNSUPPORTED`。

验证入口是 Xmake：

```sh
xmake f -c -y -m debug --toolchain=gcc
xmake test -v -j1 'test_rpc_v2_sdk/*'
xmake test -v -j1 'test_rpc_v2_server/*'
xmake test -v -j1 'test_client_record/*'
```

这三项有不同职责：纯公共业务消费、服务端终局边界、客户端复制与代次边界。
发布前仍须通过原五种完整功能模式、双生产静态分析、修改行格式门禁和安装检查，
不能用局部通过替代整个源码树的验收。
