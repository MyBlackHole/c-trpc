# 公共 V2 SDK 使用合同

`tr/rpc_v2.h` 是显式 V2 业务入口，`tr/trpc.h` 同样包含它。
应用使用安装后的头文件及 `libtrcore.a`，不需要记录层、Buffer、Reactor 或执行器头。
默认的 `tr_client_create()`、`tr_server_create()` 仍选择 V1；V2 构造和调用不自动回退。

## 接入顺序

1. 准备 `tr_rpc_method_desc`。请求和响应分别选择 `TR_RPC_ONE` 或 `TR_RPC_MANY`，
   指定双方约定的非零 codec 编号与正文上限。当前 SDK 传递原始字节，不自动编解码。
2. 服务端用 `tr_rpc_v2_method` 绑定工作函数、可选终局函数和业务参数，调用
   `tr_server_create_v2()`，再执行 `listen()`、`start()`。
3. 客户端用相同描述创建 `tr_client_create_v2()`，执行 `connect()`、`wait_ready()`。
   两端在构造时复制冻结方法表，调用方随后可以释放或改写原数组。
4. 在调用开始前准备独立的回调参数和输出句柄存储，再调用
   `tr_client_v2_call_start()`。只有 `TR_OK` 修改输出句柄并承担唯一终局责任。
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

这个函数可服务 ONE→ONE 和 MANY→MANY 方法。HALF_CLOSE 未显式选择终局时默认
以 OK 完成，仍由记录状态机检查响应基数。ONE→MANY 或 MANY→ONE 的业务须在
对应工作事件中按自己的业务规则生成多条或聚合后的单条响应。

## 消息、元数据与所有权

| 对象或操作 | 应用获得的保证 |
|---|---|
| Client `send()` 的正文 | 只借用到函数返回；`TR_OK` 后已复制并接受，源内存可立即复用 |
| `TR_AGAIN` | 本条正文没有被保存，消息计数不因本次失败增加；应用保留数据并重试 |
| Client 消息、元数据回调 | 所有字节只借用至回调返回，零字节消息仍交付一次 |
| Server 请求事件 | 正文及 OPEN 元数据是当前工作事件的有界副本，只借用至 work 返回 |
| `reply_append()` | 成功后已复制；任一追加失败锁存整批错误，不能忽略错误交付部分成功 |
| Server 方法参数 | 指针被复制，所指对象由业务拥有并保持到 Server 销毁成功 |
| Client 调用参数 | start 受理后借用到唯一 terminal；终局函数可释放它 |
| Client 调用句柄 | 可复制的弱能力，保留完整调用代次，不延长 Client 寿命 |
| Server 调用身份 | 可比较的不透明值，在同一进程中跨连接与 Server 不重复，不是发送能力 |

`tr_rpc_v2_metadata_count()`、`metadata_at()`、`metadata_find()` 提供按数量、索引和
键访问。键没有 NUL 终止符，值可含零字节。返回值继续借用原回调视图，不能把其中
的地址保留到回调之后。按索引越界或查找不存在的键返回 `TR_ERR_STALE`，失败不改
输出对象。请求元数据仍通过 `tr_rpc_call_options` 输入；初始和尾部响应元数据由
Client 回调读取，当前 Server SDK 尚无设置响应元数据的业务接口。

## 回调与终局

Client 的正常回调位于其 I/O 执行域，必须短小、不阻塞。回调内可发送、半关闭、
取消和发布客户端排空意图；递归 start、阻塞等待及销毁被拒绝。回调可能在外部
start 返回前发生，不能等返回后才初始化参数或把输出句柄放进可能被早终局释放的对象。

服务端同一调用的 OPEN、MESSAGE、HALF_CLOSE 顺序执行。前一事件产生的回复尚未
被传输接受时，不执行下一工作事件；网络背压不会使业务函数重复执行。不同调用
可以同时进入同一个方法参数，业务负责共享数据的同步。
当前工作及终局回调也不能同步调用 Client V2 的 start、发送、半关闭或取消，
跨 I/O 执行域的同步入口会返回 `TR_ERR_STATE`；阻塞生命周期操作同样被拒绝。

工作回调中的取消令牌和回复能力不能保留、交给其他线程或在回调外使用。取消是
合作式，非合作业务不会被强制抢占。完整合法 OPEN 匹配方法并通过期限准入后，
服务端承担一次业务终局责任，即使后续工作对象分配失败、OPEN 工作尚未开始也不
丢失通知。未知方法、无效或不完整 OPEN 不进入业务方法。

业务终局必须等待已提交工作退出和完成交付，网络 RESET 已接受不构成提前释放
业务状态的许可。正常终局使用 `TR_RPC_V2_DELIVERY_LIVE`；父执行域停止后，由
外部销毁线程交付的残留终局使用 `TR_RPC_V2_DELIVERY_SHUTDOWN`。两者都禁止递归
生命周期操作。终局状态不证明对端已经收到响应，单个 Client 终局也不证明所有
网络槽和 TX 已回收；完整静默由门面排空或成功销毁保证。
销毁时仍可能先在尚未退出的 I/O 域关闭连接并交付 LIVE 终局；SHUTDOWN 只描述
停止后仍未报告的残留交付，不能将所有 destroy 结果都断言为 CANCELLED/SHUTDOWN。

首次 destroy 尝试开始前，应用必须停止并排除其他 API 和弱能力的并发使用。
销毁失败时对象所有权仍在调用方，但只能继续销毁，不能复活旧调用能力。构造失败
若返回非空对象同样必须持有并重试销毁，不能把错误返回视作无需清理。

## 有界资源与大小配置

V2 构造中的 `limits.max_message_bytes` 指业务正文，`initial_window_bytes` 指正文
信用窗口，方法的最大请求和响应须同时不超过这两个值。库内部补入记录封装和
控制元数据空间，并约束信用更新阈值；应用无需知道固定头长度。帧载荷上限仍由
`max_frame_payload_bytes` 控制，整条消息可以跨帧。公共上限溢出或不能满足方法
合同会使创建明确失败，不采用截断或回绕。

每连接的并发调用上限为 `min(max_calls, max_streams)`。Client 复制发送使用有界
动态池，池满时保留精确调用的可写等待责任，容量恢复后由既有进度机制通知。
调用退役不会提前销毁池；已经接受的传输数据可以比调用活得更久。

Server 当前每调用最多保留 16 个未收敛输入事件，包含排队、运行及待发回复的事件；
一个工作事件最多追加 16 条响应，正文总额受最大方法响应上限的 16 倍约束。
所有调用还共同受有限的实例内存和任务容量约束，过载可选择
`RESOURCE_EXHAUSTED`。这些限额不承诺每个调用都能同时占满全部额度。
运行时布局、内部计费字节、线程数和任务槽不会成为公共结构成员。

内部预算通过已规范化语义上限和分片数派生，并在分配前预约；这是一项内部计费
硬上限，不是 RSS 或线程栈上限。配额推导和溢出检查集中在
[`rpc_v2.c`](../src/rpc/rpc_v2.c)，不会按对端数、调用数和每调用最大正文简单相乘
形成无界存储承诺。具体内部实现可演进，公开的失败、复制和静默合同保持明确。

## 当前边界

本接口覆盖 ONE/MANY 四种组合、请求元数据、响应元数据读取、有界回复、可写通知、
取消、期限及分层排空。它尚未提供 NONE 基数、回调外长期异步生产者、响应元数据
写入、V2 语义统计、自动重连、连接组、网络压缩或 TLS。配置不支持的已有功能会
明确返回 `TR_ERR_UNSUPPORTED`，不会悄悄运行另一种协议。

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
