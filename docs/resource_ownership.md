# 资源所有权规范

c-trpc 以 ISO C11 为语言基线。作用域自动清理使用 GCC/Clang 的 `cleanup` 属性，
但所有项目代码仍按 C11 语义组织，不启用 GNU 语句表达式、嵌套函数等额外语言特性。

本项目的资源模型以**显式所有权**为核心，而不是依赖隐式生命周期。

## 1. 资源状态

每个资源在任意时刻必须明确属于以下状态之一：

- **当前拥有**：当前存在唯一所有者，由它负责最终释放；
- **借用**：临时使用，不获得释放责任；
- **共享强引用**：通过引用计数显式持有异步共享生命周期；
- **已经转移**：所有权从一个所有者明确转移到另一个所有者。

`tr_conn_handle`、`tr_stream_handle` 等句柄是能力句柄，**不是所有权**。

`tr_rpc_v2_call` 是弱能力值：复制它不会延长 Client 生命周期。调用方必须保证
Client 尚未进入销毁流程，并按 V2 API 合同处理过期能力；`tr_rpc_v2_call_id` 只是
业务身份，不具备发送或延长生命周期的能力。旧 `tr_rpc_call_handle` 已从 SDK 移除。

## 2. 仅成功时转移所有权

除非 API 文档明确说明，否则只有返回 `TR_OK` 时才发生所有权转移。

```c
struct tr_buffer *buffer TR_AUTO(tr_buffer_cleanup) = NULL;

ret = tr_buffer_acquire(pool, size, &buffer);
if (ret != TR_OK)
    return ret;

ret = tr_reactor_send(connection, type, flags, stream_id, message_id, buffer);
if (ret != TR_OK)
    return ret;

(void)tr_buffer_take(&buffer);
return TR_OK;
```

发送失败时 Buffer 仍属于当前作用域，离开作用域会自动清理。
发送成功后 Reactor 接管 Buffer，`tr_buffer_take()` 清空本地自动清理变量，
避免重复释放。

## 3. 作用域自动清理

具有词法作用域所有权的局部资源，原则上优先使用 `TR_AUTO(cleanup_fn)`。

当前主要类型化所有权辅助接口：

- `tr_fd_cleanup()` / `tr_fd_take()`；
- `tr_buffer_cleanup()` / `tr_buffer_take()`。

新增资源类型时应提供类型化清理/接管辅助接口，不要依赖通用 `void *` 强制转换隐藏类型。

清理函数必须满足：

1. 能处理已经解除或为空的状态；
2. 一次只释放一个明确由当前变量拥有的资源；
3. 可行时把变量恢复为空状态；
4. 清理函数自身不能偷偷取得新的所有权；
5. 清理过程必须是语义上不可失败的，不能隐藏需要返回状态的生命周期屏障；
6. `detach`、`unregister`、`join`、终局 `destroy` 等失败后必须保留所有权
   以便重试的操作，禁止放进 `void cleanup`。

## 4. 清理顺序

自动清理变量按声明顺序的**逆序**执行。

如果子对象依赖父对象，应先声明父对象，再声明子对象：

```c
struct parent *parent TR_AUTO(parent_cleanup) = NULL;
struct child *child TR_AUTO(child_cleanup) = NULL;
```

离开作用域时先清理子对象，再清理父对象。

因此重排自动清理变量声明时，必须同时检查生命周期依赖关系。

## 5. 构造与事务保护

自动清理分为三种主要模式。

### 5.1 简单词法作用域所有者

适用于单一独立资源，例如 fd、Buffer、堆分配。

### 5.2 复杂构造保护

如果对象只有完成多个初始化步骤后才能调用完整析构函数，则使用构造保护对象记录哪些步骤已经成功。

典型对象包括 Reactor、Channel、V2 Record/dispatch 对象和 TaskExecutor。

构造保护主要解决两个问题：

- 对部分初始化对象错误调用完整析构函数；
- 新增初始化步骤后忘记修改某个 `goto fail_*` 路径。

构造保护只能自动清理由当前词法作用域独占、且释放不会失败的本地资源。
一旦对象地址已经发布给 Reactor 处理器、定时器、工作线程或其他异步事件源，
后续失败必须先通过显式、可返回状态的生命周期事务解除发布状态，
确认事件源已静默后，才能回到构造保护释放存储。

推荐顺序：

```text
本地资源初始化
    ->
完成所有可失败预检
    ->
单所有者 / 单事务发布回调源
    ->
后续失败在同一事务内收敛发布状态
    ->
确认不可再触达对象
    ->
词法作用域清理释放本地存储
```

如果可以通过“先预检、后提交”或调整发布顺序消除回滚，
应优先消除回滚，而不是给 `void cleanup` 增加更多补偿逻辑。

对于门面构造函数，如果对象已经跨过可失败的终局生命周期边界，
普通错误仍应尽量回滚到 `*out == NULL`。只有回滚屏障自身失败时，
允许“错误返回 + 非空 `*out`”作为显式交还所有权的出口；调用方此时不得继续
业务操作，只能持有该部分构造对象的所有权并重复调用对应 `destroy()` 直到收敛。
这比在 `void cleanup` 中吞掉错误或制造无主泄漏更安全。

成功构造完成后，必须显式解除构造保护，把完整对象交给新的所有者。

### 5.3 事务回滚保护

对于在已有所有者对象上执行多步状态修改的操作，只有当回滚本身不可失败时，
才允许使用作用域自动回滚保护。

典型的安全场景是纯内存状态恢复、释放未发布 Buffer、关闭仍由当前作用域独占的 fd。

一旦回滚包含回调解除关联、定时器注销、等待线程退出、所有者屏障或
终局销毁，就必须改成显式返回状态的事务函数。失败时保留原所有权 /
发布状态以便重试，不能因为离开 C 作用域而强行继续释放对象。

成功后必须显式解除回滚保护。

## 6. 显式所有权转移

所有权转移必须能够从代码中直接看出来。

推荐：

```c
dst = tr_buffer_take(&src);
```

不推荐：

```c
dst = src;
src = NULL;   /* 难以判断这是普通赋值还是所有权转移 */
```

因此裸 `ptr = NULL` 不应作为正常的所有权移动语法。

## 7. 异步生命周期

作用域清理只能解决当前词法作用域，不能解决跨线程、队列、回调的异步生命周期。

一个指针跨异步边界前，必须满足以下机制之一：

- 单所有者保证；
- 显式静默屏障；
- 强引用计数。

禁止先把指针发布给其他线程，再补 `get()`。

<a id="71-buffer-pool-终局所有权"></a>

### 7.1 Buffer 资源池终局所有权

Buffer 描述符可以通过 Reactor/RPC/应用回调跨异步边界。资源池销毁
不能假设调用时所有描述符已自动归还。

终局顺序：

```text
父对象停止新准入
      ->
pool.closed = 1
      ->
检查每个 descriptor.checked_out
      |
      +-- 仍有持有者：TR_ERR_STATE，父对象保持存活
      |
      +-- 全部归还：释放资源池存储
```

失败后的资源池继续允许已有持有者调用 `tr_buffer_release()`，但拒绝新的
`tr_buffer_acquire()`。持有者归还后由父对象重试销毁。

Reactor RX 载荷可以通过 `TR_FRAME_TAKE_OWNERSHIP` 保留到回调之外，因此
该终局屏障必须向上传播：

```text
保留的 RX 载荷
    -> Reactor rx_pool
    -> RuntimeShard
    -> Runtime
    -> Client/Server
```

任何一层看到下层销毁失败，都不能继续释放自己的存储。

## 8. 强引用计数

`struct tr_refcount` 是项目统一的 C11 强引用原语。

规则：

1. 所有者通常以 `ref = 1` 创建对象；
2. 异步任务在发布之前必须先取得引用；
3. 每次成功取得引用必须有且只有一次释放引用；
4. `ref = 0` 是终态，禁止复活；
5. 下溢和饱和必须报错，禁止静默回绕；
6. 只有最后一次释放引用才允许真正释放对象。

`tr_refcount_get_unless_zero()` 只用于弱引用/能力查找尝试取得强引用。
已经持有合法强引用的代码应使用 `tr_refcount_get()`。

不要因为存在引用计数原语就给所有对象增加引用计数。
能用唯一所有权或静默屏障解决时，应优先使用更简单的模型。

<a id="81-graceful-shutdown-与-terminal-destroy-分离"></a>

## 8.1 优雅关闭与终局销毁分离

门面不再把优雅关闭策略隐藏在销毁中。

```text
需要优雅关闭：
    begin_drain()
        -> wait_drained(timeout)
        -> destroy()

需要立即终止：
    destroy()
```

`tr_client_destroy()` / `tr_server_destroy()` 只负责终局所有权
收敛，不选择 1 秒或其他魔法超时，也不替调用方决定是否等待业务 Stream。

所有终局销毁返回状态：

- `TR_OK`：对象存储已释放，旧指针失效；
- 非 `TR_OK`：对象仍由调用方拥有，可能已经关闭部分准入/事件源，
  调用方解决尚未退出的持有者/等待者后重试；
- 执行所有者/工作线程回调中调用同步销毁返回
  `TR_ERR_STATE`，不会静默失败。

因此调用方不得写：

```c
tr_client_destroy(client);   /* 忽略结果 */
client = NULL;
```

必须写：

```c
ret = tr_client_destroy(client);
if (ret == TR_OK)
    client = NULL;
```

## 9. RPC V2 调用与对象所有权

R14 已从源码与静态库移除旧 V1 raw Call、Endpoint、Executor Group 和消息保留/释放
API。当前 RPC 生命周期由 V2 Client/Server、Record 状态机和共享 TaskExecutor 管理；
本节只记录仍然适用的 SDK 所有权规则。完整合同见[公共 V2 SDK 文档](rpc-v2-sdk.md)。

### 调用能力与身份

- `tr_rpc_v2_call` 是可复制的弱能力，不延长 Client 生命周期；Client 开始销毁后，
  调用方不能再使用此前保存的能力；
- `tr_rpc_v2_call_id` 是用于比较/关联业务工作的身份值，不是发送能力；
- 调用方必须在外部生命周期所有者完成 drain/destroy 前停止发布新的 Client/Server
  使用者，并遵守对应的同步销毁屏障。

### 正文、元数据与回复所有权

- Client 发送接口成功受理前会复制输入正文/元数据；返回背压或错误时，输入仍由调用方拥有；
- Client 回调中的正文和元数据视图只借用到回调返回；需要长期保存时，应用自行复制；
- Server work 收到的请求正文、OPEN 元数据和取消视图只在该 work 的借用期内有效；
- `tr_rpc_v2_reply_append()` 和元数据 setter 成功后已复制输入；调用方可以立即复用源字节；
- 异步 producer 使用显式引用；应用线程发布前取得引用，所有者必须恰好 release。Producer
  可以晚于 Server terminal/destroy 存活，最终仍须归还引用。

### 同步生命周期屏障

排空和销毁不能从依赖其推进的 Reactor、Client 回调或 Server work 回调中同步等待。
回调先发布关闭意图并返回；外部控制线程负责等待 drain/静默，然后重试销毁直到成功。
这保证执行中的 V2 回调、Server work 和资源引用都已退出后才释放所属对象。

V2 Server 的对端退役、任务 drain 与分片预算归还顺序见
[`architecture/01-runtime.md`](architecture/01-runtime.md)；调用终局、回调域和 producer
的完整合同见 [`rpc-v2-sdk.md`](rpc-v2-sdk.md)。

## 9.3 Pipeline / Listener 销毁

Pipeline 注册表只保存所有者本地索引，不拥有 Pipeline 生命周期。

正常销毁必须满足：

```text
停止准入
   ->
静默/中止 CONTROL + DATA
   ->
data_reserved == 0
data_attached == 0
stream_affinity == 0
control 已解除绑定
   ->
从注册表注销
   ->
销毁 Pipeline
   ->
销毁 Listener
```

`Pipeline destroy`、`Registry destroy` 和 `Listener destroy` 在调试构建中
会检查这些不变量。销毁函数不应再隐式承担可能失败的停止/状态转换。

## 10. Reactor 所有权

Reactor/Connection 的可变传输状态属于 Reactor 所有者线程。

当前边界：

- 连接处理器/`callback_arg` 只存放在 `tr_connection` 中，只由 Reactor 所有者线程修改；
- 外部 `tr_reactor_set_handler()` 通过同步 `TR_CMD_SET_HANDLER` 提交修改；
- 帧/事件回调分发直接读取所有者拥有的连接状态，不获取互斥锁；
- 槽位只保留代次 + 状态能力元数据，并通过一个 C11 原子值一次性发布；
- 外部状态查询读取原子槽位快照，不进入事件循环互斥锁；
- 生产者侧 `ctl_lock` 和命令队列互斥锁只负责跨线程控制面交接，不属于 Reactor 事件循环热路径。

```text
生产者 / RPC / Channel 线程
        |
        | 命令 + eventfd
        v
---------------- 跨线程边界 ----------------
        |
        v
Reactor 所有者线程
  连接处理器
  解析器
  TX/RX 队列
  epoll 关注事件
  连接可变状态
```

不要用互斥锁弥补不清楚的所有者关系。
能通过单所有者解决的状态，应优先通过所有者模型解决。

调试构建会通过所有者线程不变量检查关键 Reactor 修改，
避免未来重新引入跨线程直接写状态。

## 11. 锁所有权

真正共享的状态仍然需要锁。持有互斥锁本身也是一种作用域资源所有权。

存在多个提前返回路径时推荐：

```c
struct tr_mutex_guard guard TR_AUTO(tr_mutex_guard_cleanup) = { 0 };

if (tr_mutex_guard_acquire(&guard, &shared_state->lock) != 0)
    return TR_ERR_SYS;

/* 任意 return 都会自动解锁 */
```

需要提前解锁时调用 `tr_mutex_guard_unlock()`；
它会先解除保护对象，再执行解锁，避免重复解锁。

互斥锁保护对象只用于真正共享状态，不能因为保护对象很方便，
就给 Reactor 所有者线程热路径增加锁。

## 12. 错误路径

局部拥有的资源优先使用自动清理，因为新增提前返回时不会静默遗漏释放。

传统向下跳转的 `goto` 清理仍允许用于无法合理拆成独立作用域资源的复杂部分初始化对象。

但同一个资源不能同时由两套清理机制管理。

错误路径设计目标：所有权唯一、释放顺序明确、新增失败分支默认安全，
不产生重复释放、资源泄漏或释放后使用。

## 13. 公共 API 的所有权说明

资源相关公共 API 应明确写出：

- 输入参数是借用还是转移所有权；
- 输出对象由谁拥有；
- `TR_OK` 时所有权是否转移；
- 失败时所有权是否保持不变；
- 销毁是同步还是异步；
- 是否需要静默；
- 是否允许从回调/工作线程中调用。

示例：

```c
/*
 * 所有权：
 * - TR_OK：Reactor 接管 fd；
 * - 其他返回值：fd 仍由调用方拥有。
 */
```

## 14. 编译器策略

项目继续使用：

```text
-std=c11 -Wall -Wextra -Werror -pedantic
```

`TR_AUTO()` 只封装 GCC/Clang 的 `cleanup` 属性。

这不代表项目切换为 GNU C，也不意味着允许任意 GNU 扩展。

## 15. 注释语言

代码说明、文档说明和 API 契约说明必须使用中文，
具体规范见 `docs/code_comments.md`。

API、类型、变量、宏、协议字段、状态值、命令、日志和错误原文保持原样；
所有权、引用计数、静默、工作线程、回滚等解释性概念使用中文。
