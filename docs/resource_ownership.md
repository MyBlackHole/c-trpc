# 资源所有权规范

c-trpc 以 ISO C11 为语言基线。作用域自动清理使用 GCC/Clang 的 `cleanup` attribute，
但所有项目代码仍按 C11 语义组织，不启用 GNU statement expression、nested function 等额外语言特性。

本项目的资源模型以**显式 ownership**为核心，而不是依赖隐式生命周期。

## 1. 资源状态

每个资源在任意时刻必须明确属于以下状态之一：

- **owned**：当前存在唯一 owner，由它负责最终 release；
- **borrowed**：临时借用，不获得 release 责任；
- **shared reference**：通过 refcount 显式持有异步共享生命周期；
- **transferred**：ownership 从一个 owner 明确转移到另一个 owner。

`tr_conn_handle`、`tr_stream_handle` 等 handle 是 capability，**不是 ownership**。

## 2. 成功才转移 ownership

除非 API 文档明确说明，否则 ownership 只在返回 `TR_OK` 时发生转移。

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

send 失败时 buffer 仍属于当前作用域，离开作用域会自动 cleanup。
send 成功后 Reactor 接管 buffer，`tr_buffer_take()` 清空本地 cleanup-managed 变量，避免 double free。

## 3. 作用域自动清理

具有 lexical ownership 的局部资源，原则上优先使用 `TR_AUTO(cleanup_fn)`。

当前主要 typed ownership helper：

- `tr_fd_cleanup()` / `tr_fd_take()`
- `tr_buffer_cleanup()` / `tr_buffer_take()`

新增资源类型时应提供 typed cleanup/take helper，不要依赖通用 `void *` cast 隐藏类型。

cleanup 函数必须满足：

1. 能处理 disarmed/empty 状态；
2. 一次只释放一个明确 owned 的资源；
3. 可行时把变量恢复为空状态；
4. cleanup 自身不能偷偷取得新的 ownership。

## 4. cleanup 顺序

cleanup variable 按声明顺序的**逆序**执行。

如果 child 依赖 parent，应先声明 parent，再声明 child：

```c
struct parent *parent TR_AUTO(parent_cleanup) = NULL;
struct child *child TR_AUTO(child_cleanup) = NULL;
```

离开作用域时先 cleanup child，再 cleanup parent。

因此重排 cleanup-managed 变量声明时，必须同时检查生命周期依赖关系。

## 5. 构造与事务 guard

自动清理分为三种主要模式。

### 5.1 简单 lexical owner

适用于单一独立资源，例如 fd、buffer、heap allocation。

### 5.2 complex build guard

如果对象只有完成多个初始化步骤后才能调用完整 destructor，则使用 build guard 记录哪些步骤已经成功。

典型对象包括 Reactor、Channel、RPC Endpoint、executor group。

build guard 主要解决两个问题：

- partial object 错误调用完整 destructor；
- 新增初始化步骤后忘记修改某个 `goto fail_*` 路径。

成功构造完成后，必须显式 disarm build guard，把完整对象交给新的 owner。

### 5.3 transaction rollback guard

对于在已有 owner 上执行多步状态修改的操作，使用 armed rollback guard。

典型场景：Client connect、Server peer adoption。

失败时 scope cleanup 自动 rollback；成功后必须显式 disarm guard。

## 6. 显式 ownership transfer

ownership transfer 必须能从代码中直接看出来。

推荐：

```c
dst = tr_buffer_take(&src);
```

不推荐：

```c
dst = src;
src = NULL;   /* 难以判断这是普通赋值还是 ownership transfer */
```

因此 bare `ptr = NULL` 不应作为正常的 ownership move 语法。

## 7. 异步生命周期

scope cleanup 只能解决当前 lexical scope，不能解决跨线程、queue、callback 的异步生命周期。

一个指针跨异步边界前，必须满足以下机制之一：

- single-owner 保证；
- 显式 quiescence；
- 强引用 refcount。

禁止先把 pointer 发布给其他线程，再补 `get()`。

## 8. 强引用 refcount

`struct tr_refcount` 是项目统一的 C11 strong-reference primitive。

规则：

1. owner 通常以 ref=1 创建对象；
2. async task 在发布之前必须先 get；
3. 每次成功 get 必须有且只有一次 put；
4. ref=0 是终态，禁止 resurrection；
5. underflow 和 saturation 必须报错，禁止静默 wrap；
6. 只有最后一次 put 才允许真正 release 对象。

`tr_refcount_get_unless_zero()` 只用于 weak/capability lookup 尝试获得 strong reference。
已经持有合法 strong reference 的代码应使用 `tr_refcount_get()`。

不要因为存在 refcount primitive 就给所有对象加 refcount。能用 unique ownership 或 quiescence 解决时，应优先使用更简单的模型。

## 9. RPC Endpoint 生命周期

RPC Endpoint 是当前第一个迁移到 shared ownership 的核心对象。

```text
peer/client owner ref = 1
        |
queue executor task
        | get
        v
owner ref + task ref
        |
task completes
        | put
        v
owner-only ref
        |
destroy:
  clear handler
  quiesce Channel callbacks
  stop deadline source
  stop new executor tasks
  wait refs == 1
        |
owner put
        v
refs == 0
        |
release Endpoint
```

`tr_rpc_endpoint_destroy()` 保持同步语义，因为 Endpoint 只是 borrowed Channel。

Endpoint destroy 返回后调用方可以立即 destroy Channel，所以所有异步 Endpoint reference 必须在返回前排空。

Call 自己的 `task_refs` 职责不同：

- `tr_refcount`：保护 Endpoint 对象生命周期；
- Call `task_refs`：阻止 Call slot 在 task 尚未结束时被复用。

## 10. Reactor ownership

Reactor/Connection 的可变 Transport 状态原则上属于 Reactor owner thread。

其他线程应通过 command queue 提交操作，而不是直接取得可变状态 ownership。

不要用 mutex 去弥补不清楚的 owner 关系。能通过 single-owner 解决的状态，应优先通过 owner model 解决。

## 11. Lock ownership

真正共享的状态仍然需要锁。持有 mutex 本身也是一种 scope-owned resource。

多 early-return 路径时推荐：

```c
struct tr_mutex_guard guard TR_AUTO(tr_mutex_guard_cleanup) = { 0 };

if (tr_mutex_guard_acquire(&guard, &endpoint->lock) != 0)
    return TR_ERR_SYS;

/* 任意 return 都会自动 unlock */
```

需要提前 unlock 时调用 `tr_mutex_guard_unlock()`；它会先 disarm guard，再执行 unlock，避免重复 unlock。

mutex guard 只用于真正 shared state，不能因为 guard 很方便就给 Reactor owner-thread hot path 增加锁。

## 12. 错误路径

局部 owned 资源优先自动 cleanup，因为新增 early return 时不会静默漏 release。

传统 downward `goto` unwind 仍允许用于无法合理拆成独立 scope resource 的复杂 partial object。

但同一个资源不能同时被两套 cleanup 机制管理。

错误路径设计目标：ownership 唯一、release 顺序明确、新增失败分支默认安全、不产生 double free / leak / UAF。

## 13. 公共 API 的 ownership 说明

资源相关公共 API 应明确写出：

- 输入参数是 borrowed 还是 owned；
- 输出对象由谁拥有；
- `TR_OK` 时 ownership 是否 transfer；
- 失败时 ownership 是否保持不变；
- destroy 是同步还是异步；
- 是否需要 quiescence；
- 是否允许从 callback/worker 中调用。

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

`TR_AUTO()` 只封装 GCC/Clang 的 `cleanup` attribute。

这不代表项目切换为 GNU C，也不意味着允许任意 GNU extension。

## 15. 注释语言

代码说明以中文为主，具体规范见 `docs/code_comments.md`。

ownership、refcount、quiescence、Reactor、Channel、RPC 等术语保留英文，以便与 API 和实现结构直接对应。
