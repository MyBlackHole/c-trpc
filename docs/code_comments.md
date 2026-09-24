# c-trpc 代码说明规范

## 语言

c-trpc 的代码说明以中文为主。

以下内容保留英文：

- C/C11、POSIX、Linux、epoll、eventfd、pthread 等标准或系统名称；
- ownership、refcount、quiescence、Reactor、Channel、RPC、Call、Stream 等与代码模型直接对应的术语；
- API、类型、变量、宏、协议字段、状态名；
- 外部协议或规范中的固定名称，例如 HELLO、GOAWAY、PING/PONG、CONTROL/BULK。

说明文字应先用中文解释含义，再保留必要的英文术语，例如：

```c
/* Endpoint 使用强引用（refcount）保护跨 worker 的异步生命周期。 */
```

不要求为了“中文化”翻译标识符，也不要创造与代码中不存在的新中文术语。

## 注释目标

注释优先说明“为什么”，其次说明“不明显的约束”，不重复代码本身。

推荐说明：

- 资源由谁创建、当前谁拥有、何时发生 ownership transfer；
- 为什么需要 refcount、quiescence、锁或 memory ordering；
- 成功/失败路径上的资源归属；
- 生命周期状态转换和销毁顺序；
- 协议约束、兼容性约束和边界条件；
- 不能从代码表面直接看出的并发不变量。

不推荐：

```c
/* i 自增 */
i++;

/* 给 state 赋值 */
state = TR_CONN_ACTIVE;
```

## 资源所有权注释

资源相关 API 应明确说明以下语义之一：

- **borrowed**：借用，不接管释放责任；
- **owned**：当前调用方拥有；
- **take on TR_OK**：仅成功时转移所有权；
- **strong reference**：通过 refcount 持有共享生命周期；
- **weak/capability handle**：句柄不代表对象所有权。

推荐格式：

```c
/*
 * 所有权：
 * - TR_OK：Reactor 接管 fd；
 * - 其他返回值：fd 仍由调用方拥有。
 */
```

## 并发注释

并发说明必须尽量明确 owner。

推荐：

```c
/* 仅 Reactor owner thread 修改；其他线程通过 command queue 提交操作。 */
```

或：

```c
/* endpoint->lock 必须已持有。 */
```

如果状态可以通过单线程 owner 解决，不要用“线程安全”这种模糊表述代替所有权说明。

## 中英文混排

- 中文与英文术语之间可直接使用空格增强可读性；
- 代码标识符使用反引号只适用于 Markdown 文档，C 注释中直接写标识符；
- 一段注释中同一概念尽量保持一种英文写法，例如始终使用 ownership，不混用 owner-ship/ownership；
- 错误码、状态和 API 名称保持原样。

## 公共头文件

公共头文件注释必须优先说明调用契约：

1. 参数是否 borrowed；
2. 返回对象由谁拥有；
3. TR_OK / 失败时 ownership 是否变化；
4. 是否同步等待；
5. 是否允许从 Reactor callback / worker 中调用；
6. 是否有线程或生命周期限制。

## 源文件

源文件中的实现注释主要说明：

- 不变量；
- 锁顺序；
- refcount 生命周期；
- quiescence 边界；
- 资源 transfer；
- 故障恢复和 rollback 原因；
- 性能路径中为何避免某种操作。

## 修改要求

新增或修改非平凡代码时，新增说明默认使用中文。

历史英文注释允许分阶段迁移；当代码所在区域被修改时，应优先把同一区域的关键英文说明同步改成中文。
