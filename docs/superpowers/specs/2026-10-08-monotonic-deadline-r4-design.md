# R4：单调时钟绝对截止时间设计说明

## 背景

GitHub #154 的 R4（P1-3）要求连接与等待流程使用绝对截止时间。当前代码存在三类问题：

- `src/client.c` 和 `src/transport/group/client_group.c` 把 `uint32_t timeout_ms` 强制转换为 `poll` 的 `int` 参数。超过 `INT_MAX` 后可能变成负数，使有限等待意外成为无限等待。
- 两处连接 `poll` 在收到 `EINTR` 后都重新使用完整超时，反复中断时总耗时可以远超配置值。
- `tr_client_connect()` 分别给 TCP 连接和 HELLO 就绪等待完整超时；`tr_client_group_connect()` 也分别给 TCP 连接和 CONTROL route 前导数据发送完整超时。

已有 DATA `Connector` 在连接开始前注册单调时钟定时器，并由该定时器覆盖 TCP 连接与前导数据阶段。本次不改变这条异步路径。

## 目标与边界

- 用仓库内部的单调时钟截止时间辅助模块表达单个操作的绝对截止时间，不增加公开 API 或 ABI。
- 修复客户端和客户端连接组路径中的 `poll` 整数溢出与 EINTR 重置问题。
- 让 `tr_client_connect()` 的 TCP 连接到 HELLO 就绪共用一个总时限；让 `tr_client_group_connect()` 的 TCP 连接到 CONTROL route 前导数据发送共用一个总时限。
- 让 `tr_server_drain()` 在逐个等待多个 Channel 排空时传递同一绝对截止时间，避免每个 peer 重新构造剩余毫秒数。
- 复用辅助模块表达已有的单调时钟条件变量截止时间；保持每个公开等待函数现有的零值含义和返回码。
- 继续由调用方或当前模块按现有约定拥有 fd；超时、系统错误和回滚的资源所有权不变。

本次不改变公开配置、默认超时、自动重连策略、异步 DATA `Connector` 定时器或协议格式，也不把 Reactor 所有者线程队列中的非等待工作重新定义为可中断操作。

## 适用范围

本设计针对 Linux 用户态 C 库，使用 POSIX 的 `poll()`、`clock_gettime()` 与 `pthread_cond_timedwait()`；不声称这是内核态实现或内核补丁规范。若将来移植进 Linux 内核，应改用内核时间与等待接口，例如 `ktime_get()`、`ktime_t` 和 `wait_event_hrtimeout()`，并按内核编码风格另行审查。[内核时间接口](https://docs.kernel.org/core-api/timekeeping.html) [内核等待接口](https://docs.kernel.org/driver-api/basics.html) [内核编码风格](https://docs.kernel.org/process/coding-style.html)

## 内部截止时间契约

新增 `src/execution/deadline_internal.h` 与 `src/execution/deadline.c`，作为仅供仓库内部使用的时间原语，并将实现文件加入 `xmake.lua` 的 `trcore` 源文件清单。截止时间对象保存 `CLOCK_MONOTONIC` 绝对 `timespec` 及有限/无限标记。辅助模块必须根据构建目标的 `time_t` 可表示范围进行检查，不得假设 `time_t` 固定为 64 位；加法无法表示时饱和到最大有效值（`tv_sec` 为可表示最大值，`tv_nsec` 为 `999999999`），剩余时长比较与换算也不得发生整数回绕。该极端边界允许比请求时限更早到期，但绝不能因溢出变成更长等待或无限等待。

辅助模块提供以下能力：

1. 从毫秒超时创建有限截止时间，包括 `timeout_ms == 0` 的立即截止时间；另提供显式无限截止时间，避免把不同 API 的零值约定混在一起。有限零超时的连接 `poll` 保留一次 `poll(..., 0)` 就绪探测，之后不再等待。
2. 查询截止时间是否到期，以及取得剩余时长。系统时钟读取失败时返回 `TR_ERR_SYS`。
3. 计算 `poll` 等待值：正的剩余纳秒向上取整为毫秒，再限制到 `INT_MAX`；无限截止时间映射为 `-1`。辅助模块同时报告总截止时间是否已到期，因此一次 `INT_MAX` 上限等待返回 0 时可以重算并继续。
4. 取得可直接传给使用 `CLOCK_MONOTONIC` 条件变量的绝对 `timespec`。

向上取整避免剩余时间不足一毫秒时将有限等待提前截成 0。除上述立即连接探测外，若 `poll` 返回就绪时总截止时间已经到期，调用方仍返回超时；任何情况下都不能因此重置或延长截止时间。

## 连接路径

### 普通客户端

`tr_client_connect()` 在开始 TCP 连接前只创建一次截止时间，并把它传给内部 fd 连接等待。`poll` 收到 `EINTR`、提前返回 0 或因 `INT_MAX` 上限结束后，均重新查询该截止时间。仅当总截止时间尚未到期时才再次等待；到期返回 `TR_ERR_TIMEOUT`，其他 `poll` 错误仍返回 `TR_ERR_SYS`。

fd 连接成功后，同一截止时间继续用于 Channel 的 CONTROL HELLO 就绪等待。为此增加仅内部使用的截止时间等待入口；公开 `tr_channel_wait_ready(channel, lane, timeout_ms)` 保持原签名与语义，并由它自行从调用时刻创建截止时间。内部入口先检查截止时间再接受就绪状态，避免 TCP 与 HELLO 阶段相加后超过总预算。连接失败仍走现有回滚路径。

### 客户端连接组

`tr_client_group_connect()` 在 Reactor 所有者线程完成准备阶段后、开始 TCP 连接前创建一次截止时间，并将其传给 fd 连接等待和 `tr_client_group_send_all_fd()`。每次发送前及发送返回后都检查截止时间，避免连续小段发送或连续 `EINTR` 越过预算；遇到 `EAGAIN`、`EWOULDBLOCK` 或 `EINTR` 时继续服从该截止时间。发送等待中的 `poll` 使用与普通客户端相同的重算、向上取整和 `INT_MAX` 限制规则。超时仍关闭调用方拥有的 fd，并沿现有失败路径撤销准备状态。

截止时间覆盖 TCP 连接与前导数据发送。所有者线程的准备工作发生在 TCP 阶段之前，保持现有同步步骤及错误语义；成功发送后沿用现有 `tr_client_group_adopt_control_on_owner()` 流程。

### 服务端排空

`tr_server_drain()` 在停止接入并向现有 Channel 发布排空请求后，创建一个总截止时间，并将同一截止时间传给后续每个 Channel 的排空等待。进入下一 peer 前若总截止时间已到期，继续返回现有 `TR_ERR_TIMEOUT`；单个 Channel 的排空等待超时仍由其内部契约返回 `TR_AGAIN`。`timeout_ms == 0` 时继续逐个进行立即状态检查，不阻塞。

### DATA Connector

保持当前单调时钟定时器覆盖 TCP 连接和 DATA route 前导数据的行为，不另建第二套超时来源。测试确认 R4 对同步路径的修改没有改变该异步流程。

## 条件变量等待

Channel 就绪、Channel 排空、客户端连接组排空和 Pipeline Listener 排空当前都使用 `CLOCK_MONOTONIC` 绝对 `timespec`。改为由同一辅助模块创建和转换截止时间，去除各处重复的饱和加法；条件变量、锁顺序、状态谓词与关闭竞态处理不变。`tr_server_drain()` 的多个 Channel 等待复用同一截止时间，不再为每个 peer 分别换算剩余毫秒数。

必须保持以下零值及返回码契约：

- `tr_channel_wait_ready(..., 0)`、`tr_client_group_wait_drained(..., 0)` 和 `tr_pipeline_listener_wait_drained(..., 0)` 继续无限等待。
- `tr_channel_wait_drained(..., 0)` 继续只检查当前状态；未排空时返回 `TR_AGAIN`，不阻塞。
- 有限就绪等待超时继续返回 `TR_ERR_TIMEOUT`；排空等待的超时继续返回 `TR_AGAIN`。
- 关闭、状态不匹配、等待上下文错误和条件变量系统错误继续返回各自现有状态码。

## 测试设计

新增针对截止时间辅助模块的专用测试，并使用链接器包装 `clock_gettime`、`poll` 及必要的 socket 操作来注入时钟推进和系统调用结果；不得加入生产故障注入钩子。

测试覆盖：

- `0`、`1`、`INT_MAX`、`INT_MAX + 1`、`UINT32_MAX` 超时构造及剩余时间换算；验证有限 `poll` 等待值从不为负数，除显式无限等待外不会传入 `-1`。
- `INT_MAX` 分段等待后总截止时间未到期时继续等待，最终耗尽时返回超时；截止时间加法饱和不回绕。
- 连续多次 `EINTR` 时，每次传给 `poll` 的时间均按同一截止时间递减；模拟提前返回 0 时也重新计算剩余时间。
- 普通客户端的 TCP `poll` 消耗部分预算后，HELLO 就绪阶段只获得剩余预算；客户端连接组的 TCP 阶段消耗部分预算后，CONTROL 前导数据阶段只获得剩余预算。
- 保持连接立即完成、前导数据分段发送、超时 fd 清理及连接回滚的既有行为。
- 条件变量等待覆盖零值语义、有限超时返回码、截止时间饱和边界及关闭竞态回归；DATA `Connector` 继续只使用原有单一定时器。
- 多 Channel `tr_server_drain()` 共享一个截止时间；零超时仍为立即检查，原有总超时与单 Channel `TR_AGAIN` 返回路径保持不变。

测试应断言传入系统调用的等待参数和绝对截止时间关系，不依赖真实墙钟等待数十天或以宽松时间容差证明总时限。

## 验收标准

1. 有限超时不会因 `uint32_t` 到 `int` 转换而成为无限等待。
2. `EINTR` 和提前 `poll` 超时不重置总预算；任何多阶段同步连接只使用一个绝对截止时间。
3. 客户端、客户端连接组、条件变量等待及异步 DATA `Connector` 的既有所有权、零值和返回码语义保持不变。
4. 新增说明、注释和提交说明使用中文；代码标识符与 Linux/POSIX 固定名称保持原样。
