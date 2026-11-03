# 2026-10-14：Channel 握手协议缓冲区所有权回归

## 范围与问题

GCC 11 对 `src/transport/channel/channel.c:581` 报告
`analyzer-malloc-leak`，关联 `tr_channel_send_hello()`。
这一诊断目前仍是 `unverified`，不能在没有证据时删除告警基线或将其
视为已确认误报。

`tr_channel_protocol_buffer` 将 `struct tr_buffer` 放在首成员，并由
`release_cb` 负责释放整个分配对象。
HELLO 与 HELLO_ACK 使用相同的路径：

1. `calloc()` 取得缓冲区，当前调用栈拥有所有权；
2. `tr_reactor_send()` 返回 `TR_OK` 时，才调用
   `tr_buffer_take()` 移交给 Reactor 的 TX 项；
3. 其他返回值保留当前调用栈所有权，离开作用域由
   `TR_AUTO(tr_buffer_cleanup)` 触发 `release_cb`；
4. 成功发送的 TX 项完成、连接终止时由 Reactor 释放缓冲区。

## 本阶段新增的可达故障矩阵

`tests/test_channel_hello_ownership.c` 在测试目标链接阶段分别对
`tr_reactor_send` 和 `free` 施加 `--wrap`。每次 HELLO/HELLO_ACK
发送在执行真实准入逻辑前记录精确缓冲地址，最终释放必须命中该地址一次。
不会通过“运行成功”代替释放检查，也不会将普通 Channel 对象分配误算为
协议发送缓冲区。

| 场景 | 断言 |
| --- | --- |
| HELLO 发送准入返回 `TR_ERR_SYS` | 发送缓冲在回滚后归还一次；构造失败；连接与 Reactor 可终止 |
| HELLO 发送准入返回 `TR_AGAIN` | 临时背压拒绝同样不能转移所有权或泄漏 |
| 真实双端 HELLO/ACK 成功握手 | 2 个 HELLO、2 个 ACK 真实提交，双方就绪并终止后四个缓冲全部归还 |
| 首次 HELLO_ACK 准入返回 `TR_ERR_SYS` | 以条件变量等待真实 ACK 处理进入错误分支；所有者静默屏障后正常销毁，全部已获取缓冲必须归还 |

计数采用 C11 原子操作，以允许 Reactor 所有者线程与调用线程分别释放；
故障注入没有睡眠竞争、没有引入生产故障钩子，也未改变 SDK、协议、
默认预读状态、RPC 引用计数或告警基线。

## 验收界限

该测试验证**已进入发送准入的缓冲区**在上述可达场景中的释放和所有权转移，
不能证明所有发送队列故障、所有连接重连交错或所有内存分配路径。
需要保留 GCC `channel.c:581` 的 `unverified`，直至完成静态诊断路径
逐项核对及覆盖范围评估；其余 GCC 与 Clang 精确基线不应修改。
`#154` 保持开放，完整 CI/地址检查/线程检查必须以本次提交的精确
head 为准重新确认。
