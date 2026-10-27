# 多 DATA 成员慢消费者隔离回归

日期：2026-10-25。关联 #241。版本：`2.0.0-dev.76 -> 2.0.0-dev.77`。

## 目的

#241 要求慢消费者不能通过全局排队阻止其他流或连接继续前进。现有 `test_public_connection_group_multi_data_drain` 已证明多个 transfer 可以落在不同 DATA 成员，但两边消息此前都会立即释放，没有验证一边保留应用数据时另一边是否继续进展。

本切片让第一 DATA 成员上的服务端回调接管消息所有权并持有接收缓冲，再发送第二成员的消息；第二条消息必须在第一条仍被持有期间独立到达服务端回调并释放。路由探针检查两个消息来自不同物理连接。场景随后释放第一条消息，再继续原有的双成员 drain 验证。

## 场景与断言

- 真实 TCP Client/Server connection group 建立两条 DATA 连接，并为 stream 5001、5002 分别发布 transfer-ready。
- Client 向 stream 5001 发送首成员消息 `0x31`；Server callback 返回 `TR_CONNECTION_GROUP_MESSAGE_TAKE_OWNERSHIP`，测试保留完整 message handle。
- 在确认首消息仍被持有后，Client 向 stream 5002 发送第二成员消息 `0x32`。Server 必须在原消息释放前收到第二消息并返回 `TR_CONNECTION_GROUP_MESSAGE_RELEASE`。
- 测试检查两个 message route 使用不同 Reactor connection generation，重新校验首消息的缓冲仍有效，然后显式释放它。
- 随后执行该测试原有的双 DATA transfer drain 与连接关闭检查。

若实现把某成员的应用持有状态错误扩散为整个组的消息派发阻塞，第二条消息的等待会超时；若两条 transfer 被错误路由到同一 DATA 物理连接，连接身份断言失败。

## 测试夹具修正

初版暂用 message ID `6201` 表示被持有消息，但同一 callback 也被成员故障替代测试使用；该测试没有释放此消息，完整门面运行因此在 `tr_server_destroy()` 处失败。根因是测试 callback 的 ID 串扰，不是生产路径的所有权泄漏。慢消费者场景已改用专属 ID `6611/6612`，普通成员替代场景保持原来的即时释放行为；修正后完整门面回归稳定通过。

## 验证与边界

- GCC Release：完整 `test_connection_group_facade` 连续运行 5/5 通过。
- GCC TSan：完整目标通过，无数据竞争报告。
- GCC ASan/UBSan：完整目标通过，启用 leak 检测且无诊断。
- 仅修改测试、README、剩余工作台账与版本号；不改生产代码、公开 API/ABI 或 wire 协议。
- 本测试持有一条消息，不会耗尽 Server 全局预算；也不覆盖压缩任务/编码 wire 与多 DATA 慢消费者叠加、信用池耗尽或长时间饱和公平性。#241/#235 继续保持未完成。
