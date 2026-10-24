# RPC V2 连接组压缩 wire 在途排空修复记录

## 问题

在服务端 Connection Group 已开始 drain、但现有 DATA 成员仍有活动 transfer 时，客户端可以继续发送既有 transfer 的 DATA。普通 DATA 回调正常交付；Zstandard DATA 的异步解码完成回调却会因 listener 的 `draining` 标志丢弃消息。

## 根因与修复

`tr_pipeline_listener_message_done()` 原先以 `tracked != NULL && !listener->draining` 判断异步解码结果是否仍有效。`tr_pipeline_listener_job_connection()` 已按连接槽代次、句柄代次和完整路由快照验证 DATA 成员仍是原对象；额外的 drain 条件错误地把“关闭新接入”扩大成“撤销已附着成员上的业务消息”，与连接组 drain 期间现有连接/传输继续运行的合同冲突。

现在只以精确连接/路由代次身份判断 completion 是否仍有效。listener drain 继续关闭新连接和新 transfer 准入，但不丢弃仍属于活动 DATA 成员的压缩解码结果。

## 回归步骤与断言

1. 建立带 Zstandard 协商的 CONTROL/DATA 连接并接纳 Stream 5001。
2. 暂停客户端真实 Zstandard 压缩调用后的完成回调，并将编码输出的 DATA socket 写入阻塞在测试 wrapper。
3. 开始 Client/Server drain；两端等待均未完成，且新 Offer/Ready 不再接受。
4. 放开压缩完成回调，使已编码 wire 进入 Reactor TX；随后释放客户端 transfer。`send_bytes_inflight` 仍保持原始长度，Client drain 继续超时。
5. 放开 DATA socket 写入；服务端验证完整解压正文并交付唯一一条消息。压缩 wire 归还后 Client drain 完成；Server drain 仍等待客户端关闭 DATA 成员。
6. 关闭连接组后 Server drain 完成，组、连接、DATA 成员和活动 transfer 计数归零。

修复前，测试确认发送字节归还但服务端回调始终未执行，且 DATA 连接没有关闭事件；修复后同一测试通过。

## 验证

- GCC Release：完整 `test_connection_group_facade` 通过。
- GCC TSan：完整 `test_connection_group_facade` 通过，无数据竞争报告。
- `git diff --check` 通过。

## 范围

修复只改变 listener 已排空时对现有 DATA 压缩任务完成回调的判定；新连接和新 transfer 准入仍由 drain 标志关闭。连接故障/重连、多组并发竞争及完整 ERR-D 验收仍待继续。
