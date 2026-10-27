# RPC V2 接收配额失败隔离回归

日期：2026-10-25。关联 #242/ERR-A。版本：`2.0.0-dev.42 -> 2.0.0-dev.43`。

## 覆盖范围

`test_receive_quota_preserves_peer` 使用真实 TCP、Reactor、Channel 与 RPC Record Stream，
在同一 Channel 上建立两条活动调用。服务器预算保留到耗尽后，测试只向第一条调用发送一条
RAW MESSAGE 的完整固定头，声明正文仍有 32 字节。接收端因此必须为后续正文预约缓存；预约
失败应以 `RESOURCE_EXHAUSTED` 终结第一条调用，且不得交付 MESSAGE。

RPC Record Stream 的 abort 只选择本地终局，网络 RESET 属于上层适配责任。测试在确认服务器
本地终局后显式发送 `TR_ERR_NOMEM` RESET，并验证客户端映射到同一 RPC 状态。释放服务器预留
预算后，第二条调用继续接收 MESSAGE/HALF_CLOSE、发送 INITIAL_METADATA/MESSAGE/FINAL_STATUS，
两端均以 OK 完成。四个端点终局计数各为一次，终局消息缓存归零。

该测试同时明确了配额注入方式：完整 RAW 记录可直接借用输入视图，不会因为预算已满而失败；
必须发送不完整记录，才能覆盖有预算计费的重组缓存预约路径。

## 验证

- Release 构建：`xmake build -P . -j 4 test_rpc_record_stream`。
- Release 全目标：`build/linux/x86_64/release/test_rpc_record_stream`，输出 `rpc_record_stream: ok`。
- `git diff --check` 通过。
- 本切片只改测试与记录，不改变生产代码、公开 API/ABI 或协议。

## 边界与风险

此回归覆盖单条不完整 RAW MESSAGE、单流预算失败、显式 RESET 和同 Channel 同伴调用继续完成。
它不覆盖压缩解码工作区、数据/连接信用压力、同时多流失败及 ERR-B/C/D/E 的组合矩阵；不能据此
宣称 #242 或 V2 完成。运行时性能无影响，测试增加一个真实双 Reactor/Channel 场景。

回滚可撤销 dev.43 提交，恢复 `xmake.lua` 与 README 版本，并移除本记录及新增回归；之后运行
同一 Release 目标确认原测试集仍通过。

## 验收标准

1. 预算耗尽时服务器只终结受影响调用，并报告 `RESOURCE_EXHAUSTED`。
2. 上层 RESET 使对端收到对应终局，且每个端点至多回调一次。
3. 同 Channel 的另一条调用在释放预算后正常完成。
4. 失败调用及成功同伴调用的终局接收缓存均为零。
