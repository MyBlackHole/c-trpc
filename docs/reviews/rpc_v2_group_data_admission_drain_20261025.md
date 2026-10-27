# RPC V2 连接组 DATA 接受与排空边界

日期：2026-10-25。关联 #242/ERR-A。版本：`2.0.0-dev.70 -> 2.0.0-dev.71`。

## 问题与复现

连接组 listener 已进入 draining 后，同步授权模式仍能接受一个尚未完成 route 前导的 DATA 连接。复现使用真实 TCP listener、
Reactor、CONTROL 会话和预留 DATA route：先发送 DATA_OFFER，让客户端连接并只发送 route 前导的前 N-1 字节；调用
`tr_pipeline_listener_begin_drain()` 后再发送最后一个字节。修复前运行
`xmake run -P . test_pipeline_listener`，`data_accepts` 从 drain 前快照增长，断言失败。

## 根因与修复

`tr_pipeline_listener_accept_control()` 与异步授权入口均拒绝 draining 状态，但同步
`tr_pipeline_listener_accept_data()` 缺少该状态检查。完整 route 前导在 drain 屏障之后到达时，路径仍验证路由并附接 DATA
连接。现于同步 DATA 接受入口增加 draining 检查并返回 `TR_ERR_CLOSED`；既有前导失败路径撤销精确 route reservation、计入
route rejection 并关闭 pending 连接。完整 route 前导在 drain 前已被处理的连接不受影响。

回归保留真实 pending socket：确认 DATA_OFFER 成功并只发送前导的前 N-1 字节，开始 drain 后补齐；最终要求
`route_rejections` 增加一次、`data_accepts` 不变、pending socket 关闭且 listener connection 计数回到只有 CONTROL。

## 验证

- 修复前：新回归稳定失败，`stats.data_accepts == data_accepts_before_drain` 断言捕获了接受计数增长。
- 修复后，GCC Release、TSan、ASan/UBSan 下 `test_pipeline_listener` 与 `test_connection_group_facade` 均通过。
- `git diff --check` 在提交前执行。
- 本地临时目录与构建缓存位于仓库 `build/test-tmp` 和 `build/`；未在 `/tmp` 写测试文件。

## 影响、性能、风险与回滚

影响范围是服务端连接组同步授权下的 DATA route 新准入边界，不改公开 API/ABI、route 格式或线协议。每条 DATA route 接受路径
只增加一次 owner-local `draining` 布尔判断，无新锁、分配或按帧成本；无需性能压测。风险为 drain 开始前已发 offer、但
route 前导尚未完整到达的 DATA 连接现在会被拒绝；这与 listener drain 关闭新准入、CONTROL/异步授权现有检查保持一致，且
失败路径精确撤销预留。回滚可撤销 dev.71、恢复版本到 dev.70 并移除此回归与记录。

## 验收标准

1. draining 后完成的同步 DATA route 前导不会被附接或增加 `data_accepts`。
2. 被拒绝连接使 `route_rejections` 增加一次，route reservation 被失败路径撤销，pending TCP 连接关闭。
3. drain 前已附接的 CONTROL 与 DATA 生命周期不被本次检查改变。
4. Release、TSan、ASan/UBSan 下 listener 与连接组门面目标通过。

版本变更：`xmake.lua` 与 README 从 `2.0.0-dev.70` 更新到 `2.0.0-dev.71`。
