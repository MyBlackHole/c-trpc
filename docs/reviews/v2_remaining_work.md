# V2 开发剩余项

核对基线：2026-10-24，主线 TLS-C merge commit `0fa90817c15286485ee2e1cfe8372bb8ab8a8e5a`；TLS-D1 当前开发分支为 `feat/v2-tls-d-234`（2.0.0-dev.17）。
GitHub 总账：[架构总审查 #239](https://github.com/MyBlackHole/c-trpc/issues/239)。这里记录开发顺序与证据入口，不以单个 PR 或旧 Issue 勾选框代替整体验收。

## 已落地主线

V1 核心退役（#290）、统一 RPC Record 2、四种基数、普通 Client/Server、元数据、取消/截止、异步 producer、RESET 与单连接排空边界。#301 修复初始信用等待控制池容量时的唤醒；#302 在流槽耗尽时独立拒绝新流，详见 [PERF-V2-03](rpc_v2_stream_refusal_20261023.md)。完整可用边界见 [R17](rpc_v2_release_boundary_20261023.md)。

本次 dev.5 的 [LIVE-V2-01](rpc_v2_message_window_20261023.md) 已实现并本地验证
共享 Channel 的双向 L/W 准入、残余信用恢复和普通服务端响应收敛。
候选与实际主线验收以文中链接的 GitHub 登记为准。

dev.6 的 [Channel 预算修复](rpc_v2_channel_budget_20261023.md) 将普通 V2
Client/Server 的 Channel 固定对象、流槽和索引接入所属 shard 的共享内存预算。
这只覆盖 #241 的一个固定存储缺口；分域/全局账本与组合矩阵仍在下表。

dev.7 的 [服务端共享预算](rpc_v2_shared_memory_budget_20261023.md) 将普通 V2
Server 的 shard 本地账目连接到单个 Server 总额，避免旧等分额度过早拒绝。
租户、跨 Server、压缩等预算域和完整资源矩阵仍未完成。

dev.8 的 [DATA 标志隔离](rpc_v2_data_flag_isolation_20261023.md) 将可定位到
已打开流的非法 DATA 类型标志收敛为单流 RESET；#242 的连接组边界、
排空竞争及完整错误矩阵仍未完成。

dev.27 的 [DATA TX 容量恢复通知](rpc_v2_data_tx_ready_20261024.md) 为压缩 DATA 分帧
补上 Reactor 普通发送项池恢复事件，并由 Channel 在 owner 上恢复重试；Release 90/90、
聚焦 ASan/UBSan 与 TSan 已通过。该切片只覆盖 #247/#241 的一个活性缺口，确定性状态模型、
故障重放、分层诊断、分域预算和压缩慢消费者矩阵仍待实现，不代表横切验证或资源验收完成。

dev.28 的 [TEST-A 状态模型与轨迹回放](rpc_v2_state_model_replay_20261024.md)
增加独立 Python 标准库参考模型、严格 JSON schema v1、手写边界轨迹和确定性 CLI，
并接入现有 GCC 功能测试作业。Draft PR #317 的 14/14 GitHub 检查已通过，head 为
`d51984ffb0435bebd75984502f3fa2af902db995`；PR 仍未合并。它只建立 L1 oracle；TEST-B 真实组件确定性执行、
TEST-C 故障注入/缩减、TEST-D 集成与平台矩阵、TEST-E 诊断和容量性能仍未完成，
#247 与整体 V2 验收继续保持未完成。

dev.29 的 TEST-B1 在同一 TEST-A 轨迹上驱动真实 RPC Record Endpoint 测试夹具，检查取消后
迟到工作、重复终局回调和账本归零；Reactor、Channel、Dispatch 与 Executor 仍为受控替身，
未覆盖 TCP/真实派发调度。当前本地 GCC Debug/Release 均为 90/90，组件夹具为 35/35；
Draft PR #318 的实现提交 `f3fc0abb2766fc24bb6c737cd9f03d0702f4343c` 已通过 14/14 GitHub
检查，仍未合并；完整边界与本地/远端证据见
[TEST-B1 记录](rpc_v2_component_replay_20261024.md)。

dev.30 的 TEST-B2 在 TEST-B1 上增加 Deadline 终局后迟到工作的组件轨迹，验证 TIMEOUT RESET、
唯一终局和预算归还。它仍是受控下层交错，不代表真实 Dispatch 调度或 TCP 集成。本地 GCC
Debug/Release 全量各 90/90，模型 28/28、组件映射 9/9、Endpoint 夹具 35/35；Draft PR #319
实现 head `683c6eff5330b2f74bb4aa7c2fa631e075b1c39c` 的 14/14 GitHub 检查通过，PR 仍未合并；
证据见[TEST-B2 记录](rpc_v2_deadline_component_replay_20261024.md)。

dev.31 的 TEST-B3 增加同一 Endpoint 槽复用后旧代次 DATA 迟到轨迹。真实 Endpoint 必须
忽略旧流句柄的数据回调，不能增加新调用接收次数；新旧两次调用各自只报告一次取消终局并
退役，账本为零。当前模型 28/28、组件映射 10/10，GCC Debug/Release 全量各 90/90，
Endpoint 夹具 35/35，三条 Release L1/L2 回放均匹配；Draft PR #320 head
`b3bd7b8318cb5490a532a4a29c4a3e86865b0ea3` 的 14/14 GitHub 检查通过，PR 仍未合并。证据见
[TEST-B3 记录](rpc_v2_stale_generation_component_replay_20261024.md)。

dev.32 的 TEST-B4 增加成功 FINAL 已接受后收到 RESET 的组件轨迹。它检查 Endpoint 保留
先到的 OK 终局、不增加重复回调，并在 RESET 后退役与归还预算。当前模型 28/28、组件映射
11/11，GCC Debug/Release 全量各 90/90、Endpoint 夹具各 35/35，四条 B1–B4 回放在两种
模式均匹配；Draft PR #321 实现提交 `93502c1` 已通过 14/14 GitHub 检查，PR 待合并。证据见
[TEST-B4 记录](rpc_v2_final_reset_component_replay_20261024.md)。

dev.33 的 TEST-C1 增加固定 seed 模型探索、失败签名与确定性事件缩减，并扩展既有
`build-test (gcc)` 检查和失败工件归档。固定 64-seed 测试集覆盖 schema v1 的 17 种操作，
生成轨迹包含 APPLIED、STALE、REJECTED、RESOURCE_LIMIT；测试专用重复终局回调 mutant
能缩减到 OPEN/CANCEL。它只验证 L1 模型工具链，不是生产 C 故障注入。Draft PR #322 的 GCC、
Clang、ASan/UBSan、TSan、RPC smoke 与静态分析通过；Make/install job 在 SDK V2 消费端
`tr_client_destroy(client->handle) == TR_OK` 断言失败，详见 CI 首轮记录，原因待单独核实。
详细证据见 [TEST-C1 记录](rpc_v2_test_c1_explorer_20261025.md)。

dev.34 的 TEST-C2 增加一条固定 Endpoint 最终错误状态发送背压回放：模型 `ERROR` 映射到
夹具 `UNIMPLEMENTED`，真实 Endpoint 的 `tr_stream_sendv` 首次返回一次 `TR_AGAIN`、第二次
接受唯一最终帧；组件观察与 L1 在终局次数、工作完成、最终帧、槽退役及零预算上匹配。
该测试仍使用受控 Channel/Dispatch/Executor，不覆盖 TCP、通用故障日程或 TEST-C 完整要求；
详见 [TEST-C2 记录](rpc_v2_test_c2_endpoint_send_backpressure_20261025.md)。

dev.18/.19 的 [连接组成员代次修复](rpc_v2_group_member_generation_20261024.md)
让 DATA 路由代次在单个分片注册表生命周期内唯一，阻止旧 CONTROL 关闭后未完成的
DATA 前导跨同一 `(group_id, epoch)` 的 Pipeline 重建附着。dev.19 另修正旧 Pipeline 预留
入口未初始化新请求字段的问题，并覆盖一个 DATA 已附着、另一个前导仍 pending 时的
CONTROL 丢失交错。该切片不覆盖 GOAWAY 接受边界、RESET/错误分类或 #242 的完整组合故障矩阵。

#234 的 [TLS 接入 ADR-009](../architecture/decisions/ADR-009-tls-byte-endpoint.md)
界定字节端点、认证先于 TRR1/TRP1、WANT 状态与关闭所有权。dev.9 的
[TLS-A 原型](rpc_v2_tls_a_20261023.md)完成 BIO 对照和内部端点合同；TLS-B
冻结公开配置并加入 OpenSSL 3 后端；TLS-C 普通 V2 Client/Server Reactor 已由
PR #310 以 `0fa90817c15286485ee2e1cfe8372bb8ab8a8e5a` 合入主线，主线 CI 全绿。
TLS-D1 Task 1–4 已在 `feat/v2-tls-d-234` 实现并推送：普通 RPC mTLS/URI SAN、连接组
CONTROL/DATA 主体绑定、profile generation 轮换和旧连接撤销。TLS-D1 `.16` 的五种本地
工具链/消毒器矩阵均为 88/88，SDK header 安装修复 `.17` 的 GCC Release 复测也为
88/88；安装 SDK 的 C11/C++11 消费端和 CRC32C 无 SSE4.2 路径已验证。
GCC 11/Clang 14 静态分析和 PR #311 全部 GitHub 检查已在 `b713801` 通过；Clang 14
既有告警行号已按未变函数体证据校准，8 条告警及 `unverified` 状态保留。合并后的 main CI、
#234 部分交付登记与远程分支清理仍待 Task 5；
异步凭证属于 TLS-D2，资源总账和公网验收属于 TLS-E。

## 持续开发顺序

| 范围 | 当前剩余工作 | GitHub |
|---|---|---|
| 错误隔离 | GOAWAY 接受边界、RESET/错误分类与组合故障验收；连接组迟到 DATA 代次隔离已有回归 | #242 |
| 资源与活性 | 分域/全局预算账本、压缩、多 DATA 与慢消费者组合矩阵；共享 Channel 的 L/W 与持有释放已有回归 | #241、#235 |
| 运行模型 | 操作持有/取消上下文、父子 deadline、跨 owner 依赖和回收证明 | #243 |
| TLS 与身份 | TLS-C 已合入；TLS-D1 当前分支已实现普通 RPC/连接组 mTLS、主体连续性与 profile 轮换，本地五矩阵 88/88；固定版分析、GitHub 验收与 #234 更新待完成；TLS-D2 和 TLS-E 仍待开发 | #234 |
| 网络压缩 | 将已有独立 none/Zstd 原语接入普通 V2；方向协商和安全预算 | #235 |
| 服务目标 | 解析快照、选路、健康、等待就绪、有界 Attempt/重放 | #244 |
| 类型契约 | codec、描述符指纹、C/Rust 生成桩和有界解码 | #245 |
| 后端决策 | 明确字节/流/消息/RPC 契约，形成 TRP 与 HTTP/2 等价评估证据 | #246 |
| 横切验证 | TEST-A PR #317、TEST-B1 PR #318、TEST-B2 PR #319、TEST-B3 PR #320 与 TEST-B4 PR #321 的实现提交均已通过 14/14 GitHub 检查但待合并；TEST-C1 完成固定种子模型探索与 mutant 缩减，TEST-C2 完成一次固定 Endpoint `TR_AGAIN` 重试；通用生产 C 故障时序、TEST-B 完整事件字母表、真实网络/派发、TEST-D 集成平台矩阵、TEST-E 分层诊断与 40k 容量性能仍待完成 | #247 |
| 最终核对 | 将已实现能力逐项映射到验收，收口阶段性架构与模块任务 | #239、#240、#248、#154 |

长期治理 #249 属于持续维护职责。正式发布仍需完成上述有限范围的实现和验收；当前不得把 dev 预览称为已完成的 V2。
