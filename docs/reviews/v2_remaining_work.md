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

dev.18 的 [连接组成员代次修复](rpc_v2_group_member_generation_20261024.md)
让 DATA 路由代次在单个分片注册表生命周期内唯一，阻止旧 CONTROL 关闭后未完成的
DATA 前导跨同一 `(group_id, epoch)` 的 Pipeline 重建附着。该切片不覆盖 GOAWAY 接受
边界、RESET/错误分类或 #242 的完整组合故障矩阵。

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
| 横切验证 | 状态模型、可重放故障、分层指标、40k 容量/性能与全部资源账本 | #247 |
| 最终核对 | 将已实现能力逐项映射到验收，收口阶段性架构与模块任务 | #239、#240、#248、#154 |

长期治理 #249 属于持续维护职责。正式发布仍需完成上述有限范围的实现和验收；当前不得把 dev 预览称为已完成的 V2。
