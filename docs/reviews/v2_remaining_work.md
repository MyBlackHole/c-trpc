# V2 开发剩余项

当前候选：2026-10-29，`2.0.0-dev.179`。前一 dev.178 切片的真实握手红测复现 Client 信任目录延迟加载违背构造期快照合同：创建后新增 CA 被旧 profile 接受，删除原来源则丢失信任。新内部模块在构造返回前读取文件/目录 X509 与 CRL，不注册延迟查找器；新增 18 个快照、错误输入及分配回滚场景。GCC Release 全目标及全量 95/95、Clang Release/ASan/UBSan/TSan 各八个 TLS 目标、四编译器安装 SDK 消费端、55 单元真实编译数据库、clang-format 18.1.8 五个变更源文件检查通过，见[信任快照修复记录](rpc_v2_tls_trust_snapshot_20261029.md)。实现提交 `8f49481845dcd1411ac7b79b097287bf7f3fe470` 已推送，精确 head 远端 CI 13/13 成功；后续文档提交单独核对。旧提交 dev.177 的 250 RPS 小消息 TLS/Zstd 小时诊断完成 900000/900000，原 clean=true，Server 含预热 900032 次全部排空；361 个资源采样及严格范围见[小时诊断记录](rpc_v2_tls_zstd_soak_20261029.md)。TLS-E 恶意输入、持续握手、内核驻留、平台及完整发布验收仍未完成。

前次候选：2026-10-29，`2.0.0-dev.177`，同 profile 34 轮 TLS 周转已提交并推送为 `97e106c6b43c4c0b8134dde2c328436009fb30ea`，远端 13/13 全部成功。旧的逐轮精确字节平台假设被不同证书重复实验否定，根因定位为 RSA 盲化缓存合法扩容；最终设计逐轮核对块数，profile 释放后、全局 cleanup 前精确回到预热字节/块基线。最终设计连续 20/20，包含两次该缓存扩容，Clang Release、ASan/UBSan、TSan 各八个 TLS 目标通过；遗漏 SSL 释放的变异被捕获并恢复生产源码。全量中发现的独立 SDK 对端终局等待已作为 dev.176 提交 `dc39c5c`，四种本地配置的 SDK 目标通过，见[SDK 屏障记录](rpc_v2_sdk_remote_terminal_barrier_20261029.md)。dev.177 组合 GCC Release 构建和串行全量 95/95 通过，见[TLS 周转记录](rpc_v2_tls_resource_waves_20261029.md)。此前 dev.174/175 精确提交 `f1c0b0f8d8a394bebaf0a0885ff3657b4ccbc2ba` 远端 13/13 成功；后续文档 head `99d108ba993e423a9053e7ccd67e7afb5e73799b` 也已核对 13/13 成功。TLS-E 与完整发布验收仍未完成。

前次本地候选：2026-10-29，`2.0.0-dev.175`。独立 OpenSSL 分配账本复现并修复带预算 session 分配失败误报成功、socket BIO 方法表无释放域两项问题；方法表由 profile 拥有，会话强引用保护完整寿命，类型编号只申请一次。GCC Release 串行全量 95/95；Clang Release、ASan/UBSan（泄漏检查）、TSan 各八个 TLS 目标通过，均包含十二个独立资源场景。远端精确 head 待推送核对，不能借用前次绿色检查。证据与剩余资源边界见[TLS 分配账本记录](rpc_v2_tls_resource_ledger_20261029.md)。dev.175 单独修复并行入口暴露的完成标记测试竞态；相同调度间隙的红/绿对照、最终 Release 连续 25/25、ASan/UBSan 与 TSan 各 2/2 及最终全量 95/95 通过，见[同步修复记录](rpc_v2_completion_marker_20261029.md)。TLS-E、服务目标/类型契约/后端决策及发布总验收仍未完成。

前次接续候选：2026-10-29，`codex/release-readiness-20261029` 从原本地集成提交 `02c78a50ecfe38852e9ddb327f2cf49c4a460936` 恢复，版本为 `2.0.0-dev.173`，已推送到 [Draft PR #333](https://github.com/MyBlackHole/c-trpc/pull/333)，base 为 #332 实现分支。dev.173 为 A→B→A 增加八个真实 mTLS/压缩/子调用时机组合；GCC Debug/Release、Clang Release、ASan/UBSan、TSan 全量各 94/94，身份丢失变异验证通过。实现提交 `dfddf763b83d94742afa8d63816c321a7b5bac0d` 的 GitHub 检查 13/13 全部成功；文档接续提交另行核对精确 head。详细证据见[组合记录](rpc_v2_nested_tls_compression_20261029.md)。这些是组合候选证据，不是完整 TEST-D/E 或发布验收；下面保留历史分支与阶段记录，不把旧 head 的绿色检查当作当前整体验收。

历史阶段记录（以下保留当时的 head 与验收状态）：

核对基线：2026-10-27，V2 当前本地候选为 `2.0.0-dev.172`；#243 堆叠 PR #328 → #329 → #330 均保持 OPEN，异步操作内核 Draft PR #332 依赖设计 PR #331。PR #330 的 dev.157 head `c7fe157c24a87bb461725539884d8f71bd982907` 与 dev.158 head `a8b0f8aa31c4569005e6a404015a81cee2c55074` 均有 13/13 GitHub 检查通过；dev.158 增补共享 TaskExecutor 队列满载的 dispatch 重试回归。dev.159 实现异步操作内核及 RPC authorize、TLS provider、MessageService 接入；dev.160 修复 GCC 11 构建告警与 Clang 14 dispatch 构造清理泄漏；dev.161 修复 TLS provider reserve 解锁失败回滚；dev.162 修复 GCC 11 TSan 编译告警并局部标注 analyzer 的 table-owned 结果缓冲误报，其 PR #332 head `d245e0b4bf0fec52e8d4e1db69613a597e35cb89` 检查 13/13 通过。dev.163 CallRef × 异步 authorize 真实 TCP 回归的远端检查 13/13 通过；dev.164 增加 destroy/release 屏障并在 PR #332 精确 head `6c0a3808722ea0cc8da7d2c1e9d73878327aedce` 上通过 13/13 检查；dev.165 增补 terminal callback 内 hold 的回归，本地 GCC Release 全量测试 93/93 且定向 sanitizer 通过，GitHub 精确 head 检查 13/13 通过；dev.166 增加 A→B→A 异步嵌套调用，本地 GCC Release 全量测试 94/94、TSan/ASan/UBSan 与 clang-format 18.1.8 通过；PR #332 精确 head `d7c7d3cd60d5c89cad2255809e08191171461d00` 的 GitHub CI 与 RPC smoke 检查 13/13 全部通过（[CI](https://github.com/MyBlackHole/c-trpc/actions/runs/40422545513)、[RPC smoke](https://github.com/MyBlackHole/c-trpc/actions/runs/40422545567)）。dev.168 的 TLS Client 凭证屏障 PR head `f1bc6b4e2d139c1d079863afe22aa5ecdca47c53` 也已通过 13/13 检查（[CI](https://github.com/MyBlackHole/c-trpc/actions/runs/40426497557)、[RPC smoke](https://github.com/MyBlackHole/c-trpc/actions/runs/40426497621)）。主线 TLS-C merge commit `0fa90817c15286485ee2e1cfe8372bb8ab8a8e5a`；TLS-D1 的后续状态仍以 #234 当前记录为准。
GitHub 总账：[架构总审查 #239](https://github.com/MyBlackHole/c-trpc/issues/239)。这里记录开发顺序与证据入口，不以单个 PR 或旧 Issue 勾选框代替整体验收。

## 2026-10-27 本地集成候选（当时未推送）

`codex/integration-v2-test-d-20261027` 在本地组合 PR #332 head `19007d696adb9626acabbb4ceeffb3973d6050c6`、PR #323 最新 head `42684b77637dcdd476e3fb87897d7d6529fbafdd` 与 PR #312 head `d9c73b2758fc9dca8e0b5ce82a23a204100516a8`；集成提交为 `6e2f8bf` 与 `5445f26`。候选未推送，也未改动任何远程分支或 PR；版本为 `2.0.0-dev.172`。dev.170 基线曾在 GCC Release、GCC Debug、Clang Release、GCC ASan/UBSan、GCC TSan 下完成全量测试 94/94；dev.171 新增监听器轮换回归后，GCC Release 全量 94/94，新增两目标的 ASan/UBSan（含泄漏检查）和 TSan 通过。TEST-A/错误分类模型、组件回放与固定 64-seed trace explorer、V1 退役 inventory、工具目录、格式工具自测及变更源码格式检查通过；Linux x86_64 的 GCC/Clang 编译数据库各核验 54 个生产源文件。该历史矩阵证明 dev.170 组合基线在当前 Linux x86_64 环境通过这些配置，但没有验证 dev.171 的组合提交远端 CI，也不构成 TEST-D 所要求的完整真实集成/平台矩阵。40k 墓碑容量回归通过，但它不是 TEST-E 性能验收；开环重复性工具现在支持 TLS/压缩并保留发生器、Client、Server 账本与资源记录。本机同一二进制的明文和 TLS+Zstd 40k 对照均为 0/9 干净样本，CONTROL TX 池压力、发生器丢弃和迟到均在九个样本中复现；这些是本地诊断数据，不是通过的容量结论。TEST-E 仍需在约定基准机、固定预算和平台矩阵下完成分层归因与验收。#243 的调用方屏障与监听器凭证轮换已有 dev.171 本地回归，但其余调用方生命周期、共享调度边界以及 #234 的 TLS 总资源、持续负载和互通边界也未全部验收。故此候选仍是本地集成诊断候选，不能视为 V2 发布候选或总体验收完成。

dev.166：Server V2 `work` worker 可通过 `tr_client_call_start()` 提交子 Call；真实 TCP A→B→A 回归验证父上下文与异步回复、Client/Server 唯一终局及 drain/destroy 回收。目标 Release 连续 25 次、ASan/UBSan、TSan 通过，GCC Release 全量测试 94/94；公开头文件 C11/C++11 编译通过。详细证据见[嵌套 work 调用记录](rpc_v2_nested_work_calls_20261027.md)。该记录只关闭一条嵌套调用路径，command/completion 满载、其他交错与 TLS/压缩共享调度仍未完成；提交 `d7c7d3cd60d5c89cad2255809e08191171461d00` 的 PR #332 精确 head 检查 13/13 通过（[CI](https://github.com/MyBlackHole/c-trpc/actions/runs/40422545513)、[RPC smoke](https://github.com/MyBlackHole/c-trpc/actions/runs/40422545567)）。

dev.167：扩展 Reactor completion 队列满载回归，将真实 `tr_async_operation` 生命周期贯穿“队列满→producer 等待→owner 消费并退役”及“队列关闭→交接拒绝→owner/producer 静默→stopped-reap”两条路径。Release、TSan、ASan/UBSan 定向目标通过，结果 payload 恰好析构一次。提交 `a69da1c1767a357688a284e3f284aa9359d21c38` 的 PR #332 精确 head 检查 13/13 通过（[CI](https://github.com/MyBlackHole/c-trpc/actions/runs/40423841507)、[RPC smoke](https://github.com/MyBlackHole/c-trpc/actions/runs/40423841483)）。详细证据见[异步操作完成队列背压记录](rpc_v2_operation_completion_backpressure_20261027.md)。只补了通用内核与 Reactor 完成队列的组合证据；RPC 各调用方屏障矩阵、TLS/压缩共享调度及 V2 总体验收仍未完成，PR #332 保持 Draft。
dev.168：真实 TLS Client 凭证回归覆盖 start-active 期间外部 cancel 与 provider fetch 交错：poll 返回 TR_AGAIN 且保留输出；fetch 返回后 provider cancel 才运行一次；取消后的迟到成功完成仍以 CANCELLED 终结，操作退役后 Client destroy 成功。GCC Release 全量测试 94/94、定向目标 Release 连续 25/25、TSan、ASan/UBSan 通过；临时错误顺序变异会被回归捕获。证据见[TLS provider fetch/cancel 回调屏障回归](rpc_v2_tls_provider_fetch_cancel_barrier_20261027.md)。此切片只关闭 TLS Client 的一条取消交错，Server/listener provider 屏障和 #243/V2 总体验收仍待完成；dev.168 远端检查待推送复验。

dev.169：为 Server TLS 凭证 API 补同一 fetch/cancel 返回屏障：fetch 活动期间 poll 返回 TR_AGAIN 且保留输出，外部 cancel 不触发 provider 回调；释放 fetch 后 provider cancel 恰好一次，迟到成功仍被 CANCELLED 吸收，Server 退役后可销毁。GCC Release 全量测试 94/94，目标 Release 连续 25/25、TSan、ASan/UBSan 均通过；临时反转生产 `start_active` 屏障的变异在新断言处失败，生产文件未改。详细证据见[Server TLS provider fetch/cancel 回调屏障回归](rpc_v2_server_tls_provider_fetch_cancel_barrier_20261027.md)。dev.169 精确 head 的远端检查待本次推送复验；listener 侧握手/凭证替换以及 #243/V2 总体验收仍未完成。

dev.170：提交 `0633ff97703e8a3b85e48801b11c984c7800016e` 为开环重复性运行器加入 TLS/压缩开关，并在 bench 清理时重试公开契约允许的 Client `TR_AGAIN`。GCC Release `bench_rpc` 构建通过；RPC benchmark 单测 71/71、源码格式门禁自测 22/22 通过，新增 C helper 的 clang-format 18.1.8 changed-line 检查通过。真实 TLS 1.3 + Zstd 低负载 3000 RPS 三个公开 profile 均为 1/1 干净；每条 Server/Client 命令均记录并使用 `--tls 1 --compression zstd`。该输出记录源码提交 `0633ff9`、二进制 SHA-256 `5e6012e582e38c937f9e499e26694e009eb6e0f0506cc298a2aeeb3f96ea2170`。

同一源码提交、二进制、AMD Ryzen 7 PRO 4750U 主机和 CPU 0–3 亲和性下，40k 开环时间表分别运行明文/无压缩与 TLS+Zstd，每种配置 3 trials × 3 profiles。该 affinity 是 4 个逻辑线程、仅覆盖 2 个物理核。所有 profile 都是 0/3 干净；CONTROL TX 池耗尽、发生器丢弃和调度迟到在每种配置的 9/9 样本复现。下表中的成功 `ok_rps` 只是受压本机观察值，不是稳定吞吐量；由于发生器丢弃和迟到，两组不能据此解释为 TLS/Zstd 的性能开销或发布容量结论。

| 传输配置 | profile | 中位成功 RPC/s | 中位发生器丢弃 | 调度迟到 P99 中位数 |
|---|---|---:|---:|---:|
| 明文 / none | small | 5,255.8 | 7,829 | 8.6 ms |
| 明文 / none | default | 6,678.9 | 7,817 | 6.7 ms |
| 明文 / none | large | 6,619.2 | 7,793 | 5.8 ms |
| TLS / Zstd | small | 1,166.7 | 8,967 | 79.4 ms |
| TLS / Zstd | default | 1,518.8 | 8,953 | 105.1 ms |
| TLS / Zstd | large | 1,355.6 | 8,974 | 130.1 ms |

为区分传输配置和本机发生器/服务压力，在相同主机、源码 head `6354f0a`、dev.170 二进制 SHA-256、CPU 0–3 亲和性（2 个物理核）下，再对 3k/5k/10k RPS、250 ms 时间窗运行明文/none 与 TLS/Zstd，每个配置均为 3 trials × 3 profiles。逐条核对 JSONL 中 Server 和每个 Client 命令的 `--tls`、`--compression` 参数，9 组输出均与 metadata 一致。结果如下；“clean”按运行器的全部发生器/RPC/Server 信号判定：

| 速率 | 明文 / none clean | TLS / Zstd clean | Server 压力 | 发生器信号 |
|---:|---:|---:|---|---|
| 3k | 9/9 | 5/9 | 两配置均 0/9 | 明文 0/9；TLS/Zstd 4/9 迟到，无丢弃 |
| 5k | 2/9 | 0/9 | 两配置均 0/9 | 明文 7/9 迟到；TLS/Zstd 9/9 丢弃且迟到 |
| 10k | 0/9 | 0/9 | 两配置均 9/9 CONTROL TX 池耗尽 | 两配置均 9/9 丢弃且迟到 |

3k 时两种配置的成功速率中位数接近；5k 的 TLS/Zstd 小型 profile 另有 3/3 次 RPC `RESOURCE_EXHAUSTED`。这些 5k/10k 数据已受到发生器信号影响，10k 还同时触发 Server 压力，故不据此计算 TLS/Zstd 开销或宣布容量。当前主机只证明低负载 TLS/Zstd 参数能端到端传递且 Server 无压力；TEST-E 的固定验收硬件、预算线和性能结论仍待补齐。2 物理核结果的原始 JSONL（`3k-250ms-plain-v170.jsonl`、`3k-250ms-tls-zstd-v170.jsonl`、`5k-250ms-plain-v170.jsonl`、`5k-250ms-tls-zstd-v170.jsonl`、`10k-250ms-plain-v170.jsonl`、`10k-250ms-tls-zstd-v170.jsonl`）和临时凭证留在忽略目录 `build/rpc-bench-test/`、`build/test-tmp/`，不进入版本库。

发现 CPU 0–3 只覆盖 2 个物理核后，在同一主机上又以 `0,2,4,6,8,10,12,14`（每个物理核一个 SMT 线程）运行 TLS/Zstd，仍使用 250 ms 时间窗和 3 trials × 3 profiles：

| 速率 | clean | Server 压力 | 发生器/RPC 信号 |
|---:|---:|---:|---|
| 3k | 6/9 | 0/9 | 3/9 调度迟到，无丢弃 |
| 5k | 0/9 | 0/9 | 8/9 丢弃、9/9 迟到；small profile 3/3 `RESOURCE_EXHAUSTED` |
| 10k | 0/9 | 9/9 CONTROL TX 池耗尽 | 9/9 丢弃且迟到 |

相比 2 物理核，8 物理核的 10k 成功速率更高，但未消除发生器和 Server 信号。该扩核数据没有明文对照，也仍是短窗口诊断；不构成性能对比或 TEST-E 容量验收。相关原始 JSONL（`3k-250ms-tls-zstd-8physical-v170.jsonl`、`5k-250ms-tls-zstd-8physical-v170.jsonl`、`10k-250ms-tls-zstd-8physical-v170.jsonl`）及凭证均留在忽略目录，未纳入提交。

原始 JSONL 仅保存在本机忽略目录 `build/rpc-bench-test/40k-none-v170.jsonl`、`build/rpc-bench-test/40k-tls-zstd-v170.jsonl` 和 `build/rpc-bench-test/3k-tls-zstd-v170.jsonl`；临时 CA 与私钥留在 `build/test-tmp/`，均未加入仓库。本切片只增加诊断运行器能力，不改 RPC 运行时、公开 API/ABI 或 wire；TEST-E 仍需按固定基准机、预算和平台矩阵验收。

dev.171 为 #243 增加真实网络 listener profile 轮换回归，分别覆盖普通 RPC TLS listener 和 connection-group pipeline TLS listener：provider 初装 CA A 后可完成握手；graceful 轮换到 CA B 后，只信任 CA B 的新客户端握手通过，已接纳的旧代 RPC 或 CONTROL/DATA 会话仍完成原有工作。pipeline 测试遵循现有 generation fence，不把新代 DATA 并入旧代 CONTROL。GCC Release 全量 94/94、两个目标 Release、ASan/UBSan（泄漏检查）和 TSan 通过；临时清空 listener 轮换 profile 的变异被新 CA 握手断言捕获，随后已恢复生产源码。只增加测试、报告与版本号，不改运行时/API/ABI/wire。该切片只证明 provider profile 安装进入两条监听器握手路径，不覆盖调用方全生命周期矩阵、握手洪峰或 TLS-E 资源/平台验收；详见[Server TLS provider 监听器凭证轮换回归](rpc_v2_server_tls_provider_listener_rotation_20261027.md)。

dev.172：在 `test_rpc_v2_sdk` 增加真实 TCP 压缩请求取消与 Client destroy 屏障回归。测试链接期包装变换执行入口，确定性暂停已接受的 Zstd 变换任务；CallRef 取消先选定 CANCELLED，活动期结果仍返回 TR_AGAIN 且不改输出。释放引用后 destroy 仍返回 TR_AGAIN；解除门控并重试销毁后，stopped-reap 只交付一次 CANCELLED/SHUTDOWN，服务端只收到 OPEN、未收到压缩请求消息，并以 UNAVAILABLE 终结。GCC Release 全量测试 94/94（包含新增目标）、ASan/UBSan（含泄漏检查）、TSan 定向目标及 GCC/Clang C11、G++/Clang++ C++11 SDK 消费端通过。只修改测试、链接配置、报告和版本号；#243 其余调用方屏障、共享调度及 V2 整体验收仍未完成。

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
并接入现有 GCC 功能测试作业。原 PR #317 已作为前序 PR 关闭，其实现纳入当前汇总 PR #321；
TEST-A 与 TEST-B1–B4 的合并验收以 #321 当前 head `2abfd8fa3b37bc2c267b1a02f4ef8151cdd2cc63`
为准，Draft、14/14 GitHub 检查通过。它只建立 L1 oracle 和有限真实 Endpoint 轨迹；TEST-B
完整事件字母表/真实派发、TEST-C 通用生产 C 故障时序、TEST-D 集成与平台矩阵、TEST-E 诊断和
容量性能仍未完成，#247 与整体 V2 验收继续保持未完成。

dev.29 的 TEST-B1 在同一 TEST-A 轨迹上驱动真实 RPC Record Endpoint 测试夹具，检查取消后
迟到工作、重复终局回调和账本归零；Reactor、Channel、Dispatch 与 Executor 仍为受控替身，
未覆盖 TCP/真实派发调度。当前本地 GCC Debug/Release 均为 90/90，组件夹具为 35/35；
原 PR #318 已收敛至当前汇总 PR #321，远端完整检查通过；完整边界与本地/远端证据见
[TEST-B1 记录](rpc_v2_component_replay_20261024.md)。

dev.30 的 TEST-B2 在 TEST-B1 上增加 Deadline 终局后迟到工作的组件轨迹，验证 TIMEOUT RESET、
唯一终局和预算归还。它仍是受控下层交错，不代表真实 Dispatch 调度或 TCP 集成。本地 GCC
Debug/Release 全量各 90/90，模型 28/28、组件映射 9/9、Endpoint 夹具 35/35；
原 PR #319 已收敛至当前汇总 PR #321，远端完整检查通过；
证据见[TEST-B2 记录](rpc_v2_deadline_component_replay_20261024.md)。

dev.31 的 TEST-B3 增加同一 Endpoint 槽复用后旧代次 DATA 迟到轨迹。真实 Endpoint 必须
忽略旧流句柄的数据回调，不能增加新调用接收次数；新旧两次调用各自只报告一次取消终局并
退役，账本为零。当前模型 28/28、组件映射 10/10，GCC Debug/Release 全量各 90/90，
Endpoint 夹具 35/35，三条 Release L1/L2 回放均匹配；原 PR #320 已收敛至当前汇总 PR #321，
远端完整检查通过。证据见
[TEST-B3 记录](rpc_v2_stale_generation_component_replay_20261024.md)。

dev.32 的 TEST-B4 增加成功 FINAL 已接受后收到 RESET 的组件轨迹。它检查 Endpoint 保留
先到的 OK 终局、不增加重复回调，并在 RESET 后退役与归还预算。当前模型 28/28、组件映射
11/11，GCC Debug/Release 全量各 90/90、Endpoint 夹具各 35/35，四条 B1–B4 回放在两种
模式均匹配；当前 Draft 汇总 PR #321 将 TEST-A 与 TEST-B1–B4 合并审查，14/14 GitHub 检查通过，
仍待合并。证据见
[TEST-B4 记录](rpc_v2_final_reset_component_replay_20261024.md)。

dev.33 的 TEST-C1 增加固定 seed 模型探索、失败签名与确定性事件缩减，并扩展既有
`build-test (gcc)` 检查和失败工件归档。固定 64-seed 测试集覆盖 schema v1 的 17 种操作，
生成轨迹包含 APPLIED、STALE、REJECTED、RESOURCE_LIMIT；测试专用重复终局回调 mutant
能缩减到 OPEN/CANCEL。它只验证 L1 模型工具链，不是生产 C 故障注入。原 PR #322 已关闭并由
汇总 PR #323 取代。其 Make/install 首轮失败已查明是 SDK 测试把终局销毁首次调用误设为必然
`TR_OK`；生命周期合同允许 `TR_AGAIN`，测试现使用有界重试。初始失败证据保留在 CI 记录中，
不再作为未解释的实现失败。
详细证据见 [TEST-C1 记录](rpc_v2_test_c1_explorer_20261025.md)。

dev.34 的 TEST-C2 增加一条固定 Endpoint 最终错误状态发送背压回放：模型 `ERROR` 映射到
夹具 `UNIMPLEMENTED`，真实 Endpoint 的 `tr_stream_sendv` 首次返回一次 `TR_AGAIN`、第二次
接受唯一最终帧；组件观察与 L1 在终局次数、工作完成、最终帧、槽退役及零预算上匹配。
该测试仍使用受控 Channel/Dispatch/Executor，不覆盖 TCP、通用故障日程或 TEST-C 完整要求；
当前 Draft 汇总 PR #323 收纳 TEST-C1–C3，head 为 `a8cf248a27a0c60d1df36f003d4b55fdcf5eabac`，
13/13 GitHub 检查通过，仍待合并。父 PR #322 的首次 Make/install 失败与修复证据均保留。
详见 [TEST-C2 记录](rpc_v2_test_c2_endpoint_send_backpressure_20261025.md)。

dev.35 的 TEST-C3 在同一受控 Endpoint 夹具中覆盖相邻竞争窗口：错误最终帧首次发送返回
`TR_AGAIN` 后、Endpoint 下一次发送重试前收到远端 RESET。组件必须不再尝试发送、不报告已接受
最终帧，并在有界轮转后只交付一次终局、退役槽且归还全部预算。新增/修改源码行格式 CI 门禁按
用户要求移除；格式工具和翻译目录回归并入既有 GCC 构建作业，本地改动行检查保留。
此前 PR #323 中间 head `db4070c381c87dac0af0a5440ff9d9627bf36a40` 的格式作业因 GitHub 10 分钟超时
且零步骤执行而取消；同一 head 的本地格式检查为 1 个源码文件、0 处格式错误。该远端结果不代表
源码格式检查失败。C3 head `a858dd21fb5c3225cb4bf8b53aa5edc9ba0953d6` 的既有 CI 与 RPC smoke 共 13/13
通过；GCC 全量 Debug/Release 各 90/90，两个收尾进程快照均未发现项目子进程，旧 runner cleanup 超时未复现。
详见
[TEST-C3 记录](rpc_v2_test_c3_endpoint_reset_before_retry_20261025.md)。

dev.36 修复压缩 DATA 接收路径漏检 DATA 专属标志的问题，并补充真实 Client/Server
用例验证 GOAWAY 期间已接纳 CALL_OPEN 的 `UNAVAILABLE` 不代表“肯定未执行”、客户端不
自动重放。活动流收到额外 `LANE_BULK` 标志时只 RESET 目标流；RESET 丢弃态收到同类坏帧
时关闭对应物理连接，不影响 split 模式另一物理连接上的流。#242 其余错误分类、连接组
边界和组合故障仍待完成。详见[压缩 DATA 标志隔离记录](rpc_v2_compressed_data_flag_validation_20261025.md)
及[GOAWAY 未知结果记录](rpc_v2_goaway_unknown_outcome_20261025.md)。

dev.37 修正 GOAWAY 测试清理只允许 `tr_client_destroy()` 首次返回 `TR_OK` 的错误假设：
按公开对象所有权合同，在 `WAIT_MS` 总期限内重试 `TR_AGAIN`，其余错误仍立即失败。该变更只影响
测试夹具，不改变运行时路径。GCC Release 全量 90/90，ASan/UBSan 下 `test_rpc_v2_sdk` 连续
5/5；新 head 的 GitHub 检查仍待完成。#242 的连接组接受边界、其他错误分类和组合故障未完成。
细节见[GOAWAY 未知结果记录](rpc_v2_goaway_unknown_outcome_20261025.md)。

dev.38 修正 Clang Release 暴露的 Client call 槽异步退休测试假设：弱引用释放后，旧传输流
退休前可能暂时占满容量；stale-token 复用用例现对合法 `TR_AGAIN` 做有界 owner pump/retry，
并验证输出句柄保持不变、旧 token 仍失效。Clang Release 下 `test_client_record` 连续 10/10；
全量 90/90，格式化后的目标复验 1/1；GCC ASan/UBSan 下该目标 5/5。Draft PR #324 的 dev.38
head `6f60dbbeb87f5036e36447fe39b1ade9792b1f9f` 全部 GitHub 检查通过。该改动只影响测试夹具，
V2 的连接组接受边界、其他错误分类和组合故障仍待完成。

dev.39 增加 #242/ERR-A 的错误分类表和不可变参考状态转换：同一单流语义错误仅在帧边界、
流标识及 RESET 能力都可信时重置目标流；否则关闭连接并让每个活动调用恰好终结一次。业务拒绝
与本地资源压力分别终结目标调用，保留同连接其他流；TLS 完整性、协商、帧边界和未知私有状态损坏
默认关闭连接。新增六项 Python 回归并接入已有 GCC 模型自测步骤；这是 L1 设计参考，不证明
生产 C 行为符合该矩阵。ERR-B 标识/RESET 时序、ERR-C 信用与组合故障、ERR-D 多成员排空边界仍待验收；
ERR-E 已验证墓碑固定预算、真实 Channel 长周转与 FIFO 淘汰语义，也有 256 路确定性碰撞簇压力回归；最大容量下的碰撞成本、更高频负载和更广组合矩阵仍待实现和验收。详见
[ERR-A 分类与模型记录](rpc_v2_error_scope_model_20261025.md)。

dev.40 补上 #242/ERR-B 的本地流 ID 耗尽分配边界：客户端奇数与服务端偶数最后一个
`uint32_t` ID 各只分配一次，之后保持耗尽状态、不回绕；`tr_stream_open()` 在观察到
耗尽后释放锁并启动 Channel GOAWAY 排空。纯分配边界回归与现有 Channel 排空套件通过，
但没有运行时测试将真实 Channel 计数器注入上限，故只登记为部分实现。RESET FIFO、
tombstone/连接代次组合以及 ERR-B 全矩阵仍待验收。详见
[本地流 ID 耗尽记录](rpc_v2_stream_id_exhaustion_20261025.md)。

dev.41 为同一边界补上真实 Channel 集成回归：GNU ld wrapper 仅在测试链接中把活动
Channel 的下一 ID 游标置为耗尽，不改变生产代码开关；验证 `tr_stream_open()` 返回
`CLOSED`、真实对端收到 GOAWAY，Channel 从 DRAINING 转为 DRAINED。Release 下
`test_stream_reset` 和 `test_channel_drain` 均通过。本地流 ID 耗尽到排空的集成路径已有
直接回归，但 ERR-B 的 RESET/FIFO/代次组合与 #242 其余验收仍未完成。

dev.42 修复上述新增 helper 漏入手工列举源码的 TCP Endpoint 测试目标，避免全量 CI 在
ThreadSanitizer/独立组件链接阶段出现未定义引用。本地 TSan 下 `test_rpc_record_endpoint_tcp`
与 `test_stream_reset` 构建、运行通过；dev.42 新 head 的 GitHub 检查全部通过。

dev.43 为 #242/ERR-A 补上真实 RPC Record Stream 配额失败隔离回归：两条调用共用同一
Channel，第一条在接收不完整 RAW MESSAGE 时因服务器硬预算不足而本地终结；测试再模拟上层
将该 RESOURCE_EXHAUSTED 映射为 `TR_ERR_NOMEM` RESET，验证对端终局只发生一次。释放预算后，
第二条调用仍可完成，缓存归零。Release `test_rpc_record_stream` 全目标与 dev.43 GitHub 检查通过。该切片没有
覆盖压缩缓冲、信用争用及连接级组合故障；ERR-A 的完整生产路径对照和 ERR-B/C/D/E 仍待验收。

dev.44 为 #242/ERR-B 增加 RESET 墓碑的物理连接代次回归：在旧连接完成双向 RESET 并保留
墓碑后，关闭连接、复用同一 Reactor 连接槽为新代次、替换 Channel 并完成新 HELLO。新连接
收到旧流 ID/原因的 RESET 时必须因未知流触发协议错误，不能被旧代次墓碑吸收。该场景与既有
墓碑容量淘汰、Stream 槽复用测试互补。Release 与 TSan `test_stream_reset` 全目标通过；
不覆盖拆分连接或多次快速重连，ERR-B 其他组合及 ERR-C/D/E 仍待验收。

dev.45 为 #242/ERR-B 增加 split Channel 的 RESET 墓碑物理连接身份回归：CONTROL 上已完成的
双向 RESET 墓碑不能吸收 BULK 物理连接上相同流 ID/原因的 RESET；BULK 必须因未知流关闭，
CONTROL 保持 UP 并继续接纳、交付和终止新流。Release 与 TSan `test_stream_reset` 全目标通过。
本切片仅补测试；其他 RESET/FIFO 交错与 ERR-C/D/E 仍待验收。详见
[split 连接 RESET 墓碑回归](rpc_v2_reset_tombstone_split_connection_20261025.md)。

dev.46 为 #242/ERR-C 增加 RESET 与迟到载荷释放组合回归：双向 RESET 完成后释放旧流已交付载荷，
后续 PING 必须直接得到 PONG，不能先收到退役流的 WINDOW_UPDATE；同 Channel 的另一条流仍能收发并正常
完成 RESET。Release 与 TSan `test_stream_reset` 全目标通过。本切片不覆盖信用债务跨消息、压缩预算与
多流并发故障的 ERR-C 全矩阵；其他 ERR-A/B/D/E 项仍待验收。详见
[RESET 后迟到载荷释放与信用记录](rpc_v2_reset_late_payload_credit_20261025.md)。

dev.127 为 #242/ERR-C 增加 RESETTING 窗口中的已交付载荷释放回归：测试阻塞 Reactor owner 的 RESET
回调，在槽位退役前由应用线程释放正文，检查接收信用不增加、目标流无迟到 WINDOW_UPDATE、同伴流仍可
完成。Release（40k tombstone）、TSan（5k tombstone）及 ASan/UBSan（40k tombstone）完整
`test_stream_reset` 通过；移除生产信用停止保护的临时变异会被新断言捕获。本切片仅增加测试和记录，
不改变生产代码、API/ABI 或线协议；ERR-C 其他信用边界及 ERR-A/B/D/E 仍待验收。详见
[RESETTING 期间释放载荷的信用隔离回归](rpc_v2_reset_release_during_reset_20261026.md)。

dev.128 为 #242/ERR-C 增加跨消息信用债务回归：将协商的消息上限设为 128 字节、窗口设为 256 字节，
前两条 64 字节消息累积到 WINDOW_UPDATE 阈值并正确将通告上限推进至 384；随后一条 128 字节载荷在
RESETTING 窗口由应用释放，目标流仍不得额外获得窗口信用或产生迟到 WINDOW_UPDATE，同伴流继续完成。
Release（40k tombstone）、TSan（5k）和 ASan/UBSan（40k）的完整 `test_stream_reset` 均通过；移除
信用停止保护的临时变异会被该新用例自身捕获。本切片仅增加测试与记录，不改生产代码、API/ABI 或 wire；
ERR-C 其他信用边界及 ERR-A/B/D/E 仍待验收。详见
[RESET 时跨消息信用债务隔离回归](rpc_v2_reset_cross_message_credit_debt_20261026.md)。

dev.129 为 #242/ERR-C 增加真实 Zstandard 接收信用回归：Channel 先按压缩 DATA wire 字节计入接收与消费，
应用持有解压正文期间目标流进入 RESETTING；应用释放正文时接收量、消费量、通告上限及
WINDOW_UPDATE 发送计数均不得变化。测试解除 RESET 回调闸门并完成流退役后，还验证同 Channel 压缩同伴
继续收发。Release、TSan、ASan/UBSan 的完整 `test_transport` 均通过；临时取消 RESETTING 消费字节上限的
变异会在消费量不变量处失败。本切片仅增加测试与记录，不改生产代码、API/ABI 或 wire；ERR-C 其余信用
边界及 ERR-A/B/D/E 仍待验收。详见
[RESETTING 压缩解码正文信用隔离回归](rpc_v2_reset_compressed_payload_credit_20261026.md)。

dev.130 为 #242/ERR-C 增加同一 RESETTING 流上的并发多载荷释放回归：两条 64 字节消息均由应用持有，
远端 RESET 进入 RESETTING 且 owner 回调暂停后，两个应用线程并发释放正文。测试验证接收量与消费量最终
均为 128、通告上限保持 256、Channel `window_updates_tx` 前后不变、wire 上没有迟到 WINDOW_UPDATE；
恢复 owner 后健康同伴仍能交付并完成。移除生产信用停止保护的临时变异由新用例在通告上限断言捕获，
生产源码已恢复。GCC Release/ASan/UBSan 使用 40k 墓碑、TSan 使用 5k 墓碑的完整 `test_stream_reset`
均通过。本切片只扩展测试夹具和记录，不改生产代码、API/ABI 或 wire；不代表多流并发 RESET 或 ERR-C
全组合验收完成。详见
[RESETTING 并发载荷释放信用隔离回归](rpc_v2_reset_concurrent_payload_credit_20261026.md)。

dev.131 为 #242/ERR-C 增加两个同时 RESETTING 的流各自释放持有正文回归：每条流先接收并持有一条
128 字节重组消息，再分别本地 RESET；对端暂不确认，确保两个 RESETTING 槽同时保留。两个应用线程并发释放后，
每条流接收/消费量均为 128、通告上限仍为 256，Channel `window_updates_tx` 不变，wire 屏障未观察到目标流
WINDOW_UPDATE。两条 RESET 完成后，健康同伴仍能交付 DATA 并正常结束。GCC Release/ASan/UBSan 使用 40k 墓碑、
TSan 使用 5k 墓碑的完整 `test_stream_reset` 均通过。本切片只增加回归测试、记录和版本号，不改生产代码、API/ABI
或 wire；ERR-C 其他信用组合及 ERR-A/B/D/E 仍未完成。详见
[双 RESETTING 流并发载荷信用隔离回归](rpc_v2_reset_two_stream_concurrent_payload_credit_20261026.md)。

dev.132 扩展双 RESETTING 流场景为在途 DATA 与应用载荷释放组合：每条流已有 128 字节正文由应用持有，
分别本地 RESET 且对端确认暂缓期间，再各接收一条合法的 64 字节在途 DATA。Channel 丢弃这些晚到 DATA、
将 wire 接收与消费计数各增加 64，但不调用应用回调；随后两个应用线程并发释放原正文。每条流最终接收/消费
均为 192、通告上限仍为 256，`window_updates_tx` 不增长，RESET 完成和 PING/PONG 屏障均无目标流
WINDOW_UPDATE。完整 GCC Release/ASan/UBSan（40k 墓碑）及 TSan（5k）`test_stream_reset` 均通过。
本切片只增加测试、记录和版本号，不改生产代码、API/ABI 或 wire；ERR-C 其他信用组合及 ERR-A/B/D/E
仍未完成。详见[双 RESETTING 流在途 DATA 与并发释放回归](rpc_v2_reset_two_stream_reset_discard_credit_20261026.md)。

dev.133 将双 RESETTING 流收到的在途 DATA 从 64 字节扩展到填满冻结窗口剩余 128 字节：每条流接收量正好
到达通告上限 256，RESET discard 消费量为 128；并发释放原持有的 128 字节后消费量到 256，通告上限不变，
`window_updates_tx` 不增长且 wire 无迟到 WINDOW_UPDATE。测试保留 0/64/128 字节三档变体，完整 GCC Release/
ASan/UBSan（40k 墓碑）与 TSan（5k）`test_stream_reset` 均通过。本切片只扩展测试、记录和版本号，不改生产代码、
API/ABI 或 wire；ERR-C 其他信用组合及 ERR-A/B/D/E 仍未完成。详见[双 RESETTING 流冻结窗口边界回归]
(rpc_v2_reset_frozen_window_two_streams_20261026.md)。

dev.134 在 dev.133 的双 RESETTING 流精确填满冻结信用并并发释放后，向其中一条流再发送 1 字节 DATA，验证
超出通告上限后连接以 `TR_ERR_BAD_LENGTH` 关闭；没有额外交付应用正文或 WINDOW_UPDATE。非超限变体仍覆盖健康
同伴正常完成。完整 GCC Release/ASan/UBSan（40k 墓碑）与 TSan（5k）`test_stream_reset` 均通过。本切片只扩展
测试、记录和版本号，不改生产代码、API/ABI 或 wire；ERR-C 其他信用组合及 ERR-A/B/D/E 仍未完成。详见
[双 RESETTING 流冻结信用超限拒绝回归](rpc_v2_reset_two_resetting_stream_credit_overflow_20261026.md)。

dev.135 将冻结信用超限拒绝扩展到 Split Channel：CONTROL 物理连接上的 RESETTING 流先收到恰好填满窗口的
256 字节 discard DATA，再收 1 字节后仅该物理连接以 `TR_ERR_BAD_LENGTH` 关闭；BULK 独立连接上的健康流仍能
交付 DATA 并完成 RESET。完整 GCC Release/ASan/UBSan（40k 墓碑）与 TSan（5k）`test_stream_reset` 均通过。
本切片只增加测试、记录和版本号，不改生产代码、API/ABI 或 wire；ERR-C 其他信用组合及 ERR-A/B/D/E 仍未完成。
详见[Split Channel RESET 信用超限与物理连接隔离回归]
(rpc_v2_reset_split_overflow_connection_isolation_20261026.md)。

dev.136 将 Split Channel 冻结信用超限回归参数化为 CONTROL、BULK 两种出错方向：每次目标连接收到 256 字节
合法 discard DATA 后再收 1 字节，只该物理连接以 `TR_ERR_BAD_LENGTH` 关闭；另一 lane 的健康流仍可交付 DATA、
通过 PING/PONG 并完成 RESET。完整 GCC Release/ASan/UBSan（40k 墓碑）与 TSan（5k）`test_stream_reset` 均通过。
本切片只扩展测试、记录、计划和版本号，不改生产代码、API/ABI 或 wire；ERR-C 其他信用组合及 ERR-A/B/D/E
仍未完成。详见[Split Channel RESET 信用超限双向隔离回归]
(rpc_v2_reset_split_overflow_bidirectional_20261026.md)。

dev.137 为 #242/ERR-C 补上 RESETTING 阶段接收合法在途 `WINDOW_UPDATE` 的边界：远端将绝对发送上限从 256 增至 320 后，Channel 记录该合法更新，但不发出 WRITABLE 事件，`tr_stream_send()` 仍返回 `TR_ERR_CLOSED`；随后完成 RESET，并验证同连接健康流仍可交付 DATA、完成 RESET，且没有连接级错误。GCC Release、TSan、ASan/UBSan 完整 `test_stream_reset` 均通过。本切片只增加测试、记录和版本号，不改生产代码、API/ABI 或 wire；ERR-C 其余信用与组合故障以及 ERR-A/B/D/E 仍待验收。详见[RESETTING 期间合法在途 WINDOW_UPDATE 回归](rpc_v2_reset_inflight_window_update_20261026.md)。

dev.138 为 #242/ERR-B/C 补上 Split Channel RESET 与普通 CLOSE 的双边界回归：在 RESET 对端确认前收到的 FIFO CLOSE 不提前释放 RESETTING 流槽；双向 RESET 完成后的晚到 CLOSE 以 `TR_ERR_STATE` 关闭承载该流的物理连接。CONTROL、BULK 两种出错方向都验证另一 lane 继续交付 DATA 并完成 RESET。GCC Release、TSan、ASan/UBSan 完整 `test_stream_reset` 均通过。本切片只增加测试、记录和版本号，不改生产代码、API/ABI 或 wire；RESET/FIFO 其余组合及 ERR-A/B/D/E 仍待验收。详见[Split Channel RESET/CLOSE 顺序边界回归](rpc_v2_reset_split_close_order_20261026.md)。

dev.139 为 #242/ERR-B/C 增加 Split Channel 半关闭后的 DATA 拒绝回归：目标流本地 RESET 后先收到 FIFO `STREAM_CLOSE`，保持 RESETTING 期间又从已关闭方向收到 DATA 时，Channel 以 `TR_ERR_STATE` 关闭对应物理 lane；CONTROL、BULK 两种方向都验证目标 DATA 未交付，另一 lane 的健康流仍可交付并完成 RESET。GCC Release（40k 墓碑）、TSan（5k）、ASan/UBSan（40k）完整 `test_stream_reset` 均通过；dev.138 head GitHub CI 为 13/13 全通过。本切片只增加测试、记录和版本号，不改生产代码、API/ABI 或 wire；其他 DATA/CLOSE/RESET 交错及 ERR-A/B/C/D/E 全矩阵仍待验收。详见[Split RESET/CLOSE 后 DATA 拒绝回归](rpc_v2_reset_split_close_data_20261026.md)。

dev.140 为 #242/ERR-B/C 补充 Split Channel RESETTING 中合法 DATA 与 FIFO CLOSE 的正向顺序：目标流本地 RESET 后，先收到合法完整 DATA，再收到该方向的普通 CLOSE。RESETTING 丢弃路径将 8 字节计入接收与消费账本、不交付应用、不增加通告上限或发送 WINDOW_UPDATE；后续 CLOSE 不提前退役流，收到对端 RESET 后正常收敛。CONTROL、BULK 两种方向都验证另一 lane 健康流继续交付并完成 RESET。GCC Release、TSan、ASan/UBSan 完整 `test_stream_reset` 均通过。本切片只增加测试、记录和版本号，不改生产代码、API/ABI 或 wire；其他 DATA/CLOSE/RESET 交错及 ERR-A/B/C/D/E 全矩阵仍待验收。详见[RESETTING DATA 后 FIFO CLOSE 回归](rpc_v2_reset_split_data_close_20261026.md)。

dev.141 为 #242/ERR-B 补充 RESET 响应发送背压下的 Split Channel FIFO DATA 边界：对端先发 RESET；测试确认本地 RESET 响应帧已实际遇到 `EAGAIN` 且尚未写出，再从同一连接发送 DATA。RESET 入队并退役目标流后，迟到 DATA 以 `TR_ERR_STATE` 关闭对应 lane，不交付应用；CONTROL、BULK 两个方向均验证另一 lane 健康流仍能交付并完成 RESET。GCC Release（40k 墓碑）、TSan（5k）、ASan/UBSan（40k）完整 `test_stream_reset` 均通过。本切片只增加测试、记录和版本号，不改生产代码、API/ABI 或 wire；其余 RESET/FIFO 交错及 ERR-A/B/C/D/E 完整矩阵仍待验收。详见[Split Channel RESET 响应背压期间 DATA 隔离回归](rpc_v2_reset_response_backpressure_data_20261026.md)。
dev.142 为 #242/ERR-B/C 扩展 RESET 响应发送背压下的 Split Channel FIFO 后续帧矩阵：在本地 RESET 响应实际遇到 `EAGAIN`、目标流已退役而 lane 仍存活时，分别注入 DATA 和 `WINDOW_UPDATE`。CONTROL、BULK 两方向的四个组合都以 `TR_ERR_STATE` 关闭目标 lane；DATA 不交付，WU 不增加接收更新统计，另一 lane 的健康流仍能交付并完成 RESET。GCC Release（40k 墓碑）、TSan（5k）、ASan/UBSan（40k）完整 `test_stream_reset` 均通过。本切片只增加测试、记录和版本号，不改生产代码、API/ABI 或 wire；其余 RESET/FIFO 交错及 ERR-A/B/C/D/E 完整矩阵仍待验收。详见[Split Channel RESET 响应背压期间 DATA/WINDOW_UPDATE 隔离回归](rpc_v2_reset_response_backpressure_frames_20261026.md)。
dev.143 为 #242/ERR-B 扩展 RESET 响应发送背压下的 Split Channel FIFO 后续帧矩阵，将后续 `STREAM_CLOSE` 与 DATA、`WINDOW_UPDATE` 一并覆盖；CONTROL、BULK 两方向共六个组合。响应遇到 `EAGAIN` 且尚未写出时收到任一后续帧，均以 `TR_ERR_STATE` 关闭目标 lane；DATA 不交付、WU 接收统计不增长、CLOSE 不结束 lane 外的健康流，另一 lane 仍能交付 DATA 并完成 RESET。GCC Release（40k 墓碑）、TSan（5k）、ASan/UBSan（40k）完整 `test_stream_reset` 均通过。本切片只增加测试、记录和版本号，不改生产代码、API/ABI 或 wire；其余 RESET/FIFO 交错及 ERR-A/B/C/D/E 完整矩阵仍待验收。详见[Split Channel RESET 响应背压期间 CLOSE 隔离回归](rpc_v2_reset_response_backpressure_close_20261026.md)。
dev.144 为 #242/ERR-B 补充 RESET 响应发送背压窗口内的重复 RESET 回归：初始响应已在目标 Reactor fd 上真实遇到 `EAGAIN`、流已退役后，同原因重复 RESET 命中墓碑，目标 lane 保持 UP 且仅观察到一份原响应；不同原因重复 RESET 以 `TR_ERR_STATE` 关闭目标 lane。CONTROL、BULK 两方向都覆盖同原因和冲突原因，健康 lane 均继续交付 DATA 并完成 RESET。GCC Release（40k 墓碑）、TSan（5k）、ASan/UBSan（40k）完整 `test_stream_reset` 均通过。本切片只增加测试、记录和版本号，不改生产代码、API/ABI 或 wire；其他 RESET/FIFO 交错及 ERR-A/B/C/D/E 完整矩阵仍待验收。详见[Split Channel RESET 响应背压期间重复 RESET 回归](rpc_v2_reset_response_backpressure_duplicate_20261026.md)。
dev.145 为 #242/ERR-B 补充真实对端 RESET 响应的队列容量与部分写入 FIFO 回归：四个普通 PING 占满控制 TX 池，RESET 事件回调再将 Reactor 命令环填至容量上限并确认下一项返回 `TR_AGAIN`；固定预留 RESET 仍在所有前驱后入队。响应先实际写出 7 字节，再遇到 `EAGAIN`；在途期间收到 PING 后产生的 PONG 不能插入 RESET 半帧，wire 顺序保持四个既有 PING、完整 RESET、PONG。恢复后同连接健康流仍交付 DATA 并完成 RESET。GCC Release（40k 墓碑）、TSan（5k）、ASan/UBSan（40k）完整 `test_stream_reset` 均通过。本切片只增加测试、记录和版本号，不改生产代码、API/ABI 或 wire；其他 RESET/FIFO 交错及 ERR-A/B/C/D/E 完整矩阵仍待验收。详见[RESET 响应队列满载与 PONG 抢占边界回归](rpc_v2_reset_response_backpressure_queue_preemption_20261026.md)。
dev.146 修复 dev.145 新增压力回归的队列观察时序。CI TSan 首次触发 `stats.tx_queued_items == CONTROL` 失败：owner 同步回调中的 SEND 被排入命令环，`tr_reactor_call` 返回不代表 SEND 命令已附接到 TX 队列。另在 Release 重现了收到末尾 PONG 后立即观察到尚未完成记账的在途 TX 项。两处均改为等待可观察的队列状态再断言。干净 GCC Release（40k 墓碑）、ASan/UBSan（40k）、TSan（5k）完整 `test_stream_reset` 均通过。本切片只修改测试、记录和版本号，不改生产代码、API/ABI 或 wire；其他 RESET/FIFO 交错及 ERR-A/B/C/D/E 完整矩阵仍待验收。详见[RESET 响应队列满载回归的异步观察时序修复](rpc_v2_reset_response_backpressure_queue_sync_20261026.md)。

dev.147 为 #242/ERR-B 补充同一 Reactor 多物理连接的 RESET 固定预留隔离回归：Split Channel 的 CONTROL、BULK 连接共享的普通控制 TX 池各占两项并同时收到对端 RESET，第一个 RESET 回调还填满命令环；两条连接各自的响应都进入队列。CONTROL RESET 部分写出 7 字节后遇到 EAGAIN，BULK 仍按 PING、RESET、PONG 顺序独立排空；CONTROL 恢复后也保持 RESET、PONG FIFO，两条健康流都继续交付并完成 RESET。GCC Release（40k 墓碑）、ASan/UBSan（40k）、TSan（5k）完整 `test_stream_reset` 均通过。本切片仅修改测试、审查记录、README 和版本号，不改生产代码、API/ABI 或 wire；ERR-A/B/C/D/E 与 #242/V2 其余验收仍未完成。详见[多连接 RESET 固定预留隔离回归](rpc_v2_reset_response_reservation_multiconnection_20261026.md)。

dev.148 为 #242/ERR-B 补充同连接健康流前进回归：RESET 响应部分写出 7 字节并真实遇到 `EAGAIN` 时，仍可将 DATA 交付给同 Channel 的健康流应用；暂持载荷以避免信用更新帧改变队列观察，随后到达的 PONG 仍排在完整 RESET 之后。发送恢复后连接保持 UP，健康流完成 RESET。GCC Release、ASan/UBSan（40k 墓碑）、TSan（5k）完整 `test_stream_reset` 均通过。本切片仅修改测试、审查记录、README 和版本号，不改生产代码、API/ABI 或 wire；ERR-A/B/C/D/E 与 #242/V2 其余验收仍未完成。详见[RESET 背压期间健康同伴流前进回归](rpc_v2_reset_response_backpressure_sibling_progress_20261026.md)。

dev.149 修正 #242/ERR-A 的异步压缩结果作用域：`INTERNAL`/未知 outcome，以及服务报告 `OK` 却没有有效输出的本地契约损坏，现释放任务资源并关闭承载该结果的精确物理连接；`BAD_DATA`、`LIMIT`、真实 `NOMEM` 仍按单流处理。Release、TSan、ASan/UBSan 的完整 `test_transport` 均通过，新增真实 Channel 两流回归确认不发 RESET、两流都终结且预算归零。没有改变 wire/API/ABI；#242 的 ERR-B/C/D/E、TLS 记录完整性负例、长时资源公平和全局回收矩阵仍待验收。详见[生产错误作用域对照审计](rpc_v2_error_scope_production_audit_20261026.md)。

dev.150 为 #242 补上真实 TCP Endpoint 的 RESET/`FINAL_STATUS` 交错：服务端已接受成功 FINAL 并排入 TX 后，测试用实际 `sendmsg` EAGAIN 阻止抵达；客户端在此期间 RESET，看到 `DEADLINE_EXCEEDED` 且终局计数为一，服务端保留已接受的 OK 结果；释放阻塞后不多交付记录，健康同伴在同一 TCP 连接完成。完整 `test_rpc_record_endpoint_tcp` 在 Release、TSan、ASan/UBSan 均通过。本证据只覆盖结果不确定的这一时序，不声称业务 exactly-once；#242 其余错误隔离、配额公平、RESET/FIFO 完整矩阵、GOAWAY/连接组接受边界与全局回收仍待验收。详见[RESET 与已排队 FINAL_STATUS 竞态](rpc_v2_reset_final_status_race_20261026.md)。

dev.151 为 #242 补上真实 TCP Endpoint 的 RESET/响应 `MESSAGE` 交错：双向方法处理完请求 MESSAGE 并由 Endpoint/Channel 接受回复后，测试确认对应 DATA 帧的真实 `sendmsg` 遇到 EAGAIN，再由客户端发出 timeout RESET。以服务端终局作为 RESET 已处理边界，确认该边界后没有新的回复被接管；释放背压后客户端未收到迟到 MESSAGE，两端目标调用各终结一次，健康同伴在同一 TCP 连接成功完成。完整 `test_rpc_record_endpoint_tcp` 在 GCC Release、TSan、ASan/UBSan（泄漏检测开启）均通过。本切片只补充 DATA 交错证据；RESET/FIFO 全矩阵、配额公平、GOAWAY/连接组接受边界和统一回收仍未完成。详见[RESET 与已排队响应 MESSAGE 竞态回归](rpc_v2_reset_queued_message_race_20261026.md)。

dev.152 为 #242/#241 补充压缩接收配额耗尽后的多流恢复回归：同一 TCP Channel 上保留服务器内存预算，一条 Zstd 解码以 `RESOURCE_EXHAUSTED` 终结且缓存归零；配额仍保留时 CONTROL PING/PONG 往返成功。释放配额后，四条幸存压缩流通过屏障并发发送，均由服务端完整解码并校验正文，再完成半关闭、成功响应和唯一终局，双方缓存归零。GCC Release、TSan、ASan/UBSan 完整 `test_rpc_record_stream` 通过；ASan 目标在阶段诊断加入后连续 8 次通过。本切片只增强单 Channel 配额恢复证据，不证明长期调度公平、分域预算矩阵或 #242/#241 整体验收完成。详见[压缩配额恢复多流并发回归](rpc_v2_compressed_quota_multi_peer_recovery_20261026.md)。

dev.153 将 #242/#241 配额恢复扩展至四轮进度：每轮通过屏障同时启动四条幸存 Zstd 流，并等待四条流都交付本轮消息，再通过客户端与服务端 Reactor owner 同步屏障确认变换完成，之后才启动下一轮。每条流四轮消息的压缩类型、长度和校验和均验证；四条流最终半关闭、收到 OK 响应、两端唯一终结且缓存归零。TDD 红灯以旧单次发送重现“收到 1 条、预期 4 条”的超时；修正后 GCC Release、TSan、ASan/UBSan 完整 `test_rpc_record_stream` 均通过。本切片提高有限多轮进展证据，不证明长期延迟公平、吞吐基线、完整预算矩阵或 #242/#241 整体验收完成。详见[压缩配额四轮多流恢复回归](rpc_v2_quota_multi_peer_repeated_recovery_20261026.md)。

dev.157 在 dev.156 方法钩子上增加公共 SDK 真实 TCP 集成回归：授权回调等到服务端取消令牌置位后才迟到返回 ALLOW，覆盖主动取消与 deadline；旧调用不进入业务 work，后续健康调用仍成功。GCC Release 全套 92/92，SDK 目标在 GCC ASan/UBSan、GCC TSan、Clang Release 均通过。PR #330 当前 dev.157 head 的 CI 13/13 通过。审查记录见[方法钩子记录](rpc_v2_method_hooks_20261027.md)。#243 仍缺通用异步操作内核与显式操作引用、完成/退役屏障、单 worker/满队列/并发契约及 TLS/压缩共享调度，保持 OPEN。

dev.158 在同一 RPC Record dispatch/TaskExecutor 路径中固定单 worker 正在执行阻塞工作、其余三个容量槽全部排队；新 authorize OPEN 在满载期间不得开始认证、业务 work 或终局。解除阻塞后，认证 OPEN 被有界泵重试，后续 MESSAGE/HALF_CLOSE 仍按序执行且单次终局。该测试在 GCC Release、ASan/UBSan、TSan 通过；完整 GCC Release 构建通过、注册测试 92/92 通过，clang-format 18.1.8 修改行检查通过，PR #330 精确 head 的 GitHub CI 13/13 通过。不改生产代码，也不证明完成队列满、永久关闭或通用操作生命周期。#243 仍待通用异步操作内核、显式操作引用、完成/退役屏障及更广并发/共享调度验收。

2026-10-27 已形成[通用异步操作内核设计](../superpowers/specs/2026-10-27-rpc-v2-operation-kernel-design.md)与[实施计划](../superpowers/plans/2026-10-27-async-operation-pool-core.md)。dev.159 已实现固定容量操作池，并接入 Server V2 authorize、TLS provider 和 MessageService；池只管理操作身份、取消及结果交接，不接管工作输入或领域字节预算。dev.163 通过真实 TCP V2 Client/Server 回归证明 CallRef 在异步 authorize 迟到 ALLOW 后仍保留唯一取消快照，服务端操作先退役；CallRef 持有期间 Client drain 返回 `TR_AGAIN`，release 后 drain 收敛。dev.164 进一步证明终局 CallRef 会使 `tr_client_destroy()` 返回 `TR_AGAIN`、release 后重试成功；dev.165 验证可以在 terminal callback 内 hold 并跨回调保留引用。对应审查与验证边界见[异步操作内核记录](rpc_v2_operation_kernel_20261027.md)。更广的跨 Reactor/嵌套交错、command/completion 队列满载、全量回调和资源回收屏障、跨 TLS/压缩共享调度，以及 dev.166 新 head 的远端 CI 仍待完成，#243/#239 保持 OPEN。

dev.156 为 #243 的 Server V2 方法提供固定类型 observe/admit/authorize 钩子：异步 authorize 复用分片共享 TaskExecutor，owner 完成边界复核槽位代次、调用身份、终局与 deadline；REJECT 经空错误 FINAL 返回，迟到 ALLOW 不恢复终局调用。全套 GCC Release 92/92；dispatch/SDK 在 GCC ASan/UBSan、GCC TSan、Clang Release 通过；堆叠 PR #330 的前序 head 有 13/13 GitHub 检查通过。此切片只补齐固定 Server 方法钩子及该 authorize 路径的迟到完成保护，仍缺通用异步操作内核与显式操作引用、完成/退役屏障、单 worker/满队列/并发契约及 TLS/压缩共享调度；#243 保持 OPEN。




dev.47 为 #242/ERR-A 增加真实 TCP/Channel/RPC Endpoint 语义错误隔离回归：已准入调用收到
重复 OPEN 后以 `INVALID_ARGUMENT` 终结，后续调用仍在同一 TCP 连接完成；两端 Reactor 连接仍为 ACTIVE。
服务端 Endpoint 接管 Channel 事件回调，连接状态通过 Reactor 直接查询验证。
Release 与 TSan 下 `test_rpc_record_endpoint_tcp` 全目标通过。该用例证明一项 RPC 语义错误映射，
不代表完整分类矩阵、压缩失败、资源压力及连接级错误路径均已覆盖。详见
[真实 RPC 语义错误隔离记录](rpc_v2_semantic_error_isolation_20261025.md)。

dev.48 为 #242/ERR-A 增加真实 TCP 连接级错误关闭回归：两个 OPEN 已进入业务处理后注入头 CRC 错误，
服务端不得降级为单流处理；两端 Reactor 连接均退出活动状态，两个 Endpoint 调用各以
`UNAVAILABLE` 唯一终结，没有成功 FINAL，活动网络流归零。Release 与 TSan 下完整
`test_rpc_record_endpoint_tcp` 通过。本切片覆盖一个不可恢复线帧错误；其他连接失败、压缩/资源组合与
ERR-A 全矩阵及 ERR-B/C/D/E 仍待验收。详见
[真实 RPC 连接级错误关闭记录](rpc_v2_connection_error_termination_20261025.md)。

dev.49 为 #242/ERR-A 扩展压缩接收配额失败隔离：真实 Zstandard MESSAGE 在服务器解码工作区预算不足时只终结
目标调用，以 `RESOURCE_EXHAUSTED` 映射 RESET 通知对端；同共享 Channel 的 CONTROL/BULK lane 继续 UP。释放预算后，
同伴调用成功接收压缩正文并完成，终局缓存归零。Release 与 TSan 下完整 `test_rpc_record_stream` 通过。
该切片不覆盖完整压缩/信用组合和连接组边界；ERR-A 全分类及 ERR-B/C/D/E 仍未验收。详见
[压缩解码配额同伴隔离记录](rpc_v2_compressed_quota_peer_progress_20261025.md)。

dev.50 扩展 #242/ERR-A 的重复 OPEN 语义错误隔离证据：错误发生前，同一 TCP 连接上的同伴调用已进入
业务 MESSAGE 并暂停在 gate 中；错误流以 `INVALID_ARGUMENT` 终结期间，同伴保持未终结、连接保持 ACTIVE。
释放 gate 后该在途同伴以 OK 完成，错误后的新调用也成功。GCC Release 与 TSan 下完整
`test_rpc_record_endpoint_tcp` 通过。dev.47 的原验证仅覆盖错误后新开调用；本切片补齐已活动同伴时序，
并不代表 ERR-A 全分类或 ERR-B/C/D/E 已验收。详见
[活动同伴流语义错误隔离记录](rpc_v2_semantic_active_sibling_isolation_20261025.md)。

dev.51 为 #242/ERR-A 增加业务拒绝期间的活动同伴隔离回归：方法 0 返回 `PERMISSION_DENIED` 时，同一
TCP 连接上的方法 1 调用已进入业务 MESSAGE 并暂停；拒绝流终结后，同伴仍活动、连接仍 ACTIVE。释放 gate
后同伴以 OK 唯一终结。GCC Release 与 TSan 下完整 `test_rpc_record_endpoint_tcp` 通过。本切片只证明一项
业务终局与同伴交错，不代表 ERR-A 全分类矩阵或 ERR-B/C/D/E 已完成。详见
[业务拒绝活动同伴隔离记录](rpc_v2_business_rejection_active_sibling_20261025.md)。

dev.52 为 #242/ERR-A 增加路由拒绝期间的活动同伴隔离回归，覆盖未知方法 `UNIMPLEMENTED` 与非法 OPEN 元数据
`INVALID_ARGUMENT`。路由拒绝必须在业务 Dispatch 前完成；同连接活动同伴在拒绝期间保持未终结、连接保持 ACTIVE，
随后同伴以 OK 完成。GCC Release 与 TSan 下完整 `test_rpc_record_endpoint_tcp` 通过。本切片覆盖两种路由错误，
不代表 ERR-A 完整分类矩阵或 ERR-B/C/D/E 已完成。详见
[路由拒绝活动同伴隔离记录](rpc_v2_router_rejection_active_sibling_20261025.md)。

dev.53 为 #242/ERR-D 增加公开连接组双 DATA 成员排空回归：两个 DATA Stream 通过不同物理成员承载；开始排空后
新 Offer/Ready 被拒绝，活动传输未释放前 Client 和 Server 均保持未排空。释放两个传输后 Client 排空完成，Server
继续等待客户端关闭两条 DATA 成员，最终组/连接/DATA/传输计数均归零。SDK 测试销毁 helper 同时对 API 合法返回的
`TR_AGAIN` 作有界重试，仍会暴露其他错误。GCC Release 与 TSan 下 `test_connection_group_facade`、`test_rpc_v2_sdk`
完整目标均通过。本切片尚未覆盖发送中/压缩中 drain、成员故障/重连及完整多组竞争矩阵。详见
[双 DATA 成员连接组排空记录](rpc_v2_group_multi_data_drain_20261025.md)。

dev.54 扩展同一双成员回归：DATA 发送已被接受且停留在 Reactor TX 队列时开始 drain，逐个释放客户端 transfer 后，
Client 活动传输数为零但 `send_bytes_inflight` 仍为 1，等待继续超时。解除 socket `EAGAIN` 后服务端确实收到消息，
发送字节归还，Client 才完成本地 drain；Server 继续等待成员关闭。GCC Release 与 TSan 下完整
`test_connection_group_facade` 通过。本切片只覆盖未压缩 DATA 发送在途，不覆盖压缩任务在途、成员故障/重连或多组竞争。
详见[DATA 在途发送排空记录](rpc_v2_group_data_tx_drain_20261025.md)。

dev.55 补充 Zstandard 压缩任务取消与 drain 交错：暂停已完成压缩、尚未交付的 worker 回调，开始双端排空并释放
客户端 transfer；transfer 计数归零后，仍归属压缩任务的输入字节使 Client drain 保持未完成。放开 worker 后任务被
取消收敛、不发送 DATA，字节归还后 Client drain 完成；Server 仍等待 DATA 成员关闭。GCC Release 与 TSan 下完整
`test_connection_group_facade` 通过。本切片不覆盖压缩完成并接受发送后的 encoded wire/TX 在途排空、成员故障或多组竞争。
详见[Zstandard 任务取消排空记录](rpc_v2_group_compression_cancel_drain_20261025.md)。

dev.56 修复并覆盖压缩 wire 在途 drain：已附着 DATA 成员上的压缩消息在 Client/Server drain 后仍继续发送和交付；
此前 `tr_pipeline_listener_message_done()` 把 `listener->draining` 当作取消条件，导致连接/路由代次仍有效的解码结果被静默
丢弃。现在仍用精确连接身份判定回调有效，drain 只关闭新准入。测试阻塞真实 DATA socket 写入并暂停压缩完成回调，
验证服务端消息交付、Client TX wire 归还后排空完成、Server 等待成员关闭。GCC Release 与 TSan 下完整
`test_connection_group_facade` 通过。连接故障/重连和多组竞争仍待验收。详见
[压缩 wire 在途排空修复记录](rpc_v2_group_compressed_wire_drain_20261025.md)。

dev.57 为 #242/ERR-D 增加双 DATA 成员故障与替代回归：中止一条真实物理连接后，旧 transfer 从 Client/Server 亲和统计中
退役，另一成员仍能发送；同一 group/epoch 重新 Offer/Ready 后，替代连接可接纳新 transfer 并交付消息。释放剩余亲和并
排空后，Client 先完成本地等待，Server 在控制与所有 DATA 成员关闭后完成等待，组/连接/DATA/传输计数归零。Release 与
TSan 下完整 `test_connection_group_facade` 通过；变异版本若以错误代次撤销 transfer，会在客户端活动传输计数断言处失败。
多组并发成员故障与 ERR-D 全矩阵仍未验收。详见[双 DATA 成员故障与重建回归](rpc_v2_group_member_failure_replacement_20261025.md)。

dev.58 为 #242/ERR-D 增加单 shard 双组故障隔离回归：两个不同 group/epoch 同时持有 CONTROL 和 DATA；A 组 DATA 故障并
替代、随后关闭期间，B 组的活动 DATA transfer 仍成功交付。A 组关闭后服务端只剩 B 组，B 组继续发送并独立完成 drain，
最终组/连接/DATA/transfer 计数归零。GCC Release 与 TSan 下完整 `test_connection_group_facade` 通过。该用例不覆盖多组
同时故障、连续替代和资源竞争压力矩阵。详见[双组成员故障隔离记录](rpc_v2_group_multi_group_failure_isolation_20261025.md)。

dev.59/dev.60 为 #242/ERR-D 增加双组同时故障、两轮成员替代及组级独立关闭回归；完整连接组门面目标在 Release 与
TSan 下通过，dev.60 Release 连续运行 8/8。多轮高频竞争和资源压力仍待验收。详见
[双组重复故障与快速替代记录](rpc_v2_group_repeated_failure_replacement_20261025.md)。

dev.61 为 #242/ERR-E 增加真实内存账本下的 RESET 墓碑固定预算回归：容量为 2 的 Channel 完成三次 RESET，
验证驻留墓碑吸收重复 RESET、FIFO 淘汰后旧 RESET 触发未知流错误；周转期间 `current_bytes` 与 `peak_bytes`
均保持创建时固定值，预算拒绝数为零，销毁后当前预算归零。Release 与 TSan 下完整 `test_stream_reset` 通过。
仍待验证高频 RESET 的 CPU 成本、长时间压力及更广的信用/丢弃组合。详见
[RESET 墓碑固定预算回归记录](rpc_v2_reset_tombstone_fixed_budget_20261025.md)。

dev.62 为 #242/ERR-E 将精确墓碑历史查询改为固定开放寻址索引，复合键为物理连接代次与 stream ID；允许 split
Channel 两条连接独立前沿产生相同 ID 墓碑。FIFO 覆盖时删除旧键并修复碰撞簇，查询命中后仍核对对端原因；
索引字节纳入 Channel 固定预算。单元测试覆盖跨表尾碰撞簇、删除/槽复用，以及
40k 个单调流 ID 下插入与成功查询总探测数不超过 8 倍条目数；构造事务测试以 max_streams=1/2/3 的预算差分
确认索引字节确实计费。Release、ASan/UBSan、TSan 下索引、完整 `test_stream_reset`、Channel 构造事务和排空目标
通过。索引期望 O(1)，碰撞最坏仍为 O(max_streams)；实际连接上的长期高负载与碰撞压力仍待验收。详见
[RESET 墓碑查询索引记录](rpc_v2_reset_tombstone_index_20261025.md)。

dev.63 补齐 `test_rpc_record_endpoint_tcp` 手动维护的生产对象闭包，使其包含 dev.62 新增的
RESET 墓碑索引模块；GCC 精确诊断基线同步反映 `channel.c` 顶部结构搬移导致的既有告警行号变化，诊断本身未变。
该修复只影响测试链接和诊断定位，不改运行时逻辑；PR #324 的 GitHub 检查 21/21 通过。详见
[RESET 索引测试链接修复记录](rpc_v2_reset_tombstone_build_linkage_20261025.md)。

dev.64 在一条真实 Channel 上完成 128 次双向 RESET，让容量为 4 的墓碑环连续周转 32 轮；重复发送当前最老
和最新驻留墓碑均被吸收，重复发送第一条已淘汰墓碑则关闭连接并报告未知流。Release、TSan、ASan/UBSan 下
`test_stream_reset` 通过。该回归验证长期 FIFO 周转后的保留窗口和淘汰边界，不测量吞吐/CPU，也不覆盖恶意
碰撞压力。dev.64 的两组 GitHub CI 与 RPC smoke 检查 21/21 通过。ERR-E 的更高频负载仍待验收。详见
[RESET 墓碑长周转回归](rpc_v2_reset_tombstone_long_turnover_20261025.md)。

dev.65 为墓碑索引加入确定性碰撞簇压力回归：256 个不同流 ID 被构造为命中同一 home bucket，表容量为 512；
初始填充后连续淘汰最老键、复用 FIFO 环槽 256 次，检查每次删除后的探测终止位置、替换插入探测数，以及最终全部驻留键可查、
所有淘汰键不可查。关闭大表向后移位修复的临时变异在该新断言失败，恢复后 Release、TSan、ASan/UBSan 下
`test_reset_tombstone_index` 均通过；dev.65 两组 GitHub CI 与 RPC smoke 检查 21/21 通过。它覆盖 256 路合成碰撞，
不测量耗时，也未推断 `max_streams=40000` 的最坏碰撞 CPU 成本。详见
[RESET 墓碑索引碰撞压力回归](rpc_v2_reset_tombstone_collision_stress_20261025.md)。

dev.66 将已有 40k 顺序奇数 ID 的索引测试扩展为最大配置 FIFO 周转：填满 `max_streams=40000` 历史后，连续删除原键并
在对应环槽插入 40k 个新 ID；每个旧 ID 在淘汰后返回 stale，最终所有新 ID 均映射回原环槽。删除、淘汰查询、重插与
最终保留/淘汰查询的累计探测数不超过 1.92m。跳过大表簇修复的变异在最大容量删除断言失败；恢复后 Release、TSan、ASan/UBSan
下 `test_reset_tombstone_index` 均通过。该确定性边界不设耗时门槛，也不等同真实 Channel 40k 并发压力。最大容量对抗碰撞仍待验收。详见
[RESET 墓碑索引最大容量 FIFO 周转](rpc_v2_reset_tombstone_max_capacity_turnover_20261025.md)。

dev.67 在 `max_streams=40000`、131072 槽表和满历史占用下加入 256 路同 home 碰撞簇 FIFO 周转。每轮删除簇首后、
复用环槽前，先确认其余碰撞键仍可查，再验证旧键已淘汰；最终核对其余 39,744 个键和新驻留键槽位不变。跳过大表
簇修复的变异在复用前幸存键断言失败；恢复后 Release、TSan、ASan/UBSan 下 `test_reset_tombstone_index` 均通过。
测试累计探测预算为 2.56m，但 `remove` 的探测输出不包含 backward-shift 扫描，因此不视为 CPU 成本上界；40k 键同 home
和真实 Channel 大规模连接压力仍待验收。详见[最大容量碰撞周转记录](rpc_v2_reset_tombstone_max_capacity_collision_20261025.md)。

dev.68 扩展 #242/ERR-A、ERR-C 压缩工作区配额场景：目标流以 `RESOURCE_EXHAUSTED` 终结并 RESET 后、预算预留尚未释放时，
同一真实 Channel 的 CONTROL keepalive PING/PONG 仍可往返；随后释放预算，压缩同伴调用完整完成。GCC Release、TSan、
ASan/UBSan 下完整 `test_rpc_record_stream` 均通过。本切片只加回归测试，不证明连接组的控制面错误分类或完整资源公平矩阵。详见
[压缩配额期间控制帧进展记录](rpc_v2_compressed_quota_control_progress_20261025.md)。

dev.69 将同一 CONTROL keepalive 进展断言用于 RAW 重组预算失败：预算全部预留期间，失败流仍可通过 RESET 终结，
PONG 往返完成后释放预算，同 Channel 同伴继续成功。RAW 重组与 Zstandard 解码配额两种场景都在 GCC Release、TSan、
ASan/UBSan 下通过。仅增加测试；持续压力下延迟上界、连接组错误矩阵与其他预算/信用组合仍待验收。详见
[重组与解码配额控制面进展记录](rpc_v2_receive_quota_control_progress_20261025.md)。

dev.70 为 #242/ERR-E 增加真实 Channel 最大容量墓碑周转：Release 与 ASan/UBSan 在 `max_streams=40000` 下完成 40,000 次
OPEN/双向 RESET，验证最早与最新驻留墓碑、再加入一条后的 FIFO 淘汰及未知 RESET 连接错误；TSan 使用容量 5,000 填满同一
状态机边界，最大容量索引仍由独立索引目标执行 TSan 压力。仅增加真实传输集成测试，不改生产实现。真实 40k 键同 home
极端碰撞、持续高频 CPU/延迟上界及 ERR-A/B/C/D 其他组合仍待验收。详见
[真实 Channel 最大容量 RESET 墓碑周转记录](rpc_v2_reset_tombstone_channel_capacity_20261025.md)。

dev.71 修复 #242/ERR-A 的连接组 listener 接受边界：同步 DATA route 前导在 listener begin_drain 后才完整时，原路径仍会
接受并增加 data_accepts；现于 DATA 接受入口检查 draining，返回关闭错误后由既有失败路径精确撤销预留并关闭 pending 连接。
真实 TCP listener 回归稳定复现旧行为，并验证修复后 route_rejections 增加、data_accepts 不变。`test_pipeline_listener` 和
`test_connection_group_facade` 在 GCC Release、TSan、ASan/UBSan 下通过。仅改接受边界与测试，不改线协议或公开 API；其他分类映射、
连接组跨成员接受组合及 ERR-B/C/D/E 剩余矩阵仍待验收。详见
[连接组 DATA 接受排空边界记录](rpc_v2_group_data_admission_drain_20261025.md)。

dev.72 扩展上述接受边界回归到 CONTROL：同一真实 listener 同时持有一个 pending DATA route 和一个有效的新 CONTROL route，
两条 route 前导均在 begin_drain 前留缺最后一个字节，drain 后补齐；CONTROL/DATA 接受计数均不增加、route rejection 增加两次，
两个 pending socket 均关闭，既有 CONTROL 会话保持活动。Release、TSan、ASan/UBSan 下 `test_pipeline_listener` 通过。
仅扩展集成测试，不改运行时；连接组跨成员接受组合及 ERR-A/B/C/D/E 其他剩余验收仍待完成。详见
[CONTROL/DATA pending route 排空回归](rpc_v2_group_drain_pending_routes_20261025.md)。

dev.73 扩展到跨两个 DATA member 的并发 pending route：同一 CONTROL 下两个不同成员槽位均已预留，两个 DATA 前导和另一个有效
CONTROL 前导都在 begin_drain 前缺最后一个字节；drain 后补齐时三者全部拒绝，接受计数不变、route rejection 增加三次，所有 pending
socket 关闭且原 CONTROL 保持活动。Release、TSan、ASan/UBSan 下 `test_pipeline_listener` 通过。只扩展集成测试，不改运行时；
ERR-A 其他分类映射及 ERR-B/C/D/E 剩余矩阵仍待完成。详见
[双 DATA 成员 pending route 排空回归](rpc_v2_group_drain_multi_member_pending_20261025.md)。

dev.74 修正 ERR-E 碰撞周转测试的删除成本计量：`remove` 的 `probes_out` 原只报告键定位探测，没有计入整段探测簇校验和
backward-shift。现统计完整删除槽位检查；3 项环绕簇删除断言 7 次，256 路同 home 碰撞删除断言 513 次，最大容量混合碰撞
周转的总探测上界包含删除扫描。计数仅在测试传入输出指针时累加，生产路径不做逐槽计数。Release、TSan、ASan/UBSan 下
`test_reset_tombstone_index` 和 `test_stream_reset` 通过；40k 键全部同 home 的最坏输入和真实高频 CPU/延迟仍未验收。详见
[RESET 墓碑删除成本计量记录](rpc_v2_reset_tombstone_remove_cost_accounting_20261025.md)。

dev.75 为 #242/ERR-E 补上 40k 键全部同 home 的索引层确定性边界：在 131072 槽表中构造 40000 个唯一复合键，精确核对
尾部查找 40000 次、簇首删除 80001 次、FIFO 槽复用后的插入与查找各 40000 次、旧键查询 40001 次。Release、TSan、
ASan/UBSan 下 `test_reset_tombstone_index` 本地通过，push CI、PR CI 与 RPC benchmark smoke 均通过；该输入是合成索引布局，不证明真实连接的持续高频 CPU/延迟。
详见[最大容量同 home 成本记录](rpc_v2_reset_tombstone_same_home_max_capacity_20261025.md)。

dev.76 为 #242/ERR-E 增加真实 Channel 高频成本初测：测试在 40,000 次 OPEN/双向 RESET 周转中逐次记录 RESET 往返，汇总最近秩 P50/P95/P99，并记录包含 OPEN/RESET 的循环墙钟时间、进程 CPU 时间和吞吐。三次 Release 的分位数中位数为 1151.42/1395.03/1712.33 µs；测试机单次结果有明显波动。Release 与 ASan/UBSan 使用 40,000 条目，TSan 使用 5,000 条目并通过。数据来自本地 socketpair/Reactor/Channel 测试夹具，不能分离出生产微基准成本，也没有跨机器基线或验收阈值；因此只作为初始观测，受控性能基线、持续压力和更广信用/丢弃组合仍待验收。详见
[真实 Channel RESET 高频成本初测](rpc_v2_reset_tombstone_high_frequency_cost_20261025.md)。

dev.77 为 #241 补充真实连接组慢消费者/多 DATA 进展回归：同一组的两个 transfer 被实际路由到不同 DATA 物理连接；第一成员上的应用消息返回 TAKE_OWNERSHIP 并继续持有接收缓冲后，第二成员仍独立进入服务端回调、验证正文并释放。路由探针确认两个物理连接不同；释放第一条消息后再继续现有的双成员 drain 场景。GCC Release 连续 5/5、TSan、ASan/UBSan 下完整 `test_connection_group_facade` 均通过。测试夹具为此使用专属消息 ID，避免与其他复用相同 callback 的 fixture 串扰所有权。本次只增加测试、报告、台账和版本号，不改运行时/API/wire；它只证明单条消息保留时另一个 DATA 成员可进展，不覆盖全局预算耗尽、Zstandard/RAW 组合和持续饱和压力。详见
[多 DATA 成员慢消费者隔离回归](rpc_v2_group_slow_consumer_multi_data_20261025.md)。

dev.78 为 #241 补充真实连接组压缩慢消费者回归：两个不同 DATA 物理连接分别提交 Zstandard DATA；第一条解压消息被应用接管并持有期间，第二条仍完成解压、正文校验、服务端交付与释放。测试以测试链接观测确认两条消息实际经压缩提交，并在第二条完成后再次校验首条解压缓冲仍有效。Release 连续 5/5、TSan、ASan/UBSan 下完整 `test_connection_group_facade` 通过；忽略这两个消息的 Zstandard 选择时，新压缩提交断言按预期失败。本次只改测试、记录与版本，不改生产代码、API/ABI 或 wire。初始将帧上限设为 32 字节导致 route 前导无法接入，已移除该无关限制；完整运行中偶发单次销毁未立即收敛，改用已有的异步销毁重试屏障后连续 5/5 稳定。仍未覆盖全局预算耗尽、RAW/Zstandard 混合、多个应用持有者和持续饱和公平性。详见
[压缩与多 DATA 慢消费者隔离回归](rpc_v2_compressed_group_slow_consumer_20261025.md)。

dev.79 为 #241 增加 Zstandard/RAW 混合慢消费者回归：第一 DATA 成员的 Zstandard 解压消息被应用持有期间，第二物理 DATA 成员完成另一条 Zstandard 消息并释放，再交付一条 RAW 消息并释放；两次进展后首条缓冲仍逐字节完整。GCC Release 完整 `test_connection_group_facade` 连续 5/5、TSan、ASan/UBSan（leak detection）通过。只改测试、记录和开发版本；未耗尽 Server 全局预算，也不覆盖多条应用消息并发持有或持续饱和公平性。详见
[压缩持有期间 RAW 同伴进展回归](rpc_v2_mixed_data_slow_consumer_20261025.md)。

dev.80 为 #241 增加 1 MiB Zstandard 正文持有场景：第一 DATA 成员解压后由应用持有，服务端只有一个压缩解码任务槽时，第二 DATA 成员仍交付 RAW 小消息并释放，首缓冲在同伴进展后仍完整。该用例发现原每任务 1 MiB Zstandard 工作区无法完成 1 MiB 窗口输入的编码；现将有界工作区上限提高到 4 MiB，并沿用 `tr_group_message_budget_required` 按任务槽计入预算；低层监听器手工预算夹具同步增加。Release 完整 `test_connection_group_facade` 连续 5/5、TSan、ASan/UBSan（leak detection）及项目完整 91 项 Release 测试均通过。用例不耗尽全局预算，也不覆盖多个应用持有者或持续饱和公平性。详见
[大正文压缩持有期间 RAW 同伴进展回归](rpc_v2_large_decode_slow_consumer_20261025.md)。

dev.81 在真实 TCP `pipeline_listener` 组件测试中实际执行 Zstandard 解码并由应用持有输出，再以受控预算预约填满监听器共享消息预算；精确满额期间，另一条独立 DATA 成员上的 RAW 消息仍交付，持有缓冲逐字节有效，释放预约后账目恢复，释放持有输出及销毁 listener 后归零。负向变体验证了若 RAW 路径多申请 1 字节预算，该回归会因同伴未交付而失败；变体已撤销。Release 完整 91 项测试、该定向测试连续 5 次、ASan/UBSan（含泄漏检查）和 TSan 均通过。GitHub push CI、PR CI 与 RPC benchmark smoke 均通过：[`40166369925`](https://github.com/MyBlackHole/c-trpc/actions/runs/40166369925)、[`40166375224`](https://github.com/MyBlackHole/c-trpc/actions/runs/40166375224)、[`40166375029`](https://github.com/MyBlackHole/c-trpc/actions/runs/40166375029)。仅测试/记录/版本变更，无生产 API、ABI、wire 或运行时改动。测试预约模拟已记账持有额度，并非实际分配；这验证监听器组件在其消息预算满额时的 RAW 路径，不等于普通 Server 派生全局预算经自然应用持有耗尽。公共 Server 全局预算耗尽、多条应用消息并发持有及持续饱和公平性仍待验收。详见
[监听器消息预算满额时 RAW 同伴进展回归](rpc_v2_full_budget_raw_peer_20261025.md)。

dev.82 扩展真实公开 Server 慢消费者场景：六条 Zstandard 解码结果由应用并发持有，真实正文合计 5,308,416 字节；全部缓冲仍有效期间，另一 DATA 成员上的 RAW 消息完成回调。测试通过公开连接组 API 产生和持有这些正文，不注入或手工预约预算。GCC Release 完整 `test_connection_group_facade` 连续 5/5、ASan/UBSan（leak detection）、TSan 和完整 Release 测试 91/91 均通过。该回归验证多应用持有者下的真实跨成员进展，不读取 Server 私有预算账本，不能宣称全局预算精确耗尽；持续饱和公平性仍待验收。本次只改测试、记录和版本号，不改生产运行时/API/ABI/wire。详见
[公开 Server 多消息持有期间 RAW 同伴进展回归](rpc_v2_multi_hold_raw_peer_20261025.md)。

dev.83 在 dev.82 场景中加入有界 64 条 RAW 突发：六条 Zstandard 解码输出仍由应用持有时，另一 DATA 物理连接连续交付并校验所有 RAW 消息；测试链接探针确认突发始终使用第二成员的同一连接代次，随后再次逐字节验证持有缓冲。Release `test_connection_group_facade` 连续运行 5/5、ASan/UBSan（leak detection）、TSan 和完整 Release 91/91 均通过。该测试验证有限突发期间的跨成员进展，不模拟持续饱和源，不测量公平延迟/吞吐，也不读取全局预算账本；精确自然耗尽和持续饱和公平性仍待验收。本次只改测试、文档与版本号，不改生产运行时/API/ABI/wire。详见
[公开 Server 慢消费者期间 RAW 突发回归](rpc_v2_raw_burst_slow_consumer_20261025.md)。

dev.84 用测试链接包装 `tr_pipeline_listener_create` 捕获公开 Server 实际传给监听器的派生消息预算，不改生产钩子或预约预算。真实 Server 通过应用持有 36 条 1 MiB 解码输出消耗预算后，下一条完整 Zstandard 任务触发且仅触发一次真实预算拒绝；拒绝时 current=42,147,072、limit=46,908,160，剩余 4,761,088 字节，小于下一任务至少需要的 5 MiB 输出加工作区。压缩 DATA 成员随后关闭，另一物理 DATA 成员仍交付 64 条 RAW 消息；所有已持有缓冲在突发后仍逐字节有效。GCC Release 目标连续 5/5、ASan/UBSan（leak detection）、TSan 和完整 Release 91/91 通过。该证据证明真实预算预约自然拒绝，并不声称账面 current 数值精确等于 limit，也不证明持续饱和公平性；后者仍待验收。本次只改测试、文档与版本号，不改生产运行时/API/ABI/wire。详见
[公开 Server 自然预算拒绝期间 RAW 同伴进展](rpc_v2_server_natural_budget_rejection_20261025.md)。

dev.85 增加两个来源同时活跃的公开 Server 回归：一个 DATA 成员连续发送 32 条 1 MiB Zstandard 消息，前五条解码输出由应用持有、余下输出立即释放；另一成员由独立线程维持最多 32 条 RAW 消息在途，每收到一条即补发。计数取首条到最后一条 Zstandard 服务端回调之间的 RAW 交付，并断言至少 32 条；测试链接探针确认消息类型和两条不同物理连接，收尾再次逐字节检查五条持有正文。Release 首次运行、五次复跑及完整套件中的七次观测为 1,372–2,598 条 RAW；ASan/UBSan 为 4,514 条、TSan 为 2,579 条。Release facade 定向目标重复通过，ASan/UBSan、TSan 和完整 Release 91/91 通过。本测试证明固定 32 条压缩批次期间的并发 RAW 进展，不设公平延迟/吞吐阈值，不证明长期持续饱和公平性；后续需明确负载时长、配比、延迟边界和稳定基线。本次只改测试、文档与版本号，不改生产运行时/API/ABI/wire。详见
[公开 Server 并发 RAW/Zstandard 进展回归](rpc_v2_server_concurrent_raw_zstd_progress_20261025.md)。

dev.86 修复 dev.85 首轮 GCC Debug CI 失败的测试容量配置。旧场景只配置 8 个服务端压缩任务槽，却在服务端尚未消费完队列时提交 32 条 Zstandard；本地 Debug 复现服务端因任务槽耗尽返回 `TR_ERR_STATE` 并关闭 DATA 成员。现将容量设为 32，与固定批次相匹配，不更改生产路径。GCC Debug/Release 完整套件均 91/91；Release 定向复跑 6/6，ASan/UBSan 和 TSan 定向目标均通过。dev.86 七次 Release 观测的并发区间 RAW 进展为 1,489–2,842 条，Debug 四次为 154–273 条。该修复消除了压力测试配置造成的假失败；它仍只证明有限批次进展，不覆盖长期饱和公平延迟/吞吐。仅测试、文档和版本变更，生产行为/API/ABI/wire 不变。详见
[公开 Server 并发 RAW/Zstandard 进展回归](rpc_v2_server_concurrent_raw_zstd_progress_20261025.md)。

dev.87 将公开 Server 双 DATA 场景扩展为四批、总计 128 条 1 MiB Zstandard；每批 32 条完成回调后再提交下一批，服务端任务容量保持 32。另一成员持续维持最多 32 条 RAW 在途，每批都断言首末压缩回调之间至少交付 32 条 RAW，并验证五条持有正文。GCC Release 目标 7/7，四批 RAW 进展总和为 6,397–11,011；GCC Debug 目标 4/4，为 2,150–3,231；Clang Release（8,592 条）、ASan/UBSan（19,238 条）和 TSan（11,705 条）定向目标均通过，完整 GCC Release 套件 91/91（131.980 秒）。该切片加强多轮有限进展证据，仍不建立延迟/吞吐 SLA 或长期持续饱和公平性；#241/#235 的资源与活性验收未完成。本次仅测试、文档和版本变更，无生产运行时/API/ABI/wire 改动。详见
[公开 Server 并发 RAW/Zstandard 进展回归](rpc_v2_server_concurrent_raw_zstd_progress_20261025.md)。

dev.88 将同一测试扩为八批、256 条 1 MiB Zstandard；32 条 RAW 在途窗口、逐批至少 32 条 RAW 进展条件与五条持有输出不变，并新增每批及总压缩阶段单调墙钟计时。GCC Release 定向目标六次通过，总阶段 390–433 ms、每批 RAW 1,718–3,171 条；GCC Debug、Clang Release、ASan/UBSan（leak detection）、TSan 定向目标通过；完整 GCC Release 91/91（130.110 秒）。这些为当前开发机的有限场景观测，没有跨机器基线、每条消息最大等待界或公平比例阈值，故不关闭长期饱和公平性验收。本次只改测试、文档与开发版本号，无生产运行时/API/ABI/wire 改动。详见
[公开 Server 并发 RAW/Zstandard 进展回归](rpc_v2_server_concurrent_raw_zstd_progress_20261025.md)。

dev.89 增加同一 Split Channel 跨 lane 的 RPC Record Zstandard 正文活性回归：暂停接收端 BULK 后，16 KiB 正文在 `tx_sent_bytes == tx_send_limit`（实测 4,144/4,144）等待网络信用；CONTROL lane 的 128 字节 Zstandard 正文仍先完成。恢复 BULK 接收后，大消息发送进度恢复至 16,494 字节并完整交付。GCC Release 完整 `test_rpc_record_stream` 连续 5/5、GCC Debug 完整目标通过，五次 Release 边界读数相同。该项证明单个 lane 的信用耗尽不阻止另一个 lane 的可发送小压缩 RPC 消息；不覆盖 Channel `TR_FRAME_F_COMPRESSED` 分帧调度、共享 DATA TX 项池耗尽、同 lane Stream 之间的最大等待界、持续饱和 SLA 或跨机器基线。仅测试、记录和开发版本变化，无生产运行时/API/ABI/wire 改动。详见
[Split Channel 压缩窗口跨 lane 同伴进展回归](rpc_v2_compressed_tx_cross_lane_progress_20261025.md)。

dev.90 为通用 Channel `TR_FRAME_F_COMPRESSED` 增加真实 Split Channel 跨 lane 信用回归并修正其生产发送标志：BULK 大压缩 DATA 在服务端暂停接收后停于 128/128 字节，CONTROL 小压缩 DATA 先完整交付；恢复接收后 BULK 正文逐字节正确且 TX 推进至 330 字节。失败根因是发送器错误附加 `LANE_BULK`，而 DATA 接收标志校验只接受 `FIRST/LAST/COMPRESSED`，导致 BULK 流收到 `TR_ERR_STATE` RESET。修复后不再向 DATA 编码 lane，接收仍由 Stream 登记和连接代次校验。GCC Release 完整 `test_transport` 连续 5/5、GCC Debug 完整目标通过，完整 Release 套件 91/91（141.861 秒）。本次改动 Channel 内部运行行为，但不改变公开 API/ABI 或 wire 格式。该回归只验证有界单流跨 lane 场景；长期持续饱和公平性、每条消息等待/吞吐阈值、共享 DATA TX 池耗尽和更广预算/取消组合仍待验收。详见
[Split Channel 通用压缩 DATA 跨 lane 信用进展回归](channel_compressed_data_split_lane_progress_20261025.md)。

dev.91 在已有 Shared Channel 多帧压缩测试中让并发快 Stream 使用逻辑 BULK lane：慢 CONTROL 压缩 Stream 大于当前窗口并等待信用时，BULK 压缩小消息先到达，之后 CONTROL 消息恢复并逐字节交付。该场景覆盖两条逻辑 lane 共用一个物理连接时，`LANE_BULK` 仅用于 OPEN 路由、DATA 标志不再重复携带 lane 的约束。GCC Release 完整 `test_transport` 连续 5/5，GCC Debug 完整目标通过，完整 Release 套件 91/91（136.559 秒）。本次仅测试、台账和版本号变更；长期公平 SLA、共享 DATA TX 项池耗尽及更广预算/取消组合仍待验收。详见
[Shared Channel 逻辑 BULK 压缩 DATA 回归](channel_compressed_data_shared_bulk_lane_20261025.md)。

dev.92 将 Split Channel 跨 lane 信用回归扩展为两个方向同时发送：Client 的 BULK 大压缩消息在 Server 接收端停于 128/128 窗口时，Server 仍能在已打开的 CONTROL Stream 回发小型压缩消息，Client 校验回发正文后恢复 BULK 并完整接收大消息。该场景两侧都启用 Zstandard 接收并分别校验消息 ID、Stream ID 与正文，覆盖一个方向的窗口受阻时另一个方向仍有 TX/RX 进展。GCC Release 完整 `test_transport` 连续 5/5，GCC Debug 完整目标通过，完整 Release 套件 91/91（136.923 秒）。仅测试、台账和版本号变化；尚未覆盖双向各自窗口同时耗尽、长期公平 SLA、共享 DATA TX 池满载及更广预算/取消组合。详见
[Split Channel 压缩 DATA 双向同时发送回归](channel_compressed_data_bidirectional_cross_lane_20261025.md)。

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
既有告警行号已按未变函数体证据校准，8 条告警及 `unverified` 状态保留。2026-10-29 接续核对确认
合并后 main CI、#234 部分交付登记与远程分支清理均已完成，Task 5 已据实勾选；
异步凭证属于 TLS-D2，资源总账和公网验收属于 TLS-E。

## 持续开发顺序

dev.179 修复 [TLS 发送排空后残留写兴趣](rpc_v2_tls_idle_interest_20261029.md)：正常响应后
发送队列为空却仍监听 EPOLLOUT，空闲连接持续占用 I/O owner。新真实 TCP 回归先在
queued=0/events=2005 处失败；队列结算后重新计算兴趣后通过，读监听保留。GCC
Release 全量 95/95、Clang Release/ASan/UBSan/TSan 各十二目标通过。固定输入的
15 秒空闲 Server CPU 为旧 14.99 秒、修复后 0.59 秒；100 RPS 的 30 秒对照为
18.12/4.72 秒，双方 3000/3000 成功。只据此闭合该忙转缺陷，不宣布容量、尾延迟
或整个资源/生命周期矩阵完成。新提交的 CI 须另行核对，旧诊断数据不改写为新版本。

2026-10-29 的[持续 TLS 握手压力诊断](rpc_v2_tls_handshake_stress_20261029.md)在同一
dev.178 Server 上完成 600 秒、38400 次静默/滴灌/ClientHello 中止/明文 TCP 连接，
期间先建连接的 60000 次 TLS/Zstd RPC 及压力后新连接的 64 次恢复均成功，Server
含预热 60096/60096 收敛。617 次采样线程始终 7、fd 为 8–40，RSS 未呈现按连接
数持续增长；退出峰值高于定时采样，不能把采样最大值当硬上限。原始元数据标签
错误已单独更正并保留原记录，修正驱动的 30 秒复核通过。本项不证明满 peer 时
新连接准入、OpenSSL/内核总硬预算、最坏输入、mTLS 或容量，相关任务继续开放。

ERR-A 分类表到生产 C 错误入口族的对照已审计，代表性真实 TCP、Channel、RPC Record 与 TLS 测试证据见[生产错误作用域对照审计](rpc_v2_error_scope_production_audit_20261026.md)。dev.153 的 #327 又扩展头 CRC 连接错误回归：错误到达时保留在途 worker，验证迟到 `OK` 不改变 `UNAVAILABLE` 终局；worker 退出后预算回到基线。GCC Release、TSan、ASan/UBSan 的完整 `test_rpc_record_endpoint_tcp` 均通过。这只增加第 2、7 项在该路径上的证据；#242 的 RESET/FIFO 完整矩阵、连接组接受边界、长期公平恢复及全量资源回收仍未完成。

| 范围 | 当前剩余工作 | GitHub |
|---|---|---|
| 错误隔离 | ERR-A 分类表与纯状态参考模型已实现；真实 RPC Record Stream 覆盖 RAW/压缩接收配额失败、预算保留期间 CONTROL keepalive 往返和同 Channel 同伴恢复；真实 TCP Endpoint 覆盖重复 OPEN、业务 `PERMISSION_DENIED`、未知方法、非法 OPEN 元数据期间活动同伴继续完成，以及头 CRC 错误导致全部活动调用唯一 `UNAVAILABLE` 终结；分类到生产 C 错误入口族的对照见[审计记录](rpc_v2_error_scope_production_audit_20261026.md)；该映射不代表 #242 全部验收完成，listener draining 后拒绝未完成 CONTROL/DATA route 前导已有真实回归，当前覆盖同一 CONTROL 下两个不同 DATA member 和另一个 pending CONTROL route。普通/压缩 DATA 类型标志覆盖活动流与 RESET 丢弃态，单连接 GOAWAY cutoff 和已接纳 CALL_OPEN 未知结果/不重放有回归。ERR-B 本地 ID 耗尽及 GOAWAY 排空集成、墓碑 FIFO 淘汰、Stream/物理连接代次、split 连接相同 ID 双墓碑、RESET/CLOSE 顺序隔离、驻留重复 RESET 与索引 FIFO 周转各有回归，其他 RESET/FIFO 组合仍待验收；ERR-C 信用与组合故障已有 RESET 完成后及 RESETTING 窗口释放不补信用、合法在途 WINDOW_UPDATE 不恢复发送、跨消息债务只按阈值返还一次的回归，其余信用边界待验收；ERR-D 已有双 DATA 成员 drain、逐个 transfer 释放、未压缩 DATA TX、压缩任务/编码 wire 在途、单成员故障与同组替代、双组同 shard 先后及并发故障隔离、双组各自两轮替代及组级关闭回归；dev.120 增加真实 TCP 双组 32 轮并发故障替代回归；dev.121 增加具有活动 DATA、stream affinity 和 RESERVED route 时 CONTROL abort 的清理及旧 epoch 前导隔离组件回归；dev.122 增加真实 TCP CONTROL 关闭后活动 DATA 与 transfer affinity 回收、相同 group ID epoch 71→72 接管及旧 DATA route 拒绝回归；dev.123 将真实 TCP 故障场景扩展为一条活动 DATA、一条空闲 DATA 和一条 RESERVED route，验证三类旧 route 均不能影响 epoch 72 新组（Release、TSan、ASan/UBSan 通过），详见[双组高频成员故障替代回归](rpc_v2_group_repeated_failure_stress_20261026.md)、[CONTROL 致命拆除与 epoch 路由回归](rpc_v2_group_control_abort_epoch_20261026.md)、[真实 TCP CONTROL 故障与 epoch 切换回归](rpc_v2_group_control_loss_tcp_epoch_20261026.md)和[多状态 TCP CONTROL 故障隔离回归](rpc_v2_group_control_loss_heterogeneous_epoch_20261026.md)；dev.124 增加两个独立连接组的真实 TCP CONTROL 故障隔离回归：关闭 A 组 CONTROL 后，B 组的 CONTROL/DATA 和活动 transfer 仍工作，且 B 组继续完成 DATA 收发（Release、TSan、ASan/UBSan 通过），详见[跨组 CONTROL 故障隔离回归](rpc_v2_group_control_loss_peer_isolation_20261026.md)；dev.125 增加 32 轮真实 TCP CONTROL 关闭与相同 group ID epoch 重建，逐轮检查 DATA、active transfer 和连接计数归零、成员 generation 前进，并在第 33 个 epoch 拒绝最早和最近的旧 route（Release、TSan、ASan/UBSan 通过），详见[32 轮 CONTROL epoch 轮换回归](rpc_v2_group_control_epoch_replacement_stress_20261026.md)；dev.126 增加旧 CONTROL 断开时已进入 listener、但尚未提交 route 的 DATA socket 回归：跨到 epoch 72 后提交 epoch 71 的 route 必须关闭，且不消耗新 epoch 的 DATA 预留，随后有效 route 与 TRANSFER_READY 仍成功（Release、TSan、ASan/UBSan 通过），详见[CONTROL 故障期间 pending DATA route 的 epoch 隔离回归](rpc_v2_group_control_loss_pending_preface_epoch_20261026.md)；dev.127 增加 RESETTING 窗口释放已交付 DATA 的信用隔离测试（Release、TSan、ASan/UBSan 通过），详见[RESETTING 期间释放载荷的信用隔离回归](rpc_v2_reset_release_during_reset_20261026.md)；dev.128 增加小消息跨阈值累计后在 RESETTING 窗口释放下一条消息的信用隔离测试（Release、TSan、ASan/UBSan 通过），详见[RESET 时跨消息信用债务隔离回归](rpc_v2_reset_cross_message_credit_debt_20261026.md)；更长压力和更广组合仍待验收；ERR-E 墓碑固定预算和复合查询索引、真实 Channel 128 次 RESET/32 轮墓碑环周转、256 路小表确定性碰撞、max_streams=40000 的顺序 FIFO 周转和满历史下 256 路同 home 碰撞簇 FIFO 周转、真实 Channel 40k 历史填满与首次淘汰，以及删除成本计数覆盖 backward-shift 的最大容量 256 路同 home 簇已有回归；40k 键同 home 索引层最坏输入及真实 Channel 高频 RESET 成本已有本地初测（dev.76）；受控基线、持续压力和更广信用/丢弃组合仍待验收 | #242 |
| 资源与活性 | 分域/全局预算账本、压缩、多 DATA 与慢消费者组合矩阵；共享 Channel 的 L/W 与持有释放已有回归；单条 RAW 消息、Zstandard 解压消息、Zstandard/RAW 混合、1 MiB 解压正文持有、监听器消息预算满额、公开 Server 多应用持有和持有期间 64 条 RAW 有界突发均可跨 DATA 成员进展（dev.77–.83）；普通 Server 派生全局预算下自然持有输出导致下一压缩任务真实预算拒绝，且拒绝后 RAW 同伴继续进展（dev.84）；公开 Server 双来源发送回归扩展到八批共 256 条 Zstandard（dev.85–.88）；Split Channel 的 RPC Record 正文（dev.89）和通用 Channel 压缩 DATA（dev.90）均证明 BULK 大压缩消息耗尽窗口时 CONTROL 小压缩同伴仍先完成，之后 BULK 恢复；Shared Channel 逻辑 BULK 压缩流等待同一物理连接上的 CONTROL 大流而仍可先交付（dev.91）；双向压缩发送交错中，一个方向的 BULK 大流受阻时，反向 CONTROL 小流仍交付并校验（dev.92）；Shared Channel 的唯一 DATA TX 项由 BULK RAW Stream 占用时，CONTROL 压缩 Stream 登记容量就绪等待，释放后完成交付，双方 lane 与 TX 池归零经校验（dev.93）；Split Channel 两端 BULK 压缩窗口同时耗尽至 128/128 时，双向 CONTROL 压缩消息均先交付，恢复后两边正文和信用均完成校验（dev.94）；Shared Channel 唯一 DATA TX 项由 BULK RAW Stream 占用时，CONTROL 与另一条 BULK 压缩 Stream 两个等待者均在池项归还后恢复交付（dev.95）；同一池满场景中 RESET 一个 BULK 压缩等待者后预算立即释放，CONTROL 与存活 BULK 同伴恢复、双向 RESET 收敛且池归零（dev.96）；dev.99 在 RESET 一个等待者后验证三个存活压缩流首帧各一次，并按相同顺序继续轮转两轮，详见 [三存活压缩流公平轮转回归](channel_compressed_data_tx_three_waiter_fairness_20261025.md)；dev.100 在 RESET 已确认遇到池满的 BULK 候选后验证四个存活压缩流首帧各一次，并按相同顺序继续轮转两轮，详见 [四存活压缩流公平轮转回归](channel_compressed_data_tx_four_waiter_fairness_20261025.md)；dev.101 在同一场景中逐帧检查 24 个 DATA 帧，四条存活流完成六轮严格轮转，变异测试确认停滞游标会被捕获，详见 [四存活压缩流六轮公平性回归](channel_compressed_data_tx_six_round_fairness_20261025.md)；dev.102 在同一场景扩至六条存活流和 36 帧六轮轮转，六次 Release 定向测试通过，变异测试同样捕获停滞游标，详见 [六存活压缩流六轮公平性回归](channel_compressed_data_tx_six_survivor_fairness_20261025.md)；dev.103 保持六条存活流，把高熵正文扩至 2 KiB 并检查 72 帧/12 轮顺序，六次 Release 定向测试通过，详见 [六存活压缩流十二轮公平性回归](channel_compressed_data_tx_twelve_round_fairness_20261025.md)；dev.104 直接验证初始窗口 W=1024 下 L=1025 的 RAW 消息以 TR_ERR_BAD_LENGTH 终结拒绝、信用不变且调用方仍可释放缓冲区，目标 Release transport 回归及停滞窗口检查变异均已验证，详见 [Stream 单消息窗口准入回归](channel_stream_message_window_admission_20261025.md)；dev.105 在 Client/Server 接收窗口分别为 1 KiB/2 KiB 时验证双向发送上限取对端窗口、L=W 正文交付及反向 L>W 拒绝；回显对端窗口的变异会被捕获，详见 [非对称 Stream 初始窗口回归](channel_asymmetric_stream_windows_20261025.md)；dev.106 补齐三档关系：Client→Server 为 L=1536<W=2048，Server→Client 覆盖 L=W=1024 与 L=1025>W=1024 拒绝；错误使用本地窗口的变异会被超窗断言捕获，详见 [Stream 消息长度与非对称窗口三档回归](channel_stream_window_inequality_20261025.md)；dev.107 继续在两个非对称窗口方向应用持有首消息时探测剩余信用：Client 的 1,536/2,048 后追加 513 字节、Server 的 1,024/1,024 后追加 1 字节均返回 TR_AGAIN 且账目不变；两端释放后重试同一缓冲区并交付，Release 目标连续 3/3 通过，详见 [非对称窗口应用持有与信用恢复回归](channel_asymmetric_window_hold_recovery_20261025.md)；dev.108 将真实 TCP 多帧重组场景压到 DATA TX=1、RX=2、重组池=1 和单项发送缓冲池，1,500 字节正文跨六帧交付；应用持有期间唯一重组槽耗尽，释放后恢复。Release 目标连续 4/4 通过；既有 parser 单字节分片测试仍独立覆盖逐字节解析，详见 [小池预算下的多帧消息重组回归](channel_fragmentation_minimal_pools_20261025.md)；dev.109 在真实 TCP Channel 中以小于 64 字节的 Zstandard 编码体传输 4 KiB 正文，实际 wire 小于 128 字节窗口并由服务端完整解码交付，详见 [小编码体大解码正文接收回归](channel_compressed_expansion_20261025.md)；dev.110 在真实 TCP Channel 中验证 Zstandard 帧头未携带 content size、envelope 声明 4 KiB 解码长度时仍能完整交付且接收与消费 wire 计数相等，详见 [未知帧内容长度压缩消息接收回归](channel_zstd_unknown_content_size_20261025.md)；dev.111 以有效 Zstandard 帧配上少报 1 字节的 envelope 解码长度，验证真实 Channel 只 RESET 当前流、应用交付数不增且池和活动流回到基线，详见 [压缩 envelope 虚报原始长度隔离回归](channel_compressed_forged_length_20261025.md)；dev.112 以有效、未知内容长度的 4 KiB Zstandard 帧声明 2 MiB 窗口，验证其超过 Channel 的 1 MiB 限制后只 RESET 当前流且无应用交付，详见 [压缩解码窗口超限隔离回归](channel_compressed_window_limit_20261025.md)；dev.113 以未知 content size 的有效 4 KiB 帧配上少报 1 字节的 envelope 长度，验证解码容量探针阻止超长输出交付并触发流级 RESET，详见 [未知长度帧超出 envelope 解码容量回归](channel_compressed_unknown_output_overrun_20261025.md)；dev.114 以实际输出比 envelope 少 1 字节的有效未知长度帧验证 Channel 拒绝短输出、不交付正文且流和接收池回到基线，详见 [未知长度帧短于 envelope 声明长度回归](channel_compressed_unknown_output_underrun_20261025.md)；dev.115 修正压缩多帧回归的零 body 切点，使首个 DATA 帧携带完整 envelope 与部分 Zstandard body、后续帧携带剩余 body，并验证完整解码交付，详见 [压缩 body 跨 DATA 帧接收回归](channel_compressed_body_fragmentation_20261025.md)；dev.116 将未知 content size 的有效 Zstandard 帧与跨 DATA 帧 body 切分组合，首帧携带 envelope 和部分 body，后续帧承载余量；真实 Channel 完整交付，详见 [未知长度压缩 body 跨 DATA 帧接收回归](channel_unknown_size_compressed_body_fragmentation_20261025.md)；dev.117 验证两段各自有效的 Zstandard 帧串接后被真实 Channel 以流级错误拒绝，同时保留单帧后跟垃圾字节场景，详见 [串接有效 Zstandard 帧拒绝回归](channel_concatenated_zstd_frame_rejection_20261025.md)；dev.118 启用 Zstandard 内容校验和后破坏帧校验字节，独立解码器与真实 Channel 均拒绝，应用不收到正文且缓冲池归还，详见 [Zstandard 帧校验和损坏隔离回归](channel_zstd_checksum_corruption_20261025.md)；dev.119 在真实 Channel 中验证合法 skippable frame 与 dictionary ID 非零的 Zstandard 帧均只复位目标流、不交付正文，详见 [不支持的 Zstandard 帧隔离回归](channel_unsupported_zstd_frame_isolation_20261025.md)。四个及以上存活者的长期公平性、每条消息延迟界、吞吐阈值、跨机器稳定基线及更广预算/取消组合仍待验收；dev.84 测试不要求 current 数值等于 limit | #241、#235 |
| 运行模型 | dev.154 已拒绝闭合的跨 Reactor owner 等待环；dev.155 增加 Server V2 父上下文持有与子 Call 取消/deadline 传播；dev.156 增加 Server 方法钩子；dev.159 增加通用异步操作池并接入 authorize、TLS provider、MessageService；dev.163–165 通过真实 TCP 回归覆盖 CallRef 与迟到 authorize、terminal callback 内 hold、drain 与 destroy/release 屏障；dev.166 覆盖一条 A→B→A 嵌套提交链；dev.167 覆盖已接受异步操作遇到 Reactor completion 队列满载、恢复和停止回收；dev.168–169 覆盖 TLS Client/Server provider fetch/cancel 回调返回屏障与迟到完成终局。证据见[父子上下文记录](rpc_v2_parent_context_20261026.md)、[方法钩子记录](rpc_v2_method_hooks_20261027.md)、[异步操作内核记录](rpc_v2_operation_kernel_20261027.md)、[完成队列背压记录](rpc_v2_operation_completion_backpressure_20261027.md)、[TLS Client 回调屏障](rpc_v2_tls_provider_fetch_cancel_barrier_20261027.md)、[TLS Server 回调屏障](rpc_v2_server_tls_provider_fetch_cancel_barrier_20261027.md)。#243 仍待更广的跨 Reactor/嵌套端到端语义、RPC/TLS/压缩各调用方的完成/回调/资源回收全量屏障证明、其他 command/completion 队列满载契约与跨 TLS/压缩共享调度；dev.169 远端 CI 验收待完成 | [#243](https://github.com/MyBlackHole/c-trpc/issues/243)；[异步操作内核 PR #332](https://github.com/MyBlackHole/c-trpc/pull/332)（base: #331） |
| TLS 与身份 | TLS-C、TLS-D1 已合入主线（PR #311）；TLS-D2 与 TLS-E 包装对象预算切片在 PR #312，当前 head `d9c73b2758fc9dca8e0b5ce82a23a204100516a8` 的 GitHub 检查 14/14 通过，仍 OPEN 且未合入。预算只覆盖 c-trpc 自有包装对象；OpenSSL 内部分配、内核 socket 缓冲、持续握手洪峰/长期负载、目标平台和独立 RPC 互通矩阵仍待验收 | #234 |
| 网络压缩 | 将已有独立 none/Zstd 原语接入普通 V2；方向协商和安全预算 | #235 |
| 服务目标 | 解析快照、选路、健康、等待就绪、有界 Attempt/重放 | #244 |
| 类型契约 | 用户选择自定义传输与业务格式，要求兼容演进及学习开源优点；[开源对照](custom_message_open_source_lessons_20261029.md)覆盖六个项目，[形状实验](custom_message_shape_comparison_20261029.md)的 224 个新旧组合与六个负对照通过。[具体 CTM1 提案](../superpowers/specs/2026-10-29-ctm1-wire-contract-design.md)按最新选择撤回外置兼容目录，改为 OPEN 提供有序支持集、独立 SELECT 确认共同描述版本，首条正文等待确认，无法无损降级时明确失败；新端须保留历史 codec/入口。修订待审阅，生产实现未获批准、尚未开始，codec、历史兼容/生成工具、协商状态机和 C/Rust 桩仍待交付 | #245 |
| 后端决策 | 明确字节/流/消息/RPC 契约，形成 TRP 与 HTTP/2 等价评估证据 | #246 |
| 横切验证 | TEST-A 与 TEST-B1–B4 已收敛至 Draft PR #321（14/14 检查通过），TEST-C1–C3 已收敛至 Draft PR #323（13/13 检查通过）；旧 PR #317–#320、#322 已关闭，提交仍可从汇总分支追溯。仍缺通用生产 C 故障时序、TEST-B 完整事件字母表、真实网络/派发、TEST-D 集成平台矩阵、TEST-E 分层诊断与 40k 容量性能 | #247 |
| 最终核对 | 将已实现能力逐项映射到验收，收口阶段性架构与模块任务 | #239、#240、#248、#154 |

dev.97 修复并回归验证 Shared Channel 多帧压缩 DATA 在共享 TX 池恢复后的两流轮转：高熵压缩
消息被拆为多个不超过 128 字节的 DATA 帧，RESET 一个池满等待者后，CONTROL 与存活 BULK
压缩流的首帧由不同 Stream 先后取得发送机会，完整消息均交付且池/预算归零。回归先于修复
稳定失败，暴露池满分支将公平游标越过未获得发送机会的 Stream。GCC Release 定向目标修复后
连续 2/2，完整 91 项套件通过。该证据只覆盖两个存活压缩等待者的有限首帧轮转；三条以上
存活者的长期公平、每条消息等待界、吞吐阈值、跨机器基线和更广取消/预算矩阵仍待验收。
详见 [Shared Channel 多帧压缩 DATA 池恢复公平轮转回归](channel_compressed_data_tx_fairness_20261025.md)。

长期治理 #249 属于持续维护职责。正式发布仍需完成上述有限范围的实现和验收；当前不得把 dev 预览称为已完成的 V2。
