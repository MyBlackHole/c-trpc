# R17：RPC V2 开发预览的发行边界与后续责任

日期：2026-10-23。产品基线：`dd3332ee5f8229a5f0ef4f76531f08ae257a0562`，
tree `aa0d03075a5a719e622ca9cae1cf08afbe7fd6c9`，版本 `2.0.0-dev.1`。
任务：[RPC 清退台账 #290](https://github.com/MyBlackHole/c-trpc/issues/290)，
前置：[R16 精确合并验收](https://github.com/MyBlackHole/c-trpc/issues/290#issuecomment-6338921868)。

## 1. 本次结论与责任

V1→V2 清退已进入主线，当前版本可作为既定平台上的 **V2 开发预览与集成验证基线**。
本记录完成发行边界评估，不创建稳定版、发布标签、部署批准或新的兼容性承诺。
安全能力、目标环境容量、长期运行和其他专项仍按原任务验收。

本次由当前主会话执行材料核对与文档审阅，未使用子 agent，不声称独立审批。
产品测试证据来自下表所列精确提交的已完成 CI；本次未在本地重跑产品测试、
性能实验、漏洞扫描或跨架构矩阵。R17 文档提交自身的检查与合并结果记录在
#290，不把产品基线的 CI 冒充未来文档提交的 CI。

长期维护责任人、发行批准人和目标部署验收负责人尚待指定。这不妨碍核销
有明确边界的清退任务，也不代表月度维护或发行签署已经执行。
[长期规则 #249](https://github.com/MyBlackHole/c-trpc/issues/249) 继续适用；
R16 已移除的额外独立审查/逐条诊断合并前置要求不因本记录恢复。

## 2. 能力、平台和兼容边界

| 领域 | 本基线已有交付 | 使用边界 |
|---|---|---|
| 普通 RPC 门面 | `tr_client_create()` / `tr_server_create()` 只构造 V2；创建时冻结方法表；一元和流式共用记录/终局内核 | ONE/MANY 四种组合、零条 MANY、零字节消息；不提供 `TR_RPC_NONE` |
| 调用生命周期 | 元数据、单调期限、取消、背压恢复、排空、稳定语义统计与异步生产者 | 借用 Call 句柄不是持有引用；producer 有独立配额和 release 责任；终局、最后回调和存储回收不是同一屏障 |
| V1 退役 | 旧 Call、Endpoint、Executor Group、Wire 实现、专属安装声明与测试闭包已移除 | 不兼容旧 V1 对端；保留共享 V2 类型和独立 RAW Codec，解码视图仍借用输入 |
| 传输和协议 | 普通 RPC 使用共享 TCP Channel；帧封装版本 1、Channel 握手版本 2、RPC Record 版本 2 分层校验 | 三个版本不是同一版本号；不提供 HTTP/2 或 gRPC 互通；transport-only 连接组不等于 V2 RPC 连接组 |
| SDK | C11 静态库；九个公开头；gcc/clang/g++/clang++ 的安装消费测试 | C++11 验证 C 接口消费，内部仍是 C11；同版本头/库配套并重新编译，不承诺跨版本二进制 ABI |
| 平台 | Ubuntu 22.04 / Linux x86_64 上持续验证；Xmake 3.1.1；GCC/Clang 与运行时检查器 | arm64、32 位、大端、其他 libc 和最低内核版本未验收；QEMU 隐藏 SSE4.2 只验证 CRC 回退 |
| 安全与压缩 | 明文 TCP；独立 Message none/Zstandard 后端、预算与任务基础已经存在 | 公开 V2 网络路径未接入 TLS/mTLS 或消息压缩；底层后端存在不等于网络能力可用，CRC 不提供认证或保密 |
| 服务目标与编码 | 数字 IPv4 端点、手工方法描述及 RAW 字节载荷 | 普通 V2 RPC 不支持自动重连、透明重试、Call 重放、DNS/服务选路、类型化 codec/生成桩；低层 Channel 重连不恢复旧调用 |

完整使用合同见 [V2 SDK](../rpc-v2-sdk.md)、[平台与部署契约](../platform-support.md)
及[开发版本规则](rpc_v2_development_versioning_20261022.md)。限额是相应资源域的
契约，不能换算成进程 RSS 硬上限；取消不抢占用户业务，期限也不是任意锁竞争或
销毁过程的硬实时保证。RPC 成功不证明业务持久化或恰好执行一次。

项目已有 Apache-2.0 的 LICENSE、NOTICE 与[许可来源说明](../licensing.md)。
不能继续将 #154 原始基线的“根目录无 LICENSE”当作当前事实；本次没有进行
新的依赖漏洞或实际分发包审计，也没有改写历史第三方许可。

## 3. 精确提交的证据

以下四个工作流的 `headSha` 均为产品基线 `dd3332ee...`，本次重新读取状态，
并核对 R16 已下载的静态分析/退役/SDK 工件及新下载的性能诊断工件。

| 证据 | 实际结果 | 证明范围 |
|---|---|---|
| [CI 39764939113](https://github.com/MyBlackHole/c-trpc/actions/runs/39764939113) | 8/8 作业成功 | GCC debug/release、Clang release、ASan+UBSan、TSan、两套静态分析、Make/SDK 与 CRC CPU 回退 |
| [RPC smoke 39764939076](https://github.com/MyBlackHole/c-trpc/actions/runs/39764939076) | 5/5 作业成功 | release/ASan/TSan 的真实负载；Clang/debug 两项按既定策略仅构建，不能写成五次性能验收 |
| [diagnostics 39764939066](https://github.com/MyBlackHole/c-trpc/actions/runs/39764939066) | 1/1 作业成功 | 容量、扩展性、资源余量、40k 重复性、发生器扇出/槽位实验完成并保存结果；不设绝对吞吐门槛 |
| [格式 39764939102](https://github.com/MyBlackHole/c-trpc/actions/runs/39764939102) | 成功 | 修改源码格式检查，不证明运行语义 |

`rpc-v1-retirement-inventory` 记录 197 个 C 系列文件、74 个静态库成员；
退役精确名称、定义和跨对象引用为零，归档契约问题为零。
`rpc.c.o` / `rpc_wire.c.o` 不存在；RAW 编码/解码符号均来自 `rpc_codec.c.o`。
这证明约定的退役清单，不是全库模块依赖或所有动态行为的证明。

`build-smoke-logs/sdk-install.log` 记录四种编译器的公开头/门面消费成功，
每种编译器运行 9 个 V2 场景，共 36 次场景执行；包含 RAW、一元共同终局、
四种基数与元数据/压力、大消息所有权、取消隔离/排空、期限/排空、直接销毁、
统计/对端回收和异步生产者复制/停止。场景数不等于全库测试程序数。

## 4. 静态诊断仍有十条未裁定

GCC 11 与 Clang 14 的工件身份都指向 `dd3332ee...`，各覆盖 **43 个生产单元**，
无分析失败源码；观察集与精确基线相同，两份 `review_state` 均为 `unverified`。
告警新增、漂移或消失仍须由现有精确对照检查解释，未启用零告警模式。

| 分析器 | 当前位置与对象 | 条数 |
|---|---|---:|
| GCC | `src/transport/channel/channel.c:687:9`，`owned` | 1 |
| Clang | `src/execution/buffer.c` 的 `67:9`、`75:9`、`139:3` | 3 |
| Clang | `src/execution/command_queue.c:26:10`、`completion_queue.c:27:10` | 2 |
| Clang | `src/execution/reactor.c:320:10` | 1 |
| Clang | `src/execution/timer_queue.c:123:10`，`entries` / `heap` 各一条 | 2 |
| Clang | `src/transport/channel/channel.c:3275:10`，`build.channel` | 1 |

完整文本以 [GCC 基线](../gcc_analyzer_known_warnings.json)和
[Clang 基线](../static_analysis_known_warnings.json)为准。R14 删除整个旧 `rpc.c` 后，
原 GCC 1 条与 Clang 2 条随生产单元退出；这是 2/11 变为 1/9 的原因，
不能记为三个缺陷根因修复。

[2026-10-18 路径审查](release_static_risk_review_20261018.md)对当时 13 条路径
给出了模型误报判断，同时保留独立复核和 `unverified` 边界。该历史文档保留，
本次没有把它升级为当前所有调用者/故障路径的裁定。后续沿 #154 D1 更新
逐路径证据与故障覆盖，不创建一份重复告警总账，也不要求先清零才继续普通开发。

## 5. 性能诊断的负结果必须保留

已核对工件 `rpc-bench-diagnostics-gcc-release`（ID `12420160612`）的
`source-commit.txt` 与六份顶层 JSONL：均为该产品提交、`schema: 2`、
`api_generation: rpc_v2`，二进制摘要一致。环境为 Linux 6.8.0-1070-azure、
glibc 2.35、AMD EPYC 7763、CPU affinity 0–3，IPv4 回环；这是共享 CI runner
的实验环境，不是最低内核支持承诺或用户目标硬件。

40k 重复性实验每档 3 次、每次计划到达 10,000 个请求、16 个发生器。
原始汇总如下；`ok_rps` 是该实验成功响应速率，不是可承诺的容量：

| 公开容量档 | 无压力样本 / 总样本 | 原始分类 | 成功速率中位数（次/秒） |
|---|---|---|---:|
| `small_call_limit` | 0/3 | `reproducible_server_wall` | 4980.537 |
| `default_call_limit` | 0/3 | `reproducible_server_wall` | 6520.978 |
| `large_call_limit` | 0/3 | `reproducible_server_wall` | 6069.878 |

九次共计划到达 90,000 个：发生器丢弃 1,054 个，提交错误 69,911 个，
受理并完成 19,035 个；其中成功 13,895 个、unavailable 5,140 个，错误响应内容为零。
每档均出现控制 TX 池耗尽、发生器迟到/丢弃和调用错误。
`completed == accepted` 说明受理账本收敛，不表示全部到达被成功服务。

这些结果没有满足“40k 成功吞吐”的结论；原诊断没有这个性能通过阈值，
因此工作流成功与负结果并不矛盾。压力信号也不能单独确定根因或证明死锁。
保留[机器可读摘录与六份文件摘要](evidence/r17-release-boundary-20261023.json)，
原始工件保留期为 7 天，摘录不替代完整原始日志。

后继归属 #247（实验/故障证据）与 #241（控制资源与压力恢复）。
验收需要同语义/配置下的负载质量、成功率、尾延迟、CPU/RSS 和压力解除后恢复，
不能先增大默认池或用旧 V1 CSV 解释当前 V2。跨主机、长稳、TLS/压缩 ON 以及
与其他后端的对等性能比较均未在本次完成。

## 6. 原专项任务的当前映射

下表是发行评估所需的范围核对，不替代原 Issue 每条验收；旧 Issue 的历史描述
与当前源码不一致时，以明确提交的交付为准。下列任务保持开放，不整体核销。

| 原任务 | 当前已有交付或记录 | 仍需原任务验收的范围 |
|---|---|---|
| [#154 工程整改](https://github.com/MyBlackHole/c-trpc/issues/154) | 解绑/可重试回收、构造故障、单调期限、分析器、SDK/许可已有多轮实现和测试 | 原 A1–E3 尚需完成逐项当前证据账本；剩余告警、全生命周期/生产断言分类、generation 回绕、跨数据模型、工具价值及实际分发验证不能由本轮 CI 包办 |
| [#239 架构总审查](https://github.com/MyBlackHole/c-trpc/issues/239) | 公开默认 V2、R12–R14 旧核心与所有权退役已经进入 main | 其余 ARCH 专项分别收敛；不再重复实施 V1 清退，也不整体声明架构验收结束 |
| [#234 TLS](https://github.com/MyBlackHole/c-trpc/issues/234) | 需求与研究存在 | 非阻塞 TLS 1.3、身份验证、连接组授权、关闭/轮换、攻击负向与性能尚未交付 |
| [#235 Message/压缩](https://github.com/MyBlackHole/c-trpc/issues/235) | none/Zstandard 变换、预算/执行基础与 RPC 控制/业务记录分离 | 双向协商、公开网络接入、解压/CPU 限额与取消、秘密隔离、TLS 组合及收益验收 |
| [#240 统一调用语义](https://github.com/MyBlackHole/c-trpc/issues/240) | OPEN/MESSAGE/HALF_CLOSE/FINAL、四种基数、共同一元内核与旧协议退出 | 对原状态表/非法交错验收逐项核销，鉴权拒绝与安全/压缩组合仍需原范围核对；不能只凭门面切换关闭 |
| [#241 分域预算与活性](https://github.com/MyBlackHole/c-trpc/issues/241) | 有界复制/持有、预算释放重试、压力回归，公开方法上限补入协议开销 | L/W 不对称、解压/多 DATA/慢消费者全矩阵、分域成本、当前控制 TX 压力归因及恢复 |
| [#242 错误隔离与排空](https://github.com/MyBlackHole/c-trpc/issues/242) | V2 单流 RESET、终局恢复、共享 Channel 接收边界与门面排空 | 原矩阵中的恶意晚到帧/信用/丢弃预算及连接组多 DATA/epoch 接收边界完整验收 |
| [#243 异步能力与取消](https://github.com/MyBlackHole/c-trpc/issues/243) | 异步 producer 引用/背压、业务退出与终局回调分离、停止域回收 | 通用可持有 Call、父子取消/派生期限、跨 owner 嵌套循环与 TLS/压缩迟到完成组合 |
| [#244 服务目标/Attempt](https://github.com/MyBlackHole/c-trpc/issues/244) | 设计研究与低层数字 IPv4 连接基础 | 目标级解析/选路/健康、策略快照、有界等待与安全重试；不把低层重连当已实现 |
| [#245 codec/生成桩](https://github.com/MyBlackHole/c-trpc/issues/245) | RAW 与冻结方法表 | 统一描述、类型化有界解码、C/Rust 生成、黄金向量与生成物漂移检查 |
| [#246 传输后端](https://github.com/MyBlackHole/c-trpc/issues/246) | 当前 TRP 实现和研究候选 | 后端契约/替身、TRP 与 HTTP/2 的等价实验及 ADR；没有预先选定赢家 |
| [#247 模型/诊断](https://github.com/MyBlackHole/c-trpc/issues/247) | 确定性故障用例、实际 TCP/运行时检查器、稳定调用统计与 V2 诊断 | 可重放小状态模型、组合轨迹缩减、完整分层指标；本次性能负结果的受控复现与解释 |
| [#248 模块边界](https://github.com/MyBlackHole/c-trpc/issues/248) | Message/执行器/RPC 组件、Server peer 协议状态拆分、Runtime/V1 耦合退役 | Reactor 仍包含 TRP parser/wire/CRC；全库允许依赖/符号/回调图、受限链接和负向越界检查尚未整体验收，退役 inventory 不替代模块门禁 |
| [#249 长期治理](https://github.com/MyBlackHole/c-trpc/issues/249) | G01–G12 及任务登记/关闭规则已有 GitHub 记录 | #250 的仓库入口仍为草稿；维护/发行负责人、实际月度复核与规则检查运行证据分别登记，不将长期索引算永久全局 P0 |

旧工程成果不因 Issue 复选框未更新而失效。例如当前已有 LICENSE、CI 双分析器和
公开 V2 安装消费；反之，已有故障测试也不证明原 A1–E3 全部关闭。
[#251 的 ENG-A 研究](https://github.com/MyBlackHole/c-trpc/blob/d89bd301556b566db6d79d55dc8a37ea9dee8b85/docs/design-studies/20261015/issue-154.md)
已经定义“原发现→已有修复→当前代码→精确验证→剩余工作”的账本，后续复用它，
不另开一套工程整改任务。

## 7. 未合入候选和长期复核

本次核对时剩余七个开放 PR：

| PR | 保留原因 |
|---|---|
| [#220](https://github.com/MyBlackHole/c-trpc/pull/220) | 有界预读实验仍为草稿；未满足推广条件，不启用为默认能力 |
| [#237](https://github.com/MyBlackHole/c-trpc/pull/237)、[#238](https://github.com/MyBlackHole/c-trpc/pull/238) | 历史对标/Message 设计提议；须对当前实现审阅后决定处置，草稿内容不自动成为当前合同 |
| [#250](https://github.com/MyBlackHole/c-trpc/pull/250)、[#251](https://github.com/MyBlackHole/c-trpc/pull/251) | 治理入口与分项研究尚未合入，保留各自范围，不通过 R17 代为批准 |
| [#257](https://github.com/MyBlackHole/c-trpc/pull/257)、[#259](https://github.com/MyBlackHole/c-trpc/pull/259) | 旧 MessageService/Runtime 所有权方案仍有差异，需逐约束核对后处置，不能据功能相似直接删除 |

R16 删除了 34 条无开放 PR 引用的远端分支，剩余 main 与上述 PR 所需的 8 条分支；
这是 R16 完成时的快照。R17 自身的工作分支另行记录，结束后按相同引用核对清理。

长期规则的触发和本次执行范围：

| 规则领域 | 本次实际复核 | 后续触发 |
|---|---|---|
| G01–G04 模块/所有权/协议/资源 | 上表对照已有合同与验收，未重做全库生命周期证明 | 相关代码或配置变更时，沿原专项执行受影响的正/负回归 |
| G05–G06 安全/性能 | 明示 TLS 未提供；读取现有性能工件并保留负结果 | 网络信任边界或默认值改变、目标环境部署、实际 TLS/压缩接入前 |
| G07–G08 证据/平台/SDK | 复核精确 CI、工件身份、安装消费和未验证平台 | 新提交、新工具链/架构、候选/正式发行时 |
| G09 依赖/许可 | 读取现有许可和固定 Zstandard 依赖说明；未执行新漏洞扫描 | 依赖变更、安全通告、活跃维护期每月按 #249 复核 |
| G10–G12 诊断/文档/债务 | 校正当前告警统计、记录压力信号与原任务后继 | 每个里程碑、未解释失败、临时例外到复核日期时 |

上述是人工责任与触发条件，不是已配置的定时机器人或助手后台监控。

## 8. 下一项有限工作与 R17 关闭条件

下一项优先沿 **#247 的 V2 控制 TX 压力归因** 推进，并回链 #241：

1. 固定本记录的提交、容量、发生器数、到达率和完整计数，检查发生器迟到/丢弃
   与服务端控制池压力分别来自何处；不先修改默认池大小。
2. 在工作区保存可重放轨迹，缩小到能区分准入、发送背压、关闭/恢复与真实资源
   泄漏的场景；以相同错误范围验证其他流和压力解除后的前进。
3. 只有确定缺陷根因后才实施最小修复和回归；若属于容量限制，给出实测边界与
   代价。没有成功率/负载质量证据，不宣称达到 40k。

此项尚未实施，后续执行前在原 #247 登记唯一切片、责任、精确基线和完成条件。
#154 ENG-A、#248 边界验收及 #250/#251 候选处置继续保留，不并入本轮文档改动。

R17 的完成条件是本记录及当前文档校正进入 main、文档精确提交的适用检查通过、
#290 与原专项回链完成。届时可以关闭范围明确的 RPC 清退台账 #290；
其关闭不关闭 #154/#234/#235/#239–#249，也不代表稳定版发布获批。
