# 独立 Message 层压缩设计

**状态：Channel/Stream、RPC 与连接组 DATA 压缩路径及功能回归已完成；RPC TLS × 压缩基准已完成；Message 压缩与 V2 的整体发布验收仍未完成**<br>
**日期：2026-10-24**<br>
**关联：#235、#240、#241、#243**

## 目标

让 V2 普通 Stream、RPC 业务消息体和连接组 DATA 使用同一套可选 Message 层变换，
以方向性能力协商、逐消息授权和分开的资源账本保证安全。压缩保持默认关闭；
不改变 `none` 快路径的载荷所有权、复制数和线程路径。该设计不是 TLS 安全部署
完成的声明。

## 当前基线

- `src/message/message.h` 提供拥有型消息和共享/切片视图。
- `src/message/transform.h` 和 `zstd.c` 提供无字典有界 Zstandard 变换，校验实际
  解码长度、输出上限、窗口和工作区。
- `src/runtime/message_service.h` 提供限额任务执行和 owner 完成/停止后回收。
- `src/transport/channel/channel.h` 的 Channel HELLO 使用协议版本 3；帧头固定为
  TRP envelope v1。HELLO feature bits 包含 Stream RESET 和 Message 能力协商。
- `tr_stream_sendv()` 的 NONE 路径维持原有发送语义；显式压缩 DATA 通过独立的
  有界 TX 状态分帧推进信用，接收端预留编码存储后逐帧回补信用。
- `tr_rpc_record_stream` 按方法方向与物理 lane 能力处理 RPC MESSAGE 正文，RPC
  header 和 metadata 保持独立；V2 Client、Server、同步 Reply 和 Producer 已接通
  显式逐消息选择。
- 连接组 DATA 已接通逐路由能力协商、异步发送/接收、分片重组和应用消息所有权；
  服务端精确路由关闭/连接槽复用及客户端编码任务取消/代次替换的迟到成功用例均已覆盖；
  实现级侧信道路径检查、RPC 性能矩阵和连接组性能矩阵均已有记录；真实业务负载、生产网络
  与发布阈值验收仍待完成。

## 当前实现检查点（2026-10-24）

- 版本协调：主线为 `2.0.0-dev.17`，开放草稿 PR #312 为 `.21`、PR #313 为 `.19`；本地压缩候选
  设为 `2.0.0-dev.22`，高于三者且不改动两个 PR 的草稿状态。该协调只确定当前候选版本；若上游草稿
  在合并前继续增加版本或代码，集成时仍须按最新 head 重算版本并处理文件交集。

- Channel/Stream 压缩数据面已完成：能力代次、Zstandard envelope、MessageService
  收发、受预算约束的编码/解码存储、`C < W` / `C = W` / `C > W`、多流公平、重连和
  迟到任务处理记录在 `2026-10-24-message-compression-channel-data-plane.md`。
- RPC 正文适配已完成：方法级 request/response policy，Client、同步 Reply 和
  Producer 的压缩 API，RPC 记录分块发送，按序异步接收，以及拒绝、预算、取消、
  RESET、lane 退役、OWNER_STOPPED 回收用例记录在
  `2026-10-24-rpc-message-compression.md`。
- 连接组 DATA 适配已完成正常路径：逐 DATA route 协商、有限 Runtime MessageService
  预算、客户端显式 Zstandard 发送、服务端多帧重组和异步解码；压缩消息以
  FIRST|LAST 完整消息交付。应用 TAKE_OWNERSHIP 后，监听器会等消息释放才允许销毁。
  facade 与真实 mTLS 集成均验证了解码内容；未压缩路径仍保留逐帧交付。
- 新增 Pipeline Listener 边界验证：未协商 CAPS、未知 codec、信封解码大小超限、
  实际编码长度短于或长于信封声明值，以及 Zstandard 帧尾随/截断内容，都会关闭对应
  DATA 连接且不调用应用消息回调；另验证两条流的部分压缩消息交错后仍按各自身份重组。
  挂起中的服务端解码任务在其 DATA 连接关闭时收到取消；连接槽复用建立新 DATA 代次后，
  旧任务的迟到成功完成不交付消息，任务持有的消息预算返回 listener 固定状态基线。
- 新增连接组客户端代次替换验证：暂停已编码的 Zstandard worker，关闭旧 DATA 路由，
  在 worker 仍暂停时用相同 group/stream 建立新路由，再释放旧任务；旧任务未交付消息，
  新 transfer 保持活动且能发送新的压缩消息。真实 mTLS 用例覆盖旋转证书后的身份授权、
  新路由压缩发送，以及 `REVOKE_OLD` 后旧路由发送返回 `TR_ERR_CLOSED`。
- 验证结果：最近一次完整 `xmake test -P . -j1` 的 88 项全部通过，耗时 80.567 秒，
  已包含本轮连接组客户端路由代际替换、mTLS 撤销和 Reactor TX 池满载用例；连接槽复用、
  交错流及 Zstandard 尾随/截断 listener 用例也分别通过 release 与 ASan/UBSan。此前首次全套
  运行中 `test_rpc_v2_sdk` 曾在 Client 销毁断言失败，单独复跑 6 次及后续完整运行均通过，
  根因未复现。连接组 TLS 销毁用例按公开 Server 销毁契约对 `TR_ERR_STATE`/`TR_AGAIN`
  有界重试；随后完整全套通过。`test_pipeline_listener`、`test_connection_group_facade` 和
  `test_connection_group_tls` 在 ASan/UBSan 与泄漏检查下通过；ASan 先发现 Server 构造
  失败路径泄漏临时 shard 配置数组，已加作用域清理并复验通过。TSan 下
  `test_runtime_threads` 通过。Xmake 与 TLS 测试身份文件均限定在当前 worktree 的
  `build/`。
- RPC 边界验证已覆盖认证凭证样式及 peer-id 元数据哨兵，并确认 Message transform
  只接收 RPC MESSAGE 正文；待处理压缩发送期间完成 replacement handshake、迟到成功回调
  不能写入新 lane，以及 live encoded Message 阻止调用对象销毁的用例均已通过。
- 侧信道审查结论见下文：实现没有自动压缩、跨消息压缩上下文，也没有把 RPC 控制面、
  元数据或 TLS 身份送入压缩器；但库不能识别任意业务正文中的秘密和攻击者可控内容，
  调用方必须避免在同一条压缩消息中混合二者。该残余策略风险已补入公开 API 注释。
- 连接组压缩服务端通过 `max_inflight_compressed_messages` 显式配置解码任务容量；启用
  Zstandard 接收时必须大于 0，关闭时必须为 0，且不得超过归一化后的 Runtime 执行器队列容量。
  该值独立限制排队/执行任务数并计入消息内存预算；部分消息重组槽仍按活动传输数计算。
  没有可用任务槽时 DATA 路由以 `TR_ERR_STATE` 关闭。基准使用 1024 个任务槽以覆盖默认
  100 条预热和每客户端 1000 条测量的突发负载；这只是基准配置，不是生产容量建议。
- 后续范围：RPC 与连接组本机 TLS × 压缩矩阵已有记录；仍未覆盖生产正文分布、生产网络表现、
  项目认可的性能阈值或长期运行验收。压缩侧信道的业务正文策略也仍由应用负责。完成这些
  验收前不把整体 Message compression 或 V2 标记为完成。

## 架构边界

```text
RPC 策略/普通 Stream/连接组 DATA
                  │ 仅提交明确获准的逻辑消息体
                  ▼
        Message：none / 有界 Zstandard
                  │ 单消息编码体 + 声明解码长度
                  ▼
       Channel/Stream：能力、分片、信用、终止
                  │ TRP DATA 帧
                  ▼
       可选 TLS 字节端点 → TCP
```

- Message 只处理完整逻辑业务消息体；不解析 RPC 控制信封、不处理连接组路由前导、
  不压缩 Channel 控制帧。
- Channel 管协商与帧级状态，但不包含 Zstandard 算法实现。
- Reactor 管连接 I/O 和有界 TX 项；不压缩、不解压，也不保留无界消息队列。
- 公开 SDK 不暴露 `tr_message_service`、Reactor 槽位或压缩器内部对象。

## 协议扩展

### 能力交换

增加 feature bit `TR_CHANNEL_FEATURE_MESSAGE_CAPS`。它只表示双方识别后续的消息
能力控制帧；只有 HELLO/ACK feature 交集含该 bit 时才发送该帧。它不是双向共用
算法选择，也不能把任意 `feature_bits` 交集误作本端接收许可。

`TR_FRAME_MESSAGE_CAPS` 使用固定 12 字节 little-endian payload：

| Offset | 大小 | 字段 | 要求 |
| ---: | ---: | --- | --- |
| 0 | 2 | payload schema version | 必须为 1 |
| 2 | 2 | reserved | 必须为 0 |
| 4 | 4 | receive algorithm mask | 只允许已定义算法位；首版只含 Zstandard |
| 8 | 4 | max encoded wire bytes | 包含首帧压缩信封；0 仅在算法 mask 为 0 时有效 |

`none` 是隐式能力，不占算法位。每个物理连接各自完成交换；重连清除旧代次的
远端能力。若 feature 未协商，则不发新帧，连接继续使用 `none`。若 feature 已协商，
DATA 仍可在能力帧到达前发送 `none`；压缩 DATA 在远端接收能力就绪前必须拒绝。

能力帧必须使用 stream_id=0、message_id=0、flags=0、reserved=0。无效长度、未知
版本、未知算法位、能力与 feature 不一致或重复能力帧均为协议错误。能力帧不
承载业务数据，也不参与 Stream 窗口。

Channel 在本地压缩收发服务、内存预算和边界配置就绪后才通告相应接收算法。能力帧
交换完成后，普通 Stream 压缩 API仍按逐消息策略和当前物理 lane 能力决定是否准入；
连接替换会清除旧代次的远端能力。连接组适配须使用同一代次约束，不得缓存旧许可。

### 连接组 DATA 能力交换

连接组当前只支持客户端向服务端发送 DATA。压缩配置按方向配置：客户端允许显式选择
Zstandard 发送；服务端配置本地 Zstandard 接收许可和编码上限。现有
`tr_client_connection_group_send()` 继续表示 NONE；新增显式压缩选择入口，不做自动压缩
或隐式 fallback。

连接组没有 Channel HELLO。为避免服务端向旧客户端主动发送新帧，能力交换由显式启用
压缩的客户端在每条 DATA 路由完成 TRR1/TLS 附接后发起：客户端发送一个 12 字节
`TR_FRAME_MESSAGE_CAPS` 请求（客户端 DATA 接收暂不支持，因此本端 receive mask/limit
为 0）；服务端只在收到该请求后返回自己的 receive capability。服务端在收到请求前
不得发能力帧。能力状态与 DATA route/member generation 一起存放；DATA 连接关闭、替换
或 route 退役时清零。

旧服务端会忽略未知的 `MESSAGE_CAPS` DATA 控制帧；客户端没有收到应答时只允许 NONE，
显式 Zstandard 发送返回 `TR_AGAIN`（能力交换未完成）或 `TR_ERR_UNSUPPORTED`（已收到
无共同算法的应答），不影响 NONE 发送。能力帧不触发应用消息回调。服务端仅在当前 DATA
连接收到有效能力请求并配置了 Zstandard 接收后，才接受带 COMPRESSED 的 DATA。

客户端和服务端复用所属 Runtime 的 MessageService。连接组压缩 Message、工作区、任务
和输出均受由连接组的并发传输上限及服务端 `max_inflight_compressed_messages` 推导出的有限
分片预算约束。部分消息重组槽按活动传输数设置；完整编码消息进入 MessageService 后，使用
独立的有界任务槽，因此短时间内可排队/执行的完整消息数不再被活动传输数隐式限制。压缩接收先按
`(route generation, stream_id, message_id)` 预约完整编码体并复制每个 DATA 分片；只有
完成并成功解码后，才向应用回调交付一条完整消息。NONE 的逐帧路径、buffer 地址和
所有权保持不变。迟到任务必须匹配原 DATA 连接及 route generation，否则只释放输出。

### 压缩 DATA

增加 `TR_FRAME_F_COMPRESSED`。该标志只允许用于 DATA，并要求对应连接已协商
`MESSAGE_CAPS` 且对端声明接收所选算法。压缩标志随该逻辑消息的每个 DATA 分片
设置；FIRST/LAST 仍精确描述消息边界，BULK 仍精确描述物理 lane。

FIRST DATA payload 以 16 字节信封开始：

| Offset | 大小 | 字段 | 要求 |
| ---: | ---: | --- | --- |
| 0 | 2 | envelope version | 必须为 1 |
| 2 | 2 | codec id | 1 表示无字典 Zstandard |
| 4 | 4 | encoded body bytes `C` | 必须等于信封后实际累计编码字节数 |
| 8 | 4 | decoded message bytes `U` | 必须等于实际解码输出长度 |
| 12 | 4 | reserved | 必须为 0 |

信封必须完全放入 FIRST 帧，因此宣告压缩能力的连接要求协商后的最大帧负载不小于
16 字节。`max_encoded_message_bytes` 和能力帧中的对端接收上限均计算
`16 + C`。原始 `U` 继续受本地 `max_message_bytes` 约束。超上限、长度不匹配、
未知 codec、截断、尾随数据或解码器不接受的多帧 Zstandard 内容均终结该消息，不把
压缩字节交付上层。

未压缩 DATA 不增加信封或字段，保留现有帧字节布局。

## 逐方向逐消息策略

- 每端配置“本端允许接收的算法集合”及“本端最大编码网络负载”。对端能力帧提供
  对端接收许可，作为本端的发送方向能力。
- 单条消息的选择为 `none` 或 `zstd`。默认 `none`。请求与响应可独立选择；每条
  RPC MESSAGE、普通 Stream 消息和连接组 DATA 各自有独立 Zstd 上下文。
- Message 层只接收业务体。RPC OPEN、元数据、状态、身份和凭证保持独立且不压缩。
  即使元数据分离，业务体含凭证、秘密或攻击者可控输入时，RPC 策略仍必须拒绝压缩。
- 算法未获接收方许可时，默认返回明确拒绝；只有应用显式配置了 `none` fallback
  才可重试为 `none`。不得暗中改变消息字节语义。
- 不使用字典，不跨消息保留 Zstd 上下文；编码体无收益时由明确策略选择 none。

## 压缩侧信道审查

本轮检查 `src/message/zstd.c`、`src/rpc/rpc_record_stream.c`、RPC Client/Dispatch
适配、Channel Stream 收发，以及连接组 DATA 收发和 TLS ByteEndpoint 的数据路径，得到：

- 三种入口均要求显式逐消息选择；RPC 还要求方法方向允许该算法。默认和未选择时均为
  `none`，没有自动压缩、按数据探测后重试或压缩失败后暗中 fallback。
- 每条消息各自创建 Zstandard 上下文，不设置字典、不保留跨消息历史。压缩器只取得完整
  MESSAGE/DATA 正文；RPC OPEN、header、metadata、terminal status、peer identity、
  Channel/连接组控制帧与 TLS 认证材料均不进入压缩上下文。
- TLS 加密正文内容并验证对端，但网络观察者仍能测量 TLS record 流量长度和到达时序；
  压缩比会让同一条消息长度依赖正文内容。因此 TLS 不能消除长度/时序侧信道。
- 库接收的是不透明正文，无法判断其字节是否混有认证凭证、秘密、随机 nonce 或攻击者可控
  片段。若应用把秘密和攻击者可控输入放进同一消息，显式压缩仍可能形成可观测长度差异；
  该风险不能由传输身份认证或元数据隔离单独证明为安全。应改用 `none`、分离消息或由应用
  在压缩前消除这种共置关系。

审查没有发现跨消息上下文或控制面压缩的实现缺陷。以上结论来自实现路径审查和现有
RPC peer-id/凭证样式哨兵测试；业务正文的语义分类属于应用责任，尚无运行时检测能力。
三个公开压缩 API 已增加这一限制的注释。RPC TLS × 压缩矩阵已测量，覆盖边界见下节。

## RPC 性能矩阵

2026-10-24 使用 `bench/run_message_compression_matrix.py` 对 Release `bench_rpc` 执行
TLS 开/关 × NONE/Zstandard 的本机 RPC 基准。传输为 IPv4 loopback；平台为 Linux
7.2.11、AMD Ryzen 7 PRO 4750U。每种场景、模式做 3 次试验，每次测量 1000 个请求；
small 场景覆盖窗口 1/8/32，bulk 使用 64 KiB 载荷，mixed 覆盖 1/2 个并发客户端。
总计 72 个基准 case，84,000 个测量请求全部完成且无 RPC 错误、deadline 或无效响应。
结果文件在当前 worktree 的 `build/message-compression-matrix/fixed-close-race*`。

下表为 bulk 三次试验的中位数。RPS、延迟和客户端资源取测量阶段；服务端 CPU、分配
和 RSS 是进程启动至退出的总量（包含 warmup/drain）。分配值是分配器累计申请字节，
不代表同时驻留内存；RSS 才是峰值驻留内存。

| TLS | 压缩 | RPS | P50 / P99 (ms) | 客户端 CPU (s) / 累计分配 (B) / RSS (KiB) | 服务端 CPU (s) / 累计分配 (B) / RSS (KiB) |
| --- | --- | ---: | ---: | --- | --- |
| 关 | NONE | 1125.6 | 6.883 / 13.598 | 0.450 / 1,079,680 / 26,688 | 0.543 / 133,003,656 / 26,688 |
| 关 | Zstandard | 1149.4 | 6.537 / 13.896 | 0.507 / 1,157,624,968 / 26,684 | 0.631 / 1,302,202,716 / 26,684 |
| 开 | NONE | 1085.2 | 7.055 / 12.392 | 0.662 / 1,145,216 / 26,700 | 0.877 / 133,003,816 / 26,700 |
| 开 | Zstandard | 1220.2 | 6.394 / 10.495 | 0.756 / 1,157,567,352 / 26,812 | 0.832 / 1,302,192,796 / 26,812 |

测量显示，Zstandard 在这个高度可压缩的固定负载上没有明显 RSS 增长，但累计申请字节
约为 NONE 的客户端 1,070 倍、服务端 9.8 倍；延迟和吞吐的变化随场景、TLS 和并发而异，
不能据此得出通用性能收益。`reference_zstd_payload_ratio` 是基准生成器对合成负载做的
离线参考估算，不是实际 TCP/TLS wire 字节或实测压缩比。小消息和 mixed 场景也全部通过，
但其结果受 loopback、固定请求分布和有限试次数约束。压缩继续默认关闭；应用应在自身
负载上实测，并按安全策略逐消息显式选择。

该 RPC 矩阵不测量连接组 DATA，且未在真实网络、代表性业务正文或项目定义的最低收益/性能
阈值下验收。拒绝率矩阵和端到端 wire 字节也没有在这次 RPC 微基准中测量。下节另列连接组
TLS × 压缩性能证据；两套本机矩阵都不等于完整发布性能验收。

## 连接组 DATA 性能矩阵

2026-10-24 使用 `bench/run_connection_group_compression_matrix.py` 和公开连接组门面，执行
TLS 开/关 × NONE/Zstandard × small、bulk-compressible、bulk-incompressible、mixed × 1/2 客户端，
每种组合 3 次试验。每客户端每次有 100 条预热和 1000 条测量消息。总计 96 个 case、
144,000 条测量消息；每条消息均由服务端逐条校验序号和正文散列，所有客户端均完成提交，
无丢失、重复、损坏、路由错误或 deadline，双客户端测量区间重叠。原始结果为
`build/connection-group-compression-matrix/final.jsonl`，子进程日志位于同目录的
`final-logs/`。TCP 代理计数的是 TCP 字节流负载，含 TLS 与控制流量，不含 TCP/IP 头和重传。

环境：Linux 7.2.11、glibc 2.44、AMD Ryzen 7 PRO 4750U（CPU 0–15）、GCC 16.2.1、
Xmake 3.1.2+20261016、OpenSSL 3.6.5、Python 3.14.8。载荷含 24 字节基准头；small 正文为
32 字节，bulk 正文为 64 KiB；mixed 每 4 条中有 1 条 bulk。下表为 3 次试验的中位数；
吞吐和字节数分别列出 1/2 客户端，延迟分别列出 1/2 客户端，分配数为 1 客户端每条消息的
客户端/服务端累计申请字节。

| 场景 | TLS | 压缩 | RPS（1/2 客户端） | P50/P99 微秒（1 客户端） | P50/P99 微秒（2 客户端） | TCP 字节/消息（1/2 客户端） | 分配字节/消息（客户端/服务端，1 客户端） |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| small | 关 | NONE | 53198 / 81521 | 65 / 105 | 142 / 2435 | 96 / 96 | 186 / 0 |
| small | 关 | Zstandard | 6957 / 19761 | 221 / 529 | 344 / 1586 | 125 / 125 | 27699 / 96509 |
| small | 开 | NONE | 38542 / 33154 | 83 / 121 | 186 / 40087 | 140 / 140 | 186 / 0 |
| small | 开 | Zstandard | 2747 / 18056 | 175 / 229 | 184 / 765 | 191 / 191 | 27699 / 96509 |
| bulk-compressible | 关 | NONE | 5028 / 4876 | 13702 / 20466 | 37355 / 51833 | 65760 / 65760 | 65690 / 0 |
| bulk-compressible | 关 | Zstandard | 4888 / 5820 | 5214 / 7867 | 39726 / 147659 | 105.8 / 105.8 | 993755 / 293146 |
| bulk-compressible | 开 | NONE | 4194 / 4256 | 25066 / 30420 | 49907 / 60884 | 66001.9 / 66001.8 | 65690 / 0 |
| bulk-compressible | 开 | Zstandard | 5596 / 5369 | 4639 / 6962 | 85470 / 170074 | 171.8 / 171.8 | 993755 / 293146 |
| bulk-incompressible | 关 | NONE | 4409 / 4440 | 6541 / 17188 | 44005 / 53307 | 65760 / 65760 | 65690 / 0 |
| bulk-incompressible | 关 | Zstandard | 2367 / 3782 | 1782 / 9136 | 47343 / 63965 | 65790 / 65790 | 993755 / 358670 |
| bulk-incompressible | 开 | NONE | 3894 / 3800 | 26405 / 31962 | 61537 / 73999 | 66002 / 66001.8 | 65690 / 0 |
| bulk-incompressible | 开 | Zstandard | 1586 / 2506 | 1148 / 7838 | 2092 / 16552 | 66054 / 66054 | 993755 / 358670 |
| mixed | 关 | NONE | 6754 / 16448 | 406 / 2135 | 24256 / 44054 | 16512 / 16512 | 16562 / 0 |
| mixed | 关 | Zstandard | 1580 / 3136 | 400 / 1257 | 421 / 1447 | 120.2 / 120.2 | 269213 / 145668 |
| mixed | 开 | NONE | 8824 / 14283 | 360 / 923 | 22985 / 49764 | 16605.5 / 16605.5 | 16562 / 0 |
| mixed | 开 | Zstandard | 1315 / 2721 | 197 / 781 | 146 / 665 | 186.2 / 186.2 | 269213 / 145668 |

基准进程最大观测峰值 RSS 为 54,004 KiB；单 case 服务端 CPU 最多 1.266 秒。启用压缩时，
客户端/服务端累计分配字节分别约为 small 27.7/96.5 KiB、bulk-compressible 970/286 KiB、
bulk-incompressible 970/350 KiB、mixed 263/142 KiB；NONE 服务端在测量期间为 0。累计申请量
不等于同时驻留量。基准使用 128 KiB 编码/解码上限与 1024 个接收任务槽，按每任务 128 KiB
解码体、128 KiB 编码体、1 MiB 工作区、64 KiB 任务开销和 256 字节任务元数据计算，任务项
理论预算约 1,409,548,288 字节（1.313 GiB），另有重组槽、Reactor 与 peer 预算；这是基准
配置的上限预算，不代表实测 RSS 或推荐生产值。

Zstandard 让可压缩 bulk 的 TCP 流字节减少约 99.8%，small 消息增加协议和变换开销，
不可压缩 bulk 的字节略增；吞吐变化因负载、TLS 和客户端数而异，没有普遍性能收益证据。
压缩继续默认关闭。此矩阵是 loopback 合成负载证据，不替代真实网络、代表性业务正文、
长期运行或项目性能阈值验收。

## 资源账本与活性

分别记录且各有硬上限：

| 资源 | 计费单位 | 归还点 |
| --- | --- | --- |
| 网络信用 | 实际 DATA payload 字节，含压缩信封 | 接收帧已复制进已预约的有界编码存储，或已被有界 Message 输入责任接管 |
| 编码存储 | 实际预约的重组/消息容量 | 编码消息不再被变换或待处理任务持有 |
| 解码存储 | 实际输出容量及应用消息引用 | 最后一个 `tr_message` 引用释放 |
| Zstd 工作区 | 后端工作区最大预约 | 变换完成/取消并静默 |
| 任务/完成责任 | 接受的任务槽和 owner 完成项 | owner 完成回调或停止后 reaper 完成回调 |
| TX 消息 | 编码输出与单条流待发状态 | DATA 全部写出、终结或取消 |

收发规则：

1. 发送入口先检查 `U` 和策略。变换服务接受前必须已经拥有一个有界 per-stream
   TX 状态、网络发送责任和可用变换任务/输出/工作区预约；准入失败保留调用者消息。
2. Zstd 输出完成后检查 `C` 与远端最大编码负载。Channel 将信封和编码体当作一条
   DATA 逻辑消息；每个分片消耗实际网络信用。发送调度每次只为一个流推进有限帧，
   对活跃流轮转，WINDOW_UPDATE 后继续，控制帧只享有既有有界优先级。
3. 接收 FIRST 时先验证信封并预约一块最大编码容量受限的存储。每个 DATA 分片
   复制成功后按该帧 payload 字节推进累计接收并返还同等网络信用；这不释放编码存储。
4. 完整编码体进入有界 MessageService。服务接受前失败时保留编码消息，受单连接
   有界缓存池约束；资源无法续接时以明确流终止释放网络与存储责任。Message 输出
   `U` 由其自身预算持有，不回补编码网络信用。
5. none 单帧继续交付原 Reactor buffer；none 分片继续复用现有固定重组池。不得为了
   压缩改造给 none 引入消息包装分配、复制或任务入队。
6. 同一 Stream 的异步变换按 message_id 顺序交付。取消、RESET、重连代次变化或
   owner 停止后，任务不能再发送或交付；迟到成功输出按 owner/reaper 责任释放。
7. 对 `C > W`，发送端发至当前信用上限后等待更新；接收端已预约 `C` 存储并复制
   当前分片后回补该帧信用，直至完整 `C` 被送达。全程编码存储受 pool 上限，故
   网络信用释放不产生无界内存。若无法预约，拒绝/终止，不能无限重试。

## 分层实施计划

1. **协议能力协商**：添加 feature-gated `MESSAGE_CAPS`、方向性算法与编码上限，
   验证旧对端不收到新控制帧，验证连接替换会清除能力状态。
2. **Channel/Stream 闭环**：加入压缩信封、MessageService 入站/出站转换、独立
   编码/解码存储账本、分帧信用推进和公平发送；正反测试覆盖 `C < W`、`C = W`、
   `C > W`、窗口耗尽和连接终结。
3. **RPC 业务体适配**：只提交 MESSAGE 业务体；按方法/方向许可压缩；控制信封、
   初始元数据、最终状态和凭证样例不进入压缩器。
4. **连接组 DATA 适配**：复用同一协商和变换，不跨 DATA 路由/成员代次重用能力；
   验证多 DATA、连接切换、取消及慢消费者。
5. **安全与性能验收**：RPC TLS × 压缩矩阵已记录；还需覆盖连接组 TLS/压缩组合、
   代表性应用负载、网络和拒绝率，以及 ASan/UBSan/TSan/静态分析的发布范围。收益不明确
   则默认 OFF。

## 验收矩阵

- `none` 路径：与现有 buffer 地址、复制/分配计数和线程路径一致。
- 协商：无 feature 的旧端、方向不对称算法、无共同算法、重复/损坏/未知能力帧、
  连接替换和重新 HELLO。
- 正常压缩：空/小/大消息、无收益消息、单帧/多帧、随机不可压缩数据、普通 Stream、
  RPC 请求/响应和连接组 DATA。
- 资源安全：`U` 超限、`C` 超限、伪造长度、截断/尾随/损坏、多帧 Zstd、超大窗口、
  工作区与任务槽耗尽、所有者内存预算耗尽、每步分配失败。
- 活性：`C < W`、`C = W`、`C > W`；双向同时发送；多 Stream 竞争；慢流与快速流混合；
  窗口最小值、池最小容量、单 worker；cancel/RESET/连接关闭/重连/迟到完成。
- 资源守恒：发送拒绝、压缩失败、解码失败、取消、断连和销毁后，任务、编码/解码
  存储、工作区、网络额度及回调责任恰好归还；应用保留消息时只保留匹配的解码预算。
- RPC 与连接组 TLS ON/OFF 性能测量和压缩侧信道审查分别记录，不能用 TLS 成功替代数据策略
  审查；真实业务负载、生产网络和性能阈值仍须单独验收。

## 非目标

- TLS 连接状态机、证书配置和部署安全；这些仍由独立 TLS 决策管理。
- 字典压缩、跨消息上下文、自动按压缩率调整算法或在未授权时 fallback。
- 以放大窗口、线程数或全局待发队列来绕过单消息、全局资源和公平性限制。
- 本 spec 本身不代表 Message 压缩已上线；只有所有入口、矩阵和性能/安全证据齐全后
  才可更新 #235 完成状态。
