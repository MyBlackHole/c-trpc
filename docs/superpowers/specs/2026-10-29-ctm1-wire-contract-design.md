# CTM1 具体提案：字段格式、共同描述版本协商与有界视图

日期：2026-10-29。状态：待用户审阅的具体方案，未批准生产实现。
承接[总体设计](2026-10-29-custom-message-codec-design.md)及
[形状对照](../../reviews/custom_message_shape_comparison_20261029.md)。
本文中的“要求”是拟采纳规则，不是当前 SDK 已实现的能力。旧探针不是本规范的
符合性实现；特别是实验四字节根长度不进入 RPC MESSAGE 正文。
用户已选择“改为协商共同描述版本”。本版撤回外置 CTMC 兼容目录及目录先行部署
要求；新端保留真实历史描述与编解码能力，双方选择一个完全相同的方法描述来通信。

## 1. 正文边界与字段语法

通用业务格式选用紧凑顺序字段，初版只实现这一种。RPC 方法上下文固定根类型；
MESSAGE 的 decoded_size 给出根正文长度，CTM1 不重复增加完整描述或消息长度头。
独立调用 verify_as(Type, bytes) 时类型由调用者提供；正文不支持自动识别类型。
裸文件的自包含封装不属于本次 codec/生成桩交付，不将其冒充已有网络能力。

拟保留 codec ID 1 为 RAW，新增库内 CTM1 ID 2；应用 codec 编号使用 65536 及以上。
这是正式发布前的编号规则提案，当前预览版仍将非零 codec 当作不透明数字；不会
声称所有历史开发版使用编号 2 的自定义格式均可继续兼容。
拟在构造时拒绝 codec ID 2 却缺少生成类型描述/已核对 profile 的方法；RAW 与应用
自定义编号继续使用原始字节内核，不由远端哈希将它们升级成 typed 方法。

```text
message = field*                    // 精确消费上层提供的字节边界
field   = ULEB32(tag) ULEB32(length) value[length]
tag     = (field_id << 5) | kind
```

- field_id 为 1..134217727；0 非法。同一 message 的字段 ID 严格递增，已知与
  未知字段一并检查；重复、逆序均拒绝。源文件字段顺序不决定网络顺序。
- tag/length 均为最短 unsigned LEB128，最多五字节；第五字节只允许低四位，
  不允许继续位。溢出、非最短、缺字节、length 超过剩余正文均为非法编码。
- 空 message 是零字节，空字段可显式出现。缺席、数值零、空 bytes/string/list、
  已存在的空子消息分别表示，不省略显式默认值。没有通用 wire-required。
- 不发送本机结构填充、地址、对齐或宿主端序；解析固定数值使用按字节装载或
  memcpy，不将不对齐缓冲强转为数值指针。不默许尾部垃圾。

下表规定本地 schema 认识字段时的 value 检查；kind 必须与字段声明完全相同。

| kind | 类型 | 表示与约束 |
|---:|---|---|
| 0 | bool | 恰一字节，仅 0 或 1 |
| 1 / 2 / 3 / 5 | u8 / u16 / u32 / u64 | 分别 1/2/4/8 字节，小端 |
| 10 / 11 / 4 / 6 | i8 / i16 / i32 / i64 | 同宽小端二补码，禁止截断或符号重解释 |
| 7 | enum | 四字节小端 i32；未知数值保留，包括未声明的 0 或负数 |
| 8 / 9 | f32 / f64 | 四/八字节 IEEE 754 binary32/binary64 原始位 |
| 12 | bytes | 原样字节，空值合法 |
| 13 | string | RFC 3629 UTF-8；空值及 U+0000 合法，不带 NUL 终止符 |
| 14 | list | 元素类型由本地字段描述固定，布局见下一节 |
| 15 | message | 对目标类型递归验证，值的长度即子消息边界 |
| 16..31 | 后续扩展 | 当前 schema 不得生成；未知字段可按长度跳过 |

所有浮点位模式均可传输，包括正负零、Inf 和 NaN；codec 不做浮点运算或 NaN
规范化。默认 C/Rust API 使用 u32/u64 位包装，保留原位；转换为本机 float/double
是显式辅助操作，只在目标表示已验证时提供，不据函数传参承诺保留 signaling NaN。
标量固定宽度取直接装载与固定解析成本，实际类型可选 u8/u16/u32/u64；现有探针
没有证明它优于值 varint/ZigZag。小整数密集负载的空间/CPU 取舍仍须在生产验收
中测量，不把此设计宣布为最小编码或性能最优。以后改变已知字段表示须按不兼容
变更处理，不能偷偷复用 kind/ID。

已知 string 拒绝过长形式、孤立续字节、截断、代理码点及超过 U+10FFFF 的编码。
不进行 Unicode 规范化、大小写变换或 BOM 去除；合法非字符与零字节不自动拒绝。
这是明确的字节串合同，不以 strlen 或 C 字符串代替。[UTF-8 规则](https://www.rfc-editor.org/rfc/rfc3629)

未知 ID 只检查公共 TLV 边界/顺序并计未知额度；不解释其值，不递归验证未知
子消息，也不验证未知 string 的 UTF-8。旧端接受/携带未知内容不证明未来语义
合法；新端认识该字段后仍执行完整验证。reserved 的历史字段作为未知内容处理，
schema 的保留编号阻止重新定义，不阻止旧消息的可选字段被安全跳过。

## 2. list、类型引用与 presence

初版 list 的元素可为标量、enum、bytes、string 或已命名 message；不允许直接
list<list>，复合容器使用命名 message。所有引用在生成期解析为有限无环图。

- 固定宽度标量/enum 的 list 为 packed 值，既不逐项加 tag，也不发送重复计数。
  length 必须整除元素宽度，元素数量由商得出；bool 数组逐值验证 0/1。
- bytes/string/message 的 list 为 `ULEB32(item_length) item_bytes` 的顺序串，
  元素数量由边界遍历得出。空元素仍占至少一个长度字节并计一次元素工作；
  空子消息同样计节点/深度。没有零成本元素或无限遍历的隐式压缩表示。
- list 不允许缺席元素或每项改变类型。字段缺席与已存在的空 list 区分；保留
  顺序，不排序或去重。map 的键唯一性、集合与互斥业务规则由应用验证，不伪装为
  通用 wire 功能。改变元素类型或引用目标，即使长度碰巧相同，也是不兼容变化。

字段值中的 message/enum 引用不逐项发送 type ID；根及其引用闭包绑定到方法
合同。类型不同但字节恰好能解析，不能当作类型匹配证明。C 用 has_value，Rust
用 Option 表达字段 presence；开放 enum 使用 i32 新类型与已知常量，不以封闭
Rust enum 强转未知整数。[紧凑标签与 packed 的参考](https://protobuf.dev/programming-guides/encoding/)

## 3. 单源 schema 与描述规范化

构建输入为 UTF-8 JSON，format 为 `ctm-schema-1`；不在网络传输 JSON。根键为
format、family、namespace、types、services、profiles，以及 reserved_type_ids/
reserved_type_names/reserved_service_ids/reserved_service_names；未知键和重复键均拒绝。
family 是非全零的 32 个小写十六进制字符，表示 16 个身份字节。namespace/name
为 `[a-z][a-z0-9_]{0,62}`，生成前拒绝 C/Rust 关键字及派生名称冲突。

- types 共用正 u32 ID 空间。message 项含 id/name/kind/semantic_revision/fields/
  reserved_ids/reserved_names；field 含 id/name/type，另可声明 deprecated 布尔值。
  enum 项将 fields 换成 values 与 reserved_values；value 含 name 和 i32 number，
  本版拒绝同值别名，数值 0 允许。保留数值与名称不可再次使用。
- type 为上述基本类型字符串、`{"ref": type_id}` 或 `{"list": element_type}`；
  ref 指向 message/enum。所有外部引用、环、缺失类型及非法嵌套在生成前拒绝。
- service 含 id/name/methods/reserved_method_ids/reserved_method_names；method 含
  id/name/request/response/semantic_revision/
  replay/policy。service_id/method_id 为 u32，允许 0，继续按监听器内二元组唯一。
  request/response 含 type/cardinality/required：type 为 message ID，cardinality
  为 one 或 many，required 为根字段 ID 集合；它是方法业务验证，不是 wire-required。
- replay 为 never 或 idempotent，必须显式声明。生成权限不等于执行重试，实际
  Attempt、提交点、期限及资源仍按 #244；其他重放机制另行设计并验收。
- policy 含 request_profile/response_profile/lane/request_compression/
  response_compression/max_supported_revisions/negotiation_timeout_ms。
  max_supported_revisions 为 1..8，限制本地方法支持版本数；negotiation_timeout_ms
  为正 u32，限定协商总等待，不能以 0 取消上限。两者不进入 method revision。
  profile 名称引用 profiles，lane 为 control/bulk，压缩
  列表只能包含 none/zstd，必须有 none。它们生成方法大小/路由/算法策略，不进入
  正文类型身份。deadline 和实际尝试参数继续由 Call 配置固定。

类型、服务、方法移除后分别保留其编号与名称，不能绕过字段的保留规则复用上层
身份。保留集与现存定义不得相交；发布历史检查同时覆盖不在运行支持窗口内的定义。

semantic_revision 为正 u32，表示已存在业务含义的世代，不是每次加字段递增的
schema 发布计数。改变它、已存在字段含义或方法 required 集合属于语义变更；
默认兼容工具要求新类型/方法并行，不把新增 optional 当作绕过途径。任意业务代码
含义无法由 IDL 自动证明，维护者仍须声明并审阅变化。

规范化先验证并补齐允许省略的 deprecated=false，再按定义 ID 排序 types/services/
methods/fields，按 number 排序 enum values，保留 ID/数值集合数值升序，名称集合及
profiles 按 ASCII 名称排序。required 和压缩视为集合，去重不静默进行，重复即拒绝；
其规范顺序分别为数值升序与 none,zstd。引用用稳定数字，不含路径、时间或注册顺序。

规范化结果用 RFC 8785 JCS 序列化后 SHA-256。JSON 数值只允许上述 i32/u32 范围
的整数，拒绝浮点/指数形式；family 用字符串，避免超出精确整数范围。对象键只用
固定 ASCII 键，标识符也限 ASCII；不以任意 json.dumps 配置代替这一验证过程。
输入 JSON 深度/字节与定义数量有生成期有限上限，解析前也须限制，不能先建立
无界对象再检查。[JCS 的输入限制与规范化](https://www.rfc-editor.org/rfc/rfc8785)

生成两类有域分隔的摘要，输入分别为对应规范投影；都是完整 32 字节，不截短：

| 摘要 | 哈希输入 |
|---|---|
| artifact | `ctm-artifact-1\n` 加完整规范化 schema 的 JCS 字节；包括 API 名与策略 |
| method revision | `ctm-method-1\n` 加下述方法 wire/语义投影的 JCS 字节 |

方法投影只含 family、service_id、method_id、请求/响应 type ID、基数、codec ID 2、
wire_major=1、方法 semantic_revision、required、replay，以及请求/响应可达类型闭包。
闭包保留 type ID/kind/semantic_revision，message 的 field ID/type 与 reserved IDs，
enum 的数值与保留数值；去掉 API 名、deprecated、策略及文件组织。全部引用仍为
稳定 ID；完整闭包按 type ID 排序。新增可选字段改变 revision，但不自动变成不兼容。
同一业务消息的编码字节不用于生成这些摘要，不提供消息签名规范化功能。
哈希域中的 `\n` 表示一个 0x0a 字节。方法投影的精确键/层级由
[示例投影](../../reviews/data/ctm_method_1_projection_example_20261029.json)规定：
request/response 均含 type/cardinality/codec_id/required，enum 的 values 为数值
数组；message 的 fields 仅含 id/type。样例之外按同一规则展开引用闭包。
本地策略继续保存独立不可变值快照，不额外引入尚无用途的网络策略指纹。

[完整单源示例](../../reviews/data/ctm_schema_1_example_20261029.json)给出可选字段、
enum、方法、业务必需项和 profile 的全部输入键；[预期字节/摘要](../../reviews/data/ctm1_contract_examples_20261029.json)
仅是设计算例，未宣称 C/Rust 已通过符合性测试。

## 4. 协商共同描述版本

这里比较过三种安排，下表是设计推导，还没有相应网络实现/性能测量：

| 安排 | 部署收益 | 代价 |
|---|---|---|
| 协商共同描述版本（用户选择） | 旧实例无需提前认识新 revision；同一 Call 两端使用相同描述 | 新端保留多版本 codec/适配；首条 typed 请求等待选择，增加一次协商往返 |
| 本地兼容目录（撤回） | 请求可继续流水发送，允许兼容的不同描述互读 | 旧实例须先取得扩大后的可信目录，用户不采用此部署前提 |
| 在线交换完整描述 | 可在调用时比较独立发布的新定义 | 需要新增有界描述交换、缓存/失效及准入流程，现有 512 字节元数据不足以承载一般描述闭包 |

借鉴 Kafka 的按共同 API 版本编码、响应沿用所选格式的原则；CTM1 使用精确摘要
而非连续版本号，允许支持集合有空洞，不从哈希大小猜测先后。Kafka 也指出未知
标签被忽略不一定符合发送方意图；因此本提案把降级适配与可解析分开。
[官方协议说明](https://kafka.apache.org/protocol.html)

另参考 Apache Avro RPC 握手：有状态连接在确认双方已知协议后，可以对该连接的后续
调用省略重复握手。CTM1 当前选择逐 Call SELECT，换取每个 Call 都能独立校验 offer、
选择与取消/终局顺序；代价是每次 typed Call 都增加一次网络往返，不能把它描述成
“仅首个连接握手”。首版验收须用实际 RPC 负载测出单次与同连接重复 Call 的延迟代价。
若不能满足产品延迟目标，应另行设计连接代次内的按方法协商缓存，涵盖并发首次调用、
对端重启/连接替换、缓存上限与失效；不能以本地历史选择代替当前连接确认。
[Avro RPC 握手规范](https://avro.apache.org/docs/1.12.0/specification/#handshake)

### 4.1 支持的是真实描述，不是哈希允许表

生成工具以当前 schema 和显式列出的历史 schema 为输入；历史输入最多七份，
不在 Call 中读文件、下载描述或运行生成器。每个方法生成按偏好排序的版本表：
当前描述在首，历史描述依构建输入顺序排列，相同 method revision 合并为一项。
每项必须具有完整可达类型闭包、验证器、编码器及版本专属 C/Rust 接口。生成清单
记录工具格式版本、各输入摘要及最终有序支持表，生成漂移检查覆盖全部输入。

历史输入须经发布基线检查，不能借“共同版本”继续生成已复用编号或改变既有含义
的描述。支持表各项必须属于相同 family/service/method，保持相同 codec、根类型
身份、ONE/MANY、semantic_revision、required 和 replay；破坏性变化使用新类型/
方法并行。仅 API 名不同或策略不同的同一 method revision 不重复协商。

注册 Server 支持版本，还须绑定该版本的业务处理入口。可直接注册版本专属处理器，
或使用显式、可失败的适配入口连接当前处理器；缺少入口的版本构造失败，不能只
登记哈希。自动生成的无损适配器仅处理经核对的字段对应/presence 与开放枚举表示，
不能自动推导业务默认值或废弃字段的处理含义。Client 同样须具有所通告版本的真实
请求编码/响应解码与结果接口。版本专属接口始终返回所选版本的值，不假装为当前型。

Client/Server 构造时冻结本地有序表、适配绑定和当前资源策略。历史 schema 的策略
不覆盖当前运行额度；所有版本共用当前配置的逐方向 profile、lane 和压缩策略。
类型描述静态只读，Call 独立持有适用快照，producer 独立持有所需描述/编码策略。

### 4.2 OPEN offer、选择记录与额度

Client 在 OPEN 的保留元数据键中给出本次愿意使用的描述，顺序即明确偏好：

```text
key = "ctm-contract"                       // 12 字节
OPEN value = "CTO1" | u8 N | revision[0..N)[32]
1 <= N <= 8                                // 不截短 SHA-256，不允许重复
```

Server 依 offer 顺序选第一个在本地可用表中的 revision；不按 Server 注册顺序、
哈希字典序或“最近似描述”选取。Client 可以只 offer 某一版本以要求其语义；未列入
本次 offer 的本地版本也不允许被选中。无交集时直接非 OK FINAL_STATUS，状态为
FAILED_PRECONDITION，不返回任意替代版本，不触发自动重放。

现有元数据条目头为 u8 key_length + u16LE value_length。OPEN 条目占
`3+12+5+32*N = 20+32*N` 字节：N=1/2/8 时为 52/84/276 字节，应用分别剩余
460/428/236 字节。保留项与应用请求元数据合计仍须 <=512，不能截掉应用条目或
将 offer 截短来凑额度。生成桩和 typed bytes 入口拒绝应用自己提供 ctm-contract；
该键也不得伪装为应用 INITIAL/FINAL 元数据。

版本选择不复用 INITIAL_METADATA：当前 SDK 只允许一次 INITIAL，且业务可以在
读完请求后才设置它。提前占用会丢失该能力，等业务设置又会与等待正文互相阻塞。
本提案在正式发布前扩展 Record V2，保留原 48 字节固定头和 kind 1..5，新增：

```text
kind = 6  CONTRACT_SELECT                  // 仅 Server -> Client，至多一次
metadata = 单个 ctm-contract 条目
value = "CTS1" | selected_revision[32]     // 36 字节，条目共 51 字节
service/method/codec/compression/encoded/decoded/status/remaining_timeout = 0
metadata_size = 51                         // 无业务体，无应用元数据
```

选择记录仍受单条 512 字节元数据上限约束，不消耗另一次业务 INITIAL 的额度。
其网络大小为 48+51=99 字节，须计入控制发送与 Call 预算，不是免费旁路。N=2 时
offer 增量 84 字节加选择记录 99 字节，共增加 183 字节；不含既有 OPEN 头、业务
INITIAL、传输封装和 TLS 开销。没有当前实现或延迟测量支持性能收益宣称。

这是拟议的 Record 扩展，现有源码还只接受 kind 1..5。第一版 CTM1 双端都必须
实现该控制流程；不承诺与没有 CTM1 协商的历史开发版互通，不以它们为 schema
滚动升级中的“旧版”。RAW/应用自定义 codec 不发送 offer/SELECT，保持自身路径；
在非 CTM1 Call 收到 SELECT 或保留合同项明确拒绝，不能被自动升级为 typed 方法。
现状依据：[记录格式/状态机](../../../src/rpc/rpc_record.c)、
[元数据编码](../../../src/rpc/rpc_v2.c)、[公开额度](../../../include/tr/rpc.h)。

### 4.3 状态顺序与业务事件

1. Client 受理时冻结 offer 与原期限，仅发送 OPEN，进入 WAIT_SELECT；第一条
   MESSAGE 和 HALF_CLOSE 都必须等待有效 SELECT。取消/期限/非 OK FINAL 可先结束。
2. Server 完整核对 OPEN 元数据、路由和本地支持表，选择版本并预留一次 99 字节
   控制发送所需资源。没有有效 offer/共同版本不得进入业务钩子。不要求等到请求
   MESSAGE、HALF_CLOSE、authorize 或业务 work 才产生选择记录。
3. Server 在 owner 有界进度泵提交 SELECT；背压时保留一个有预算的待发控制项，
   不阻塞 owner/业务线程、不占 producer 的应用单槽、不反复创建工作。只有下游
   接受后才提交选择状态，并向既有 observe/admit/authorize/work 路径交付一次 OPEN。
   应用产生的响应严格排在 SELECT 之后，不得抢先提交 INITIAL/MESSAGE/OK FINAL。
4. Client 完整验证 CTS1、长度、所选项确在本次 offer 及本地表中，提交 SELECTED；
   选择事件仅交给 typed 控制层，不作为业务 metadata 回调。随后才能编码/发送
   请求。Server 接收请求、双方接收响应都严格按选中描述验证，禁止回落到当前型。
5. 业务 INITIAL 仍至多一次，可以等到读完请求后产生；响应 MESSAGE 仍要求先有
   INITIAL。typed Call 的 OK FINAL 必须先有 SELECT，零响应 MANY 也一样；零条
   MANY 不补造 MESSAGE，不强制产生本来无需的业务 INITIAL。ONE 的零字节 MESSAGE
   仍是一条消息。非 OK FINAL 可以在 SELECT/INITIAL 之前结束。

保留合同项在交付业务 OPEN 元数据前剥离；所选 revision 通过 typed Call 上下文
提供。流式桩提供一次 on_selected 通知以唤醒发送者，沿现有有界回调执行域并与
本 Call 其他回调串行，排在业务 metadata/body 回调前；不冒充 INITIAL。通知排队
期间若取消/期限/停机已胜出，只交付 terminal，不再通知可发送；正常远端记录按
接纳顺序交付，不能因 FINAL 已到达就反转此前事件。回调借用不延长正文寿命。

选择只证明描述一致，不是 admit、authorize、请求解码成功或业务未执行证明。
没有共同版本的 Server 拒绝发生在业务钩子前，不交付应用 terminal；选择提交后
进入原受理点的调用，仍按原统计和唯一终局规则交付。Client 因错误 SELECT 拒绝
不能保证 Server 尚未执行 OPEN 业务钩子；不能据状态自动重放。

Server 在选择提交前收到请求 MESSAGE/HALF_CLOSE，或 Client 在有效选择前收到
INITIAL/MESSAGE/OK FINAL，均不得交付业务事件。重复 SELECT、请求方向 SELECT、
错长度/魔数、选择未 offer 的项也终结当前 Call。完整合法 Record 中的合同错误
限定为 Call；底层非法 Record/连接错误仍按 #242，不一概改成连接错误或 Call 错误。
Server 只能验证自己的提交顺序，不能声称能证明对端实际等待过确认。

当前一元入口把 OPEN+MESSAGE+HALF_CLOSE 一并受理；typed 桩必须新增上述等待阶段，
不能直接套用现状宣称已经完成协商。通用 bytes 入口若指向注册 CTM1 的方法也必须
走同一 gate：预编码输入显式绑定其 revision，只 offer 对应版本，不能把当前字节
发给协商出的旧描述，也不能绕过正文验证。未知 codec 不因携带 offer 得到授权。

### 4.4 降级不静默丢失字段

选中旧描述意味着按旧描述编码和解释，不能“哈希选旧、正文仍发新”。默认当前型
便利接口在编码前执行可失败投影；双方可以改用版本专属接口直接表达选中类型。

- 当前新字段缺席时，且其他字段/业务声明兼容，可无损投影；对应新字段显式为零、
  空串、空 list 或空子消息仍算存在，旧描述无法表达时拒绝。嵌套与 list 元素逐层
  检查，不能只核对根字段或省略默认值。接收旧描述时当前新增字段保持缺席。
- 新枚举数值在旧描述中仍作为开放整数保留，业务可明确拒绝；不映射为 0。移除/
  废弃字段也不能自动消失：适配必须保留其可见表示，或以明确原因失败。
- 不能无损表达的请求/响应，默认以 TR_ERR_UNSUPPORTED 加 adaptation 原因拒绝，
  本地编码方不提交该条 MESSAGE。显式舍弃或语义转换须由应用调用具名适配函数并负责审阅；
  生成器不提供默认 ignore_unknown/new_fields 开关来偷偷降级。
- 合同内仍允许第 1/5 节的未知字段跳过/显式携带，但它不替代版本适配：当前型的
  已知新字段不能塞入旧型 unknown 区绕过检查，也不能因当前 schema 认识某编号，
  就把选中旧描述下的 opaque 字节直接提升为当前型字段。这种提升须显式重新验证
  目标描述并走适配/业务检查。copy_wire 不修改字节，也不修改其描述绑定。

一元便利接口先在本地有限工作预算下检查请求在各候选中的无损可表达性，保留
可用候选原顺序；空集合在受理前失败。最终 offer 冻结后不再隐式改变；元数据
容量不足仍失败。输入为明确的 owning snapshot，受理前预留存储并复制请求及所需
字段信息；返回后不保留调用者栈或可变源的借用。不能只保存临时 C 指针等 SELECT。
所有候选检查与最终投影/编码共享该消息的 encode work，不给每个版本重置预算；
检查预算耗尽是资源失败，不能把尚未检查的候选误报为“不支持”。

流式 Call 无法预知未来每条数据。调用者可通过只 offer 某版本要求能力；协商后
send 对无法表达的新值同步失败且不保留本条输入，先前已受理消息不会撤回。Server
响应投影失败须终结为 INTERNAL 并保留具体适配诊断；若先前已发流式响应也不能
假装从未执行。后续修正/重试发送仅能在尚未终结、API 允许的同一 Call 内进行。
Server 将合法所选请求适配到当前处理器失败，或 Client 将合法所选响应适配到当前
结果型失败，均为 FAILED_PRECONDITION 加 adaptation 原因；不能将本地适配局限
冒充对端坏编码。已执行/已收到的部分仍按原规则处理，不自动重放。

### 4.5 等待、取消、资源与滚动部署

协商期限为原 Call 期限与从本地 OPEN 受理时刻起 negotiation_timeout_ms 的较早值；
Server 也从收到完整 OPEN 起用本地上限及传播的剩余期限。没有声明 Call 期限也
不能无限等待选择。到期用 DEADLINE_EXCEEDED，取消沿 CANCELLED，仍只提交一次终局；
不重置原期限，不产生新的 Attempt。EOF/连接失败沿既有传输终局，不视为选择成功。
本地选择阶段成功后撤销协商专用计时，后续继续使用原 Call 期限，不把 5000ms
示例上限错误施加到已经完成协商的长流。

owner 串行维护 offer/SELECT 状态，其他线程的取消与发送通过既有操作提交；异步
投影/验证持 OperationRef，提交前复核代次、状态及原期限。terminal 后的迟到选择/
完成不能打开 gate 或发送正文。回收遵守“最后使用者结束，再释放，再归还预算”；
从等待态转为发送态只迁移所有权，不提前归还仍占用的源、scratch 或输出额度。

版本项最多八个，匹配最多 8*8 次完整 32 字节比较；元数据长度/重复项先在固定
上限内校验，不按远端 N 分配可变表。版本代码/静态描述的构建大小在生成清单列出；
构造复制表、逐 Call offer/控制槽、待发请求 owning snapshot、适配 scratch/输出和
排队/在途结果都进入 #241 预算。streaming send 在 WAIT_SELECT 返回 TR_AGAIN，
不接受输入；一元便利接口最多保留一条已计费请求，不为每个候选编码八份副本。
应用 handler/适配代码不在 owner 执行；短表选择与状态提交不得调用应用代码。

旧实例支持 {A}，新实例支持 {B,A} 时，旧→新和新→旧均选择 A；新→新可选 B。
旧实例无需提前认识 B 或取得外置目录，新实例必须实际包含 A codec 与可用入口。
排空所有依赖 A 的实例及 Call 后才停止支持 A；不存在共同版本时明确失败，不
承诺无限历史窗口。独立分支 {B,A}/{C,A} 选 A，禁止把 B/C 独立新增的相同字段 ID
按各自当前型互相解释；后续合并仍须历史检查解决编号冲突。

这里的“旧实例”特指已实现 CTM1 OPEN offer 与 `CONTRACT_SELECT` 的运行时、但仅
支持旧描述 A；它不包括当前只接受 Record kind 1..5 的历史运行时。两种兼容维度
分两段发布，不能混为“新旧 schema 自动互通”：

1. **先滚动运行时能力，暂不发 typed Call。** 部署包含 Record kind 6、CTM1 offer/
   SELECT 状态机及拒绝路径的版本；期间只开放既有 RAW/应用 codec。只有在调用方
   已具备 CTM1 client，且服务发现/路由确认所有可能接收 CTM1 的实例都支持此运行
   时能力后，才启用 typed 流量。
   旧运行时可能把保留元数据当普通应用数据或拒绝新 Record kind，因此不得向它发送
   typed OPEN，也不能把其错误响应当作“没有共同描述”的正常协商结果。
2. **再滚动 schema 描述。** 首个可用版本支持 `{A}`；后续版本加入 B 时必须继续
   打包 A 的描述、codec 与业务处理/适配入口，形成 `{B,A}`。调用方 offer `{B,A}`，
   与只支持 A 的 CTM1 实例协商为 A；双方支持 B 时优先 B。回滚前先停用只支持 B
   的调用行为，排空依赖 B 的 Call，并确认所有实例仍保留 A。撤销 CTM1 运行时能力
   前，还须停用全部 typed 流量并排空 kind 6 记录。

这项先后关系不要求发布外置兼容目录；历史描述和真实处理入口随程序构建产物发布。
运行时能力准备与 schema 版本扩展是两种部署闸门，发布清单应分别记录已覆盖实例、
启用状态与可回滚版本，避免只凭 schema 哈希交集推断 Record 层可互通。

选择不协商或证明对端逐方法资源配额、lane 或压缩。滚动发布仍须保证实际发送
域和支持窗口接收 profile 的条件；未满足时明确 RESOURCE_EXHAUSTED/原策略错误，
不截断、改值或重放。哈希不提供身份认证、防降级认证或 TLS 替代；无安全传输时
不增加本来没有的对端真实性承诺。本版不增加跨连接协商缓存或在线描述下载。

示例中的 5000ms 仅为显式有限配置算例，不是已测得的产品默认值或性能目标。

### 4.6 必须验收的关键轨迹

下表是未来实现的判定条件，尚未运行网络状态机测试；设计算例中的七个交集
计算只验证集合选择，不证明这些时序、所有权或取消边界。

| 轨迹/输入 | 预期 |
|---|---|
| ONE/ONE：OPEN→SELECT→请求 MESSAGE/半关闭→业务 INITIAL/响应 MESSAGE/OK FINAL | 双向按选择描述，on_selected 在业务响应事件前，唯一成功终局 |
| MANY/MANY：OPEN→SELECT→请求半关闭→OK FINAL | 双向零条有效，不补 MESSAGE，也不必补业务 INITIAL |
| OPEN→非 OK FINAL，没有 SELECT | 合法拒绝，无 on_selected、无 typed body |
| offer 的 N=0/9、重复项、截断、尾部或保留键冲突 | 格式拒绝，不进业务钩子，不按远端计数分配 |
| {B,A}/{A}、{A}/{B,A}、{B,A}/{B,A}、有空洞、独立分支 | 分别按 Client 首个共同项，选后不得切换 |
| 无交集、返回未 offer 项、重复/错向 SELECT、确认前正文/成功终局 | 拒绝该 Call，同连接健康调用不受合同错误牵连 |
| 新字段缺席/显式零/空，嵌套或 list 元素含新字段 | 仅无损者可自动投影；失败不接受本条发送、不静默丢值 |
| SELECT 发送背压、控制预算 OOM、候选检查预算耗尽 | 不执行依赖确认的业务，不创建无界队列、不假报无共同版本 |
| 等待确认时取消/到期/停机，随后迟到 SELECT/投影完成 | 原终局不被覆盖，无新正文，持有与预算最终归零 |
| 选择后 OPEN 业务已执行，Client 随后拒绝/发送失败 | 不推断未执行，不自动重放；按原 Attempt/幂等规则处理 |

## 5. 有界验证、访问与重编码

生成的类型图是只读静态描述，C 调用者须维持其存活与不可变，Rust 使用静态借用；
不在运行时解析 schema。Client/Server 构造复制冻结实际策略和支持版本表；各 Call
持有适用快照。typed producer 还须独立持有所需编码策略，关闭后的拥有型句柄
可以长于 Server，不能在编码或释放时访问已经销毁的 Server 裸指针。

每个 profile 明确给出以下有限值，数字范围 u32：max_wire_bytes、max_fields、
max_elements、max_depth、max_unknown_bytes、max_verify_work、max_access_work、
max_encode_work、max_scratch_bytes、max_output_bytes。depth 至少 1，各 work 至少 1；
其余零值表示真实的零额度，不表示无限/默认值。workspace 不足以建立所需验证
栈/根视图的配置拒绝创建。方法请求/响应 max bytes 从对应 max_wire_bytes 生成，
输出上限不得超过它；字段长度与工作等限额可以变化，差异按策略报告。

验证计价固定为以下逻辑单位，不宣称是 CPU 纳秒或应用总 CPU 硬上限：

- 进入根/已知子消息计 1 work，根深度 1；每个已知嵌套增加一层。
- 每字段计 1 field 与 1 work；每读取 tag/length 的字节计 1 work。
- 已知独立标量值检查再计 1 work。bytes 不逐字节扫描；string 预扣其 value
  字节数的 work，再运行 UTF-8 检查。不得在计费前扫描未获准内容。
- list 每元素计 1 element 与 1 work，固定标量检查包含在该项；变量元素长度
  的每字节再计 1 work，string 元素另扣 UTF-8 字节，message 元素另计节点。
- 未知字段累计完整 tag/length/value 原始字节；跳过再计 1 work，不解释内部。

输入字节只按根边界计一次，不能把祖先 value 长度重复累加当作实际输入字节。
字段/元素/未知/work 沿整棵消息共享；先检查扣减和乘加溢出，再移动游标或建立
对象。失败不退还已消耗工作计数，运行时不允许对同一未结束消息重新创建预算
以绕过限制。独立调用者自己的任意循环不属于库能限制的 CPU 范围。

verify 使用调用方提供的有限 scratch 和显式栈，无隐藏堆分配；成功才发布根
只读视图及已验证来源。根字段索引大小由生成描述确定，计入 scratch 要求；不
建立整棵对象树。子视图/变长迭代器共享同一 session 的 access work：重新扫描
字段头仍按头部计价，逐项访问再计 1，固定数组索引访问计 1，复制计输出字节数。
重新建立迭代器不能重置 session；已验证字符串可直接返回原字节，不重复扫描。

view 的源与 session 必须保持有效且不可变。Rust View 同时借用原字节与 session，
不提供能绕过计数的 Clone/Sync；C 使用私有字段及显式单一执行者合同。跨异步保存
使用显式有界 owned copy，先预留预算再分配，真实释放后才归还；CallRef 不持有
正文。当前 SDK 的 body 仅回调借用，没有通用 Message hold，不能假称已有该能力。
C 的零长 span 可以为 NULL，presence 仍独立；Rust 必须转换为合法空 slice，不能
把 NULL 构造成借用引用。ONE 中零字节正文仍是一条消息，不按缺席处理。

未知重编码以原 view 为携带来源，按 ID 将其未知原片段与当前已知输入合并；
已知 ID 即使当前缺席也不允许被携带项伪造。每层子消息分别指定其携带来源，
不得用根来源补子消息；变长改写重算祖先长度。跨安全域携带由应用显式选择。
copy_wire 保持原正文字节；一般重编码只承诺定义的语义与允许未知内容，不自动
成为内容寻址/签名的规范字节。字段定义不能从远端输入扩张。

编码输入仅借用至返回，两遍预检/写入期间必须稳定；调用方提供输出，禁止与
输入、视图、scratch 或 written 存储重叠。预检计验证/尺寸遍历与准备的写入工作，
实际复制/写入按字节预留 encode work；第二遍不重置额度。容量或预算不足不发布
written，输出暂存字节不作保证。解码失败不修改业务输出；错误诊断可以单独写入。

RPC adapter 在分配前为消息源、验证 scratch、异步结果及外借复制预留 #241 额度。
Server 验证复用既有工作任务，在调用 typed 业务处理器之前完成；Client 验证接入
既有有界执行/OperationRef 流程。Reactor 只做有限合同/准入操作，不扫描任意大
正文。迟到完成必须复核 Call 代次、取消和原期限，终局后不发布 typed view，
归还所有持有。小消息内联优化须另有计价/公平性证据，不以本方案宣布已经实现。

## 6. 错误分类与生成接口边界

| codec 原因 | 本地返回 | 收到请求 / 收到响应 |
|---|---|---|
| 参数或借用/重叠前提错误 | TR_ERR_INVALID | 本地拒绝，不创建新 Call |
| 已知值非法、坏长度/整数/顺序/UTF-8 | TR_ERR_INVALID | INVALID_ARGUMENT / DATA_LOSS |
| 方法声明的业务必需项缺席 | TR_ERR_INVALID，业务前提原因 | INVALID_ARGUMENT / DATA_LOSS |
| 已知 kind 或正文类型不匹配 | TR_ERR_BAD_TYPE | INVALID_ARGUMENT / DATA_LOSS |
| 合法 offer 无共同版本 | TR_ERR_BAD_TYPE，合同原因 | FAILED_PRECONDITION / 不适用 |
| 非法 offer/选择记录、选择未 offer 的项或越过 gate | TR_ERR_BAD_TYPE，合同格式/状态原因 | INVALID_ARGUMENT / DATA_LOSS |
| 当前型不能无损适配所选版本 | TR_ERR_UNSUPPORTED，适配原因 | 本地发送拒绝；收到合法值但本地适配失败为 FAILED_PRECONDITION；Server 编码响应为 INTERNAL |
| 未支持 codec/wire 主版本 | TR_ERR_UNSUPPORTED | UNIMPLEMENTED / UNIMPLEMENTED |
| 输入、元素、工作、输出或存储额度耗尽 | TR_ERR_NOMEM | RESOURCE_EXHAUSTED / RESOURCE_EXHAUSTED |
| 实际分配失败（adapter/profile/owned copy） | TR_ERR_NOMEM，独立原因 | RESOURCE_EXHAUSTED / RESOURCE_EXHAUSTED |

结构化原因区分参数、格式、类型、合同、业务前提、额度与分配，不仅凭一个 C 返回码推断。
本地生成响应出现非法值是实现错误，映射 INTERNAL；调用者编码请求失败在受理前
返回。取消/期限沿原终局，不能被迟到 codec 错误覆盖。未知字段不是失败，也不
代表业务已接受；业务必需/枚举策略由生成方法验证和应用逻辑执行。

完整但非法的 CTM1 正文/合同只终结当前 Call，同连接健康调用继续；底层 Record/
连接结构错误继续按 #242 原有作用域处理。本表是实现目标，不是现状验收证明。

生成器输出同源 C11 客户端桩、Server 派发表与 C/Rust codec；四种 ONE/MANY 均
接入现有统一 Call 内核。ONE 恰一条，MANY 零到多条；生成一元接口不提前以收到
MESSAGE 当成功，取消、背压、唯一终局及 drain 责任沿用原内核。Rust 包装绑定
Client/Server 所有者、CallRef/producer 的成对释放与借用，禁止裸 C 指针变成安全
拥有型值；回调借用不得逃逸，owned copy 的预算随所有者释放。每 Call 最多一个
HeldCall，禁止 Copy/Clone；producer 的 try_clone 必须真实 retain，每个并发使用者
有自己的拥有引用，允许按 C 合同跨线程及长于 Server 存活。close 失败须保留仍需
销毁的对象，不能把 C 的 TR_AGAIN 当作释放成功；Drop 的执行域与清理流程须在
实现计划中证明，不能假设任意回调里阻塞销毁都安全。公开头/API 可在
本次正式发布前按 #245 调整；当前 11 头是现状，不是阻止完整设计的兼容约束。

实施前先获本方案审阅，再写分任务计划；codec、历史输入/适配/生成漂移、OPEN/SELECT
集成、四基数桩与 Rust 安全包装分别交付，但均为 #245 必需部分。实际验证须含
跨语言黄金/错误向量、同 ID 不同描述、支持集有空洞/无交集/错选/重复/过早正文、
新旧双向与独立分支选共同旧版、新字段显式零/空及嵌套投影失败、零响应 MANY 的确认、
恶意深层/大数量、OOM、取消后迟到结果及同连接健康调用。现有两个研究探针均
不计为这些生产验收通过，#245 与正式发布目标继续开放。

本次修订已在本地核对三个 JSON 算例文件无重复键、两类摘要、SELECT 固定头/
条目偏移及 99 字节长度、三个 offer 大小边界和七个有序交集算例；原 14 个正文
字节向量与六个非法 UTF-8 向量未改变。策略键变化只改变 artifact 摘要，样例
method revision 不变。十二个本地引用目标存在，git diff 空白检查通过。这些是
设计记账检查，未运行生产协商、适配、C/Rust codec 或资源/取消状态机测试。
