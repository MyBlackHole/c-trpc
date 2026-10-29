# CTM1 具体提案：字段格式、方法合同与有界视图

日期：2026-10-29。状态：待用户审阅的具体方案，未批准生产实现。
承接[总体设计](2026-10-29-custom-message-codec-design.md)及
[形状对照](../../reviews/custom_message_shape_comparison_20261029.md)。
本文中的“要求”是拟采纳规则，不是当前 SDK 已实现的能力。旧探针不是本规范的
符合性实现；特别是实验四字节根长度不进入 RPC MESSAGE 正文。

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
  response_compression/max_peer_revisions。后者为正 u32，限制该方法的目录大小。
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

## 4. OPEN 合同及可信兼容目录

这里比较过三种安排，下表是设计推导，还没有相应网络实现/性能测量：

| 安排 | 部署收益 | 代价 |
|---|---|---|
| 本地兼容目录（本提案） | 复用 OPEN/INITIAL；请求可继续流水发送；保留新可选字段 | 旧 schema 实例须先取得扩大后的可信目录 |
| 协商共同描述版本 | 旧实例无需提前认识新 revision | 新端保留多版本 codec/适配；首条 typed 请求须等待版本选择，或另有等价预协商 |
| 在线交换完整描述 | 可在调用时比较独立发布的新定义 | 需要新增有界描述交换、缓存/失效及准入流程，现有 512 字节元数据不足以承载一般描述闭包 |

本方案选择已知发布窗口的兼容目录，不在 Call 中下载描述或信任对端自报规则。
每个本地生成方法绑定当前 method revision，以及可信本地部署配置提供的有限
accepted_peer_revisions 集合；本地 revision 总在集合内。构造时核对目录绑定的
local_revision、排序、重复项、数量上限和预算，复制冻结；Call 持有适用快照。

目录由离线工具比较完整本地/对端描述生成，逐对检查请求/响应双向 wire、API、
业务与策略差异。允许集合只包含 wire 可双向读取、业务声明兼容的 pair，策略
差异另列部署条件；不能由 A↔B、B↔C 推断 A↔C。永久保留发布历史以防 ID 复用，
运行支持窗口可以更小。目录不是认证凭证，也不能从远端哈希反向增加允许项。

旧二进制必须具有外置目录加载能力，不能仅把新哈希重新编译进新程序。离线工具
输出每方法本地 `.ctmc` 文件，构造时按本地配置路径读入、核对、复制冻结并关闭
文件；不在 Call 中读文件。文件通过原子替换部署，路径不进入 schema 摘要。
这不是热更新承诺：旧实例取得新目录后重启/轮换，既有实例仍持有原快照。
未配置外置目录时仅允许本地 revision；配置了文件却缺失/损坏时构造失败，不能
回退到单项目录掩盖部署错误。外置目录入口是第一版生产实现的必需能力。

```text
offset  size  CTMC v1
0       4     43 54 4d 43                  // CTMC
4       2     little-endian version = 1
6       2     reserved = 0
8       32    local method revision SHA-256
40      4     little-endian entry count N
44      32*N  accepted peer revisions，按无符号字节字典序递增，包含 local revision
```

文件必须精确为 44+32*N 字节，1<=N<=max_peer_revisions，所有乘加先防溢出。
只接受普通文件和完整快照，实际读取也限于上限，不能只信任 stat 尺寸；构造预留
目录工作/存储预算后才分配，错版本/保留位/绑定/顺序/截断/尾部均拒绝。目录保存
哈希，不要求旧二进制解析未来 schema；对应的完整描述与兼容审阅报告是本地发布
凭据，另行归档。允许项可信不代表对端已认证，身份与权限仍由现有 TLS/钩子负责。

```text
OPEN metadata:   key = "ctm-contract"
INITIAL metadata:key = "ctm-contract"
value = 43 54 4d 31 | method_revision_sha256[32]   // 共 36 字节
```

键长 12，已有元数据条目头 3 字节，所以每个方向占用现有 512 字节额度中的 51
字节；应用剩余 461 字节。不能挤掉用户条目、另设隐形额度或增加 Record 头字段。
生成桩拒绝应用自己提供这个键。原始 bytes 方法不启用本合同；库生成 CTM1 方法
的检查是内核固定前置条件，不是可被用户 admit/authorize 替换的约定。
注册了 CTM1 的方法即使经通用 bytes API 调用，也由内核注入/核对合同并验证正文；
生成桩提供类型化便利，不形成另一套可以绕过检查的 Call。已预编码输入的异步
验证失败按已受理 Call 的唯一终局交付，不能谎报同步受理失败或重放已提交输入。
现状依据为[元数据编码](../../../src/rpc/rpc_v2.c)、
[记录层键/重复检查](../../../src/rpc/rpc_record_stream.c)与
[公开元数据上限](../../../include/tr/rpc.h)；本提案没有修改这些实现。

Server 在完整 OPEN 元数据验证后、业务 observe/admit/authorize/work 前核对值、
本地路由和目录成员资格；没有有效合同不得进入业务钩子。MESSAGE 的 codec 仍
按请求/响应方向核对。响应 INITIAL 注入 Server 自身 revision，Client 在交付
任何 typed metadata/body 或成功终局前验证。零响应的 MANY 也必须在 OK FINAL 前
发 INITIAL；合法的非 OK FINAL 可以没有合同确认，不虚构成功或响应值。
该前置检查还需接入唯一终局/统计提交点，不能仅由用户回调收到错误后再改写结果。
未通过合同的 OPEN 不计 Server 已受理业务调用，也不交付应用 terminal；仍由
协议层终结该 Call 并回收资源。未知 revision 映射 FAILED_PRECONDITION；缺失、
长度非 36 或魔数非 CTM1 为格式错误。应用元数据合并失败沿用现有参数/长度错误。
Client 拒绝 INITIAL/响应合同时，请求可能已在 Server 执行；即使状态为
FAILED_PRECONDITION 也不能推断“未执行”或自动重放。Server 拒绝 OPEN 的保证
则是此次尚未进入业务钩子；两种观察点在诊断中区分。

滚动部署流程明确为：

1. 离线比较旧/新 schema，发布逐 pair 的兼容及策略报告和新目录。
2. 先为仍使用旧 schema 的实例部署扩大后的本地目录；实现须能在构造时读取
   可信目录数据，无需它认识未来字段。已有 Call 继续用旧快照。
3. 再滚动启用新 schema/生成桩，发送 profile 维持已知支持窗口的接收能力。
4. 排空退出支持窗口的实例及 Call 后，才移除目录项或提高发送范围。

如果跳过目录先行，旧端即使能按 TLV 跳过新字段，也会明确拒绝未知 revision。
这是防止同 ID 不同定义错误派发的代价；不宣称无需部署准备即可接受任意未来
schema。新旧完整描述无需相等，已登记的兼容 pair 可以双向调用。破坏性变化
用新类型/方法并行，默认目录不授权带破坏语义的同 ID pair。
目录不协商或证明对端实际资源配额。发布报告必须列出发送数据域和接收 profile
条件；缺少条件证明就不能宣称全值域无中断升级。超出实际额度仍明确拒绝为
RESOURCE_EXHAUSTED，不通过截断、改值或重放掩盖。类型兼容与资源拒绝分别验收。

## 5. 有界验证、访问与重编码

生成的类型图是只读静态描述，C 调用者须维持其存活与不可变，Rust 使用静态借用；
不在运行时解析 schema。Client/Server 构造复制冻结实际策略和兼容目录；各 Call
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
| 格式正确但目录未允许的合同 revision | TR_ERR_BAD_TYPE，合同原因 | FAILED_PRECONDITION / FAILED_PRECONDITION |
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

实施前先获本方案审阅，再写分任务计划；codec、目录工具/生成漂移、OPEN/INITIAL
集成、四基数桩与 Rust 安全包装分别交付，但均为 #245 必需部分。实际验证须含
跨语言黄金/错误向量、同 ID 不同描述、目录缺失/逐 pair 升级、零响应 MANY 的确认、
恶意深层/大数量、OOM、取消后迟到结果及同连接健康调用。现有两个研究探针均
不计为这些生产验收通过，#245 与正式发布目标继续开放。
