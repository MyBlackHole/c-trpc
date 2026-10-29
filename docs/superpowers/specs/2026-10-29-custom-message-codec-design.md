# 自定义消息格式、schema 演进与 C/Rust 编解码生成设计

日期：2026-10-29。状态：补齐兼容演进后的具体设计待审阅，尚未实施。
依据：用户选择“传输和业务消息格式都自定义”，并要求先支持 schema 兼容演进。
关联 #245、#241、#242、#247；主会话内联执行，不使用子 agent。

## 交付边界与方案

业务消息不引入 Protobuf。现有自定义 TRP/RPC Record 继续承载正文，RAW 保留。
schema 的 JSON 只是构建输入，网络正文使用下述自定义 CTM1 二进制格式。

本子项目交付单源 schema 校验、兼容性检查、确定性描述与指纹、C11/Rust 编解码
生成、跨版本/跨语言黄金向量。后续 RPC OPEN 方法契约匹配、类型化 Client/Server
桩和 Rust 安全 Call 包装另行接入，未完成前不关闭 #245。

初稿 fbd9301 采用位置编码和精确指纹拒绝；用户要求滚动兼容后，改用有长度边界的
字段记录。完整描述指纹允许不同，兼容性由稳定身份、字段规则和冻结资源上限确定。
不把“散列不同”一律视为不兼容，也不把“版本更高”视为可接受的证明。
本设计不证明远端持有哪份完整 schema：未识别的完整指纹仍按本地结构合同检查数据，
不能因此登记为“已验证兼容版本”。正式兼容声明来自固定支持基线和双向运行矩阵；
远端伪报指纹不能改变本地检查、配额或授权。

| 方案 | 取舍 |
|---|---|
| 稳定 ID、字段记录、兼容性检查及生成代码（采用） | 可增删可选字段，支持旧/新双向读取；维护自定义格式和编译器 |
| 位置编码、精确指纹匹配（初稿，未实施） | 格式较紧凑，但相关定义变化要求双端同步更新，不满足本次滚动兼容要求 |
| 通用描述符加手写正文 | 接线少，但正文与方法仍会独立漂移，不作为完整类型方案 |

## 三种身份及版本

1. **wire 身份**：schema family UUID（128 位）和非零 u32 type ID。family 在源文件
   显式给出，不从路径、package 名或注册顺序推导；同一身份的字段 ID 永不复用。
2. **完整描述指纹**：规范化整个 schema 的 SHA-256，用于产物一致性、审计和诊断。
   可选字段演进会改变它；接收方不以指纹不同单独拒绝一个结构兼容的消息。
3. **方法兼容契约指纹**：单独散列 family、service/method ID、codec/wire 主版本、
   请求/响应 type ID、ONE/MANY 基数、冻结消息上限、lane、压缩集合和重放声明。
   不包含可兼容演进的类型内部可选字段或生成语言名称。后续 OPEN 接入必须比较它，
   在任何业务 work 前拒绝错配；本子项目只生成该契约，不冒充已接入。

类型 revision 为非零 u32；同一类型的本地 wire 字段定义变化须递增，不能回退。
接收方不只因 revision 较高而拒绝；它逐项验证实际字段。wire 主版本变化不是 minor
schema 演进，旧解码器明确拒绝。任何指纹、UUID 或版本自报均不授予安全权限。

## schema 与构建期限制

输入后缀 `.ctm.json`；顶层仅允许 format、family、package、types、services、
reserved_types、reserved_services、doc。两种顶层 reserved 列表可省略，规范化为空。
format 固定 `ctm-schema-1`，family 为规范小写非零 UUID。package 是点分 ASCII
标识符；生成标识符也仅允许 ASCII，拒绝 C11/Rust 关键字及生成名称冲突。
doc 为说明文字，不参与 wire 契约或指纹；不直接插入可执行代码或未转义注释。

JSON 重复键、未知键、浮点数字、非法 UTF-8、超过 64 层及超过 1 MiB 的输入拒绝。
类型/服务/方法/字段/枚举项 ID 均显式给出非零 u32，在所属域内唯一；名称也唯一。
不按列表顺序编号。每文件最多 256 个类型、64 个服务；每类型最多 64 个在用字段，
每服务最多 256 个方法。reserved ID/名称合计每个命名域最多 4096 项。

结构类型图必须无环，引用必须存在，结构/数组嵌套深度最多 16。第一版不支持
递归、map、联合体、任意对象、无界容器或用户解码回调。类型包括 bool、u8/u16/
u32/u64、i8/i16/i32/i64、f32/f64、bytes、UTF-8 string、命名 struct/enum 及 array。
字段显式声明 required/optional；optional 缺席表示 None/has_value=0，不隐式填 0。

每个 struct 显式给出冻结的 max_wire_bytes 和 max_fields，含完整帧上限最多
1 MiB，max_fields 最多 64。max_wire_bytes 必须容纳当前定义的最坏编码；它不会
随每次增加可选字段自动扩大。每个 bytes/string 有 max_bytes，每个 array 有
max_count（最多 65535）；所有上限用检查算术计算。嵌套引用按子类型的冻结预算
计算父类型最坏成本，以便子类型将来在其预算内演进而不突破旧父类型容量。

最小完整示例：

```json
{
  "format": "ctm-schema-1",
  "family": "8aa6b305-e886-46d6-b557-4a231604039f",
  "package": "backup.v1",
  "types": [
    {"id": 1, "name": "Chunk", "kind": "struct", "revision": 1,
     "max_wire_bytes": 131072, "max_fields": 16,
     "fields": [
       {"id": 1, "name": "offset", "presence": "required", "type": "u64"},
       {"id": 2, "name": "data", "presence": "required",
        "type": {"kind": "bytes", "max_bytes": 65536}}
     ], "reserved": []}
  ],
  "services": [
    {"id": 1, "name": "Backup", "reserved": [], "methods": [
      {"id": 1, "name": "Put", "request": {"type_id": 1, "cardinality": "ONE"},
       "response": {"type_id": 1, "cardinality": "ONE"}, "lane": "BULK",
       "compression": {"request": ["none", "zstd"], "response": ["none", "zstd"]},
       "replay": "never"}
    ]}
  ]
}
```

字段 type 为标量名，或 bytes/string 的 kind+max_bytes、
`{"kind":"ref","type_id":1}`、
`{"kind":"array","max_count":8,"element":"u32"}`；对象只允许相应键。
删除时 reserved 项为 `{"id":3,"name":"old_label"}`；纯改名使用
`{"name":"old_label"}`，仅保留旧名称而不封禁仍在用的 ID。保留 ID 与在用 ID
不得相交，保留名称与在用名称不得相交；重复保留项拒绝。枚举类型为 kind=enum、
revision、open、values（id/name 项）与 reserved，不设置 struct 的字节/字段预算。
open 必填：开放枚举保留任何 u32 数值，提供已知值查询，不把未知值变成 0；封闭枚举
拒绝未知值。所有枚举值，包括未知的 0，均按此规则处理；声明项 ID 自身仍非零。

compression 两方向必须含 none，仅允许 none/zstd。replay 缺省规范化为 never，
也可为 explicit，表示业务声明允许重放；仍须受未来尝试策略、提交点和预算限制，
并不启动自动重试。方法消息上限从对应类型冻结的 max_wire_bytes 导出。

## CTM1 二进制格式

整数逐字段小端编码；不发送内存填充、本机指针或本机结构布局。codec 编号为新
常量 `TR_RPC_CODEC_CTM1=2`，既有 RAW=1 不变。独立 struct 消息固定头为 68 字节：

| 字段 | 大小 |
|---|---:|
| CTM1 magic | 4 |
| wire_major=1 / flags=0 | u16 / u16 |
| 正文字节数 | u32 |
| family UUID，按源文本十六进制顺序的 16 字节，不套用本机 UUID 布局 | 16 |
| type ID / type revision | u32 / u32 |
| 完整 schema SHA-256 | 32 |

输入必须恰好为头加正文长度且不超过本地冻结上限。family/type ID 必须匹配本地
预期类型；revision 非零；flags 未知位拒绝。SHA-256 差异允许，不据此跳过检查。

正文为严格递增 field ID 的连续记录；不另设可被伪造的字段总数。每项固定头 12
字节：u32 field ID、u8 wire_kind、u8 flags、u16 reserved=0、u32 value_bytes，
后接恰好 value_bytes 字节。field ID 非零，重复或乱序均拒绝。
flags 的 bit0 为 MUST_UNDERSTAND：required 字段置 1，optional 置 0；其余位拒绝。

- 未知 optional 字段按经检查的 value_bytes 跳过，保留原始输入但不解释其内部。
  可以跳过未来非零 wire_kind；不递归扫描未知字段的任意嵌套内容。
- 未知 MUST_UNDERSTAND 字段拒绝；不能用“前向兼容”静默丢弃必需数据。
- 已知字段必须匹配 wire_kind 和 required/optional 标记，不能做整数截断或类型猜测。
  已知 required 字段最终必须全部出现。字段总数含未知项，仍受 max_fields 限制。

wire_kind 固定为 BOOL=1、U8=2、U16=3、U32=4、U64=5、I8=6、I16=7、I32=8、
I64=9、F32=10、F64=11、BYTES=12、STRING=13、STRUCT=14、ENUM=15、ARRAY=16；
0 保留。已有编号不复用。

| 类型 | 值编码 |
|---|---|
| bool | 恰好一字节，只允许 0/1 |
| 整数 | 固定宽度；有符号数为补码位模式，转换不触发有符号溢出 |
| f32/f64 | IEEE 754 位模式经 u32/u64 小端传输，保留 NaN 位模式，不声称数值规范化 |
| bytes/string | 原始内容，长度由字段头或数组元素长度给出；string 严格检查 UTF-8 |
| struct | u32 type ID、u32 结构正文字节数、该类型的字段记录；family 从外层继承 |
| enum | u32 enum type ID、u32 数值；按本地 open/closed 契约处理 |
| array | u32 count、u8 element_kind、u8 flags=0、u16 reserved=0，然后逐项 u32 长度+值 |

数组按本地 element 类型检查每项；嵌套 struct/enum 的 type ID 也必须匹配。
计数在循环前检查，长度从剩余字节扣减；不把溢出后的总长当作有效长度。
空结构、空数组、零长度内容及 optional 缺席分别有明确表示。字段出现而内容长度为
零不等于字段缺席；一条零长度业务消息也不等于 MANY 的零条消息。

独立 enum 不直接形成顶层消息；方法请求/响应必须引用 struct，可用一个字段包装
枚举或标量。嵌套 struct 的冻结字节预算为 max_wire_bytes-68+8；编译器先验证
max_wire_bytes>=68，再做减法。完整解析只遍历已知类型，深度最多 16。

## 哪些变更兼容

同一 family/type 身份的 required 字段、既有字段类型、presence、长度/数量限制、
嵌套类型身份、open/closed、max_wire_bytes、max_fields 是冻结契约，不因 revision
增加而改变。资源限制不接受“只调大一点”的隐式兼容：新发送者可能填满新上限，
旧接收者仍必须安全拒绝；要保证双向滚动接受，就必须冻结旧上限。

| 变更 | 同一兼容族允许？ | 原因/要求 |
|---|---|---|
| 增加 optional 字段 | 是 | 使用从未用过的 ID；当前最坏编码及总字段数仍在旧冻结预算内 |
| 删除 optional 字段 | 是 | 同时永久 reserved 原 ID/名称；旧端仍发来时作为未知 optional 跳过 |
| 增加/删除 required 字段 | 否 | 旧数据缺必需字段，或旧读者无法理解新必需字段 |
| 字段类型、presence、含义或字段 ID 复用 | 否 | 不能用 wire 大小碰巧相同代替语义兼容；含义变化须由业务审查识别 |
| 字节/数量/字段/消息/深度预算变化 | 否 | 保留双向接受与旧端资源上限；不能以协商字样掩盖未实现的协商 |
| 字段源码顺序或生成语言名称调整 | 是 | wire 按 ID 排序；若只是改名，旧名称记入 reserved，ID 继续对应原字段 |
| 开放枚举增加值 | 是 | 旧端保留 Unknown(u32)，业务显式处理，不替换为旧默认值 |
| 封闭枚举增加值，删除/重用枚举值，open/closed 改变 | 否 | 旧读者会拒绝或改变原值含义 |
| 方法请求/响应类型、基数、大小、lane、压缩或重放契约变化 | 否 | 方法兼容契约变化；分配新方法/服务 ID 并与旧方法并行注册 |

required 字段的含义不允许通过另一个新 optional 字段重新解释。新功能若必须让
对端理解，须使用新方法契约，不能把必需语义标为 optional 求得表面兼容。
未知字段跳过和开放枚举保留值，只保证定义的读取规则，不证明业务逻辑可忽略它们。

增加 optional 字段不需要运行时获取远端完整 schema；冻结的族预算和字段自描述
使旧解码器能够安全跳过。没有动态下载 schema、无限描述交换或以远端声明放宽上限。
这不是任意 schema 的兼容协商；越过上述规则时明确拒绝或使用新的并行契约。

## 基线、滚动升级与退出

生成器支持重复的 `--baseline <schema>`，对所有受支持发布基线执行结构检查，而非
只比上一提交。基线文件、摘要和支持窗口索引纳入版本控制，发布后不改写原文。
reserved 约束向后续版本继承，防止“删除一次后下一版又复用”。改变 wire 字段时
revision 必须严格增加，纯顺序/doc/生成名称调整不强制增加。

普通用户生成可以创建新 family；要宣称兼容某发布版本，必须给出该基线并通过检查。
源文件描述该 family 的完整自包含快照，第一版不支持跨文件动态导入或跨 family
字段引用。删除不再使用的类型/服务仍须 reserved 身份；支持窗口内的基线方法和其
可达类型不得删除。revision 用尽时不能回绕，须分配新的契约身份。
正式仓库构建与 CI 的生成漂移检查必须自动读取支持窗口索引，不允许只省略参数
就绕过兼容检查；没有基线的首次版本明确标为首次发布，不伪造兼容结论。

推荐滚动次序：先部署能够接受旧消息且可选处理新字段的 Server，再部署新 Client；
在混部窗口内新响应也必须被旧 Client 读取。因为只允许上述双向规则，反向部署顺序
也须通过数据向量；推荐次序不代替这个要求。

破坏性变化分配新的类型/方法身份，或采用新的 family/wire 主版本。新旧契约同时
注册并在各自限额内运行，直到部署清单确认旧调用者退出，才能移除旧方法。
现有 RPC 表按 service/method ID 唯一注册，family 不会让重复方法 ID 自动合法；
并行契约必须使用不同 service/method ID，不能错误派发或透明改用另一方法。
不提供执行结果未知时的自动回退/重放。

类型化重建对象不保证保留未知字段；透明转发必须保留或有界复制完整已验证 wire
视图，生成器提供不修改原字节的 copy_wire。普通业务读取后重建响应可能丢弃自己
不处理的 optional 数据，这个行为需在适配器文档和跨版本测试中明确，不宣称无损代理。

## 具体双向读取例子

上述 v1 的 Chunk 在 v2 保持 family、type ID、字段 1/2 和冻结预算不变，revision
增至 2，并增加字段 3：`{"id":3,"name":"label","presence":"optional",
"type":{"kind":"string","max_bytes":128}}`。
当前最坏帧由 65636 字节增至 65776 字节，仍在原 131072 字节上限和 16 字段预算内。

以 offset=7、data=单字节 0x41 为例，v1 的两个字段记录如下（空格仅便于阅读）：

```text
01 00 00 00 05 01 00 00 08 00 00 00 07 00 00 00 00 00 00 00
02 00 00 00 0c 01 00 00 01 00 00 00 41
```

正文为 33 字节，完整帧 101 字节。v2 若发送 label="新"，增加记录：

```text
03 00 00 00 0d 00 00 00 03 00 00 00 e6 96 b0
```

v2 正文为 48 字节，完整帧 116 字节。旧读者校验字段 1/2，按长度跳过未知且
非 MUST_UNDERSTAND 的字段 3；新读者读 v1 时给出 label=None，不把缺席变成空串。
两个方向都不扩大原预算。v3 删除 label 时 reserved 其 ID/名称，旧 v2 仍能读取
缺席值；v3 也能跳过旧 v2 发来的 optional label。

把字段 3 的 flags 改成 MUST_UNDERSTAND，旧读者必须拒绝；把字段 1 改成 U32，
即使数值 7 能表示，也不能自动缩窄为原 U64 契约。生成器在基线检查先拒绝这种
破坏性 schema，解码器仍独立拒绝伪造字节。这些是设计向量，尚非真实 codec 通过证据。

## 编解码所有权与执行域

C 输入只借用到 encode 返回，输出缓冲由调用方提供。完整值/上限/所需大小和
重叠检查通过后才编码；成功才写 written。源结构、借用切片及输出区重叠时提前拒绝。
解码先验证完整帧再发布视图，失败不改输出。没有隐式堆分配、元素数组分配或容器扩张。

C 视图明确借用原始不可变输入；数组用有界迭代器逐项读取。Rust 用 View<'a>、
借用切片、Option、已知值/Unknown 枚举及迭代器表达相同寿命，不把 C 指针伪装成
安全所有者，不用 unsafe/unchecked 下标绕过检查。存储只含调用方视图、游标和
最多 16 层已知类型栈；未知字段按长度跳过，不建立未知对象树。

后续 RPC adapter 仍须遵守 work/消息回调的借用寿命，持有 CallRef 不代表持有正文。
异步保存须使用明确的持有/有界复制，生成的安全 Rust 接口不能让请求视图跨回调逃逸。
本阶段不增加线程、队列、Reactor 业务解码或网络 schema 获取。

C/Rust 错误对应同一现有状态：magic 为 BAD_MAGIC，wire 主版本为 BAD_VERSION，
family/type/已知字段类型/封闭枚举为 BAD_TYPE，截断/尾随/容量/数量为 BAD_LENGTH，
未知必需字段为 UNSUPPORTED，重复/乱序字段及未知 flags/reserved 为 BAD_FLAGS，
非法参数/重叠/bool/UTF-8 为 INVALID。完整消息解码不返回 TR_AGAIN。
codec 不自行关闭连接或选择 RPC 终局；后续 RPC 接入按 #242 隔离目标 Call。

## 确定性、构建与验收

规范化语义对象固定为 ASCII JSON：键排序、列表按 ID 排序、无额外空白；doc 和
末尾换行不参与指纹，未提供的 replay 规范化为 never。完整 schema SHA-256 使用
ctm-schema-1 域；方法兼容指纹使用 ctm-method-compatible-1 域。输入路径、时间戳、
当前目录与注册顺序不进入产物。生成器版本另入 manifest，不代替内容指纹。

工具只依赖 Python 标准库，输出 .ctm.h/.ctm.c/.ctm.rs/.manifest.json，总字节最多
64 MiB，完成全量校验后发布；--check 拒绝缺失、额外或漂移产物。生成代码/注释为
简体中文约定，CLI 人读诊断接入 tool_i18n，机器键与错误标记保持稳定。
Xmake 为构建/测试入口，Rust 验证经显式目标调用配置的 rustc；缺工具不假称通过。
不改原必需 CI 身份或测试参数/超时；临时文件、缓存、日志和产物仅在 build/。
当前仍是 11 头 SDK，新增安装边界另行验证，不自动安装内部生成辅助文件。

验收必须实际执行：

1. v1↔v2 C/Rust 四向组合：增加 optional 请求/响应、删除并 reserved、未知字段位于
   首/中/尾、开放枚举新值、零值/空值/缺席区分、透明 copy_wire 保留原始字节。
2. 反例：增加 required、复用 reserved ID/名称、改变类型/限制/基数/压缩/重放、
   封闭枚举新值、revision 回退、跨 family 同 ID、相同 service/method 不同契约。
   编译器对承诺兼容的破坏性变更先拒绝，运行时仍独立验证不可信输入。
3. 每个截断点、尾随字节、假长度、巨大 count、重复/乱序字段、未知必需字段、未知
   flags、错 enum/struct ID、非法 UTF-8/bool、最大已知嵌套和未知可选深层字节。
   不因较新 fingerprint/revision 就扩大资源额度或跳过已知字段检查。
4. 全部标量边界与浮点位模式黄金数据；C 编码→Rust 解码及反向字节一致；旧/新
   合法重编码可以因未知字段丢弃而不同，只有 copy_wire 要求完整字节一致。
5. 失败输出不变、编码重叠拒绝、无隐藏分配、Rust 借用编译正反例、C11/C++11
   边界、GCC/Clang、ASan/UBSan、生成顺序/漂移/兼容基线及工具国际化检查。
6. 后续 RPC 接入补 OPEN 前方法匹配、四种基数、同连接健康调用、取消后迟到结果、
   压缩/内存预算及 Rust Call 所有权矩阵；本设计及纯 codec 测试不提前算作这些通过。
