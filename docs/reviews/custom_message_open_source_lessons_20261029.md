# 自定义消息格式的开源方案对照与设计修正

日期：2026-10-29。关联 #245、#241、#242、#247。
依据：用户要求保留自定义传输和业务格式，并学习开源项目的优点。
本文包含官方文档/源码对照、项目设计建议及受限候选实验，不是第三方性能测量或
CTM1 实现验收。

## 对照范围与证据

只使用项目官方文档和维护方源码；不把项目宣传、默认限额或某语言行为泛化为所有实现。
不引入上述运行库依赖，不复制第三方实现代码或改写其许可证。

| 项目 | 值得学习的机制 | 在 c-trpc 的应用建议 | 不直接照搬的部分 |
|---|---|---|---|
| Protobuf | 稳定字段编号、显式 presence、保留删除编号、未知字段往返 | 独立跟踪缺席/零值；生成兼容检查；透明转发保留未知内容 | 不据此采用 .proto/运行库；不把普通序列化字节当作稳定指纹 |
| FlatBuffers | schema conformity 检查；访问前验证；借用原缓冲 | 验证后发布 C/Rust 只读视图；CI 检查演进；约束深度和验证对象数量 | 不先引入偏移指针图、C++ 构建器或声称端到端零复制 |
| Cap’n Proto | 无复制视图、遍历/深度限制、可演进编号 | CPU/访问工作也有预算；空元素与重复访问计入工作量 | 不引入跨段指针、对象重叠、能力 RPC 或动态加载远端 schema |
| Apache Thrift | IDL 同源生成；optional/isset；Compact 减少字段开销 | 传输可解析与业务必需分别验证；比较紧凑字段编码 | 不采用隐式 requiredness，不把不同语言默认值行为视为统一保证 |
| SBE | 顺序游标、直接缓冲读写、无分配 flyweight、版本/块边界 | codec 沿已有 Message 顺序访问，不增加线程跳转或中间复制；保留固定块候选作对照 | 其块尾追加规则不等于任意字段删除/任意布局均兼容 |
| Nanopb | 可配置 max_size/max_count；生成期选择存储策略 | C11 生成码显式有界；存储选项与 wire 身份区分 | 不把静态大数组作为唯一输出表示，也不启用无上限回调/动态分配 |
| Apache Avro RPC | 用协议摘要识别双方协议；有状态连接在握手成功后可停止重复发送握手，维护近期协议缓存 | CTM1 可先用逐 Call SELECT 保持状态简单；若每次调用的额外往返不可接受，再按连接代次缓存已选择的方法描述，并为缓存失效/并发首次协商定义状态机 | 不照搬完整协议文本交换、MD5 身份摘要或全协议缓存；CTM1 的 SHA-256 方法摘要只标识本地已有描述，不是认证 |

Protobuf 官方说明字段号不能复用、删除应 reserved；显式 presence 能区分缺席与
显式默认值。二进制解析后保留未知字段并不代表任何转换都保留：转 JSON 或逐字段
重建会丢失未知数据。[演进及未知字段](https://protobuf.dev/programming-guides/proto3/)、
[presence](https://protobuf.dev/programming-guides/field_presence/)

其 tag/varint、固定宽度与打包标量各有明确编码；确定性序列化也不是跨二进制版本
的规范化保证。项目应散列自身规范描述，而非任意消息序列化结果。
[编码与确定性边界](https://protobuf.dev/programming-guides/encoding/)

FlatBuffers table 的追加/显式 ID、deprecation 和 `flatc --conform` 提供演进约束。
访问不可信缓冲前的验证，以及验证器独立的深度、表数量和缓冲大小选项，值得借鉴。
这些选项本身不证明本项目当前已有总 CPU 硬预算。
[演进](https://flatbuffers.dev/evolution/)、
[验证器源码](https://github.com/google/flatbuffers/blob/master/include/flatbuffers/verifier.h)

Cap’n Proto 的读取器对遍历量及深度设限；其编码规范特别讨论了零大小列表元素的
工作放大。由此可推得：小 wire 不必然意味着少工作，不能只用字节数代替步骤上限。
不据较早网页的安全状态描述判断当前版本安全性。
[读取选项源码](https://github.com/capnproto/capnproto/blob/master/c%2B%2B/src/capnp/message.h)、
[编码安全考虑](https://capnproto.org/encoding.html#security-considerations)

Thrift 官方明确指出 required 会限制软版本演进；optional 使用是否设置的状态。
Compact 使用紧凑字段表示和整数编码，说明固定 12 字节字段头不是唯一选择。
其 Compact 说明具有文档标明的实现/历史背景，不当作当前所有语言实现的运行证据。
[IDL](https://thrift.apache.org/docs/idl)、
[Compact 规范](https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md)

SBE 强调直接缓冲、顺序访问、无分配 flyweight；版本机制以块边界及 actingVersion
读取旧布局，追加有位置规则。它为高吞吐路径提供另一种候选，而非“必然比 TLV 快”
的证明。[设计原则](https://github.com/aeron-io/simple-binary-encoding/wiki/Design-Principles)、
[版本机制](https://github.com/aeron-io/simple-binary-encoding/wiki/Message-Versioning)

Nanopb 支持独立的生成选项文件及静态/指针/回调等存储选择，显示 wire 格式和本机
存储策略可以分开。项目可借鉴受限生成思路，同时维持 C/Rust 的借用与额度合同。
[概念](https://github.com/nanopb/nanopb/blob/master/docs/concepts.md)、
[生成选项](https://github.com/nanopb/nanopb/blob/master/docs/reference.md)

Avro RPC 的官方握手通过协议哈希确认双方已知的协议；实现可以缓存近期协议，并在
有状态连接完成成功握手后，不再为该连接的后续调用附带握手。CTM1 采用每 Call
独立 SELECT，状态隔离更直接，但会让每次 typed Call 都多等一次网络往返；它不具备
Avro 有状态连接握手的摊销特性。若性能验收不能接受此成本，应另行设计连接代次内
按方法缓存的首次协商/并发合并/重连失效，不可仅凭本地曾成功过就跳过远端确认。
[Avro RPC 握手规范](https://avro.apache.org/docs/1.12.0/specification/#handshake)

## 对上一版草案的具体修正

以下为本项目根据资料作出的设计判断，不是上述项目对 c-trpc 的保证。

1. **拆开四类兼容性。** wire 可解析、生成 API、业务语义和资源策略分别登记。
   调小接收限额可能使大消息被拒绝，但不能因此直接判定字段编码发生破坏性变化；
   反过来，wire 能解析也不能证明业务语义或部署容量兼容。
2. **撤销全盘冻结运行参数。** 消息/数组/工作限额、压缩、lane、deadline 和重试
   策略不应全部混入类型身份。不能因调一个限额就强制新方法 ID。各项是否需要
   双端协商/部署约束要逐项说明，未实现的协商不冒充已有能力。
3. **减少 wire-level required。** 缺席在生成 API 中显式表达。业务必要字段由
   方法验证/准入规则处理；新增业务必需语义仍可能破坏旧调用者，不能伪装为 optional
   以绕过兼容检查。变更分类和方法契约检查须覆盖这一点。
4. **未知字段提供受限往返。** 保留原始视图以无损转发；还要设计显式的有界未知
   字段携带/重编码接口。已知字段修改时拒绝 ID 冲突，不能只提供整包 copy 后便宣称
   支持升级代理；也不默认把未知内容跨安全域转发。
5. **单独约束验证工作。** 除字节、元素和深度，还记录验证节点/步骤或遍历预算。
   迭代器生命周期与访问费用明确，不能通过反复建立视图绕过预算。超额与坏编码
   用不同原因报告，RPC adapter 分别映射资源拒绝和非法请求。
6. **撤回未测量的 wire 定稿。** 原 68 字节完整描述头、12 字节字段头没有足够
   证据支持作为默认。比较紧凑顺序字段编码与 SBE 式固定块；方法上下文已经知道
   的类型身份不必未经论证在每个 MESSAGE 再完整发送。独立存储帧和 RPC 正文可有
   各自明确封装，但不能在未校验方法契约时省略必要的匹配检查。
7. **兼容检查进入构建。** 参考 conformity 思路，固定发布支持基线、生成漂移及
   新↔旧双向向量；不依赖维护者肉眼对照 diff。报告 wire、API、语义、策略差异，
   不用一个“兼容”布尔值掩盖其中任何一类。

## 字节开销算例，尚非性能结论

逻辑输入固定为两个显式存在的字段：u64 offset=7、bytes data。下表只比较正文，
不含 RPC Record、Transport、TLS 或方法契约交换；也不比较第三方自包含文件头。
Protobuf 参照字段为 optional fixed64 与 optional bytes，不用默认值省略制造优势。

紧凑候选仅用于算例：每字段为 u32 最短 varint tag、u32 最短 varint 长度和值；
tag 由 field ID 和 5 位 kind 构成。该算例不代表已采纳格式，也不替代下节的受限实验。

| data 字节数 | 旧草案正文 | Protobuf fixed64/bytes 正文 | 紧凑候选正文 |
|---:|---:|---:|---:|
| 0 | 32 | 11 | 12 |
| 1 | 33 | 12 | 13 |
| 4096 | 4128 | 4108 | 4109 |
| 65536 | 65568 | 65549 | 65550 |

旧正文为 `20+12+n`；参照正文为 `9+1+varint_size(n)+n`；候选正文为
`10+1+varint_size(n)+n`。原草案另有每消息 68 字节头，该开销未偷偷并入对手列。
这些值由规格逐项记账，不是运行第三方编码器或本项目生成器的结果；没有 CPU、
尾延迟、分配、验证或压缩开销证据，不能据此宣布哪种方案更快。

## 已运行的受限候选实验

为避免只凭字段数猜测，编写两个可丢弃的 C11 候选，实际比较 u64 offset、bytes
正文及新增 optional ASCII label；二者都加四字节实验长度，CPU 包含相同外层检查，
下表正文字节不含这四字节。固定块候选借鉴 SBE 思路，但不是运行上游 SBE 库。

最终源码在 GCC Release、Clang Release、GCC ASan/UBSan（泄漏检查）及恢复的 GCC
Release 下各通过 1/1 独立目标。检查 v1/v2 双向读取、六种长度、截断/伪造外层长度
后的内部边界、非最短/溢出整数、工作额度、失败输出不变，以及修改 offset 后保留
旧读者不理解的 label。只改固定宽度字段，不能算作通用代理重编码已完成。

本机 Ryzen 7 PRO 4750U / Linux 7.2.11 / GCC 16.2.1 / Clang 23.1.4 / Xmake 3.1.2；
测量固定 CPU 0、热缓冲、共享开发主机。九轮交错顺序，预先指定前两轮预热，其余
七轮全部保留，产生 576 个批次记录。下表为 v2（含 label="new"）的 CPU 批次均值
中位数，单位 ns/op；它不是单次操作分位数或 RPC P99。

| data 字节数 | 阶段 | 紧凑正文 / 固定块正文 | 紧凑 CPU | 固定块 CPU |
|---:|---|---:|---:|---:|
| 1 | encode | 18 / 25 | 16.647 | 17.679 |
| 1 | verify | 18 / 25 | 34.014 | 17.289 |
| 4096 | encode | 4114 / 4120 | 43.330 | 44.974 |
| 4096 | verify | 4114 / 4120 | 35.946 | 17.439 |
| 65536 | encode | 65555 / 65560 | 1093.017 | 1143.346 |
| 65536 | verify | 65555 / 65560 | 36.623 | 17.598 |

完整消费 64 KiB 正文另为 9368.509 / 9437.952 ns/op；其差异也保留，不只挑选
有利列。在这个平坦 bytes 样例中，完整消费成本明显大于两者验证成本差；不能据此
推广到多字段、嵌套或随机读取，更不能宣布端到端性能相同。固定块验证更省 CPU，
紧凑字段节省正文空间，两项观察同时保留，没有据此次窄探针选定生产 wire。

[冻结实验源码及复现说明](experiments/ctm_encoding_spike_20261029/README.md)、
[完整数据](data/ctm_encoding_spike_20261029_results.csv)、
[七轮范围与汇总](data/ctm_encoding_spike_20261029_summary.json)、
[环境及输入摘要](data/ctm_encoding_spike_20261029_metadata.json)。
五个实验源文件逐字节匹配记录的 source_sha256；最终二进制 SHA-256 为
`8a841d5509d8260a55317f3bc8a8129367ffedf17dfd33a57a8d9d53b3acf4ae`。
原始构建日志、早期探针、临时二进制仍在本机 build/codec-spike-20261029/。
归档前统一源码换行后重新测量，未混用旧输入的数值；跨次纳秒变化尚未归因，
主机未锁频或隔离，不把这种变化宣称为格式优化收益。

这个实验没有数组、深层结构、完整 UTF-8、Rust、通用字段修改、RPC/TLS/压缩或
完整资源账本；结构步骤额度也不是总 CPU 硬预算。Xmake 必须显式 `-P .`，原误触
主工程的中止运行不计通过。详情和限制写入实验 README，代码不接入产品构建。

## 进入 wire 定稿前的剩余对照

后续[数组、嵌套、稀疏与未知字段修改实验](custom_message_shape_comparison_20261029.md)
已补 28 个形状的 Python 语义/字节参考与六个负对照，建议通用 RPC 优先收敛紧凑字段。
它未补齐 C/Rust 性能、借用及生成器验收，不改写上文冻结的平坦 C 数据与限制。

保持同样的类型/安全/预算语义，比较紧凑字段与固定块候选，至少包括：

- 小标量请求、4 KiB/64 KiB bytes、标量数组、嵌套结构及稀疏可选字段。
- 编码、完整验证、顺序读取、重复随机读取、未知字段保留/修改后的重编码。
- 新↔旧请求/响应、零值/缺席、ID 复用、未知枚举、坏长度/超长整数、深度及工作耗尽。
- C11/Rust 正反向黄金字节、实际分配/复制、CPU、P50/P99、总内存和代码体积。

性能比较必须记录工具版本、源码、二进制、主机/CPU、参数、分配器及校验范围，
保留负结果；不把只算字节的表替代运行。所有临时数据只使用工作区 build/。
完成对照并审阅具体设计后才进入生产 codec/生成器实现。
