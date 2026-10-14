# 一手来源与借鉴范围

访问/研究日期：2026-10-15。本文登记研究入口，不表示引入相应依赖或已完成其全量源码审计。

默认分支和 latest 文档可能变化，实施时应固定真实选用版本并再次验证。下面列出的具体版本仅是本轮读取样本，不宣称它是最新、安全补丁已全覆盖或已被项目采用。官方文档说明的行为与本项目的候选设计分开；不将软上限、单语言运行时或示例程序泛化为全部系统保证。

<a id="s01"></a>
## S01：gRPC HTTP/2 协议

来源：[PROTOCOL-HTTP2.md](https://raw.githubusercontent.com/grpc/grpc/master/doc/PROTOCOL-HTTP2.md)，默认分支文档。
关注调用头、消息序列、方向性结束、最终状态及消息压缩上下文。历史重试文字不代替 S02 的后续重试设计；不以该文件证明各语言线程实现一致。

<a id="s02"></a>
## S02：gRFC A6 客户端重试

来源：[A6-client-retries.md](https://raw.githubusercontent.com/grpc/proposal/master/A6-client-retries.md)，本轮所读文件标明已实现，并含 2024-08-23 更新记录。
关注逻辑调用/尝试、提交条件、重放缓存、尝试数、退避、pushback 与服务配置责任。它不提供通用业务幂等性检测或恰好执行一次保证。

<a id="s03"></a>
## S03：gRPC 压缩指南

来源：[Compression](https://grpc.io/docs/guides/compression/)。
关注请求/响应方向不对称、接收能力和逐消息禁压缩。不能把应用消息压缩与 HTTP 头部或 TLS 记录压缩混同。

<a id="s04"></a>
## S04：HTTP/2 规范

来源：[RFC 9113](https://www.rfc-editor.org/rfc/rfc9113.html)。
关注流状态、连接/流信用、RESET、GOAWAY 与错误作用域。用于语义参照，不要求 c-trpc 照搬字段或把单连接顺序套用多 socket 连接组。

<a id="s05"></a>
## S05：Envoy 流控与水位

来源：[源码流控设计](https://github.com/envoyproxy/envoy/blob/main/source/docs/flow_control.md)、[官方流控说明](https://www.envoyproxy.io/docs/envoy/latest/faq/configuration/flow_control)。
关注高低水位触发反压及流/连接间传播。水位是软限制，不能当成总进程或逐消息存储的严格硬上限。

<a id="s06"></a>
## S06：nghttp2 C 帧层与消费信用

来源：[项目说明](https://nghttp2.org/)、[nghttp2_session_consume](https://nghttp2.org/documentation/nghttp2_session_consume.html)、[连接消费反馈](https://nghttp2.org/documentation/nghttp2_session_consume_connection.html)。
关注 C 后端能力及应用消费对流/连接信用的分别影响。动态生成文档可能标开发版；本轮没有确定产品依赖版本或继承其全部内存保证。

<a id="s07"></a>
## S07：grpc-go 流控实现

来源：[internal/transport/flowcontrol.go](https://github.com/grpc/grpc-go/blob/master/internal/transport/flowcontrol.go)。本轮读取的文件 blob SHA：`98cef9ec2baf0cde5f1870ea0305b66faf8adcf4`。
关注 writeQuota、trInFlow、inFlow、maybeAdjust、onRead。文件明确的软发送额度不能被重命名为本项目硬预算；这里是 Go 实现，不代表所有 gRPC 后端。

<a id="s08"></a>
## S08：libuv handle 与执行模型

来源：[handle](https://docs.libuv.org/en/v1.x/handle.html)、[设计概览](https://docs.libuv.org/en/v1.x/design.html)。
关注关闭请求、回调和存储回收分离。借鉴生命周期，不要求采用 libuv，也不把 handle close 与所有 request 的取消视为同一契约。

<a id="s09"></a>
## S09：Tokio 有界发送与容量预约

来源：[mpsc Sender](https://docs.rs/tokio/latest/tokio/sync/mpsc/struct.Sender.html)，本轮页面展示版本 1.53.3。
关注 reserve/permit、发送接受和接收执行的差异，容量等待的取消语义。示例语言的自动析构不能直接替代 C 的显式释放。

<a id="s10"></a>
## S10：Seastar gate

来源：[教程 gate](https://docs.seastar.io/master/split/15.html)、[gate API](https://docs.seastar.io/master/classseastar_1_1gate.html)。
关注关闭准入并等待已经进入的工作退出。gate 不强制终止任意业务代码，也不回收仍由外部持有的资源。

<a id="s11"></a>
## S11：OpenSSL 非阻塞 TLS

来源：[OpenSSL 3.5 非阻塞客户端指南](https://docs.openssl.org/3.5/man7/ossl-guide-tls-client-non-block/)。
关注 WANT_READ/WANT_WRITE、重复尝试与错误终止。示例等待方式不是 c-trpc Reactor 的实现模板；采用后端时需固定实际版本。

<a id="s12"></a>
## S12：OpenSSL 写入与重试契约

来源：[OpenSSL 3.5 SSL_write](https://docs.openssl.org/3.5/man3/SSL_write/)。
关注输入稳定性、部分成功、重试参数与消费进度。不能把已消费明文当远端 RPC 完成。

<a id="s13"></a>
## S13：OpenSSL 内部待处理数据

来源：[OpenSSL 3.5 SSL_pending](https://docs.openssl.org/3.5/man3/SSL_pending/)。
关注已处理明文和未处理记录不同；有 pending 记录不保证立即有应用明文。处理循环必须具备预算及无进度出口。

<a id="s14"></a>
## S14：gRPC 认证边界

来源：[Authentication](https://grpc.io/docs/guides/auth/)。
关注连接凭证与调用凭证分离，传输身份与调用权限不可互相替代。

<a id="s15"></a>
## S15：OpenSSL 分配器接口

来源：[OpenSSL 3.5 OPENSSL_malloc](https://docs.openssl.org/3.5/man3/OPENSSL_malloc/)。
关注分配/释放配对及分配器替换约束。该接口不自然提供逐 SSL 对象独立且可随时替换的预算域；内部分配纳入硬限制须另证。

<a id="s16"></a>
## S16：Zstandard 公共 API

来源：[v1.5.7 zstd.h](https://raw.githubusercontent.com/facebook/zstd/v1.5.7/lib/zstd.h)，固定 tag 研究样本。
关注 compressBound 错误、完整/增量处理、frame 边界和 ZSTD_d_windowLogMax 在流式路径的限制。解码窗口、实际输出、工作区、CPU 是不同约束；tag 不是当前安全版本的声明。

<a id="s17"></a>
## S17：Netty 引用计数对象

来源：[Reference counted objects](https://netty.io/wiki/reference-counted-objects.html)。
关注派生视图、共享存储和 retain/release 责任。C 按值复制描述符不自动增加持有引用，不能照搬语言层外观忽略生命周期。

<a id="s18"></a>
## S18：protobuf-c 描述与分配

来源：[生成代码](https://protobuf-c.github.io/protobuf-c/gencode.html)、[API](https://protobuf-c.github.io/protobuf-c/group__api.html)、[ProtobufCAllocator](https://protobuf-c.github.io/protobuf-c/structProtobufCAllocator.html)。
关注描述同源、unpack/free 配对和自定义分配器。堆限制不自动限制递归栈/元素复杂度/同步 CPU；选型需实测恶意输入。

<a id="s19"></a>
## S19：Nanopb 有界字段

来源：[概念文档](https://jpa.kapsi.fi/nanopb/docs/concepts.html)、[参考文档源码](https://github.com/nanopb/nanopb/blob/master/docs/reference.md)。
关注字段上限、静态存储与回调处理模式。使用回调的可变数据不自动继承全部静态存储保证。

<a id="s20"></a>
## S20：Buf 变化检查

来源：[Breaking change detection](https://buf.build/docs/breaking/)、[规则](https://buf.build/docs/breaking/rules/)。
关注源 API 与 wire 等不同层次的变化。工具揭示影响，是否批准首版前破坏性设计由项目决定；不强制拒绝所有变更。

<a id="s21"></a>
## S21：gRPC 截止时间

来源：[Deadlines](https://grpc.io/docs/guides/deadlines/)。
关注剩余预算传播与取消，避免把跨主机绝对单调时间戳直接比较。总期限不等于两个进程同一瞬间停止执行。

<a id="s22"></a>
## S22：libcurl multi

来源：[libcurl-multi](https://curl.se/libcurl/c/libcurl-multi.html)。
关注传输任务与事件驱动接口的分离。借鉴集成方式，不引入完整 curl 协议栈或假定其队列为本项目硬配额。

<a id="s23"></a>
## S23：Envoy 断路与资源域

来源：[Circuit breaking](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/circuit_breaking)。
关注连接、待处理请求、活动请求与重试的分别限制。阈值的实际语义/作用域需逐项核对，不能泛化为全局硬存储上限。

<a id="s24"></a>
## S24：gRPC 负载均衡接口

来源：[Custom load balancing](https://grpc.io/docs/guides/custom-load-balancing/)。
关注名称解析、连接管理和调用时 Picker。不同语言公开能力不同，服务目标对象不等于单个物理连接。

<a id="s25"></a>
## S25：Bazel 依赖可见性

来源：[Visibility](https://bazel.build/concepts/visibility)。
关注允许依赖可以由构建工具验证的原则。c-trpc 保留 Xmake，需用自己的组件/符号规则实现等价检查，不为引用此文迁移构建系统。

<a id="s26"></a>
## S26：SQLite 故障测试

来源：[How SQLite Is Tested](https://www.sqlite.org/testing.html)、[Memory Allocation](https://www.sqlite.org/malloc.html)。
关注第 N 次单次失败与从 N 起持续失败、故障与回收的组合。仅借测试方法，不引入数据库事务语义，也不宣称已获得或运行其全部测试套件。

<a id="s27"></a>
## S27：LLVM libFuzzer

来源：[LibFuzzer](https://llvm.org/docs/LibFuzzer.html)。
关注进程内覆盖引导测试、种子与最小化。项目维护和功能演进状态按官方说明核对，不以无崩溃或覆盖率作为正确性证明。

<a id="s28"></a>
## S28：Linux kref

来源：[kref](https://docs.kernel.org/core-api/kref.html)。
关注引用取得的时点和生命周期责任。不能先拿可能失效的裸指针，再任意加计数当成安全 retain。

<a id="s29"></a>
## S29：Linux 故障注入

来源：[Fault injection capabilities](https://docs.kernel.org/fault-injection/fault-injection.html)。
关注可控制地触发稀有失败。用户态项目采用适合自己的测试入口，不为模拟内核制造生产故障开关。

<a id="s30"></a>
## S30：gRPC OpenTelemetry 指标

来源：[OpenTelemetry metrics](https://grpc.io/docs/guides/opentelemetry-metrics/)。
关注调用与尝试等对象层次和指标口径。实现时固定实际使用版本、标签集合和成本；不能用无限业务标识增加指标基数。

<a id="s31"></a>
## S31：Google 代码审查标准

来源：[The Standard of Code Review](https://google.github.io/eng-practices/review/reviewer/standard.html)。
关注持续改善代码健康、评审证据和避免追求抽象完美。不能据此容忍已知的所有权/安全错误，也不是无期限重构理由。

<a id="s32"></a>
## S32：GitHub Actions 安全使用

来源：[Secure use reference](https://docs.github.com/en/actions/reference/security/secure-use)。
关注权限最小化、不可信输入与依赖标识固定。固定完整 SHA 不证明其内容可信或后续不需要安全更新。

<a id="s33"></a>
## S33：SLSA 构建来源

来源：[SLSA v1.1 Provenance](https://slsa.dev/spec/v1.1/provenance)，固定版本研究样本。
关注输入、构建和产物的来源记录。仅保存一份清单不等于具备来源真实性或达到某个 SLSA 等级。

<a id="s34"></a>
## S34：可复现构建定义

来源：[Reproducible Builds definition](https://reproducible-builds.org/docs/definition/)。
关注源码、构建环境和指令的共同作用。产物可复现、源码可信和不存在漏洞是不同证据。

<a id="s35"></a>
## S35：HdrHistogram

来源：[README](https://github.com/HdrHistogram/HdrHistogram/blob/master/README.md)、[DoubleHistogram API](https://hdrhistogram.github.io/HdrHistogram/JavaDoc/org/HdrHistogram/DoubleHistogram.html)。本轮读取 README 的 blob SHA：`ef0e6c8d8b3718d8da776ca1b3f2bf93a67cdca6`。
关注固定分辨率统计、单写者/多线程同步差异、间隔采样与协调遗漏处理。估算修正的样本不得当成真实发出的请求；普通 Histogram 不是天然线程安全。

<a id="s36"></a>
## S36：Tokio Receiver 关闭与未归还许可

来源：[mpsc Receiver](https://docs.rs/tokio/latest/tokio/sync/mpsc/struct.Receiver.html)，本轮页面展示版本 1.53.3。
关注关闭准入后仍存在的容量许可和接收结束条件。启发 c-trpc 区分停止、预约归还与对象最终释放。

<a id="s37"></a>
## S37：gRPC 服务配置

来源：[Service Config](https://grpc.io/docs/guides/service-config/)。
关注服务/方法级策略及其配置来源。c-trpc 需额外明确快照与信任边界，不允许不可信解析结果解除认证要求。

<a id="s38"></a>
## S38：gRPC FuzzingEventEngine

来源：[fuzzing_event_engine.h](https://github.com/grpc/grpc/blob/master/test/core/event_engine/fuzzing_event_engine/fuzzing_event_engine.h)。本轮读取的 blob SHA：`445af89c07171d2110c4e11e2c4b055e29595326`。
关注 Tick、可控定时任务、端点对和异步动作重放。它是测试实现，不是要求 c-trpc 采用 C++ 或照搬所有运行时对象。

<a id="s39"></a>
## S39：Linux refcount 与 atomic

来源：[refcount_t API compared to atomic_t](https://docs.kernel.org/core-api/refcount-vs-atomic.html)。
关注计数 API 的内存序差异。用户态 C11 的发布/获取证明应单独完成，不能直接假定 Linux 内存模型等于 C11 模型。

<a id="s40"></a>
## S40：Envoy HTTP 语义过滤接口

来源：[HTTP filters](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/http/http_filters)。
关注跨 HTTP 版本的请求事件抽象和暂停/继续处理。c-trpc 可采用静态窄接口，不要求通用动态插件链或每层队列。

<a id="s41"></a>
## S41：Linux close 的错误与 fd 复用

来源：[close(2)](https://man7.org/linux/man-pages/man2/close.2.html)。
关注 Linux 上失败关闭与重试数字 fd 的风险。逻辑 teardown 重试不是无条件再次 close 同一个数值；其他平台语义需另查。

<a id="s42"></a>
## S42：Clang 编译数据库

来源：[JSON Compilation Database Format](https://clang.llvm.org/docs/JSONCompilationDatabase.html)。
关注真实翻译单元、编译目录和命令参数记录。数据库存在不证明其与构建一致；项目必须保留覆盖及配置身份验证。

## 后续引用与更新

实施 PR 应对实际选择的库/生成器/分析器记录版本、来源和必要摘要；关键语义改变时附旧/新差异及回归。不为保持文档链接而冻结过时依赖，也不把本次来源读取当成持续漏洞监测。发现参照与目标不符时，更新取舍理由而不是强行套用。
