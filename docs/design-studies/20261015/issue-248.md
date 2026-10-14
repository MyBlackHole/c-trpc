# #248：模块化阶段整改与可执行边界分析

日期：2026-10-15。性质：长期治理 G01 下的有限阶段任务，不是要求项目永久重构。关联 #154/#234—#247/#249。

## 1. 现状与学习对象

P5 的目录/SDK 头迁移有真实成果，但 [Reactor](https://github.com/MyBlackHole/c-trpc/blob/130835414f3e00cf97d49ca8af21d7a83c25146d/src/execution/reactor.c)、[RPC 核心](https://github.com/MyBlackHole/c-trpc/blob/130835414f3e00cf97d49ca8af21d7a83c25146d/src/rpc/rpc.c)、[Runtime peer](https://github.com/MyBlackHole/c-trpc/blob/130835414f3e00cf97d49ca8af21d7a83c25146d/src/runtime/runtime_internal.h) 仍展示协议/执行/装配边界交织。本文不把任意 include 或合理的组合根当作缺陷。

[Bazel visibility](sources.md#s25) 表明组件的允许依赖可以被构建检查；[Envoy 协议无关过滤接口](sources.md#s40) 表明语义事件可以跨后端表达；[Netty 存储视图](sources.md#s17) 提醒共享数据不等于共享私有池布局。只吸收原则：保留 Xmake，不引入另一套构建、不复制全动态过滤链。

## 2. 目标组件与状态所有者

资源原语：拥有存储、预算令牌、引用；不认识 Method/流 ID。执行底座：事件源/定时器/任务/停止屏障；不解释 TRP。连接适配：Connector/TLS/字节进度。传输后端：帧、流、网络信用、错误域。Message：业务体/变换状态。RPC：Call/Method/元数据/最终状态。组核心：成员/亲和/epoch；组适配：实际 socket、前导门控和完成。会话组合根：生命周期装配与配置，不把可写私有 peer 表借给所有模块。

源文件层面应区分 socket 原语与依赖 Reactor 的 Connector；否则按 io/execution 两个目录画出的“循环”可能混淆逻辑层次。目标是组件依赖有向且可解释，运行时回调通过契约返回结果，不要求回调方向和编译依赖方向相同。

## 3. 构建清单与私有契约

建议增加受版本管理的模块清单（文件名待实施确定），每项包含：component_id、sources、exported_headers、private_headers、allowed_deps、exported_symbols、tests 与 transition_exceptions。消费者只能编译到必要的导出契约，不自动获得整个 src 包含路径。

一个模块有多个 .c 和私有共享头是合理的；不合理的是将全部 Endpoint/Channel 可变字段放入跨模块大 internal.h。库内导出契约也可以叫 internal，但名称不是豁免；私有符号不得通过手写 extern 绕过。

原始字节视图与持有句柄分开，池 free_list/lock/checked_out 只由分配器实现操作。热路径可内嵌/内联并共置分配，不强迫每次跨模块调用都堆分配或虚调用。只追求 opaque 指针数量会产生另一种复杂性，重点是外部是否能破坏状态不变量。

## 4. 模块化与生命周期一起迁移

推荐用一个真实切片牵引：应用字节 → Message none → 传输接受 → RX 持有 → release。明确两个接受点、拒绝/取消责任和预算 token，然后把相关调用方一起迁移。

RPC 拆分时，Call 终态仍由唯一核心控制；每 Call 顺序/准入属于 RPC 调度策略，通用 worker 不理解 STATUS。Channel 也不能把终态、重组取消和 drain 标志复制到多个模块各自维护。Runtime 的 reserve/publish/detach/retire 应是完整操作，不是为每个 peer 字段加 setter 继续共享状态。

每次迁移记录旧入口与消费者、新契约、资源责任、适配器寿命和删除条件。迁移适配器只在明确阶段存在，不能无限期让新旧两个协议所有者同时工作。允许破坏开发期 API，但运行时新旧版本识别必须正确。

## 5. 三类依赖检查与负向测试

编译图：从真实编译数据库及依赖文件提取，覆盖当前选项下条件编译；配置矩阵之外的分支明确未覆盖。符号图：对象未定义/导出符号与契约对照，测试手写 extern 绕过；静态内联/宏读取私有布局需另做头可见性/审查。运行图：回调、持有和同步等待关系，检查 owner 自等待及取消回收。

最终产品仍可聚合成一个静态库；模块测试必须能只链接规定对象与替身，不靠全 trcore 解决本不该存在的依赖。

负向用例包括：新源码未登记、生产依赖测试能力、越界私有头、逆向私有符号、去掉必要依赖却靠全局 include 侥幸编译、模块配置变化后门禁用旧 compdb。例外需要具体边、任务、理由、复核条件；禁止 src/** 大豁免。

## 6. 阶段切片和关闭条件

MOD-A：生成当前/目标组件清单及责任表；区分合法装配与违规边，不能拿目录图当全部证据。

MOD-B：实现依赖门禁与其负向自测；对当前债务只允许精确过渡记录，新增边界破坏必须失败。

MOD-C：完成资源/Message none 的局部可测试闭环，迁移真实消费者并删除旧私有入口。

MOD-D：随着 #240/#243/#246 迁出 RPC 通用执行与 Reactor 的协议状态，保留故障回归；不是先全仓大搬家。

MOD-E：组/会话/诊断入口收敛，验证没有通过 snapshot 返回可写内部指针。

关闭 #248 需要本轮定义的边界已实现、例外有批准结论且不掩盖原验收欠账、隔离和组合测试通过。长期 G01 规则继续；后续新缺口另建有限任务。本文没有实施依赖扫描、拆模块或证明所有边都合法。
