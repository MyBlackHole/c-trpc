# R14 RPC V1 核心退役设计

## 状态与基线

- 状态：设计已由用户批准，待审阅；尚未开始实现。
- 唯一跟踪入口：GitHub Issue #290，任务 R14。
- 前置切片：R12 Server V1 退役、R13 Runtime 旧 RPC Executor 所有权移除。
- 实施基线：`refactor/rpc-v1-retirement-r13-20261021`，提交
  `1b92b6790ec9304663fca78d0d977e57dc16eb48`。
- 当前候选分支：`refactor/rpc-v1-retirement-r14-20261021`。
- 架构边界：遵守 Issue #239、#240、#248、#249；本切片不代表 PR #288
  或 `main` 已集成。

## 问题

R11–R13 已让公开 Client、Server 和 Runtime 不再拥有 V1 Endpoint 或 V1
Executor Group，但 `src/rpc/rpc.c` 仍将 raw V1 Call/Endpoint、执行组和生命周期
故障路径编译进静态库。该对象仍引用 `rpc_wire` 和公开 RAW Codec。大量测试继续
直接构造 V1 Endpoint；公共 `tr/rpc.h` 也保留了 V1 续处理、回调、拦截器和消息
所有权类型。只删除实现、保留这些声明会产生无法链接的 SDK 合同；只删除测试
又会掩盖尚未迁移的行为约束。

R14 将这个闭包整体退役，同时保持当前 V2 公共门面与仍有独立用途的 RAW Codec。
RPC V2 的调用记录、传输状态和终局行为继续由 `rpc_record*`、`rpc_v2.c`、
`client.c` 与 `server.c` 实现。

## 目标与不做事项

### 目标

1. 删除 V1 raw Call、Endpoint、Executor Group 和仅供它们使用的旧 Wire 实现。
2. 从安装 SDK 移除不再实现的 V1 Call、Endpoint、Interceptor 等声明与配置位，
   仅保留 V2 使用的公共类型和操作。
3. 保留 `tr_rpc_bytes`、`TR_RPC_CODEC_RAW`、`tr_rpc_raw_encode()` 与
   `tr_rpc_raw_decode()` 的公开合同和实现；RAW Codec 是独立复制式工具，不代表
   V1 Endpoint 或旧 RPC Wire 仍受支持。
4. 对旧 V1 测试逐项记录去向：迁入 V2 覆盖、补充 V2 覆盖，或说明其只验证已退役
   的内部机制；完成分类后才删除相应 V1 fixture 和目标。
5. 让源码清单、安装头文件和静态归档都能自动证明旧核心已退役。

### 不做事项

- 不改变 RPC V2 的帧、调用、所有权、取消、期限、背压、排空或销毁语义。
- 不重新设计公共 RAW Codec，不增加另一种 Codec、拦截器、TLS 或压缩能力。
- 不合并 PR #288，不把 R14 候选状态描述为已集成 `main` 或可发布。
- 不重写历史审计、旧 benchmark 数据或既有故障记录；仅更新仍作为当前使用说明
  的文档。
- 不改变 Xmake 入口、CI 必需检查名称、矩阵或其触发规则。

## 候选方案

1. **在 R14 一次删除旧核心与 V1 公共声明（采用）**：先按行为逐项迁移或确认
   退役理由，再删除旧实现、私有接口、测试闭包和安装 SDK 中的 V1 声明。V2 共享
   类型及 RAW Codec 保留。这样不会留下可见但无实现的 V1 API。
2. 先删除实现、在下一阶段清理公共声明：中间版本的头文件仍承诺静态库不再提供的
   符号，安装消费者可能在链接期失败，拒绝采用。
3. 先拆出独立的 V1 Endpoint/Executor 模块再删除：Endpoint 与 Executor Group
   在同一退役切片内同时失去生产消费者，拆分只会新增即将删除的过渡边界，拒绝采用。

## 架构与公共接口

### V2 与共享公共类型

- 保留 `src/rpc/rpc_record*.c/.h`、`src/rpc/rpc_v2.c` 及其内部合同。任何仍被
  V2 使用的共享类型不得因文件名或 `tr_rpc_endpoint_*` 前缀而误删。
- `include/tr/rpc.h` 收敛为 V2 共享 RPC 类型：元数据与调用选项、基数、状态、
  语义统计和冻结方法描述。移除 V1 `tr_rpc_call_handle`、V1 消息/回调类型、
  Endpoint/注册/一元续处理声明、V1 上下文/拦截器类型及相关续处理函数。
- 移除 `tr_client_config` / `tr_server_config` 中仅用于拒绝 V1 拦截器的旧配置字段。
  普通 V2 Client/Server 的默认行为不变；此前非空字段已经返回
  `TR_ERR_UNSUPPORTED`，V2 不存在拦截器执行合同。
- 保留 `include/tr/rpc_codec.h`、`src/rpc/rpc_codec.c` 和其 ABI。保留 C/C++ 安装
  消费测试，并用单独 RAW Codec 测试覆盖长度、容量、零字节和数据往返边界。

### 删除的旧闭包

经消费者审计确认无 V2 依赖后，从 Xmake 和源码树移除 `src/rpc/rpc.c`、
`src/rpc/rpc_internal.h`、`src/rpc/rpc_wire.c` 与 `src/rpc/rpc_wire.h`。
同时清理旧 Endpoint/Executor Group 的符号引用、故障注入 wrapper、raw V1
Client 测试 fixture 和仅验证 V1 私有状态机的测试目标。静态归档不得再含
`rpc.c.o` 或 `rpc_wire.c.o`；必须保留 `rpc_codec.c.o`。

V2 对旧协议的拒绝验证继续存在，但由测试内固定的旧 Wire 字节样本驱动，不能再
调用被删除的 `tr_rpc_wire_*` 编解码器。此测试验证 V2 拒绝旧协议，不验证旧协议
编解码器自身仍可用。

### 清单与命名

- 将精确退役的 V1 名称加入 `rpc_v1_inventory.py` 的 deny 路径与 CI 检查；不要用
  会同时命中 V2 `tr_rpc_endpoint_result`、`tr_rpc_endpoint_terminal_fn` 等名称的
  前缀规则。
- inventory 自测必须同时证明旧 V1 函数/类型被拒绝、V2 Endpoint 结果/终局类型和
  `tr_rpc_raw_*` 公共 Codec 仍被允许。
- 使用归档成员清单和 `nm` 按对象检查结果，分别验证旧对象不存在、旧全局符号及
  未解析引用清零、RAW Codec 符号仍由 `rpc_codec.c.o` 提供。

## 测试行为和迁移规则

| 当前消费者 | R14 处理规则 |
| --- | --- |
| `test_rpc_executor_group_drain.c` | 将仍有价值的 V2 Server 已受理工作排空测试移到 V2 Server 生命周期目标；删除 raw Executor Group 控制竞争/停止重试测试。V2 通用 TaskExecutor 的自身合同由 `test_task_executor` 覆盖。 |
| `test_rpc_deadline_failure.c`、`test_rpc_deadline_fault.c` | 对照 R08 V2 deadline/cancel 矩阵逐场景核对时钟、准入、终局、取消和回收；未被 V2 合同覆盖的场景先补 V2 测试或以明确的 V1 专属机制退役，再删除 V1 夹具。 |
| `test_rpc_terminal_backpressure.c`、`test_rpc_final_close_retry.c`、`test_rpc_overload.c`、`test_rpc_stream_overload.c`、`test_rpc_stream_backpressure.c`、`test_rpc_unary_failure.c`、`test_rpc_executor_reserve.c` | 对照 R09 与 V2 记录调用、背压、终局/关闭重试及资源预算覆盖；保留每个适用于 V2 的语义断言，删除只观察 V1 私有队列/执行器结构的断言。 |
| `test_rpc_message_ownership.c` | 保留其通用池/预算所有权测试；不得因目标名中包含 RPC 而批量删除。 |
| `test_transport.c` | 删除其中 raw Endpoint、旧 Wire 和 V1 调用场景；保留 Channel/传输层覆盖及 facade V2 网络策略场景。RAW Codec 断言移入独立测试目标。 |
| `test_rpc_record.c` | 保留 V2 记录协议对旧协议的拒绝断言，改由固定 V1 Wire 字节样本输入；移除 `rpc_wire.h` include 和符号引用。 |
| `test_server_record.c`、`test_complex_constructor_faults.c`、`test_runtime_threads.c` | 清掉仅注入 V1 Executor Group 的 wrapper/构造故障或线程回收场景；保留 V2 task executor、Reactor、Server 回滚和重试合同。 |
| `tests/rpc_v1_test_client_fixture.*` | 删除其最后消费者（`test_transport`）后移除整个 raw V1 Client fixture。 |

上述矩阵是实施审查表，不预先声称每一条旧测试都已存在等价 V2 覆盖。任何与 V2
相符的旧语义，都必须先找到现有 V2 测试或新增确定性 V2 回归；仅对 V1 内部结构
和已不存在语义做验证的测试可直接退役，并需在 Issue #290 记录理由。

## 失败和兼容边界

- 默认 V2 门面从 R11–R13 起不协商或回退到 V1，因此 R14 删除旧 Wire 后，连接到
  旧 V1 对端仍应被拒绝；V2 协议解析/状态机不新增容错或回退路径。
- 保留的 RAW Codec 是字节复制辅助 API，不含服务/方法、Call、Wire Header 或
  RPC 状态；调用者自行管理输入/输出缓冲区，既有错误码与写入边界保持不变。
- 删除 V1 公开声明及不支持的拦截器字段会改变当前开发阶段 SDK 的源码/结构布局。
  项目尚未正式发布，架构总审查明确允许正式发布前清理旧 API/协议；本切片须在
  提交说明中标明需重新编译，不承诺与 V1 应用源/ABI 兼容。
- 生产 V2 行为不依赖 V1 配置。若删除某字段后安装消费或源码搜索发现新的消费者，
  先确定该消费者是否属于有效 V2 合同，再按本设计补充类型归属或迁移；不能保留
  一个无实现的 V1 API 作为临时兼容。

## 验收标准

1. 安装头文件没有 V1 Endpoint/Call/续处理/拦截器声明或旧配置字段；V2 公共消费
   源码不需私有头文件，RAW Codec API 可独立编译、链接、运行。
2. 源码和 Xmake 不再包含旧 RPC core 实现/依赖；release `libtrcore.a` 中没有
   `rpc.c.o`、`rpc_wire.c.o`，没有退役 V1 全局定义或内部消费者，且保留
   `rpc_codec.c.o` 与 RAW Codec 符号。
3. inventory 规则精确拒绝旧符号并允许 V2 同前缀类型；新增/更新的 inventory
   自测证明正反例。不得通过删弱规则、过滤构建对象或改告警基线规避失败。
4. 旧测试迁移表逐项核销；所有适用的 V2 deadline/cancel、唯一终局、背压/关闭、
   所有权和 drain 行为均有实际测试证据。旧协议拒绝测试保留但不链接旧 Wire。
5. Xmake 全量测试、安装后 C11/C++11 SDK 消费、源码格式、GCC/Clang CI、ASan/UBSan、
   TSan 和双静态分析按现有必需矩阵通过；CI 检查名称与触发规则不变。临时文件、
   日志、测试输出和构建产物均位于仓库已有 `build/` 目录，不写入 `/tmp`。
6. README、公共 SDK 文档、仍表达当前实现状态的架构/测试策略文档不再声称 raw V1
   Endpoint、Executor 或 Wire 仍可用；历史记录和旧 V1 基准数据保留原样。
7. Issue #290 同步精确基线、实现提交、实际运行的测试与 CI、未运行项、剩余风险、
   R15 下一步；R14 代码提交仍标记为待集成，不能据分支 CI 宣称已合并 `main`。

## 风险与缓解

- **V1 测试和 V2 行为不等价**：旧测试删除过早可能造成覆盖倒退。按上表逐个对照，
  将适用于 V2 的缺口转换为真实 TCP 或确定性故障注入测试后才删除旧消费者。
- **公共头文件共享类型混杂**：整体删除 `rpc.h` 会破坏 V2。仅删除 V1 专属声明，
  以安装 SDK 的 C/C++ 消费和源码搜索验证每个保留类型的所有者。
- **符号前缀误杀 V2 Endpoint 类型**：deny 清单按准确 V1 函数名/类型边界维护，
  inventory 合成用例验证相邻命名反例。
- **旧协议拒绝测试依赖被删模块**：将 V1 字节样本固定在测试里，确保测试目标不再
  链接 `rpc_wire.c.o`。

## 实施入口

R14 开始实施前，在 Issue #290 登记唯一切片、范围、不变量、上述 R13 基线、前置
依赖、验收标准和目标分支；按本 spec 写独立实施计划。每个行为迁移步骤采用先写
V2 测试、观察旧实现或缺失行为下的结果、再实现/保留最小测试的顺序。实现完成后
独立审查、检查精确 head 的 Actions 结果并同步台账。最终集成和 R15 全局退役门禁
仍是后续独立任务。
