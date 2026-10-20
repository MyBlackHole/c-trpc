# RPC V1 退役扫描与第一批死代码清理（2026-10-19）

## 固定身份与证据口径

- 集成基线：#286 的 `4d9cc0b9`；前置叠加 PR #281 → #285 → #286，均尚未直接合并 main。
- 此前全树审查仅是静态初探；本轮新增 `scripts/rpc_v1_inventory.py` 与 CI JSON 工件。
- 真实扫描首次通过的提交为 `8420215a71fbbdaa8201abd8c0ea86046ee2dd23`，完整 GCC release 构建后的日志/工件见 [CI #39268559822](https://github.com/MyBlackHole/c-trpc/actions/runs/39268559822)。
- 扫描覆盖 209 个 C/C++ 源码及头文件，识别 115 个 V1 相关名称（包含类型、函数和声明）；归档中有 50 个相关全局定义、20 个存在生产对象未解析引用的名称，52 个名称在 V1 核心之外的生产源码出现。
- 完整文件路径、源码行、Xmake 引用、静态库对象定义及未解析引用均以 `rpc-v1-retirement-inventory` 工件中的 JSON 为准。该扫描不是 C AST/跨线程调度证明，不能仅凭零引用判断整个旧引擎可删。

## 已确认不可直接删除的部分

| 部分 | 已发现的存活依赖 | 处理前置条件 |
| --- | --- | --- |
| 旧 RPC 执行器组 | `runtime/legacy_rpc_execution.c` 仍引用 group create/start/stop/can_start/destroy | 先迁出 Runtime 的 legacy group 生命周期/预算、完整替换失败回滚验收 |
| 旧 RPC Endpoint | Client 和 Server 遗留构造/释放、统计及断连收尾仍跨对象引用 | 完成 V1 facade 分支删除、V2 终止与强引用等价覆盖 |
| `rpc_wire.c` | 旧 `rpc.c` 调用 encode/decode_ex；旧协议测试仍在 | 清理 V1 路径并迁移 wire 负向测试 |
| `rpc_codec.c` | `tr_rpc_raw_encode` 被旧 rpc.c 使用；RAW 编码/解码在公开 `rpc_codec.h` 声明且有测试 | 先明确 Codec 共享 API 的长期归属，不按名字整文件删除 |
| 旧调用能力 | 多个 `tr_rpc_call_*` 入口仍有旧 RPC 故障测试，部分在 `rpc.c` 内部相互调用 | 迁移完成错误路径、背压/终局/取消测试后再逐批删除 |

**不应删除** `tr_rpc_endpoint_destroy_with_stats`：它仍由同一 `rpc.c` 中的 `tr_rpc_endpoint_destroy()` 调用，即使静态库 `nm -u` 不显示对象内部引用。源码内调用关系与归档对象级依赖必须一起审查。

## 第一批删除：已证明无源级调用者的旧 Endpoint 私有入口

1. `tr_rpc_endpoint_finalize_detached_with_stats`：扫描只发现 `rpc.c` 的定义和 `rpc_internal.h` 的声明；无其他 C/C++ 源码调用，无静态库跨对象引用。旧 Server 的现实收尾使用 `arm_detached_finalizer` / `release_detached_owner`，本函数未参与其中。
2. `tr_rpc_endpoint_flush`：同样只有定义及私有声明，无生产/测试/基准调用；其静态帮助函数 `tr_rpc_endpoint_flush_on_owner` 只被该包装调用，故一并删除。正常 Channel 刷新、协议重试和后台进度机制不因此改变。

这两个入口不是公开安装 SDK 的声明。移除它们及不再可达的静态函数，不改变仍在调用的 Endpoint API、线程所有权、流协议或资源上限。删除点均晚于旧静态分析器仍待核销的 `rpc.c` 早期诊断位置；任何变更都须由新 head 原生分析器核实。

在 CI 运行盘点脚本时新增 `--deny` 门禁，对上述三个已退役名字同时查源码引用和静态库全局定义，禁止以后重新加入。

## 必须迁移的 V1 回归场景

| V1 典型测试 | 要转移的真实不变量 | V2 对应验证入口（仅是工作入口，尚未宣称等价） |
| --- | --- | --- |
| `test_runtime_threads`、`test_rpc_executor_group_drain` | pthread 创建失败、停止/销毁竞争、join 部分失败重试、任务额度收敛 | `test_task_executor`、`test_server_record`、`test_runtime_threads` |
| `test_rpc_deadline_failure`、`test_rpc_deadline_fault` | 超时竞争、晚到成功不能复活、取消消息可靠终局 | `test_server_record_deadline`、`test_rpc_record_cancel` |
| `test_rpc_stream_backpressure`、`test_rpc_terminal_backpressure` | 队列满与终局控制信用、可靠排空、停止后不丢任务 | `test_rpc_record_dispatch`、`test_rpc_record_endpoint_tcp` |
| `test_rpc_final_close_retry`、`test_rpc_message_ownership` | 强引用与完成顺序、异常 close 重试、消息所有权不泄露 | `test_rpc_record_endpoint_contract`、`test_rpc_record_drain` |
| `test_transport` 与旧 Wire 场景 | 非法/断连/分片输入不跨流破坏、明确拒绝旧协议 | `test_rpc_record`、`test_rpc_record_stream`、`test_rpc_v2_sdk` |

旧测试必须保持启用直到新的**确定性错误注入和等价断言**得到验证。绿灯只代表当前树通过，不能把迁移候选列表当作测试覆盖证明。

## 合并与发布限制

本切片先完成审计自动化及无调用者的内部死代码删除。整体旧 RPC 状态机、执行器组、私有配置及旧测试仍存在，禁止声称 V1 已彻底退出生产构建。后续仍需独立代码审核、全部 CI/安装消费、源码格式、双分析器、Sanitizer 以及完整链接闭包确认；保持叠加草稿 PR，不直接合并 main。

## R11 当前进展（2026-10-20）

R11 已从 `tr_client` 移除旧 Endpoint 分支和 Client legacy 内部入口；V1 Server
回归通过 `tests/rpc_v1_test_client_fixture.*` 直接接入旧 Endpoint。
`test_rpc_message_ownership` 与 `test_rpc_executor_group_drain` 继续覆盖保留的 V1
Server/Endpoint 和 Executor Group，通过 raw V1 fixture 移除旧 Client facade 调用；
R10 的公开 V2 大消息所有权和多调用 drain 用例独立验证 V2 合同。其它仍验证旧
Server/Endpoint 行为的测试继续保留，不据此删除 V1 核心。消费者迁移、源码门禁和证据见
[`rpc-v1-client-retirement-20261020.md`](rpc-v1-client-retirement-20261020.md)。
