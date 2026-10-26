# RPC V2 方法钩子审查记录

## 变更标识

- 实现提交：`619d353f2fba8c5769154a6a93ec497c1df18ab7`（`【F-0243】RPC: 增加类型化 V2 方法钩子, 2.0.0-dev.155 -> 2.0.0-dev.156`）
- 基线提交：`1a5a08c7ed9f3207e417889289acb089b55b2a1f`
- 分支：`feat/rpc-v2-method-hooks-243`
- 范围：本次 V2 Server 方法钩子切片；不代表 issue #243 或 V2 整体完成。

## 行为与所有权审查

- OPEN 顺序为 owner 上的观察回调、owner 上的快速准入回调、共享 TaskExecutor 上的可取消认证、owner 完成复核、应用 OPEN work。
- authorize 使用原 OPEN job、输入副本、peer identity 快照和既有单调用 pending 预算；不创建专用线程或无界队列。
- authorize 完成只在 owner `done` 边界发布。发布前检查 job/slot、route generation、V2 call identity、终局、停止状态和 deadline。
- REJECT 与非法结构化结果复用有界 OPEN job 形成空错误 FINAL；非法结果归一为 INTERNAL。迟到 ALLOW 不恢复已终局调用。
- 被接受的任务参数一直存活到 executor `done` 返回；终局观察只在 OPEN 观察已调用且提交工作已退出后执行。
- 未配置钩子的方法继续走原有路径；协议 wire 格式没有变化。`observe`、`admit` 与 `authorize` 都是固定类型字段，不引入动态或全局 middleware 链。

## 验证证据

| 配置 | 命令/覆盖 | 结果 |
| --- | --- | --- |
| GCC Release | `TMPDIR="$PWD/build/tmp" xmake test -P . -v -j1` | 92/92 通过 |
| GCC ASan/UBSan | `test_rpc_record_dispatch/*`、`test_rpc_v2_sdk/*` | 2/2 通过 |
| GCC TSan | `test_rpc_record_dispatch/*`、`test_rpc_v2_sdk/*` | 2/2 通过 |
| Clang Release | `test_rpc_record_dispatch/*`、`test_rpc_v2_sdk/*` | 2/2 通过 |
| C++11 public header | `g++ -std=c++11 -Wall -Wextra -Werror -pedantic -pthread -Iinclude -fsyntax-only -x c++ tests/test_rpc_v2_sdk.c` | 通过 |
| 格式与补丁 | 仓库 clang-format 18.1.8 changed-line 检查、`git diff --check` | 8 个源文件、0 项格式错误；补丁干净 |

所有 Xmake 验证使用 `TMPDIR="$PWD/build/tmp"`；未向 `/tmp` 写测试文件、日志或产物。

## 性能影响与限制

- 没有钩子的方法不增加回调或任务阶段。
- 配置观察/准入的调用会在 Reactor owner 上同步执行用户回调；应用应保持回调短小。
- 配置 authorize 的调用会多经过一个共享执行器任务阶段，认证耗时期间占用既有有界任务槽和单调用 pending 额度。取消是协作式的，不能强制中断阻塞中的用户回调。
- 本次未运行性能基准；性能影响是基于执行路径的审查结论，不是量化测量结果。
- 本地全套与专项验证通过不等于远端 CI 通过，也不覆盖 #243 / V2 其余未完成的验收项。

## dev.157 公共 SDK 迟到授权集成补充

本次只修改 `tests/test_rpc_v2_sdk.c` 与验证记录，没有修改生产代码或公共 ABI。新增用例沿公共
V2 Client/Server 的真实 TCP 路径运行：授权回调在服务端取消令牌置位后才返回 `ALLOW`，分别
覆盖主动取消与 deadline。测试检查原调用分别保持 `CANCELLED`/`DEADLINE_EXCEEDED` 终局，业务
OPEN work 不会执行，观察回调与终局回调顺序正确，并在同一 Client/Server 上完成后续健康调用。

| 配置 | 命令/覆盖 | 结果 |
| --- | --- | --- |
| GCC Release | `TMPDIR="$PWD/build/test-tmp" xmake test -P . -v -j1 'test_rpc_v2_sdk/*'` | 通过 |
| GCC ASan/UBSan | 同一 SDK 目标，`detect_leaks=1` | 通过 |
| GCC TSan | 同一 SDK 目标 | 通过 |
| Clang Release | 同一 SDK 目标 | 通过 |
| GCC Release 完整套件 | `TMPDIR="$PWD/build/test-tmp" xmake test -P . -v -j1`，产物位于项目 `build/linux/...` | 92/92 通过；包含 40k RESET 压测 |

测试夹具要求二进制位于项目 `build/linux/...`，临时文件位于工作区 `build/test-tmp`；验证期间未向
`/tmp` 写入文件、日志或构建产物。此证据只关闭方法钩子在公共 SDK TCP 路径上的迟到 ALLOW
集成缺口，仍不覆盖通用异步操作内核、显式操作引用、队列/并发组合或跨 TLS/压缩调度。

## dev.158 共享执行器满载重试回归

本次仅增加 `tests/test_rpc_record_dispatch.c` 集成测试与验收记录，没有修改生产代码、公共 ABI
或 wire。测试在真实 RPC Record dispatch 所用的 TaskExecutor 上固定一个运行中的阻塞任务，
再排满其余三个容量槽；带 authorize 的 OPEN 进入 dispatch 后，在持续满载期间不得开始认证、
业务 work 或终局。释放阻塞任务后，之前已接受的四个任务均完成交付，OPEN 经有界 pump 重试，
authorize、业务 OPEN/MESSAGE/HALF_CLOSE FIFO 执行且终局仅一次。

| 配置 | 命令/覆盖 | 结果 |
| --- | --- | --- |
| GCC Release | `TMPDIR="$PWD/build/tmp" xmake test -P . -v -j1 'test_rpc_record_dispatch/*'` | 通过 |
| GCC ASan/UBSan | `-m asan`，泄漏检查开启，同一 dispatch 目标 | 通过 |
| GCC TSan | `-m tsan`，`halt_on_error=1:second_deadlock_stack=1`，同一目标 | 通过 |
| GCC Release 全量 | `TMPDIR="$PWD/build/tmp" xmake build -P . --all -v`；`TMPDIR="$PWD/build/tmp" xmake test -P . -v -j1` | 构建通过；92/92 测试通过 |

此证据验证队列满时的延期/恢复合同，不声称复现生产缺陷，也不覆盖完成队列满、永久执行器关闭、
通用操作引用或多方向 TLS/压缩共享调度。完整注册测试及 clang-format 18.1.8 修改行检查已通过；
dev.158 精确 head `a8b0f8aa31c4569005e6a404015a81cee2c55074` 的 GitHub CI 13/13 通过。所有本地测试临时路径均位于 `build/tmp`，未写入 `/tmp`。
