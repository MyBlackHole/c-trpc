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
