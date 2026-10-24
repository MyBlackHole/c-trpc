# RPC V2 TEST-C2 Endpoint 发送背压回放记录

日期：2026-10-25。关联 #247 TEST-C；基线 TEST-C1 head `384a97a52adb7e9f22c2d20bc77f564316077b8a`。

## 交付范围

新增固定 schema v1 轨迹 `component-final-send-backpressure-v1.json`，关联 TEST-A/B/C，覆盖
OPEN、ACCEPT、MESSAGE、HALF_CLOSE、工作完成、ERROR 终局、终局后的 RESET 和槽释放。Python
适配器严格检查固定事件、字段、task/invariant IDs，并将模型抽象 `ERROR` 明确映射到 C 夹具的
`UNIMPLEMENTED` Dispatch 结果。

组件回放链接实际 `src/rpc/rpc_record_endpoint.c`。新增 `--replay-send-backpressure` 专用入口，
仅接受固定事件序列；C 夹具在 `tr_stream_sendv` 包装点设置一次拒绝。回放要求发送函数调用两次、
唯一拒绝被消费、只接受一个 `UNIMPLEMENTED` 最终状态帧，先发送后停止 credit、随后关闭，用户终局
回调一次、Endpoint 槽退役并且预算回到零。Python 比较状态、回调、工作完成、帧数、退役、预算与静默。

变更限于组件测试 C、Python 测试/适配器、新固定轨迹、既有 GCC 自测步骤、README、剩余工作记录、
实施/设计文档和版本号 dev.33→dev.34；没有修改生产 `src/`、公开 `include/`、ABI、协议、依赖、
Xmake target 或 CI job/check 名称。Channel、Dispatch、Reactor 与 Executor 仍是受控替身；本切片
不覆盖真实 TCP、生产派发调度、任意故障时序或 TEST-C 全部 P0 要求。

## 验证

临时目录指向当前 worktree `build/test-tmp/`，Python bytecode 写入关闭：

| 验证 | 结果 |
|---|---:|
| 新增前的组件映射测试（RED） | 11/15 通过；新增 trace 因未登记而按预期失败 |
| TEST-B/C2 组件映射自测 | 16/16 |
| L1 模型自测 | 28/28 |
| TEST-C1 explorer 自测和固定种子探索 | 15/15；3072 个事件，无模型失败 |
| Release C2 专用 Endpoint 回放 | 匹配；发送尝试 2 次，最终帧 1、回调 1、预算 0 |
| Python L1/L2 对照 CLI | `matched=true`，事件期望与实际 observation 匹配 |
| Endpoint 契约目标 | Debug 35/35；Release 35/35 |
| Release B1–B4 与 C2 组件回放 | 5/5 `matched=true` |
| 安装后的 GCC V2 SDK consumer | 首次与连续五次均通过；PR #322 首轮失败仍保留，不据此推断根因 |

`clang-format 18.1.8 --dry-run --Werror` 全文件检查与 staged `git diff --check` 通过；PR #322
的远端首轮 Make/install 检查失败记录在 C1 评估中。相同 Client 销毁断言此前已出现过，当前
本地连续六次未复现，但根因仍未确定；该失败不能记作本切片通过或环境偶发。

TEST-C2 仅证明一个固定 Endpoint 错误最终帧在一次 `TR_AGAIN` 后恢复；普通事件生成器尚未向真实
C 组件驱动故障日程，TEST-D/E、真实网络/派发、#248 和 V2 整体验收仍未完成。
