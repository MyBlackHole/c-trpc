# GOAWAY 期间 RPC CALL_OPEN 的未知结果边界

日期：2026-10-25；关联 #242/#244；当前版本 `2.0.0-dev.38`。

## 合同

GOAWAY 的最后受理边界只描述 Channel 物理连接上的传输流准入，不等于 RPC
`CALL_OPEN` 的业务准入，也不表示业务副作用已经或尚未发生。客户端在收到
`FINAL_STATUS` 前若只观察到连接/GOAWAY 相关 `UNAVAILABLE`，当前 SDK 不提供
“肯定未执行”的结果位，也不自动重试或重放。

## 回归

`test_rpc_v2_sdk` 使用真实 V2 Client/Server：手动启动调用并等待服务端处理
RPC OPEN，随后启动 Server drain。此时调用已被业务层接纳，drain 因未终结调用
返回 `TR_AGAIN`。测试再关闭 Client；Client 收到且只收到一次
`UNAVAILABLE/LIVE`，服务端恰好观测一次 OPEN 和一次业务终局。该结果不能被
解释为“服务端没有执行”，也没有客户端重放。

该目标在 GCC Release、Debug、ASan/UBSan 下均通过。新增场景后的 GCC Release 全量
`xmake test -P . -v -j1` 为 90/90，耗时 89.009s。消毒器启用泄漏检测并在首个问题处退出。

dev.37 修正该测试的销毁清理假设。GitHub Actions 曾在 `test_rpc_v2_sdk` 的一次性
`tr_client_destroy() == TR_OK` 检查处失败；这是测试没有遵守非 `TR_OK` 时对象仍归调用方、
需等待并重试的生命周期合同，并非 sanitizer 报告。该 GOAWAY 场景现只对 `TR_AGAIN` 在
`WAIT_MS` 总期限内重试，最终仍要求 `TR_OK`，其他错误立即失败。仅测试夹具及版本/记录改变，
运行时行为不变。GCC Release 全量为 90/90；ASan/UBSan 下该目标连续 5/5 通过。更新后 head
的 GitHub Actions 尚待完成。

dev.38 另修正 Clang Release 全量中暴露的 `test_client_record` stale-token 槽复用假设：
terminal callback 和弱引用释放可能早于传输层旧槽退休，容量为 1 时第二次 unary submit
可以暂时返回合法 `TR_AGAIN`。测试现按既有 slot-reuse 规则在 `WAIT_MS` 内推进 owner 并重试，
期间断言输出句柄不变且没有提前发出新 OPEN；最终仍要求 `TR_OK` 并检查同槽复用、旧 token
失效。Clang Release 下该目标连续 10/10、全量 90/90；格式化后的目标复验 1/1。GCC
ASan/UBSan 启用泄漏检测后该目标 5/5。更新 head 的 GitHub Actions 尚待完成。

传输层 `test_channel_graceful_drain` 已覆盖另一侧：本地排空前沿后的竞态
STREAM_OPEN 不分配流槽，远端按 GOAWAY cutoff 退役精确 OPENING 槽。
两项测试分别检查传输准入边界与 RPC 结果不确定边界。

## 未覆盖

此测试不实现幂等键、Attempt、跨连接重试或 exactly-once；这些属于 #244。
连接组 CONTROL/DATA 的跨成员接受记录和失败组合仍属于 #242/#243 的后续验收。
