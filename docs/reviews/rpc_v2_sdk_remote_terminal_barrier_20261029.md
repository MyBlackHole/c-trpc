# SDK 压缩取消回归的对端终局屏障

日期：2026-10-29；版本 `2.0.0-dev.176`，关联 #243、#247。
主会话内联执行，不使用子 agent。只修测试等待，不修改运行时、公开 API/ABI 或 wire。

## 失败与根因

TLS 周转候选的最终串行全量得到 94/95，既有
`test_cancel_compressed_call_destroy_barrier` 在检查服务端终局计数时失败。
该用例在 Client 的压缩 worker 挂起时取消 CallRef，释放引用并解除 worker，
等待本地 Client destroy 成功后，立即检查 Server 的独立 terminal 回调。
本地销毁只保证本地所有权回收，不构成对端 Reactor 已处理断连的屏障。

先列出“对端回调尚未到达”“终局状态不同”“跨用例状态残留”三项假设，再在
此用例临时启用服务端 terminal 进入后、取得观察锁之前的 200ms 延时，强制重建。
原顺序明确输出 `opens=1 requests=0 terminals=0 status=0` 并触发原断言；
加入对服务端 terminal 的等待后，相同延时输出 `terminals=1 status=14`
（UNAVAILABLE）并通过，随后移除探针的最终源码也通过。
这定位为缺失等待，状态与跨用例残留两项假设未复现。

## 修复与边界

在 Client destroy 成功后，复用原有 mutex/cond 和 `WAIT_MS` 的 `wait_count`，
等待服务端独立终局，再执行原有请求零交付、唯一终局、状态与统计断言。
未放宽任何计数、状态、超时，没有在最终源码保留 sleep 或诊断字段。
修改位于原 `TR_RPC_V2_TEST_FAULTS` 场景内，不进入安装 SDK 的独立 C11/C++11
消费端编译路径。TLS 周转测试另行提交，不与本修复合并描述。

原始证据在 `build/performance-20261029/`：
`dev176-boundary-gcc-release-final-test.log`、`sdk-remote-barrier-red.log`、
`sdk-remote-barrier-green-delay.log`、`sdk-remote-barrier-green-final.log`。
新探针和运行日志均留在工作区，生产源码 diff 为空。

## 验证

最终 SDK 目标在 Clang Release、GCC ASan/UBSan（泄漏检查）、GCC TSan、
GCC Release 四种配置均通过，配置/构建/测试命令墙钟分别为
13.547、15.978、13.493、12.237 秒；均包含压缩取消/销毁场景。
汇总与日志见 `build/performance-20261029/sdk-remote-barrier-matrix*`。
本修复与后续 dev.177 TLS 周转测试的最终组合 GCC Release 全量 95/95 通过。
包含本修复的精确提交 `97e106c6b43c4c0b8134dde2c328436009fb30ea` 远端检查
13/13 全部成功：[CI](https://github.com/MyBlackHole/c-trpc/actions/runs/40825127918) 8/8、
[RPC smoke](https://github.com/MyBlackHole/c-trpc/actions/runs/40825127993) 5/5。
内联审查确认条件变量使用原锁、等待者释放锁后回调才能更新并广播，所有原断言
保留；修改只增加远端完成观察，不改变销毁、取消或统计合同。
完整发布要求仍未完成；本修复只修正测试对远端回调完成时机的观察。
