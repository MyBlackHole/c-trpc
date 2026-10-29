# Reactor 完成标记测试同步修复

日期：2026-10-29；版本 `2.0.0-dev.175`。关联 #243、#247。
仅调整测试的观察同步顺序，生产 Reactor/异步操作路径和公开 API/ABI/wire 均未改变。
主会话内联执行，没有子 agent。

## 根因与复现

dev.174 之前未指定 `-j1` 的并行全量结果为 94/95：
`test_operation_completion_full_wait_resume` 的 `ctx.completions == CAPACITY + 1U`
断言失败。源码显示 `operation_completion_callback` 先设置 done 并广播，再调用
`completion_callback` 更新最后一次计数；被唤醒线程可以在两步之间取得 mutex。
两个字段分别持锁，不会形成裸数据竞争，但观察顺序不成立，TSan 不能替代这项验证。

独立于 TLS 修复，临时在广播解锁后加入 20ms 调度间隙，并对每个候选强制
`xmake build -r test_reactor_fairness` 再经 `xmake test -v -j1` 执行，避免短时间内
改写相同尺寸源码导致增量构建读取旧二进制。

- 原顺序 + 间隙：原计数断言失败。
- 计数移至 done 发布前 + 相同间隙：通过。
- 移除间隙后的最终源码：通过。

原始证据在工作区忽略目录 `build/performance-20261029/`：
`dev174-gcc-release-test.log`、`fairness-marker-red.log`、
`fairness-marker-green-gap.log`、`fairness-marker-green-final.log`。
最终源码不保留 sleep、调度探针或放宽断言/超时。

## 修复与验证范围

`completion_callback(ctx)` 先完成所有观察计数，随后才通过原有 mutex/cond
发布操作回调完成标记。等待线程观察到 done 后，计数、payload 析构和操作退役
均已完成。未新增运行时锁或修改生产生命周期合同。

最终无探针目标 GCC Release 连续 25/25；ASan/UBSan（泄漏检查）与 TSan 各运行
完成队列目标和 dev.174 的 TLS 资源账本目标，均为 2/2，命令墙钟分别为
3.532、3.727 秒。对应日志、逐轮日志和汇总位于工作区忽略目录
`build/performance-20261029/dev175-*`。最终 GCC Release 构建通过，串行全量
95/95，命令墙钟 169.723 秒；远端精确 head 推送后另行核对。

内联审查检查了标记的 happens-before、原有锁顺序、payload 析构与操作退役，
确认计数仍只发生一次、原计数断言及停止回收场景仍保留；生产源码 diff 为空。
完整资源账本、目标平台、容量以及 #243 其余生命周期组合仍属开放发布要求。
