# RESET 墓碑真实 Channel 高频成本初测

日期：2026-10-25。关联 #242/ERR-E。版本：`2.0.0-dev.75 -> 2.0.0-dev.76`。

## 目的

dev.75 已覆盖最大容量下 40,000 个同 home 复合键的索引层最坏布局，但那是合成索引状态，不包含真实 Channel 处理路径。本切片在既有 `test_tombstone_real_channel_capacity` socketpair/Reactor/Channel 场景中补充计时观测，记录真实 OPEN/双向 RESET 周转的端到端测试夹具成本。

本次只增加测试计时、报告、剩余工作台账和版本号；没有改生产代码、公开 API/ABI 或 wire 协议，也没有为耗时建立机器相关的测试门槛。

## 计量方式

- Release 与 ASan/UBSan 配置 `max_streams=40000`，TSan 配置 5,000 以控制检测器运行时间；三者都执行相同周转和统计逻辑。
- 每轮先通过真实 Channel 测试夹具打开一条流，再从调用 `reset()` 前记录 `CLOCK_MONOTONIC`，在 RESET 完成帧被处理后记录结束时间。对全部轮次使用最近秩法报告 P50/P95/P99。
- 另以 `CLOCK_MONOTONIC` 记录整个 OPEN/RESET 循环墙钟时间，以 `CLOCK_PROCESS_CPUTIME_ID` 记录同一循环的进程 CPU 时间，并用循环次数除以墙钟时间估算 open_reset/s。
- 每条 RESET 的延迟窗口包含测试夹具的 RESET 发送、Reactor/Channel 处理和完成帧处理；整个循环计时还包含 OPEN。进程 CPU 计时是进程范围的值，不是单独某个线程的 CPU 成本。

## 环境与结果

本机 Linux x86_64，AMD Ryzen 7 PRO 4750U，16 个逻辑 CPU，GCC 16.2.1。三个 Release 运行各完成 40,000 次 OPEN/双向 RESET：

| Release 运行 | RESET P50 | RESET P95 | RESET P99 | 循环墙钟 | 进程 CPU | OPEN/RESET 每秒 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1151.42 µs | 1395.03 µs | 1712.33 µs | 47.97 s | 47.80 s | 834 |
| 2 | 1173.70 µs | 1687.32 µs | 2225.53 µs | 51.45 s | 51.41 s | 777 |
| 3 | 860.88 µs | 1018.86 µs | 1290.41 µs | 37.03 s | 37.33 s | 1080 |
| 中位数 | 1151.42 µs | 1395.03 µs | 1712.33 µs | — | — | 834 |

三次 Release 结果波动明显：P50 为 860.88–1173.70 µs、P95 为 1018.86–1687.32 µs、P99 为 1290.41–2225.53 µs。TSan（5,000 次）和 ASan/UBSan（40,000 次）下测试也通过；其计时受检测器开销影响，不与 Release 样本合并比较。

## 结论与限制

这组数据证明测试现在能观察真实 Channel 高频 RESET 路径，并给出当前开发机上的初步延迟分布和循环吞吐。它不是隔离生产代码的微基准：RESET 的测量区间包含 socketpair、Reactor 和 Channel 的集成往返；总 CPU/墙钟与吞吐还包含 OPEN、测试驱动、调度和其他进程活动。当前没有先前版本同机基线、专用安静机器、跨架构数据、CPU 亲和性/频率控制或性能阈值，三次样本的离散程度也不足以支持稳定性能结论。

因此，ERR-E 高频成本目前登记为“有本地初测，待受控基线和持续压力验收”。后续仍需在固定环境下重复基线比较，并结合长时间运行、不同 Channel 容量及信用/丢弃组合测量；本报告不关闭 #242，也不代表 RPC V2 已完成。

## 本地验证

- GCC Release：`test_stream_reset`，40,000 次，连续三次通过并记录上述数据。
- GCC TSan：`test_stream_reset`，5,000 次，通过。
- GCC ASan/UBSan：`test_stream_reset`，40,000 次，通过。
- `git diff --check` 与 `git clang-format --diff HEAD -- tests/test_stream_reset.c` 通过。
