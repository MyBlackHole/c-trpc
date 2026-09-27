# TCP / RPC 混合负载与 deadline 压力基准

## 本轮边界

这是 opt-in 的诊断工具，本身不绕过或私自覆盖生产 socket 策略。当前高层
Client/Server facade 默认对已连接 TCP socket 启用 `TCP_NODELAY`，显式
`TR_TCP_NODELAY_DISABLED` 可保留 Linux 默认 Nagle 行为；自动 reconnect
继承 Client 的同一策略。默认构建不生成 `bench_rpc`。原有 CRC、调度、生命周期
和 Transport/RPC 单元测试继续保留；本工具不替代它们。

真实路径为：独立 Client 进程 → IPv4 TCP → Channel → RPC worker → Unary
response → Client result callback。方法 1 是 CONTROL lane 的小 Unary echo，
方法 2 是 BULK lane 的大 Unary echo；二者共享同一个 facade TCP connection。
BULK 不是裸 TCP 测试，也不是长期 Streaming / `send_buffer()` 零拷贝吞吐测试。
默认大消息 64 KiB、frame 上限 16 KiB，真实执行 RPC envelope、分帧、CRC 和
接收重组。请求前 8 字节携带序号，成功响应必须与请求长度、所有字节完全一致。

## 使用

```sh
xmake f -c -y -m release --toolchain=gcc --crc32c_portable=n
xmake build bench_rpc
python3 bench/run_rpc_bench.py \
  --binary build/linux/x86_64/release/bench_rpc \
  --trials 3 --requests 1000 --label gcc-release-auto \
  --output /tmp/rpc-bench/auto.jsonl

# 独立重新构建并比较纯软件 CRC；不要跨后端复用旧二进制。
xmake f -c -y -m release --toolchain=gcc --crc32c_portable=y
xmake build bench_rpc
python3 bench/run_rpc_bench.py \
  --binary build/linux/x86_64/release/bench_rpc \
  --trials 3 --requests 1000 --label gcc-release-portable \
  --output /tmp/rpc-bench/portable.jsonl

# 功能 smoke / 统计核算自测；不用于发布性能结论。
python3 -m unittest discover -s bench -p test_rpc_bench.py -v
python3 bench/run_rpc_bench.py \
  --binary build/linux/x86_64/release/bench_rpc \
  --smoke --output /tmp/rpc-bench/smoke.jsonl
```

Python 只使用标准库。输出包含构建标签、Git revision（存在时）、二进制 SHA256、
CPU/内核/affinity/cgroup 配额、完整 server/client 命令和每阶段原始汇总结果。
每个 case 的 stdout/stderr 单独保存在 `<output-stem>-logs/`。失败退出码和失败
case 不会被吞掉。正常/恢复 case 有丢失、超时或拒绝时不是可比较的成功样本；
错误计数仍保留在该 case 的原始 JSONL 中。

默认仅监听 loopback。也可在两个终端手动运行（服务端 stdin EOF 后 drain）：

```sh
./build/linux/x86_64/release/bench_rpc server --port 9000 --bulk-bytes 65536
./build/linux/x86_64/release/bench_rpc client --port 9000 --scenario mixed \
  --requests 2000 --window 8 --bulk-bytes 65536 --bulk-every 4
```

`--host` 只接受数字 IPv4。手动跨主机运行需要两端配置匹配、安全网络边界和分别
收集日志；自动 runner 只覆盖回环。工具不是生产认证服务，不应直接暴露到公网。

## 负载与口径

| runner 场景 | 内容 |
|---|---|
| small | 32 B Unary，窗口 1 / 8 / 32，分别运行 |
| bulk | 64 KiB BULK Unary，窗口 8 |
| mixed | 每 4 个请求一个 BULK，其余 small，窗口 8 |
| mixed、多客户端 | 两个独立 Client 进程，各一个连接和窗口 8，共享一个 Server |
| pressure → recovery | 16 窗口、5 ms deadline 调用 25 ms 延迟方法，随后同一 Client/连接发 64 个普通请求 |

Server 使用 2 个共享业务 worker；每个 Client 使用 4 个 callback worker。
普通 timeout 为 5 秒，容量参数为 64；所有池与消息大小都有显式上界。
每个 Client 预热结束后通过 stdin gate 等待，所有 Client ready 后才一起放行。
多客户端 case 还检查测量时间区间确实重叠，不能用串行测试冒充并发连接。

- **闭环窗口模型**：一个有界异步窗口，收到 callback 后补充请求，不额外创建
  每请求线程。不是独立于响应速率的 open-loop 到达模型。停止补充期间不会记录
  本来可能到达但未实际提交的请求；无 coordinated-omission 修正，不能据此承诺
  固定外部到达率下的 P99 或最大 QPS。
- **延迟**：在 submit 前读取 CLOCK_MONOTONIC，到 callback 入口；包含同步
  submit 的等待，不包含此前生成请求 payload 的时间。报告成功请求 P50/P99，
  同时报告所有已接受请求（含 deadline/error）的 P99。nearest-rank 计算。
  对应样本数为零时数值为 0，仅是占位，不表示零延迟。
- **速率**：`ok_rps` 只计算经过内容验证的成功响应。`payload_MiB_s` 计算成功
  请求 + 成功响应的应用字节总和，即 echo 情况为 2×payload；不是单向链路线速，
  不含 frame/RPC/TCP 头、ACK、重传，也不是备份落盘速度。
- **失败**：`submit_again`、其他 submit error、RPC deadline/error、内容错误分开
  计数。每个失败提交只记录一次，不静默重试。attempted = accepted + submit
  failures；accepted = completed = ok + callback failures。
- **CPU/RSS**：Client CPU 是阶段内进程 user+system 差值；Client RSS 是该进程
  生命周期峰值，不是阶段增量或全系统 RSS。Server 输出的是包含启动、预热、
  drain 的生命周期 CPU/RSS，不能当成纯测量区间值。
- **多进程**：每个 Client 保留自己的延迟分布和时间区间，不通过平均各进程 P99
  冒充总体 P99。多连接吞吐也不能忽略各自的测量区间而直接相加。

pressure 故意让 deadline 小于受控 handler 延迟，覆盖失败后继续服务的路径；
recovery 不启用 reconnect、不重建 Client/Server、也不先睡眠清空压力。它不是
CPU/内存/FD 耗尽、随机网络故障或长稳测试。没有证据时不把它称为“已证明所有
过载都能恢复”。故障矩阵和长稳仍需后续独立补充。

## Open-loop 到达率与 executor 容量曲线

`run_rpc_capacity.py` 使用独立于 callback 完成速度的固定到达时间表。它不是
“收到一个响应再发下一个”的 closed-loop window；每个预定 arrival 都必须落入
以下三类之一：

1. 本地 in-flight slot 已满，记为 `scheduler_dropped`；
2. 实际调用提交 API，但被同步拒绝，分别记为 `submit_again` / `submit_errors`；
3. 提交成功，最终必须收到 callback，按 OK / deadline / RPC error / 数据损坏分类。

因此 offered load 不会因响应变慢而自动下降。latency 仍从**实际 submit** 到 callback，
而预定时刻到实际 submit 的偏差独立记录为 `scheduler_late_*`，避免把负载发生器
自己跟不上误解释成服务端性能。每轮结束后还记录 `drain_tail_ms`，用于观察 arrival
停止后积压还需要多久才能排空。

默认容量实验使用 2 个 Server worker、10 ms 受控 handler delay、16 个
`executor_queue_capacity`、128 个本地 in-flight slots。默认 rate 点来自
`workers × 1000 / slow_ms` 的 0.25×/0.5×/1×/2×/4×；这个算式只是选择测试点的
**handler-only 理论上限**，忽略 Transport、RPC、调度和共享运行环境成本，不是实际
系统容量，也不能当作通过门槛。

```sh
xmake f -c -y -m release --toolchain=gcc
xmake build bench_rpc
python3 bench/run_rpc_capacity.py \
  --binary build/linux/x86_64/release/bench_rpc \
  --trials 3 --requests 256 \
  --workers 2 --slow-ms 10 --executor-queue 16 \
  --window 128 --capacity 128 --timeout-ms 1000 \
  --output /tmp/rpc-bench/open-loop.jsonl

# 自定义 offered-rate 曲线
python3 bench/run_rpc_capacity.py \
  --binary build/linux/x86_64/release/bench_rpc \
  --rates 50,100,200,400,800 \
  --output /tmp/rpc-bench/open-loop-custom.jsonl
```

Server executor 队列耗尽时，只要请求仍处于**首个业务 task 尚未进入 executor**
的 admission 阶段，就会显式返回容量拒绝而不是伪装成连接故障：

- Unary 返回空 payload 的 `RESOURCE_EXHAUSTED` response；
- Streaming 首条 REQUEST 若尚未执行 `on_open/on_message`，返回最终
  `STATUS=RESOURCE_EXHAUSTED`；
- 物理 TCP/Channel 保持可用，后续 Call 可以继续服务。

真正的 connection/transport failure 仍使用 `UNAVAILABLE`。

已经执行过 Streaming 业务 callback 后发生的 **mid-stream executor saturation**
采用有界背压而不是立即关闭 Stream：

- 每个已开始的 Server Streaming Call 最多保留 1 个尚未进入 executor 的 task；
- pending message 保持 RX buffer ownership，因此在真正进入 worker 前不归还该
  message 的 Stream flow-control credit；
- executor worker 从 queue 取走 task、实际释放 node capacity 后，会向 Reactor
  owner 合并投递 retry；owner 再把 pending task 按 Call 顺序放回 executor；
- peer half-close 若恰好发生在 pending message 期间，也会排在该 message 之后，
  不允许 `on_half_close` 越过 `on_message`；
- 如果同一个 Call 在已有 1 个 pending task 时又产生第二个无法接纳的 continuation，
  bounded pending 已耗尽，此时只终止这个 Call 并返回最终
  `RESOURCE_EXHAUSTED`，TCP/Channel 继续可用；
- 该 Call 在终止前可能已经执行过早先 callback，因此这个
  `RESOURCE_EXHAUSTED` **不是**“业务从未执行”的 admission rejection。

确定性 `test_rpc_stream_backpressure` 使用 1 worker + 16 queue：先让两个
Streaming Call 真正进入业务层，再用独立 filler Calls 填满 executor；验证 A 的
continuation 被保留且 RX credit 暂不归还，释放 worker 后 A 按序恢复并继续成功；
B 的第二个 pending continuation 超过每 Call 上限后得到
`RESOURCE_EXHAUSTED`，但同一 Channel/Connection 上新的恢复 Call 仍成功。

容量工具分别统计 `resource_exhausted` 与 `unavailable`，两者都必须是
`rpc_errors` 的子集，不能相互冒充。

### Worker / handler scalability attribution matrix

`run_rpc_scalability.py` reuses the same fixed-rate arrival accounting but
varies Server worker count and controlled handler cost. Its default matrix is:

```text
workers:    1, 2, 4, 8
handler_ms: 0, 1, 10
executor:   64 nodes
window:     128
```

For nonzero handler delays, offered rates are generated from the handler-only
arithmetic `workers * 1000 / handler_ms` at 0.5x/1x/2x/4x. For the 0 ms
handler there is no meaningful handler-only capacity formula, so the default
offered rates are explicit: 5000/10000/20000/40000 RPS.

The matrix is multi-source when needed. A rate at or below the configured
per-generator target uses one Client; higher rates are partitioned across up to
32 independent `bench_rpc client` processes. High-rate generators default to
one RPC worker each so the load generator does not spend shared-runner CPU on
idle Client worker pools. Generators complete their
window=1 warmup **sequentially**, so fanout cannot turn warmup itself into an
unmeasured saturation workload. After every Client has reached the start gate,
the runner sends absolute `CLOCK_MONOTONIC` start timestamps with small phase
offsets so their local fixed-rate schedules interleave instead of starting as
an accidental burst. Each process keeps its own scheduler lateness/drop
accounting; the runner sums exact counts and keeps the worst generator lateness
for load-fidelity attribution.

```sh
python3 bench/run_rpc_scalability.py \
  --binary build/linux/x86_64/release/bench_rpc \
  --trials 1 --requests 256 \
  --workers 1,2,4,8 --handler-ms 0,1,10 \
  --executor-queue 64 --window 128 --capacity 128 \
  --max-generators 32 --target-rate-per-generator 1250 \
  --generator-workers 1 --min-arrival-ms 250 \
  --output /tmp/rpc-bench/scalability.jsonl
```

Every rate point retains the full Client and Server diagnostic records. Each
`(workers, handler_ms)` group also emits a `scalability_summary` with the
highest **clean scheduled rate**, the first exact Server pressure rate, the first
load-generator lateness/drop rate, and maxima for Reactor busy ratio and
executor queue-wait/handler P99.

High-rate rows also lengthen the measurement automatically. The runner treats
`--requests` as a floor and raises the request count as needed to keep the
scheduled arrival window at least `--min-arrival-ms`. This prevents a short
high-QPS burst from fitting entirely inside the bounded executor queue and being
misreported as sustained capacity.

“Clean scheduled rate” is deliberately stricter than “all requests eventually
succeeded”: besides zero submit/RPC/resource-exhaustion errors, P99
schedule-to-submit lateness must stay within one arrival interval. If a 10 kRPS
schedule (100 us spacing) is being submitted several milliseconds late, the
tool marks `load_generator_late` instead of claiming that the Server sustained
10 kRPS. This boundary is about fidelity of the offered schedule, not a Server
latency SLA.

The tool intentionally does **not** turn a Reactor-busy percentage or an RPC
latency value into an automatic architecture verdict. Server wall signals come
from observable events: executor admission/hard-full, Reactor
command/completion queue full, or RX/TX/RPC-message/reassembly pool exhaustion.
Load-generator backlog is a separate signal derived from P99 submission
lateness crossing one scheduled arrival interval; explicit scheduler drops are
reported separately as well. Neither condition is attributed to the Server.

The GCC release CI job records one full matrix with normal runtime CRC dispatch
as a diagnostic artifact. It has no throughput, scaling-efficiency, P99, or
busy-ratio pass/fail threshold.

### Bounded-resource headroom A/B

When a scalability point first hits a bounded pool, `run_rpc_headroom.py`
separates a configuration wall from a deeper runtime wall by rerunning the same
offered rates with one controlled resource change.

The default experiment follows the first multi-source result as a staged
single-variable chain:

```text
workers:              8
handler:              0 ms
rates:                10k / 20k / 40k RPS

baseline:
  RX buffers:         benchmark-derived (272 at capacity=128)
  executor queue:     64

rx_headroom:
  RX buffers:         1024
  executor queue:     64

rx_executor_headroom:
  RX buffers:         1024
  executor queue:     256
  CONTROL TX items:   benchmark default (128)

rx_executor_control_headroom:
  RX buffers:         1024
  executor queue:     256
  CONTROL TX items:   2048

full_headroom:
  RX buffers:         4096
  executor queue:     256
  CONTROL TX items:   2048
  command queue:      benchmark default (1024)

rx_ceiling_headroom:
  RX buffers:         8192
  executor queue:     256
  CONTROL TX items:   2048
  command queue:      benchmark default (1024)

command_headroom:
  RX buffers:         8192
  executor queue:     256
  CONTROL TX items:   2048
  command queue:      4096

control_ceiling_headroom:
  RX buffers:         8192
  executor queue:     256
  CONTROL TX items:   8192
  command queue:      4096

executor_ceiling_headroom:
  RX buffers:         8192
  executor queue:     1024
  CONTROL TX items:   8192
  command queue:      4096

RPC message pool:     unchanged in all stages
reassembly pool:      unchanged in all stages
```

```sh
python3 bench/run_rpc_headroom.py \
  --binary build/linux/x86_64/release/bench_rpc \
  --rates 10000,20000,40000 \
  --workers 8 --handler-ms 0 \
  --headroom-rx-buffers 1024 \
  --full-headroom-rx-buffers 4096 \
  --ceiling-rx-buffers 8192 \
  --headroom-executor-queue 256 \
  --ceiling-executor-queue 1024 \
  --headroom-control-tx-items 2048 \
  --ceiling-control-tx-items 8192 \
  --headroom-command-capacity 4096 \
  --generator-window 256 --generator-capacity 256 \
  --max-generators 32 --target-rate-per-generator 1250 \
  --generator-workers 1 \
  --output /tmp/rpc-bench/resource-headroom.jsonl
```

The A/B runner also separates generator in-flight headroom from Server capacity.
The Server keeps `capacity=128` by default, while high-rate Clients use
`generator-window=256` and `generator-capacity=256`. This prevents a full
Client slot window from being mislabeled as a Server resource wall without
silently changing the Server's per-peer limits.

The benchmark binary exposes Server-only overrides for
`--rx-buffers`, `--rpc-message-pool`, `--reassembly-pool`,
`--control-tx-items`, and `--command-capacity`. A zero override keeps the
existing derived benchmark default. These flags do not change
library/facade defaults and are not passed to benchmark Clients.

The output records the actual observed pool capacities/executor queue size and
emits staged pairwise comparisons:

1. `baseline -> rx_headroom`, isolating RX buffer pressure;
2. `rx_headroom -> rx_executor_headroom`, isolating executor hard-full;
3. `rx_executor_headroom -> rx_executor_control_headroom`, isolating
   CONTROL TX item exhaustion;
4. `rx_executor_control_headroom -> full_headroom`, increasing RX from 1024
   to 4096 after CONTROL TX has headroom;
5. `full_headroom -> rx_ceiling_headroom`, increasing RX from 4096 to 8192
   while leaving command capacity at its default;
6. `rx_ceiling_headroom -> command_headroom`, increasing only Reactor command
   capacity from 1024 to 4096;
7. `command_headroom -> control_ceiling_headroom`, increasing only CONTROL TX
   item capacity from 2048 to 8192;
8. `control_ceiling_headroom -> executor_ceiling_headroom`, increasing only
   per-Endpoint executor queue capacity from 256 to 1024.

These last stages are deliberately ordered so a 40k CONTROL TX wall cannot hide
an executor wall, and neither can be mistaken for Reactor CPU saturation.

All staged comparisons retain the first exact Server pressure rate and the
wall-signal set before/after the resource change.

This is an isolation experiment, not an argument to increase production pool
defaults. If RX exhaustion disappears and another bounded resource becomes the
first wall, that new signal is investigated next. If Reactor busy time becomes
the limiting evidence only after bounded resources have headroom, then a
Reactor architecture change has a measurement basis.

### Shared-runner wall repeatability

Single high-rate runs on GitHub shared runners are diagnostic samples, not
stable machine-capacity measurements. If two runs disagree about the first
bounded wall, `run_rpc_repeatability.py` repeats the exact 40k schedule before
any further architecture or capacity change is considered.

The GCC release CI job runs three trials for:

- `baseline`: benchmark-derived bounded resources;
- `command_headroom`: RX=8192, executor=256, CONTROL TX=2048,
  command capacity=4096;
- `ceiling_headroom`: RX=8192, executor=1024, CONTROL TX=8192,
  command capacity=4096.

Each trial retains the full Server/Client diagnostics. The summary reports
clean-trial count, exact-Server-pressure-trial count, per-signal occurrence
counts, min/median/max successful RPC/s, Reactor busy range, command-budget-hit
range, and command queue peak. It also emits a machine-readable classification:

- `no_server_wall`: none of the repeated trials has an exact Server pressure signal;
- `non_reproducible_server_wall`: Server pressure appears, but no exact Server
  signal is present in every trial;
- `reproducible_server_wall`: at least one exact Server signal is present in
  every trial.

The summary separately exposes `reproducible_server_signals` and
`sporadic_server_signals`. Client/load-generator signals remain in the general
`signal_counts` but cannot by themselves classify a Server resource wall. There
is still no throughput or latency CI gate.

A wall observed in only one of three shared-runner trials is treated as
non-reproducible evidence, not as a reason to change the production
architecture. A repeatedly reproduced exact wall can then receive a targeted
A/B experiment.

#### PR #44 repeatability result

The GCC release repeatability artifact for PR #44 ran all three profiles three
times at 40k scheduled RPC/s. All 9 cases reported zero exact Server pressure,
zero command-budget hits and zero command-queue-full events. Command queue peak
was only 35--44 entries even for the 4096-capacity profiles; successful rate was
about 39.26k--39.58k RPC/s and Reactor busy ratio was about 0.267--0.289. The
separate staged headroom run also completed its 40k points without an exact
Server wall.

Therefore the earlier single shared-runner sample that filled the 4096 command
queue is classified as non-reproducible evidence. It is not a basis for raising
production command capacity or `TR_COMMAND_BATCH`; the latter remains a Reactor
fairness policy and should only change after a stable repeated bottleneck is
shown.

### Streaming continuation reserve

Server RPC Endpoint 额外提供 opt-in 的
`executor_continuation_reserve`（facade 对应
`limits.executor_continuation_reserve`）。默认值为 0，因此现有容量曲线和部署行为
不变。

启用 reserve 后仍只有一套有界 executor node pool：

- 新 Unary 和 Streaming 首个业务 task 属于 admission work，只能使用
  `executor_queue_capacity - reserve` 个 node；
- 已经接受的 Streaming Call 的后续 message、half-close、writable、close task
  可以继续使用全部 node；
- admission 在 reserve 边界命中 `TR_AGAIN` 后，沿已有逻辑返回
  `RESOURCE_EXHAUSTED`，不会把保留容量消耗掉；
- reserve 不改变 worker 数、ready-Call 调度或同一 Call 的串行规则；
- reserve 必须严格小于最终 executor queue capacity；0 表示关闭。

这一策略的目标是避免大量新 Call 把所有 task node 占满，使已接受 Streaming Call
连 continuation/lifecycle 工作都无法进入 executor。它不是吞吐量保证，也不是
per-service QoS；默认 benchmark 暂时保持 reserve=0。后续应增加独立 Streaming
fixed-rate 曲线，对比 reserve=0 与 reserve>0 下的 admission reject、continuation
进展和尾延迟，再决定是否推荐 facade 默认值。

CI 对 open-loop 只检查计数守恒、payload 正确性、固定到达时间窗和最终 drain，
不要求某个 QPS/P99，也不要求共享 runner 必须在某一 rate 点出现饱和。GCC release
额外保存一轮 5 点容量曲线作为诊断 artifact；它仍不是发布 SLA。

### 2026-09-27 executor 饱和诊断

在改成显式 `RESOURCE_EXHAUSTED` 之前，PR #29 的 GitHub shared runner（GCC
release、2 workers、10 ms handler、executor queue=16、128 offered arrivals）已经
确认了瓶颈位置：50/100/200 rps 全部成功；400 rps 为 80 OK + 48 UNAVAILABLE；
800 rps 为 48 OK + 80 UNAVAILABLE。400/800 rps 的成功吞吐都约 196.6 RPC/s，
而负载发生器 `scheduler_dropped=0`、P99 scheduler lateness 约 80/105 微秒。
这些数字只用于证明 executor saturation 是真实瓶颈并校验新状态语义，不作为
跨机器性能结论。新实现应在同类过载点把“handler 未执行的容量拒绝”报告为
`RESOURCE_EXHAUSTED`，而不是把它伪装成连接不可用。

## 校验与 CI

新 workflow 以 GCC debug/release、Clang release、ASan/UBSan、TSan 分别运行
自动/纯软件 CRC 两种构建。每次执行 7 个真实场景；pressure 附带 recovery。
校验计数守恒、每类请求覆盖、并发测量重叠、正常/恢复全部成功和实际出现压力
失败；不检查毫秒级延迟或 MiB/s 是否达到阈值。超时仅用于发现挂死。

统计校验器的 10 组测试会拒绝漏计 completion、隐藏 submit rejection、类别合计
错误、畸形/NaN 数字、错误时间区间、数据损坏、虚假 mixed 和无失败的 pressure。
C 程序在 callback 可能提前到达时也不持锁调用同步 submit；sample/payload
保持存活直到 callback 完成，异常退出清理先 destroy/join Client 再释放借用数据。

CI 上传完整日志、输出和精确 Git source archive，保留原始失败退出码。源快照只
用于重现当前代码，不含凭据、环境变量转储或 `.git`。没有自动重试失败的测试。

## 2026-09-26 本地测量

生产基线 `6d6f42535ed5d2ba12692852b5225bb6ff2d9df6`。从既有 CI archive 和
已合并补丁还原后，整个 `src/` tree 为 `0b70881848ed4a8ae04ca1b3ffb17d21d660de64`，
整个 `include/` tree 为 `d4f20305bcca4b95a9d2b3249c9e97b8ac0792a7`，与 GitHub
基线完全相同。没有把旧 Reactor 或离线控制调度候选版本作为当前主线测量。

GCC 14.2、`-O2 -g -pthread`，无 sanitizer/LTO；共享 KVM 环境，CPU 报告为
AMD EPYC 9V74，CPU 时间配额为 4 核等价，无绑核。每 case 预热 100 次，每个
普通 Client 测量 256 次，pressure 测量 64 次；三轮交替运行自动/纯软件后端。
每 case 新建 Server，普通消息使用相同参数，pressure/recovery 在同一连接中。
本地仓库是重建快照，原始 metadata 的本地 Git id 不是上述 GitHub 基线 id；
基线识别依赖已核对的 source tree 与 binary hash，而不是伪造本地 HEAD。

下表为三轮**单客户端**结果中位数；原始逐次运行汇总见
`bench/results/rpc-20260926.csv`。它不是每请求 latency trace，更不是独占硬件 SLA。
小样本、调度噪声和 TCP 延迟行为使结果波动明显，不用三轮数据给出显著性结论。

| 负载 | 自动 CRC | 纯软件 CRC |
|---|---:|---:|
| small，窗口 1，成功 RPC/s | 9364.78 | 7533.52 |
| small，窗口 1，成功 P99，微秒 | 269.87 | 277.16 |
| bulk，窗口 8，双向应用 MiB/s | 286.51 | 205.44 |
| mixed，窗口 8，成功 RPC/s | 207.77 | 175.96 |
| mixed 中 small 成功 P99，微秒 | 87975.35 | 88256.18 |

自动/软件版本在普通场景均完成全部请求。三轮 pressure 各 64 个请求均以
DEADLINE_EXCEEDED 结束；后续 64 个恢复请求均成功。结果证明了这些有限场景，
不证明不存在其他取消/恢复竞态。Sanitizer 数据只验证正确性，不混入性能表。

### 历史发现与生产落地：TCP_NODELAY

PR #26 测量时的主线 `src/socket.c` 没有设置 TCP_NODELAY。当时仅在未提交的临时对照构建中，给
创建及 accept 成功的 TCP socket 设置并检查 TCP_NODELAY，其余代码/参数相同。
三轮 mixed 单客户端 small P99 中位数从约 88 ms 变为约 0.616 ms；成功速率中位数
从约 208 变为约 14092 RPC/s。两客户端实验也观察到明显变化，详见 CSV 中
`nodelay-experiment`，**该实验不是提交版本的默认行为或性能**。

这个 A/B 结果支持优先审查 Nagle/ACK 与小包发送的交互，而不是继续优化 CRC 或
直接增加 Reactor。没有 packet trace，仍不能声称已经证明每一次延迟的具体来源。
RFC 9293 Appendix A.3 讨论了 Nagle + delayed ACK 对 request/response 的影响；
Linux TCP_NODELAY 的含义见 TCP 手册。

后续生产实现把策略收敛到高层 facade 边界：Client 首次连接、Server 已接受 peer、
以及 Client 自动 reconnect 都在 Reactor 接管 fd **之前**应用同一策略；低层
`tr_tcp_*` helper 不被全局强制修改。策略可显式禁用。测试通过链接期
`setsockopt` observer 验证默认双向设置、显式禁用和 reconnect 继承，不以
loopback 延迟数字作为 CI 通过门槛。上面的 2026-09-26 性能表仍是修改前历史基线，
不能当作当前默认 socket 策略的测量结果。

参考：
- https://www.rfc-editor.org/rfc/rfc9293.html#appendix-A.3
- https://man7.org/linux/man-pages/man7/tcp.7.html
