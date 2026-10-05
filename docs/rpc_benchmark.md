# TCP / RPC 混合负载与截止时间压力基准

## 本轮边界

这是按需启用的诊断工具，本身不会绕过或私自覆盖生产套接字策略。
当前高层 Client/Server 门面默认对已经连接的 TCP 套接字启用 `TCP_NODELAY`，
显式 `TR_TCP_NODELAY_DISABLED` 可以保留 Linux 默认 Nagle 行为；
自动重连继承 Client 的同一策略。
默认构建不生成 `bench_rpc`。
原有 CRC、调度、生命周期和传输/RPC 单元测试继续保留；
本工具不替代它们。

真实路径为：
独立 Client 进程 → IPv4 TCP → Channel → RPC 工作线程 → 一元响应 → Client 结果回调。
方法 1 是 CONTROL 通道的小型一元 Echo，
方法 2 是 BULK 通道的大型一元 Echo；
二者共享同一个门面 TCP 连接。
BULK 不是原始 TCP 测试，也不是长期流式调用 / `send_buffer()` 零复制吞吐测试。
默认大消息 64 KiB、帧上限 16 KiB，
真实执行 RPC 信封、分帧、CRC 和接收重组。
请求前 8 字节携带序号，
成功响应必须与请求长度、所有字节完全一致。

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

# 功能冒烟 / 统计核算自测；不用于发布性能结论。
python3 -m unittest discover -s bench -p test_rpc_bench.py -v
python3 bench/run_rpc_bench.py \
  --binary build/linux/x86_64/release/bench_rpc \
  --smoke --output /tmp/rpc-bench/smoke.jsonl
```

Python 只使用标准库。
输出包含构建标签、Git 版本（存在时）、二进制 SHA256、
CPU/内核/亲和性/cgroup 配额、完整服务端/客户端命令和每阶段原始汇总结果。
每个测试场景的 `stdout/stderr` 单独保存在 `<output-stem>-logs/`。
失败退出码和失败场景不会被吞掉。
正常/恢复场景如果有丢失、超时或拒绝，就不是可比较的成功样本；
错误计数仍保留在该场景的原始 JSONL 中。

默认只监听回环地址。
也可以在两个终端手动运行
（服务端标准输入 EOF 后进入排空）：

```sh
./build/linux/x86_64/release/bench_rpc server --port 9000 --bulk-bytes 65536
./build/linux/x86_64/release/bench_rpc client --port 9000 --scenario mixed \
  --requests 2000 --window 8 --bulk-bytes 65536 --bulk-every 4
```

`--host` 只接受数字 IPv4。
手动跨主机运行需要两端配置匹配、安全网络边界和分别收集日志；
自动运行器只覆盖回环。
工具不是生产认证服务，不应直接暴露到公网。

## 负载与口径

| 运行器场景 | 内容 |
|---|---|
| `small` | 32 B 一元调用，窗口 1 / 8 / 32，分别运行 |
| `bulk` | 64 KiB BULK 一元调用，窗口 8 |
| `mixed` | 每 4 个请求一个 BULK，其余为 `small`，窗口 8 |
| `mixed`、多客户端 | 两个独立 Client 进程，各一个连接和窗口 8，共享一个 Server |
| `pressure -> recovery` | 16 窗口、5 ms 截止时间调用 25 ms 延迟方法，随后同一 Client/连接发送 64 个普通请求 |

Server 使用 2 个共享业务工作线程；
每个 Client 使用 4 个回调工作线程。
普通超时为 5 秒，容量参数为 64；
所有资源池与消息大小都有显式上界。
每个 Client 预热结束后通过标准输入门闩等待，
全部 Client 就绪后才一起放行。
多客户端场景还检查测量时间区间确实重叠，
不能用串行测试冒充并发连接。

- **闭环窗口模型**：使用一个有界异步窗口，收到回调后补充请求，
  不额外创建每请求线程。
  这不是独立于响应速率的开环到达模型。
  停止补充期间不会记录本来可能到达但未实际提交的请求；
  没有协调遗漏修正，因此不能据此承诺固定外部到达率下的 P99 或最大 QPS。
- **延迟**：提交前读取 `CLOCK_MONOTONIC`，到回调入口结束；
  包含同步提交的等待，不包含此前生成请求载荷的时间。
  报告成功请求 P50/P99，同时报告全部已经接受请求
  （包含截止时间/错误）的 P99。
  使用最近秩计算。
  对应样本数为零时数值为 0，只是占位，不表示零延迟。
- **速率**：`ok_rps` 只计算经过内容验证的成功响应。
  `payload_MiB_s` 计算成功请求 + 成功响应的应用字节总和，
  即 Echo 情况为 2×载荷；
  不是单向链路线速，不包含帧/RPC/TCP 头、ACK、重传，
  也不是备份落盘速度。
- **失败**：`submit_again`、其他提交错误、RPC 截止时间/错误、内容错误分开计数。
  每个失败提交只记录一次，不静默重试。
  `attempted = accepted + submit failures`；
  `accepted = completed = ok + callback failures`。
- **CPU/RSS**：Client CPU 是阶段内进程用户态 + 内核态 CPU 时间差值；
  Client RSS 是该进程生命周期峰值，不是阶段增量或全系统 RSS。
  Server 输出的是包含启动、预热、排空的生命周期 CPU/RSS，
  不能当成纯测量区间值。
- **多进程**：每个 Client 保留自己的延迟分布和时间区间，
  不通过平均各进程 P99 冒充总体 P99。
  多连接吞吐也不能忽略各自测量区间而直接相加。

`pressure` 故意让截止时间小于受控处理器延迟，
覆盖失败后继续服务的路径；
`recovery` 不启用重连、不重建 Client/Server，
也不先睡眠清空压力。
它不是 CPU/内存/fd 耗尽、随机网络故障或长稳测试。
没有证据时不能把它称为“已经证明所有过载都能恢复”。
故障矩阵和长稳仍需要后续独立补充。

## 开环到达率与执行器容量曲线

`run_rpc_capacity.py` 使用独立于回调完成速度的固定到达时间表。
它不是“收到一个响应再发下一个”的闭环窗口；
每个预定到达必须落入以下三类之一：

1. 本地进行中槽位已满，记录为 `scheduler_dropped`；
2. 实际调用提交 API，但被同步拒绝，
   分别记录为 `submit_again` / `submit_errors`；
3. 提交成功，最终必须收到回调，
   按成功 / 截止时间 / RPC 错误 / 数据损坏分类。

因此提供负载不会因为响应变慢而自动下降。
延迟仍从**实际提交**到回调，
而预定时刻到实际提交的偏差独立记录为
`scheduler_late_*`，
避免把负载发生器自身跟不上误解释成服务端性能。
每轮结束后还记录 `drain_tail_ms`，
用于观察停止到达后积压还需要多久才能排空。

默认容量实验使用 2 个 Server 工作线程、
10 ms 受控处理器延迟、
16 个 `executor_queue_capacity`、
128 个本地进行中槽位。
默认速率点来自
`workers × 1000 / slow_ms` 的 0.25×/0.5×/1×/2×/4×；
这个算式只是选择测试点的**仅处理器理论上限**，
忽略传输、RPC、调度和共享运行环境成本，
不是实际系统容量，也不能作为通过门槛。

```sh
xmake f -c -y -m release --toolchain=gcc
xmake build bench_rpc
python3 bench/run_rpc_capacity.py \
  --binary build/linux/x86_64/release/bench_rpc \
  --trials 3 --requests 256 \
  --workers 2 --slow-ms 10 --executor-queue 16 \
  --window 128 --capacity 128 --timeout-ms 1000 \
  --output /tmp/rpc-bench/open-loop.jsonl

# 自定义提供速率曲线
python3 bench/run_rpc_capacity.py \
  --binary build/linux/x86_64/release/bench_rpc \
  --rates 50,100,200,400,800 \
  --output /tmp/rpc-bench/open-loop-custom.jsonl
```

Server 执行器队列耗尽时，
只要请求仍处于**首个业务任务尚未进入执行器**的准入阶段，
就会显式返回容量拒绝，而不是伪装成连接故障：

- 一元调用返回空载荷的 `RESOURCE_EXHAUSTED` 响应；
- 流式调用首条 `REQUEST` 如果尚未执行 `on_open/on_message`，
  返回最终 `STATUS=RESOURCE_EXHAUSTED`；
- 物理 TCP/Channel 保持可用，后续 Call 可以继续服务。

真正的连接/传输故障仍使用 `UNAVAILABLE`。

已经执行过流式业务回调后发生的**流中期执行器饱和**
采用有界背压，而不是立即关闭 Stream：

- 每个已经开始的 Server 流式 Call 最多保留 1 个尚未进入执行器的任务；
- 等待中的消息保持 RX Buffer 所有权，
  因此真正进入工作线程前不归还该消息的 Stream 流量控制额度；
- 执行器工作线程从队列取走任务、实际释放节点容量后，
  会向 Reactor 所有者合并投递重试；
  所有者再把等待任务按 Call 顺序放回执行器；
- 对端半关闭如果恰好发生在等待消息期间，
  也会排在该消息之后，
  不允许 `on_half_close` 越过 `on_message`；
- 如果同一个 Call 已有 1 个等待任务时，
  又产生第二个无法接纳的续处理，
  有界等待容量已经耗尽，
  此时只终止这个 Call 并返回最终 `RESOURCE_EXHAUSTED`，
  TCP/Channel 继续可用；
- 该 Call 在终止前可能已经执行过更早的回调，
  因此这个 `RESOURCE_EXHAUSTED`
  **不是**“业务从未执行”的准入拒绝。

确定性 `test_rpc_stream_backpressure` 使用 1 个工作线程 + 16 个队列节点：
先让两个流式 Call 真正进入业务层，
再用独立填充 Call 填满执行器；
验证 A 的续处理被保留且 RX 额度暂不归还，
释放工作线程后 A 按顺序恢复并继续成功；
B 的第二个等待续处理超过每 Call 上限后得到 `RESOURCE_EXHAUSTED`，
但同一个 Channel/Connection 上的新恢复 Call 仍成功。

容量工具分别统计 `resource_exhausted` 与 `unavailable`，
两者都必须是 `rpc_errors` 的子集，不能互相冒充。

### 工作线程 / 处理器可扩展性归因矩阵

`run_rpc_scalability.py` 复用同一套固定速率到达记账，
但改变 Server 工作线程数量和受控处理器成本。
默认矩阵为：

```text
工作线程：      1, 2, 4, 8
处理器毫秒：    0, 1, 10
执行器：        64 个节点
窗口：          128
```

对于非零处理器延迟，
提供速率从仅处理器算式
`workers * 1000 / handler_ms`
的 0.5×/1×/2×/4× 生成。
对于 0 ms 处理器，
不存在有意义的仅处理器容量算式，
因此默认提供速率显式设为
5000/10000/20000/40000 RPS。

需要时矩阵使用多个负载源。
不高于单个发生器目标速率的点使用一个 Client；
更高速率在最多 32 个独立 `bench_rpc client` 进程之间拆分。
高负载发生器默认各使用一个 RPC 工作线程，
避免负载发生器在共享运行器 CPU 上为闲置 Client 工作线程池付出成本。
各发生器以窗口 1 完成预热时采用**顺序预热**，
避免发生器数量本身把预热阶段变成未测量的饱和负载。
所有 Client 到达开始门闩后，
运行器发送绝对 `CLOCK_MONOTONIC` 开始时间戳，并加入很小的相位偏移，
使各进程本地固定速率时间表交错，而不是偶然形成同一时刻突发。
每个进程保留自己的调度迟到/丢弃记账；
运行器汇总精确计数，并保留最差发生器迟到值用于负载保真度归因。

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

每个速率点都保留完整 Client 与 Server 诊断记录。
每个 `(workers, handler_ms)` 分组还会输出一个
`scalability_summary`，
其中包含：

- 最高**干净计划速率**；
- 第一个精确 Server 压力速率；
- 第一个负载发生器迟到/丢弃速率；
- Reactor 忙碌比例最大值；
- 执行器队列等待/处理器 P99 最大值。

高速率行还会自动拉长测量时间。
运行器把 `--requests` 当作下限，
并按需要增加请求数，
保证计划到达时间窗至少达到 `--min-arrival-ms`。
这样可以防止短时高 QPS 突发全部装入有界执行器队列，
却被误报为持续容量。

“干净计划速率”有意比“所有请求最终都成功”更严格：
除了提交/RPC/资源耗尽错误必须为零，
P99 计划到提交的迟到还必须保持在一个到达间隔以内。
如果 10 kRPS 时间表（100 微秒间隔）实际提交已经晚了几毫秒，
工具会标记 `load_generator_late`，
而不是声称 Server 持续承载了 10 kRPS。
这条边界衡量的是提供时间表的保真度，不是 Server 延迟 SLA。

工具有意**不会**把 Reactor 忙碌百分比或某个 RPC 延迟值
自动转换成架构结论。
Server 端瓶颈信号来自可观察事件：

- 执行器准入/物理满；
- Reactor 命令/完成队列满；
- RX/TX/RPC 消息/重组资源池耗尽。

负载发生器积压是独立信号，
由 P99 提交迟到超过一个计划到达间隔推导；
显式调度丢弃也单独报告。
这两类发生器信号都不能归因给 Server。

GCC `release` CI 任务使用正常运行时 CRC 分派记录一份完整矩阵，
作为诊断构建产物。
它没有吞吐、扩展效率、P99 或忙碌比例通过/失败阈值。

### 有界资源余量对照实验

当可扩展性测试点首次碰到某个有界资源池时，
`run_rpc_headroom.py` 通过在相同提供速率下
只改变一个受控资源，
区分“配置容量墙”和“更深层运行时瓶颈”。

默认实验沿用第一个多源结果，
按单变量逐步增加：

```text
工作线程：              8
处理器：                0 ms
速率：                  10k / 20k / 40k RPS

baseline:
  RX buffers:           基准推导值（capacity=128 时为 272）
  executor queue:       64

rx_headroom:
  RX buffers:           1024
  executor queue:       64

rx_executor_headroom:
  RX buffers:           1024
  executor queue:       256
  CONTROL TX items:     基准默认值（128）

rx_executor_control_headroom:
  RX buffers:           1024
  executor queue:       256
  CONTROL TX items:     2048

full_headroom:
  RX buffers:           4096
  executor queue:       256
  CONTROL TX items:     2048
  command queue:        基准默认值（1024）

rx_ceiling_headroom:
  RX buffers:           8192
  executor queue:       256
  CONTROL TX items:     2048
  command queue:        基准默认值（1024）

command_headroom:
  RX buffers:           8192
  executor queue:       256
  CONTROL TX items:     2048
  command queue:        4096

control_ceiling_headroom:
  RX buffers:           8192
  executor queue:       256
  CONTROL TX items:     8192
  command queue:        4096

executor_ceiling_headroom:
  RX buffers:           8192
  executor queue:       1024
  CONTROL TX items:     8192
  command queue:        4096

command_ceiling_headroom:
  RX buffers:           8192
  executor queue:       1024
  CONTROL TX items:     8192
  command queue:        16384

RPC message pool:       各阶段保持不变
reassembly pool:        各阶段保持不变
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
  --ceiling-command-capacity 16384 \
  --generator-window 256 --generator-capacity 256 \
  --max-generators 32 --target-rate-per-generator 1250 \
  --generator-workers 1 \
  --output /tmp/rpc-bench/resource-headroom.jsonl
```

对照运行器还把发生器进行中余量与 Server 容量分开。
Server 默认保持 `capacity=128`，
而高速率 Client 使用
`generator-window=256` 和 `generator-capacity=256`。
这样可以防止 Client 槽位窗口填满时，
在不改变 Server 每对端限制的前提下被误标记成 Server 资源墙。

基准二进制为 Server 单独提供以下覆盖参数：

`--rx-buffers`、`--rpc-message-pool`、`--reassembly-pool`、
`--control-tx-items`、`--command-capacity`。

覆盖值为零时保持现有基准推导默认值。

这些参数属于**仓库内部诊断调优**，
通过 `tr_facade_tuning` / `tr_server_create_with_tuning()` 接入。
它们有意不进入已安装 `tr_facade_limits` SDK 契约，
不改变公开门面默认值，
也不会作为应用配置传给基准 Client。

输出会记录实际观察到的资源池容量/执行器队列大小，
并输出逐阶段成对比较：

1. `baseline -> rx_headroom`：隔离 RX Buffer 压力；
2. `rx_headroom -> rx_executor_headroom`：隔离执行器物理满；
3. `rx_executor_headroom -> rx_executor_control_headroom`：隔离 CONTROL TX 项耗尽；
4. `rx_executor_control_headroom -> full_headroom`：CONTROL TX 已有余量后，把 RX 从 1024 增到 4096；
5. `full_headroom -> rx_ceiling_headroom`：命令容量保持默认，把 RX 从 4096 增到 8192；
6. `rx_ceiling_headroom -> command_headroom`：只把 Reactor 命令容量从 1024 增到 4096；
7. `command_headroom -> control_ceiling_headroom`：只把 CONTROL TX 项容量从 2048 增到 8192；
8. `control_ceiling_headroom -> executor_ceiling_headroom`：只把每 Endpoint 执行器队列容量从 256 增到 1024；
9. `executor_ceiling_headroom -> command_ceiling_headroom`：只把 Reactor 命令队列容量从 4096 增到 16384。

最后这些阶段有意按该顺序执行，
避免 CONTROL TX 和执行器压力掩盖命令队列墙。
4096 -> 16384 是改变现有每轮命令公平性预算之前最后一个纯容量对照实验：

- 如果队列满信号消失，说明之前 4096 的墙属于有界突发余量；
- 如果队列满仍持续出现，同时 Reactor 忙碌比例保持较低、
  命令预算命中反复出现，
  下一步应隔离命令排空/公平性，
  而不是直接跳到多 Reactor。

全部分阶段比较都会保留资源变化前后的第一个精确 Server 压力速率和瓶颈信号集合。

这是一项隔离实验，不是提高生产资源池默认值的论据。
如果 RX 耗尽消失后另一个有界资源成为第一个墙，
就继续调查新的信号。
只有在有界资源已经具有余量后，
Reactor 忙碌时间才成为限制证据，
才具备修改 Reactor 架构的测量基础。

### 共享运行器上的瓶颈可重复性

GitHub 共享运行器上的单次高负载结果只是诊断样本，
不是稳定机器容量测量。
如果两次运行对第一个有界瓶颈得出不同结论，
`run_rpc_repeatability.py` 会在考虑任何进一步架构或容量修改之前，
重复完全相同的 40k 时间表。

GCC `release` CI 任务针对以下配置各运行三次：

- `baseline`：基准推导的有界资源；
- `command_headroom`：RX=8192、执行器=256、CONTROL TX=2048、命令容量=4096；
- `ceiling_headroom`：RX=8192、执行器=1024、CONTROL TX=8192、命令容量=4096；
- `command_ceiling_headroom`：与 `ceiling_headroom` 相同，但命令容量=16384。

每次试验保留完整 Server/Client 诊断。
汇总会报告：

- 干净试验数量；
- 出现精确 Server 压力的试验数量；
- 各信号出现次数；
- 成功 RPC/s 的最小/中位/最大值；
- Reactor 忙碌比例范围；
- 命令预算命中范围；
- 命令队列峰值。

还会输出机器可读分类：

- `no_server_wall`：重复试验都没有精确 Server 压力信号；
- `non_reproducible_server_wall`：出现过 Server 压力，但没有任何一个精确 Server 信号在全部试验中都存在；
- `reproducible_server_wall`：至少一个精确 Server 信号在每次试验中都存在。

汇总还分别暴露
`reproducible_server_signals` 和 `sporadic_server_signals`。
Client/负载发生器信号仍保留在通用 `signal_counts` 中，
但它们自身不能把某个瓶颈分类为 Server 资源墙。
仍然没有吞吐或延迟 CI 门槛。

如果一个瓶颈只在三次共享运行器试验中的一次出现，
就把它视为不可重复证据，
不能据此修改生产架构。
只有反复复现的精确瓶颈才进入针对性对照实验。

#### PR #44 可重复性结果

PR #44 的 GCC `release` 可重复性构建产物
对全部四个配置在 40k 计划 RPC/s 下各运行三次。
全部试验都报告：

- 精确 Server 压力为 0；
- 命令预算命中为 0；
- 命令队列满事件为 0。

即使在命令容量为 4096 的配置中，
命令队列峰值也只有 35--44；
成功速率约 39.26k--39.58k RPC/s；
Reactor 忙碌比例约 0.267--0.289。
独立的分阶段余量实验在 40k 点也没有出现精确 Server 墙。

因此更早那次单一共享运行器样本中
“4096 命令队列填满”的结果被分类为不可重复证据。
不能据此提高生产命令容量或 `TR_COMMAND_BATCH`。
后者仍然是 Reactor 公平性策略，
只有稳定、可重复的瓶颈出现后才应调整。

### 负载发生器进程数量敏感性

固定速率可扩展性工具可以把同一总提供速率
分配到多个独立 Client 进程。
更多发生器会降低每个 Client 的本地调度速率，
但每增加一个进程也会在和 Server 相同的共享运行器上消耗调度时间、内存和 CPU。

发生器进程数量实验还必须保持**Client 总进行中容量**不变。
否则改变进程数量的同时也改变总可用 Client 槽位，
会一次改变两个变量。

因此 `run_rpc_fanout.py` 默认把总发生器窗口/容量固定为 1024 槽位，
再分配给不同进程数量：

```text
总提供速率：            40000 RPC/s
发生器数量：            4 / 8 / 16 / 32 个 Client
每发生器速率：          10000 / 5000 / 2500 / 1250 RPC/s
发生器总槽位：          所有场景均为 1024
每发生器槽位：          256 / 128 / 64 / 32
发生器工作线程：        1
RX buffers:             8192
executor queue:         1024
CONTROL TX items:       8192
command queue:          16384
```

发生器窗口和容量都使用同一个每发生器槽位数。
总槽位数必须能被所有请求的发生器数量整除，
并且每个 Client 必须保持在基准工具 1..256 槽位限制内。

两次试验使用相反的发生器数量顺序。
这样可以降低运行器预热、节流或瞬时宿主竞争造成的固定顺序偏差。

```sh
python3 bench/run_rpc_fanout.py \
  --binary build/linux/x86_64/release/bench_rpc \
  --trials 2 --rate 40000 --fanouts 4,8,16,32 \
  --aggregate-generator-slots 1024 \
  --output /tmp/rpc-bench/generator-fanout-40k.jsonl
```

每种发生器数量的汇总都会把发生器压力与 Server 压力分开，
并报告：

- 每发生器和总槽位控制值；
- 干净、发生器丢弃、发生器迟到、精确 Server 压力试验数；
- 接受/提供比例和成功 RPC/s 范围；
- 调度器 P99 迟到和丢弃到达数量；
- Client 汇总 CPU 时间和峰值 RSS；
- Reactor 忙碌比例；
- 命令队列和 CONTROL TX 峰值占用。

#### PR #48 初始扫描及混杂变量

PR #48 的第一轮发生器数量扫描保持**每个 Client**
窗口/容量都是 256。
因此总 Client 槽位会随发生器数量增长：

- 4 个发生器：1024 槽位；
- 8 个发生器：2048 槽位；
- 16 个发生器：4096 槽位；
- 32 个发生器：8192 槽位。

全部 8 个场景的精确 Server 压力都为零，
而接受/提供比例中位数从 4 个发生器时约 0.60
上升到 32 个发生器时约 0.91。
这个趋势不能归因于进程数量，
因为 Client 总并发容量同时增加了 8 倍。
该结果保留为“负载发生器侧容量会影响结果”的证据，
不能作为“32 个进程本质上更好”的证据。

#### PR #49 固定总槽位的发生器数量结果

PR #49 把发生器总容量固定为 1024 槽位，
只改变 4/8/16/32 个进程。
全部 8 个场景同样没有精确 Server 压力。
发生器侧出现两类不同失败模式：

- 4 个发生器（每个 256 槽位）接受全部计划到达，
  但调度器 P99 迟到中位数约 393 ms。
  10k-RPS 本地调度器无法保持预期到达时刻，
  实际上把负载时间线拉长。
- 8 个发生器（每个 128 槽位）
  把 P99 迟到中位数降到约 60 ms，
  但只接受约 49% 的到达。
- 16 个发生器（每个 64 槽位）
  把 P99 迟到中位数降到约 5.7 ms，
  同时接受约 41%。
- 32 个发生器（每个 32 槽位）
  把 P99 迟到中位数进一步降到约 2.6 ms，
  同时接受约 38%。

在这一组中，32 个发生器的调度保真度最好，
但总共 1024 个槽位不足以吸收积压，
而共享运行器上的 Server 只能排空约 13--15k 成功 RPC/s。
因此进程数量和总积压容量必须作为两个独立的负载发生器维度调优。

### 负载发生器槽位敏感性

`run_rpc_generator_slots.py` 固定 32 个发生器的时间表，
只改变发生器总进行中容量：

```text
总提供速率：            40000 RPC/s
发生器：                32
每发生器速率：          1250 RPC/s
发生器总槽位：          1024 / 2048 / 4096 / 8192
每发生器槽位：          32 / 64 / 128 / 256
发生器工作线程：        1
RX buffers:             8192
executor queue:         1024
CONTROL TX items:       8192
command queue:          16384
```

两次试验以相反顺序运行不同槽位容量。
总提供速率、进程数量和 Server 余量保持不变，
因此可以隔离“负载发生器侧需要多少积压容量才能不再丢弃到达”。

```sh
python3 bench/run_rpc_generator_slots.py \
  --binary build/linux/x86_64/release/bench_rpc \
  --trials 2 --rate 40000 --generators 32 \
  --aggregate-generator-slots 1024,2048,4096,8192 \
  --output /tmp/rpc-bench/generator-slots-40k.jsonl
```

该诊断不能证明应该提高任何生产 Client/Server 容量。
它只测量负载发生器保持请求开环时间表的能力。
如果现有 8192 槽位最大值仍会丢弃到达，
下一步应改进基准发生器自己的容量模型，
而不是把得到的曲线解释为 Server 上限。

只有在负载发生器调度压力和发生器进行中容量都受控后，
仍然稳定存在的 Server 瓶颈，
才能用于架构或生产默认值变更。

### 流式续处理预留

Server RPC Endpoint 引擎仍支持按需启用
`executor_continuation_reserve`。
在高层门面中，
该工作线程/节点/预留布局已经迁到仓库内部 `tr_facade_tuning`，
不再属于已安装 SDK。
默认预留仍为 0，
因此现有容量曲线和公开门面默认部署行为不变。

启用预留后仍然只有一套有界执行器节点资源池：

- 新一元调用和流式首个业务任务属于准入工作，
  只能使用 `executor_queue_capacity - reserve` 个节点；
- 已经接受的流式 Call 的后续消息、半关闭、可写、关闭任务
  可以继续使用全部节点；
- 准入在预留边界命中 `TR_AGAIN` 后，
  沿已有逻辑返回 `RESOURCE_EXHAUSTED`，
  不会消耗保留容量；
- 预留不改变工作线程数、就绪 Call 调度或同一 Call 的串行规则；
- 预留必须严格小于最终执行器队列容量；0 表示关闭。

该策略的目标是避免大量新 Call 占满全部任务节点，
导致已经接受的流式 Call 连续处理/生命周期工作都无法进入执行器。
它不是吞吐保证，也不是每服务 QoS；
默认基准暂时保持 `reserve=0`。
后续应增加独立流式固定速率曲线，
比较 `reserve=0` 与 `reserve>0`
下的准入拒绝、续处理进展和尾延迟，
再决定是否需要设计应用可见的语义准入策略；
不应直接把执行器节点预留数量重新暴露成稳定门面字段。

CI 对开环测试只检查计数守恒、载荷正确性、
固定到达时间窗和最终排空，
不要求某个 QPS/P99，
也不要求共享运行器必须在某个速率点出现饱和。
GCC `release` 额外保存一轮 5 点容量曲线作为诊断构建产物；
它仍然不是发布 SLA。

### 2026-09-27 执行器饱和诊断

在改成显式 `RESOURCE_EXHAUSTED` 之前，
PR #29 的 GitHub 共享运行器
（GCC `release`、2 个工作线程、10 ms 处理器、
执行器队列=16、128 个提供到达）
已经确认瓶颈位置：

- 50/100/200 RPS 全部成功；
- 400 RPS 为 80 成功 + 48 `UNAVAILABLE`；
- 800 RPS 为 48 成功 + 80 `UNAVAILABLE`。

400/800 RPS 的成功吞吐都约 196.6 RPC/s，
而负载发生器 `scheduler_dropped=0`，
P99 调度迟到约 80/105 微秒。
这些数字只用于证明执行器饱和是真实瓶颈并校验新的状态语义，
不作为跨机器性能结论。
新实现应在同类过载点把
“处理器未执行的容量拒绝”报告为 `RESOURCE_EXHAUSTED`，
而不是伪装成连接不可用。

## 校验与 CI

新工作流使用 GCC `debug/release`、Clang `release`、
ASan/UBSan、TSan，
分别运行自动/纯软件 CRC 两种构建。
每次执行 7 个真实场景；
`pressure` 附带 `recovery`。
校验计数守恒、每类请求覆盖、并发测量重叠、
正常/恢复全部成功和实际出现压力失败；
不检查毫秒级延迟或 MiB/s 是否达到阈值。
超时只用于发现挂死。

统计校验器的 10 组测试会拒绝：

- 漏计完成事件；
- 隐藏提交拒绝；
- 类别合计错误；
- 畸形/NaN 数字；
- 错误时间区间；
- 数据损坏；
- 虚假混合负载；
- 没有失败的压力场景。

C 程序在回调可能提前到达时也不会持锁调用同步提交；
样本/载荷会保持存活直到回调完成；
异常退出清理先销毁/等待 Client，再释放借用数据。

CI 上传完整日志、输出和精确 Git 源码归档，
保留原始失败退出码。
源码快照只用于重现当前代码，
不包含凭据、环境变量转储或 `.git`。
失败测试没有自动重试。

## 2026-09-26 本地测量

生产基线 `6d6f42535ed5d2ba12692852b5225bb6ff2d9df6`。
从既有 CI 归档和已合并补丁还原后，
整个 `src/` 树为
`0b70881848ed4a8ae04ca1b3ffb17d21d660de64`，
整个 `include/` 树为
`d4f20305bcca4b95a9d2b3249c9e97b8ac0792a7`，
与 GitHub 基线完全相同。
没有把旧 Reactor 或离线控制调度候选版本作为当前主线测量。

GCC 14.2、`-O2 -g -pthread`，无检查器/LTO；
共享 KVM 环境，CPU 报告 AMD EPYC 9V74，
CPU 时间配额为 4 核等价，没有绑核。
每个场景预热 100 次，
每个普通 Client 测量 256 次，
`pressure` 测量 64 次；
三轮交替运行自动/纯软件后端。
每个场景新建 Server，
普通消息使用相同参数，
`pressure/recovery` 在同一个连接中。
本地仓库是重建快照，
原始元数据中的本地 Git 标识不是上述 GitHub 基线标识；
基线识别依赖已经核对的源码树与二进制哈希，
而不是伪造本地 HEAD。

下表为三轮**单客户端**结果中位数；
原始逐次运行汇总见
`bench/results/rpc-20260926.csv`。
它不是每请求延迟跟踪，更不是独占硬件 SLA。
小样本、调度噪声和 TCP 延迟行为使结果波动明显，
不使用三轮数据给出显著性结论。

| 负载 | 自动 CRC | 纯软件 CRC |
|---|---:|---:|
| `small`，窗口 1，成功 RPC/s | 9364.78 | 7533.52 |
| `small`，窗口 1，成功 P99，微秒 | 269.87 | 277.16 |
| `bulk`，窗口 8，双向应用 MiB/s | 286.51 | 205.44 |
| `mixed`，窗口 8，成功 RPC/s | 207.77 | 175.96 |
| `mixed` 中 `small` 成功 P99，微秒 | 87975.35 | 88256.18 |

自动/软件版本在普通场景都完成全部请求。
三轮 `pressure` 各 64 个请求全部以
`DEADLINE_EXCEEDED` 结束；
后续 64 个恢复请求全部成功。
结果只证明这些有限场景，
不能证明不存在其他取消/恢复竞争。
内存/线程检查器数据只验证正确性，
不混入性能表。

### 历史发现与生产落地：TCP_NODELAY

PR #26 测量时的主线 `src/socket.c`
没有设置 `TCP_NODELAY`。
当时只在未提交的临时对照构建中，
给创建及 `accept` 成功的 TCP 套接字设置并检查 `TCP_NODELAY`，
其余代码/参数相同。

三轮 `mixed` 单客户端 `small` P99 中位数
从约 88 ms 变为约 0.616 ms；
成功速率中位数从约 208 变为约 14092 RPC/s。
两客户端实验也观察到明显变化，
详见 CSV 中 `nodelay-experiment`。
**该实验不是提交版本的默认行为或性能。**

这个对照结果支持优先审查 Nagle/ACK 与小包发送的交互，
而不是继续优化 CRC 或直接增加 Reactor。
没有抓包跟踪，仍不能声称已经证明每一次延迟的具体来源。
RFC 9293 附录 A.3 讨论了 Nagle + 延迟 ACK 对请求/响应的影响；
Linux `TCP_NODELAY` 的含义见 TCP 手册。

后续生产实现把策略收敛到高层门面边界：
Client 首次连接、Server 已接收对端、
以及 Client 自动重连，
都会在 Reactor 接管 fd **之前**应用同一策略；
低层 `tr_tcp_*` 辅助接口不会被全局强制修改。
策略可以显式禁用。
测试通过链接期 `setsockopt` 观察器验证
默认双向设置、显式禁用和重连继承，
不以回环延迟数字作为 CI 通过门槛。
上面的 2026-09-26 性能表仍是修改前历史基线，
不能当作当前默认套接字策略的测量结果。

参考：

- https://www.rfc-editor.org/rfc/rfc9293.html#appendix-A.3
- https://man7.org/linux/man-pages/man7/tcp.7.html
