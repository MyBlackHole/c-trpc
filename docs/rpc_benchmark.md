# TCP / RPC 混合负载与 deadline 压力基准

## 本轮边界

这是 opt-in 的诊断工具，不改变 `src/`、公开头文件、socket 选项、RPC/wire 协议
或运行时线程模型。默认构建不生成 `bench_rpc`。原有 CRC、调度、生命周期和
Transport/RPC 单元测试继续保留；本工具不替代它们。

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
CPU/内存/FD 耗尽、随机网络故障、队列容量拐点或长稳测试。没有证据时不把它
称为“已证明所有过载都能恢复”。后续应独立增加 open-loop 负载和故障矩阵。

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

### 新发现：TCP 小包发送策略应优先做独立验证

主线 `src/socket.c` 没有设置 TCP_NODELAY。仅在未提交的临时对照构建中，给
创建及 accept 成功的 TCP socket 设置并检查 TCP_NODELAY，其余代码/参数相同。
三轮 mixed 单客户端 small P99 中位数从约 88 ms 变为约 0.616 ms；成功速率中位数
从约 208 变为约 14092 RPC/s。两客户端实验也观察到明显变化，详见 CSV 中
`nodelay-experiment`，**该实验不是提交版本的默认行为或性能**。

这个 A/B 结果支持优先审查 Nagle/ACK 与小包发送的交互，而不是继续优化 CRC 或
直接增加 Reactor。没有 packet trace，不能声称已经证明每一次延迟的具体来源。
RFC 9293 Appendix A.3 讨论了 Nagle + delayed ACK 对 request/response 的影响；
Linux TCP_NODELAY 的含义见 TCP 手册。正式修改仍需独立的 socket 类型适用范围、
错误路径、RPC/Transport 回归、消息大小和物理网络对比，不能直接照搬实验改动。

参考：
- https://www.rfc-editor.org/rfc/rfc9293.html#appendix-A.3
- https://man7.org/linux/man-pages/man7/tcp.7.html
