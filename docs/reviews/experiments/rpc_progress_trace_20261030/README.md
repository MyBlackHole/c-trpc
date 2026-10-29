# RPC 进度追踪复现

probes.json 保存五组相对 3ca47d15c9ac571882d2b61a522e73fb315bfb72 的完整临时
补丁、逐文件原/插桩 SHA-256 与冻结 ELF 摘要。patch 字段保留实际 diff 字节。
生产运行时等同 0d81342 / dev.182；不要把插桩 ELF 当成生产基准。
实验结论及限制见[报告](../../rpc_v2_rpc_progress_trace_20261030.md)。

在独立工作副本检出基线，通过项目 Xmake 配置 GCC Release、crc32c_portable=n。
逐一核对 files 内的原摘要，git apply --check 后应用该项 patch，确认插桩摘要，
再 xmake build bench_rpc。复制冻结 ELF，记录本机真实摘要并恢复修改前原文件。
五个补丁都相对同一干净基线，不能叠加。结束后经 Xmake 重建并核对原 ELF。

实际插桩器、执行器、分析器及全部输出在[原始 ZIP](../../data/rpc_progress_trace_20261030_raw.zip)。
INDEX.json 对 596 个原始成员逐项登记摘要和字节数。helpers/ 中脚本引用此次
build/performance-20261029/ 下专用目录，并拒绝覆盖既有逐场输出；复现时使用新
副本/新空目录，修改路径后记录新脚本摘要。原 repeatability 入口保持 --trials 1
--rate 40000 --min-arrival-ms 250，仅预先选择明文三 profile；没有过滤失败样本。

将 TMPDIR、TMP、TEMP、XMAKE_TMPDIR 都设置为本工作区 build/test-tmp，预先确认
目录存在，并设置 PYTHONDONTWRITEBYTECODE=1。所有构建、测试、日志、profile 和
临时文件均须留在工作区，禁止在 /tmp 写入。不要在负载期间并行构建另一版本。

进度 CPU 探针在 Endpoint pump 入口/出口取线程 CPU 和单调时间；CPU 为边界内
包含调用的计数，不是独占采样。任务直方图的前 32 项为 1us、2us、4us 等上界的
计数，第 32/33 下标分别为纳秒总和/最大值；只在工作返回并交付 done 后更新。
实验无授权 hook，未验证同一探针在异步授权任务复用等其他场景的统计语义。

业务关联探针使用正文前八字节的高 32 位 PID/低 32 位计划序号，不改正文长度、
方法、帧布局或原回包校验。Client stderr 的 rpc_admission_probe 包含原始样本；
message_callback_ns 在回调取得应用锁后记录，不能当作内核收包时刻。丢弃项的
terminal_ns 沿用原样本 end_ns，不是 RPC 终局；仅已接受调用解释该列。
Server 工作时间在 unary_server_stage_probe 内；PID 为零的记录来自原预热。

客户端库关联以 client_credit_stage_probe 记录 owner 准备、传输 OPEN 接受、
初始信用及 Record 接受/接收；阶段编码见 client_credit 项的 stages。帧探针新增
stream_map 关联真实 wire stream_id，以单进程唯一控制 TCP 端口对关联另一端。
reactor_frame_path_probe 的数组列顺序在 frame_columns，RX 两个时间相同，TX
为完成当前帧那次发送调用的开始与返回。对端可在发送方返回前收到完整帧，
所以跨端区间使用发送开始，保留两个边界，不强制发送返回先于对端 RX。

analyze_rpc_unary_stages.py、analyze_rpc_client_credit.py、analyze_rpc_frame_path.py
与 analyze_rpc_rx_frame_budget.py 从原始记录关联每个成功调用，检查单调顺序与
阶段总和。仅对已关联 OK 样本计算 nearest-rank；错误/缺少消息另计，不能删除
其原始 RPC 结果。原帧分析器和 summary.first.json 保留，索引化复算字节一致。
仓库内 analyze_rpc_tx_return_gap.py 直接读取此 ZIP，不解压临时日志；它核对关联
成功数，并将发送调用区间、TX 记录至对端 RX 的有符号间隔写入
../../data/rpc_progress_trace_20261030_send_return_gap.json。
64 帧候选仅更改原 turn 的完整帧处理上限，前后 RX 调度点共享，原字节/dispatch
预算不变；候选未采用，不能沿用它作为下一版生产基线。

延伸的 v3/v4 ingress 插桩和复算见[原始归档](../../data/rpc_ingress_poll_hold_probe_20261030_raw.zip)。
两个分析器直接读取归档：analyze_rpc_rx_poll.py 关联 epoll_wait 返回、目标连接事件、
RX schedule 与 owner recv；analyze_rpc_rx_hold.py 只计算每帧附带的最近一个已完成
`rx_held` 区间与客户端发送至服务端 recv 的重叠，不能外推为窗口内全部 hold。复算结果
分别保留在[epoll 结果](../../data/rpc_ingress_poll_probe_20261030_summary.json)与
[hold 结果](../../data/rpc_ingress_hold_probe_20261030_summary.json)。v3 从 v4 诊断源中移除 hold/pause
字段后重建并编译，ELF SHA-256 与当时 v3 metadata 完全一致；两个版本仍为单次 dirty
诊断，没有改变 40k 验收判定。

Callgrind 使用原无探针 ELF，单 Client/64 窗口/256 请求加 32 预热/30000ms 期限，
不属于原 40k 矩阵。仅 Server 使用 --collect-atstart=no、
--toggle-collect=tr_reactor_thread_main、--separate-threads=yes；只解释 owner Ir。
显式 --vgdb=no、工作区 --vgdb-prefix/--log-file/--callgrind-out-file 与
--enable-debuginfod=no，TMPDIR 等同样指向工作区。原始 profile、flat/tree 注释和
精确命令均保留。第一次驱动校验参数错误的目录保留；只使用新 verified 目录的
两端退出零与 288 项排空结论。

v5 将 RX hold 从“每连接最近一次完整区间”扩展到全部完整区间，并在成功解除 hold 后
调用 FIONREAD 只读采样；连接关闭时保留未闭合区间标志及解析器进度。每反应器最多 131072
条，超额计数后继续；没有溢出。额外的内存、时间戳和 ioctl 使该 ELF 仅用于诊断。

[v5 原始归档](../../data/rpc_ingress_rx_hold_v5_20261030_raw.zip)含完整补丁、三档所有
server/client 原始日志、repeatability JSONL、执行元数据、分析器、汇总和 INDEX.json；
[汇总](../../data/rpc_ingress_rx_hold_v5_20261030_summary.json)可直接查看结果。
分析器以服务端正文 PID/序号和客户端 stream_map 将每条成功业务 MESSAGE 关联到客户端
request MESSAGE 帧，之后以反向端口、stream_id、message_id 2 配对完整 hold 时间线。它也
会分别统计窗口内 hold 重叠、FIONREAD 正值解除、发送 syscall 返回早于解除的样本，以及
全体请求的 hold 覆盖比例；同一解除区间可能被多个相邻请求窗口覆盖。FIONREAD 不是按
stream 计数，不能把连接内的待收字节标成某条具体 RPC 的字节。

复算时先将归档解压到工作区 build 子目录；将 TMPDIR、TMP、TEMP、XMAKE_TMPDIR 指向项目
`build/test-tmp`，设置 PYTHONDONTWRITEBYTECODE=1，再运行：

```sh
python3 docs/reviews/experiments/rpc_progress_trace_20261030/analyze_rpc_rx_hold_intervals.py \
  --logs-dir build/rpc-ingress-hold-v5-replay/logs \
  --output build/rpc-ingress-hold-v5-replay/analysis.json
```

本次基线为 `006af2aec502d59a4fbcdf7c0f9e212795548618` / dev.182。v5 补丁在隔离树通过
`git apply --check`，重放所得 diff 与归档补丁逐字节相同。探针编译和单轮明文 40k 三档
运行完成，但每档仍有发生器迟到/丢弃和 `control_tx_pool_exhausted`，不得把 dirty 样本
转换成性能通过。运行后受跟踪源文件恢复为基线；Xmake GCC Release 重建产物 SHA 为
`57f1e6ca…2466ec`，随后恢复的冻结 ELF SHA 为 `55d1c4b9…bd137e`，与源码重建产物不同，
不声称二者逐字节等价。


## v6：pending hold 原因与解除来源

v5 证明部分 RX hold 期间 socket 仍有待读数据，但没有记录是哪条 pending 路径造成 hold、
以及由哪个进度事件解除。v6 临时插桩记录 pending OPEN/REFUSED、进度调用数、发送尝试数、
解除触发源及连接代次；只在成功解除 RX hold 后记录终点，连接退役则单独结束。事件表有界，
带原子发布标记；没有溢出、重叠或未发布事件。本探针不采样 FIONREAD，也不改运行行为。

独立复跑时使用干净的 dev.182 工作副本，并选择新的输出目录：

```sh
mkdir -p build/test-tmp
export TMPDIR="$PWD/build/test-tmp"
export TMP="$TMPDIR"
export TEMP="$TMPDIR"
export XMAKE_TMPDIR="$TMPDIR"
export PYTHONDONTWRITEBYTECODE=1
xmake f -c -y -m release --toolchain=gcc --crc32c_portable=n
xmake build bench_rpc
python3 bench/run_rpc_repeatability.py \
  --binary build/linux/x86_64/release/bench_rpc \
  --output build/rpc-pending-cause-probe-20261030/repeatability.jsonl \
  --label rx-pending-cause-20261030 --trials 1 --rate 40000 \
  --min-arrival-ms 250 --max-generators 16 \
  --target-rate-per-generator 2500 --generator-window 64 \
  --generator-capacity 64 --compression none
```

运行前须将 [原始归档](../../data/rpc_ingress_pending_cause_20261030_raw.zip)解到工作区内，
并只在本地独立副本重放 `diagnostic.patch`：

```sh
mkdir -p build/rpc-pending-cause-replay
unzip docs/reviews/data/rpc_ingress_pending_cause_20261030_raw.zip \
  -d build/rpc-pending-cause-replay
# 先确认源码与基线匹配，再重放该归档中的临时诊断补丁。
git apply --check build/rpc-pending-cause-replay/diagnostic.patch
git apply build/rpc-pending-cause-replay/diagnostic.patch
```

归档含完整 benchmark 日志、精确 patch、执行身份、复算器及 `INDEX.json`。本轮摘要见
[summary](../../data/rpc_ingress_pending_cause_20261030_summary.json)。三个 profile 均 dirty；
这些观测用于定位 hold 路径，不用于 40k 性能结论。运行后须恢复仅由本探针修改的三个
跟踪源码，并用 Xmake 重建基线 bench；不要删除其他未跟踪文件。
