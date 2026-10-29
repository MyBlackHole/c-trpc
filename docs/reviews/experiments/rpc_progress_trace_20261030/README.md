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
64 帧候选仅更改原 turn 的完整帧处理上限，前后 RX 调度点共享，原字节/dispatch
预算不变；候选未采用，不能沿用它作为下一版生产基线。

Callgrind 使用原无探针 ELF，单 Client/64 窗口/256 请求加 32 预热/30000ms 期限，
不属于原 40k 矩阵。仅 Server 使用 --collect-atstart=no、
--toggle-collect=tr_reactor_thread_main、--separate-threads=yes；只解释 owner Ir。
显式 --vgdb=no、工作区 --vgdb-prefix/--log-file/--callgrind-out-file 与
--enable-debuginfod=no，TMPDIR 等同样指向工作区。原始 profile、flat/tree 注释和
精确命令均保留。第一次驱动校验参数错误的目录保留；只使用新 verified 目录的
两端退出零与 288 项排空结论。
