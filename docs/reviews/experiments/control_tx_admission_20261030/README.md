# CONTROL TX 与准入诊断复现

本目录的 probes.json 保存三个实际临时二进制的完整补丁、原/插桩文件 SHA-256、
基线提交和 ELF 摘要。patch 字段保留原 diff 字节，未为通过空白检查裁剪上下文。
实际插桩器、执行器、原始 stdout/stderr、构建日志、命令、元数据和配置在
[原始 ZIP](../../data/control_tx_admission_20261030_raw.zip)内，INDEX.json 逐项登记
字节数与 SHA-256。范围与负结果见[诊断报告](../../rpc_v2_control_tx_admission_20261030.md)。

在独立工作副本检出 a3077c32d449190eeb86b0cfb1b22918312da1f4，使用项目 Xmake
GCC Release / crc32c_portable=n 配置。为每个探针使用干净源文件：先核对 source_path
的 original_sha256，用 git apply --check 后应用该条 patch，再核对
instrumented_sha256，通过 xmake build bench_rpc 构建。将 ELF 复制冻结到工作区，
记录本机实际摘要；恢复本次修改前的原字节后，再准备下一个探针。
每个探针都是相对同一原基线的补丁，不能叠加使用。

原执行器为 ZIP 中 helpers/run_control_tx_probe.py、run_control_tx_interleave_probe.py
和 run_rpc_admission_probe.py。它们调用原 bench/run_rpc_repeatability.py，每种传输
--trials 1 --rate 40000 --min-arrival-ms 250，保留三个原 profile。目录写死为此次
build/performance-20261029/ 下三个专用目录，运行前拒绝覆盖原输出；复现时使用新
副本/新实验目录，同步修改路径并登记新脚本摘要。不要覆盖归档实验，也不要在加载
期间并行构建另一探针。TLS fixture 依照既有[握手说明](../tls_handshake_stress_20261029/README.md)
在工作区准备，不从归档中获取私钥。

将 TMPDIR、TMP、TEMP、XMAKE_TMPDIR 都设为本工作区 build/test-tmp，并设置
PYTHONDONTWRITEBYTECODE=1；先确认目录存在。所有构建、测试、日志和缓存均须留在
工作区，禁止在 /tmp 写入。结束后恢复原文件，经 Xmake 重建并核对无探针二进制；
本次恢复后的 ELF 与此前生产候选的摘要完全一致。

池探针在既有 mutex 下累计取得/失败/归还，每 Reactor 至多保存八个首次
帧类型/阶段快照。phase 顺序在 probes.json 中；它标记外层循环当前阶段，候选
在 RX 内插入的 TX 仍属 RX 外层阶段。只在销毁后输出，避免逐次热路径日志。
报告中的 204 个进程是两组各六场 × 每场 Server 加 16 Client。

准入探针在原 sample 中增加两个时间戳，不改变 stdout schema。stderr 每个 Client
输出一个 rpc_admission_probe，columns 给出原始数组列顺序；terminal_ns 对已接受
调用表示终局，丢弃项沿用原样本的 end_ns=唤醒时间，不能当作 RPC 终局。
API 可能先收到回调再返回，所以不要求 api_return_ns 早于 terminal_ns。
main_thread_cpu_ns 覆盖本函数前置分配、等待与排空，不是单 API 的 CPU 总和。

从成功接受的 submit_ns/terminal_ns 建立有序事件集合，在每个丢弃项的
lock_acquired_ns 重建在途量，60000 条到达中 49110 次丢弃均为 64；准备和 API
耗时分别取 submit_ns-lock_acquired_ns、api_return_ns-submit_ns。
合并同场 16 个发生器后用 nearest-rank 计算分位数；保留原 runner 的判定与结果。
这些临时单次诊断不构成性能收益、无插桩容量或资源硬上限验收。
