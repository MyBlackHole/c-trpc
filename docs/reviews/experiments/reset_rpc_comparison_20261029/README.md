# RESET 改动前后的 RPC 对照执行器

run_ab.py 是本次诊断实际执行脚本的原样副本，不参与产品构建。入口使用原
bench/run_rpc_bench.py 和 bench/run_rpc_repeatability.py，保留原配置及全部样本。

复现时使用两个独立工作副本：旧版检出 1f33cdd，候选检出 33f6300（生产代码
与 0d81342 相同）。分别按项目 Xmake 配置 GCC Release、CRC portable=n 并构建
bench_rpc，将 ELF 复制到主副本 build/performance-20261029/reset-rpc-ab-v182/bin/
的 baseline/ 与 candidate/。先验证两边 55 单元真实编译配置与命令等价。

在主副本的上述实验目录创建 manifest.json，可从原始 ZIP 读取字段结构；将
binaries 中 binary/checkout/runtime_source/metadata_source 改为本次真实路径与
版本，重算 ELF 摘要并记录当前亲和性。不要直接沿用归档机器路径或假定二进制
摘要相同。TLS fixture 按既有[握手诊断说明](../tls_handshake_stress_20261029/README.md)
在工作区准备，脚本引用 build/performance-20261029/tls-zstd-hour-3600s/。

从主项目根目录执行 python3 加本文件 run_ab.py 的路径。脚本会拒绝覆盖已存在
的逐场输出；使用新副本/新空实验目录，不删除原始诊断结果。设置 TMPDIR、TMP、
TEMP、XMAKE_TMPDIR 到主副本 build/test-tmp、PYTHONDONTWRITEBYTECODE=1，禁止
在 /tmp 写入测试、编译、日志或缓存。不要同时运行基线与候选或其他本任务构建。

每种传输先做旧/新各三个闭环正确性预检，再交错执行三轮固定 40k 计划负载，
一轮含三个原 profile。metadata、binary、实际命令和 Server 的 TLS/压缩字段
都核对；clean 判定原样保留。不干净样本仍保存为有效诊断，不转换为容量成功。
当前源码本身不接受目录/速率覆盖参数；更换实验目录时须同步目录引用并保留
新脚本摘要，不能把修改后运行冒充本次执行。完整范围见[对照记录](../../rpc_v2_reset_rpc_comparison_20261029.md)。
