# 可丢弃的编码候选实验存档

本目录仅保存 2026-10-29 的研究证据，不是 CTM1、SBE、Protobuf 实现或可发布 SDK。
源代码不接入主工程 trcore、安装范围或原测试目标，不能直接用于产品。
结论及限制见[开源对照记录](../../custom_message_open_source_lessons_20261029.md)。

## 实验范围

两个手写 C11 候选对照：紧凑字段与固定块。消息只含 u64 offset、bytes data 和
一个 v2 可选 ASCII label；业务样例需要前两个字段，不代表通用 wire-required。
两者加相同四字节实验外层长度，正文统计不含它；没有 RPC/TLS/压缩。

覆盖 v1/v2 双向读取、六个长度（0、1、127、128、4096、65536）、完整帧逐字节截断、
伪造外层长度后的内部边界、非最短/溢出标签整数、零/不足结构工作额度、失败输出
不变及改写 offset 后保留未知尾部 label。完整去掉 optional 字段是合法旧消息，
不能把它错误当作“所有截断都应拒绝”的证据。

未知额度均按原始编码字节计，已知 label 最多 128 字节，未知区最多 256 字节。
结构工作额度为 64；它不是泛用的总 CPU 预算。仅验证 label 的 ASCII 子集。
不覆盖数组、嵌套、完整 UTF-8、任意已知字段修改或任意位置的未知字段往返。

## 在工作区 build/ 中复现

要求 Linux/x86_64、CPU 0 可用、GCC、Clang、Python 和 Xmake；原测量为 Ryzen 7 PRO
4750U、GCC 16.2.1、Clang 23.1.4、Xmake 3.1.2。运行器最终恢复探针的 GCC Release。
不更改主工程配置；`-P .` 必须保留，避免 Xmake 向上发现主工程。

从仓库根运行，使用一个新的、尚未存在的输出目录：

```sh
mkdir -p build/test-tmp build/codec-spike-reproduce
cp docs/reviews/experiments/ctm_encoding_spike_20261029/* build/codec-spike-reproduce/
TMPDIR="$PWD/build/test-tmp" TMP="$PWD/build/test-tmp" TEMP="$PWD/build/test-tmp" \
XMAKE_TMPDIR="$PWD/build/test-tmp" PYTHONDONTWRITEBYTECODE=1 \
python3 -u build/codec-spike-reproduce/run_probe.py
```

内部实际入口为该独立目录中的 `xmake f -P .`、`xmake test -P . -v -j1` 和
`xmake run -P . codec_spike`；编译参数及链接器由 Xmake 日志记录。源码为 C11，
本机原工具链链接步骤使用系统 g++，没有 C++ 源码；这不是 SDK C 链接边界测试。
ASan/UBSan 开启泄漏和失败退出，临时目录只在根工作区 build/test-tmp。

## 数据与解释

运行器在 CPU 0 上九轮交错两候选，前两轮预先指定为预热；其余七轮全部保留。
每轮记录 encode、verify、consume_all、copy_edit 四个批次。CPU 是整批进程 CPU
除以次数，不是单次操作分位数或 RPC P99；墙钟另计，不用 CPU 时间掩盖调度。
源/目标均是热缓冲，共享开发主机未隔离其他活动。

结果见[完整 CSV](../../data/ctm_encoding_spike_20261029_results.csv)、
[汇总](../../data/ctm_encoding_spike_20261029_summary.json)、
[环境及 SHA-256](../../data/ctm_encoding_spike_20261029_metadata.json)。
metadata 的 source_commit 是当时仓库上下文 f670b77；探针原在忽略目录，其真实
输入由 source_sha256 绑定，本目录五个源文件与该记录逐字节相同。换输出路径会
改变调试路径和二进制摘要，不能要求异机二进制/纳秒结果相同。

初轮仅校验外层截断，随后补齐伪造长度内部检查；又统一了未知字节的统计口径。
这些版本的日志和原始数据保留在本机 build/codec-spike-20261029/first-run、
second-run、third-run，最终存档采用补齐后的源码，没有挑选最快一轮。
归档格式核对后，最后又统一一处源码换行并完整重编译/测量，前一次数据保存在
fourth-run。跨次纳秒值有变化，未锁频/隔离主机活动，也未进一步归因；不把排版
重编译后的差值解释为编码性能改善。报告只采用最终输入绑定的完整九轮。
曾误触主工程测试，已按独有日志句柄停止；不将该次中止计为任何通过。

这些结果只用于提出下一步候选问题；不能作为发布容量、完整 schema 兼容性、
生产错误隔离或第三方实现的优劣结论。当前 wire 仍待更完整的候选对照与审阅。
