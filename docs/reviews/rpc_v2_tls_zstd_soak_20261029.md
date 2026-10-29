# TLS 与双向 Zstd 持续运行记录

日期：2026-10-29。当前状态：**固定输入的一小时诊断通过**，源码为 dev.177。
关联 #234、#235、#241、#247；不改变生产代码或既有容量判定。

## 固定输入与范围

源码提交 `97e106c6b43c4c0b8134dde2c328436009fb30ea`，版本 `2.0.0-dev.177`；
Xmake GCC Release 基准二进制 SHA-256：
`12292f983347e4081fc1512cd000712bafecede39e4ba0a49edb612b44c3111d`。
本机 Linux 7.2.11 / Ryzen 7 PRO 4750U / OpenSSL 3.6.5，亲和性为逻辑 CPU 0–15。

复用原 `bench_rpc` 的公开 V2 Client/Server，在同一对进程、同一连接上运行：

- 250 RPS × 3600 秒，共 900000 个开环到达；每条正文 4096 字节。
- Server capacity 128、Client window 64、消息上限 65536、业务延时 0、调用期限 3000ms。
- TLS 1.3 验证独立测试 CA 与 `server.example.test` SAN；此基准只启用服务端认证，
  没有启用 mTLS。请求/响应均为 Zstd，计数与正文由原基准检查。
- 每 10 秒采样拥有的 Server/Client 的 `/proc` RSS、PSS、匿名/私有页、fd 和线程数。
  结束后按原 `validate_open_phase`、`validate_server_exit`、`clean_result` 和
  `exact_wall_signals` 核对；不放宽发生器迟到、丢弃或资源压力判定。

现有容量运行器的 180 秒超时不适合小时诊断，故使用工作区忽略目录内的一次性
进程驱动，复用原校验函数；没有修改既有运行器的参数/字段/超时。它只停止自己
创建的子进程，证书、密钥、探针、日志全部位于 `build/performance-20261029/`。

## 预跑与采样口径

20 秒预跑完成 5000/5000 成功调用，原质量判定 `clean=true`、wall signals 为空。
原始结果见 `tls-zstd-smoke-20s/result.json`；小时目录为 `tls-zstd-hour-3600s/`。

基准在测量前为全部请求预分配 `struct sample[]`，页会随采样逐渐驻留。
用已构建 ELF 的调试类型核对，当前 `sizeof(struct sample)=56`，900000 项为
50400000 字节；结束时排序临时数组另计。因此 Client RSS 不能直接解释为库泄漏。
Server RSS 也包括 OpenSSL、Zstd、libc 分配器与线程栈，不能等同请求字节预算。
fd/线程数和完整序列保存在 `resources.jsonl`，不能仅凭端点截图宣称内存硬上限。

启动记录器曾继承基础 metadata 的无 TLS/无压缩默认标签；两端命令、Server ready
回执与 `/proc` 实际参数均已核实为 TLS=1、compression=zstd。标签已修正并附说明，
初始文件原样保存在 `metadata-initial-default-labels.json`，实际进程参数证据保存在
`runtime-command-evidence.json`。这次修正不改变运行配置；最终已再次断言 Client
测量行和 Server ready 回执均为 TLS=1、compression=zstd，并核对下述完整计数。

## 最终结果

| 指标 | 实际结果 |
|---|---:|
| offered / accepted / completed / ok | 各 900000 |
| Server started / finished，含预热 | 各 900032 |
| Server drain_status / 残留 queued / running | 0 / 0 / 0 |
| 原 clean_result / wall signals | true / 空 |
| 发生器丢弃、提交错误、deadline、RPC 错误、无效正文 | 均 0 |
| 成功速率 | 249.998 RPC/s |
| 成功延时 P50 / P99 | 24.312 / 46.139 ms |
| 调度迟到 P99 / 最大值 | 0.420 / 74.695 ms |
| Client 测量含排空时间 / 尾部 | 3600.025575 秒 / 25.575 ms |
| Server 生命周期 / Client 测量阶段 CPU | 1763.428 / 2541.321 秒 |
| Server CONTROL TX 池峰值 / 容量 / 耗尽次数 | 19 / 128 / 0 |
| RX、TX、重组池耗尽 / command、completion 队列满 | 均 0 |

测量窗内提交并接纳全部 900000 项，窗内完成 899994 项，余下六项在上述尾部完成。
没有把尾部完成隐去，也没有把最大调度迟到等同于原运行器的 P99 质量判定。
Server 的 `peers_ready_total=peers_reaped_total=1`，压缩参数及正文校验保持原基准规则。

共 361 个资源样本，从 0.000 到 3600.022 秒：

| 资源，单位 KiB | Server 首 / 末 | Server 最小–最大 | Client 首 / 末 | Client 最小–最大 |
|---|---:|---:|---:|---:|
| RSS | 12732 / 13064 | 12732–13064 | 12628 / 62076 | 12628–62076 |
| PSS | 3796 / 4120 | 3796–4188 | 3810 / 53250 | 3810–53250 |
| 匿名驻留页 | 2656 / 3004 | 2656–3004 | 2676 / 52156 | 2676–52156 |

所有采样中 Server fd=9、线程=7，Client fd=7、线程=3。RSS 为 `/proc/status`
的采样读数；退出行 `getrusage` 的生命周期峰值另为 Server 25696、Client 61216
KiB。它们的时间区间和统计来源不同，差值未进一步归因，未混合成一条峰值序列。
Client 逐渐驻留的 50400000 字节样本区已在前述口径中说明；未根据 RSS 增量
直接得出库泄漏或无泄漏结论。完整数值序列见[资源 CSV](data/rpc_v2_tls_zstd_soak_20261029.csv)，
计数、环境及原始文件 SHA-256 见[结果摘要](data/rpc_v2_tls_zstd_soak_20261029_summary.json)。
原始 JSONL 和私钥仍留在忽略目录，不纳入提交。

## 剩余验收

本机是共享开发主机，期间存在本地构建、测试及其他活动，未隔离 CPU/内存压力；
不能用这次低负载诊断给出硬件容量、吞吐基线或 TLS/Zstd 性能开销对比。
它验证 dev.177 原二进制的单连接服务端认证 TLS/Zstd 小消息持续运行，
不冒充后续 dev.178 信任来源修复的精确提交结果。
本场景与 40k 容量、mTLS 握手洪峰、恶意输入、慢消费者、凭证轮换/撤销和多平台
验收分别登记；这些发布要求仍开放。更多资源边界见[TLS 周转记录](rpc_v2_tls_resource_waves_20261029.md)。
