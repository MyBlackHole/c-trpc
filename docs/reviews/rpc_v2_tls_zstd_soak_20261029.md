# TLS 与双向 Zstd 持续运行记录

日期：2026-10-29。当前状态：**一小时运行中，尚未计入通过**。
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
`runtime-command-evidence.json`。这次修正不改变运行配置；最终原始计数行仍须再次核对。

## 剩余验收

完整小时尚未结束，不能据当前样本宣称通过或给出最终趋势。
本场景与 40k 容量、mTLS 握手洪峰、恶意输入、慢消费者、凭证轮换/撤销和多平台
验收分别登记；这些发布要求仍开放。更多资源边界见[TLS 周转记录](rpc_v2_tls_resource_waves_20261029.md)。
