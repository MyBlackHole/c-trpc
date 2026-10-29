# 客户端 unary 提交 AGAIN 归因诊断

日期：2026-10-30；关联 #241、#247。此诊断只定位公开 unary 提交返回 `TR_AGAIN` 的来源，不改变产品行为。

基线 HEAD 为 `adc4574ab2a1a0bd8df34f06348099e0e12656b9`。场景复用 `generator-window-16x256` 的一格：总计划速率 40,000/s，16 个发生器，10,000 次到达，客户端窗口 256、bench capacity 256、明文、无压缩、3,000ms deadline。bench 将 `max_calls` 和 `max_streams` 配置为 `2 * capacity`，即 512；客户端默认 `rpc_message_pool_count` 仍为 64。

临时计数器覆盖 unary start 的空调用槽、父 context、正文复制池、`tr_stream_open()` 和 Record binding 失败分支，并在 Client 释放时读取正文池容量、高水位和耗尽次数。ZIP 中的 `probe/probe.patch` 是运行后依照同一计数逻辑重建的复现补丁，已通过 `git apply --check`；首次插桩编译前没有保留该源码缓冲的独立摘要，因此不将它声称为首次编译源文件的逐字节副本。计数器只在 `src/rpc/rpc_record_client.c` 的临时诊断 ELF 中，源文件随后恢复。执行器使用 `run_rpc_scalability.run_multisource_case()`；所有日志和缓存位于工作区 `build/`，没有写入 `/tmp`。

一次运行中 10,000 次提交全部尝试，2,992 次受理并完成且均为 OK；7,008 次返回 `TR_AGAIN`。这 7,008 次与正文池失败计数逐项相等；16 个客户端池容量和峰值均为 64。空槽、context、`STREAM_OPEN`、binding 失败计数均为 0。服务端 3,024 次调用开始/完成包含 32 次 warmup；排空成功。服务端 CONTROL TX 池另有 1,655 次耗尽，命令与完成队列满计数均为 0。

这证明该场景下的同步 `TR_AGAIN` 来源是客户端 unary 正文复制池。公开文档已经允许复制池不足时返回 `TR_AGAIN`；本结果是资源预算归因，不单独证明改变默认池大小是正确的 API 设计，也不证明扩大池能提升吞吐。后续是否增加池预算或公开配置需要另行比较内存上限、受理率和服务端控制池压力。

临时 ELF SHA-256：`aebbe3ef9612d27a6d9b9ddb6d6bfbca225f36810b42b5484af1e019a77886fb`。恢复后的 `bench_rpc` 与此前冻结候选 SHA-256 `55d1c4b990f1878f7999017960c2c6cf5443fdf998be9ece389c5fa946bd137e` 匹配；当前源码树干净。Xmake 在源恢复后的重建产物哈希不同于冻结候选；对比确认两个提交之间的源差异只有 `channel.h` 的注释变更，因此恢复时使用归档冻结 ELF，并未声称重建产物逐字节相同。

[机器可读摘要](../../data/rpc_client_submit_origin_20261030.json)、[原始日志包](../../data/rpc_client_submit_origin_20261030_raw.zip)保存汇总与 16 个客户端/Server 输出。原始包不含二进制或凭证。
