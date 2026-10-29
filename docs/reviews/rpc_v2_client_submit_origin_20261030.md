# 客户端 unary 提交 `TR_AGAIN` 来源

日期：2026-10-30；关联 #241、#247。

## 结论

在 16 个客户端、每客户端窗口 256 的高并发诊断中，7,008 次公开 unary 提交 `TR_AGAIN` 全部由客户端正文复制池耗尽触发。调用槽耗尽、父 context、`STREAM_OPEN` 和 Record binding 的 `TR_AGAIN` 计数均为零。正文池容量和峰值均为 64。

`tr_client_unary_call()` 复用 `tr_record_client_call_start()`；它在创建流之前先复制请求正文，`tr_buffer_acquire()` 在空闲描述符为零时返回 `TR_AGAIN`，该返回未经转换地交给应用。bench 参数将 `max_calls`、`max_streams` 设为 `2 * capacity`，所以 `capacity=256` 时公开调用/流上限为 512，但默认复制池仍为 64。参见 [`rpc_record_client.c`](../../src/rpc/rpc_record_client.c)、[`buffer.c`](../../src/execution/buffer.c)、[`facade.c`](../../src/facade.c) 和 [`bench_rpc.c`](../../bench/bench_rpc.c)。SDK 文档已说明复制池不足会返回 `TR_AGAIN`；因此本次指出的是压测窗口与默认内部资源池之间的预算不匹配，尚不能单独决定应扩大默认值、公开配置还是维持拒绝语义。

## 输入与结果

运行基于 HEAD `adc4574ab2a1a0bd8df34f06348099e0e12656b9`，使用 Xmake Release 生成的临时插桩 ELF。测量参数为 40,000 次/秒、10,000 次到达、16 个发生器、每客户端窗口 256、bench capacity 256、无 TLS/压缩、3,000ms deadline。完整配置和按客户端数据见[汇总 JSON](data/rpc_client_submit_origin_20261030.json)。

| 观察项 | 结果 |
|---|---:|
| 计划 / 实际尝试 | 10,000 / 10,000 |
| 受理 / 完成 / OK | 2,992 / 2,992 / 2,992 |
| 提交返回 `TR_AGAIN` | 7,008 |
| 正文池 `TR_AGAIN` / pool exhausted | 7,008 / 7,008 |
| 空调用槽 / context / `STREAM_OPEN` / binding `TR_AGAIN` | 0 / 0 / 0 / 0 |
| 正文池容量 / 峰值 | 64 / 64（每客户端）|
| Server CONTROL TX 池耗尽 | 1,655 |
| Server 命令队列 / 完成队列满 | 0 / 0 |

Server 共开始并完成 3,024 次调用，含 32 次 warmup；排空状态为 `TR_OK`。CONTROL TX 池耗尽与客户端 unary 同步提交返回值是不同观察：前者可能影响后续进度，但不是本次 7,008 次 `TR_AGAIN` 的直接返回路径。调度发生迟到，因此这次运行也不构成 40k 容量通过证据。

## 归因范围

这是单次插桩复测，没有 pool=64 与更大 pool 的配对数据，也没有改变产品资源配置。插桩 ELF 的临时源码缓冲未单独留存；原始包中的 `probe.patch` 是按相同分支计数逻辑重新生成并通过 `git apply --check` 的复现补丁，未声称其源文件哈希与最初编译缓冲逐字节相同。结论只定位当前提交拒绝来源；后续需单独验证更大正文池是否改变受理率、内存预算与 Server CONTROL TX 压力。详见[原始日志包](data/rpc_client_submit_origin_20261030_raw.zip)与[探针记录](experiments/rpc_client_submit_origin_20261030/README.md)。

## 恢复与身份

临时探针 ELF SHA-256：`aebbe3ef9612d27a6d9b9ddb6d6bfbca225f36810b42b5484af1e019a77886fb`。源文件恢复后，其 SHA-256 为 `cddc7176a068daacb0fb9781a6b3e28516caa9e4d833fe6c9eb0602c0acdf970`，与 HEAD 文件一致；跟踪的运行时代码未保留探针修改。当前 `build/linux/x86_64/release/bench_rpc` 已恢复为此前冻结候选，SHA-256 为 `55d1c4b990f1878f7999017960c2c6cf5443fdf998be9ece389c5fa946bd137e`。恢复源码后的 Xmake 重建产物曾得到不同哈希；检查 HEAD 与先前冻结候选构建提交间的代码路径差异，仅 `src/transport/channel/channel.h` 的生命周期注释发生变化。因此这里恢复的是留存的冻结候选 ELF，不将重建结果声称为逐字节相同。
