# RPC V2 双连接组重复故障与快速替代回归

## 范围

本记录对应 `2.0.0-dev.60` 与 GitHub #242/ERR-D。在 dev.59 的真实 TCP 双组场景上再增加一轮同时故障与替代，覆盖 DATA 成员两次连续换代。不改生产代码、公开 API 或线协议。

## 场景与断言

1. 两个不同 group/epoch 共用同一 Server shard，各由独立 Client Reactor 承载活动 transfer。
2. 两个 DATA 成员经屏障并发失效；两组 transfer 均变成 stale，Server 保留两个 CONTROL 组且 DATA/transfer 计数归零。
3. 两组各自建立第一代替代 DATA 成员并交付消息；随即再次并发故障。第二次失效后，两组上一代 transfer 再次返回 `TR_ERR_STALE`，组计数保持一致。
4. 两组分别建立第二代替代成员并交付消息，物理连接句柄相对上一代发生变化。关闭 A 组后 B 组仍成功发送并独立 drain，最终 Server 组、连接、DATA 与 transfer 计数归零。

## 验证

- GCC Release：完整 `test_connection_group_facade` 连续运行 8/8 通过。
- GCC TSan：完整 `test_connection_group_facade` 通过，无数据竞争报告。
- 销毁时序：诊断运行观察到压缩 drain 测试的 `tr_server_destroy()` 偶发返回 `TR_AGAIN`；这是生命周期 API 的可重试结果，生产代码已有对应的重试设计。将两处一次性 `TR_OK` 断言改为复用有界重试辅助后，Release 连续 8/8 通过；超出 `TR_AGAIN`/`TR_ERR_STATE` 的错误仍立即失败，未放宽业务断言。
- dev.59 head `487177e` 的 PR #324 GitHub 检查已全部通过；dev.60 尚未推送。
- `git diff --check` 通过。

构建使用工作树内隔离的 xmake 输出与临时目录，不写入 `/tmp`：

```sh
TMPDIR="$PWD/build/test-tmp" xmake f -P . -m release -o build/test-r14
TMPDIR="$PWD/build/test-tmp" xmake build -P . test_connection_group_facade
TMPDIR="$PWD/build/test-tmp" xmake run -P . test_connection_group_facade

TMPDIR="$PWD/build/test-tmp" xmake f -P . -m tsan -o build/test-r14-tsan --toolchain=gcc --crc32c_portable=n
TMPDIR="$PWD/build/test-tmp" xmake build -P . test_connection_group_facade
TMPDIR="$PWD/build/test-tmp" TSAN_OPTIONS=halt_on_error=1 xmake run -P . test_connection_group_facade
```

## 未覆盖范围

本回归覆盖两个连接组各自两轮并发故障与替代，不覆盖更多轮次、高压资源竞争、慢消费者交错或完整 ERR-D 矩阵。#242 与 RPC V2 整体验收仍未完成。
