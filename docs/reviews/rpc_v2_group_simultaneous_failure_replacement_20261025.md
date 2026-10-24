# RPC V2 双连接组并发故障与替代回归

## 范围

本记录对应 `2.0.0-dev.59` 与 GitHub #242/ERR-D。扩展 dev.58 的真实 TCP 连接组门面测试，不改生产代码、公开 API 或线协议。两个不同 group/epoch 共用显式配置为单 shard 的 Server，并分别由独立 Client Reactor 持有 CONTROL 与 DATA 成员。

## 场景与断言

1. 两组各建立一个活动 transfer，并通过真实 DATA 帧确认传输归属和成员句柄。
2. 两个测试线程在同一条件变量屏障释放后，并发中止两条 DATA 物理连接；两组各收到一次成员关闭事件，Client 的 DATA/transfer 计数分别归零，Server 保留两个 CONTROL 组但 DATA/transfer 计数均为零。旧 transfer 发送均返回 `TR_ERR_STALE`。
3. Server 分别为两组重新 Offer/Ready；两个替代成员使用新物理句柄，各自交付独立消息，Server 统计恢复为两个组、两个 DATA 成员和两个 transfer。
4. 关闭 A 组后，B 组继续发送并独立 drain；最后 Client/Server 的组、连接、DATA 与 transfer 计数归零。

## 验证

- GCC Release：完整 `test_connection_group_facade` 通过。
- GCC TSan：完整 `test_connection_group_facade` 通过，无数据竞争报告。
- 变异检查：暂时将 group `0x9921` 的 DATA 退役查找错路由到 `0x9922`；测试在 Server 组统计收敛断言处失败。恢复按原始 Pipeline ID 查找后，Release 与 TSan 均通过。
- dev.58 的 PR #324 GitHub 检查已全部通过；dev.59 尚未推送。
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

本回归覆盖两个组同轮并发故障及各自一次替代，不覆盖多轮快速替代、压力资源竞争或完整 ERR-D 矩阵。#242 与 RPC V2 整体验收仍未完成。
