# RPC V2 双连接组成员故障隔离回归

## 范围

本记录对应 `2.0.0-dev.58` 与 GitHub #242/ERR-D。新增测试专用的真实 TCP 连接组场景，不改生产代码、公开 API 或线协议。测试显式配置单个 Server shard，使两个不同 group/epoch 共用同一个服务端 owner 与 Pipeline Registry。

## 场景与断言

1. 两个 Client 同时连接到同一 Server，各自持有独立 CONTROL 与 DATA 成员；为两个组分别建立并发送活动 transfer。
2. 中止 A 组捕获到的 DATA 物理连接。等待服务端成员关闭事件后，A 组数据/transfer 归零，B 组仍保持一个 DATA 成员和活动 transfer；B 组继续交付消息。
3. 在 A 组相同 group/epoch 上重新 Offer/Ready，替代成员通过新物理连接交付消息。此时 B 组再次成功发送，Server 汇总仍有两个组、两个 DATA 成员和两个活动 transfer。
4. 关闭 A 组后 Server 只剩 B 组；B 组再次发送成功。随后只对 B 组完成 Client drain、关闭其成员并完成 Server drain，最终组、连接、DATA 与 transfer 计数归零。

## 验证

- GCC Release：完整 `test_connection_group_facade` 通过，包含双 DATA drain、单组成员替代、双组故障隔离及压缩 drain 回归。
- GCC TSan：完整 `test_connection_group_facade` 通过，无数据竞争报告。
- 变异检查：暂时将 A 组 `0x9921` 的 DATA 退役查找改为 B 组 `0x9922`；新回归在故障后的服务端组统计收敛等待处失败。恢复按原始 Pipeline ID 查找后，Release 与 TSan 均通过。
- dev.57 head `d4b0ac0` 的 PR #324 GitHub CI 已全部通过；dev.58 尚未推送，因此其 GitHub CI 未运行。
- `git diff --check` 通过。

构建和运行使用当前工作树内隔离的 xmake 输出目录与测试临时目录，不写入 `/tmp`：

```sh
TMPDIR="$PWD/build/test-tmp" xmake f -P . -m release -o build/test-r14
TMPDIR="$PWD/build/test-tmp" xmake build -P . test_connection_group_facade
TMPDIR="$PWD/build/test-tmp" xmake run -P . test_connection_group_facade

TMPDIR="$PWD/build/test-tmp" xmake f -P . -m tsan -o build/test-r14-tsan --toolchain=gcc --crc32c_portable=n
TMPDIR="$PWD/build/test-tmp" xmake build -P . test_connection_group_facade
TMPDIR="$PWD/build/test-tmp" TSAN_OPTIONS=halt_on_error=1 xmake run -P . test_connection_group_facade
```

## 未覆盖范围

本回归只验证两个 group 中一个组单次 DATA 故障、替代与关闭，不覆盖多个组同时故障、多组连续快速替代、压力资源竞争和完整 ERR-D 矩阵。#242 与 RPC V2 整体验收仍未完成。
