# RPC V2 双 DATA 成员故障与替代回归

## 范围

本记录对应 `2.0.0-dev.57` 与 GitHub #242/ERR-D。新增公开连接组门面集成测试，不改变生产代码或公开 API；目标是证明一个 DATA 成员故障时只撤销对应亲和，另一个成员继续工作，并可在同组同 epoch 上重新建立替代成员。

## 场景与断言

1. 真实 Client/Server 连接组建立两个 DATA 成员；测试 wrapper 捕获两条 DATA 消息使用的客户端 `tr_conn_handle`，确认它们是不同物理连接。
2. 为两个成员分别发出 Transfer Ready 并发送 DATA。对第一条 DATA 物理连接调用 `tr_reactor_abort()`，等待服务端观察到成员关闭。
3. Client/Server 统计均只剩一个 DATA 成员和一个活动亲和；旧 Client transfer 的发送返回 `TR_ERR_STALE`，第二条成员上的消息仍成功交付。
4. 在相同 group/epoch 上重新发 DATA Offer 与 Transfer Ready。替代连接的 Reactor handle 与故障连接不同；替代成员上的新 transfer 成功发送并到达服务端。
5. 释放两个剩余亲和并开始双端 drain。Client drain 可在保留成员连接时完成；Server 在 CONTROL 和所有 DATA 连接退出前保持未完成。关闭 Client 连接组后 Server drain 完成，组、连接、DATA 成员与 transfer 计数归零。

## 验证

- GCC Release：完整 `test_connection_group_facade` 通过，包含多 DATA drain、成员故障/替代、压缩任务 drain 与压缩 wire drain。
- GCC TSan：完整 `test_connection_group_facade` 通过，无数据竞争报告。
- 变异检查：将故障回调的 transfer 失效代次改为 `member_generation + 1` 后，测试在 `client_stats.active_transfers == 1` 断言失败；恢复精确代次后 Release 与 TSan 均通过。
- `git diff --check` 通过；dev.56 的 Draft PR #324 GitHub 检查已全部通过，dev.57 head 的 CI 尚未运行。

构建和运行使用当前工作树内隔离的 xmake 输出目录与测试临时目录，避免复用父工作区产物：

```sh
TMPDIR="$PWD/build/test-tmp" xmake f -P . -m release -o build/test-r14
TMPDIR="$PWD/build/test-tmp" xmake build -P . test_connection_group_facade
TMPDIR="$PWD/build/test-tmp" xmake run -P . test_connection_group_facade

TMPDIR="$PWD/build/test-tmp" xmake f -P . -m tsan -o build/test-r14-tsan --toolchain=gcc --crc32c_portable=n
TMPDIR="$PWD/build/test-tmp" xmake build -P . test_connection_group_facade
TMPDIR="$PWD/build/test-tmp" TSAN_OPTIONS=halt_on_error=1 xmake run -P . test_connection_group_facade
```

## 未覆盖范围

本回归只覆盖同一个组内一个成员故障并替代的单次路径，不覆盖多组并发竞争、多次快速替代、压缩任务恰在成员故障时完成、CONTROL 故障及完整 ERR-D 矩阵。#242 与 RPC V2 整体验收仍未完成。
