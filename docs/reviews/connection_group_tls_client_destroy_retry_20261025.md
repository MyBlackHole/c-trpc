# 连接组 TLS Client 异步销毁测试收敛

日期：2026-10-25。版本：`2.0.0-dev.97 -> 2.0.0-dev.98`。关联 #234、PR #324。

## 问题描述

Push CI 的 GCC Debug 全量测试在 `test_public_connection_group_mtls` 收尾时偶发失败，位置为
`assert(tr_client_destroy(client) == TR_OK)`。同一提交的 PR CI 与 Push Release 测试通过，失败
只在异步连接组/TLS 代次回收竞争期间出现。

## 原因分析

测试已等待服务端组退休和客户端 CONTROL/DATA 连接统计归零，但这不保证客户端 Reactor owner
销毁屏障在同一瞬间已经收敛。`tr_client_destroy` 可因短暂未收敛的生命周期所有权返回
`TR_ERR_STATE` 或 `TR_AGAIN`。同一测试文件已有 `destroy_server_after_async_release` 重试助手，
而该 Client 路径原来只尝试一次。

## 解决方案

新增 `destroy_client_after_async_release`，按 1 ms 间隔最多重试 5 秒，只接受 `TR_ERR_STATE`/
`TR_AGAIN` 作为可重试状态；其他错误仍立即由断言报告。仅更新测试清理步骤，不改生产逻辑。

## 影响范围

仅影响 `tests/test_connection_group_tls.c` 的 Public connection-group mTLS 用例收尾，不改变
库/API/ABI/wire 或用户可见行为。风险等级：低。

## 性能影响

生产路径无变化。测试在销毁屏障立即收敛时不增加等待；只有可重试状态时按现有测试约定短暂轮询，最长 5 秒。

## 复现步骤

1. GCC Debug 运行完整 `test_connection_group_tls/default`。
2. `test_public_connection_group_mtls` 完成 TLS generation revoke、等待两端连接数归零并启动 Server drain。
3. 若 Client owner 销毁屏障尚未收敛，单次 `tr_client_destroy` 返回可重试状态，旧断言失败；重试助手等待屏障后销毁成功。

执行命令：

```sh
TMPDIR="$PWD/build/test-tmp" XMAKE_TMPDIR="$PWD/build/test-tmp" \
  xmake test -P . -r test_connection_group_tls/default
```

## 测试验证

- GCC Debug `test_connection_group_tls/default`：变更后 6/6 连续通过。
- PR #324 dev.97 的全量 GCC/Clang、ASan/UBSan、TSan、静态分析及兼容安装检查全部通过。
- dev.97 Push CI 首轮唯一失败为 Debug 该 Client 销毁断言；dev.98 提交后的 CI 将重新验证。
- clang-format 18.1.8 修改行检查及 `git diff --check` 通过。

## 相关 Issue

GitHub #234（TLS 与身份集成），PR #324（当前 Draft 验证分支）。

## 风险评估

低。助手仅对已有异步生命周期状态重试，并为异常错误保留立即失败行为。重试上限 5 秒，超时仍断言失败。

## 回滚方案

执行 `git revert <本次提交>` 可恢复单次销毁断言并撤销对应记录和版本变更。

## 版本变更

- `xmake.lua`：`2.0.0-dev.97 -> 2.0.0-dev.98`
- `README.md`：候选版本和 dev.98 验证记录同步更新。
