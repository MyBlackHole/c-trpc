# Server TLS provider fetch/cancel 回调屏障回归

## 范围

dev.169 扩展 `test_tls_provider` 的 Server 凭证路径。测试只增加确定性回归，不改 Server 运行时、公开 API/ABI、wire 或 TLS 策略。

## 验收路径

1. Server start 在线程 A 受理凭证轮换并进入 provider `fetch`；测试 provider 用门闩保持回调活动。
2. 控制线程查询同一操作时，poll 返回 `TR_AGAIN` 并保留结果哨兵；外部 cancel 返回 `TR_OK`，provider cancel 此时仍为零次。
3. 释放门闩并等 start 返回后，provider cancel 恰好调用一次，且没有在 fetch 活动时重入。
4. provider 提交迟到成功完成后，poll 得到 `TR_ERR_CANCELLED`、generation 为零；操作退役后 Server 可销毁。

测试只覆盖 Server TLS 凭证轮换 API 的 start/fetch/cancel/complete/poll 生命周期，不等价于对端 TLS 握手、连接组 listener 接纳、profile generation 轮换或 TLS-E 压力与平台矩阵的验收。门闩只用于测试；生产 provider 仍须按 `include/tr/tls.h` 约定快速返回。

## 验证

- GCC Release `test_tls_provider` 连续运行 25/25；全量 Release 套件 94/94。
- GCC TSan 与 ASan/UBSan 的 `test_tls_provider` 目标通过。
- 临时将 `tls_credentials_start_active` 置零的变异在 fetch 活动期间 provider cancel 次数断言处失败；源码已恢复，恢复实现下目标通过。
- 所有测试临时目录位于当前 worktree 的 `build/test-tmp`，未写入 `/tmp`。
