# TLS Client provider fetch/cancel 回调屏障回归

## 范围

dev.168 只扩展 `test_tls_provider`。通过确定性门闩让凭证 provider 的 `fetch` 回调处于活动状态，再由外部控制线程取消同一操作，核对 `fetch` 返回屏障、provider 通知顺序和异步结果终局。不修改生产代码、公开 API/ABI、wire 或 TLS 策略。

门闩只存在于测试 provider 中，用于稳定制造并发窗口；生产 provider 仍须遵守 `include/tr/tls.h` 的快速返回契约，不得等待外部 I/O。

## 验收路径

1. Client start 在线程 A 受理操作并进入 `fetch`。测试保存 provider 收到的操作身份并阻塞该测试回调。
2. 控制线程调用 `tr_client_tls_credentials_cancel()`。返回 `TR_OK` 后，provider cancel 尚未运行；fetch 仍活动。并发 poll 返回 `TR_AGAIN`，结果哨兵保持不变。
3. 测试释放 fetch 门闩。start 返回同一操作句柄后，provider cancel 恰好调用一次，且观察到 fetch 已返回；没有在 fetch 活动期间重入。
4. provider 在取消之后提交迟到成功完成。poll 只交付 `TR_ERR_CANCELLED`，profile generation 为零；操作退役后 Client destroy 成功。

该顺序覆盖 Client 路径中的 `start_active` 延迟通知逻辑与操作池迟到完成清理，不将“请求取消”当作 provider 静默或操作退役。

## 验证

- GCC Release：`test_tls_provider/default` 通过，新增门闩路径连续运行 25/25。
- GCC Release 全量测试：94/94 通过。
- GCC TSan：`test_tls_provider/default` 通过。
- GCC ASan/UBSan：`test_tls_provider/default` 通过。
- 变异检查：临时将 `tls_credentials_start_active` 错设为 0 后，回归在“fetch 仍活动时 cancel 已执行”断言处失败；立即还原生产文件，再次通过。
- `git diff --check` 通过；clang-format 18.1.8 改动行检查提交后执行。
- 测试证书和全部临时内容位于当前 worktree `build/test-tmp`，未写入 `/tmp`。

## 未结项边界

该回归仅证明 TLS Client 凭证路径的一种取消/回调顺序。Server 与连接组 listener 的 provider 生命周期、profile/route generation 失配、TLS-D2/E 的平台和压力矩阵、其他 TLS/压缩/RPC 调用方屏障及跨能力共享调度仍需分别验证。#243、#234 与 V2 总体验收保持开放。
