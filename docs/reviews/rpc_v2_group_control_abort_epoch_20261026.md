# RPC V2 CONTROL 致命拆除与 epoch 路由回归

## 目的

为 #242 的连接组错误拆除语义补充组件级证据：CONTROL 丢失时，已附接 DATA、活动 stream affinity 和未消费的 DATA 预留都必须随旧 Pipeline 一起退役；复用 pipeline ID 建立新 epoch 后，旧路由前导不得影响新预留。

## 场景与断言

- 在真实 Reactor owner 上创建 Pipeline Control，配置 3 个 DATA 槽和 2 个 stream affinity 槽。
- 附接两个 DATA 成员，并分别建立 transfer；再保留一个尚未附接的 DATA offer。拆除前精确断言 CONTROL 已绑定、2 个 DATA 已附接、1 个 offer 仍预留、2 个 stream affinity 活跃。
- 调用 `tr_pipeline_control_abort()`，验证注册表中的 Pipeline 数量归零，并拒绝旧 epoch 的 DATA route。
- 以相同 pipeline ID 创建 epoch 42；验证新 DATA 代次高于旧 offer。再次提交 epoch 41 的 route 必须返回 `TR_ERR_STALE`，且新 offer 仍保持预留；有效的新 route 随后可附接和分离。
- 最后正常关闭新 epoch，检查注册表回到零。

## 验证

- GCC Release：定向 `test_pipeline_control/*` 通过。
- GCC TSan 与 ASan/UBSan（泄漏检测开启）：定向 `test_pipeline_control/*` 通过。
- clang-format 18.1.8 changed-lines 检查与 `git diff --check` 通过。
- 临时目录与构建产物位于工作树 `build/err-d-highfrequency/`。

## 范围与限制

本次只增加组件测试、证据记录和开发版本号，不更改生产代码、公开 API/ABI 或 wire。测试使用无效的模拟 Reactor connection handle，因此验证的是 Pipeline Control、Registry、DATA 代次和 affinity 的软状态回收，不直接证明真实 TCP 套接字关闭回调或公开连接组客户端的远端故障处理。真实 TCP 的双组 32 轮并发故障替代证据见[双组高频成员故障替代回归](rpc_v2_group_repeated_failure_stress_20261026.md)；#242 的完整验收仍未完成。
