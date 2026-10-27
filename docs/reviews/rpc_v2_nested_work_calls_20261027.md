# RPC V2 嵌套 work 调用回归

日期：2026-10-27。对应范围：#243 的 Server V2 work worker 跨 Client owner 提交子 Call。

## 实现范围

`tr_dispatch_v2_work()` 仅在实际业务 `work` 调用期间设置独立线程局部标记。现有广义 dispatch callback 标记仍用于禁止其他同步重入；Client 同步入口只有 `tr_client_call_start()` 和 `tr_client_unary_call()` 的提交路径可以检查该 work 标记并提交短操作。Client 回调、Server authorize 和 Reactor owner 回调继续受原有跨 owner 禁入条件保护。

work callback 只能提交子 Call 并立即返回，不能等待子 Call terminal 或依赖当前 TaskExecutor 的工作。测试通过 `parent_context` 继承父取消与 deadline，并用 `tr_rpc_v2_reply_acquire_producer()` 将父回复延迟到子 Call terminal 后完成。

## 真实 TCP 场景

测试建立两个独立 Client/Server facade：A Server work 提交 B Client 子 Call，B Server work 再提交 A Client 子 Call，第二次到达 A 的 Server 作为叶节点返回一条响应。两条中间调用的 parent context 与 producer 都必须释放。

回归断言包含：

- 两次 work 内 `tr_client_call_start()` 均返回 `TR_OK`，叶响应正文准确到达。
- 根、子、孙 Client Call 各交付一次 terminal；A Server 两次、B Server 一次 terminal。
- Client writable callback、Server owner observer、Server authorize 与 Server terminal callback 跨到另一 Client 提交 Call 均返回 `TR_ERR_STATE`，并保持输出哨兵不变。
- 所有 Client/Server drain 与 destroy 成功，两个 parent context 和两个 producer 引用均释放。

## 验证

- 先运行旧门禁确认红灯：第一次 work 内嵌套提交返回 `TR_ERR_STATE`。
- GCC Release 定向目标通过；连续运行 25 次通过。
- GCC ASan/UBSan 定向目标通过，泄漏检测开启。
- GCC TSan 定向目标通过。
- GCC Release 完整测试套件 94/94 通过。
- 公开头文件 C11 与 C++11 `-fsyntax-only` 编译通过；`git diff --check` 通过。
- GitHub 检查待推送到 PR #332 后复验。

## 未完成范围

本回归只证明一条 A→B→A 提交与异步父回复链，不表示 #243 已完成。更广的跨 Reactor 交错、command/completion 队列满载、所有 callback/deferred completion/resource drain 屏障、TLS/压缩共享调度与组合压力仍需单独验证；#243/#239 保持 OPEN。
