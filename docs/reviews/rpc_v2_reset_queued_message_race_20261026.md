# RESET 与已排队响应 MESSAGE 竞态回归

日期：2026-10-26。关联 [#242 / ARCH-04](https://github.com/MyBlackHole/c-trpc/issues/242)，补充“RESET 与 DATA/STATUS/半关闭并发”的真实 Endpoint 响应数据时序。

## 场景和观察

测试使用真实 TCP、Reactor、Channel 与 RPC Record Endpoint，打开一条双向目标调用和一条健康同伴。目标调用的请求 MESSAGE 被服务端业务处理后，Endpoint 将响应 MESSAGE 交给 Channel；测试通过 `sendmsg` 包装识别其对应的 DATA 帧，并令该真实 socket 写入返回 `EAGAIN`，客户端确认尚未收到响应后发送 timeout RESET。

服务端处理 RESET 并终结目标调用后，测试记录此时已被接受的响应 MESSAGE 数量，再解除写阻塞并等待 RESET 收敛。断言包括：

- 客户端和服务端均以 `DEADLINE_EXCEEDED` 终结目标调用，终局计数各为一；
- 服务端处理 RESET 后不再接受该调用的新回复；
- 解除背压后，客户端未收到目标调用的迟到 MESSAGE，连接没有 Channel down；
- 同连接健康调用仍完成成功，目标调用终局不重复。

计数边界选在服务端终局回调，是因为客户端发出 RESET 到服务端观察 RESET 之间，已在途的业务完成可能合法地先提交回复；测试允许这一竞态，只禁止服务端处理 RESET 后的新接管，并验证先前排队的数据不会迟到交付。

## 验证

- GCC Release：完整 `test_rpc_record_endpoint_tcp` 通过；
- GCC ThreadSanitizer：完整目标通过；
- GCC AddressSanitizer/UndefinedBehaviorSanitizer，泄漏检测开启：完整目标通过；
- `git diff --check` 与改动行 clang-format 检查通过。

本切片只增加 Endpoint 回归、审查记录、README、剩余工作记录和候选版本号，不修改生产逻辑、公开 API/ABI 或 wire。它只补充 RESET/DATA 交错证据，不关闭 #242 的完整 RESET/FIFO、配额公平、GOAWAY/连接组接受边界或全局资源回收验收。
