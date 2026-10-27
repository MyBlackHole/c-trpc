# Server TLS provider 监听器凭证轮换回归

日期：2026-10-27。关联 #243。版本：`2.0.0-dev.170 -> 2.0.0-dev.171`。

## 需求与背景

为异步 Server TLS 凭证 provider 增加端到端监听器覆盖。已有测试分别证明凭证 API 的 fetch/cancel 生命周期和静态 TLS listener 握手；本回归确认 provider 交付与轮换的新 profile 实际用于普通 RPC listener 和 connection-group pipeline listener。它只关闭 #243 中 listener profile 更新这一条路径，不代表整体调用方屏障或 V2 验收完成。

## 实现与影响

- `test_rpc_v2_tls_identity` 在监听前由 provider 安装 CA A 的 profile；先验证旧客户端 RPC，再将 profile graceful 轮换到 CA B。已接纳的旧 TLS 连接仍能完成 RPC；只信任 CA B 的新客户端完成 TLS 握手和 RPC。三次调用均检查唯一终局、客户端消息、OPEN/MESSAGE/HALF_CLOSE work 计数、零应用错误，以及 provider fetch 两次、cancel 零次。
- `test_pipeline_listener_tls` 通过公开 connection-group listener 入口验证无凭证时 listen 返回 `TR_AGAIN` 且端口输出不变；安装 CA A 后 listener 可接收 mTLS 连接。轮换至 CA B 后，新客户端只信任 CA B、仍呈交受信任的 A 客户端证书，握手成功；已有旧代 CONTROL/DATA 则在 graceful 策略下继续完成第二条 DATA 消息。
- pipeline 用例保留现有代次隔离契约：新代 TLS peer 只用于证明 listener 使用新 profile，不把它并入旧代 CONTROL；旧代会话继续排空。
- 仅修改测试、开发版本与验收文档；不改生产运行时、公开 API/ABI、TLS 策略或 wire。无预期运行性能影响。

## 验证

- GCC Release 定向构建：`test_rpc_v2_tls_identity`、`test_pipeline_listener_tls` 通过。
- 定向测试：两项目标均通过。
- GCC Release 全量测试：`xmake test -P . -v`，94/94 通过。
- GCC ASan/UBSan（`ASAN_OPTIONS=detect_leaks=1`）和 TSan 下，两项目标均通过。
- 敏感性检查：临时变异 Server owner 安装路径，使 connection-group listener 在 profile 轮换时收到空 profile；新 CA 握手断言按预期失败。随后恢复 `src/server.c`，重建并重跑正式用例通过；最终生产源无差异。
- clang-format changed-line 检查无输出，`git diff --check` 通过。证书临时文件由现有测试夹具写入当前 worktree 的 `build/test-tmp`，未写入 `/tmp`。

## 边界、风险与回滚

本回归不测 provider 回调与并发握手交错、TLS 握手洪峰、OpenSSL/内核内存总账、持续负载、平台互通，也不补齐 #243 中 RPC authorize、MessageService、其他 TLS/压缩调用方的生命周期及共享队列矩阵。若测试环境无法生成或读取 PEM，断言会令目标失败；测试身份均由既有 worktree 内夹具生成并清理。回滚时撤销 dev.171 提交即可同时移除测试、记录和版本号变更，不涉及运行数据迁移。

## 验收标准

1. provider 首次交付的 profile 能服务真实 TLS listener 握手。
2. graceful 轮换后，新 CA 验证新连接成功，已接纳的旧代会话仍能完成其既有工作。
3. 上述行为在普通 RPC listener 与 connection-group listener 两条公开入口均有真实网络回归。
4. Release 定向与全量测试通过，故障注入能让轮换 listener 未更新的变体失败。
5. 不把此切片标记为 #243 或 V2 总体验收完成。
