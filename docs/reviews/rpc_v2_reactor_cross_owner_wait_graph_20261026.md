# RPC V2 Reactor 跨 owner 同步等待图验证记录

日期：2026-10-26。基于 #327 的 `2.0.0-dev.153` 链尾，候选版本 `2.0.0-dev.154`；关联 #243 / ARCH-07。

## 范围

本切片给 Reactor owner 的同步跨 Reactor 等待登记依赖边，保留无环 A→B、A→B→C 调用；若新增边闭合环，在命令入队前返回 `TR_ERR_STATE`。覆盖 `tr_reactor_call`、`tr_reactor_set_handler`、`tr_reactor_quiesce`、`tr_reactor_stop`，并让 `tr_reactor_close_graceful` 的 scope 横跨命令完成与 TLS `close_notify` 完成。Reactor 被停止后若仍有已唤醒 waiter 尚未退出 scope，`tr_reactor_destroy` 返回 `TR_AGAIN`，保留对象供重试。

## 实现与影响

- 每个 Reactor 保存唯一活动出边、嵌套 scope 深度、入边 waiter 数与销毁准入标记；静态等待图 mutex 保护这些字段。
- 图锁不跨越命令队列、条件变量、用户 callback 或 `pthread_join`；start/destroy 的原有外层生命周期串行合同仍有效。
- 新字段由已有 `sizeof(struct tr_reactor)` 固定内存账目自动计入；无新动态分配、线程、公共 API/ABI 或 wire 字段。
- 每次 owner 同步跨 Reactor 等待在 begin/end 路径取图锁，环检查沿活动依赖链 O(n)；非 owner 等待和异步/数据热路径不访问该锁。未声称吞吐提升或独立基准结果。

## 验证

以下命令均在该 worktree 执行，`TMPDIR` 与 `XMAKE_TMPDIR` 指向 `build/reactor-wait-graph/<mode>/`：

```sh
xmake test -P . -j1 'test_reactor_*/*' 'test_rpc_v2_tls_reactor/*'
```

- GCC Release：12/12 通过，包括真实 TLS Reactor graceful-close smoke。
- GCC ASan/UBSan：12/12 通过。
- GCC TSan：12/12 通过。
- Release `test_reactor_owner_wait_graph` 连续 100 次：100/100 通过。
- `xmake test -P . -j1 'test_rpc_v2_sdk/*'`：1/1 通过；安装后公共头文件及真实 V2 consumer 在 GCC/Clang C11、G++/Clang++ C++11 下编译运行通过。
- 固定 clang-format 18.1.8：tracked 改动行检查无额外格式 diff；新增测试全文件 dry-run 通过。`git diff --check` 与 staged diff 检查通过。

新增回归覆盖二/三节点环与无环链、quiesce/set-handler/stop 环竞争、真实 TLS graceful-close completion 期间的反向调用拒绝、以及 inbound waiter 仍退出时 destroy 返回 `TR_AGAIN` 后重试成功。测试用有界 watchdog；destroy 竞态由测试二进制内 `pthread_cond_wait` wrapper 控制，不增加生产测试 hook。

## 边界与后续

此记录只证明 Reactor 同步入口的等待环保护及目标生命周期，不代表 #243 或 RPC V2 整体完成。Call 级父子 deadline、取消上下文、迟到完成处理、最终结果与最后回调分离、持有引用回收证明等验收仍开放。TLS-C/D 全验证矩阵与 #234 也仍需独立验收。
