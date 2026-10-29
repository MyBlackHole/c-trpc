# TLS 发送排空后残留写兴趣的忙转修复

日期：2026-10-29；版本 `2.0.0-dev.179`，关联 #234、#243、#247。
TLS 连接完成正常响应后，发送队列已空却仍监听 EPOLLOUT，使 I/O owner 在没有
业务请求时持续忙转。本修复在发送队列结算后重新计算 epoll 兴趣，保留必要读监听、
未完成发送及 TLS WANT 重试；不修改公开 API/ABI、wire、压缩或业务终局规则。

## 复现与因果证据

[持续握手诊断](rpc_v2_tls_handshake_stress_20261029.md)记录了高 CPU 和大量 Reactor
轮转，但当时没有将它归因于握手。使用同一冻结二进制、同一凭证和 100 RPS 的
TLS/Zstd 正常负载，移除握手压力后，30 秒仍消耗 18.12 秒 Server CPU；其中单一
I/O 线程为 17.44 秒，故握手洪峰不是必要条件。

进一步在 32 次预热后让正常 Client 停在启动 gate，15 秒不提交业务请求。Server
消耗 14.99 秒 CPU，全部来自同一个线程；业务 worker 没有新增 CPU tick。
实际内核 fdinfo 在前后都显示 TLS socket 的 epoll events=0x201d，包含 EPOLLOUT。
没有通过更改超时、窗口或增加线程消除此现象。

旧 [tr_connection_flush_tx](../../src/execution/reactor.c) 在 SSL 写成功时更新兴趣，
此时最后一个 tr_tx_item 尚未结算出队，因此仍选中 EPOLLOUT。随后队列排空，
函数返回前未再收窄；可写 socket 持续唤醒 epoll，而事件分派发现无 tx_head 就
不再调度发送，也不会修正兴趣。后续偶然收到数据可能清除它，不能依赖此时机。

## 先红后绿的回归

扩展既有 [test_rpc_v2_tls_reactor](../../tests/test_rpc_v2_tls_reactor.c) 的真实 TCP
TLS 回显/分片场景，保留原 target/default/120000ms 参数及全部原断言。
只在此测试二进制包装 epoll_ctl 和 TLS fd 接管函数，成功的系统调用仍真实执行，
以固定容量表记录当前注册。捕获两端 fd 与能力，在各自 owner 上确认连接 active、
tx_queued_items=0 后，断言无 EPOLLOUT 且仍有 EPOLLIN；无需 CPU/轮转性能阈值。

有效红测入口为：

```sh
xmake build test_rpc_v2_tls_reactor
xmake test -v -j1 'test_rpc_v2_tls_reactor/*'
```

未改生产代码时，真实运行在 `idle_fd=17 events=2005 queued=0` 处触发
`(events & EPOLLOUT) == 0U` 断言，Xmake 报 1/1 失败（6.077 秒）。修复后同一
完整目标 1/1 通过（7.191 秒）。测试内核观察值 0x2005 与 fdinfo 的 0x201d
分别保留；两者均含 0x4 写兴趣，不将不同来源的掩码冒充完全相同。

记录原始红/绿结果的文件为 build/performance-20261029/ 下
tls-idle-interest-red-test.log、tls-idle-interest-green-test.log。

## 实现及内联审查

发送循环完成后，若仍为 active 且存在 byte endpoint，再调用已有兴趣计算函数。
该函数只在目标掩码改变时执行 epoll_ctl；失败沿既有连接级 SYS 错误关闭和清理。
仍在等待 WANT/网络背压的路径保持其等待条件，尚未发送完的队列继续保有发送
机会；TLS 前导成功发送后清除缓冲的路径也在函数返回前按最终状态重新计算。

协议状态由原 owner 修改。修复不增加生产分配、引用、锁、线程或用户回调，不
改变 Buffer 的取得/释放边界。active 检查避免在关闭后继续修改事件兴趣。
测试观察表使用 mutex，开关使用 atomic；只有两端门面仍存活时检查弱连接能力，
销毁完成后才关闭观察。全程没有把测试包装链接到 trcore 或安装 SDK。

审查同时核对了 WANT_WRITE/WANT_READ、未完成队列、发送预算耗尽、前导清除、
graceful close 和更新兴趣失败的分支。原始成功写后的兴趣更新仍保留，只补充
队列结算后的最终更新，没有盲目关闭全部写事件。此审查是主会话内联自检，
不替代维护者独立审阅，也不宣称完整 #243 生命周期矩阵已闭合。

## 固定输入前后对照

[完整数据](data/tls_idle_interest_20261029_comparison.json)保留四个独立进程场景、
命令、/proc tick、逐线程 CPU、fdinfo、调用计数及原始输出摘要。
旧二进制来自 `60630875a2e1fe0c36d66e5d459b8e8040c0f69e`，SHA-256 为
`7cd245121138a344bd80d9d19a9f47137d8ebf9c6c1ca0761c9a9c3dc39a117a`；
修复二进制为 `19471c5a5fff52d58edf81bf1dd7ee773f492fe87b4a8d7ee7849eec6a64df28`，
测量时基于提交 358abee 的工作区，reactor.c/xmake.lua 摘要另存并在归档时核对。
原冻结文件没有被构建覆盖。Xmake 为 GCC Release，凭证、4 KiB 正文、双向 Zstd、
128-call/64-window、100 RPS、32 次预热和 3000ms 调用期限保持相同。

| 场景 | 旧 Server CPU / 秒 | 修复后 CPU / 秒 | 旧退出累计轮转 | 修复后累计轮转 |
|---|---:|---:|---:|---:|
| gate 中空闲 15 秒 | 14.99 | 0.59 | 2496527 | 15819 |
| 100 RPS × 30 秒 | 18.12 | 4.72 | 2425933 | 45650 |

CPU 为 `/proc/<pid>/stat` 的测量窗口差值（100 tick/s），不含窗口外构造和预热。
Reactor 计数是整个 Server 生命周期，空闲场景还含预热及 gate 释放后的 64 次
正常请求；不能将它们当作窗口内精确轮转率。修复后空闲 fdinfo 为 0x2019，
写兴趣已消失，读兴趣仍在；周期维护仍有成本，没有承诺零 CPU。

正常负载的前后请求均 3000/3000 成功；两个空闲场景在 gate 释放后各 64/64 成功。
普通负载成功 p99 为旧 32.256163ms、修复后 33.569541ms，不据此宣布尾延迟改善。
这是共享开发机上的各一次对照，未隔离 CPU/内存，也没有随机交错/多轮统计；
只将确定性回归、实际兴趣变化和该场景 CPU 差异作为本缺陷证据。

## 验证与发布边界

全部测试临时文件、凭证、缓存和日志只在工作区 build/，不写系统临时目录。
完成以下最终工作区验证；配置和命令仍走 Xmake 原入口，原测试名/参数/超时不变：

| 配置 | 实际通过 | 测试命令墙钟 |
|---|---:|---:|
| Clang Release | 12/12 | 37.336 秒 |
| GCC ASan/UBSan，启用泄漏检查 | 12/12 | 43.855 秒 |
| GCC TSan | 12/12 | 47.368 秒 |
| 恢复 GCC Release，全量 | 95/95 | 185.901 秒 |

12 个目标包括八个 TLS 目标、Reactor budget/fairness、真实嵌套调用及公开 V2 SDK；
均包含新的空闲兴趣检查，并保留十三个 TLS 账本场景、34 轮连接周转及十八个信任
快照场景。最终 GCC Release 二进制摘要与前后对照中的修复版本一致。
矩阵保存在 build/performance-20261029/dev179-matrix.json 和对应构建/测试日志；
归档数据含结果和原始日志摘要。内联自检核对 owner/观察锁、跨文件接管、队列
排空、错误清理和范围；没有改变既有静态分析告警基线。

旧基准与旧提交的 CI 不为新实现背书，新修复提交须单独核对远端检查。
本修复不证明 40k 容量、全部 TLS 内存硬上限、任意前导背压或完整
错误/取消组合；CTM1、服务目标/Attempt、平台及 V2 其余发布要求仍开放。
