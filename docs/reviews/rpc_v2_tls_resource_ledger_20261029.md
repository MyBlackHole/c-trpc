# TLS OpenSSL 分配账本与构造回滚

日期：2026-10-29。关联 #234、#241、#247；版本 `2.0.0-dev.174`。
本记录推进 TLS-E 的资源证据，完整发布验收仍未完成。

## 已复现的缺陷

新增独立目标 `test_tls_resource_ledger`。测试先证明缺少分配计数时自检失败，
再接入 OpenSSL 内存回调。在原有实现上发现两个独立问题：

1. 带 memory budget 构造 session，预留成功把局部状态改为 `TR_OK`；其后的
   session `calloc` 或 `SSL_new` 失败沿清理路径返回成功，但输出仍为空。
   第 1/2/3/4 次 OpenSSL 分配失败均触发空 session 断言。
2. 自定义 socket `BIO_METHOD` 与名称由进程级 once 单例持有，无释放域。
   profile/session 释放并执行测试进程的 `OPENSSL_cleanup()` 后仍有 138 字节、
   两个 OpenSSL 请求块。只修返回状态后残留仍存在，确认两项根因相互独立。

原始红灯日志保存在工作区忽略目录 `build/performance-20261029/`：
`openssl-ledger-red.log`、`openssl-ledger-cleanup-red.log`、
`openssl-ledger-after-status-fix.log`。日志和生成证书不提交。

## 生产修复与所有权

预算预留完成后，将后续构造失败的默认状态明确设为 `TR_ERR_NOMEM`。
失败路径保留原调用方 fd，释放已构造对象，归还已预留的包装对象预算。

每个不可变 profile 拥有自己的 socket BIO 方法表；session 取得 profile 强引用，
先销毁 `SSL`/BIO，再归还该引用，最后一个 profile 引用释放方法表。
调用方可在 session 创建成功后归还自己的 profile 引用。方法表构造失败释放部分
profile，不污染下一次创建。生产库不安装全局分配钩子、不调用全局 OpenSSL 清理、
不注册可能跨越库卸载寿命的退出回调。

自审增补 160 次 profile 轮换回归，先捕获每次重建方法表都申请新类型编号的问题。
类型编号改为仅申请一次的无堆静态状态，各 profile 方法表复用该类型。
清理后仍无 OpenSSL 请求块残留，轮换超过类型编号范围也可继续创建。
方法表释放与类型编号含义见 [OpenSSL BIO 方法契约](https://docs.openssl.org/3.0/man3/BIO_meth_new/)。

每个 profile 增加一个方法表和一个指针；每个 session 增加一个指针、一对引用计数
操作。session 增大的 `sizeof` 仍在分配前纳入既有包装对象预算；该预算不包括
profile 和 OpenSSL 内部堆。尚无该修改的吞吐/尾延迟性能结论。

## 账本口径与测试场景

测试通过 `posix_spawn` 启动独立 exec 子进程，在第一次 OpenSSL 分配前安装
malloc/realloc/free 三种回调。对齐头保存请求大小，mutex 保护计数，realloc
失败保留原指针和账目。自检覆盖最大基础对齐、扩容、注入失败及清零释放。
每个子进程用 `OPENSSL_INIT_NO_ATEXIT` 初始化，最后显式清理且不再初始化。
这些限制遵循 OpenSSL 的[内存回调契约](https://docs.openssl.org/3.0/man3/OPENSSL_malloc/)
和[全局初始化/清理契约](https://docs.openssl.org/3.0/man3/OPENSSL_init_crypto/)。

- 同时保留 1/4/16 对真实 TCP TLS 1.3 mTLS 会话，每个方向传输 64 KiB，
  按 16 KiB 调用推进；小发送缓冲触发非阻塞等待，检查全部正文。
  握手前释放调用方 profile 引用，传输后释放 session、检查 fd 仍归调用方。
- 注入 session 创建的第 1/2/3/4/8 次 OpenSSL 分配失败；另分别包装本库 session
  和 BIO 用户数据 `calloc`，以及 profile 的 `BIO_meth_new`。检查失败状态/空输出、
  fd 保留、预算回滚、再次构造成功，以及全局清理后的请求字节/块归零。
- profile 连续创建/释放 160 次，验证凭证轮换不消耗新的 BIO 类型编号。

账本 `profile_bytes` 包含两个 profile 构造后仍存活的 OpenSSL 全局初始化/凭证
请求字节，不是两个 profile 的独占大小。`peak_bytes` 包含该基数和活动会话。
`managed_peak_bytes` 是本库 session/BIO 用户数据预算峰值。
`socket_capacity_bytes` 只是各 fd 的 `SO_SNDBUF + SO_RCVBUF` 配置容量之和；
内核自动调节、实际驻留、skb/协议元数据不能由此推算。
头部、libc 元数据、RSS、其他本库对象和内核驻留均不计入 OpenSSL 请求字节。

## 当前本机观察值

Linux x86_64，GCC 16.2.1、OpenSSL 3.6.5。

Clang 23.1.4 Release、GCC ASan/UBSan（开启泄漏检查）、GCC TSan 各运行八个
TLS 目标并通过，Xmake 测试命令墙钟分别为 23.948、28.407、29.108 秒。
每种配置均包含全部 12 个独立资源场景。最终 GCC Release 构建通过、串行全量
测试 95/95，测试命令墙钟 156.464 秒。原始矩阵保存在
`build/performance-20261029/dev174-matrix.json` 及对应日志。
安装 SDK 与远端精确 head 证据如下，不借用 dev.173 的检查结果。

安装 SDK 恰有 11 个公开头；GCC/Clang C11、G++/Clang++ C++11 各自逐头检查
并编译、运行安装目录外的真实 TCP SDK 消费端，四者全部通过。静态 Zstd 唯一定义、
安装与源码归档的许可逐字节校验通过。日志见
`build/performance-20261029/dev174-sdk-driver.log` 和 `build/release-readiness/sdk-validation.log`。

包含本修复与 dev.175 测试同步修复的精确提交
`f1c0b0f8d8a394bebaf0a0885ff3657b4ccbc2ba` 已推送至 Draft PR #333，
GitHub 检查 13/13 全部成功：[固定工具链 CI](https://github.com/MyBlackHole/c-trpc/actions/runs/40821469302)
8/8、[RPC smoke](https://github.com/MyBlackHole/c-trpc/actions/runs/40821469239) 5/5。
包括 GCC 11 与 Clang 14 静态分析；该证据只适用于该精确 head，不覆盖后续未验收修改。

先前误用未指定 `-j1` 的并行全量入口得到 94/95，暴露既有
`test_reactor_fairness` 的完成标记先于计数发布的测试竞态。
串行通过并不能消除该竞态，后续单独修复并验证，不以更改超时掩盖。

| 同时连接对数 | profile 后请求字节 | OpenSSL 请求峰值 | 包装预算峰值 | socket 配置容量 |
| --- | ---: | ---: | ---: | ---: |
| 1 | 557480 | 780127 | 144 | 278528 |
| 4 | 557480 | 1198115 | 576 | 1114112 |
| 16 | 557480 | 2882759 | 2304 | 4456448 |

这些数值来自当前本机 Release 场景，不是跨版本保证或资源硬上限。
各场景清理后 OpenSSL live 请求字节/块均为零，本库预算 current 也为零。

## 内联审查

遵循 `code-review-checklist` 与 Linux 用户态所有权/错误恢复规范，未使用子 agent。
架构检查确认方法表释放域属于 profile，BIO 销毁发生在最后一次 profile release 前；
逐行检查覆盖预算预留、profile retain 失败、SSL/BIO/用户数据分配失败、SNI 配置失败
及正常析构。类型编号随轮换耗尽的审查发现已通过先红后绿的 160 次回归修正。
跨文件检查确认 profile generation 仍要求唯一可变引用，安装 SDK 与 wire 无新增项，
测试包装和分配钩子仅属于新测试可执行文件。范围检查确认没有恢复已撤销的性能候选。
这轮审查不宣称 OpenSSL 全部潜在分配点或完整网络安全矩阵已覆盖。

## 尚未完成的发布要求

最大证书链/恶意握手输入、持续握手与取消洪峰、长稳运行、分域及全局资源预算、
内核实际驻留、目标平台矩阵和固定基准机性能仍未验收。
本机 OpenSSL 账本清零不能关闭 TLS-E、#241、#247 或发布总目标。
相关性能诊断及已撤销候选见[活跃分派实验记录](../superpowers/plans/2026-10-29-rpc-active-dispatch-progress.md)。
