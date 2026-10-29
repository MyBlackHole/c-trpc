# TLS 信任来源构造期快照修复

日期：2026-10-29，候选 `2.0.0-dev.178`。关联 #234、#243、#247。
修复 Client 信任目录在首次握手时读取，导致已创建 profile 的信任随文件变化的问题。
主会话内联执行，不使用子 agent。

## 已运行的失败证据

TLS-B 原计划明确“profile 创建时同步读取完毕，不保存路径”，但目录和系统默认
来源通过 OpenSSL 的延迟查找器注册。[OpenSSL 哈希目录文档](https://docs.openssl.org/3.0/man3/X509_LOOKUP_hash_dir/)
说明目录按需读取并缓存；这与构造期快照及握手 owner 的 I/O 边界不一致。

真实端点握手先复现两种相反偏移：

- profile 构造时目录只有 CA B，之后加入 CA A，旧 profile 接受 CA A 服务端，返回 TR_OK。
- profile 构造时目录有 CA A，之后删除文件和目录，旧 profile 无法验证 CA A，返回 -12。

原始日志：`build/performance-20261029/trust-directory-snapshot-add-red.log`、
`trust-directory-snapshot-remove-red.log`。两项回归没有先建立连接或预热该 profile 的
信任缓存，定位的是创建之后、首次握手之前的变化。

## 实现与兼容边界

新增内部 `tls_trust_snapshot` 模块，在 Client profile 返回前读取信任对象，装入
SSL_CTX 内存 store；不注册哈希目录或 URI 延迟查找器。适用于普通、连接组及 PEM
凭证更新创建的 Client profile。Server 的 client_ca_file 原本就是构造期文件加载，
本次不改该路径。公开结构、API/ABI 与 wire 不变，安装 SDK 仍为 11 个公开头。

显式文件/目录只加载这些来源；均为空时解析 OpenSSL 默认文件/目录及环境覆盖，
特权环境遵循 OPENSSL_issetugid 规则。缺失默认来源可跳过；已有来源损坏、显式
错误目录和不支持的特殊文件在构造时失败。新加入或删除来源须创建/轮换新 profile。
本修复不保证构造期间外部并发写文件形成跨文件原子事务。

目录按小写八位 hash 的 .0/.r0 组头，从零连续读取到首个缺口；支持冒号目录列表、
符号链接与 CRL。每个目录条目限单个对应 PEM 证书/CRL，并核对 subject/issuer hash。
不匹配的索引不被信任，允许目录中共存旧 hash 别名；损坏 PEM、混合对象和 FIFO
等特殊文件拒绝，避免急切加载扩大原哈希选择的信任集合。普通 CA 文件保留 PEM
证书/CRL 合集的加载，信任加载不会交互请求私钥口令。

文件由 openat+CLOEXEC+NONBLOCK 打开并核对普通文件；fd BIO 加有界缓冲，避免
逐行 PEM 读取变成逐字节系统调用。所有 fd、目录和暂存 PEM 对象在构造返回前
释放；入库对象由 SSL_CTX 持有。失败不发布部分 profile，OpenSSL 分配失败归一化
为 NOMEM。构造 I/O 发生在外部生命周期调用线程，握手不再访问这些信任路径。

这会把目录 I/O/解析成本提前，完整目录快照也可能比首次按需加载使用更多构造期
内存；未给出吞吐改善或总 RSS 硬上限保证。Xmake 显式新增一个生产单元，当前应为
55 个生产 C 单元；真实 GCC Release 编译数据库已逐项核验 55 个命令。
固定 Clang 分析仍以本次提交的远端结果为准，不能只靠构建成功确认覆盖。

## 回归与所有权验证

原 `test_tls_openssl` 中新增 18 个场景，目标名、参数与 120000ms 超时保留：

- 显式与系统默认目录，各自覆盖创建后加入 CA、删除 CA/目录的真实握手。
- 多目录、文件/目录重复来源、符号链接、旧 hash 别名共存；删除来源后仍完成握手。
- 不匹配索引不受信、序号缺口不被越过；FIFO、坏 PEM、多对象哈希文件构造拒绝。
- fd BIO、缓冲 BIO、证书入库、目录复制失败的 NOMEM、空输出、fd 回滚及成功重试。
- CRL 入库失败回滚与重试；删除 .r0 后，通过测试会话的 CRL_CHECK 验证真实握手。

故障注入只在该测试可执行文件中通过链接包装生效，不给库新增生产故障开关。
旧的 OpenSSL 资源账本 13 场景也通过定向验证，profile 释放前的全局基线和最终
清理归零仍成立。原始输出见 `trust-snapshot-*-green.log` 与
`trust-snapshot-final-gcc-directed.log`。负面测试目录使用进程唯一名称，红测遗留
目录已归档到工作区忽略目录，没有在 /tmp 写入。

## 最终验证与剩余范围

最终源码的本地验证：

| 配置 | 构建 | 测试 |
|---|---|---|
| GCC Release | 全目标通过 | 串行全量 95/95，188.300 秒 |
| Clang Release | 全目标通过 | 八个 TLS 目标 8/8 |
| GCC ASan/UBSan，启用泄漏检查 | 全目标通过 | 八个 TLS 目标 8/8 |
| GCC TSan | 全目标通过 | 八个 TLS 目标 8/8 |

每种配置均核对新增 18 个快照场景、13 个独立资源账本场景和 34 轮同 profile
周转输出。原始日志为 `build/performance-20261029/dev178-*-{build,test}.log`，
矩阵汇总为 `dev178-matrix.json`。没有对失败命令盲目重跑：首次全目标构建暴露
TCP 专测独立源对象闭包遗漏新单元，补齐 Xmake 第二处显式列表后重新验证；
原失败摘要保存在 `trust-snapshot-build-closure-red.txt`。

安装 SDK 的 11 个头在 GCC/Clang C11、G++/Clang++ C++11 下独立编译，四种真实
TCP 消费端通过；静态 Zstd 符号唯一定义、安装包及源码归档许可逐字节一致。
真实 Xmake GCC Release 编译数据库核验 55 个生产命令，包含新单元。
日志为 `build/release-readiness/sdk-validation.log`、`dev178-compdb.log`。
`git diff --check` 通过；固定 clang-format 18.1.8 检查五个变更源文件，0 失败。

实现提交 `8f49481845dcd1411ac7b79b097287bf7f3fe470` 已推送到 Draft PR #333，
精确 head 的远端检查 **13/13 全部成功**：
[CI](https://github.com/MyBlackHole/c-trpc/actions/runs/40832468616) 8/8、
[RPC smoke](https://github.com/MyBlackHole/c-trpc/actions/runs/40832468602) 5/5，
包含 GCC 11 和 Clang 14 静态分析、新单元编译数据库覆盖、完整功能矩阵及安装消费端。
后续文档提交的检查单独核对，不借用本实现提交结果。

另取回该 CI 的实际分析产物复核：GCC 11 与 Clang 14 均分析 55 个生产单元，
新 `tls_trust_snapshot.c` 的分析退出为 0、诊断为 0。全项目仍分别有 1 条 GCC、
8 条 Clang 已登记基线诊断，门禁相同不等于全项目零告警。PR 运行实际检出合并
参考 `8f87d64514d2964918f815dd5d0e26c2cec59fdc`，其父提交含实现 head 8f49481；
Git tree `6dad000b111877ff2d405e19eedd61ad0cb95338` 与实现 head 完全相同，
因此源码树一致。原产物及身份核对保存在 `dev178-ci-artifacts/` 和
`dev178-ci-merge-identity.json`，不把关联 head 与实际 checkout 身份混写。

小时级 TLS/Zstd 诊断单独按旧提交 97e106c 记录，不能当作本次修复的精确提交结果。

最大/恶意输入、全局资源预算、持续握手/取消、长期负载、目标平台与 V2 其余功能
验收仍开放；不关闭 #234、#243、#247 或完整发布目标。
