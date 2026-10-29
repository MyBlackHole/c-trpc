# RPC V2 嵌套调用的 mTLS 与压缩组合验证

日期：2026-10-29。关联 #243、#234、#235、#247。版本：`2.0.0-dev.172 -> 2.0.0-dev.173`。

## 需求与实现

原 `test_rpc_v2_nested_calls` 只通过明文、未压缩连接运行 A→B→A。
本次复用该目标，验证已经集成的异步方法授权、父上下文、reply producer、TLS 身份与
MessageService 能在真实 TCP 嵌套调用中共同推进。所有生产源文件及公开头文件保持不变。

测试覆盖两种子调用提交时机：在父 OPEN work 中提交，以及收齐父请求的 HALF_CLOSE
work 中提交。每种时机分别运行明文/原文、明文/Zstd、mTLS/原文、mTLS/Zstd，共八个场景。

- 每个请求携带 256 字节 `Q`，叶节点响应携带 256 字节 `R`，逐字节验证。
  Zstd 场景显式设置双向方法压缩许可，使用公开压缩发送与回复接口。
- mTLS 场景要求双方证书验证，并在每次 OPEN/MESSAGE/HALF_CLOSE work 中检查
  `spiffe://c-trpc.example/client` URI SAN。明文场景检查身份为空。
- 半关闭在压缩任务尚未提交时可返回 `TR_AGAIN`。夹具记录已经接受的请求，只在后续
  WRITABLE 中重试半关闭，避免重复发送。首轮新增测试在旧夹具的立即半关闭断言失败，
  原因是夹具未处理这一公开背压合同，不是生产缺陷。
- OPEN 场景保留原有早回复时序：子调用可能在父请求正文到达前完成，不能要求父一定
  收到正文；HALF_CLOSE 场景则要求三个 Server Call 各收到且只收到一条完整请求。
- 每个场景验证三个 Client Call 各有一次终局、两个 Server 分别终结两次与一次，
  两份父上下文和两份 producer 全部释放，Client/Server 排空与销毁全部成功。
  原有 Client callback、Server observe/authorize/terminal 的跨 owner 禁入断言保留。

Xmake 只为既有目标补入已有 `tls_test_identity.c`，不增改测试目标、超时或 CI 检查标识。
身份 fixture 和所有日志、临时文件均保存在工作区 `build/`，不写入 `/tmp`。

## 验证证据

恢复基线为本地集成提交 `02c78a50ecfe38852e9ddb327f2cf49c4a460936`：
原始 dev.172 的 GCC Release 全量构建与 94/94 注册测试通过，用时 160.398 秒。
新增八场景的 GCC Release 定向测试通过。

身份传递变异：临时把 `tr_dispatch_v2_work()` 构造 request 时传入的 peer identity
置空，真实测试在 `request->peer_identity.authentication` 断言失败；在 `finally` 中
逐字节恢复原生产源码后，八场景全部通过。生产源码没有遗留变异。

GCC Debug 全量构建与 94/94 注册测试通过，用时 192.652 秒。
其余全量矩阵正在执行，最终结果尚待补充；不得把这条记录当作完整矩阵通过证明。
状态模型、错误分类、组件回放、轨迹探索脚本分别为 28/28、6/6、19/19、15/15 通过。
命令均采用 CI 规定的 `python3 scripts/test_*.py -v` 直接脚本入口。

本机为 Arch Linux、Linux 7.2.11 x86_64、GCC 16.2.1、Clang 23.1.4、
OpenSSL 3.6.5 和 Xmake 3.1.2。它不能替代 Ubuntu 22.04、OpenSSL 3.0 与固定 CI 工具链。
完整 configure/build/test、变异验证和模型日志位于忽略目录 `build/release-readiness/`。

## 范围与后续发布条件

本次只补齐嵌套调用的八个真实网络组合，不能据此关闭 #243/#234/#235/#247 或发布 V2。
持续共享调度/满队列、公平性、取消/超时的更多组合及全部调用方资源屏障仍需验证。
TLS-E 仍缺 OpenSSL/内核完整资源账本、持续洪峰、长稳和目标平台验收。
现有 40k 性能诊断含发生器丢弃、迟到与 CONTROL TX 池压力，不能算容量通过。
服务目标/Attempt（#244）、类型契约/生成桩（#245）、后端决策（#246）仍按
[V2 总账](v2_remaining_work.md) 推进，不能因本地测试全绿缩减原发布范围。

生产路径没有新增分配、复制或同步，无产品运行时性能影响；测试增加八场景与一次
证书 fixture 生成。撤销本次提交即可移除扩展测试、目标 fixture 和版本记录，
不涉及协议、数据或 SDK 迁移。
