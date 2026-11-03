# 普通 RPC 门面切换到 V2（2026-10-18）

## 范围

依赖 #279 的工作域解耦与其后的 V2 一元自动发送候选。本切片将普通 SDK 的
构造、流式与一元入口直接接到同一记录内核；不是增加一个新的可选入口。
项目未发布，按 #240 允许破坏 API，不做旧签名或线协议静默回退。

- `tr_client_create(config, methods, count, out)` 和
  `tr_server_create(config, methods, count, out)` 直接执行原 V2 构造实现。
- `tr_client_call_start(..., options, callbacks, out)` 与
  `tr_client_unary_call(..., request, options, callbacks, out)` 共用 V2 调用槽、
  请求发送进度、弱能力和唯一终局模型。保留 V2 能力类型，不适配 V1 结果回调。
- 方法在构造时复制冻结。空表/零条是错误，不表示选择 V1；方法不能随后以旧注册
  接口追加。普通头不再声明旧注册、旧 `_ex` 调用或旧两参数构造。
- 移除 `_create_v2`、`tr_client_v2_call_start`、`tr_client_v2_unary_call` 导出名字，
  不用宏或额外包装维护第二个公开入口。静态 SDK 调用者必须重新编译。

## 连接组不能成为 RPC 回退入口

已有连接组能力由 `tr_client_create_transport(config, out)` 与
`tr_server_create_transport(config, out)` 显式创建，继续使用原有 Group API。
传输工厂与旧内部门面复用资源布局；它不声称已经完成所有旧内部资源的拆除。
成功构造后固定为 transport_only，公开 RPC connect/listen 在产生套接字或
监听副作用前返回 UNSUPPORTED；RPC 调用/旧内部注册也不接受该对象。
配置中的 RPC 拦截器及 Client 自动重连不能被静默忽略，构造时明确拒绝。
构造失败的非空部分对象仍按既有 destroy 重试合同交还调用者。

## 回归与调用者迁移

公开 SDK 的四基数、元数据、取消、期限、异步生产者及停止回收测试现在直接使用
普通函数名；旧 `_v2` 工厂不再作为测试入口。增加空方法表拒绝检查。
连接组专测改用传输工厂，补充拒绝 RPC connect/listen、失败不修改端口、
拒绝无效 RPC 配置的断言。原有真实 DATA、排空和销毁场景继续保留。

Echo 两个独立程序迁移到同一默认 V2 路径。客户端在 message 回调复制到有界
应用缓冲，只有 terminal 决定成功；超长本地响应不得截断后伪报成功。
销毁失败时保留静态回调参数和同步对象，不能把失败当作回调已经退出。
独立进程迁移测试曾复现销毁首次返回 AGAIN、重试才成功。示例由外部控制线程
以单调钟五秒总预算重试；其他错误或超时仍失败，CLI 桩另覆盖一次 AGAIN 后成功。
原 gettext 文本与语言选项保持不变，语言测试桩改用新签名和两阶段结果交付。

R11 移除了 Client V1 门面。仍有价值的 V1 Server/Endpoint 回归与 `bench_rpc`
通过 `tests/rpc_v1_test_client_fixture.*` 直接创建低层 V1 Endpoint；Server 旧入口
仍使用私有 `src/facade_legacy_internal.h`。这个头不安装，也不提供 Client V1 名称。
这些检查不计作默认 V2 覆盖；旧基准吞吐不能为 V2 性能背书。
旧 RPC 引擎及部分公开低层类型/能力声明尚未删除，完整静态库仍可带入这些对象；
本切片不是“V1 源码已清零”或“整个 SDK 不再链接旧对象”的证明。

## 验证要求

保持原 Xmake 源列表、测试目标、超时、格式规则和静态告警门禁。本切片应通过
精确提交上的全量原生 CI、SDK 安装消费、独立进程 Echo、双静态分析和源码格式。
本地直接编译是补充证据，不替代 Xmake；实际运行身份和结果记录在实现 PR，
不能继承 #279 或上一轮补丁的旧绿色结果。

后续删除旧内核时须逐项迁移其故障覆盖；重连、TLS、网络压缩、Codec 类型化与
性能验收不因为默认构造切换自动完成。
