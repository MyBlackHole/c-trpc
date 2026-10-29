# GCC Release 生产 include 依赖快照

日期：2026-10-30；关联 #248。此记录是当前构建配置的依赖证据，不定义允许边界。

## 采集口径

基于 `codex/release-readiness-20261029` 的提交
`4bc86551101c0e62704b9bd9608fcbceac9a0fce`，Linux x86_64、GCC 16.2.1、Xmake
3.1.2，配置为 `trcore` / Release / `crc32c_portable=n`。先用项目的
`check_compile_commands.py` 导出并校验当前 Xmake 数据库：55 个生产 C 翻译单元，数据库
共 161 条记录，SHA-256 为
`cce6abef1906f6d49d59a0fb09fd1d74da837c3cee53a7b97268bf49a4fbee20`。

随后逐条沿用数据库内的编译器、工作目录和参数，仅移除 `-c`、`-o <object>`，追加
`-fsyntax-only -H`。解析 GCC 实际启用的 include 树，只保留仓库内 `src/` 与
`include/tr/` 的直接边。完整边、来源翻译单元、重复包含次数和目录分组保存在
[JSON 快照](data/module_dependency_inventory_20261030.json)。快照包含数据库摘要与构建身份，
不包含本机绝对工作区路径。

## 观察结果

55 个生产翻译单元得到 389 条唯一的仓库内直接 include 边；按当前目录分组有 249 条
跨目录边、140 条同目录边。目录只用于呈现，不等同于已经批准的逻辑模块，跨目录数量也
不是违规数。

确认 Reactor 对 TRP 的耦合是直接编译依赖：`src/execution/reactor.c` 直接包含
`src/transport/protocol/parser.h` 和 `wire.h`，`src/execution/reactor.h` 直接包含
`src/transport/protocol/frame.h`。连接组适配、RPC、Runtime 等目录间也存在实际头文件边；
逐条清单见 JSON，不把它们预判为错误，因为组合根和合法适配器可以跨组件依赖。

## 验收边界

现有 `check_compile_commands.py` 验证生产源文件覆盖及 Xmake 实际编译参数；它不比较允许的
模块依赖图，也不检查私有符号/布局越界或受限链接。此快照只覆盖当前 Linux GCC Release
激活的预处理分支，不包含其他平台/工具链条件，也不表示符号调用、运行时回调、对象布局
访问或生命周期方向。

因此，本证据确认 #248 所需的现状图可以从真实构建导出，但不能关闭 M1/M6。仍需定义由谁拥有
模块状态、允许哪些编译/符号边，并加入跨模块负向门禁与受限链接测试；在策略获批前，不把
目录边自动标成允许或拒绝。


## 对象间全局符号引用

对同一编译数据库对应的 55 个生产 `.o`，使用 GNU Binutils `nm` 2.47 分别收集全局定义与
未定义符号，并按符号名匹配到其他生产对象。得到 786 个去重后的“消费者对象/未定义符号”
配对、170 条唯一对象引用边，其中 128 条跨目录分组；没有发现一个未定义符号对应多个
生产对象定义的歧义。具体对象、符号和提供者见[符号图 JSON](data/module_object_symbol_inventory_20261030.json)。

`reactor.c.o` 明确引用 `frame.c.o`、`parser.c.o`、`wire.c.o` 的符号，和前述直接 include 图
相互印证。该图只覆盖编译为全局符号的对象间引用，不记录引用点/调用次数，不是运行时调用图；
静态函数、函数指针/回调、共享结构字段、条件未启用的实现及链接后的可达性仍需其他检查。
目录计数仍不是违规分类，也没有替代 #248 所需的模块策略和负向门禁。

## M3 Reactor/TRP 状态与回归测试映射

只读审计 `src/execution/reactor.h`、`reactor_internal.h` 与 `reactor.c` 后，确认 TRP 耦合不止是头文件边：

- `reactor.h` 公开内部 `tr_frame` 回调、TRP type/flags/stream/message 参数、frame 计数、最大 payload 限制，以及 RX/TX buffer 与 TX item pool 配置；`reactor_internal.h` 还包含 GOAWAY、RESET、REFUSED、DATA 帧发送及协议接收 hold 操作。
- `reactor.c` 的每连接状态直接持有 `tr_parser`、协议帧 callback、TX 消息/帧游标、控制优先队列、生命周期与 RESET 内嵌发送槽。RX 负责 parser prepare/produce 和 frame dispatch；TX 负责 wire header/CRC、分片、部分写入延续、控制帧越过 DATA 的限制及发送项回收。
- `src/io/byte_endpoint.h` 已提供 TLS 握手、read/write、WANT 方向及 close_notify 能力；其契约要求 `TR_AGAIN` 重试期间保持同一缓冲区和长度。它可作为协议层的字节 I/O 基础，但当前仍由 Reactor 执行和保存 endpoint 状态。

M3 抽离时至少要保持以下已测试行为。这里是测试映射，不表示本节重新运行了这些测试：

| 不变量 | 现有回归目标 |
|---|---|
| 跨连接共享的每轮 RX/TX 字节及 dispatch 限额；极小预算下超过一个 epoll 批次仍轮转进展；EAGAIN 后允许等待 | `test_reactor_budget`、`test_reactor_fairness` |
| 一旦帧已部分写出，必须先完成该帧；未开始的合规控制帧可按规则越过 DATA；旧控制 FIFO 与跨轮 DATA 进展有上限 | `test_tx_priority` |
| 普通控制/DATA 池恢复通知有界、一次性，并与 TX 分派共享公平预算 | `test_reactor_control_ready` |
| TX_DRAINED 固定命令前沿，且等待普通队列及 GOAWAY/RESET 预留发送完成；关闭与槽位复用不误通知 | `test_reactor_tx_drain`、`test_stream_reset` |
| 停止或 fatal owner 退出时，已接受 payload/命令各释放一次；失败连接不留下排队帧或池占用 | `test_reactor_stop_full`、`test_reactor_fatal_exit`、`test_reactor_budget` |
| TLS 的 read/write WANT、稳定重试参数、排空与关闭仍按 ByteEndpoint 生命周期处理 | `test_rpc_v2_tls_reactor`、`test_tls_resource_ledger` |

上述映射说明 M3 不能只把 `parser.c` 或 TX helper 改放目录。协议后端需要成为帧解析、TX 队列、优先级/终止预留及帧级诊断的唯一状态所有者；Reactor 需经窄的 owner-scoped 字节进度接口提供 fd/TLS、每轮 I/O 配额、ready/timer/取消和连接生命周期。接口还必须定义异步发送命令的所有权转移、命令顺序前沿、关闭回调静默及 TX 容量恢复，否则现有无额外线程的快速路径、TX_DRAINED、部分写入和资源回收无法证明保留。

这仍是审计结论，不是已批准的 API 设计。没有更改产品代码，也没有重新运行上述测试；实现前须审阅具体接口、逐项迁移调用方，并用受限链接证明不含 TRP 的 Reactor 对象可独立验证。
