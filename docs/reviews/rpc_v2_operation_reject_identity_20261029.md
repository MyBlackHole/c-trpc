# 异步预留撤销的引用关联校验

日期：2026-10-29；候选 `2.0.0-dev.181`；关联 #243、#241。
基线为 `5fc327a9d058540b6901c0ffe5ea25d755094966`（dev.180），精确远端检查
13/13 成功：[CI](https://github.com/MyBlackHole/c-trpc/actions/runs/40884947657)、
[RPC smoke](https://github.com/MyBlackHole/c-trpc/actions/runs/40884947582)。当前修复提交的
远端检查单独核对，不使用基线或其他文档 head 的结果替代。

## 反例与已确认边界

内部异步操作池的 `reject(pool, operation, producer_ref)` 应仅撤销对应预留。
旧实现先查 operation 的槽与 RESERVED 状态，再独立验证 producer_ref 的
instance/generation/slot/token 有效，却没有确认两次查找指向同一项。

容量为 2 的同一池预留 A、B 后，两者都是 generation=1、token=1，但 slot
不同。用 operation A 与 B 的有效 producer 引用撤销时，旧实现返回 OK，
清空 A 的槽却清空 B 的引用：A 的引用对应已释放槽，B 的槽仍保留但能力被消费。
这破坏唯一交接与拒绝时不消费无关操作的约束，并可能使调用方无法正常撤回 B。

本次通过内部 API 的最小反例证实此条件。当前 TLS provider、RPC authorize
和 MessageService 的生产撤销调用点均传入成对保留的 operation/ref，未发现
它们实际混用，因此不据此宣称已发生线上泄漏、远端利用或这些调用方的事故。

## 修改和所有权

producer_ref 查找返回其实际槽，在同一原有 pool mutex 下与 operation 的槽
比较；不同返回 STALE，进入任何清空/消费之前退出。lookup 仍负责 instance、
代次和 token 校验；已有 RESERVED 状态与 owner 校验的先后顺序保持。
正确的 operation/ref 对继续撤销并清空引用；无效状态或失效引用保持原错误路径。

比较的是同一池内查找得到的槽地址，整个判定与提交都在原锁内，槽不能在两次
查找之间被并发回收/复用。不新增分配、队列、线程或锁，不改变结构布局、公开
API/ABI 或 wire。内部头补充关联与失败不消费的注释；不会将 weak identity
或复制 ref 当作新的存储所有权。

## 验证

在原 `test_async_operation` 增加同池双预留反例，刻意使 generation 与 token
相同、slot 不同，排除依赖“其他字段恰好不同”的偶然拒绝。双向错配都须返回
STALE，逐字段确认两项引用不变，stats 仍有两个 RESERVED；然后分别用正确配对
撤销、关闭/销毁池，最终预算为零。保留原容量、旧代次、接受/发布/消费/退役、
取消并发、停止回收和 owner 交接检查，不新增测试目标或调整原超时。

旧实现的 GCC Release 回归在 0.185 秒因第一个错配未返回 STALE 失败（测试报告
0/1）；修复后操作池及三个真实调用方的四个目标通过 4/4。
Clang Release、GCC ASan/UBSan（含泄漏检查）和 TSan 各 11/11。
最终 GCC Release 完整复验 95/95，Xmake 测试耗时 185.701 秒，其中
RESET 目标为 81.936 秒。源/头/测试/配置四个摘要与矩阵启动时一致；
[验证清单](data/operation_reject_identity_20261029_validation.json)记录三组 11 目标、
首次失败、定向诊断、完整复验与原始文件摘要。所有测试、编译临时目录和日志位于工作区 build/。

首次 GCC Release 全量为 94/95：唯一失败是 `test_stream_reset` 在既定 120 秒
上限终止（120.119 秒）。RESET 测试/传输源码未改，ELF 符号确认不链接本次
修改的 `tr_async_operation_reject`。当时主机存在大量并行编译；这只是相关观测，
没有完成耗时波动的根因归属。

首次定向复查的观测脚本把 `ps -C cc1` 在无匹配进程时的返回 1 当作错误，
提前结束，未产生测试结论；修正观测脚本后，同一 ELF 摘要下定向复查 1/1，
耗时 89.756 秒。18 个采样中 16 个未观察到 cc1，另外两个分别有 3/4 个；
没有声称整个窗口完全隔离。40k 输入、原 default 和 120000ms 超时均保持。
原始全量超时与无效观测均保留，不能只选择成功运行或据此宣布 #247 容量达标。

审查核对 TLS provider reserve 失败/解锁失败、RPC authorize executor 拒绝、
MessageService submit 拒绝的调用点，均保留本次操作自身的 producer ref；新增
检查不会把正确回滚变成错配。这里仅核对相关调用链，没有据此证明所有异步调用
方的资源/回调屏障、跨域总预算或 #243 全矩阵完成。

CTM1 共同描述协商仍待设计审阅，服务目标/Attempt、类型工具与桩、后端等价
网络比较、TLS-E、固定基准机容量、平台与最终发布核对仍未完成；发布目标保持开放。
