# MessageService 停止回收的 producer 屏障

日期：2026-10-29；候选 `2.0.0-dev.180`；关联 #243、#241、#247。
基线为 `3823743a5f4aeb7bf469a0d15bf6f62f88cb2329`（dev.179）。
该基线远端检查精确核对 13/13 成功：[CI](https://github.com/MyBlackHole/c-trpc/actions/runs/40881697189)、
[RPC smoke](https://github.com/MyBlackHole/c-trpc/actions/runs/40881697096)。
它们不替代本次修复提交的检查，后者单独跟踪。

## 触发条件与根因

Runtime 的 Reactor 正常停止或因 epoll 错误退出时，MessageService 仍可能持有
运行中与排队中的 Zstd 任务。旧 `tr_message_service_reap_stopped()` 取得旧 owner
的停止租约、请求取消并接管 TaskExecutor 清理 owner 后，立即以参数 1 调用
`tr_async_operation_reap_stopped()`。这只证明旧 Reactor owner 已 join，尚未证明
producer worker 已停止；`tr_task_executor_adopt_cleanup()` 只移交完成 owner，
允许 worker 继续执行，不能充当 worker join 屏障。

操作池清空槽位后，worker 仍调用 `tr_async_operation_publish()`，真实返回
`TR_ERR_STALE`（-14）。原 MessageService 最后仍等待任务结束并以 OWNER_STOPPED
交付取消回调；既有测试只验证这项外部取消结果，遗漏了接受后的完成发布能力。
本次已确认的缺陷是 producer 尚存活时提前退役其槽位，使正常迟到交接被视为
内部错误；没有从这一现象推导已发生悬空访问、漏回调或预算泄漏。

## 修改与不变量

只在停止后接管路径增加已有 TaskExecutor join 检查：

1. 旧 Reactor 成功 join，恢复租约阻止重启/销毁；停止准入并请求在途任务取消。
2. 接管执行器清理 owner；worker 尚未退出返回 TR_AGAIN，join 失败返回 SYS。
   两种返回都保留服务、任务、操作槽、通知 fd 和租约，重试继续同一对象。
3. 全部 worker 退出且成功 join 后才回收异步操作池，随后由既有关闭路径交付
   唯一 OWNER_STOPPED/CANCELLED、销毁输出及任务槽、解绑事件与定时器并归还预算。
4. `executor_retiring` 表明此前已越过 join/解绑屏障并进入执行器部分销毁；重试
   跳过新增 join 检查，继续原先同步对象的剩余销毁步骤。

join 在 workers_alive 非零时直接返回 AGAIN，不在 owner 或 reaper 中等待压缩
执行。worker 发布操作结果后才在执行器锁内标记任务完成并退出，成功 join 因而
保证所有 producer 的最后发布已结束；操作池不存在随后再次发布的工作方。
新增路径不分配资源，不增加线程/队列/锁，不改变公开 API、ABI 或 wire。
正常 owner 关闭流程继续使用既有顺序；本次只修复停止后接管的静默证明。

## 回归如何区分旧实现

增强既有 `test_runtime_message_recovery`，只在该目标链接时包装 publish 并调用
真实实现，记录发布次数与错误；不替换调度、结果、操作池或压缩算法。
原真实 Runtime 测试已用算法门控暂停 Zstd worker，并接受第二条排队任务，
再触发正常/致命 owner 退出、owner join 失败、worker join 失败及解绑/销毁重试。
新增检查要求两条任务都完成真实 publish 且无失败；worker join 失败时两次
publish 已结束，但用户取消回调尚未开始。混合用例同时覆盖已外借输出、已完成
待交付、运行中和排队任务，四次 publish 全部成功，最终四项各回调一次。

修复前，Xmake 定向测试在 0.237 秒失败，日志记录四次 -14（混合用例两项、
随后致命回收用例两项），断言 `publication_failures == 0` 失败。修复后同一真实
路径与相邻三个目标 4/4 通过；原取消、唯一回调、输入释放、外借预算、恢复租约、
重入拒绝、部分销毁和 Runtime 重启检查保留。

## 验证与范围

最终 GCC Release 全量 95/95，通过时间为 Xmake 测试报告 201.112 秒（命令总耗时
202.745 秒）；最终 Runtime 回收目标连续 25/25。Clang Release、GCC ASan/UBSan
（含泄漏检查）与 GCC TSan 各 9/9；命令耗时分别为 17.049、19.581、18.780 秒。
[验证清单](data/message_reap_producer_barrier_20261029_validation.json)保留三个源码/配置
摘要、各配置与日志摘要及 25 次重复记录。定向目标为 `test_runtime_message_recovery`、
`test_message_service`、`test_message_service_partial_destroy`、`test_async_operation`、
`test_task_executor`、`test_runtime_threads`、`test_rpc_v2_sdk`、`test_rpc_record_stream`
及 `test_rpc_v2_nested_calls`，保留原有目标、default 名称和超时。
所有构建、测试、日志和缓存位于工作区 build/，
临时目录由 TMPDIR/TMP/TEMP/XMAKE_TMPDIR 指向 build/test-tmp，未向 /tmp 写入。

内联审查核对 MessageService → TaskExecutor → 异步操作池的实际顺序：构造回滚
尚无发布任务；RPC dispatch 的 stopped-reap 则先检查 submitted/pending/producer
与外部上下文引用。本次没有据此宣称其他调用方屏障或 #243 全矩阵已验证。
CTM1 共同版本协商仍待设计审阅，#241 的跨域总预算、#244/#245/#246、TLS-E、
平台/固定机器容量与 #239/#154 最终发布核对仍未完成，发布目标保持开放。
