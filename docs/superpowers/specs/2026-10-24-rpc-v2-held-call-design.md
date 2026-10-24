# RPC V2 可持有 Call 引用设计

**状态：** 已批准的开发切片；关联 #243、#240。  
**基线：** 集成候选 `c873e18c20c167946cd6d72e7de70424664e9ab0`。  
**范围：** 只扩展普通 V2 Client 的 Call 句柄生命周期，不改变线协议、Server work/producer 和现有 NONE/ZSTD 数据路径。

## 问题

`tr_rpc_v2_call` 是由 Client 槽位、调用代次组成的弱能力。它可复制，但不延长调用槽或 Client 生命周期；终局回调结束并且 Channel 接管终止帧后，槽位可立即退役。应用不能仅把该值保存到回调之后，再安全地查询结果或取消一个仍在进行的调用。

异步应用需要区分借用视图和拥有型引用。拥有型引用应保存有限的调用结果，并在活动阶段提供取消入口，同时明确阻止底层存储被释放。它不得保留发送能力，也不能让已终止的调用重新进入协议状态。

## 目标与非目标

目标：

1. 现有 `tr_rpc_v2_call` 继续是可复制的弱句柄，既有 ABI 布局和 send/half-close/cancel 行为不变。
2. 增加独立的 `tr_rpc_v2_call_ref`，由仍有效的弱句柄显式取得；引用允许跨越任意用户回调边界。
3. 引用能从任意普通线程读取一次选定的终局快照，并在调用尚未终局时请求合作式取消。
4. 每个 Call 至多有一个当前存活的拥有型引用。该限制使能力为唯一、可检测的 token，不增加引用对象池或无界分配。
5. 终局后，网络/协议绑定照常回收；被引用的固定槽只保留状态快照，直到引用释放。
6. 未释放引用时，Client destroy 可以按现有契约停止 Runtime 和交付 shutdown 终局，但不得释放被引用的 Call/Client 存储；返回 `TR_AGAIN`，release 后可重试销毁。
7. 所有槽位和引用状态均纳入现有固定 Client/Call 容量，不新增线程、队列、堆分配或等待循环。

非目标：

- 不允许引用发送 MESSAGE 或 HALF_CLOSE；不提供远端副作用回滚。
- 不增加多个独立引用、引用克隆或自动共享所有权 API。应用若要跨多个组件共享，必须由应用层同步地转交同一唯一引用，并只释放一次。
- 不延长 Client 销毁期间的停机 deadline，也不替代外部 Client 生命周期串行化。
- 不实现父子调用树、派生 deadline、跨 Reactor 等待图或通用异步 executor；这些仍属于 #243 的其他验收项。

## 公开 API

在 `include/tr/rpc_v2.h` 增加单独的值类型和结果快照：

```c
struct tr_rpc_v2_call_ref {
	uint64_t _private[4];
};

struct tr_rpc_v2_call_result {
	enum tr_rpc_status status;
	enum tr_rpc_v2_delivery delivery;
};

int tr_rpc_v2_call_hold(struct tr_rpc_v2_call call,
			struct tr_rpc_v2_call_ref *out);
int tr_rpc_v2_call_ref_result(struct tr_rpc_v2_call_ref ref,
			      struct tr_rpc_v2_call_result *out);
int tr_rpc_v2_call_ref_cancel(struct tr_rpc_v2_call_ref ref);
int tr_rpc_v2_call_ref_release(struct tr_rpc_v2_call_ref *ref);
```

私有 token 字段表达所属 record Client、Call generation、固定槽索引及本次 hold 的唯一 token。调用方不得读取、拼接或修改私有字段。

合同：

- `hold` 仅在调用已被接受且槽仍活动时成功；可在普通线程、message/writable 回调或 terminal 回调调用。成功才写 `out`。已有 hold、槽已退役、代次无效和 Client 正在销毁分别以确定错误返回；所有失败保留 `out`。
- 一个 Call 同时只允许一个 hold。token 是唯一所有权值；可在应用代码中转移，但不得复制成多个所有者。`release` 接受指针，成功时清零调用方 token。过期、重复释放和错误代次不得改变任何槽位或 Client 账目。
- `result` 非阻塞且可在线程间读取。终局尚未交付/报告时返回 `TR_AGAIN` 并不修改输出；最终 status 与 `delivery` 域在 terminal callback 即将执行时一次性发布，之后返回 `TR_OK`。该时点保证 shutdown reaper 对尚未报告终局设置的 `OWNER_STOPPED` 已包含在快照内。快照在 token 有效期间不可变。
- `cancel` 与弱 Call cancel 具有相同的协议语义和唯一终局规则。活动调用中成功仅表示取消已被本地内核接受，不代表业务线程立即停止；终局已选或 Client 进入停机后返回 `TR_ERR_STATE`。
- 引用不是弱 Call 的替代类型。持有引用不改变 `timeout_ms`，不保留原调用参数，不延长传输 deadline。
- token 释放后不再可读或取消。release 与同一 token 的 result/cancel 必须由调用方串行化；Client destroy 与新的 hold 操作沿用现有外部生命周期串行化合同。

## 内部状态与生命周期

每个 `tr_record_client_slot` 增加：

- 原子 `held_ref_token`：零表示无引用；非零值为 CallRef 表中的唯一 token。owner 独占更新的 `next_ref_token` 负责分配；到 `UINT64_MAX` 后拒绝新 hold，不回绕。hold 在 owner 上发布 token；release 通过 CAS 精确消费 token，复制出的旧值不能释放随后产生的新 token。
- 原子 `result_ready`、`result_status` 和 `result_delivery`。在 `tr_record_client_report()` 中，delivery 域已确定、terminal callback 即将执行时，先以 relaxed store 写 status/delivery，再以 release store 发布 `result_ready=true`；读者 acquire `result_ready` 后读取结果。快照一旦发布不再修改。
- 现有 `active` 保持为真直到协议绑定退役且引用释放。调用完成后，`tr_record_client_retire()` 完成 RESET/CLOSE 接管、route 移除和 RPC binding 销毁，然后在 token 非零时保留无网络引用的槽位状态；下一 owner pump 在 token 清零后归还槽位。

`tr_rpc_record_client_reap_stopped()` 按固定容量扫描槽。对已终局、已报告且协议绑定已清理的 held slot，它保留结果快照和活动槽，记录仍有外部引用；完成其他槽回收后返回 `TR_AGAIN`，不 detach/free record Client。最后一个 hold 被释放后，destroy 重试清除该槽并继续销毁。

`tr_client_destroy()` 沿用现有顺序：首次尝试可以停止 Runtime 并交付 shutdown 终局。reaper 若遇到 held slot，不释放其底层固定存储，故 destroy 返回 `TR_AGAIN` 且保留 facade/record Client 对象。release 后重试销毁。destroy 已开始后，弱 Call 和普通 Client 操作按既有合同无效；held ref 只允许读取已发布结果和释放。这样父 owner 已先行终止时也能由 reaper 建立最终结果，不会因为提前 hold 门禁阻止恢复回收。

`wait_drained` 使用现有槽位静默谓词，因此被引用的终局槽保持 busy；应用必须先释放引用，再等待完整 drain。受理容量包含引用中的终局槽，容量用尽仍返回既有 `TR_AGAIN`，不另建队列。

## 并发与错误处理

- hold 的 owner 事务验证 Client 未进入 stopping、Call 已受理且 generation 相同，再生成唯一 token。若终局回调正在运行，槽尚未退役，允许在回调中 hold 已选定结果。
- release 先校验 token 所属 Client/slot，再以 CAS 将精确 `held_ref_token` 从调用 token 改为零。CAS 失败返回 `TR_ERR_STALE`，不改变任何预算；成功后清零输入句柄。
- owner 只在 terminal 已报告、传输责任已安全转交、binding 已销毁、token 为零时清零槽。slot reuse 会分配新 Call generation 与新 hold token。
- terminal snapshot 在最终 status/delivery 报告前发布；P（最终结果选择）和 C（最后用户回调完成）仍分别有独立时点。不得在 FINAL 接受、RESET 提交或 `TR_OK` 本地发送时提前发布，也不得在 shutdown reaper 可能改写 delivery 域之前发布。
- facade destroy 遇到活动引用时可以完成 Runtime 停止和终局交付，但必须保留 held slot 并返回 `TR_AGAIN`。释放最后 token 后再次 destroy 才能释放 Client。
- `TR_AGAIN`、`TR_ERR_STALE`、`TR_ERR_STATE`、`TR_ERR_INVALID` 都不修改输出结构；过期 token 不释放当前新调用的资源。

## 预算与性能

不为 hold 申请动态内存。每槽新增 token 和原子结果快照字段，固定槽数组仍通过 `sizeof(tr_rpc_record_client) + slots` 记入现有 Runtime memory budget。结果读取只有原子 load 和固定大小复制；hold/release 只增加常量时间状态更新。唯一限制是有引用的终局调用会占用一个既有 Call 槽，直到 release；destroy 重试期间 Client 已进入 stopping，除 held ref result/release 外不提供业务操作。

## 验收

1. RED/GREEN：terminal 回调内 hold；回调返回后仍查询准确 status/delivery；释放后该槽可重用。
2. pending call hold 后取消，成功 terminal 前 result 返回 `TR_AGAIN`；终局后 result 返回 `CANCELLED`，迟到 send/half-close 不被 ref 引入。
3. 同一 Call 第二次 hold 明确拒绝；旧/重复/伪造 generation token 不释放新调用槽，不改变输出和预算。
4. 唯一引用持有时，capacity=1 的新调用返回 `TR_AGAIN`，`wait_drained` 不误报静默。
5. Client destroy 在持有引用时完成 Runtime 停止/终局回收后返回 `TR_AGAIN`；引用结果可读且 release 成功，重试销毁成功，Runtime/预算归零。
6. call result 的终局发布和 release 与 owner pump 并发重复执行，TSan 无数据竞争；ASan/UBSan 无悬空访问。
7. 普通无 hold 的现有 API、NONE/ZSTD、取消、期限和 shutdown 行为不变；安装 SDK 的 C11/C++11 消费端引用所有新符号。

## 风险和回滚

主要风险是终局槽被应用长期持有导致调用容量耗尽。该行为是显式、可计数且有界的反压；API 文档强调 release 责任，测试验证容量满和 release 后恢复。atomic token、结果发布或 destroy 门禁若未通过并发/销毁矩阵，不得发布。

回滚通过撤销本切片的 CallRef API、slot/facade 原子字段、文档和测试，并恢复开发版本即可；无线协议变更、持久数据迁移或外部 ABI layout 改动。现有 `tr_rpc_v2_call` 布局保持不变。

## 切片边界

本 spec 只关闭 #243 的“借用 Call 与显式持有的调用/结果能力分离”子项。通用异步操作池、父子取消/deadline、跨 owner A→B→A 自等待、服务端任意业务执行强制终止、全部故障模型和 V2 发布验收仍未完成。
