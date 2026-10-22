#ifndef TR_EXECUTION_WORKER_CONTEXT_H
#define TR_EXECUTION_WORKER_CONTEXT_H

#include <stdbool.h>

/* 执行层的线程局部工作域，不依赖 RPC 协议或任何执行器对象。
 * 保存值只在同一线程内按相反顺序恢复；不能跨线程发布或当作所有权能力。
 * 进入本域不授予 Reactor 所有者身份，退出也不是任务/回调静默屏障。
 */
struct tr_worker_context {
	bool previous;
};

/* 只供执行器线程入口使用。保存/恢复而非递增计数，允许词法嵌套且不回绕。
 * 无分配、无锁、不可失败；线程函数正常退出前必须恢复原值。
 */
struct tr_worker_context tr_execution_enter_worker(void);
void tr_execution_leave_worker(struct tr_worker_context saved);

/* 所有框架工作线程共用此拒绝门禁，不能仅排除调用者所熟悉的执行器。
 * 普通应用线程返回 0；不能据此证明对象存活或允许并发销毁。
 */
int tr_execution_in_worker_context(void);

#endif
