#ifndef TR_TASK_EXECUTOR_RECOVERY_H
#define TR_TASK_EXECUTOR_RECOVERY_H

#include "task_executor.h"

/* 库内生命周期协调者专用，不是运行中迁移或通用抢占接口。
 *
 * 前置条件：旧完成所有者已通过外部静默屏障（例如所属 Reactor 的成功
 * stop/join）退出，且不可能再次调用执行器。调用方串行化本操作、其他
 * 所有者 API 及最终销毁；工作线程可以继续运行，普通取消仍由锁保护。
 * 必须先 stop；部分最终销毁失败的对象也允许转移其剩余 destroy 责任。
 *
 * TR_OK：当前线程成为唯一回收所有者，可继续 dispatch/join/destroy。
 * 锁内清除通知，返回后没有原通知回调正在执行或将再次执行；不丢弃任务，
 * 不释放参数或结果，也不改写已经提交的任务终局。新所有者须决定其原有
 * 完成函数能否在此执行，或通过组合适配改走专用丢弃/回收回调。
 *
 * 只允许首次接管或同一回收线程重试。拒绝开放准入、回调中及第二接管者。
 * 检查不能替代外部静默证明；不要对活跃所有者调用此函数。旧 pthread_t
 * 在 join 后可能失效，首次接管不会比较或探测该值是否“还活着”。
 */
int tr_task_executor_takeover_quiesced(struct tr_task_executor *executor);

#endif
