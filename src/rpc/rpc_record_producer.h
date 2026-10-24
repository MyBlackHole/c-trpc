#ifndef TR_RPC_RECORD_PRODUCER_H
#define TR_RPC_RECORD_PRODUCER_H

#include "tr/rpc_v2.h"
#include "../execution/buffer.h"
#include "../memory_budget.h"

#include <stdbool.h>

struct tr_rpc_record_producer_domain;

/* 独立引用域在一个 Server 分片的全部对端间共享。每个未最终释放的
 * producer 都占一个有限租约，包括已经关闭但仍由应用持有的句柄。
 * 控制存储和正文在域存活期间同时计入 upstream；所有预算访问都经由
 * 域锁，使用方不能保存 upstream 裸指针。绑定的派发器借用本域。
 */
int tr_rpc_record_producer_domain_create(
        size_t capacity, struct tr_memory_budget *upstream,
        struct tr_rpc_record_producer_domain **out);
/* 宿主已关闭所有派发器及其传输后调用。锁内归还 upstream 的剩余计费
 * 并清除该指针，剩余存储只由应用引用和正在退出的复制操作拥有。
 * 不等待应用归还引用；晚释放只访问独立域。成功清空 *domain。
 */
int tr_rpc_record_producer_domain_close(
        struct tr_rpc_record_producer_domain **domain);

/* 只在当前 public work 内创建，成功取得应用和 job 各一个强引用。
 * STAGED 可复制一条命令，但只能由 work/done 交接后的 owner 激活。
 * job 在交接前独占库内引用，owner 不读取正在执行的 job 可变结果。
 * headers_sent 是任务提交时的不可变快照，之前批次已按 FIFO 接受。
 */
int tr_rpc_record_producer_create(struct tr_rpc_record_producer_domain *domain,
                                  uint32_t max_message, bool headers_sent,
                                  uint32_t response_compression_algorithms,
                                  struct tr_rpc_v2_producer **out);
void tr_rpc_record_producer_activate(struct tr_rpc_v2_producer *producer);
/* 仅归还库内引用，不能代替 close。最后应用引用放弃未 finish 的生产者
 * 会请求取消；owner 须在继续发送/提交工作前观察 abandoned。
 */
void tr_rpc_record_producer_put(struct tr_rpc_v2_producer *producer);
bool tr_rpc_record_producer_abandoned(struct tr_rpc_v2_producer *producer);

struct tr_rpc_record_producer_command {
	/* TR_RPC_RECORD_INITIAL_METADATA / MESSAGE / FINAL_STATUS。 */
	unsigned kind;
	enum tr_rpc_status status;
	enum tr_rpc_compression compression;
	struct tr_buffer *body;
	const uint8_t *metadata;
	uint32_t metadata_size;
};

/* owner 非阻塞尝试借用单条命令；成功后必须恰好 complete 一次。
 * 借用期间邮箱仍占用，不能在取出后开放第二槽。调用传输时不持有
 * producer/domain 锁；路由终局可能同步重入，因此 close 可返回 AGAIN。
 * 所有指针只借用至 complete；MESSAGE 的 body 仅在传输 TR_OK 时转交。
 */
int tr_rpc_record_producer_take(struct tr_rpc_v2_producer *producer,
                                struct tr_rpc_record_producer_command *out);
/* accepted=true 仅用于邮箱命令本身已被传输接受；如先补空 INITIAL，
 * 必须传 false 保留 MESSAGE。TR_OK 发送可内联释放 body，本函数不再
 * 解引用已转交的正文。失败/AGAIN 保留原命令与原正文，供下一轮重试。
 */
void tr_rpc_record_producer_complete(struct tr_rpc_v2_producer *producer,
                                     bool accepted);
/* 在 complete(false) 后将非法业务输出收敛为错误 FINAL。原 FINAL 的
 * 合法尾部保留；未发送正文释放。只能由 owner 调用，不能重入 take。
 */
int tr_rpc_record_producer_fail(struct tr_rpc_v2_producer *producer,
                                enum tr_rpc_status status);
/* 关闭并唤醒等待者，丢弃未转交记录。复制在锁外执行，迟到复制自行
 * 丢弃，仅持独立域；不阻塞在复制或应用引用上。仍有 take 借用时返回
 * AGAIN，调用方须保留库内引用并在 complete 后重试，成功后才能 put。
 */
int tr_rpc_record_producer_close(struct tr_rpc_v2_producer *producer);

#endif
