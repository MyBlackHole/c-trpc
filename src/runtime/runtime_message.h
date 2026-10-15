#ifndef TR_RUNTIME_MESSAGE_H
#define TR_RUNTIME_MESSAGE_H

#include "message_service.h"

struct tr_runtime_shard;

/* 可选的库内组合入口，现有 Client/Server 默认不启用。每个已运行分片
 * 显式启用一次，Runtime 独占服务生命周期，不向调用方暴露可销毁指针。
 * config 仅借用至返回；create/start 期间失败不代表没有部分资源，Runtime
 * 始终持有其责任，调用方须 disable 或 stop/destroy 重试收敛。
 * 所有接口只允许在该分片的当前 Reactor owner 上执行；enable/disable
 * 与 Runtime start/stop/destroy 由外部串行协调，不允许并发变更生命周期。
 */
int tr_runtime_shard_message_enable_on_owner(
        struct tr_runtime_shard *shard,
        const struct tr_message_service_config *config);

/* 幂等关闭；AGAIN/错误保持所有权，禁止由调用者关闭父 Reactor 绕过屏障。
 * 只有成功后才允许重新 enable；Runtime stop 自动执行此步骤。
 */
int tr_runtime_shard_message_disable_on_owner(struct tr_runtime_shard *shard);

/* 借用 input 至返回，只有 TR_OK 消费它并发布一次完成责任。回调线程、
 * 取消结果和输出的借用/保留遵循 MessageService 契约。输出可晚于服务
 * 释放；Runtime destroy 将保留分片预算，直到最后外部引用释放。
 */
int tr_runtime_shard_message_submit_on_owner(
        struct tr_runtime_shard *shard, struct tr_message *input,
        const struct tr_message_transform_config *config,
        tr_message_service_done_fn done, void *arg,
        struct tr_task_handle *handle);
int tr_runtime_shard_message_cancel_on_owner(struct tr_runtime_shard *shard,
                                             struct tr_task_handle handle);
int tr_runtime_shard_message_stats_on_owner(
        struct tr_runtime_shard *shard, struct tr_message_service_stats *out);

#endif
