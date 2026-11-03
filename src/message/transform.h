#ifndef TR_MESSAGE_TRANSFORM_H
#define TR_MESSAGE_TRANSFORM_H

#include <stdbool.h>
#include "message.h"

struct tr_memory_budget;
struct tr_message_transform;

enum tr_message_transform_kind {
	TR_MESSAGE_ZSTD_ENCODE = 1,
	TR_MESSAGE_ZSTD_DECODE
};

/* 与接口的 tr_status 分开：TR_OK 接受操作，不代表变换成功。
 * LIMIT 是输出/窗口/工作区上限；NOMEM 是实际分配失败。
 */
enum tr_message_transform_outcome {
	TR_MESSAGE_TRANSFORM_OK,
	TR_MESSAGE_TRANSFORM_CANCELLED,
	TR_MESSAGE_TRANSFORM_LIMIT,
	TR_MESSAGE_TRANSFORM_NOMEM,
	TR_MESSAGE_TRANSFORM_BAD_DATA,
	TR_MESSAGE_TRANSFORM_INTERNAL
};

struct tr_message_transform_config {
	enum tr_message_transform_kind kind;
	struct tr_message_limits limits;
	/* 解码必须给出消息信封声明的原始长度，并验证实际结果恰好相等。
	 * Zstandard 帧头长度未知时也不能绕过这个界限。编码忽略此字段。
	 */
	size_t decoded_size;
	/* 包含后端分配记账头的工作区硬上限，必须非零。 */
	size_t max_workspace_bytes;
	unsigned max_window_log;
	/* 编码接受 1..ZSTD_maxCLevel()；解码忽略此字段。 */
	int compression_level;
};

/* 分阶段操作，不创建线程、不入队、不调用完成回调。
 *
 * prepare 只在 TR_OK 时消费并清空 input，发布 *out；其他结果保留两者。
 * *out 必须为 NULL。输入已经通过 Message 存储计费，不重复计费。
 * budget 必须有非零硬上限且存活到操作、输出及其切片全部释放之后。
 * 接受前预约操作/输出控制块、输出容量和完整工作区额度；实际分配仍可能
 * 在 run 失败。零长度输出实际保留一个字节，不能靠空消息绕过准入。
 *
 * 接受后调用方必须恰好驱动一次 run，再 finish；排队失败也要 cancel/run/
 * finish 回收。此接口不是执行器已接受任务或网络发送的承诺。
 * prepare 可以分配元数据；run 执行 CPU 密集工作，不得放入 Reactor 回调。
 * 配置按值保存；跨线程发布操作指针仍需调用方同步。
 */
int tr_message_transform_prepare(
        struct tr_message *input,
        const struct tr_message_transform_config *config,
        struct tr_memory_budget *budget, struct tr_message_transform **out);

/* 仅一个执行者调用。TR_OK 表示终局结果已保存，结果由 finish 取走。
 * 返回前释放输入和所有后端工作区；未取走的成功输出仍占用容量。
 * 取消为协作式，在有界字节块之间检查；不承诺实时抢占单次算法/分配调用。
 */
int tr_message_transform_run(struct tr_message_transform *operation);

/* 组合方提供的单调取消查询，仅在本次 run_checked 调用栈内借用。
 * 查询必须短小、不可失败、不能重入操作或销毁参数；不在操作中保留指针。
 * 每个后端检查点都能观察执行器取消，观察到 true 后锁存为本操作取消。
 * 仍不保证实时抢占；上层终局提交晚于算法结束时还必须再次裁决取消。
 * check == NULL 等价于普通 run，arg 不被访问。
 */
typedef bool (*tr_message_cancel_check_fn)(void *arg);
int tr_message_transform_run_checked(struct tr_message_transform *operation,
                                     tr_message_cancel_check_fn check,
                                     void *arg);

/* 可与 run 并发。TR_OK 表示取消在终局提交之前获胜（重复请求也成功），
 * 不释放任何资源，不构成静默屏障；终局已提交时返回 TR_ERR_STATE。
 */
int tr_message_transform_cancel(struct tr_message_transform *operation);

/* 调用方必须先保证 run 和所有 cancel 调用均已退出，再取结果/销毁操作。
 * 未完成返回 TR_AGAIN；参数/状态错误不消费操作。out 须初始化为空。
 * TR_OK 清空 *operation，写 outcome；只有成功结果移交有效 out。
 * 失败/取消无部分输出。成功输出可独立于操作存活、克隆和切片。
 * 内部预算失配时仍可能消费操作并返回错误，不得重试已清空的指针。
 */
int tr_message_transform_finish(struct tr_message_transform **operation,
                                enum tr_message_transform_outcome *outcome,
                                struct tr_message *out);

#endif
