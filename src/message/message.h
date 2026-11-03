#ifndef TR_MESSAGE_H
#define TR_MESSAGE_H

#include <stddef.h>
#include <stdint.h>

struct tr_message_storage;

/* 仅读取字节，不延长存储寿命；使用期间必须持有原消息或其克隆。 */
struct tr_message_view {
	const uint8_t *data;
	size_t len;
};

/* 库内拥有型消息句柄，不是已安装 SDK。
 *
 * 初始化为 {0}。禁止直接复制、修改 _ 字段或覆盖存活句柄；共享必须
 * clone/slice，移交使用 none_move。各线程只操作各自独占的句柄；
 * 句柄的发布/移交仍需要调用方同步。引用计数不保护悬空指针。
 * 有效的零长度消息与未持有消息不同，不能只凭 len == 0 判断。
 */
struct tr_message {
	const uint8_t *_data;
	size_t _len;
	struct tr_message_storage *_storage;
};

/* 两个域独立验证；0 只允许零长度消息，SIZE_MAX 表示本机最大长度。 */
struct tr_message_limits {
	size_t max_decoded_bytes;
	size_t max_encoded_bytes;
};

int tr_message_view(const struct tr_message *message,
                    struct tr_message_view *out);

/* 以下操作要求 out 已初始化且未持有消息，并与输入是不同句柄。
 * 仅 TR_OK 修改 out；失败保留输入和输出原状。
 * clone/slice 只增加存储引用，不重复预约字节预算，不分配或复制载荷。
 * slice 的 offset/len 相对当前视图；不允许从裸存储指针获得引用。
 */
int tr_message_clone(const struct tr_message *message, struct tr_message *out);
int tr_message_slice(const struct tr_message *message, size_t offset,
                     size_t len, struct tr_message *out);

/* none 仅校验并移交原引用，成功清空 input，不改动原载荷。
 * 这是同步消息变换，不是网络发送准入或异步操作完成的承诺。
 */
int tr_message_none_move(struct tr_message *input,
                         const struct tr_message_limits *limits,
                         struct tr_message *out);

/* 消费并清空一个存活句柄；同一已清空句柄再次释放返回 TR_ERR_STATE。
 * 最后一个引用同步执行存储释放回调，随后才归还存储预算。
 * 即使内部引用/预算失配而返回错误，句柄也已被消费，不得重试它。
 * 回调可以销毁存储控制块；函数在回调后不再访问消息或控制块。
 * 未经 clone 的结构体复制、已释放内存及并发操作同一句柄不受支持。
 */
int tr_message_release(struct tr_message *message);

#endif
