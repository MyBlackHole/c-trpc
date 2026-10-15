#ifndef TR_RPC_RECORD_STREAM_H
#define TR_RPC_RECORD_STREAM_H

#include "rpc_record.h"
#include "../transport/channel/channel.h"

struct tr_memory_budget;
struct tr_rpc_record_stream;

enum tr_rpc_record_delivery {
	TR_RPC_RECORD_ON_OWNER,
	TR_RPC_RECORD_OWNER_STOPPED
};

/* V2 的每调用传输绑定。调用者先以本地方法表解析/选择 method，再创建；
 * 对端 OPEN 必须匹配该描述，不能用远端字段修改本地方法基数或消息限制。
 * 当前仅绑定 NONE；压缩需异步验证真实输出，不能在 owner 中直接解压。
 * 此接口是迁移用库内接口，尚未替换旧 tr_rpc_endpoint/V1 的默认路径。
 */
struct tr_rpc_record_stream_config {
	enum tr_rpc_record_direction send_direction;
	struct tr_rpc_method_desc method;
};

typedef void (*tr_rpc_record_received_fn)(struct tr_rpc_record_stream *call,
                                          const struct tr_rpc_record_view *view,
                                          void *arg);
typedef void (*tr_rpc_record_terminal_fn)(struct tr_rpc_record_stream *call,
                                          enum tr_rpc_status status,
                                          enum tr_rpc_record_delivery delivery,
                                          void *arg);

struct tr_rpc_record_stream_callbacks {
	/* 已提交语义后在 owner 上同步调用。view 和字节只借用到返回；异步
	 * 使用必须另持有存储，不可保存解析缓存指针。允许发送/本地取消，
	 * 禁止递归接收或销毁。这里只适合准入/调度，不执行阻塞业务工作。
	 */
	tr_rpc_record_received_fn received;
	/* 每个调用至多一次，延后到当前 received 回调退出。发送 FINAL 被
	 * 下游接受即选择本地终局，不表示已经写入网络或远端已经执行。
	 * 可释放 arg；之后不再调用 received。OWNER_STOPPED 只交付取消，
	 * 不冒充原 owner，不能再次访问已停止的网络业务接口。
	 */
	tr_rpc_record_terminal_fn terminal;
	void *arg;
};

/* 精确所属 owner；不替换 Channel 的共享 handler，调用者负责按 Stream
 * 槽位及代次路由 data/event。channel 与非零硬预算必须保持存活；失败不改
 * *out。绑定不拥有 Channel、流或任何后台线程。每个流只允许一个绑定。
 */
int tr_rpc_record_stream_create_on_owner(
        struct tr_stream_handle stream,
        const struct tr_rpc_record_stream_config *config,
        const struct tr_rpc_record_stream_callbacks *callbacks,
        struct tr_memory_budget *budget, struct tr_rpc_record_stream **out);

/* 头与元数据会复制到独立且计费的 TX 前缀，业务体使用原 tr_buffer，
 * 不复制其载荷。body 可为 NULL（无业务体），或指向拥有的缓冲区句柄。
 * 仅 TR_OK 接管并清空 *body，所有失败保留状态及 body；发送先预检状态
 * 副本，在真实 tr_stream_sendv 接受后才提交，不在适配器里隐式排队。
 * 空 MESSAGE 发送固定头，仍计一条；零长 body 在成功后由适配器释放。
 * metadata 长度由 header 声明。控制TLV使用 u8 key_len/u16 value_len，
 * 键仅小写ASCII、数字、_.-，不允许重复或保留的冒号键。业务体长度、
 * codec、方法方向和本地方法上限均验证。期限字段承载剩余值，不建立定时器。
 */
int tr_rpc_record_stream_send_on_owner(
        struct tr_rpc_record_stream *call,
        const struct tr_rpc_record_header *header, const uint8_t *metadata,
        struct tr_buffer **body);

/* 借用一段有序 Stream 字节；记录可跨多次调用，也可在一次调用中串接。
 * 完整连续记录直接借用输入；只有不完整记录才有一份按声明精确预约的
 * 缓存，头完整后先校验方法/状态/长度再分配。TR_OK 表示本段已吸收，
 * 不代表已有完整记录。协议/内存错误选择终局并释放缓存，不可重试本段；
 * 预算不足返回 NOMEM 而不是暗示可重放的 AGAIN。调用者仍负责释放原 RX
 * 缓冲和其网络信用。固定头最多48字节内嵌，无额外无界队列。
 */
int tr_rpc_record_stream_receive_on_owner(struct tr_rpc_record_stream *call,
                                          struct tr_stream_handle stream,
                                          const uint8_t *data, size_t size);
int tr_rpc_record_stream_event_on_owner(struct tr_rpc_record_stream *call,
                                        struct tr_stream_handle stream,
                                        enum tr_stream_event event);
int tr_rpc_record_stream_abort_on_owner(struct tr_rpc_record_stream *call,
                                        enum tr_rpc_status status);
int tr_rpc_record_stream_snapshot_on_owner(struct tr_rpc_record_stream *call,
                                           struct tr_rpc_record_state *out,
                                           size_t *cached_bytes);

/* 必须先从外层路由表移除，且禁止新的并发调用。正常 owner 上允许销毁
 * 未OPEN对象或已终局对象，活跃非终局/回调中拒绝。TX前缀不引用绑定对象，
 * 但持有预算直到真正释放；预算寿命不能只由本对象销毁决定。
 */
int tr_rpc_record_stream_destroy_on_owner(struct tr_rpc_record_stream **call);
/* 复用既有 Reactor 停止恢复租约，要求父线程已 join。未终局调用只在清理
 * 线程通知 OWNER_STOPPED/CANCELLED，回调至多一次，不访问失效 Stream。
 * 外层路由与所有调用者已静默是前置条件；弱句柄不提供并发 free 保护。
 */
int tr_rpc_record_stream_reap_stopped(struct tr_rpc_record_stream **call);

#endif
