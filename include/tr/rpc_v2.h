#ifndef TR_RPC_V2_H
#define TR_RPC_V2_H

#include <stddef.h>
#include <stdint.h>

#include "tr/client.h"
#include "tr/rpc_codec.h"
#include "tr/server.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * 显式选择 RPC V2 的业务接口，不协商或回退到 V1。
 * 方法使用 ONE/MANY 的四种组合；ONE 恰好一条，MANY 允许零到多条。
 * 零字节 MESSAGE 仍是一条消息。TR_RPC_NONE 基数尚不支持。
 * codec_id 是应用约定的非零编号；这里传递原始正文，不自动编解码或压缩。
 *
 * 两个构造入口复用 Client/Server 配置及连接、就绪、排空、销毁操作。
 * config==NULL 或上限字段为零采用原配置默认值。仅在 V2 构造入口中，
 * limits.max_message_bytes 是业务正文上限，initial_window_bytes 是正文
 * 信用窗口；库补入记录开销和必要控制空间，并限制信用更新阈值以保证
 * 整条消息可继续发送。方法正文上限须同时不超过这两个公开上限。
 * max_calls 与 max_streams 中较小者限制每个连接的并发调用。
 *
 * 方法表在构造期间复制并冻结。方法数受 max_methods 约束。
 * V2 不支持连接组、拦截器和 Client 自动重连；配置这些能力返回
 * TR_ERR_UNSUPPORTED。V1 方法注册和调用接口同样不适用；语义统计复用
 * tr_client_get_rpc_semantic_stats/tr_server_get_rpc_semantic_stats。
 * 构造失败的所有权规则与 tr_client_create/tr_server_create 相同：
 * 错误返回但 *out 非空时，调用方仍拥有部分对象，只能重试 destroy。
 */
int tr_client_create_v2(const struct tr_client_config *config,
                        const struct tr_rpc_method_desc *methods,
                        size_t method_count, struct tr_client **out);

/* 值类型弱能力，可复制，零值无效，不延长 Client 生命周期。
 * 不得读取、修改或推导私有字。首次 destroy 尝试开始前，调用方必须
 * 停止并排除所有并发能力使用；即使 destroy 失败也不能重新使用旧能力。
 * Client 存活时，调用代次可识别已经退役并被重用的调用。
 */
struct tr_rpc_v2_call {
	uint64_t _private[3];
};

/* 业务调用身份，可按完整值比较；不是用于发送或延长对象寿命的能力。
 * 同一进程中跨连接、跨 Server 的有效身份不重复，编号耗尽会拒绝准入。
 */
struct tr_rpc_v2_call_id {
	uint64_t _private[2];
};

/* LIVE 回调在所属 I/O 执行域；SHUTDOWN 是停止后由外部销毁线程交付。
 * 两个交付域都必须短小、不阻塞；终局状态不证明对端已经收到回复。
 */
enum tr_rpc_v2_delivery {
	TR_RPC_V2_DELIVERY_LIVE = 0,
	TR_RPC_V2_DELIVERY_SHUTDOWN = 1
};

/* 仅在所属回调中借用。不得修改私有字或保留其中的地址。
 * 空视图可以零初始化。键和值均带精确长度，键没有 NUL 终止符，值可含零。
 */
struct tr_rpc_v2_metadata_view {
	uintptr_t _private[2];
};

struct tr_rpc_v2_metadata_entry {
	struct tr_rpc_bytes key;
	struct tr_rpc_bytes value;
};

size_t tr_rpc_v2_metadata_count(const struct tr_rpc_v2_metadata_view *view);
/* 只有 TR_OK 修改 out。索引越界或键不存在返回 TR_ERR_STALE。
 * 返回的键值仍借用原视图；find 的输入 key 是 NUL 终止字符串。
 */
int tr_rpc_v2_metadata_at(const struct tr_rpc_v2_metadata_view *view,
                          size_t index, struct tr_rpc_v2_metadata_entry *out);
int tr_rpc_v2_metadata_find(const struct tr_rpc_v2_metadata_view *view,
                            const char *key, struct tr_rpc_bytes *out);

enum tr_rpc_v2_metadata_phase {
	TR_RPC_V2_METADATA_INITIAL = 0,
	TR_RPC_V2_METADATA_TRAILING = 1
};

struct tr_rpc_v2_client_callbacks {
	/* 必需。消息只借用至返回；单独收到正文尚不构成成功终局。 */
	void (*message)(struct tr_rpc_v2_call call,
	                const struct tr_rpc_bytes *body, void *arg);
	/* 可选。包括空元数据；尾部元数据先于唯一 terminal 交付。 */
	void (*metadata)(struct tr_rpc_v2_call call,
	                 enum tr_rpc_v2_metadata_phase phase,
	                 const struct tr_rpc_v2_metadata_view *view, void *arg);
	/* 可选。OPEN 已被本地传输接受时首次提示，其后通知发送可重试。
	 * 提示不预约容量，重试仍可返回 TR_AGAIN；不得在回调内忙等。
	 */
	void (*writable)(struct tr_rpc_v2_call call, void *arg);
	/* 必需。start 受理成功后恰好一次，且等待当前接收回调退出。
	 * 可释放 arg；返回后不再访问它或调用本调用的其他业务回调。
	 * terminal 不等于传输和调用槽已经退役，静默由 wait_drained/destroy
	 * 保证。
	 */
	void (*terminal)(struct tr_rpc_v2_call call, enum tr_rpc_status status,
	                 enum tr_rpc_v2_delivery delivery, void *arg);
	void *arg;
};

/* 普通线程可调用；内部同步提交短操作，不承诺硬实时或信号安全。
 * 仅 TR_OK 写 out 并承担一次终局责任。回调可能先于 start 返回，arg 必须
 * 预先准备，out 的存储不能依赖可在早终局中释放的 arg。
 * options 的元数据在受理期间复制；timeout_ms==0 表示无调用期限。
 *
 * Client 回调可发送、半关闭、取消及 begin_drain；禁止递归 start、阻塞
 * wait 或 destroy，也不能同步跨到另一 I/O 所有者。生命周期由外部串行化。
 */
int tr_client_v2_call_start(struct tr_client *client, uint32_t service_id,
                            uint32_t method_id,
                            const struct tr_rpc_call_options *options,
                            const struct tr_rpc_v2_client_callbacks *callbacks,
                            struct tr_rpc_v2_call *out);

/* 只在本次调用期间借用 body 及其字节；TR_OK 表示已经复制并接受一条
 * MESSAGE，返回后原字节可立即复用。失败不接受、不保留本条正文。
 * TR_AGAIN 不隐式排队，由应用保留正文并在 writable 提示后重试。
 * 所有存储均受内部有限资源约束；资源压力可返回 TR_AGAIN/NOMEM。
 * 发送入口也会观察已经到期/终止的调用，不承诺失败时调用仍然存活。
 */
int tr_rpc_v2_call_send(struct tr_rpc_v2_call call,
                        const struct tr_rpc_bytes *body);
int tr_rpc_v2_call_half_close(struct tr_rpc_v2_call call);
/* 半关闭后仍可取消。取消、期限和对端终局按原有先到者选择唯一终局。 */
int tr_rpc_v2_call_cancel(struct tr_rpc_v2_call call);

enum tr_rpc_v2_request_kind {
	TR_RPC_V2_REQUEST_OPEN = 0,
	TR_RPC_V2_REQUEST_MESSAGE = 1,
	TR_RPC_V2_REQUEST_HALF_CLOSE = 2
};

struct tr_rpc_v2_request {
	enum tr_rpc_v2_request_kind kind;
	struct tr_rpc_bytes body;
	/* 仅 OPEN 携带请求元数据；所有地址只借用至 work 返回。 */
	struct tr_rpc_v2_metadata_view metadata;
};

struct tr_rpc_v2_cancel;
struct tr_rpc_v2_reply;

/* 取消令牌和回复能力仅借用至当前 work 返回，不得保存、跨线程使用，
 * 或在回调外访问。取消查询正常返回 0/1；无效参数/执行域返回负错误码。
 * 取消是合作式，不能强制终止业务线程；业务仍在运行时不会交付终局或
 * 释放其输入。不得 pthread_exit、longjmp 或等待依赖所属 I/O 域的工作。
 */
int tr_rpc_v2_cancel_requested(const struct tr_rpc_v2_cancel *cancel);

/* append 在回调内预约有界存储并复制正文，成功后可立即复用源字节。
 * 一个事件当前最多生成 16 条回复，正文总额最多为该 Server 的最大方法
 * 响应正文上限的 16 倍，并与其他调用共享有限的实例资源。
 * append 的任何失败都会锁存整批错误，丢弃本批正文和全部元数据；忽略
 * 错误后返回 OK 不能交付截断的成功批次。网络背压由库保留已生成批次
 * 并继续发送，不重复调用 work。已被接受的前批记录不受后批错误影响。
 * finish 选择当前批次的终局，之后不能 append；非 OK 终局丢弃本批正文。
 * 当前不提供回调外长期持有回复能力或异步生产者。
 */
int tr_rpc_v2_reply_append(struct tr_rpc_v2_reply *reply,
                           const struct tr_rpc_bytes *body);

/* 设置当前批次一个阶段的元数据，每阶段最多调用一次，包括空列表。
 * 输入数组、键和值在成功返回前复制，随后可立即复用。键是以 NUL 结尾
 * 的 1..63 字节小写 ASCII [a-z0-9_.-]，同一阶段不允许重复键；值可为
 * 任意字节，零长度可用 NULL。每阶段编码总额最多 512 字节（每项另占
 * 3 字节长度字段），count 为 0 时 entries 可为 NULL。两阶段存储另计入
 * 实例的共享有限预算，不占正文条数和正文批次额度。
 *
 * INITIAL 可在任意事件中设置，但调用此前不得已经发送初始元数据；
 * 无论与本批 append 的调用次序如何，总是先发送 INITIAL 再发送正文。
 * 空列表也显式发送一次空 INITIAL。TRAILING 只属于当前终局批次，须在
 * finish 前设置；HALF_CLOSE 的默认完成或 work 返回非 OK 也构成终局。
 * 设置 TRAILING 后以普通非终局批次返回会锁存 FAILED_PRECONDITION。
 *
 * finish 后设置、重复设置或 INITIAL 阶段已关闭返回 TR_ERR_STATE，无
 * 既有错误时锁存 FAILED_PRECONDITION；无效 phase/数组/指针或 count
 * 超过 128 返回 TR_ERR_INVALID，
 * 键或总额长度错误返回 TR_ERR_BAD_LENGTH，非法字符或重复键返回
 * TR_ERR_BAD_FLAGS，均锁存 INVALID_ARGUMENT。分配失败返回 TR_ERR_NOMEM
 * 并锁存 RESOURCE_EXHAUSTED。首次失败不可被后续 OK 或业务状态覆盖，
 * 本批全部正文和元数据均丢弃；NULL 或回调外的无效能力不触及任何批次。
 *
 * 显式 finish(non-OK) 或 work 返回非 OK 会保留有效初始和尾部元数据，
 * 仅丢弃本批正文；尾部随错误 FINAL 发送。成功只表示本批复制受理，
 * 不表示对端收到。网络背压保留副本重试；即使已观察取消，仍有效的
 * 回调能力可复制成功，但取消、期限或停机可丢弃尚未交付的整个批次。
 */
int tr_rpc_v2_reply_set_metadata(struct tr_rpc_v2_reply *reply,
                                 enum tr_rpc_v2_metadata_phase phase,
                                 const struct tr_rpc_metadata *entries,
                                 size_t count);
int tr_rpc_v2_reply_finish(struct tr_rpc_v2_reply *reply,
                           enum tr_rpc_status status);

/* 同一调用的事件 FIFO 执行，前一事件生成的回复被接受后才执行下一事件。
 * 不同调用可并发使用同一 arg，业务负责同步。OPEN/MESSAGE/HALF_CLOSE
 * 各事件最多执行一次；已取消但尚未运行的事件可被跳过。
 * 工作和终局回调中不能同步调用 Client V2 的 start/send/half_close/cancel
 * 或阻塞生命周期接口；跨 I/O 执行域的同步调用会返回 TR_ERR_STATE。
 * 非 OK 返回丢弃当前事件的全部正文，并选择错误终局；HALF_CLOSE 在未
 * 显式 finish 时默认选择 OK，仍须满足方法的 ONE/MANY 基数合同。
 */
typedef enum tr_rpc_status (*tr_rpc_v2_work_fn)(
        struct tr_rpc_v2_call_id call, const struct tr_rpc_v2_request *request,
        const struct tr_rpc_v2_cancel *cancel, struct tr_rpc_v2_reply *reply,
        void *arg);

/* 完整合法 OPEN 匹配方法并通过期限准入后，承担一次 terminal 责任，
 * 即使 OPEN work 尚未开始就因资源不足/取消终止，也会交付。未知方法、
 * 无效或不完整 OPEN 不进入业务。终局等待所有工作返回及完成交付，不能
 * 提前释放业务状态；正常或停机终局回调中均禁止阻塞生命周期操作。
 */
typedef void (*tr_rpc_v2_server_terminal_fn)(struct tr_rpc_v2_call_id call,
                                             enum tr_rpc_status status,
                                             enum tr_rpc_v2_delivery delivery,
                                             void *arg);

struct tr_rpc_v2_method {
	struct tr_rpc_method_desc descriptor;
	tr_rpc_v2_work_fn work;
	tr_rpc_v2_server_terminal_fn terminal;
	/* 指针本身复制冻结，所指对象由业务拥有并借用至 destroy 成功。 */
	void *arg;
};

int tr_server_create_v2(const struct tr_server_config *config,
                        const struct tr_rpc_v2_method *methods,
                        size_t method_count, struct tr_server **out);

#ifdef __cplusplus
}
#endif

#endif
