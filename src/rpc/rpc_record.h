#ifndef TR_RPC_RECORD_H
#define TR_RPC_RECORD_H

#include <stddef.h>
#include <stdint.h>
#include "tr/rpc.h"

/* 候选 V2 调用记录；当前 V1 网络入口尚未切换。字段逐一编码，禁止将
 * C 结构直接写到网络。控制信息与业务体分离，MESSAGE 不允许携带元数据。
 */
#define TR_RPC_RECORD_VERSION 2U
#define TR_RPC_RECORD_HEADER_SIZE 48U

/* 固定头：TRPC / u16 version / u16 kind，随后为八个小端 u32：
 * service、method、codec、compression、encoded、decoded、metadata、status；
 * 最后为小端 u64 remaining_timeout_ns。头后依次是元数据与编码业务体。
 * 不传输本机绝对单调时钟；0 表示未声明期限，过期必须本地终止，不能写0。
 */
enum tr_rpc_record_kind {
	TR_RPC_RECORD_OPEN = 1,
	TR_RPC_RECORD_INITIAL_METADATA,
	TR_RPC_RECORD_MESSAGE,
	TR_RPC_RECORD_HALF_CLOSE,
	TR_RPC_RECORD_FINAL_STATUS
};

enum tr_rpc_record_compression {
	TR_RPC_RECORD_NONE = 0,
	TR_RPC_RECORD_ZSTD = 1
};

#define TR_RPC_RECORD_ACCEPT_NONE (1U << TR_RPC_RECORD_NONE)
#define TR_RPC_RECORD_ACCEPT_ZSTD (1U << TR_RPC_RECORD_ZSTD)

struct tr_rpc_record_header {
	enum tr_rpc_record_kind kind;
	uint32_t service_id;
	uint32_t method_id;
	uint32_t codec_id;
	uint32_t compression;
	uint32_t encoded_size;
	uint32_t decoded_size;
	uint32_t metadata_size;
	uint32_t status;
	uint64_t remaining_timeout_ns;
};

struct tr_rpc_record_limits {
	uint32_t max_encoded_size;
	uint32_t max_decoded_size;
	uint32_t max_metadata_size;
	/* 此方向已获准的编码集合，不是从远端自报能力推导的授权。必须含
	 * NONE，只允许本实现认识的位；配置0是无效值，不是隐式全开放。
	 */
	uint32_t accepted_compression;
};

struct tr_rpc_record_view {
	struct tr_rpc_record_header header;
	/* 借用原输入；长度为0时指针为NULL，不因此省略零长度MESSAGE事件。
	 * 不延长存储寿命。异步使用者必须另外持有输入或其 Message 切片。
	 */
	const uint8_t *metadata;
	const uint8_t *body;
};

/* 成功才写输出。encode_header 只写固定头，不复制元数据/业务体，调用方
 * 负责将三段存储保持到发送结束；不允许输出和 header/limits 存储重叠。
 * decode_header 足够识别版本时即拒绝V1/未知版本；不完整返回AGAIN。
 * 完整头先做长度/编码/字段作用域检查，再允许上层预约正文存储。
 * header 的未使用字段必须为0；OPEN的 service/method 是不透明数字，允许0。
 */
int tr_rpc_record_encode_header(uint8_t *out, size_t capacity,
                                const struct tr_rpc_record_header *header,
                                const struct tr_rpc_record_limits *limits);
int tr_rpc_record_decode_header(const uint8_t *data, size_t size,
                                const struct tr_rpc_record_limits *limits,
                                struct tr_rpc_record_header *out,
                                size_t *record_size);

/* 解析首条完整记录，成功才发布 view/consumed；不消费后续串接记录。
 * 未完整返回AGAIN，调用者须保留已有字节；EOF+AGAIN是截断，不是正常终止。
 * 本函数不分配、不保存解析状态、不验证元数据TLV/身份或运行解压算法。
 * 元数据语义、方法/codec匹配及解压实际长度仍由上层完成后才能交付。
 */
int tr_rpc_record_decode(const uint8_t *data, size_t size,
                         const struct tr_rpc_record_limits *limits,
                         struct tr_rpc_record_view *out, size_t *consumed);

/* 下列状态由单一所有者修改；可内嵌，不分配、不调用应用代码。
 * 字段只供本模块及只读测试观察，不可由其他模块直接修改。
 * 基数来自本地已注册方法，不信任远端OPEN来选择基数。
 */
struct tr_rpc_record_state {
	enum tr_rpc_cardinality request_cardinality;
	enum tr_rpc_cardinality response_cardinality;
	uint64_t request_messages;
	uint64_t response_messages;
	uint32_t final_status;
	unsigned initialized;
	unsigned opened;
	unsigned request_closed;
	unsigned response_headers;
	unsigned terminal;
};

enum tr_rpc_record_direction {
	TR_RPC_RECORD_REQUEST_DIRECTION,
	TR_RPC_RECORD_RESPONSE_DIRECTION
};

int tr_rpc_record_state_init(struct tr_rpc_record_state *state,
                             enum tr_rpc_cardinality request,
                             enum tr_rpc_cardinality response);

/* 失败不改state，成功只提交一个语义事件。消息数量按MESSAGE记录而非
 * 字节数计数；ONE恰好一条，MANY可为零。响应MESSAGE前必须有INITIAL；
 * FINAL_STATUS错误可以提前结束，成功必须满足ONE基数。请求半关闭不是
 * 调用终局；仅收到响应数据也不是成功。终局后任何记录都不得复活调用。
 *
 * 此检查不代表网络接受：发送方应对本地副本预检，下游接受后才提交副本；
 * 两者之间必须保持owner串行且无重入修改。下游拒绝不得推进真实状态。
 * 接收者则在记录及其业务体/元数据验证成功后应用事件。本模块不控制资源
 * 释放、回调或网络RESET；非法记录的错误作用域由绑定层决定。
 */
int tr_rpc_record_state_apply(struct tr_rpc_record_state *state,
                              enum tr_rpc_record_direction direction,
                              const struct tr_rpc_record_header *record);

/* 本地取消/期限/传输错误的终局选择，不编码成业务消息；status必须非OK。
 * 最先接受的终局胜出。重复终止返回STATE而不改最终结果，不负责释放资源。
 */
int tr_rpc_record_state_abort(struct tr_rpc_record_state *state,
                              enum tr_rpc_status status);

#endif
