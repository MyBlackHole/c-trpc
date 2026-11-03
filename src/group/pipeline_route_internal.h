#ifndef TR_PIPELINE_ROUTE_INTERNAL_H
#define TR_PIPELINE_ROUTE_INTERNAL_H

#include <stddef.h>
#include <stdint.h>

#define TR_PIPELINE_ROUTE_PREFACE_SIZE 48U
#define TR_PIPELINE_ROUTE_VERSION 1U
#define TR_PIPELINE_ROUTE_MEMBER_CONTROL UINT32_MAX

#define TR_PIPELINE_ROUTE_OFF_MAGIC 0U
#define TR_PIPELINE_ROUTE_OFF_VERSION 4U
#define TR_PIPELINE_ROUTE_OFF_ROLE 6U
#define TR_PIPELINE_ROUTE_OFF_FLAGS 8U
#define TR_PIPELINE_ROUTE_OFF_OWNER_SHARD 12U
#define TR_PIPELINE_ROUTE_OFF_PIPELINE_ID 16U
#define TR_PIPELINE_ROUTE_OFF_EPOCH 24U
#define TR_PIPELINE_ROUTE_OFF_MEMBER_INDEX 32U
#define TR_PIPELINE_ROUTE_OFF_MEMBER_GENERATION 36U
#define TR_PIPELINE_ROUTE_OFF_HEADER_CRC 40U
#define TR_PIPELINE_ROUTE_OFF_RESERVED 44U

#define TR_PIPELINE_ROUTE_F_KNOWN_MASK 0U

enum tr_pipeline_route_role {
	TR_PIPELINE_ROUTE_CONTROL = 1,
	TR_PIPELINE_ROUTE_DATA = 2
};

/* 加入 Pipeline 的物理连接在正常传输分帧前发送的固定路由标识。
 *
 * member_generation 是 CONTROL 平面签发的 Pipeline 成员关系代次。
 * 它有意与反应器连接槽位代次无关。
 */
struct tr_pipeline_route_preface {
	uint16_t version;
	uint16_t role;
	uint32_t flags;
	uint32_t owner_shard_id;
	uint64_t pipeline_id;
	uint64_t epoch;
	uint32_t member_index;
	uint32_t member_generation;
	uint32_t header_crc32c;
	uint32_t reserved;
};

int tr_pipeline_route_preface_encode(
	uint8_t out[TR_PIPELINE_ROUTE_PREFACE_SIZE],
	const struct tr_pipeline_route_preface *preface);
int tr_pipeline_route_preface_decode(
	const uint8_t in[TR_PIPELINE_ROUTE_PREFACE_SIZE],
	struct tr_pipeline_route_preface *preface);
int tr_pipeline_route_preface_validate_fields(
	const struct tr_pipeline_route_preface *preface);
int tr_pipeline_route_preface_validate(
	const uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE],
	const struct tr_pipeline_route_preface *preface);

/* 用于 TCP 分片与合并的增量解析器。
 *
 * feed() 最多消耗剩余前导字节。如果输入还包含后续传输 HELLO 或帧的
 * 字节，*consumed 恰好停在 TR_PIPELINE_ROUTE_PREFACE_SIZE，
 * 以便调用者继续传递尾部。
 *
 * 无效完整前导使解析器进入终态；后续 feed() 返回 TR_ERR_STATE。
 * 只有完整前导成功通过验证时才修改 *out。
 */
struct tr_pipeline_route_parser {
	uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE];
	uint32_t have;
	int done;
};

void tr_pipeline_route_parser_init(struct tr_pipeline_route_parser *parser);
int tr_pipeline_route_parser_feed(
	struct tr_pipeline_route_parser *parser,
	const uint8_t *data, size_t len, size_t *consumed,
	struct tr_pipeline_route_preface *out, int *ready);

#endif
