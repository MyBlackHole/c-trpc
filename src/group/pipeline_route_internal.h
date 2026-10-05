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

/*
 * 物理连接加入 Pipeline 时，在普通 Transport 分帧之前发送的固定路由标识。
 *
 * member_generation 是由 CONTROL 控制面签发的 Pipeline 成员代次，
 * 有意与 Reactor 连接槽位代次保持无关。
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

/*
 * 支持 TCP 分片与合并的增量解析器。
 *
 * feed() 最多消费剩余前导信息字节。
 * 如果输入还包含后续 Transport HELLO/帧字节，
 * *consumed 会精确停在 TR_PIPELINE_ROUTE_PREFACE_SIZE，
 * 使调用方可以继续转交尾部字节。
 *
 * 完整但非法的前导信息对该解析器属于终止状态；
 * 后续 feed() 返回 TR_ERR_STATE。
 * 只有完整前导信息成功校验后才修改 *out。
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
