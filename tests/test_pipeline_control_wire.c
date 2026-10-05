#include "../src/group/pipeline_control_internal.h"
#include "../src/group/pipeline_control_wire_internal.h"
#include "../src/group/pipeline_registry_internal.h"

#include "../src/endian.h"
#include "../src/execution/reactor.h"
#include "tr/status.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static struct tr_conn_handle fake_connection(struct tr_reactor *reactor,
					     uint32_t slot,
					     uint32_t generation)
{
	struct tr_conn_handle handle;

	memset(&handle, 0, sizeof(handle));
	handle.reactor = reactor;
	handle.slot = slot;
	handle.generation = generation;
	return handle;
}

static struct tr_pipeline_control_wire_message offer_message(void)
{
	struct tr_pipeline_control_wire_message message;

	memset(&message, 0, sizeof(message));
	message.version = TR_PIPELINE_CONTROL_WIRE_VERSION;
	message.type = TR_PIPELINE_CONTROL_DATA_OFFER;
	message.owner_shard_id = 3U;
	message.pipeline_id = UINT64_C(0x1122334455667788);
	message.epoch = UINT64_C(0x0102030405060708);
	message.data_index = 5U;
	message.data_generation = 9U;
	return message;
}

static void test_control_wire_roundtrip(void)
{
	struct tr_pipeline_control_wire_message input = offer_message();
	struct tr_pipeline_control_wire_message output;
	struct tr_pipeline_route_preface route;
	uint8_t raw[TR_PIPELINE_CONTROL_WIRE_SIZE];

	assert(tr_pipeline_control_wire_encode(raw, &input) == TR_OK);
	assert(raw[0] == 'T' && raw[1] == 'R' &&
	       raw[2] == 'C' && raw[3] == '1');
	assert(tr_get_le16(raw + TR_PIPELINE_CONTROL_WIRE_OFF_VERSION) ==
	       TR_PIPELINE_CONTROL_WIRE_VERSION);
	assert(tr_get_le64(raw + TR_PIPELINE_CONTROL_WIRE_OFF_PIPELINE_ID) ==
	       input.pipeline_id);

	memset(&output, 0, sizeof(output));
	assert(tr_pipeline_control_wire_decode(
		       raw, sizeof(raw), &output) == TR_OK);
	assert(output.type == TR_PIPELINE_CONTROL_DATA_OFFER);
	assert(output.owner_shard_id == input.owner_shard_id);
	assert(output.pipeline_id == input.pipeline_id);
	assert(output.epoch == input.epoch);
	assert(output.stream_id == 0U);
	assert(output.data_index == input.data_index);
	assert(output.data_generation == input.data_generation);

	memset(&route, 0, sizeof(route));
	assert(tr_pipeline_control_wire_data_route(&output, &route) == TR_OK);
	assert(route.version == TR_PIPELINE_ROUTE_VERSION);
	assert(route.role == TR_PIPELINE_ROUTE_DATA);
	assert(route.owner_shard_id == input.owner_shard_id);
	assert(route.pipeline_id == input.pipeline_id);
	assert(route.epoch == input.epoch);
	assert(route.member_index == input.data_index);
	assert(route.member_generation == input.data_generation);

	input.type = TR_PIPELINE_CONTROL_TRANSFER_READY;
	input.stream_id = 77U;
	assert(tr_pipeline_control_wire_encode(raw, &input) == TR_OK);
	assert(tr_pipeline_control_wire_decode(
		       raw, sizeof(raw), &output) == TR_OK);
	assert(output.type == TR_PIPELINE_CONTROL_TRANSFER_READY);
	assert(output.stream_id == 77U);
	assert(tr_pipeline_control_wire_data_route(&output, &route) ==
	       TR_ERR_INVALID);
}

static void test_control_wire_validation(void)
{
	struct tr_pipeline_control_wire_message message = offer_message();
	struct tr_pipeline_control_wire_message decoded;
	uint8_t raw[TR_PIPELINE_CONTROL_WIRE_SIZE];

	assert(tr_pipeline_control_wire_encode(raw, &message) == TR_OK);
	assert(tr_pipeline_control_wire_decode(
		       raw, sizeof(raw) - 1U, &decoded) == TR_ERR_BAD_LENGTH);

	assert(tr_pipeline_control_wire_encode(raw, &message) == TR_OK);
	raw[0] = 'X';
	assert(tr_pipeline_control_wire_decode(raw, sizeof(raw), &decoded) ==
	       TR_ERR_BAD_MAGIC);

	assert(tr_pipeline_control_wire_encode(raw, &message) == TR_OK);
	tr_put_le16(raw + TR_PIPELINE_CONTROL_WIRE_OFF_VERSION, 2U);
	assert(tr_pipeline_control_wire_decode(raw, sizeof(raw), &decoded) ==
	       TR_ERR_BAD_VERSION);

	assert(tr_pipeline_control_wire_encode(raw, &message) == TR_OK);
	tr_put_le16(raw + TR_PIPELINE_CONTROL_WIRE_OFF_TYPE, 99U);
	assert(tr_pipeline_control_wire_decode(raw, sizeof(raw), &decoded) ==
	       TR_ERR_BAD_TYPE);

	assert(tr_pipeline_control_wire_encode(raw, &message) == TR_OK);
	tr_put_le32(raw + TR_PIPELINE_CONTROL_WIRE_OFF_FLAGS, 1U);
	assert(tr_pipeline_control_wire_decode(raw, sizeof(raw), &decoded) ==
	       TR_ERR_BAD_FLAGS);

	assert(tr_pipeline_control_wire_encode(raw, &message) == TR_OK);
	tr_put_le32(raw + TR_PIPELINE_CONTROL_WIRE_OFF_RESERVED, 1U);
	assert(tr_pipeline_control_wire_decode(raw, sizeof(raw), &decoded) ==
	       TR_ERR_RESERVED);

	message = offer_message();
	message.owner_shard_id = UINT32_MAX;
	assert(tr_pipeline_control_wire_encode(raw, &message) == TR_ERR_INVALID);

	message = offer_message();
	message.pipeline_id = 0U;
	assert(tr_pipeline_control_wire_encode(raw, &message) == TR_ERR_INVALID);

	message = offer_message();
	message.epoch = 0U;
	assert(tr_pipeline_control_wire_encode(raw, &message) == TR_ERR_INVALID);

	message = offer_message();
	message.data_index = UINT32_MAX;
	assert(tr_pipeline_control_wire_encode(raw, &message) == TR_ERR_INVALID);

	message = offer_message();
	message.data_generation = 0U;
	assert(tr_pipeline_control_wire_encode(raw, &message) == TR_ERR_INVALID);

	message = offer_message();
	message.stream_id = 1U;
	assert(tr_pipeline_control_wire_encode(raw, &message) == TR_ERR_INVALID);

	message = offer_message();
	message.type = TR_PIPELINE_CONTROL_TRANSFER_READY;
	assert(tr_pipeline_control_wire_encode(raw, &message) == TR_ERR_INVALID);
}

static void test_control_wire_state_machine(void)
{
	struct tr_reactor *owner = NULL;
	struct tr_pipeline_registry_config registry_config;
	struct tr_pipeline_registry *registry = NULL;
	struct tr_pipeline_control_config control_config;
	struct tr_pipeline_control *control = NULL;
	struct tr_pipeline_control_wire_message offer;
	struct tr_pipeline_control_wire_message ready;
	struct tr_pipeline_control_wire_message cancel;
	struct tr_pipeline_route_preface route;
	struct tr_conn_handle control_connection;
	struct tr_conn_handle data_connection;
	uint8_t offer_raw[TR_PIPELINE_CONTROL_WIRE_SIZE];
	uint8_t ready_raw[TR_PIPELINE_CONTROL_WIRE_SIZE];
	uint8_t cancel_raw[TR_PIPELINE_CONTROL_WIRE_SIZE];
	uint32_t i;

	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &owner) == TR_OK);
	assert(tr_reactor_start(owner) == TR_OK);

	memset(&registry_config, 0, sizeof(registry_config));
	registry_config.owner = owner;
	registry_config.owner_shard_id = 4U;
	registry_config.capacity = 1U;
	assert(tr_pipeline_registry_create(&registry_config, &registry) == TR_OK);

	memset(&control_config, 0, sizeof(control_config));
	control_config.registry = registry;
	control_config.pipeline_id = UINT64_C(0x7001);
	control_config.epoch = UINT64_C(51);
	control_config.data_capacity = 2U;
	control_config.stream_affinity_capacity = 4U;

	control_connection = fake_connection(owner, 10U, 1U);
	assert(tr_pipeline_control_create(
		       &control_config, control_connection, &control) == TR_OK);

	/* DATA_OFFER 由预留能力的同一操作创建。 */
	memset(offer_raw, 0, sizeof(offer_raw));
	assert(tr_pipeline_control_reserve_data_wire(
		       control, offer_raw) == TR_OK);
	assert(tr_pipeline_control_wire_decode(
		       offer_raw, sizeof(offer_raw), &offer) == TR_OK);
	assert(offer.type == TR_PIPELINE_CONTROL_DATA_OFFER);
	assert(offer.owner_shard_id == 4U);
	assert(offer.pipeline_id == UINT64_C(0x7001));
	assert(offer.epoch == UINT64_C(51));

	/*
	 * 预留不等于就绪。适配层不能在线协议上发送任何令牌，
	 * 直到精确 DATA offer 已经通过注册表完成 attach。
	 */
	memset(ready_raw, 0xa5, sizeof(ready_raw));
	assert(tr_pipeline_control_prepare_transfer_wire(
		       control, 2001U, ready_raw) == TR_AGAIN);
	for (i = 0; i < sizeof(ready_raw); ++i)
		assert(ready_raw[i] == 0U);

	memset(&route, 0, sizeof(route));
	assert(tr_pipeline_control_wire_data_route(&offer, &route) == TR_OK);
	data_connection = fake_connection(owner, 20U, 1U);
	assert(tr_pipeline_registry_attach_data_route(
		       registry, &route, data_connection, NULL) == TR_OK);

	assert(tr_pipeline_control_prepare_transfer_wire(
		       control, 2001U, ready_raw) == TR_OK);
	assert(tr_pipeline_control_wire_decode(
		       ready_raw, sizeof(ready_raw), &ready) == TR_OK);
	assert(ready.type == TR_PIPELINE_CONTROL_TRANSFER_READY);
	assert(ready.pipeline_id == offer.pipeline_id);
	assert(ready.epoch == offer.epoch);
	assert(ready.stream_id == 2001U);
	assert(ready.data_index == offer.data_index);
	assert(ready.data_generation == offer.data_generation);

	assert(tr_pipeline_control_release_transfer(control, 2001U) == TR_OK);
	assert(tr_pipeline_registry_detach_data_route(
		       registry, &route, data_connection) == TR_OK);

	/*
	 * DATA_CANCEL 对精确代次幂等。第一次消息会执行
	 * RESERVED -> FREE；重复相同代次时返回成功空操作。
	 */
	assert(tr_pipeline_control_reserve_data_wire(
		       control, offer_raw) == TR_OK);
	assert(tr_pipeline_control_wire_decode(
		       offer_raw, sizeof(offer_raw), &offer) == TR_OK);
	cancel = offer;
	cancel.type = TR_PIPELINE_CONTROL_DATA_CANCEL;
	assert(tr_pipeline_control_wire_encode(cancel_raw, &cancel) == TR_OK);
	assert(tr_pipeline_control_cancel_data_wire(
		       control, cancel_raw, sizeof(cancel_raw)) == TR_OK);
	assert(tr_pipeline_control_cancel_data_wire(
		       control, cancel_raw, sizeof(cancel_raw)) == TR_OK);

	/*
	 * 槽位复用会推进代次，因此旧的幂等取消不能
	 * 影响替换后的预留。
	 */
	assert(tr_pipeline_control_reserve_data_wire(
		       control, offer_raw) == TR_OK);
	assert(tr_pipeline_control_cancel_data_wire(
		       control, cancel_raw, sizeof(cancel_raw)) == TR_ERR_STALE);

	/* 伪造的 epoch 不能取消真实预留。 */
	assert(tr_pipeline_control_wire_decode(
		       offer_raw, sizeof(offer_raw), &offer) == TR_OK);
	cancel = offer;
	cancel.type = TR_PIPELINE_CONTROL_DATA_CANCEL;
	cancel.epoch++;
	assert(tr_pipeline_control_wire_encode(cancel_raw, &cancel) == TR_OK);
	assert(tr_pipeline_control_cancel_data_wire(
		       control, cancel_raw, sizeof(cancel_raw)) == TR_ERR_STALE);
	cancel.epoch = offer.epoch;
	assert(tr_pipeline_control_wire_encode(cancel_raw, &cancel) == TR_OK);
	assert(tr_pipeline_control_cancel_data_wire(
		       control, cancel_raw, sizeof(cancel_raw)) == TR_OK);

	assert(tr_pipeline_control_close(control, control_connection) == TR_OK);
	tr_pipeline_registry_destroy(registry);
	assert(tr_reactor_stop(owner) == TR_OK);
	tr_reactor_destroy(owner);
}

int main(void)
{
	test_control_wire_roundtrip();
	test_control_wire_validation();
	test_control_wire_state_machine();
	puts("pipeline CONTROL wire/state-machine: ok");
	return 0;
}
