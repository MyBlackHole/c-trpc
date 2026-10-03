#ifndef TR_PIPELINE_CONTROL_INTERNAL_H
#define TR_PIPELINE_CONTROL_INTERNAL_H

#include <stdint.h>

#include "pipeline_internal.h"
#include "pipeline_registry_internal.h"
#include "pipeline_route_internal.h"
#include "pipeline_control_wire_internal.h"

struct tr_pipeline_control;

struct tr_pipeline_control_config {
	struct tr_pipeline_registry *registry;
	uint64_t pipeline_id;
	uint64_t epoch;
	uint32_t data_capacity;
	uint32_t stream_affinity_capacity;
};

struct tr_pipeline_data_offer {
	struct tr_pipeline_data_ref data;
	struct tr_pipeline_route_preface route;
};

/*
 * CONTROL session owns one registered Pipeline runtime object.
 *
 * create:
 *   create Pipeline -> bind CONTROL -> register in shard registry
 *
 * close:
 *   allowed only after all ATTACHED DATA and Stream affinities are gone.
 *   outstanding RESERVED capabilities are cancelled by CONTROL clear before
 *   unregister/destroy.
 */
int tr_pipeline_control_create(
	const struct tr_pipeline_control_config *config,
	struct tr_conn_handle control_connection,
	struct tr_pipeline_control **out);

int tr_pipeline_control_reserve_data(
	struct tr_pipeline_control *control,
	struct tr_pipeline_data_offer *out);
int tr_pipeline_control_cancel_data(
	struct tr_pipeline_control *control,
	const struct tr_pipeline_data_offer *offer);

int tr_pipeline_control_prepare_transfer(
	struct tr_pipeline_control *control, uint32_t stream_id,
	struct tr_pipeline_transfer_ready *out);
int tr_pipeline_control_release_transfer(
	struct tr_pipeline_control *control, uint32_t stream_id);

/*
 * CONTROL wire adapters keep runtime state transition and serialized identity
 * in one place. reserve/prepare roll back the newly-created soft state if the
 * fixed-size message cannot be encoded.
 */
int tr_pipeline_control_reserve_data_wire(
	struct tr_pipeline_control *control,
	uint8_t out[TR_PIPELINE_CONTROL_WIRE_SIZE]);
int tr_pipeline_control_cancel_data_wire(
	struct tr_pipeline_control *control, const uint8_t *data, uint32_t len);
int tr_pipeline_control_prepare_transfer_wire(
	struct tr_pipeline_control *control, uint32_t stream_id,
	uint8_t out[TR_PIPELINE_CONTROL_WIRE_SIZE]);

int tr_pipeline_control_close(
	struct tr_pipeline_control *control,
	struct tr_conn_handle expected_control);

#endif
