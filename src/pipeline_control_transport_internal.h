#ifndef TR_PIPELINE_CONTROL_TRANSPORT_INTERNAL_H
#define TR_PIPELINE_CONTROL_TRANSPORT_INTERNAL_H

#include <stdint.h>

#include "pipeline_control_internal.h"
#include "pipeline_route_internal.h"
#include "tr/buffer.h"
#include "tr/reactor.h"

struct tr_pipeline_control_transport;

typedef void (*tr_pipeline_control_transport_closed_cb)(
	uint64_t pipeline_id, uint64_t epoch, int teardown_status, void *arg);

struct tr_pipeline_control_transport_config {
	struct tr_pipeline_control *control;
	struct tr_conn_handle connection;
	struct tr_buffer_pool *message_pool;
	struct tr_pipeline_route_preface control_route;
	tr_pipeline_control_transport_closed_cb closed_cb;
	void *closed_arg;
};

/*
 * Installs the real TRP1 frame handler for one CONTROL connection.
 * Ownership of control transfers to the transport on TR_OK.
 */
int tr_pipeline_control_transport_create(
	const struct tr_pipeline_control_transport_config *config,
	struct tr_pipeline_control_transport **out);

/*
 * Server-side CONTROL emissions. These APIs keep Pipeline state transition and
 * wire submission atomic with respect to failure: if queueing fails, the new
 * reservation/affinity is rolled back.
 */
int tr_pipeline_control_transport_send_data_offer(
	struct tr_pipeline_control_transport *transport, uint64_t message_id,
	struct tr_pipeline_route_preface *route_out);
int tr_pipeline_control_transport_send_transfer_ready(
	struct tr_pipeline_control_transport *transport, uint32_t stream_id,
	uint64_t message_id);
int tr_pipeline_control_transport_release_transfer(
	struct tr_pipeline_control_transport *transport, uint32_t stream_id);

uint64_t tr_pipeline_control_transport_pipeline_id(
	const struct tr_pipeline_control_transport *transport);
uint64_t tr_pipeline_control_transport_epoch(
	const struct tr_pipeline_control_transport *transport);

#endif
