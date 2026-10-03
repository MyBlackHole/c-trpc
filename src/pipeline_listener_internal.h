#ifndef TR_PIPELINE_LISTENER_INTERNAL_H
#define TR_PIPELINE_LISTENER_INTERNAL_H

#include <stdint.h>

#include "pipeline_route_internal.h"
#include "tr/reactor.h"

struct tr_pipeline_listener;

typedef int (*tr_pipeline_listener_authorize_control_cb)(
	const struct tr_pipeline_route_preface *route, void *arg);

struct tr_pipeline_listener_config {
	struct tr_reactor *owner;
	uint32_t owner_shard_id;

	uint32_t pipeline_capacity;
	uint32_t connection_capacity;
	uint32_t data_capacity_per_pipeline;
	uint32_t stream_affinity_capacity_per_pipeline;
	uint32_t control_message_count;

	/*
	 * Required CONTROL admission/fencing hook. The listener never treats a
	 * client-provided TRR1 CONTROL identity as authorization by itself.
	 */
	tr_pipeline_listener_authorize_control_cb authorize_control;
	void *authorize_arg;

	tr_reactor_frame_cb data_frame_cb;
	tr_reactor_event_cb data_event_cb;
	void *data_callback_arg;
};

struct tr_pipeline_listener_stats {
	uint32_t pipeline_capacity;
	uint32_t pipelines_current;
	uint32_t pipelines_peak;
	uint32_t connection_capacity;
	uint32_t connections_current;
	uint32_t connections_peak;
	uint64_t control_accepts;
	uint64_t data_accepts;
	uint64_t route_rejections;
	uint64_t capacity_rejections;
};

/*
 * One listener belongs to exactly one Reactor/shard owner and owns its
 * shard-local Pipeline registry plus bounded CONTROL message buffers.
 */
int tr_pipeline_listener_create(
	const struct tr_pipeline_listener_config *config,
	struct tr_pipeline_listener **out);
int tr_pipeline_listener_listen_ipv4(
	struct tr_pipeline_listener *listener, const char *address,
	uint16_t port, int backlog, uint16_t *out_bound_port);
int tr_pipeline_listener_stop(struct tr_pipeline_listener *listener);
void tr_pipeline_listener_destroy(struct tr_pipeline_listener *listener);

uint16_t tr_pipeline_listener_bound_port(
	const struct tr_pipeline_listener *listener);

int tr_pipeline_listener_send_data_offer(
	struct tr_pipeline_listener *listener, uint64_t pipeline_id,
	uint64_t epoch, uint64_t message_id,
	struct tr_pipeline_route_preface *route_out);
int tr_pipeline_listener_send_transfer_ready(
	struct tr_pipeline_listener *listener, uint64_t pipeline_id,
	uint64_t epoch, uint32_t stream_id, uint64_t message_id);
int tr_pipeline_listener_release_transfer(
	struct tr_pipeline_listener *listener, uint64_t pipeline_id,
	uint64_t epoch, uint32_t stream_id);

int tr_pipeline_listener_get_stats(
	struct tr_pipeline_listener *listener,
	struct tr_pipeline_listener_stats *out);

#endif
