#ifndef TR_PIPELINE_INGRESS_INTERNAL_H
#define TR_PIPELINE_INGRESS_INTERNAL_H

#include "pipeline_registry_internal.h"
#include "tr/reactor.h"

struct tr_pipeline_ingress_config {
	struct tr_pipeline_registry *registry;
	tr_reactor_frame_cb frame_cb;
	tr_reactor_event_cb event_cb;
	void *callback_arg;
};

/*
 * Owner-only accepted-fd handoff for Pipeline DATA sockets.
 *
 * On TR_OK the Reactor owns fd and a fixed 48-byte TRR1 preface gate is
 * installed. Only an exact registry reservation attach transitions the
 * connection to normal TRP1 frame dispatch.
 *
 * On error ownership of fd remains with the caller.
 */
/*
 * Attach an already-adopted connection after a shared TRR1 gate has parsed an
 * exact DATA route. Used by the shard Pipeline listener so CONTROL and DATA
 * can share one accepted-socket routing gate.
 */
int tr_pipeline_ingress_attach_data_route_on_owner(
	const struct tr_pipeline_ingress_config *config,
	const struct tr_pipeline_route_preface *preface,
	struct tr_conn_handle connection);

int tr_pipeline_ingress_adopt_data_fd_on_owner(
	const struct tr_pipeline_ingress_config *config, int fd,
	struct tr_conn_handle *out);

#endif
