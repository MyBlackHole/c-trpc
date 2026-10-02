#ifndef TR_PIPELINE_REGISTRY_INTERNAL_H
#define TR_PIPELINE_REGISTRY_INTERNAL_H

#include <stdint.h>

#include "pipeline_internal.h"
#include "pipeline_route_internal.h"

struct tr_pipeline_registry;

struct tr_pipeline_registry_config {
	struct tr_reactor *owner;
	uint32_t owner_shard_id;
	uint32_t capacity;
};

struct tr_pipeline_registry_stats {
	uint32_t owner_shard_id;
	uint32_t capacity;
	uint32_t count;
};

/*
 * Registry is a bounded shard-local index. It does not own Pipeline lifetime:
 * callers must unregister a Pipeline before destroying it.
 */
int tr_pipeline_registry_create(
	const struct tr_pipeline_registry_config *config,
	struct tr_pipeline_registry **out);
void tr_pipeline_registry_destroy(struct tr_pipeline_registry *registry);

int tr_pipeline_registry_register(struct tr_pipeline_registry *registry,
				  struct tr_pipeline *pipeline);
int tr_pipeline_registry_unregister(struct tr_pipeline_registry *registry,
				    struct tr_pipeline *pipeline);

int tr_pipeline_registry_lookup(struct tr_pipeline_registry *registry,
				uint64_t pipeline_id, uint64_t epoch,
				struct tr_pipeline **out);

/*
 * DATA route attach consumes only an exact RESERVED Pipeline capability.
 * The preface is assumed to have passed raw magic/CRC validation; this API
 * independently revalidates semantic fields and registry identity.
 */
int tr_pipeline_registry_attach_data_route(
	struct tr_pipeline_registry *registry,
	const struct tr_pipeline_route_preface *preface,
	struct tr_conn_handle connection,
	struct tr_pipeline **pipeline_out,
	struct tr_pipeline_data_ref *data_out);

int tr_pipeline_registry_get_stats(
	struct tr_pipeline_registry *registry,
	struct tr_pipeline_registry_stats *out);

#endif
