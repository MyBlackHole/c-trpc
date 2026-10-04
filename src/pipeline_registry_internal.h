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
 * callers must unregister a Pipeline before destroying it. Pipeline pointers
 * do not escape the owner domain; the only persistent pointer capability is
 * the owner-local DATA ingress binding returned at exact attach time.
 */
int tr_pipeline_registry_create(
	const struct tr_pipeline_registry_config *config,
	struct tr_pipeline_registry **out);
void tr_pipeline_registry_destroy(struct tr_pipeline_registry *registry);

struct tr_reactor *
tr_pipeline_registry_owner(const struct tr_pipeline_registry *registry);
uint32_t
tr_pipeline_registry_owner_shard_id(const struct tr_pipeline_registry *registry);

int tr_pipeline_registry_register(struct tr_pipeline_registry *registry,
				  struct tr_pipeline *pipeline);
int tr_pipeline_registry_unregister(struct tr_pipeline_registry *registry,
				    struct tr_pipeline *pipeline);

/*
 * DATA route attach consumes only an exact RESERVED Pipeline capability.
 * The preface is assumed to have passed raw magic/CRC validation; this API
 * independently revalidates semantic fields and registry identity.
 */
int tr_pipeline_registry_attach_data_route(
	struct tr_pipeline_registry *registry,
	const struct tr_pipeline_route_preface *preface,
	struct tr_conn_handle connection,
	struct tr_pipeline_data_ref *data_out);

/*
 * Owner-only ingress attach variant. In addition to consuming the exact DATA
 * reservation it returns the owner-local Pipeline capability used by the DATA
 * connection's ingress handler. The returned pointer must not escape the owner
 * domain or outlive that exact DATA membership.
 */
int tr_pipeline_registry_attach_data_route_local_on_owner(
	struct tr_pipeline_registry *registry,
	const struct tr_pipeline_route_preface *preface,
	struct tr_conn_handle connection,
	struct tr_pipeline_data_ref *data_out,
	struct tr_pipeline **pipeline_out);

/*
 * Cancel one exact RESERVED route after DATA establishment fails before
 * membership becomes live. Already ATTACHED/stale/reused capabilities are not
 * affected.
 */
int tr_pipeline_registry_cancel_data_route(
	struct tr_pipeline_registry *registry,
	const struct tr_pipeline_route_preface *preface);

int tr_pipeline_registry_detach_data_route(
	struct tr_pipeline_registry *registry,
	const struct tr_pipeline_route_preface *preface,
	struct tr_conn_handle expected_connection);

int tr_pipeline_registry_get_stats(
	struct tr_pipeline_registry *registry,
	struct tr_pipeline_registry_stats *out);

#endif
