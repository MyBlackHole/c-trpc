#include "peer_identity_internal.h"

#include <stdlib.h>
#include <string.h>

#include "../../include/tr/status.h"
#include "../memory_budget.h"
#include "../refcount.h"

struct tr_peer_identity_snapshot {
	struct tr_refcount references;
	struct tr_memory_budget *budget;
	size_t allocation_bytes;
	uint32_t uri_san_len;
	uint8_t uri_san[];
};

int tr_peer_identity_snapshot_create(const uint8_t *uri_san,
                                     uint32_t uri_san_len,
                                     struct tr_peer_identity_snapshot **out)
{
	struct tr_peer_identity_snapshot *identity;
	int status;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	if (!uri_san || uri_san_len == 0U)
		return TR_ERR_INVALID;
	if (uri_san_len > 512U)
		return TR_ERR_BAD_LENGTH;
	for (uint32_t index = 0U; index < uri_san_len; ++index) {
		uint8_t byte = uri_san[index];
		if (byte == 0U || byte < 0x20U || byte == 0x7fU || byte > 0x7fU)
			return TR_ERR_INVALID;
	}
	identity = calloc(1U, sizeof(*identity) + uri_san_len);
	if (!identity)
		return TR_ERR_NOMEM;
	status = tr_refcount_init(&identity->references, 1U);
	if (status != TR_OK) {
		free(identity);
		return status;
	}
	identity->uri_san_len = uri_san_len;
	identity->allocation_bytes = sizeof(*identity) + uri_san_len;
	memcpy(identity->uri_san, uri_san, uri_san_len);
	*out = identity;
	return TR_OK;
}

int tr_peer_identity_snapshot_retain(struct tr_peer_identity_snapshot *identity)
{
	if (!identity)
		return TR_ERR_INVALID;
	return tr_refcount_get(&identity->references);
}

int tr_peer_identity_snapshot_charge(struct tr_peer_identity_snapshot *identity,
                                     struct tr_memory_budget *budget)
{
	int status;
	if (!identity || !budget)
		return TR_ERR_INVALID;
	if (identity->budget)
		return TR_ERR_STATE;
	status = tr_memory_budget_reserve(budget, identity->allocation_bytes);
	if (status != TR_OK)
		return status;
	identity->budget = budget;
	return TR_OK;
}

void tr_peer_identity_snapshot_release(
        struct tr_peer_identity_snapshot *identity)
{
	int status;
	if (!identity)
		return;
	status = tr_refcount_put(&identity->references);
	if (status == 1) {
		if (identity->budget)
			(void)tr_memory_budget_release(
			        identity->budget, identity->allocation_bytes);
		free(identity);
	}
}

int tr_peer_identity_snapshot_view(
        const struct tr_peer_identity_snapshot *identity,
        struct tr_peer_identity_view *out)
{
	if (!identity || !out)
		return TR_ERR_INVALID;
	*out = (struct tr_peer_identity_view){
	        .authentication = TR_PEER_IDENTITY_TLS_CERTIFICATE,
	        .uri_san = identity->uri_san,
	        .uri_san_len = identity->uri_san_len,
	};
	return TR_OK;
}
