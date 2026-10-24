#include "tls_provider_operation_internal.h"

#include <openssl/crypto.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "../memory_budget.h"
#include "tr/status.h"

enum tr_tls_provider_operation_state {
	TR_TLS_PROVIDER_OPERATION_FREE = 0,
	TR_TLS_PROVIDER_OPERATION_RESERVED,
	TR_TLS_PROVIDER_OPERATION_ACCEPTED,
	TR_TLS_PROVIDER_OPERATION_CANCEL_REQUESTED,
	TR_TLS_PROVIDER_OPERATION_READY
};

struct tr_tls_provider_operation_slot {
	uint8_t *result;
	size_t result_len;
	uint64_t generation;
	uint32_t result_capacity;
	size_t certificate_chain_len;
	size_t private_key_len;
	size_t private_key_password_len;
	enum tr_tls_provider_operation_state state;
	enum tr_tls_provider_operation_kind kind;
	int credentials_result;
	int status;
	int cancel_reason;
};

static _Atomic uint64_t tr_tls_provider_next_instance = 1U;

static int
tr_tls_provider_operation_kind_valid(enum tr_tls_provider_operation_kind kind)
{
	return kind == TR_TLS_PROVIDER_OPERATION_CREDENTIALS ||
	       kind == TR_TLS_PROVIDER_OPERATION_AUTHORIZATION;
}

static int tr_tls_provider_operation_instance(uint64_t *out_instance)
{
	uint64_t current = atomic_load_explicit(&tr_tls_provider_next_instance,
	                                        memory_order_relaxed);

	if (!out_instance)
		return TR_ERR_INVALID;
	for (;;) {
		if (current == 0U || current == UINT64_MAX)
			return TR_ERR_STATE;
		if (atomic_compare_exchange_weak_explicit(
		            &tr_tls_provider_next_instance, &current,
		            current + 1U, memory_order_relaxed,
		            memory_order_relaxed)) {
			*out_instance = current;
			return TR_OK;
		}
	}
}

static int
tr_tls_provider_operation_lock(struct tr_tls_provider_operation_table *table)
{
	if (!table || !table->lock_ready)
		return TR_ERR_INVALID;
	return pthread_mutex_lock(&table->lock) == 0 ? TR_OK : TR_ERR_SYS;
}

static int tr_tls_provider_operation_lookup_locked(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation operation,
        struct tr_tls_provider_operation_slot **out_slot)
{
	struct tr_tls_provider_operation_slot *slot;

	if (operation.instance == 0U || operation.generation == 0U ||
	    !tr_tls_provider_operation_kind_valid(operation.kind))
		return TR_ERR_INVALID;
	if (operation.instance != table->instance ||
	    operation.slot >= table->capacity)
		return TR_ERR_STALE;
	slot = &table->slots[operation.slot];
	if (slot->state == TR_TLS_PROVIDER_OPERATION_FREE ||
	    slot->generation != operation.generation ||
	    slot->kind != operation.kind)
		return TR_ERR_STALE;
	if (out_slot)
		*out_slot = slot;
	return TR_OK;
}

int tr_tls_provider_operation_table_init(
        struct tr_tls_provider_operation_table *table, uint32_t capacity,
        struct tr_memory_budget *budget)
{
	uint64_t storage_bytes;
	int ret;

	if (!table || !budget || capacity == 0U)
		return TR_ERR_INVALID;
	storage_bytes = (uint64_t)capacity *
	                sizeof(struct tr_tls_provider_operation_slot);
	memset(table, 0, sizeof(*table));
	ret = tr_memory_budget_reserve(budget, storage_bytes);
	if (ret != TR_OK)
		return ret;
	table->slots = calloc(capacity, sizeof(*table->slots));
	if (!table->slots) {
		(void)tr_memory_budget_release(budget, storage_bytes);
		return TR_ERR_NOMEM;
	}
	if (pthread_mutex_init(&table->lock, NULL) != 0) {
		free(table->slots);
		table->slots = NULL;
		(void)tr_memory_budget_release(budget, storage_bytes);
		return TR_ERR_SYS;
	}
	ret = tr_tls_provider_operation_instance(&table->instance);
	if (ret != TR_OK) {
		(void)pthread_mutex_destroy(&table->lock);
		free(table->slots);
		table->slots = NULL;
		(void)tr_memory_budget_release(budget, storage_bytes);
		return ret;
	}
	table->budget = budget;
	table->slot_storage_bytes = storage_bytes;
	table->capacity = capacity;
	table->lock_ready = 1;
	return TR_OK;
}

int tr_tls_provider_operation_table_destroy(
        struct tr_tls_provider_operation_table *table)
{
	uint32_t index;
	int ret;

	if (!table || !table->slots || !table->budget)
		return TR_ERR_INVALID;
	if (table->lock_ready) {
		ret = tr_tls_provider_operation_lock(table);
		if (ret != TR_OK)
			return ret;
		for (index = 0U; index < table->capacity; ++index) {
			if (table->slots[index].state !=
			    TR_TLS_PROVIDER_OPERATION_FREE) {
				(void)pthread_mutex_unlock(&table->lock);
				return TR_AGAIN;
			}
		}
		if (pthread_mutex_unlock(&table->lock) != 0)
			return TR_ERR_SYS;
		if (pthread_mutex_destroy(&table->lock) != 0)
			return TR_ERR_SYS;
		table->lock_ready = 0;
	}
	ret = tr_memory_budget_release(table->budget,
	                               table->slot_storage_bytes);
	if (ret != TR_OK)
		return ret;
	free(table->slots);
	memset(table, 0, sizeof(*table));
	return TR_OK;
}

int tr_tls_provider_operation_reserve(
        struct tr_tls_provider_operation_table *table,
        enum tr_tls_provider_operation_kind kind, uint32_t result_capacity,
        struct tr_tls_provider_operation *out_operation)
{
	struct tr_tls_provider_operation_slot *slot = NULL;
	struct tr_tls_provider_operation operation;
	uint32_t index;
	uint32_t exhausted = 0U;
	uint8_t *result = NULL;
	int ret;

	if (!table || !out_operation ||
	    !tr_tls_provider_operation_kind_valid(kind))
		return TR_ERR_INVALID;
	ret = tr_tls_provider_operation_lock(table);
	if (ret != TR_OK)
		return ret;
	for (index = 0U; index < table->capacity; ++index) {
		struct tr_tls_provider_operation_slot *candidate =
		        &table->slots[index];
		if (candidate->state != TR_TLS_PROVIDER_OPERATION_FREE)
			continue;
		if (candidate->generation == UINT64_MAX) {
			++exhausted;
			continue;
		}
		slot = candidate;
		break;
	}
	if (!slot) {
		(void)pthread_mutex_unlock(&table->lock);
		return exhausted == table->capacity ? TR_ERR_STATE : TR_AGAIN;
	}
	if (result_capacity != 0U) {
		ret = tr_memory_budget_reserve(table->budget, result_capacity);
		if (ret != TR_OK) {
			(void)pthread_mutex_unlock(&table->lock);
			return ret;
		}
		result = malloc(result_capacity);
		if (!result) {
			(void)tr_memory_budget_release(table->budget,
			                               result_capacity);
			(void)pthread_mutex_unlock(&table->lock);
			return TR_ERR_NOMEM;
		}
	}
	slot->generation++;
	slot->result = result;
	slot->result_len = 0U;
	slot->result_capacity = result_capacity;
	slot->state = TR_TLS_PROVIDER_OPERATION_RESERVED;
	slot->kind = kind;
	slot->status = TR_AGAIN;
	slot->cancel_reason = TR_OK;
	operation = (struct tr_tls_provider_operation){
	        .instance = table->instance,
	        .generation = slot->generation,
	        .slot = index,
	        .kind = kind,
	};
	if (pthread_mutex_unlock(&table->lock) != 0)
		return TR_ERR_SYS;
	*out_operation = operation;
	return TR_OK;
}

int tr_tls_provider_operation_accept(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation operation)
{
	struct tr_tls_provider_operation_slot *slot;
	int ret = tr_tls_provider_operation_lock(table);
	if (ret != TR_OK)
		return ret;
	ret = tr_tls_provider_operation_lookup_locked(table, operation, &slot);
	if (ret == TR_OK) {
		if (slot->state == TR_TLS_PROVIDER_OPERATION_RESERVED)
			slot->state = TR_TLS_PROVIDER_OPERATION_ACCEPTED;
		else if (slot->state == TR_TLS_PROVIDER_OPERATION_READY)
			ret = slot->status;
		else
			ret = TR_ERR_STATE;
	}
	if (pthread_mutex_unlock(&table->lock) != 0 && ret == TR_OK)
		return TR_ERR_SYS;
	return ret;
}

int tr_tls_provider_operation_complete(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation operation, int status,
        const void *result, size_t result_len)
{
	struct tr_tls_provider_operation_slot *slot;
	int ret = tr_tls_provider_operation_lock(table);
	if (ret != TR_OK)
		return ret;
	ret = tr_tls_provider_operation_lookup_locked(table, operation, &slot);
	if (ret == TR_OK) {
		if (status != TR_OK && status >= 0) {
			status = TR_ERR_INVALID;
			ret = TR_ERR_INVALID;
		} else if (result_len != 0U && !result) {
			status = TR_ERR_INVALID;
			ret = TR_ERR_INVALID;
		}
		if (slot->state == TR_TLS_PROVIDER_OPERATION_CANCEL_REQUESTED) {
			slot->status = slot->cancel_reason;
			slot->result_len = 0U;
			slot->state = TR_TLS_PROVIDER_OPERATION_READY;
			ret = TR_OK;
		} else if (slot->state != TR_TLS_PROVIDER_OPERATION_ACCEPTED) {
			if (ret == TR_OK)
				ret = TR_ERR_STATE;
		} else if (ret != TR_OK) {
			slot->status = status;
			slot->result_len = 0U;
			slot->state = TR_TLS_PROVIDER_OPERATION_READY;
		} else if (slot->state == TR_TLS_PROVIDER_OPERATION_ACCEPTED &&
		           result_len > slot->result_capacity) {
			slot->status = TR_ERR_BAD_LENGTH;
			slot->result_len = 0U;
			slot->state = TR_TLS_PROVIDER_OPERATION_READY;
			ret = TR_ERR_BAD_LENGTH;
		} else {
			if (status == TR_OK && result_len != 0U)
				memcpy(slot->result, result, result_len);
			slot->result_len = status == TR_OK ? result_len : 0U;
			slot->status = status;
			slot->state = TR_TLS_PROVIDER_OPERATION_READY;
			ret = TR_OK;
		}
	}
	if (pthread_mutex_unlock(&table->lock) != 0 && ret == TR_OK)
		return TR_ERR_SYS;
	return ret;
}

int tr_tls_provider_operation_complete_credentials(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation operation, int status,
        const struct tr_tls_credential_bundle *bundle)
{
	struct tr_tls_provider_operation_slot *slot;
	size_t total = 0U;
	int ret = tr_tls_provider_operation_lock(table);
	if (ret != TR_OK)
		return ret;
	ret = tr_tls_provider_operation_lookup_locked(table, operation, &slot);
	if (ret == TR_OK &&
	    operation.kind != TR_TLS_PROVIDER_OPERATION_CREDENTIALS)
		ret = TR_ERR_INVALID;
	if (ret == TR_OK) {
		if (status != TR_OK && status >= 0) {
			status = TR_ERR_INVALID;
			ret = TR_ERR_INVALID;
		}
		if (status == TR_OK &&
		    (!bundle || !bundle->certificate_chain_pem.data ||
		     bundle->certificate_chain_pem.len == 0U ||
		     !bundle->private_key_pem.data ||
		     bundle->private_key_pem.len == 0U ||
		     (bundle->private_key_password.len != 0U &&
		      !bundle->private_key_password.data))) {
			status = TR_ERR_INVALID;
			ret = TR_ERR_INVALID;
		}
		if (slot->state == TR_TLS_PROVIDER_OPERATION_CANCEL_REQUESTED) {
			slot->status = slot->cancel_reason;
			slot->result_len = 0U;
			slot->credentials_result = 0;
			slot->state = TR_TLS_PROVIDER_OPERATION_READY;
			ret = TR_OK;
		} else if (slot->state != TR_TLS_PROVIDER_OPERATION_ACCEPTED) {
			if (ret == TR_OK)
				ret = TR_ERR_STATE;
		} else if (ret != TR_OK) {
			slot->status = status;
			slot->result_len = 0U;
			slot->credentials_result = 0;
			slot->state = TR_TLS_PROVIDER_OPERATION_READY;
		} else if (status != TR_OK) {
			slot->status = status;
			slot->result_len = 0U;
			slot->credentials_result = 0;
			slot->state = TR_TLS_PROVIDER_OPERATION_READY;
		} else {
			const struct tr_tls_bytes *fields[] = {
			        &bundle->certificate_chain_pem,
			        &bundle->private_key_pem,
			        &bundle->private_key_password,
			};
			size_t lengths[3];
			size_t offsets[3];
			size_t index;

			for (index = 0U; index < 3U; ++index) {
				if (fields[index]->len > SIZE_MAX - total) {
					ret = TR_ERR_BAD_LENGTH;
					break;
				}
				offsets[index] = total;
				lengths[index] = fields[index]->len;
				total += fields[index]->len;
			}
			if (ret == TR_OK && total > slot->result_capacity)
				ret = TR_ERR_BAD_LENGTH;
			if (ret != TR_OK) {
				slot->status = ret;
				slot->result_len = 0U;
				slot->credentials_result = 0;
				slot->state = TR_TLS_PROVIDER_OPERATION_READY;
			} else {
				for (index = 0U; index < 3U; ++index)
					if (lengths[index] != 0U)
						memcpy(slot->result +
						               offsets[index],
						       fields[index]->data,
						       lengths[index]);
				slot->certificate_chain_len = lengths[0];
				slot->private_key_len = lengths[1];
				slot->private_key_password_len = lengths[2];
				slot->result_len = total;
				slot->credentials_result = 1;
				slot->status = TR_OK;
				slot->state = TR_TLS_PROVIDER_OPERATION_READY;
			}
		}
	}
	if (pthread_mutex_unlock(&table->lock) != 0 && ret == TR_OK)
		return TR_ERR_SYS;
	return ret;
}

int tr_tls_provider_operation_request_cancel(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation operation, int reason,
        int *out_notify_provider)
{
	struct tr_tls_provider_operation_slot *slot;
	int notify = 0;
	int ret;

	if (!out_notify_provider || reason >= 0)
		return TR_ERR_INVALID;
	ret = tr_tls_provider_operation_lock(table);
	if (ret != TR_OK)
		return ret;
	ret = tr_tls_provider_operation_lookup_locked(table, operation, &slot);
	if (ret == TR_OK) {
		switch (slot->state) {
		case TR_TLS_PROVIDER_OPERATION_RESERVED:
			slot->status = reason;
			slot->cancel_reason = reason;
			slot->state = TR_TLS_PROVIDER_OPERATION_READY;
			break;
		case TR_TLS_PROVIDER_OPERATION_ACCEPTED:
			slot->cancel_reason = reason;
			slot->state =
			        TR_TLS_PROVIDER_OPERATION_CANCEL_REQUESTED;
			notify = 1;
			break;
		case TR_TLS_PROVIDER_OPERATION_CANCEL_REQUESTED:
		case TR_TLS_PROVIDER_OPERATION_READY:
			break;
		case TR_TLS_PROVIDER_OPERATION_FREE:
			ret = TR_ERR_STALE;
			break;
		}
	}
	if (ret == TR_OK && pthread_mutex_unlock(&table->lock) != 0)
		return TR_ERR_SYS;
	if (ret != TR_OK) {
		(void)pthread_mutex_unlock(&table->lock);
		return ret;
	}
	*out_notify_provider = notify;
	return TR_OK;
}

int tr_tls_provider_operation_result(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation operation, int *out_status,
        void *out_result, size_t result_capacity, size_t *out_result_len)
{
	struct tr_tls_provider_operation_slot *slot;
	int status;
	size_t result_len;
	int ret;

	if (!out_status || !out_result_len)
		return TR_ERR_INVALID;
	ret = tr_tls_provider_operation_lock(table);
	if (ret != TR_OK)
		return ret;
	ret = tr_tls_provider_operation_lookup_locked(table, operation, &slot);
	if (ret == TR_OK) {
		if (slot->state != TR_TLS_PROVIDER_OPERATION_READY) {
			ret = TR_AGAIN;
		} else if (slot->result_len > result_capacity ||
		           (slot->result_len != 0U && !out_result)) {
			ret = TR_ERR_BAD_LENGTH;
		} else {
			status = slot->status;
			result_len = slot->result_len;
			if (result_len != 0U)
				memcpy(out_result, slot->result, result_len);
			*out_status = status;
			*out_result_len = result_len;
			ret = TR_OK;
		}
	}
	if (pthread_mutex_unlock(&table->lock) != 0 && ret == TR_OK)
		return TR_ERR_SYS;
	return ret;
}

int tr_tls_provider_operation_credentials_result(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation operation, int *out_status,
        struct tr_tls_credential_bundle *out_bundle)
{
	struct tr_tls_provider_operation_slot *slot;
	struct tr_tls_credential_bundle bundle = {0};
	int status;
	int ret;

	if (!out_status || !out_bundle)
		return TR_ERR_INVALID;
	ret = tr_tls_provider_operation_lock(table);
	if (ret != TR_OK)
		return ret;
	ret = tr_tls_provider_operation_lookup_locked(table, operation, &slot);
	if (ret == TR_OK &&
	    operation.kind != TR_TLS_PROVIDER_OPERATION_CREDENTIALS)
		ret = TR_ERR_INVALID;
	if (ret == TR_OK) {
		if (slot->state != TR_TLS_PROVIDER_OPERATION_READY) {
			ret = TR_AGAIN;
		} else {
			status = slot->status;
			if (status == TR_OK) {
				if (!slot->credentials_result) {
					ret = TR_ERR_STATE;
				} else {
					bundle.certificate_chain_pem.data =
					        slot->result;
					bundle.certificate_chain_pem.len =
					        slot->certificate_chain_len;
					bundle.private_key_pem.data =
					        slot->result +
					        slot->certificate_chain_len;
					bundle.private_key_pem.len =
					        slot->private_key_len;
					bundle.private_key_password.data =
					        slot->result +
					        slot->certificate_chain_len +
					        slot->private_key_len;
					bundle.private_key_password.len =
					        slot->private_key_password_len;
				}
			}
			if (ret == TR_OK) {
				*out_status = status;
				*out_bundle = bundle;
			}
		}
	}
	if (pthread_mutex_unlock(&table->lock) != 0 && ret == TR_OK)
		return TR_ERR_SYS;
	return ret;
}

int tr_tls_provider_operation_retire(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation operation)
{
	struct tr_tls_provider_operation_slot *slot;
	uint32_t capacity;
	int ret = tr_tls_provider_operation_lock(table);
	if (ret != TR_OK)
		return ret;
	ret = tr_tls_provider_operation_lookup_locked(table, operation, &slot);
	if (ret == TR_OK) {
		if (slot->state != TR_TLS_PROVIDER_OPERATION_READY) {
			ret = TR_AGAIN;
		} else {
			capacity = slot->result_capacity;
			ret = tr_memory_budget_release(table->budget, capacity);
			if (ret == TR_OK) {
				if (slot->result) {
					OPENSSL_cleanse(slot->result, capacity);
					free(slot->result);
				}
				memset(slot, 0, sizeof(*slot));
				slot->generation = operation.generation;
				slot->state = TR_TLS_PROVIDER_OPERATION_FREE;
			}
		}
	}
	if (pthread_mutex_unlock(&table->lock) != 0 && ret == TR_OK)
		return TR_ERR_SYS;
	return ret;
}

int tr_tls_provider_operation_test_set_generation(
        struct tr_tls_provider_operation_table *table, uint32_t slot,
        uint64_t generation)
{
	int ret = tr_tls_provider_operation_lock(table);
	if (ret != TR_OK)
		return ret;
	if (slot >= table->capacity) {
		ret = TR_ERR_INVALID;
	} else if (table->slots[slot].state != TR_TLS_PROVIDER_OPERATION_FREE) {
		ret = TR_ERR_STATE;
	} else {
		table->slots[slot].generation = generation;
		ret = TR_OK;
	}
	if (pthread_mutex_unlock(&table->lock) != 0 && ret == TR_OK)
		return TR_ERR_SYS;
	return ret;
}
