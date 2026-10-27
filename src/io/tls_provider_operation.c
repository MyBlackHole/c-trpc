#include "tls_provider_operation_internal.h"

#include <openssl/crypto.h>
#include <stdlib.h>
#include <string.h>

#include "../execution/async_operation_internal.h"
#include "../memory_budget.h"
#include "tr/status.h"

struct tr_tls_provider_operation_slot {
	struct tr_async_operation operation;
	struct tr_async_operation_ref producer_ref;
	struct tr_async_operation_ref completion_ref;
	uint8_t *result;
	size_t result_len;
	size_t certificate_chain_len;
	size_t private_key_len;
	size_t private_key_password_len;
	uint32_t result_capacity;
	enum tr_tls_provider_operation_kind kind;
	int status;
	int cancel_reason;
	int credentials_result;
	int active;
	int accepted;
	int completed;
	int consumed;
	int cancel_requested;
};

static int
tr_tls_provider_operation_kind_valid(enum tr_tls_provider_operation_kind kind)
{
	return kind == TR_TLS_PROVIDER_OPERATION_CREDENTIALS ||
	       kind == TR_TLS_PROVIDER_OPERATION_AUTHORIZATION;
}

static int tr_tls_provider_operation_size_multiply(size_t left, size_t right,
                                                   size_t *out)
{
	if (left != 0U && right > SIZE_MAX / left)
		return TR_ERR_BAD_LENGTH;
	*out = left * right;
	return TR_OK;
}

static int
tr_tls_provider_operation_lock(struct tr_tls_provider_operation_table *table)
{
	if (!table || !table->lock_ready)
		return TR_ERR_INVALID;
	return pthread_mutex_lock(&table->lock) == 0 ? TR_OK : TR_ERR_SYS;
}

static int tr_tls_provider_operation_owner_locked(
        struct tr_tls_provider_operation_table *table)
{
	/* 旧 provider API 可由不同的串行控制线程调用；table->lock 是交接证明。
	 */
	return tr_async_operation_pool_handoff_owner(table->operations, 1);
}

static int tr_tls_provider_operation_lookup_locked(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation operation,
        struct tr_tls_provider_operation_slot **out_slot)
{
	struct tr_tls_provider_operation_slot *slot;

	if (!tr_tls_provider_operation_kind_valid(operation.kind) ||
	    operation.instance == 0U || operation.generation == 0U)
		return TR_ERR_INVALID;
	if (operation.instance != table->instance ||
	    operation.slot >= table->capacity)
		return TR_ERR_STALE;
	slot = &table->slots[operation.slot];
	if (!slot->active ||
	    slot->operation.generation != operation.generation ||
	    slot->kind != operation.kind)
		return TR_ERR_STALE;
	if (out_slot)
		*out_slot = slot;
	return TR_OK;
}

static int tr_tls_provider_operation_publish_locked(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation_slot *slot, int status,
        int completion_status)
{
	struct tr_async_operation_ref completion_ref = {0};
	int ready = 0;
	int ret;
	ret = tr_async_operation_publish(table->operations, &slot->producer_ref,
	                                 (struct tr_async_operation_completion){
	                                         .status = completion_status,
	                                 },
	                                 &completion_ref, &ready);
	if (ret != TR_OK)
		return ret;
	if (ready)
		slot->completion_ref = completion_ref;
	slot->status = status;
	slot->completed = 1;
	return TR_OK;
}

static int tr_tls_provider_operation_consume_locked(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation_slot *slot)
{
	struct tr_async_operation_spec spec = {0};
	struct tr_async_operation_completion completion = {0};
	struct tr_async_operation_ref completion_ref = {0};
	int cancelled = 0;
	int cancel_reason = TR_OK;
	int ready = 0;
	int ret;
	if (!slot->completed)
		return TR_AGAIN;
	if (slot->consumed)
		return TR_OK;
	ret = tr_tls_provider_operation_owner_locked(table);
	if (ret != TR_OK)
		return ret;
	if (!slot->accepted) {
		ret = tr_async_operation_accept(table->operations,
		                                slot->operation,
		                                &completion_ref, &ready);
		if (ret != TR_OK)
			return ret;
		if (!ready)
			return TR_ERR_STATE;
		slot->accepted = 1;
		slot->completion_ref = completion_ref;
	}
	if (slot->completion_ref.token == 0U)
		return TR_AGAIN;
	ret = tr_async_operation_consume(
	        table->operations, &slot->completion_ref, &spec, &completion,
	        &cancelled, &cancel_reason);
	if (ret != TR_OK)
		return ret;
	if (spec.kind != TR_ASYNC_OPERATION_KIND_TLS_PROVIDER ||
	    spec.domain_generation != (uint64_t)slot->kind ||
	    completion.payload != NULL || completion.destroy != NULL)
		return TR_ERR_STATE;
	if (cancelled) {
		slot->status = cancel_reason;
		slot->result_len = 0U;
		slot->credentials_result = 0;
	} else {
		slot->status = completion.status;
	}
	slot->consumed = 1;
	return TR_OK;
}

int tr_tls_provider_operation_table_init(
        struct tr_tls_provider_operation_table *table, uint32_t capacity,
        struct tr_memory_budget *budget)
{
	size_t storage_bytes;
	int ret;
	if (!table || !budget || capacity == 0U)
		return TR_ERR_INVALID;
	ret = tr_tls_provider_operation_size_multiply(
	        (size_t)capacity, sizeof(struct tr_tls_provider_operation_slot),
	        &storage_bytes);
	if (ret != TR_OK)
		return ret;
	memset(table, 0, sizeof(*table));
	ret = tr_memory_budget_reserve(budget, (uint64_t)storage_bytes);
	if (ret != TR_OK)
		return ret;
	table->slots = calloc((size_t)capacity, sizeof(*table->slots));
	if (!table->slots) {
		(void)tr_memory_budget_release(budget, (uint64_t)storage_bytes);
		return TR_ERR_NOMEM;
	}
	if (pthread_mutex_init(&table->lock, NULL) != 0) {
		free(table->slots);
		table->slots = NULL;
		(void)tr_memory_budget_release(budget, (uint64_t)storage_bytes);
		return TR_ERR_SYS;
	}
	ret = tr_async_operation_pool_create(capacity, budget,
	                                     &table->operations);
	if (ret != TR_OK) {
		(void)pthread_mutex_destroy(&table->lock);
		free(table->slots);
		table->slots = NULL;
		(void)tr_memory_budget_release(budget, (uint64_t)storage_bytes);
		return ret;
	}
	table->budget = budget;
	table->slot_storage_bytes = (uint64_t)storage_bytes;
	table->capacity = capacity;
	table->lock_ready = 1;
	return TR_OK;
}

int tr_tls_provider_operation_table_destroy(
        struct tr_tls_provider_operation_table *table)
{
	int ret;
	if (!table || !table->slots || !table->budget)
		return TR_ERR_INVALID;
	if (table->lock_ready) {
		ret = tr_tls_provider_operation_lock(table);
		if (ret != TR_OK)
			return ret;
		ret = TR_OK;
		for (uint32_t index = 0U; index < table->capacity; ++index) {
			if (table->slots[index].active) {
				(void)pthread_mutex_unlock(&table->lock);
				return TR_AGAIN;
			}
		}
		if (table->operations) {
			ret = tr_tls_provider_operation_owner_locked(table);
			if (ret == TR_OK)
				ret = tr_async_operation_pool_close(
				        table->operations);
			if (ret == TR_OK)
				ret = tr_async_operation_pool_destroy(
				        &table->operations);
		}
		if (pthread_mutex_unlock(&table->lock) != 0 && ret == TR_OK)
			ret = TR_ERR_SYS;
		if (ret != TR_OK)
			return ret;
		if (pthread_mutex_destroy(&table->lock) != 0)
			return TR_ERR_SYS;
		table->lock_ready = 0;
	} else if (table->operations) {
		return TR_ERR_STATE;
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
	struct tr_async_operation_spec spec;
	struct tr_async_operation operation;
	struct tr_async_operation_ref producer_ref;
	struct tr_tls_provider_operation public_operation;
	uint8_t *result = NULL;
	int ret;
	if (!table || !out_operation ||
	    !tr_tls_provider_operation_kind_valid(kind))
		return TR_ERR_INVALID;
	ret = tr_tls_provider_operation_lock(table);
	if (ret != TR_OK)
		return ret;
	ret = tr_tls_provider_operation_owner_locked(table);
	if (ret != TR_OK)
		goto done;
	spec = (struct tr_async_operation_spec){
	        .kind = TR_ASYNC_OPERATION_KIND_TLS_PROVIDER,
	        .domain_generation = (uint64_t)kind,
	        .object_generation = 0U,
	};
	ret = tr_async_operation_reserve(table->operations, &spec, &operation,
	                                 &producer_ref);
	if (ret != TR_OK)
		goto done;
	if (result_capacity != 0U) {
		ret = tr_memory_budget_reserve(table->budget, result_capacity);
		if (ret != TR_OK)
			goto reject;
		result = malloc(result_capacity);
		if (!result) {
			(void)tr_memory_budget_release(table->budget,
			                               result_capacity);
			ret = TR_ERR_NOMEM;
			goto reject;
		}
	}
	slot = &table->slots[operation.slot];
	*slot = (struct tr_tls_provider_operation_slot){
	        .operation = operation,
	        .producer_ref = producer_ref,
	        .result = result,
	        .result_capacity = result_capacity,
	        .kind = kind,
	        .status = TR_AGAIN,
	        .cancel_reason = TR_OK,
	        .active = 1,
	};
	/* 所有权已由 slot 接管；后续回滚只从 slot 取出并释放结果缓冲。 */
	result = NULL;
	if (table->instance == 0U)
		table->instance = operation.instance;
	else if (table->instance != operation.instance) {
		if (slot->result) {
			OPENSSL_cleanse(slot->result, result_capacity);
			free(slot->result);
			(void)tr_memory_budget_release(table->budget,
			                               result_capacity);
			slot->result = NULL;
		}
		memset(slot, 0, sizeof(*slot));
		ret = TR_ERR_STATE;
		goto reject;
	}
	public_operation = (struct tr_tls_provider_operation){
	        .instance = operation.instance,
	        .generation = operation.generation,
	        .slot = operation.slot,
	        .kind = kind,
	};
	ret = TR_OK;
	goto done;
reject:
	(void)tr_async_operation_reject(table->operations, operation,
	                                &producer_ref);
done:
	if (pthread_mutex_unlock(&table->lock) != 0) {
		if (ret != TR_OK)
			return ret;
		if (!slot) {
			(void)pthread_mutex_unlock(&table->lock);
			return TR_ERR_SYS;
		}
		/* reserve 尚未向调用方发布句柄。撤销底层预留并释放 slot
		 * 资源，随后重试解锁，避免错误返回留下不可访问的操作。 */
		if (tr_async_operation_reject(table->operations, operation,
		                              &producer_ref) == TR_OK) {
			if (slot->result) {
				OPENSSL_cleanse(slot->result,
				                slot->result_capacity);
				free(slot->result);
				(void)tr_memory_budget_release(
				        table->budget, slot->result_capacity);
			}
			memset(slot, 0, sizeof(*slot));
		}
		(void)pthread_mutex_unlock(&table->lock);
		return TR_ERR_SYS;
	}
	/* GCC 11 -fanalyzer does not follow the result pointer into the
	 * table-owned slot. The buffer remains owned there until
	 * retire/destroy. */
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wanalyzer-malloc-leak"
#endif
	if (ret == TR_OK)
		*out_operation = public_operation;
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif
	return ret;
}

int tr_tls_provider_operation_accept(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation operation)
{
	struct tr_tls_provider_operation_slot *slot;
	struct tr_async_operation_ref completion_ref = {0};
	int ready = 0;
	int ret = tr_tls_provider_operation_lock(table);
	if (ret != TR_OK)
		return ret;
	ret = tr_tls_provider_operation_lookup_locked(table, operation, &slot);
	if (ret == TR_OK && slot->accepted)
		ret = TR_ERR_STATE;
	if (ret == TR_OK)
		ret = tr_tls_provider_operation_owner_locked(table);
	if (ret == TR_OK)
		ret = tr_async_operation_accept(table->operations,
		                                slot->operation,
		                                &completion_ref, &ready);
	if (ret == TR_OK) {
		slot->accepted = 1;
		if (ready)
			slot->completion_ref = completion_ref;
		if (slot->cancel_requested)
			ret = slot->cancel_reason;
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
	int completion_status;
	if (ret != TR_OK)
		return ret;
	ret = tr_tls_provider_operation_lookup_locked(table, operation, &slot);
	if (ret != TR_OK)
		goto done;
	if (!slot->accepted || slot->completed) {
		ret = TR_ERR_STATE;
		goto done;
	}
	if (slot->cancel_requested) {
		completion_status = slot->cancel_reason;
		slot->result_len = 0U;
	} else if ((status != TR_OK && status >= 0) ||
	           (result_len != 0U && !result)) {
		completion_status = TR_ERR_INVALID;
		ret = TR_ERR_INVALID;
		slot->result_len = 0U;
	} else if (status == TR_OK && result_len > slot->result_capacity) {
		completion_status = TR_ERR_BAD_LENGTH;
		ret = TR_ERR_BAD_LENGTH;
		slot->result_len = 0U;
	} else {
		completion_status = status;
		slot->result_len = status == TR_OK ? result_len : 0U;
		if (slot->result_len != 0U)
			memcpy(slot->result, result, slot->result_len);
	}
	slot->credentials_result = 0;
	{
		int publish_ret = tr_tls_provider_operation_publish_locked(
		        table, slot, completion_status, completion_status);
		if (publish_ret != TR_OK)
			ret = publish_ret;
	}
done:
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
	size_t lengths[3] = {0U, 0U, 0U};
	const struct tr_tls_bytes *fields[3];
	int ret = tr_tls_provider_operation_lock(table);
	int completion_status;
	if (ret != TR_OK)
		return ret;
	ret = tr_tls_provider_operation_lookup_locked(table, operation, &slot);
	if (ret == TR_OK &&
	    operation.kind != TR_TLS_PROVIDER_OPERATION_CREDENTIALS)
		ret = TR_ERR_INVALID;
	if (ret != TR_OK)
		goto done;
	if (!slot->accepted || slot->completed) {
		ret = TR_ERR_STATE;
		goto done;
	}
	if (slot->cancel_requested) {
		completion_status = slot->cancel_reason;
		slot->result_len = 0U;
		slot->credentials_result = 0;
	} else if (status != TR_OK && status >= 0) {
		completion_status = TR_ERR_INVALID;
		ret = TR_ERR_INVALID;
		slot->result_len = 0U;
		slot->credentials_result = 0;
	} else if (status != TR_OK) {
		completion_status = status;
		slot->result_len = 0U;
		slot->credentials_result = 0;
	} else if (!bundle || !bundle->certificate_chain_pem.data ||
	           bundle->certificate_chain_pem.len == 0U ||
	           !bundle->private_key_pem.data ||
	           bundle->private_key_pem.len == 0U ||
	           (bundle->private_key_password.len != 0U &&
	            !bundle->private_key_password.data)) {
		completion_status = TR_ERR_INVALID;
		ret = TR_ERR_INVALID;
		slot->result_len = 0U;
		slot->credentials_result = 0;
	} else {
		fields[0] = &bundle->certificate_chain_pem;
		fields[1] = &bundle->private_key_pem;
		fields[2] = &bundle->private_key_password;
		for (size_t index = 0U; index < 3U; ++index) {
			if (fields[index]->len > SIZE_MAX - total) {
				ret = TR_ERR_BAD_LENGTH;
				break;
			}
			lengths[index] = fields[index]->len;
			total += lengths[index];
		}
		if (ret == TR_OK && total > slot->result_capacity)
			ret = TR_ERR_BAD_LENGTH;
		if (ret != TR_OK) {
			completion_status = ret;
			slot->result_len = 0U;
			slot->credentials_result = 0;
		} else {
			size_t offset = 0U;
			for (size_t index = 0U; index < 3U; ++index) {
				if (lengths[index] != 0U)
					memcpy(slot->result + offset,
					       fields[index]->data,
					       lengths[index]);
				offset += lengths[index];
			}
			slot->certificate_chain_len = lengths[0];
			slot->private_key_len = lengths[1];
			slot->private_key_password_len = lengths[2];
			slot->result_len = total;
			slot->credentials_result = 1;
			completion_status = TR_OK;
		}
	}
	{
		int publish_ret = tr_tls_provider_operation_publish_locked(
		        table, slot, completion_status, completion_status);
		if (publish_ret != TR_OK)
			ret = publish_ret;
	}
done:
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
	if (ret != TR_OK)
		goto done;
	if (slot->completed) {
		ret = TR_OK;
	} else if (!slot->accepted && slot->producer_ref.token != 0U) {
		ret = tr_tls_provider_operation_publish_locked(table, slot,
		                                               reason, reason);
		if (ret == TR_OK) {
			slot->cancel_requested = 1;
			slot->cancel_reason = reason;
		}
	} else {
		ret = tr_async_operation_request_cancel(
		        table->operations, slot->operation, reason, &notify);
		if (ret == TR_OK && notify) {
			slot->cancel_requested = 1;
			slot->cancel_reason = reason;
		} else if (ret == TR_ERR_STATE && slot->completed) {
			ret = TR_OK;
		}
	}
	if (ret == TR_OK)
		*out_notify_provider = notify;
done:
	if (pthread_mutex_unlock(&table->lock) != 0 && ret == TR_OK)
		return TR_ERR_SYS;
	return ret;
}

int tr_tls_provider_operation_result(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation operation, int *out_status,
        void *out_result, size_t result_capacity, size_t *out_result_len)
{
	struct tr_tls_provider_operation_slot *slot;
	int ret;
	if (!out_status || !out_result_len)
		return TR_ERR_INVALID;
	ret = tr_tls_provider_operation_lock(table);
	if (ret != TR_OK)
		return ret;
	ret = tr_tls_provider_operation_lookup_locked(table, operation, &slot);
	if (ret == TR_OK)
		ret = tr_tls_provider_operation_consume_locked(table, slot);
	if (ret == TR_OK && (slot->result_len > result_capacity ||
	                     (slot->result_len != 0U && !out_result)))
		ret = TR_ERR_BAD_LENGTH;
	if (ret == TR_OK) {
		if (slot->result_len != 0U)
			memcpy(out_result, slot->result, slot->result_len);
		*out_status = slot->status;
		*out_result_len = slot->result_len;
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
	if (ret == TR_OK)
		ret = tr_tls_provider_operation_consume_locked(table, slot);
	if (ret == TR_OK) {
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
	if (pthread_mutex_unlock(&table->lock) != 0 && ret == TR_OK)
		return TR_ERR_SYS;
	return ret;
}

int tr_tls_provider_operation_retire(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation operation)
{
	struct tr_tls_provider_operation_slot *slot;
	uint64_t generation;
	int ret = tr_tls_provider_operation_lock(table);
	if (ret != TR_OK)
		return ret;
	ret = tr_tls_provider_operation_lookup_locked(table, operation, &slot);
	if (ret == TR_OK && !slot->completed)
		ret = TR_AGAIN;
	if (ret == TR_OK)
		ret = tr_tls_provider_operation_consume_locked(table, slot);
	if (ret == TR_OK)
		ret = tr_tls_provider_operation_owner_locked(table);
	if (ret == TR_OK)
		ret = tr_async_operation_retire(table->operations,
		                                &slot->completion_ref);
	if (ret == TR_OK) {
		generation = slot->operation.generation;
		if (slot->result) {
			OPENSSL_cleanse(slot->result, slot->result_capacity);
			free(slot->result);
		}
		(void)tr_memory_budget_release(table->budget,
		                               slot->result_capacity);
		memset(slot, 0, sizeof(*slot));
		slot->operation.generation = generation;
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
	ret = tr_tls_provider_operation_owner_locked(table);
	if (ret == TR_OK)
		ret = tr_async_operation_test_set_generation(table->operations,
		                                             slot, generation);
	if (pthread_mutex_unlock(&table->lock) != 0 && ret == TR_OK)
		return TR_ERR_SYS;
	return ret;
}
