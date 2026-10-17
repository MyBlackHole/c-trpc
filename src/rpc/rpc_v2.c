#include "rpc_v2_internal.h"
#include "rpc_record.h"
#include "../endian.h"
#include "../facade_tuning_internal.h"
#include "tr/status.h"

#include <stdbool.h>
#include <string.h>

int tr_rpc_v2_metadata_encode(const struct tr_rpc_metadata *entries,
                              size_t count, uint8_t *out, uint32_t *size)
{
	size_t used = 0;
	if (!out || !size || (count && !entries) ||
	    count > TR_RPC_METADATA_MAX_BYTES / 4U)
		return TR_ERR_INVALID;
	for (size_t i = 0; i < count; ++i) {
		const struct tr_rpc_metadata *entry = &entries[i];
		size_t key_size = 0, entry_size;
		if (!entry->key || (entry->value_len && !entry->value))
			return TR_ERR_INVALID;
		while (key_size <= TR_RPC_METADATA_MAX_KEY_LEN &&
		       entry->key[key_size])
			++key_size;
		entry_size = 3U + key_size + entry->value_len;
		if (!key_size || key_size > TR_RPC_METADATA_MAX_KEY_LEN ||
		    entry_size > TR_RPC_METADATA_MAX_BYTES - used)
			return TR_ERR_BAD_LENGTH;
		for (size_t j = 0; j < key_size; ++j) {
			unsigned char ch = (unsigned char)entry->key[j];
			if (!((ch >= 'a' && ch <= 'z') ||
			      (ch >= '0' && ch <= '9') || ch == '_' ||
			      ch == '.' || ch == '-'))
				return TR_ERR_BAD_FLAGS;
		}
		for (size_t prior = 0; prior < used;) {
			size_t n = out[prior];
			if (n == key_size &&
			    !memcmp(out + prior + 3U, entry->key, n))
				return TR_ERR_BAD_FLAGS;
			prior += 3U + n + tr_get_le16(out + prior + 1U);
		}
		out[used] = (uint8_t)key_size;
		tr_put_le16(out + used + 1U, entry->value_len);
		memcpy(out + used + 3U, entry->key, key_size);
		if (entry->value_len)
			memcpy(out + used + 3U + key_size, entry->value,
			       entry->value_len);
		used += entry_size;
	}
	*size = (uint32_t)used;
	return TR_OK;
}

void tr_rpc_v2_metadata_view_init(struct tr_rpc_v2_metadata_view *out,
                                  const uint8_t *data, size_t size)
{
	if (!out)
		return;
	out->_private[0] = (uintptr_t)data;
	out->_private[1] = (uintptr_t)size;
}

/* 公共访问器保留长度检查；视图地址的存活期仍由回调借用合同保证。
 * 解析器不向外泄漏编码字节，只返回精确长度的键值。
 */
static int tr_rpc_v2_metadata_next(const struct tr_rpc_v2_metadata_view *view,
                                   size_t *offset,
                                   struct tr_rpc_v2_metadata_entry *entry)
{
	const uint8_t *data;
	size_t size, pos, key_size, value_size;
	if (!view || !offset || !entry)
		return TR_ERR_INVALID;
	data = (const uint8_t *)view->_private[0];
	size = (size_t)view->_private[1];
	pos = *offset;
	if (size > TR_RPC_METADATA_MAX_BYTES || (size && !data) || pos > size)
		return TR_ERR_INVALID;
	if (pos == size)
		return TR_ERR_STALE;
	if (size - pos < 3U)
		return TR_ERR_INVALID;
	key_size = data[pos];
	value_size = (size_t)data[pos + 1U] | ((size_t)data[pos + 2U] << 8U);
	pos += 3U;
	if (!key_size || key_size > TR_RPC_METADATA_MAX_KEY_LEN ||
	    key_size > size - pos || value_size > size - pos - key_size)
		return TR_ERR_INVALID;
	entry->key.data = data + pos;
	entry->key.len = (uint32_t)key_size;
	entry->value.data = value_size ? data + pos + key_size : NULL;
	entry->value.len = (uint32_t)value_size;
	*offset = pos + key_size + value_size;
	return TR_OK;
}

size_t tr_rpc_v2_metadata_count(const struct tr_rpc_v2_metadata_view *view)
{
	struct tr_rpc_v2_metadata_entry entry;
	size_t count = 0, offset = 0;
	int ret;
	while ((ret = tr_rpc_v2_metadata_next(view, &offset, &entry)) == TR_OK)
		++count;
	return ret == TR_ERR_STALE ? count : 0;
}

int tr_rpc_v2_metadata_at(const struct tr_rpc_v2_metadata_view *view,
                          size_t index, struct tr_rpc_v2_metadata_entry *out)
{
	struct tr_rpc_v2_metadata_entry entry;
	size_t offset = 0;
	int ret;
	if (!out)
		return TR_ERR_INVALID;
	for (size_t i = 0;; ++i) {
		ret = tr_rpc_v2_metadata_next(view, &offset, &entry);
		if (ret != TR_OK)
			return ret;
		if (i == index) {
			*out = entry;
			return TR_OK;
		}
	}
}

int tr_rpc_v2_metadata_find(const struct tr_rpc_v2_metadata_view *view,
                            const char *key, struct tr_rpc_bytes *out)
{
	struct tr_rpc_v2_metadata_entry entry;
	size_t key_size = 0, offset = 0;
	int ret;
	if (!key || !out)
		return TR_ERR_INVALID;
	while (key_size <= TR_RPC_METADATA_MAX_KEY_LEN && key[key_size])
		++key_size;
	if (!key_size || key_size > TR_RPC_METADATA_MAX_KEY_LEN)
		return TR_ERR_INVALID;
	while ((ret = tr_rpc_v2_metadata_next(view, &offset, &entry)) ==
	       TR_OK) {
		if (entry.key.len == key_size &&
		    memcmp(entry.key.data, key, key_size) == 0) {
			*out = entry.value;
			return TR_OK;
		}
	}
	return ret;
}

int tr_rpc_v2_prepare_limits(struct tr_facade_limits *limits)
{
	struct tr_facade_limits effective;
	uint64_t minimum, record_limit, threshold;
	if (!limits || !limits->max_message_bytes ||
	    !limits->initial_window_bytes || !limits->max_frame_payload_bytes ||
	    limits->max_message_bytes < limits->max_frame_payload_bytes ||
	    limits->max_message_bytes >
	            UINT32_MAX - TR_RPC_RECORD_HEADER_SIZE ||
	    limits->initial_window_bytes >
	            UINT64_MAX - TR_RPC_RECORD_HEADER_SIZE)
		return TR_ERR_INVALID;
	effective = *limits;
	minimum = TR_RPC_RECORD_HEADER_SIZE + TR_RPC_METADATA_MAX_BYTES;
	effective.max_message_bytes += TR_RPC_RECORD_HEADER_SIZE;
	if (effective.max_message_bytes < minimum)
		effective.max_message_bytes = (uint32_t)minimum;
	effective.initial_window_bytes += TR_RPC_RECORD_HEADER_SIZE;
	if (effective.initial_window_bytes < minimum)
		effective.initial_window_bytes = minimum;
	record_limit = effective.max_message_bytes;
	if (record_limit > effective.initial_window_bytes)
		record_limit = effective.initial_window_bytes;
	/* 一整条记录原子准入，未公告的已消费信用不能挡住下一条合法记录。 */
	threshold = effective.initial_window_bytes - record_limit + 1U;
	if (!effective.window_update_threshold_bytes ||
	    effective.window_update_threshold_bytes > threshold)
		effective.window_update_threshold_bytes = threshold;
	*limits = effective;
	return TR_OK;
}

static bool tr_rpc_v2_add_product(uint64_t *total, uint64_t count,
                                  uint64_t size)
{
	if (count && size > (UINT64_MAX - *total) / count)
		return false;
	*total += count * size;
	return true;
}

int tr_rpc_v2_memory_limit(const struct tr_facade_limits *limits,
                           const struct tr_facade_tuning *tuning,
                           uint32_t shards, uint64_t *out)
{
	uint64_t total = 0, retained;
	if (!limits || !tuning || !out || !shards ||
	    !limits->max_message_bytes || !limits->max_frame_payload_bytes)
		return TR_ERR_INVALID;
	/* 预算是内部计费分配的硬上限，不是预分配量或 RSS 上限。
	 * 固定池按声明容量计费；动态正文在每分片 64 条最大正文的资源额度内
	 * 与任务/回复共享预算。额外每分片 64 MiB 覆盖表、描述符和控制记录。
	 * 不按 peers*calls*pending 乘出近乎无界的正文承诺；过载有界失败。
	 */
	retained = UINT64_C(64) * shards;
	if (retained < tuning->rpc_message_pool_count)
		retained = tuning->rpc_message_pool_count;
	if (!tr_rpc_v2_add_product(&total, shards,
	                           UINT64_C(64) * 1024U * 1024U) ||
	    !tr_rpc_v2_add_product(&total, tuning->rx_buffer_count,
	                           limits->max_frame_payload_bytes) ||
	    !tr_rpc_v2_add_product(&total, tuning->reassembly_pool_count,
	                           limits->max_message_bytes) ||
	    !tr_rpc_v2_add_product(&total, retained,
	                           limits->max_message_bytes) ||
	    total > SIZE_MAX)
		return TR_ERR_INVALID;
	*out = total;
	return TR_OK;
}
