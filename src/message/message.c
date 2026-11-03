#include "message.h"
#include "storage.h"
#include "../memory_budget.h"
#include "tr/status.h"

/* 预约与生命周期状态由提供方串行修改；LIVE 期间仅引用计数变化。
 * 最后一次 put 获得独占清理责任，其后不得再通过任何裸指针取得引用。
 */
enum tr_message_storage_state {
	TR_MESSAGE_STORAGE_EMPTY = 0,
	TR_MESSAGE_STORAGE_RESERVED,
	TR_MESSAGE_STORAGE_LIVE,
	TR_MESSAGE_STORAGE_RETIRED
};

_Static_assert(SIZE_MAX <= UINT64_MAX,
               "message budget requires <= 64-bit size_t");

int tr_message_storage_reserve(struct tr_message_storage *storage,
                               struct tr_memory_budget *budget, size_t capacity)
{
	int ret;

	if (!storage || !budget)
		return TR_ERR_INVALID;
	if (storage->_state != TR_MESSAGE_STORAGE_EMPTY)
		return TR_ERR_STATE;

	ret = tr_memory_budget_reserve(budget, (uint64_t)capacity);
	if (ret != TR_OK)
		return ret;
	storage->_budget = budget;
	storage->_capacity = (uint64_t)capacity;
	storage->_state = TR_MESSAGE_STORAGE_RESERVED;
	return TR_OK;
}

int tr_message_storage_cancel(struct tr_message_storage *storage)
{
	int ret;

	if (!storage)
		return TR_ERR_INVALID;
	if (storage->_state != TR_MESSAGE_STORAGE_RESERVED)
		return TR_ERR_STATE;

	ret = tr_memory_budget_release(storage->_budget, storage->_capacity);
	if (ret != TR_OK)
		return ret;
	storage->_budget = NULL;
	storage->_capacity = 0;
	storage->_state = TR_MESSAGE_STORAGE_EMPTY;
	return TR_OK;
}

int tr_message_storage_publish(struct tr_message_storage *storage,
                               const uint8_t *data, size_t len,
                               tr_message_storage_release_fn release, void *arg,
                               struct tr_message *out)
{
	int ret;

	if (!storage || !release || !out)
		return TR_ERR_INVALID;
	if (storage->_state != TR_MESSAGE_STORAGE_RESERVED || out->_storage)
		return TR_ERR_STATE;
	if ((uint64_t)len > storage->_capacity)
		return TR_ERR_BAD_LENGTH;
	if (!data && storage->_capacity != 0)
		return TR_ERR_INVALID;

	ret = tr_refcount_init(&storage->_refs, 1U);
	if (ret != TR_OK)
		return ret;
	storage->_release = release;
	storage->_release_arg = arg;
	storage->_state = TR_MESSAGE_STORAGE_LIVE;
	out->_data = data;
	out->_len = len;
	out->_storage = storage;
	return TR_OK;
}

int tr_message_view(const struct tr_message *message,
                    struct tr_message_view *out)
{
	if (!message || !out)
		return TR_ERR_INVALID;
	if (!message->_storage)
		return TR_ERR_STATE;
	out->data = message->_data;
	out->len = message->_len;
	return TR_OK;
}

int tr_message_slice(const struct tr_message *message, size_t offset,
                     size_t len, struct tr_message *out)
{
	const uint8_t *data;
	int ret;

	if (!message || !out || message == out)
		return TR_ERR_INVALID;
	if (!message->_storage || out->_storage)
		return TR_ERR_STATE;
	if (offset > message->_len || len > message->_len - offset)
		return TR_ERR_BAD_LENGTH;

	/* 合法空消息可使用 NULL；避免对 NULL 做哪怕 +0 的指针运算。 */
	data = message->_data;
	if (offset != 0)
		data += offset;
	ret = tr_refcount_get(&message->_storage->_refs);
	if (ret != TR_OK)
		return ret;
	out->_data = data;
	out->_len = len;
	out->_storage = message->_storage;
	return TR_OK;
}

int tr_message_clone(const struct tr_message *message, struct tr_message *out)
{
	if (!message)
		return TR_ERR_INVALID;
	return tr_message_slice(message, 0, message->_len, out);
}

int tr_message_none_move(struct tr_message *input,
                         const struct tr_message_limits *limits,
                         struct tr_message *out)
{
	if (!input || !limits || !out || input == out)
		return TR_ERR_INVALID;
	if (!input->_storage || out->_storage)
		return TR_ERR_STATE;
	if (input->_len > limits->max_decoded_bytes ||
	    input->_len > limits->max_encoded_bytes)
		return TR_ERR_BAD_LENGTH;

	*out = *input;
	*input = (struct tr_message){0};
	return TR_OK;
}

int tr_message_release(struct tr_message *message)
{
	struct tr_message_storage *storage;
	struct tr_memory_budget *budget;
	tr_message_storage_release_fn release;
	uint64_t capacity;
	void *arg;
	int last;

	if (!message)
		return TR_ERR_INVALID;
	storage = message->_storage;
	if (!storage)
		return TR_ERR_STATE;

	/* 句柄可以嵌在最终回调将释放的对象里，故必须先清空再放下引用。 */
	*message = (struct tr_message){0};
	last = tr_refcount_put(&storage->_refs);
	if (last < 0)
		return last;
	if (last == 0)
		return TR_OK;

	budget = storage->_budget;
	capacity = storage->_capacity;
	release = storage->_release;
	arg = storage->_release_arg;
	storage->_state = TR_MESSAGE_STORAGE_RETIRED;
	/* 回调可以释放 storage，返回后只能使用此前保存的局部值。 */
	release(arg);
	return tr_memory_budget_release(budget, capacity);
}
