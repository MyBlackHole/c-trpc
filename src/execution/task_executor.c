#include "task_executor.h"
#include "task_executor_internal.h"
#include "task_executor_recovery.h"
#include "../memory_budget.h"
#include "tr/status.h"

#include <pthread.h>
#include <stdlib.h>

#define TR_TASK_END UINT32_MAX

enum tr_task_state {
	TR_TASK_FREE,
	TR_TASK_QUEUED,
	TR_TASK_RUNNING,
	TR_TASK_COMPLETED,
	TR_TASK_DELIVERING
};

enum tr_executor_state {
	TR_EXECUTOR_NEW,
	TR_EXECUTOR_STARTING,
	TR_EXECUTOR_OPEN,
	TR_EXECUTOR_STOPPING,
	TR_EXECUTOR_JOINED
};

struct tr_task_cancel {
	_Atomic bool requested;
};

struct tr_task_slot {
	struct tr_task_spec spec;
	struct tr_task_cancel cancel;
	struct tr_task_result result;
	uint64_t generation;
	uint32_t next;
	enum tr_task_state state;
};

struct tr_task_executor {
	pthread_mutex_t lock;
	pthread_cond_t work_available;
	pthread_t owner;
	pthread_t *threads;
	struct tr_task_slot *slots;
	struct tr_memory_budget *budget;
	size_t bytes;
	size_t workers;
	size_t started;
	size_t joined;
	uint64_t instance;
	uint32_t free_head;
	uint32_t pending_head;
	uint32_t pending_tail;
	uint32_t complete_head;
	uint32_t complete_tail;
	struct tr_task_executor_stats stats;
	enum tr_executor_state state;
	bool dispatching;
	/* 开始最终销毁后只准所有者重试 destroy，不再接受其他 API。 */
	bool retiring;
	bool condition_destroyed;
	bool recovery_owned;
	tr_task_notify_fn notify;
	void *notify_arg;
};

static _Atomic uint64_t tr_task_instances;

/* 所有正常同步对象均为成功初始化且未销毁的非 robust pthread 原语。
 * 队列、槽位、代次和统计由 lock 保护，工作线程只无锁读取取消令牌。
 */
static void tr_task_enqueue(struct tr_task_executor *executor, uint32_t *head,
                            uint32_t *tail, uint32_t index)
{
	executor->slots[index].next = TR_TASK_END;
	if (*tail != TR_TASK_END)
		executor->slots[*tail].next = index;
	else
		*head = index;
	*tail = index;
}

static uint32_t tr_task_dequeue(struct tr_task_executor *executor,
                                uint32_t *head, uint32_t *tail)
{
	uint32_t index = *head;

	*head = executor->slots[index].next;
	if (*head == TR_TASK_END)
		*tail = TR_TASK_END;
	return index;
}

static void *tr_task_worker(void *arg)
{
	struct tr_task_executor *executor = arg;

	(void)pthread_mutex_lock(&executor->lock);
	for (;;) {
		struct tr_task_slot *slot;
		uint32_t index;
		int status;

		while (executor->pending_head == TR_TASK_END &&
		       executor->state != TR_EXECUTOR_STOPPING)
			(void)pthread_cond_wait(&executor->work_available,
			                        &executor->lock);
		if (executor->pending_head == TR_TASK_END)
			break;
		index = tr_task_dequeue(executor, &executor->pending_head,
		                        &executor->pending_tail);
		slot = &executor->slots[index];
		slot->state = TR_TASK_RUNNING;
		--executor->stats.queued;
		++executor->stats.running;
		(void)pthread_mutex_unlock(&executor->lock);

		status = slot->spec.work(slot->spec.arg, &slot->cancel);

		(void)pthread_mutex_lock(&executor->lock);
		/* 取消与此终局提交均在同一把锁下。已完成任务不再被取消改写。
		 * 完成记录复用原槽位，无需分配或等待另一条完成队列的容量。
		 */
		slot->result.work_status = status;
		slot->result.cancelled =
		        tr_task_cancel_requested(&slot->cancel);
		slot->state = TR_TASK_COMPLETED;
		--executor->stats.running;
		++executor->stats.completed;
		tr_task_enqueue(executor, &executor->complete_head,
		                &executor->complete_tail, index);
		/* 先发布完成，再唤醒；通知不接管槽位，失败仍由完成队列持有。 */
		if (executor->notify)
			executor->notify(executor->notify_arg);
	}
	--executor->stats.workers_alive;
	(void)pthread_mutex_unlock(&executor->lock);
	return NULL;
}

int tr_task_executor_create(const struct tr_task_executor_config *config,
                            struct tr_memory_budget *budget,
                            struct tr_task_executor **out)
{
	struct tr_task_executor *executor;
	size_t bytes;
	uint64_t instance;
	int ret;

	if (!config || !budget || !out || budget->limit_bytes == 0 ||
	    config->workers == 0 || config->workers > config->capacity ||
	    config->capacity == 0 || config->capacity >= TR_TASK_END)
		return TR_ERR_INVALID;
	if (*out)
		return TR_ERR_STATE;
	if (config->capacity >
	    (SIZE_MAX - sizeof(*executor)) / sizeof(struct tr_task_slot))
		return TR_ERR_BAD_LENGTH;
	bytes = sizeof(*executor) +
	        config->capacity * sizeof(struct tr_task_slot);
	if (config->workers > (SIZE_MAX - bytes) / sizeof(pthread_t))
		return TR_ERR_BAD_LENGTH;
	bytes += config->workers * sizeof(pthread_t);
	ret = tr_task_instance_next(&tr_task_instances, &instance);
	if (ret != TR_OK)
		return ret;
	ret = tr_memory_budget_reserve(budget, bytes);
	if (ret != TR_OK)
		return ret;
	executor = calloc(1, sizeof(*executor));
	if (!executor) {
		(void)tr_memory_budget_release(budget, bytes);
		return TR_ERR_NOMEM;
	}
	executor->slots = calloc(config->capacity, sizeof(*executor->slots));
	executor->threads = calloc(config->workers, sizeof(*executor->threads));
	if (!executor->slots || !executor->threads) {
		ret = TR_ERR_NOMEM;
		goto fail;
	}
	ret = pthread_mutex_init(&executor->lock, NULL);
	if (ret != 0) {
		ret = TR_ERR_SYS;
		goto fail;
	}
	ret = pthread_cond_init(&executor->work_available, NULL);
	if (ret != 0) {
		(void)pthread_mutex_destroy(&executor->lock);
		ret = TR_ERR_SYS;
		goto fail;
	}
	executor->owner = pthread_self();
	executor->budget = budget;
	executor->bytes = bytes;
	executor->workers = config->workers;
	executor->instance = instance;
	executor->stats.capacity = config->capacity;
	executor->pending_head = executor->pending_tail = TR_TASK_END;
	executor->complete_head = executor->complete_tail = TR_TASK_END;
	for (size_t i = 0; i < config->capacity; ++i) {
		executor->slots[i].next = i + 1 < config->capacity
		                                  ? (uint32_t)(i + 1)
		                                  : TR_TASK_END;
		atomic_init(&executor->slots[i].cancel.requested, false);
	}
	*out = executor;
	return TR_OK;

fail:
	free(executor->threads);
	free(executor->slots);
	free(executor);
	(void)tr_memory_budget_release(budget, bytes);
	return ret;
}

int tr_task_executor_takeover_quiesced(struct tr_task_executor *executor)
{
	int ret = TR_OK;

	if (!executor)
		return TR_ERR_INVALID;
	(void)pthread_mutex_lock(&executor->lock);
	if (executor->dispatching ||
	    (!executor->retiring && executor->state != TR_EXECUTOR_STOPPING &&
	     executor->state != TR_EXECUTOR_JOINED) ||
	    (executor->recovery_owned &&
	     !pthread_equal(pthread_self(), executor->owner))) {
		ret = TR_ERR_STATE;
	} else {
		/* 旧 owner 已由组合层证明静默，不读取可能失效的 pthread_t。
		 * 通知也使用此锁，清除后可安全撤销其 fd/参数的外部发布。
		 * retiring 时条件变量可能已销毁，不能广播或再次 stop。
		 */
		executor->owner = pthread_self();
		executor->recovery_owned = true;
		executor->notify = NULL;
		executor->notify_arg = NULL;
	}
	(void)pthread_mutex_unlock(&executor->lock);
	return ret;
}

int tr_task_executor_set_notify(struct tr_task_executor *executor,
                                tr_task_notify_fn notify, void *arg)
{
	int ret = TR_OK;

	if (!executor || (!notify && arg))
		return TR_ERR_INVALID;
	if (!pthread_equal(pthread_self(), executor->owner))
		return TR_ERR_STATE;
	(void)pthread_mutex_lock(&executor->lock);
	if (executor->dispatching ||
	    (executor->state != TR_EXECUTOR_NEW &&
	     !(executor->state == TR_EXECUTOR_JOINED && !notify &&
	       executor->stats.outstanding == 0))) {
		ret = TR_ERR_STATE;
	} else {
		executor->notify = notify;
		executor->notify_arg = arg;
	}
	(void)pthread_mutex_unlock(&executor->lock);
	return ret;
}

int tr_task_executor_start(struct tr_task_executor *executor)
{
	int ret = TR_OK;

	if (!executor)
		return TR_ERR_INVALID;
	if (!pthread_equal(pthread_self(), executor->owner))
		return TR_ERR_STATE;
	(void)pthread_mutex_lock(&executor->lock);
	if (executor->state != TR_EXECUTOR_NEW) {
		(void)pthread_mutex_unlock(&executor->lock);
		return TR_ERR_STATE;
	}
	executor->state = TR_EXECUTOR_STARTING;
	/* 持锁创建：工作线程尚不能消费，其他提交者也不能看见半启动状态。 */
	while (executor->started < executor->workers) {
		if (pthread_create(&executor->threads[executor->started], NULL,
		                   tr_task_worker, executor) != 0) {
			ret = TR_ERR_SYS;
			break;
		}
		++executor->started;
		++executor->stats.workers_alive;
	}
	executor->state =
	        ret == TR_OK ? TR_EXECUTOR_OPEN : TR_EXECUTOR_STOPPING;
	executor->stats.accepting = ret == TR_OK;
	(void)pthread_cond_broadcast(&executor->work_available);
	(void)pthread_mutex_unlock(&executor->lock);
	return ret;
}

int tr_task_executor_submit(struct tr_task_executor *executor,
                            const struct tr_task_spec *spec,
                            struct tr_task_handle *out)
{
	struct tr_task_slot *slot;
	uint32_t index;
	int ret = TR_OK;

	if (!executor || !spec || !spec->work || !spec->done || !out)
		return TR_ERR_INVALID;
	(void)pthread_mutex_lock(&executor->lock);
	if (executor->state != TR_EXECUTOR_OPEN) {
		ret = executor->state >= TR_EXECUTOR_STOPPING ? TR_ERR_CLOSED
		                                              : TR_ERR_STATE;
		goto unlock;
	}
	index = executor->free_head;
	if (index == TR_TASK_END) {
		ret = TR_AGAIN;
		goto unlock;
	}
	slot = &executor->slots[index];
	ret = tr_task_generation_next(&slot->generation);
	if (ret != TR_OK)
		goto unlock;
	executor->free_head = slot->next;
	slot->spec = *spec;
	atomic_store_explicit(&slot->cancel.requested, false,
	                      memory_order_relaxed);
	slot->state = TR_TASK_QUEUED;
	tr_task_enqueue(executor, &executor->pending_head,
	                &executor->pending_tail, index);
	++executor->stats.queued;
	++executor->stats.outstanding;
	*out = (struct tr_task_handle){executor->instance, slot->generation,
	                               index};
	(void)pthread_cond_signal(&executor->work_available);
unlock:
	(void)pthread_mutex_unlock(&executor->lock);
	return ret;
}

bool tr_task_cancel_requested(const struct tr_task_cancel *cancel)
{
	return cancel &&
	       atomic_load_explicit(&cancel->requested, memory_order_acquire);
}

int tr_task_executor_cancel(struct tr_task_executor *executor,
                            struct tr_task_handle handle)
{
	struct tr_task_slot *slot;
	int ret = TR_OK;

	if (!executor)
		return TR_ERR_INVALID;
	(void)pthread_mutex_lock(&executor->lock);
	if (handle.instance != executor->instance ||
	    handle.slot >= executor->stats.capacity || handle.generation == 0) {
		ret = TR_ERR_STALE;
		goto unlock;
	}
	slot = &executor->slots[handle.slot];
	if (slot->state == TR_TASK_FREE ||
	    slot->generation != handle.generation)
		ret = TR_ERR_STALE;
	else if (slot->state != TR_TASK_QUEUED &&
	         slot->state != TR_TASK_RUNNING)
		ret = TR_ERR_STATE;
	else
		atomic_store_explicit(&slot->cancel.requested, true,
		                      memory_order_release);
unlock:
	(void)pthread_mutex_unlock(&executor->lock);
	return ret;
}

int tr_task_executor_dispatch(struct tr_task_executor *executor, size_t limit,
                              size_t *delivered)
{
	size_t count = 0;

	if (!executor || !delivered || limit == 0)
		return TR_ERR_INVALID;
	if (!pthread_equal(pthread_self(), executor->owner))
		return TR_ERR_STATE;
	(void)pthread_mutex_lock(&executor->lock);
	if (executor->dispatching) {
		(void)pthread_mutex_unlock(&executor->lock);
		return TR_ERR_STATE;
	}
	executor->dispatching = true;
	while (count < limit && executor->complete_head != TR_TASK_END) {
		uint32_t index =
		        tr_task_dequeue(executor, &executor->complete_head,
		                        &executor->complete_tail);
		struct tr_task_slot *slot = &executor->slots[index];
		struct tr_task_handle handle = {executor->instance,
		                                slot->generation, index};

		slot->state = TR_TASK_DELIVERING;
		--executor->stats.completed;
		++executor->stats.delivering;
		(void)pthread_mutex_unlock(&executor->lock);
		slot->spec.done(slot->spec.arg, handle, slot->result);
		(void)pthread_mutex_lock(&executor->lock);
		/* 回调可以释放 arg；其后仅修改引擎拥有的槽位，不再解引用 arg。
		 */
		slot->spec = (struct tr_task_spec){0};
		slot->state = TR_TASK_FREE;
		slot->next = executor->free_head;
		executor->free_head = index;
		--executor->stats.delivering;
		--executor->stats.outstanding;
		++count;
	}
	executor->dispatching = false;
	(void)pthread_mutex_unlock(&executor->lock);
	*delivered = count;
	return TR_OK;
}

int tr_task_executor_stop(struct tr_task_executor *executor,
                          enum tr_task_stop_mode mode)
{
	if (!executor || (mode != TR_TASK_DRAIN && mode != TR_TASK_CANCEL))
		return TR_ERR_INVALID;
	(void)pthread_mutex_lock(&executor->lock);
	if (executor->state != TR_EXECUTOR_JOINED)
		executor->state = TR_EXECUTOR_STOPPING;
	executor->stats.accepting = false;
	if (mode == TR_TASK_CANCEL) {
		for (size_t i = 0; i < executor->stats.capacity; ++i) {
			struct tr_task_slot *slot = &executor->slots[i];

			if (slot->state == TR_TASK_QUEUED ||
			    slot->state == TR_TASK_RUNNING)
				atomic_store_explicit(&slot->cancel.requested,
				                      true,
				                      memory_order_release);
		}
	}
	(void)pthread_cond_broadcast(&executor->work_available);
	(void)pthread_mutex_unlock(&executor->lock);
	return TR_OK;
}

int tr_task_executor_join(struct tr_task_executor *executor)
{
	int ret = TR_OK;

	if (!executor)
		return TR_ERR_INVALID;
	if (!pthread_equal(pthread_self(), executor->owner))
		return TR_ERR_STATE;
	(void)pthread_mutex_lock(&executor->lock);
	if (executor->dispatching || executor->state < TR_EXECUTOR_STOPPING)
		ret = TR_ERR_STATE;
	else if (executor->stats.workers_alive != 0)
		ret = TR_AGAIN;
	(void)pthread_mutex_unlock(&executor->lock);
	if (ret != TR_OK)
		return ret;
	while (executor->joined < executor->started) {
		if (pthread_join(executor->threads[executor->joined], NULL) !=
		    0)
			return TR_ERR_SYS;
		++executor->joined;
	}
	(void)pthread_mutex_lock(&executor->lock);
	executor->state = TR_EXECUTOR_JOINED;
	(void)pthread_mutex_unlock(&executor->lock);
	return TR_OK;
}

int tr_task_executor_get_stats(struct tr_task_executor *executor,
                               struct tr_task_executor_stats *out)
{
	if (!executor || !out)
		return TR_ERR_INVALID;
	(void)pthread_mutex_lock(&executor->lock);
	*out = executor->stats;
	(void)pthread_mutex_unlock(&executor->lock);
	return TR_OK;
}

int tr_task_executor_destroy(struct tr_task_executor **executor)
{
	struct tr_task_executor *current;
	struct tr_memory_budget *budget;
	size_t bytes;
	int ret = TR_OK;

	if (!executor || !*executor)
		return TR_ERR_INVALID;
	current = *executor;
	if (!pthread_equal(pthread_self(), current->owner))
		return TR_ERR_STATE;
	if (!current->retiring) {
		(void)pthread_mutex_lock(&current->lock);
		if (current->dispatching ||
		    (current->state != TR_EXECUTOR_NEW &&
		     current->state != TR_EXECUTOR_JOINED))
			ret = TR_ERR_STATE;
		else if (current->stats.outstanding != 0)
			ret = TR_AGAIN;
		if (ret == TR_OK)
			current->retiring = true;
		(void)pthread_mutex_unlock(&current->lock);
		if (ret != TR_OK)
			return ret;
	}
	if (!current->condition_destroyed) {
		if (pthread_cond_destroy(&current->work_available) != 0)
			return TR_ERR_SYS;
		current->condition_destroyed = true;
	}
	if (pthread_mutex_destroy(&current->lock) != 0)
		return TR_ERR_SYS;
	budget = current->budget;
	bytes = current->bytes;
	*executor = NULL;
	free(current->threads);
	free(current->slots);
	free(current);
	return tr_memory_budget_release(budget, bytes);
}
