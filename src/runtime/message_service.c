#define _POSIX_C_SOURCE 200809L
#include "../execution/worker_context.h"
#include "../execution/async_operation_internal.h"
#include "message_service.h"
#include "../execution/reactor_internal.h"
#include "../memory_budget.h"
#include "tr/status.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <time.h>
#include <unistd.h>

struct tr_message_service_job {
	struct tr_message_service *service;
	struct tr_message_service_job *next;
	struct tr_message_transform *operation;
	struct tr_async_operation async_operation;
	struct tr_async_operation_ref producer_ref;
	struct tr_async_operation_ref completion_ref;
	struct tr_task_handle handle;
	uint64_t generation;
	tr_message_service_done_fn done;
	void *arg;
	bool used;
	bool accepted;
};

struct tr_message_service {
	struct tr_reactor *reactor;
	struct tr_task_executor *executor;
	struct tr_async_operation_pool *operations;
	struct tr_memory_budget *budget;
	struct tr_message_service_config config;
	struct tr_reactor_timer_handle timer;
	pthread_t owner;
	int fd;
	_Atomic int notification_error;
	pthread_mutex_t jobs_lock;
	size_t bytes;
	size_t held_jobs;
	size_t depth;
	bool start_attempted;
	bool accepting;
	bool event_attached;
	bool timer_attached;
	bool executor_retiring;
	bool recovering;
	bool jobs_lock_ready;
	bool operations_reaped;
	struct tr_reactor_recovery_guard recovery_guard;
	struct tr_message_service_job *free_jobs;
	struct tr_message_service_job jobs[];
};

static int tr_service_owner(struct tr_message_service *service)
{
	if (!service)
		return TR_ERR_INVALID;
	if (service->recovering ||
	    !tr_reactor_in_owner_context_for(service->reactor) ||
	    !pthread_equal(pthread_self(), service->owner))
		return TR_ERR_STATE;
	return TR_OK;
}

static void tr_service_fault(struct tr_message_service *service, int error)
{
	int expected = 0;

	(void)atomic_compare_exchange_strong_explicit(
	        &service->notification_error, &expected, error,
	        memory_order_release, memory_order_relaxed);
}

/* 可在执行器锁内调用：只做至多四次非阻塞系统调用及一个原子写入。
 * EAGAIN 表示计数器已经可读，不能将其当成丢失完成或继续忙等。
 */
static void tr_service_notify(void *arg)
{
	struct tr_message_service *service = arg;
	uint64_t one = 1;

	for (unsigned attempt = 0; attempt < 4; ++attempt) {
		ssize_t ret = write(service->fd, &one, sizeof(one));

		if (ret == (ssize_t)sizeof(one))
			return;
		if (ret < 0 && errno == EAGAIN)
			return;
		if (ret < 0 && errno == EINTR)
			continue;
		tr_service_fault(service, ret < 0 ? errno : EIO);
		return;
	}
	tr_service_fault(service, EINTR);
}

static void tr_service_ack(struct tr_message_service *service)
{
	uint64_t value;

	for (unsigned attempt = 0; attempt < 4; ++attempt) {
		ssize_t ret = read(service->fd, &value, sizeof(value));

		if (ret == (ssize_t)sizeof(value) ||
		    (ret < 0 && errno == EAGAIN))
			return;
		if (ret < 0 && errno == EINTR)
			continue;
		tr_service_fault(service, ret < 0 ? errno : EIO);
		return;
	}
	tr_service_fault(service, EINTR);
}

struct tr_service_cancel {
	const struct tr_task_cancel *token;
};

static bool tr_service_cancel_check(void *arg)
{
	struct tr_service_cancel *cancel = arg;

	return tr_task_cancel_requested(cancel->token);
}

static int tr_service_work(void *arg, const struct tr_task_cancel *cancel)
{
	struct tr_message_service_job *job = arg;
	struct tr_service_cancel context = {cancel};
	struct tr_async_operation_ref completion_ref = {0};
	int ready = 0;
	int ret = tr_message_transform_run_checked(
	        job->operation, tr_service_cancel_check, &context);
	int publish_ret = tr_async_operation_publish(
	        job->service->operations, &job->producer_ref,
	        (struct tr_async_operation_completion){.status = ret},
	        &completion_ref, &ready);
	if (publish_ret != TR_OK)
		return publish_ret;
	if (ready)
		job->completion_ref = completion_ref;
	return ret;
}

static int tr_service_operation_consume(struct tr_message_service_job *job,
                                        struct tr_task_result result,
                                        int *out_cancelled)
{
	struct tr_message_service *service = job->service;
	struct tr_async_operation_ref completion_ref = {0};
	struct tr_async_operation_spec spec = {0};
	struct tr_async_operation_completion completion = {0};
	int ready = 0;
	int cancelled = 0;
	int cancel_reason = TR_OK;
	int ret;
	if (!job->accepted) {
		ret = tr_async_operation_accept(service->operations,
		                                job->async_operation,
		                                &completion_ref, &ready);
		if (ret != TR_OK)
			return ret;
		job->accepted = true;
		if (ready)
			job->completion_ref = completion_ref;
	}
	ret = tr_async_operation_consume(
	        service->operations, &job->completion_ref, &spec, &completion,
	        &cancelled, &cancel_reason);
	if (ret != TR_OK)
		return ret;
	if (spec.kind != TR_ASYNC_OPERATION_KIND_MESSAGE_TRANSFORM ||
	    spec.domain_generation != 1U ||
	    spec.object_generation != job->generation ||
	    completion.status != result.work_status ||
	    completion.payload != NULL || completion.destroy != NULL)
		return TR_ERR_STATE;
	*out_cancelled = cancelled;
	return TR_OK;
}

static void tr_service_cancel_all(struct tr_message_service *service,
                                  int reason)
{
	for (size_t index = 0; index < service->config.tasks.capacity;
	     ++index) {
		struct tr_async_operation operation = {0};
		if (pthread_mutex_lock(&service->jobs_lock) != 0) {
			tr_service_fault(service, EIO);
			return;
		}
		if (service->jobs[index].used)
			operation = service->jobs[index].async_operation;
		if (pthread_mutex_unlock(&service->jobs_lock) != 0) {
			tr_service_fault(service, EIO);
			return;
		}
		if (operation.generation != 0U) {
			int notify;
			(void)tr_async_operation_request_cancel(
			        service->operations, operation, reason,
			        &notify);
		}
	}
}

static void tr_service_done(void *arg, struct tr_task_handle handle,
                            struct tr_task_result result)
{
	struct tr_message_service_job *job = arg;
	struct tr_message_service *service = job->service;
	struct tr_message output = {0};
	struct tr_message_view view;
	enum tr_message_transform_outcome outcome =
	        TR_MESSAGE_TRANSFORM_INTERNAL;
	int operation_cancelled = 0;
	int operation_valid = 0;
	int operation_ret = TR_OK;
	int ret;
	if (!service->recovering) {
		operation_ret = tr_service_operation_consume(
		        job, result, &operation_cancelled);
		if (operation_ret == TR_OK &&
		    (handle.instance != job->handle.instance ||
		     handle.generation != job->handle.generation ||
		     handle.slot != job->handle.slot))
			operation_ret = TR_ERR_STALE;
		if (operation_ret == TR_OK)
			operation_valid = 1;
		else
			tr_service_fault(service, EIO);
	}
	ret = tr_message_transform_finish(&job->operation, &outcome, &output);

	if (ret != TR_OK || result.work_status != TR_OK) {
		tr_service_fault(service, EIO);
		outcome = TR_MESSAGE_TRANSFORM_INTERNAL;
	}
	if (!service->recovering && !operation_valid)
		outcome = TR_MESSAGE_TRANSFORM_INTERNAL;
	/* 即使后端最终检查之后才取消，也不能越过执行器的终局提交点。 */
	if (result.cancelled || operation_cancelled || service->recovering)
		outcome = TR_MESSAGE_TRANSFORM_CANCELLED;
	if (outcome != TR_MESSAGE_TRANSFORM_OK &&
	    tr_message_view(&output, &view) == TR_OK)
		(void)tr_message_release(&output);
	job->done(job->arg, handle,
	          (struct tr_message_service_result){
	                  outcome, service->recovering
	                                   ? TR_MESSAGE_SERVICE_OWNER_STOPPED
	                                   : TR_MESSAGE_SERVICE_ON_OWNER},
	          &output);
	if (tr_message_view(&output, &view) == TR_OK)
		(void)tr_message_release(&output);
	job->done = NULL;
	job->arg = NULL;
	if (!service->recovering) {
		ret = tr_async_operation_retire(service->operations,
		                                &job->completion_ref);
		if (ret != TR_OK) {
			tr_service_fault(service, EIO);
			return;
		}
	}
	/* 非法内部状态下不遗失仍被持有的操作；close 明确拒绝假回收。 */
	if (job->operation || (!service->recovering && !operation_valid)) {
		tr_service_fault(service, EIO);
		return;
	}
	if (pthread_mutex_lock(&service->jobs_lock) != 0) {
		tr_service_fault(service, EIO);
		return;
	}
	job->used = false;
	job->accepted = false;
	job->async_operation = (struct tr_async_operation){0};
	job->producer_ref = (struct tr_async_operation_ref){0};
	job->completion_ref = (struct tr_async_operation_ref){0};
	job->handle = (struct tr_task_handle){0};
	if (pthread_mutex_unlock(&service->jobs_lock) != 0) {
		tr_service_fault(service, EIO);
		return;
	}
	--service->held_jobs;
	job->next = service->free_jobs;
	service->free_jobs = job;
}

static int tr_service_pump(struct tr_message_service *service)
{
	struct tr_task_executor_stats stats;
	size_t delivered;
	int ret;

	if (service->depth || service->executor_retiring)
		return TR_ERR_STATE;
	++service->depth;
	/* 必须先清通知再读完成队列。先交付再清通知可能吞掉其间的新完成。
	 * 一次 read 清除聚合计数，但实际完成条数只以执行器队列为准。
	 */
	tr_service_ack(service);
	if (atomic_load_explicit(&service->notification_error,
	                         memory_order_acquire) != 0) {
		service->accepting = false;
		tr_service_cancel_all(service, TR_ERR_CANCELLED);
		(void)tr_task_executor_stop(service->executor, TR_TASK_CANCEL);
	}
	ret = tr_task_executor_dispatch(
	        service->executor, service->config.dispatch_limit, &delivered);
	if (ret == TR_OK)
		ret = tr_task_executor_get_stats(service->executor, &stats);
	if (ret == TR_OK && stats.completed != 0)
		tr_service_notify(service);
	--service->depth;
	return ret;
}

static void tr_service_event(int fd, uint32_t events, void *arg)
{
	struct tr_message_service *service = arg;

	(void)fd;
	if (events & (EPOLLERR | EPOLLHUP))
		tr_service_fault(service, EIO);
	if (tr_service_pump(service) != TR_OK)
		tr_service_fault(service, EIO);
}

static uint64_t tr_service_watchdog(void *arg, uint64_t now_ns)
{
	struct tr_message_service *service = arg;

	/* 通知故障时仍能交付已接受结果。一个服务只占一个已有 Reactor 定时器，
	 * 不创建轮询线程；这一周期性成本必须纳入后续性能验收。
	 */
	if (tr_service_pump(service) != TR_OK)
		tr_service_fault(service, EIO);
	return now_ns > UINT64_MAX - service->config.watchdog_ns
	               ? UINT64_MAX
	               : now_ns + service->config.watchdog_ns;
}

int tr_message_service_create_on_owner(
        struct tr_reactor *reactor,
        const struct tr_message_service_config *config,
        struct tr_memory_budget *budget, struct tr_message_service **out)
{
	struct tr_message_service *service;
	size_t bytes;
	int ret;

	if (!reactor || !config || !budget || !out || !budget->limit_bytes ||
	    !config->dispatch_limit || config->watchdog_ns < 1000000 ||
	    config->watchdog_ns > 1000000000 || !config->tasks.capacity)
		return TR_ERR_INVALID;
	if (config->tasks.capacity > UINT32_MAX)
		return TR_ERR_BAD_LENGTH;
	if (*out || !tr_reactor_in_owner_context_for(reactor))
		return TR_ERR_STATE;
	if (config->tasks.capacity >
	    (SIZE_MAX - sizeof(*service)) / sizeof(service->jobs[0]))
		return TR_ERR_BAD_LENGTH;
	bytes = sizeof(*service) +
	        config->tasks.capacity * sizeof(service->jobs[0]);
	/* 同时验证总长，禁止后续分配在任何整数模型下得到过小控制块。 */
	if (bytes < sizeof(*service))
		return TR_ERR_BAD_LENGTH;
	ret = tr_memory_budget_reserve(budget, bytes);
	if (ret != TR_OK)
		return ret;
	service = calloc(1, bytes);
	if (!service) {
		(void)tr_memory_budget_release(budget, bytes);
		return TR_ERR_NOMEM;
	}
	service->fd = -1;
	service->fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	if (service->fd < 0) {
		ret = TR_ERR_SYS;
		goto fail;
	}
	service->reactor = reactor;
	service->owner = pthread_self();
	service->budget = budget;
	service->config = *config;
	service->bytes = bytes;
	atomic_init(&service->notification_error, 0);
	if (pthread_mutex_init(&service->jobs_lock, NULL) != 0) {
		ret = TR_ERR_SYS;
		goto fail;
	}
	service->jobs_lock_ready = true;
	ret = tr_task_executor_create(&config->tasks, budget,
	                              &service->executor);
	if (ret != TR_OK)
		goto fail;
	ret = tr_async_operation_pool_create((uint32_t)config->tasks.capacity,
	                                     budget, &service->operations);
	if (ret != TR_OK)
		goto fail;
	ret = tr_async_operation_pool_bind_owner(service->operations);
	if (ret != TR_OK)
		goto fail;
	for (size_t i = 0; i < config->tasks.capacity; ++i) {
		service->jobs[i].service = service;
		service->jobs[i].next = service->free_jobs;
		service->free_jobs = &service->jobs[i];
	}
	*out = service;
	return TR_OK;
fail:
	if (service->operations) {
		uint32_t reaped;
		int close_ret =
		        tr_async_operation_pool_close(service->operations);
		if (close_ret != TR_OK)
			close_ret = tr_async_operation_reap_stopped(
			        service->operations, 1, &reaped);
		if (close_ret == TR_OK)
			(void)tr_async_operation_pool_destroy(
			        &service->operations);
	}
	if (service->executor)
		(void)tr_task_executor_destroy(&service->executor);
	if (service->fd >= 0)
		(void)close(service->fd);
	if (service->jobs_lock_ready)
		(void)pthread_mutex_destroy(&service->jobs_lock);
	free(service);
	(void)tr_memory_budget_release(budget, bytes);
	return ret;
}

int tr_message_service_start_on_owner(struct tr_message_service *service)
{
	struct timespec now;
	uint64_t now_ns;
	int ret = tr_service_owner(service);

	if (ret != TR_OK)
		return ret;
	if (service->start_attempted || service->executor_retiring)
		return TR_ERR_STATE;
	service->start_attempted = true;
	ret = tr_task_executor_set_notify(service->executor, tr_service_notify,
	                                  service);
	if (ret != TR_OK)
		goto fail;
	ret = tr_reactor_aux_event_register(service->reactor, service->fd,
	                                    EPOLLIN, tr_service_event, service);
	if (ret != TR_OK)
		goto fail;
	service->event_attached = true;
	ret = tr_reactor_timer_register(service->reactor, tr_service_watchdog,
	                                service, &service->timer);
	if (ret != TR_OK)
		goto fail;
	service->timer_attached = true;
	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0 ||
	    (uint64_t)now.tv_sec > (UINT64_MAX - 2000000000) / 1000000000) {
		ret = TR_ERR_SYS;
		goto fail;
	}
	now_ns = (uint64_t)now.tv_sec * 1000000000 + (uint64_t)now.tv_nsec;
	ret = tr_reactor_timer_arm(service->timer,
	                           now_ns + service->config.watchdog_ns);
	if (ret != TR_OK)
		goto fail;
	ret = tr_task_executor_start(service->executor);
	if (ret != TR_OK)
		goto fail;
	service->accepting = true;
	return TR_OK;
fail:
	(void)tr_task_executor_stop(service->executor, TR_TASK_CANCEL);
	return ret;
}

int tr_message_service_submit_on_owner(
        struct tr_message_service *service, struct tr_message *input,
        const struct tr_message_transform_config *config,
        tr_message_service_done_fn done, void *arg,
        struct tr_task_handle *handle)
{
	struct tr_message_service_job *job;
	struct tr_message copy = {0};
	struct tr_task_spec spec;
	struct tr_async_operation_spec operation_spec;
	uint64_t generation;
	int operation_ready = 0;
	int ret = tr_service_owner(service);

	if (ret != TR_OK)
		return ret;
	if (!input || !config || !done || !handle)
		return TR_ERR_INVALID;
	if (!service->accepting ||
	    atomic_load_explicit(&service->notification_error,
	                         memory_order_acquire) != 0)
		return TR_ERR_CLOSED;
	job = service->free_jobs;
	if (!job)
		return TR_AGAIN;
	if (job->generation == UINT64_MAX)
		return TR_ERR_STATE;
	generation = job->generation + 1U;
	++service->depth;
	ret = tr_message_clone(input, &copy);
	if (ret != TR_OK)
		goto finish;
	ret = tr_message_transform_prepare(&copy, config, service->budget,
	                                   &job->operation);
	if (ret != TR_OK) {
		(void)tr_message_release(&copy);
		goto finish;
	}
	operation_spec = (struct tr_async_operation_spec){
	        .kind = TR_ASYNC_OPERATION_KIND_MESSAGE_TRANSFORM,
	        .domain_generation = 1U,
	        .object_generation = generation,
	};
	ret = tr_async_operation_reserve(service->operations, &operation_spec,
	                                 &job->async_operation,
	                                 &job->producer_ref);
	if (ret != TR_OK) {
		enum tr_message_transform_outcome outcome;
		(void)tr_message_transform_cancel(job->operation);
		(void)tr_message_transform_run(job->operation);
		(void)tr_message_transform_finish(&job->operation, &outcome,
		                                  &copy);
		goto finish;
	}
	job->done = done;
	job->arg = arg;
	spec = (struct tr_task_spec){tr_service_work, tr_service_done, job};
	ret = tr_task_executor_submit(service->executor, &spec, handle);
	if (ret != TR_OK) {
		enum tr_message_transform_outcome outcome;

		if (tr_async_operation_reject(service->operations,
		                              job->async_operation,
		                              &job->producer_ref) != TR_OK)
			tr_service_fault(service, EIO);
		job->async_operation = (struct tr_async_operation){0};
		job->completion_ref = (struct tr_async_operation_ref){0};
		/* 原 input 仍持有引用，回滚仅归还未发布克隆，不在 owner
		 * 上运行算法。 */
		(void)tr_message_transform_cancel(job->operation);
		(void)tr_message_transform_run(job->operation);
		(void)tr_message_transform_finish(&job->operation, &outcome,
		                                  &copy);
		job->done = NULL;
		job->arg = NULL;
		goto finish;
	}
	service->free_jobs = job->next;
	job->generation = generation;
	if (pthread_mutex_lock(&service->jobs_lock) != 0) {
		tr_service_fault(service, EIO);
		job->handle = *handle;
		job->used = true;
		job->accepted = false;
	} else {
		job->handle = *handle;
		job->used = true;
		job->accepted = false;
		if (pthread_mutex_unlock(&service->jobs_lock) != 0)
			tr_service_fault(service, EIO);
	}
	++service->held_jobs;
	{
		int accept_ret = tr_async_operation_accept(
		        service->operations, job->async_operation,
		        &job->completion_ref, &operation_ready);
		if (accept_ret == TR_OK)
			job->accepted = true;
		else
			tr_service_fault(service, EIO);
	}
	/* 同一 owner turn 尚不可能交付 done；接受成功后才消费原句柄。 */
	if (tr_message_release(input) != TR_OK)
		tr_service_fault(service, EIO);
finish:
	--service->depth;
	return ret;
}

int tr_message_service_cancel(struct tr_message_service *service,
                              struct tr_task_handle handle)
{
	struct tr_async_operation operation = {0};
	int operation_ret = TR_ERR_STALE;
	if (!service || !service->executor)
		return TR_ERR_INVALID;
	if (pthread_mutex_lock(&service->jobs_lock) != 0)
		return TR_ERR_SYS;
	for (size_t index = 0; index < service->config.tasks.capacity;
	     ++index) {
		struct tr_message_service_job *job = &service->jobs[index];
		if (job->used && job->handle.instance == handle.instance &&
		    job->handle.generation == handle.generation &&
		    job->handle.slot == handle.slot) {
			operation = job->async_operation;
			break;
		}
	}
	if (pthread_mutex_unlock(&service->jobs_lock) != 0)
		return TR_ERR_SYS;
	if (operation.generation != 0U) {
		int notify;
		operation_ret = tr_async_operation_request_cancel(
		        service->operations, operation, TR_ERR_CANCELLED,
		        &notify);
		if (operation_ret != TR_OK && operation_ret != TR_ERR_STATE &&
		    operation_ret != TR_ERR_STALE)
			return operation_ret;
	}
	return tr_task_executor_cancel(service->executor, handle);
}

int tr_message_service_stop_on_owner(struct tr_message_service *service,
                                     enum tr_task_stop_mode mode)
{
	int ret = tr_service_owner(service);

	if (ret != TR_OK)
		return ret;
	if (service->executor_retiring)
		return TR_ERR_STATE;
	if (mode == TR_TASK_CANCEL)
		tr_service_cancel_all(service, TR_ERR_CANCELLED);
	ret = tr_task_executor_stop(service->executor, mode);
	if (ret == TR_OK)
		service->accepting = false;
	return ret;
}

int tr_message_service_get_stats_on_owner(struct tr_message_service *service,
                                          struct tr_message_service_stats *out)
{
	int ret = tr_service_owner(service);

	if (ret != TR_OK)
		return ret;
	if (!out)
		return TR_ERR_INVALID;
	if (service->executor_retiring)
		return TR_ERR_STATE;
	ret = tr_task_executor_get_stats(service->executor, &out->tasks);
	if (ret != TR_OK)
		return ret;
	out->held_jobs = service->held_jobs;
	out->notification_error = atomic_load_explicit(
	        &service->notification_error, memory_order_acquire);
	out->accepting = service->accepting && !out->notification_error;
	out->event_attached = service->event_attached;
	out->timer_attached = service->timer_attached;
	return TR_OK;
}

static int tr_service_close(struct tr_message_service **pointer)
{
	struct tr_message_service *service;
	struct tr_task_executor_stats stats;
	struct tr_memory_budget *budget;
	size_t bytes;
	int ret;
	int close_ret;

	if (!pointer)
		return TR_ERR_INVALID;
	service = *pointer;
	if (!service)
		return TR_ERR_INVALID;
	if (service->depth)
		return TR_ERR_STATE;
	if (!service->executor_retiring) {
		service->accepting = false;
		(void)tr_task_executor_stop(service->executor, TR_TASK_DRAIN);
		ret = tr_service_pump(service);
		if (ret != TR_OK)
			return ret;
		ret = tr_task_executor_join(service->executor);
		if (ret != TR_OK)
			return ret;
		ret = tr_task_executor_get_stats(service->executor, &stats);
		if (ret != TR_OK)
			return ret;
		if (stats.outstanding)
			return TR_AGAIN;
		if (service->held_jobs)
			return TR_ERR_STATE;
		if (service->event_attached) {
			ret = tr_reactor_aux_event_unregister(service->reactor,
			                                      service->fd);
			if (ret != TR_OK)
				return ret;
			service->event_attached = false;
		}
		if (service->timer_attached) {
			ret = tr_reactor_timer_unregister(service->timer);
			if (ret != TR_OK)
				return ret;
			service->timer_attached = false;
		}
		ret = tr_task_executor_set_notify(service->executor, NULL,
		                                  NULL);
		if (ret != TR_OK)
			return ret;
		service->executor_retiring = true;
	}
	if (service->executor) {
		ret = tr_task_executor_destroy(&service->executor);
		if (service->executor)
			return ret;
	} else {
		ret = TR_OK;
	}
	if (service->operations) {
		if (!service->operations_reaped) {
			ret = tr_async_operation_pool_close(
			        service->operations);
			if (ret != TR_OK)
				return ret;
		}
		ret = tr_async_operation_pool_destroy(&service->operations);
		if (ret != TR_OK)
			return ret;
	}
	if (service->jobs_lock_ready) {
		if (pthread_mutex_destroy(&service->jobs_lock) != 0)
			return TR_ERR_SYS;
		service->jobs_lock_ready = false;
	}
	/* 两个事件源已跨过解绑屏障，工作线程已 join，不再有 fd/arg 使用者。
	 * Linux close 的错误不能通过重试相同数字解决，避免误关被复用的 fd。
	 */
	close_ret = close(service->fd);
	budget = service->budget;
	bytes = service->bytes;
	if (service->recovery_guard.reactor) {
		int release_ret =
		        tr_reactor_recovery_release(&service->recovery_guard);
		/* 该租约由本服务独占，正确的生命周期调用不可能失配。 */
		if (release_ret != TR_OK)
			return release_ret;
	}
	*pointer = NULL;
	free(service);
	if (tr_memory_budget_release(budget, bytes) != TR_OK)
		return TR_ERR_STATE;
	return ret != TR_OK ? ret : (close_ret == 0 ? TR_OK : TR_ERR_SYS);
}

int tr_message_service_close_on_owner(struct tr_message_service **pointer)
{
	int ret;

	if (!pointer)
		return TR_ERR_INVALID;
	ret = tr_service_owner(*pointer);
	return ret == TR_OK ? tr_service_close(pointer) : ret;
}

int tr_message_service_reap_stopped(struct tr_message_service **pointer)
{
	struct tr_message_service *service;
	int ret;

	if (!pointer || !*pointer)
		return TR_ERR_INVALID;
	if (tr_reactor_in_owner_context() || tr_execution_in_worker_context())
		return TR_ERR_STATE;
	service = *pointer;
	if (service->depth)
		return TR_ERR_STATE;
	ret = tr_reactor_recovery_acquire(service->reactor,
	                                  &service->recovery_guard);
	if (ret != TR_OK)
		return ret;
	service->recovering = true;
	service->accepting = false;
	if (!service->executor_retiring) {
		tr_service_cancel_all(service, TR_ERR_CANCELLED);
		(void)tr_task_executor_stop(service->executor, TR_TASK_CANCEL);
	}
	/* 租约先证明旧 owner 已 join，才能将完成清理移交当前线程。
	 * 不持 Reactor 的 ctl_lock 调用用户完成，避免回调重入死锁。 */
	if (service->executor) {
		ret = tr_task_executor_adopt_cleanup(service->executor);
		if (ret != TR_OK)
			return ret;
	}
	if (service->operations && !service->operations_reaped) {
		uint32_t reaped;
		ret = tr_async_operation_reap_stopped(service->operations, 1,
		                                      &reaped);
		if (ret != TR_OK)
			return ret;
		service->operations_reaped = true;
	} else if (service->operations && service->operations_reaped) {
		ret = tr_async_operation_pool_handoff_owner(service->operations,
		                                            1);
		if (ret != TR_OK)
			return ret;
	}
	return tr_service_close(pointer);
}
