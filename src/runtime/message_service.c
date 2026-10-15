#define _POSIX_C_SOURCE 200809L
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
	tr_message_service_done_fn done;
	void *arg;
};

struct tr_message_service {
	struct tr_reactor *reactor;
	struct tr_task_executor *executor;
	struct tr_memory_budget *budget;
	struct tr_message_service_config config;
	struct tr_reactor_timer_handle timer;
	pthread_t owner;
	int fd;
	_Atomic int notification_error;
	size_t bytes;
	size_t held_jobs;
	size_t depth;
	bool start_attempted;
	bool accepting;
	bool event_attached;
	bool timer_attached;
	bool executor_retiring;
	struct tr_message_service_job *free_jobs;
	struct tr_message_service_job jobs[];
};

static int tr_service_owner(struct tr_message_service *service)
{
	if (!service)
		return TR_ERR_INVALID;
	if (!pthread_equal(pthread_self(), service->owner) ||
	    !tr_reactor_in_owner_context())
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

	return tr_message_transform_run_checked(
	        job->operation, tr_service_cancel_check, &context);
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
	int ret =
	        tr_message_transform_finish(&job->operation, &outcome, &output);

	if (ret != TR_OK || result.work_status != TR_OK) {
		tr_service_fault(service, EIO);
		outcome = TR_MESSAGE_TRANSFORM_INTERNAL;
	}
	/* 即使后端最终检查之后才取消，也不能越过执行器的终局提交点。 */
	if (result.cancelled)
		outcome = TR_MESSAGE_TRANSFORM_CANCELLED;
	if (outcome != TR_MESSAGE_TRANSFORM_OK &&
	    tr_message_view(&output, &view) == TR_OK)
		(void)tr_message_release(&output);
	job->done(job->arg, handle, outcome, &output);
	if (tr_message_view(&output, &view) == TR_OK)
		(void)tr_message_release(&output);
	job->done = NULL;
	job->arg = NULL;
	/* 非法内部状态下不遗失仍被持有的操作；close 明确拒绝假回收。 */
	if (job->operation) {
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
	if (*out || !tr_reactor_in_owner_context())
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
	service->fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	if (service->fd < 0) {
		ret = TR_ERR_SYS;
		goto fail;
	}
	ret = tr_task_executor_create(&config->tasks, budget,
	                              &service->executor);
	if (ret != TR_OK) {
		(void)close(service->fd);
		goto fail;
	}
	service->reactor = reactor;
	service->owner = pthread_self();
	service->budget = budget;
	service->config = *config;
	service->bytes = bytes;
	atomic_init(&service->notification_error, 0);
	for (size_t i = 0; i < config->tasks.capacity; ++i) {
		service->jobs[i].service = service;
		service->jobs[i].next = service->free_jobs;
		service->free_jobs = &service->jobs[i];
	}
	*out = service;
	return TR_OK;
fail:
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
	job->done = done;
	job->arg = arg;
	spec = (struct tr_task_spec){tr_service_work, tr_service_done, job};
	ret = tr_task_executor_submit(service->executor, &spec, handle);
	if (ret != TR_OK) {
		enum tr_message_transform_outcome outcome;

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
	++service->held_jobs;
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
	if (!service || !service->executor)
		return TR_ERR_INVALID;
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

int tr_message_service_close_on_owner(struct tr_message_service **pointer)
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
	ret = tr_service_owner(service);
	if (ret != TR_OK)
		return ret;
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
	ret = tr_task_executor_destroy(&service->executor);
	if (service->executor)
		return ret;
	/* 两个事件源已跨过解绑屏障，工作线程已 join，不再有 fd/arg 使用者。
	 * Linux close 的错误不能通过重试相同数字解决，避免误关被复用的 fd。
	 */
	close_ret = close(service->fd);
	budget = service->budget;
	bytes = service->bytes;
	*pointer = NULL;
	free(service);
	if (tr_memory_budget_release(budget, bytes) != TR_OK)
		return TR_ERR_STATE;
	return ret != TR_OK ? ret : (close_ret == 0 ? TR_OK : TR_ERR_SYS);
}
