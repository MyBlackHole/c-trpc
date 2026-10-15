#include "runtime_internal.h"

#include <assert.h>
#include <errno.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include "../io/socket.h"
#include "tr/status.h"
#include "../execution/reactor_internal.h"
#include "../rpc/rpc_internal.h"
#include "../io/socket_internal.h"

struct tr_runtime_shard {
	uint32_t shard_id;
	struct tr_reactor *reactor;
	struct tr_rpc_executor_group *rpc_executor;
	struct tr_message_service_config message_config;
	struct tr_message_service *messages;
	struct tr_memory_budget memory_budget;

	struct tr_runtime_peer *peers;
	uint64_t peer_storage_bytes;
	uint32_t peer_capacity;
	uint32_t peer_count;
	uint32_t peer_count_peak;
	_Atomic uint32_t peer_reaping_count;
	uint64_t peers_ready_total;
	_Atomic uint64_t peers_reaped_total;
	uint64_t peer_capacity_rejections;
	int peer_event_fd;
	int peer_events_registered;

	int listen_fd;
	uint16_t bound_port;
	int listener_events_registered;
	int started;
};

struct tr_runtime {
	uint32_t shard_count;
	struct tr_runtime_shard *shards;
	int started;
};

static int tr_runtime_shard_release(struct tr_runtime_shard *shard)
{
	int ret;

	if (!shard)
		return TR_OK;
	if (shard->messages)
		return TR_ERR_STATE;

	/*
	 * 终止准入源可以提前关闭。如果保留的接收负载使反应器继续存活，
	 * 保持分片存储与预算不变，以便应用归还该负载后重试释放。
	 */
	ret = tr_runtime_shard_close_listener(shard);
	if (ret != TR_OK)
		return ret;
	ret = tr_runtime_shard_disable_peer_events(shard);
	if (ret != TR_OK)
		return ret;
	if (shard->peer_event_fd >= 0) {
		close(shard->peer_event_fd);
		shard->peer_event_fd = -1;
	}

	if (shard->reactor) {
		ret = tr_reactor_destroy(shard->reactor);
		if (ret != TR_OK)
			return ret;
		shard->reactor = NULL;
	}

	if (shard->rpc_executor) {
		ret = tr_rpc_executor_group_destroy_checked(shard->rpc_executor);
		if (ret != TR_OK)
			return ret;
		shard->rpc_executor = NULL;
	}

	free(shard->peers);
	shard->peers = NULL;
	if (shard->peer_storage_bytes != 0U) {
		(void)tr_memory_budget_release(
			&shard->memory_budget, shard->peer_storage_bytes);
		shard->peer_storage_bytes = 0U;
	}
	shard->peer_capacity = 0U;
	/* 输出视图可以越过停止存活。预算仍被借用时保留 Runtime 外壳，
	 * 防止最后 release 向已释放的预算对象写入。 */
	if (atomic_load_explicit(&shard->memory_budget.current_bytes,
	                         memory_order_acquire) != 0)
		return TR_AGAIN;
	return TR_OK;
}

static int tr_runtime_rpc_executor_config_valid(
	const struct tr_runtime_rpc_executor_config *executor)
{
	if (!executor)
		return 0;
	if (executor->endpoint_capacity == 0U)
		return executor->max_calls_per_endpoint == 0U &&
		       executor->thread_count == 0U;
	return executor->max_calls_per_endpoint != 0U;
}

static int
tr_runtime_shard_config_valid(const struct tr_runtime_shard_config *config)
{
	if (!config)
		return 0;
	if (!tr_runtime_rpc_executor_config_valid(&config->rpc_executor))
		return 0;
	if (!config->messages.tasks.capacity)
		return config->messages.tasks.workers == 0;
	return config->memory_budget_bytes && config->messages.tasks.workers &&
	       config->messages.tasks.workers <=
	               config->messages.tasks.capacity &&
	       config->messages.tasks.capacity < UINT32_MAX &&
	       config->messages.dispatch_limit &&
	       config->messages.watchdog_ns >= 1000000 &&
	       config->messages.watchdog_ns <= 1000000000;
}

static int tr_runtime_create_rollback(
	struct tr_runtime **out, struct tr_runtime *runtime, int cause)
{
	int rollback_ret;

	if (!out || !runtime)
		return cause;

	rollback_ret = tr_runtime_destroy(runtime);
	if (rollback_ret == TR_OK) {
		*out = NULL;
		return cause;
	}

	/*
	 * 构造回滚自身失败时，部分构造的运行时必须继续有所有者。
	 * 客户端与服务端构造函数会接住非空 *out，并继续自己的终止
	 * 回滚；若仍失败，最终所有权继续向最外层调用方传播。
	 */
	*out = runtime;
	return rollback_ret;
}

int tr_runtime_create(const struct tr_runtime_config *config,
		      struct tr_runtime **out)
{
	struct tr_runtime *runtime;
	uint32_t i;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	if (!config || config->shard_count == 0U || !config->shards)
		return TR_ERR_INVALID;
	for (i = 0; i < config->shard_count; ++i)
		if (!tr_runtime_shard_config_valid(&config->shards[i]))
			return TR_ERR_INVALID;

	runtime = (struct tr_runtime *)calloc(1, sizeof(*runtime));
	if (!runtime)
		return TR_ERR_NOMEM;

	runtime->shards = (struct tr_runtime_shard *)calloc(
		config->shard_count, sizeof(*runtime->shards));
	if (!runtime->shards) {
		free(runtime);
		return TR_ERR_NOMEM;
	}
	runtime->shard_count = config->shard_count;

	/*
	 * 先初始化所有分片的 fd 哨兵值，使部分构造的运行时在任意构造点都能
	 * 安全进入正式 destroy()；未访问分片的其他字段保持 calloc 零值。
	 */
	for (i = 0; i < runtime->shard_count; ++i) {
		runtime->shards[i].listen_fd = -1;
		runtime->shards[i].peer_event_fd = -1;
	}

	for (i = 0; i < runtime->shard_count; ++i) {
		const struct tr_runtime_shard_config *shard_config =
			&config->shards[i];
		struct tr_runtime_shard *shard = &runtime->shards[i];
		int ret;

		shard->shard_id = i;
		shard->message_config = shard_config->messages;
		tr_memory_budget_init(
			&shard->memory_budget, shard_config->memory_budget_bytes);
		shard->peer_capacity = shard_config->peer_capacity;
		if (shard->peer_capacity != 0U) {
			uint64_t peer_storage_bytes =
				(uint64_t)shard->peer_capacity *
				(uint64_t)sizeof(*shard->peers);

			ret = tr_memory_budget_reserve(
				&shard->memory_budget, peer_storage_bytes);
			if (ret == TR_OK) {
				shard->peer_storage_bytes = peer_storage_bytes;
				shard->peers = (struct tr_runtime_peer *)calloc(
					shard->peer_capacity,
					sizeof(*shard->peers));
				if (!shard->peers)
					ret = TR_ERR_NOMEM;
			}
			if (ret == TR_OK) {
				shard->peer_event_fd =
					eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
				ret = shard->peer_event_fd < 0 ?
					      TR_ERR_SYS : TR_OK;
			}
		} else {
			ret = TR_OK;
		}
		if (ret == TR_OK) {
			struct tr_reactor_config reactor_config =
				shard_config->reactor;

			reactor_config.memory_budget = &shard->memory_budget;
			ret = tr_reactor_create(
				&reactor_config, NULL, NULL, NULL,
				&shard->reactor);
		}
		if (ret == TR_OK &&
		    shard_config->rpc_executor.endpoint_capacity != 0U)
			ret = tr_rpc_executor_group_create(
				shard_config->rpc_executor.endpoint_capacity,
				shard_config->rpc_executor.max_calls_per_endpoint,
				shard_config->rpc_executor.thread_count,
				&shard->rpc_executor);
		if (ret != TR_OK)
			return tr_runtime_create_rollback(out, runtime, ret);
	}

	*out = runtime;
	return TR_OK;
}

static int tr_runtime_message_start(void *arg)
{
	struct tr_runtime_shard *shard = arg;
	int ret = tr_message_service_create_on_owner(
	        shard->reactor, &shard->message_config, &shard->memory_budget,
	        &shard->messages);

	return ret == TR_OK ? tr_message_service_start_on_owner(shard->messages)
	                    : ret;
}

static int tr_runtime_message_cancel(void *arg)
{
	struct tr_runtime_shard *shard = arg;

	return tr_message_service_stop_on_owner(shard->messages,
	                                        TR_TASK_CANCEL);
}

static int tr_runtime_shard_stop_execution(
	struct tr_runtime_shard *shard)
{
	int result = TR_OK;
	int ret;

	if (!shard)
		return TR_OK;

	/*
	 * 保持既有顺序：先收敛反应器所有者，再等待分片执行器工作线程退出并回收。
	 * 工作线程在反应器已停止时已有本地完成回退，不会把线程回收
	 * 屏障变成对反应器进展的依赖。
	 */
	if (shard->started) {
		if (shard->messages) {
			/* 可能已致命退出；调用未执行也不失去随后 stop/join 和
			 * 停止回收责任，不能把 CLOSED 当成已释放服务。 */
			(void)tr_reactor_call(shard->reactor,
			                      tr_runtime_message_cancel, shard);
		}
		ret = tr_reactor_stop(shard->reactor);
		if (ret == TR_OK)
			shard->started = 0;
		else
			result = ret;
	}

	if (!shard->started && shard->messages) {
		ret = tr_message_service_reap_stopped(&shard->messages);
		if (ret != TR_OK && result == TR_OK)
			result = ret;
	}

	if (shard->rpc_executor) {
		ret = tr_rpc_executor_group_stop(shard->rpc_executor);
		if (ret != TR_OK && result == TR_OK)
			result = ret;
	}
	return result;
}

int tr_runtime_start(struct tr_runtime *runtime)
{
	uint32_t i;

	if (!runtime)
		return TR_ERR_INVALID;
	if (runtime->started)
		return TR_ERR_STATE;

	/*
	 * 新轮次必须在任何 pthread_create 之前验证所有分片都已从上一次
	 * 停止或回滚完整收敛。否则后面的分片仍处于停止中时，前面的
	 * 分片会先被重新启动，重新制造部分启动的轮次。
	 */
	for (i = 0; i < runtime->shard_count; ++i) {
		struct tr_runtime_shard *shard = &runtime->shards[i];

		if (shard->started || shard->messages || !shard->reactor)
			return TR_ERR_STATE;
		if (shard->rpc_executor &&
		    tr_rpc_executor_group_can_start(shard->rpc_executor) != TR_OK)
			return TR_ERR_STATE;
	}

	for (i = 0; i < runtime->shard_count; ++i) {
		struct tr_runtime_shard *shard = &runtime->shards[i];
		int ret;

		/*
		 * 工作线程轮次在运行时已经由调用方拥有之后才启动。若部分
		 * pthread_create 或回滚时的线程回收失败，组存储留在分片中，
		 * runtime_stop()/destroy() 可以继续收敛。
		 */
		if (shard->rpc_executor) {
			ret = tr_rpc_executor_group_start(shard->rpc_executor);
			if (ret != TR_OK)
				goto rollback;
		}

		ret = tr_reactor_start(shard->reactor);
		if (ret != TR_OK)
			goto rollback;
		shard->started = 1;
		if (shard->message_config.tasks.capacity) {
			ret = tr_reactor_call(shard->reactor,
			                      tr_runtime_message_start, shard);
			if (ret != TR_OK)
				goto rollback;
		}
		continue;

	rollback: {
		int cause = ret;
		int rollback_ret = TR_OK;
		uint32_t j = i + 1U;

		/*
		 * 包含当前分片：组可能已经启动而反应器尚未成功。
		 * 每个分片的停止都是幂等且可重试的。
		 */
		while (j != 0U) {
			int stop_ret;

			--j;
			stop_ret = tr_runtime_shard_stop_execution(
			        &runtime->shards[j]);
			if (stop_ret != TR_OK && rollback_ret == TR_OK)
				rollback_ret = stop_ret;
		}
		return rollback_ret != TR_OK ? rollback_ret : cause;
	}
	}

	runtime->started = 1;
	return TR_OK;
}

int tr_runtime_stop(struct tr_runtime *runtime)
{
	uint32_t i;
	int result = TR_OK;

	if (!runtime)
		return TR_ERR_INVALID;

	/*
	 * 即使总状态 runtime->started 为 false，也可能存在启动失败
	 * 留下的部分执行器轮次，因此不能只根据反应器 started 早退。
	 */
	if (tr_reactor_in_owner_context() || tr_rpc_in_worker_context() ||
	    tr_task_in_worker_context())
		return TR_ERR_STATE;

	i = runtime->shard_count;
	while (i != 0U) {
		int ret;

		--i;
		ret = tr_runtime_shard_stop_execution(&runtime->shards[i]);
		if (ret != TR_OK && result == TR_OK)
			result = ret;
	}

	if (result == TR_OK)
		runtime->started = 0;
	return result;
}

int tr_runtime_destroy(struct tr_runtime *runtime)
{
	uint32_t i;
	int ret;

	if (!runtime)
		return TR_OK;

	ret = tr_runtime_stop(runtime);
	if (ret != TR_OK)
		return ret;

	for (i = 0; i < runtime->shard_count; ++i) {
		ret = tr_runtime_shard_release(&runtime->shards[i]);
		if (ret != TR_OK)
			return ret;
	}
	free(runtime->shards);
	runtime->shards = NULL;
	free(runtime);
	return TR_OK;
}


uint32_t tr_runtime_shard_count(const struct tr_runtime *runtime)
{
	return runtime ? runtime->shard_count : 0U;
}

struct tr_runtime_shard *tr_runtime_shard_at(struct tr_runtime *runtime,
					     uint32_t index)
{
	if (!runtime || index >= runtime->shard_count)
		return NULL;
	return &runtime->shards[index];
}

uint32_t tr_runtime_shard_id(const struct tr_runtime_shard *shard)
{
	return shard ? shard->shard_id : UINT32_MAX;
}

struct tr_reactor *
tr_runtime_shard_reactor(const struct tr_runtime_shard *shard)
{
	return shard ? shard->reactor : NULL;
}

int tr_runtime_shard_call(struct tr_runtime_shard *shard,
			  int (*fn)(void *arg), void *arg)
{
	if (!shard || !fn)
		return TR_ERR_INVALID;
	return tr_reactor_call(shard->reactor, fn, arg);
}

struct tr_rpc_executor_group *
tr_runtime_shard_rpc_executor(const struct tr_runtime_shard *shard)
{
	return shard ? shard->rpc_executor : NULL;
}

struct tr_memory_budget *
tr_runtime_shard_memory_budget(struct tr_runtime_shard *shard)
{
	return shard ? &shard->memory_budget : NULL;
}

void tr_runtime_shard_memory_stats(
	const struct tr_runtime_shard *shard,
	struct tr_memory_budget_stats *out)
{
	tr_memory_budget_get_stats(
		shard ? &shard->memory_budget : NULL, out);
}

int tr_runtime_shard_listen_ipv4_ex(struct tr_runtime_shard *shard,
				    const char *address, uint16_t port,
				    int backlog, int reuse_port,
				    uint16_t *out_bound_port)
{
	int fd = -1;
	uint16_t bound = 0;
	int ret;

	if (!shard || !address || backlog <= 0)
		return TR_ERR_INVALID;
	if (shard->listen_fd >= 0)
		return TR_ERR_STATE;

	ret = tr_tcp_listen_ipv4_ex(address, port, backlog, reuse_port,
				    &fd, &bound);
	if (ret != TR_OK)
		return ret;

	shard->listen_fd = fd;
	shard->bound_port = bound;
	if (out_bound_port)
		*out_bound_port = bound;
	return TR_OK;
}

int tr_runtime_shard_listen_ipv4(struct tr_runtime_shard *shard,
				 const char *address, uint16_t port,
				 int backlog, uint16_t *out_bound_port)
{
	return tr_runtime_shard_listen_ipv4_ex(shard, address, port, backlog,
					       0, out_bound_port);
}

int tr_runtime_shard_listener_fd(const struct tr_runtime_shard *shard)
{
	return shard ? shard->listen_fd : -1;
}

uint16_t tr_runtime_shard_bound_port(const struct tr_runtime_shard *shard)
{
	return shard ? shard->bound_port : 0U;
}

int tr_runtime_shard_enable_listener_events(struct tr_runtime_shard *shard,
					    tr_runtime_listener_cb callback,
					    void *arg)
{
	int ret;

	if (!shard || !callback || shard->listen_fd < 0)
		return TR_ERR_INVALID;
	if (shard->listener_events_registered)
		return TR_ERR_STATE;

	ret = tr_reactor_listener_register(shard->reactor, shard->listen_fd,
					   callback, arg);
	if (ret == TR_OK)
		shard->listener_events_registered = 1;
	return ret;
}

int tr_runtime_shard_disable_listener_events(struct tr_runtime_shard *shard)
{
	int ret;

	if (!shard)
		return TR_ERR_INVALID;
	if (!shard->listener_events_registered)
		return TR_OK;

	ret = tr_reactor_listener_unregister(shard->reactor, shard->listen_fd);
	if (ret == TR_OK)
		shard->listener_events_registered = 0;
	return ret;
}

int tr_runtime_shard_close_listener(struct tr_runtime_shard *shard)
{
	int ret;

	if (!shard)
		return TR_ERR_INVALID;

	ret = tr_runtime_shard_disable_listener_events(shard);
	if (ret != TR_OK)
		return ret;

	tr_socket_close(&shard->listen_fd);
	shard->bound_port = 0U;
	return TR_OK;
}

uint32_t tr_runtime_shard_peer_capacity(const struct tr_runtime_shard *shard)
{
	return shard ? shard->peer_capacity : 0U;
}

struct tr_runtime_peer *
tr_runtime_shard_peer_at(struct tr_runtime_shard *shard, uint32_t slot)
{
	if (!shard || slot >= shard->peer_capacity)
		return NULL;
	return &shard->peers[slot];
}

void tr_runtime_shard_peer_note_added(struct tr_runtime_shard *shard)
{
	if (!shard)
		return;

	shard->peer_count++;
	if (shard->peer_count > shard->peer_count_peak)
		shard->peer_count_peak = shard->peer_count;
}

void tr_runtime_shard_peer_note_ready(struct tr_runtime_shard *shard)
{
	if (shard)
		shard->peers_ready_total++;
}

int tr_runtime_shard_peer_reaping_at_capacity(
	const struct tr_runtime_shard *shard)
{
	uint32_t current;

	if (!shard || shard->peer_capacity == 0U)
		return 0;
	current = atomic_load_explicit(
		&shard->peer_reaping_count, memory_order_acquire);
	return current >= shard->peer_capacity;
}

void tr_runtime_shard_peer_note_removed_for_reap(struct tr_runtime_shard *shard)
{
	if (!shard)
		return;

	if (shard->peer_count != 0U)
		shard->peer_count--;
	(void)atomic_fetch_add_explicit(&shard->peer_reaping_count, 1U,
					 memory_order_release);
}

void tr_runtime_shard_peer_note_reaped(struct tr_runtime_shard *shard)
{
	uint32_t current;
	int decremented = 0;

	if (!shard)
		return;

	current = atomic_load_explicit(
		&shard->peer_reaping_count, memory_order_acquire);
	while (current != 0U) {
		if (atomic_compare_exchange_weak_explicit(
			    &shard->peer_reaping_count, &current,
			    current - 1U, memory_order_acq_rel,
			    memory_order_acquire)) {
			decremented = 1;
			break;
		}
	}

#ifndef NDEBUG
	assert(decremented);
#endif
	if (decremented)
		(void)atomic_fetch_add_explicit(
			&shard->peers_reaped_total, 1U,
			memory_order_relaxed);
}

void tr_runtime_shard_peer_note_capacity_rejection(
	struct tr_runtime_shard *shard)
{
	if (shard)
		shard->peer_capacity_rejections++;
}

void tr_runtime_shard_peer_stats(const struct tr_runtime_shard *shard,
				 struct tr_runtime_peer_stats *out)
{
	if (!out)
		return;

	memset(out, 0, sizeof(*out));
	if (!shard)
		return;

	out->capacity = shard->peer_capacity;
	out->current = shard->peer_count;
	out->peak = shard->peer_count_peak;
	out->reaping_current = atomic_load_explicit(
		&shard->peer_reaping_count, memory_order_relaxed);
	out->ready_total = shard->peers_ready_total;
	out->reaped_total = atomic_load_explicit(
		&shard->peers_reaped_total, memory_order_relaxed);
	out->capacity_rejections = shard->peer_capacity_rejections;
}

int tr_runtime_shard_peer_event_fd(const struct tr_runtime_shard *shard)
{
	return shard ? shard->peer_event_fd : -1;
}

int tr_runtime_shard_enable_peer_events(struct tr_runtime_shard *shard,
					tr_runtime_peer_event_cb callback,
					void *arg)
{
	int ret;

	if (!shard || shard->peer_event_fd < 0 || !callback)
		return TR_ERR_INVALID;
	if (shard->peer_events_registered)
		return TR_ERR_STATE;

	ret = tr_reactor_peer_event_register(shard->reactor,
					     shard->peer_event_fd,
					     callback, arg);
	if (ret == TR_OK)
		shard->peer_events_registered = 1;
	return ret;
}

int tr_runtime_shard_disable_peer_events(struct tr_runtime_shard *shard)
{
	int ret;

	if (!shard)
		return TR_ERR_INVALID;
	if (!shard->peer_events_registered)
		return TR_OK;

	ret = tr_reactor_peer_event_unregister(shard->reactor,
					       shard->peer_event_fd);
	if (ret == TR_OK)
		shard->peer_events_registered = 0;
	return ret;
}

void tr_runtime_shard_signal_peer_event(struct tr_runtime_shard *shard)
{
	uint64_t one = 1U;
	ssize_t written;

	if (!shard || shard->peer_event_fd < 0)
		return;

	do {
		written = write(shard->peer_event_fd, &one, sizeof(one));
	} while (written < 0 && errno == EINTR);

	/*
	 * EAGAIN 表示 eventfd 计数器已经饱和；必然已有待处理唤醒，
	 * 因此无需额外操作。
	 */
}

void tr_runtime_shard_drain_peer_event(struct tr_runtime_shard *shard)
{
	uint64_t value;
	ssize_t n;

	if (!shard || shard->peer_event_fd < 0)
		return;

	for (;;) {
		do {
			n = read(shard->peer_event_fd, &value, sizeof(value));
		} while (n < 0 && errno == EINTR);
		if (n == (ssize_t)sizeof(value))
			continue;
		if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
			break;
		break;
	}
}

struct tr_runtime_message_request {
	struct tr_runtime_shard *shard;
	struct tr_message *input;
	const struct tr_message_transform_config *config;
	tr_message_service_done_fn done;
	void *arg;
	struct tr_task_handle *handle;
};

static int tr_runtime_message_submit_on_owner(void *arg)
{
	struct tr_runtime_message_request *request = arg;
	struct tr_message_service *service = request->shard->messages;

	if (!service)
		return TR_ERR_CLOSED;
	return tr_message_service_submit_on_owner(
	        service, request->input, request->config, request->done,
	        request->arg, request->handle);
}

int tr_runtime_shard_message_submit(
        struct tr_runtime_shard *shard, struct tr_message *input,
        const struct tr_message_transform_config *config,
        tr_message_service_done_fn done, void *arg,
        struct tr_task_handle *handle)
{
	struct tr_runtime_message_request request = {shard, input, config,
	                                             done,  arg,   handle};

	if (!shard || !input || !config || !done || !handle)
		return TR_ERR_INVALID;
	if (!shard->message_config.tasks.capacity)
		return TR_ERR_UNSUPPORTED;
	return tr_reactor_call(shard->reactor,
	                       tr_runtime_message_submit_on_owner, &request);
}
