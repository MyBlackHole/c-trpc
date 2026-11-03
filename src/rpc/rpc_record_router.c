#include "rpc_record_router.h"
#include "rpc_record_transport_internal.h"
#include "../execution/reactor_internal.h"
#include "../transport/channel/channel_internal.h"
#include "../memory_budget.h"
#include "tr/status.h"

#include <stdlib.h>
#include <string.h>

struct tr_rpc_route {
	struct tr_rpc_record_router *router;
	struct tr_stream_handle stream;
	struct tr_rpc_record_stream *binding;
	const struct tr_rpc_record_method *method;
	uint64_t generation;
	uint32_t index;
	unsigned active;
	unsigned terminal;
	uint8_t head[TR_RPC_RECORD_HEADER_SIZE];
	size_t used;
};

struct tr_rpc_record_router {
	struct tr_channel *channel;
	struct tr_reactor *reactor;
	struct tr_memory_budget *budget;
	struct tr_rpc_record_method *methods;
	struct tr_rpc_route *routes;
	size_t method_count;
	size_t bytes;
	uint32_t capacity;
	uint64_t generation;
	unsigned depth;
	unsigned retiring;
	tr_rpc_route_terminal_fn terminal;
	void *arg;
};

static int tr_router_owner(struct tr_rpc_record_router *router)
{
	if (!router)
		return TR_ERR_INVALID;
	return tr_reactor_in_owner_context_for(router->reactor) ? TR_OK
	                                                        : TR_ERR_STATE;
}

static struct tr_rpc_route_handle tr_route_handle(struct tr_rpc_route *route)
{
	return (struct tr_rpc_route_handle){route->router, route->generation,
	                                    route->index};
}

static int tr_router_lookup(struct tr_rpc_route_handle handle,
                            struct tr_rpc_route **out)
{
	struct tr_rpc_record_router *router = handle.router;
	struct tr_rpc_route *route;
	int ret = tr_router_owner(router);
	if (ret != TR_OK)
		return ret;
	if (router->retiring)
		return TR_ERR_CLOSED;
	if (!handle.generation || handle.slot >= router->capacity)
		return TR_ERR_STALE;
	route = &router->routes[handle.slot];
	if (!route->active || route->generation != handle.generation)
		return TR_ERR_STALE;
	*out = route;
	return TR_OK;
}

static struct tr_rpc_route *tr_router_find(struct tr_rpc_record_router *router,
                                           struct tr_stream_handle stream)
{
	if (stream.channel != router->channel)
		return NULL;
	for (uint32_t i = 0; i < router->capacity; ++i) {
		struct tr_rpc_route *r = &router->routes[i];
		if (r->active && r->stream.slot == stream.slot &&
		    r->stream.generation == stream.generation)
			return r;
	}
	return NULL;
}

static void tr_router_terminal(struct tr_rpc_route *route,
                               enum tr_rpc_status status,
                               enum tr_rpc_record_delivery delivery)
{
	struct tr_rpc_record_router *router = route->router;
	if (route->terminal)
		return;
	route->terminal = 1;
	route->used = 0;
	++router->depth;
	router->terminal(tr_route_handle(route), route->stream, status,
	                 delivery, router->arg);
	--router->depth;
}

static void tr_router_binding_terminal(struct tr_rpc_record_stream *binding,
                                       enum tr_rpc_status status,
                                       enum tr_rpc_record_delivery delivery,
                                       void *arg)
{
	(void)binding;
	tr_router_terminal(arg, status, delivery);
}

static void tr_router_received(struct tr_rpc_record_stream *binding,
                               const struct tr_rpc_record_view *view, void *arg)
{
	struct tr_rpc_route *route = arg;
	(void)binding;
	++route->router->depth;
	route->method->received(tr_route_handle(route), view,
	                        route->method->arg);
	--route->router->depth;
}

static int tr_method_compare(const struct tr_rpc_method_desc *a,
                             const struct tr_rpc_method_desc *b)
{
	if (a->service_id != b->service_id)
		return a->service_id < b->service_id ? -1 : 1;
	return a->method_id < b->method_id ? -1 : a->method_id != b->method_id;
}

static const struct tr_rpc_record_method *
tr_router_method(struct tr_rpc_record_router *router, uint32_t service,
                 uint32_t method)
{
	struct tr_rpc_method_desc key = {.service_id = service,
	                                 .method_id = method};
	size_t lo = 0, hi = router->method_count;
	while (lo < hi) {
		size_t mid = lo + (hi - lo) / 2;
		int cmp = tr_method_compare(&router->methods[mid].descriptor,
		                            &key);
		if (!cmp)
			return &router->methods[mid];
		if (cmp < 0)
			lo = mid + 1;
		else
			hi = mid;
	}
	return NULL;
}

int tr_rpc_record_router_create_on_owner(
        struct tr_channel *channel, const struct tr_rpc_record_method *methods,
        size_t method_count, uint32_t capacity,
        tr_rpc_route_terminal_fn terminal, void *arg,
        struct tr_memory_budget *budget, struct tr_rpc_record_router **out)
{
	struct tr_rpc_record_router *router;
	struct tr_reactor *reactor;
	size_t bytes, route_bytes;
	int ret;
	if (!channel || !methods || !method_count || !capacity || !terminal ||
	    !budget || !budget->limit_bytes || !out)
		return TR_ERR_INVALID;
	reactor = tr_channel_reactor(channel);
	if (*out || !tr_reactor_in_owner_context_for(reactor))
		return TR_ERR_STATE;
	if (capacity > tr_channel_max_streams(channel))
		return TR_ERR_INVALID;
	if (sizeof(struct tr_rpc_route) >
	    (SIZE_MAX - sizeof(*router)) / (size_t)capacity)
		return TR_ERR_BAD_LENGTH;
	route_bytes = (size_t)capacity * sizeof(struct tr_rpc_route);
	bytes = sizeof(*router) + route_bytes;
	if (method_count > (SIZE_MAX - bytes) / sizeof(*methods))
		return TR_ERR_BAD_LENGTH;
	bytes += method_count * sizeof(*methods);
	for (size_t i = 0; i < method_count; ++i) {
		struct tr_rpc_record_state state;
		const struct tr_rpc_method_desc *d = &methods[i].descriptor;
		if (!methods[i].received || !d->request_codec_id ||
		    !d->response_codec_id || d->lane > TR_RPC_LANE_BULK ||
		    tr_rpc_record_state_init(&state, d->request_cardinality,
		                             d->response_cardinality) != TR_OK)
			return TR_ERR_INVALID;
	}
	ret = tr_memory_budget_reserve(budget, bytes);
	if (ret != TR_OK)
		return ret;
	router = calloc(1, sizeof(*router));
	if (!router) {
		(void)tr_memory_budget_release(budget, bytes);
		return TR_ERR_NOMEM;
	}
	router->routes = calloc(capacity, sizeof(*router->routes));
	router->methods = malloc(method_count * sizeof(*methods));
	if (!router->routes || !router->methods) {
		ret = TR_ERR_NOMEM;
		goto fail;
	}
	/* 只在创建期原地排序，不借排序库的隐藏临时分配。运行期只做二分
	 * 查找；描述和参数指针一起复制，调用者修改原数组不会改变路由。
	 */
	for (size_t i = 0; i < method_count; ++i) {
		size_t j = i;
		while (j &&
		       tr_method_compare(&router->methods[j - 1].descriptor,
		                         &methods[i].descriptor) > 0) {
			router->methods[j] = router->methods[j - 1];
			--j;
		}
		if (j && !tr_method_compare(&router->methods[j - 1].descriptor,
		                            &methods[i].descriptor)) {
			ret = TR_ERR_STATE;
			goto fail;
		}
		router->methods[j] = methods[i];
	}
	router->channel = channel;
	router->reactor = reactor;
	router->budget = budget;
	router->bytes = bytes;
	router->capacity = capacity;
	router->method_count = method_count;
	router->terminal = terminal;
	router->arg = arg;
	for (uint32_t i = 0; i < capacity; ++i) {
		router->routes[i].router = router;
		router->routes[i].index = i;
	}
	*out = router;
	return TR_OK;
fail:
	free(router->methods);
	free(router->routes);
	free(router);
	(void)tr_memory_budget_release(budget, bytes);
	return ret;
}

int tr_rpc_record_router_admit_on_owner(struct tr_rpc_record_router *router,
                                        struct tr_stream_handle stream,
                                        struct tr_rpc_route_handle *out)
{
	struct tr_stream_diagnostics diagnostics;
	struct tr_rpc_route *free_route = NULL;
	int ret = tr_router_owner(router);
	if (ret != TR_OK)
		return ret;
	if (!out)
		return TR_ERR_INVALID;
	if (router->depth || router->retiring)
		return TR_ERR_STATE;
	if (stream.channel != router->channel)
		return TR_ERR_STALE;
	ret = tr_stream_get_diagnostics(stream, &diagnostics);
	if (ret != TR_OK)
		return ret;
	for (uint32_t i = 0; i < router->capacity; ++i) {
		struct tr_rpc_route *r = &router->routes[i];
		/* RESET 可以先释放传输槽，旧任务仍持有不同代次的路由。
		 * 只拒绝同一条流重复准入，不能让旧任务阻塞新流的空闲路由。 */
		if (r->active && r->stream.slot == stream.slot &&
		    r->stream.generation == stream.generation)
			return TR_ERR_STATE;
		if (!r->active && !free_route)
			free_route = r;
	}
	if (!free_route)
		return TR_AGAIN;
	if (router->generation == UINT64_MAX)
		return TR_ERR_STATE;
	free_route->stream = stream;
	free_route->generation = ++router->generation;
	free_route->active = 1;
	*out = tr_route_handle(free_route);
	return TR_OK;
}

int tr_rpc_record_router_receive_on_owner(struct tr_rpc_record_router *router,
                                          struct tr_stream_handle stream,
                                          const uint8_t *data, size_t size)
{
	const struct tr_rpc_record_limits limits = {
	        0, 0, TR_RPC_METADATA_MAX_BYTES, TR_RPC_RECORD_ACCEPT_NONE};
	struct tr_rpc_route *route;
	struct tr_rpc_record_header header;
	size_t total, amount;
	int ret = tr_router_owner(router);
	if (ret != TR_OK)
		return ret;
	if (!data && size)
		return TR_ERR_INVALID;
	if (router->depth || router->retiring)
		return TR_ERR_STATE;
	route = tr_router_find(router, stream);
	if (!route)
		return TR_ERR_STALE;
	if (route->terminal)
		return TR_ERR_STATE;
	++router->depth;
	if (!route->binding) {
		amount = sizeof(route->head) - route->used;
		if (amount > size)
			amount = size;
		if (amount) {
			memcpy(route->head + route->used, data, amount);
			route->used += amount;
			data += amount;
			size -= amount;
		}
		ret = tr_rpc_record_decode_header(route->head, route->used,
		                                  &limits, &header, &total);
		if (ret == TR_AGAIN) {
			ret = TR_OK;
			goto out;
		}
		if (ret == TR_OK && header.kind != TR_RPC_RECORD_OPEN)
			ret = TR_ERR_BAD_TYPE;
		if (ret != TR_OK)
			goto reject;
		route->method = tr_router_method(router, header.service_id,
		                                 header.method_id);
		if (!route->method) {
			ret = TR_ERR_UNSUPPORTED;
			tr_router_terminal(route, TR_RPC_STATUS_UNIMPLEMENTED,
			                   TR_RPC_RECORD_ON_OWNER);
			goto out;
		}
		struct tr_rpc_record_stream_config config = {
		        TR_RPC_RECORD_RESPONSE_DIRECTION,
		        route->method->descriptor};
		struct tr_rpc_record_stream_callbacks callbacks = {
		        tr_router_received, tr_router_binding_terminal, route};
		ret = tr_rpc_record_stream_create_on_owner(
		        stream, &config, &callbacks, router->budget,
		        &route->binding);
		if (ret != TR_OK) {
			/* 已吸收头的拒绝是终局，不能把预算 AGAIN
			 * 当成可重放输入。 */
			if (ret == TR_AGAIN)
				ret = TR_ERR_NOMEM;
			goto reject;
		}
		ret = tr_rpc_record_stream_receive_on_owner(
		        route->binding, stream, route->head,
		        sizeof(route->head));
		route->used = 0;
		if (ret != TR_OK || route->terminal)
			goto out;
	}
	ret = tr_rpc_record_stream_receive_on_owner(route->binding, stream,
	                                            data, size);
	goto out;
reject:
	tr_router_terminal(route,
	                   ret == TR_ERR_NOMEM
	                           ? TR_RPC_STATUS_RESOURCE_EXHAUSTED
	                           : TR_RPC_STATUS_INVALID_ARGUMENT,
	                   TR_RPC_RECORD_ON_OWNER);
out:
	--router->depth;
	return ret;
}

int tr_rpc_record_router_event_on_owner(struct tr_rpc_record_router *router,
                                        struct tr_stream_handle stream,
                                        enum tr_stream_event event, int status)
{
	struct tr_rpc_route *route;
	int ret = tr_router_owner(router);
	if (ret != TR_OK)
		return ret;
	if (router->depth || router->retiring)
		return TR_ERR_STATE;
	route = tr_router_find(router, stream);
	if (!route)
		return TR_ERR_STALE;
	if (event < TR_STREAM_EVENT_OPENED || event > TR_STREAM_EVENT_REFUSED)
		return TR_ERR_INVALID;
	if (event == TR_STREAM_EVENT_REFUSED && status != TR_ERR_NOMEM)
		return TR_ERR_INVALID;
	if (event == TR_STREAM_EVENT_RESET &&
	    tr_rpc_record_status_from_reset(status) ==
	            (enum tr_rpc_status)TR_RPC_STATUS_COUNT)
		return TR_ERR_INVALID;
	if (route->terminal)
		return TR_OK;
	++router->depth;
	if (route->binding)
		ret = tr_rpc_record_stream_event_on_owner(
		        route->binding, stream, event, status);
	else if (event == TR_STREAM_EVENT_RESET ||
	         event == TR_STREAM_EVENT_REFUSED)
		tr_router_terminal(route,
		                   tr_rpc_record_status_from_reset(status),
		                   TR_RPC_RECORD_ON_OWNER);
	else if (event != TR_STREAM_EVENT_OPENED &&
	         event != TR_STREAM_EVENT_WRITABLE)
		tr_router_terminal(route,
		                   route->used ? TR_RPC_STATUS_DATA_LOSS
		                               : TR_RPC_STATUS_UNAVAILABLE,
		                   TR_RPC_RECORD_ON_OWNER);
	--router->depth;
	return ret;
}

int tr_rpc_record_router_send_on_owner(
        struct tr_rpc_route_handle handle,
        const struct tr_rpc_record_header *header, const uint8_t *metadata,
        struct tr_buffer **body)
{
	struct tr_rpc_route *route;
	int ret = tr_router_lookup(handle, &route);
	if (ret != TR_OK)
		return ret;
	if (!route->binding || route->terminal)
		return TR_ERR_STATE;
	++handle.router->depth;
	ret = tr_rpc_record_stream_send_on_owner(route->binding, header,
	                                         metadata, body);
	--handle.router->depth;
	return ret;
}

int tr_rpc_record_router_abort_on_owner(struct tr_rpc_route_handle handle,
                                        enum tr_rpc_status status)
{
	struct tr_rpc_route *route;
	int ret = tr_router_lookup(handle, &route);
	if (ret != TR_OK)
		return ret;
	if (status <= TR_RPC_STATUS_OK || status >= (int)TR_RPC_STATUS_COUNT)
		return TR_ERR_INVALID;
	if (route->terminal)
		return TR_ERR_STATE;
	++handle.router->depth;
	if (route->binding)
		ret = tr_rpc_record_stream_abort_on_owner(route->binding,
		                                          status);
	else
		tr_router_terminal(route, status, TR_RPC_RECORD_ON_OWNER);
	--handle.router->depth;
	return ret;
}

int tr_rpc_record_router_retire_on_owner(struct tr_rpc_route_handle handle)
{
	struct tr_rpc_route *route;
	int ret = tr_router_owner(handle.router);
	if (ret != TR_OK)
		return ret;
	if (handle.router->depth)
		return TR_ERR_STATE;
	ret = tr_router_lookup(handle, &route);
	if (ret != TR_OK)
		return ret;
	if (handle.router->depth || !route->terminal)
		return TR_ERR_STATE;
	if (route->binding) {
		ret = tr_rpc_record_stream_destroy_on_owner(&route->binding);
		if (ret != TR_OK)
			return ret;
	}
	route->active = route->terminal = 0;
	route->used = 0;
	route->method = NULL;
	return TR_OK;
}

static int tr_router_free(struct tr_rpc_record_router **pointer)
{
	struct tr_rpc_record_router *router = *pointer;
	struct tr_memory_budget *budget = router->budget;
	size_t bytes = router->bytes;
	free(router->methods);
	free(router->routes);
	free(router);
	*pointer = NULL;
	return tr_memory_budget_release(budget, bytes);
}

int tr_rpc_record_router_destroy_on_owner(struct tr_rpc_record_router **pointer)
{
	struct tr_rpc_record_router *router;
	int ret;
	if (!pointer)
		return TR_ERR_INVALID;
	router = *pointer;
	ret = tr_router_owner(router);
	if (ret != TR_OK)
		return ret;
	if (router->depth)
		return TR_ERR_STATE;
	router->retiring = 1;
	++router->depth;
	for (uint32_t i = 0; i < router->capacity; ++i) {
		struct tr_rpc_route *route = &router->routes[i];
		if (!route->active)
			continue;
		if (route->binding) {
			if (!route->terminal)
				(void)tr_rpc_record_stream_abort_on_owner(
				        route->binding,
				        TR_RPC_STATUS_CANCELLED);
			ret = tr_rpc_record_stream_destroy_on_owner(
			        &route->binding);
			if (ret != TR_OK) {
				--router->depth;
				return ret;
			}
		} else {
			tr_router_terminal(route, TR_RPC_STATUS_CANCELLED,
			                   TR_RPC_RECORD_ON_OWNER);
		}
		route->active = 0;
	}
	return tr_router_free(pointer);
}

int tr_rpc_record_router_reap_stopped(struct tr_rpc_record_router **pointer)
{
	struct tr_reactor_recovery_guard guard = {0};
	struct tr_rpc_record_router *router;
	int ret;
	if (!pointer || !*pointer)
		return TR_ERR_INVALID;
	router = *pointer;
	ret = tr_reactor_recovery_acquire(router->reactor, &guard);
	if (ret != TR_OK)
		return ret;
	if (router->depth) {
		(void)tr_reactor_recovery_release(&guard);
		return TR_ERR_STATE;
	}
	router->retiring = 1;
	ret = tr_reactor_recovery_release(&guard);
	if (ret != TR_OK)
		return ret;
	/* 下层逐绑定持有自己的恢复租约。外部禁止重启/销毁父循环，不能
	 * 嵌套取得不同租约；任何一步失败仍保留上层对象和未完成责任。
	 */
	++router->depth;
	for (uint32_t i = 0; i < router->capacity; ++i) {
		struct tr_rpc_route *route = &router->routes[i];
		if (!route->active || !route->binding)
			continue;
		ret = tr_rpc_record_stream_reap_stopped(&route->binding);
		if (ret != TR_OK) {
			--router->depth;
			return ret;
		}
		if (route->terminal)
			route->active = 0;
	}
	--router->depth;
	ret = tr_reactor_recovery_acquire(router->reactor, &guard);
	if (ret != TR_OK)
		return ret;
	++router->depth;
	for (uint32_t i = 0; i < router->capacity; ++i) {
		struct tr_rpc_route *route = &router->routes[i];
		if (route->active)
			tr_router_terminal(route, TR_RPC_STATUS_CANCELLED,
			                   TR_RPC_RECORD_OWNER_STOPPED);
	}
	--router->depth;
	ret = tr_reactor_recovery_release(&guard);
	return ret == TR_OK ? tr_router_free(pointer) : ret;
}
