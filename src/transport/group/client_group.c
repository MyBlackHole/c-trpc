#define _GNU_SOURCE
#include "client_group_internal.h"

#include <assert.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>

#include "../../io/connector_internal.h"
#include "../../group/pipeline_control_wire_internal.h"
#include "../../group/pipeline_route_internal.h"
#include "../../execution/reactor_internal.h"
#include "../../io/socket_internal.h"
#include "../../execution/buffer.h"
#include "../../io/socket.h"
#include "tr/status.h"
#include "../protocol/wire.h"

#define TR_CLIENT_GROUP_CONTROL_BUFFER_BASE 4U
#define TR_CLIENT_GROUP_NO_SLOT UINT32_MAX
#define TR_CLIENT_GROUP_ADDRESS_CAPACITY 64U

enum tr_client_group_data_state {
	TR_CLIENT_GROUP_DATA_FREE = 0,
	TR_CLIENT_GROUP_DATA_QUEUED,
	TR_CLIENT_GROUP_DATA_CONNECTING,
	TR_CLIENT_GROUP_DATA_ACTIVE
};

struct tr_client_group_data {
	struct tr_pipeline_route_preface route;
	struct tr_conn_handle connection;
	uint64_t offer_message_id;
	enum tr_client_group_data_state state;
};

enum tr_client_group_transfer_state {
	TR_CLIENT_GROUP_TRANSFER_EMPTY = 0,
	TR_CLIENT_GROUP_TRANSFER_USED = 1,
	TR_CLIENT_GROUP_TRANSFER_TOMBSTONE = 2
};

struct tr_client_group_transfer {
	uint32_t stream_id;
	uint32_t data_slot;
	uint32_t data_generation;
	enum tr_client_group_transfer_state state;
};

struct tr_client_group {
	struct tr_client_group_config config;
	struct tr_conn_handle control;
	struct tr_pipeline_route_preface control_route;
	uint32_t control_generation;

	char address[TR_CLIENT_GROUP_ADDRESS_CAPACITY];
	uint16_t port;
	int control_connecting;
	int closing;
	int draining;

	/*
	 * 协议状态仍严格属于 Reactor owner。这里仅发布 drain lifecycle generation
	 * 给外部 waiter，不允许 waiter 借此锁直接读取/修改 transfer/data 状态。
	 */
	pthread_mutex_t drain_wait_lock;
	pthread_cond_t drain_wait_cond;
	uint64_t drain_generation;
	uint64_t drained_generation;
	uint32_t drain_waiters;
	int drain_wait_active;
	int drain_wait_closed;

	struct tr_buffer_pool control_pool;
	int control_pool_ready;

	struct tr_client_group_data *data;
	uint32_t data_capacity;

	struct tr_client_group_transfer *transfers;
	uint32_t transfer_limit;
	uint32_t transfer_capacity;
	uint32_t transfer_count;

	uint64_t send_bytes_limit;
	uint64_t send_bytes_inflight;

	uint32_t connector_slot;
	struct tr_connector *connector;
};

struct tr_client_group_send_buffer {
	struct tr_buffer buffer;
	struct tr_client_group *group;
	uint32_t accounted_bytes;
	uint8_t storage[];
};

void tr_connection_group_client_config_init(
	struct tr_connection_group_client_config *config)
{
	if (config)
		memset(config, 0, sizeof(*config));
}

static int tr_client_group_conn_equal(struct tr_conn_handle a,
				      struct tr_conn_handle b)
{
	return a.reactor == b.reactor && a.slot == b.slot &&
	       a.generation == b.generation;
}

static uint64_t tr_client_group_now_ns(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
		return 0;
	return (uint64_t)ts.tv_sec * UINT64_C(1000000000) +
	       (uint64_t)ts.tv_nsec;
}

static int
tr_client_group_drain_complete_on_owner(const struct tr_client_group *group)
{
	uint32_t i;

	if (!group || !group->draining || group->transfer_count != 0U ||
	    group->send_bytes_inflight != 0U ||
	    group->connector_slot != TR_CLIENT_GROUP_NO_SLOT)
		return 0;

	for (i = 0; i < group->data_capacity; ++i)
		if (group->data[i].state == TR_CLIENT_GROUP_DATA_QUEUED ||
		    group->data[i].state == TR_CLIENT_GROUP_DATA_CONNECTING)
			return 0;
	return 1;
}

static void tr_client_group_publish_drain_progress_on_owner(
	struct tr_client_group *group)
{
	if (!tr_client_group_drain_complete_on_owner(group))
		return;

	pthread_mutex_lock(&group->drain_wait_lock);
	if (group->drain_wait_active &&
	    group->drained_generation != group->drain_generation) {
		group->drained_generation = group->drain_generation;
		pthread_cond_broadcast(&group->drain_wait_cond);
	}
	pthread_mutex_unlock(&group->drain_wait_lock);
}

static void tr_client_group_publish_drain_start_on_owner(
	struct tr_client_group *group)
{
	pthread_mutex_lock(&group->drain_wait_lock);
	group->drain_generation++;
	if (group->drain_generation == 0U)
		group->drain_generation = 1U;
	group->drained_generation = 0U;
	group->drain_wait_active = 1;
	pthread_mutex_unlock(&group->drain_wait_lock);
}

static void tr_client_group_publish_connected_on_owner(
	struct tr_client_group *group)
{
	pthread_mutex_lock(&group->drain_wait_lock);
	group->drain_wait_active = 0;
	pthread_cond_broadcast(&group->drain_wait_cond);
	pthread_mutex_unlock(&group->drain_wait_lock);
}

static int tr_client_group_close_wait_admission(
	struct tr_client_group *group)
{
	int ret = TR_OK;

	pthread_mutex_lock(&group->drain_wait_lock);
	group->drain_wait_closed = 1;
	if (group->drain_waiters != 0U)
		ret = TR_ERR_STATE;
	pthread_mutex_unlock(&group->drain_wait_lock);
	return ret;
}

static int tr_client_group_connect_fd(const char *address, uint16_t port,
				      uint32_t timeout_ms, int *out_fd)
{
	struct pollfd pfd;
	int fd = -1;
	int ret;

	if (!address || !out_fd || port == 0U)
		return TR_ERR_INVALID;
	*out_fd = -1;

	ret = tr_tcp_connect_ipv4(address, port, &fd);
	if (ret == TR_OK) {
		*out_fd = fd;
		return TR_OK;
	}
	if (ret != TR_IN_PROGRESS)
		return ret;

	memset(&pfd, 0, sizeof(pfd));
	pfd.fd = fd;
	pfd.events = POLLOUT;
	do {
		ret = poll(&pfd, 1, (int)timeout_ms);
	} while (ret < 0 && errno == EINTR);
	if (ret == 0) {
		tr_socket_close(&fd);
		return TR_ERR_TIMEOUT;
	}
	if (ret < 0) {
		tr_socket_close(&fd);
		return TR_ERR_SYS;
	}

	ret = tr_tcp_finish_connect(fd);
	if (ret != TR_OK) {
		tr_socket_close(&fd);
		return ret;
	}
	*out_fd = fd;
	return TR_OK;
}

static int tr_client_group_send_all_fd(int fd, const uint8_t *data, size_t len,
				       uint32_t timeout_ms)
{
	uint64_t start_ns = tr_client_group_now_ns();

	if (fd < 0 || (len != 0U && !data))
		return TR_ERR_INVALID;

	while (len != 0U) {
		struct pollfd pfd;
		ssize_t n;
		int wait_ms;
		int ret;

		n = send(fd, data, len, MSG_NOSIGNAL);
		if (n > 0) {
			data += (size_t)n;
			len -= (size_t)n;
			continue;
		}
		if (n < 0 && errno == EINTR)
			continue;
		if (n >= 0 || (errno != EAGAIN && errno != EWOULDBLOCK))
			return TR_ERR_SYS;

		if (timeout_ms == 0U) {
			wait_ms = -1;
		} else {
			uint64_t now_ns = tr_client_group_now_ns();
			uint64_t elapsed_ms;

			if (now_ns == 0U || now_ns < start_ns)
				return TR_ERR_SYS;
			elapsed_ms = (now_ns - start_ns) / UINT64_C(1000000);
			if (elapsed_ms >= timeout_ms)
				return TR_ERR_TIMEOUT;
			wait_ms = (int)(timeout_ms - elapsed_ms);
		}

		memset(&pfd, 0, sizeof(pfd));
		pfd.fd = fd;
		pfd.events = POLLOUT;
		do {
			ret = poll(&pfd, 1, wait_ms);
		} while (ret < 0 && errno == EINTR);
		if (ret == 0)
			return TR_ERR_TIMEOUT;
		if (ret < 0)
			return TR_ERR_SYS;
	}
	return TR_OK;
}

static uint32_t tr_client_group_next_generation(struct tr_client_group *group)
{
	group->control_generation++;
	if (group->control_generation == 0U)
		group->control_generation = 1U;
	return group->control_generation;
}

static int tr_client_group_message_matches(
	const struct tr_client_group *group,
	const struct tr_pipeline_control_wire_message *message)
{
	return group && message &&
	       message->owner_shard_id == group->control_route.owner_shard_id &&
	       message->pipeline_id == group->control_route.pipeline_id &&
	       message->epoch == group->control_route.epoch;
}

static int tr_client_group_send_cancel_on_owner(
	struct tr_client_group *group,
	const struct tr_pipeline_route_preface *route, uint64_t message_id)
{
	struct tr_pipeline_control_wire_message cancel;
	struct tr_buffer *buffer = NULL;
	int ret;

	if (!group || !route || !group->control.reactor)
		return TR_ERR_STATE;

	ret = tr_buffer_acquire(
		&group->control_pool, TR_PIPELINE_CONTROL_WIRE_SIZE, &buffer);
	if (ret != TR_OK)
		return ret;

	memset(&cancel, 0, sizeof(cancel));
	cancel.version = TR_PIPELINE_CONTROL_WIRE_VERSION;
	cancel.type = TR_PIPELINE_CONTROL_DATA_CANCEL;
	cancel.owner_shard_id = route->owner_shard_id;
	cancel.pipeline_id = route->pipeline_id;
	cancel.epoch = route->epoch;
	cancel.data_index = route->member_index;
	cancel.data_generation = route->member_generation;
	ret = tr_pipeline_control_wire_encode(buffer->data, &cancel);
	if (ret != TR_OK)
		goto fail;
	buffer->len = TR_PIPELINE_CONTROL_WIRE_SIZE;

	ret = tr_reactor_send(
		group->control, TR_FRAME_PIPELINE_CONTROL, 0U, 0U,
		message_id, buffer);
	if (ret == TR_OK)
		return TR_OK;

fail:
	tr_buffer_release(buffer);
	return ret;
}

static void tr_client_group_start_next_on_owner(struct tr_client_group *group);
static void tr_client_group_invalidate_data_transfers(
	struct tr_client_group *group, uint32_t data_slot,
	uint32_t data_generation);

static void tr_client_group_start_next_on_owner(struct tr_client_group *group);
static void tr_client_group_invalidate_data_transfers(
	struct tr_client_group *group, uint32_t data_slot,
	uint32_t data_generation);

static enum tr_frame_disposition tr_client_group_data_frame(
	struct tr_conn_handle connection, struct tr_frame *frame, void *arg)
{
	(void)frame;
	(void)arg;
	/*
	 * Server-to-Client DATA delivery 尚未进入当前 public capability。
	 * 收到这类 payload 时明确终止 DATA lane，避免静默丢弃业务数据。
	 */
	(void)tr_reactor_abort_on_owner(connection, TR_ERR_UNSUPPORTED);
	return TR_FRAME_RELEASE;
}

static void tr_client_group_data_event(
	struct tr_conn_handle connection, enum tr_connection_event event,
	int status, void *arg)
{
	struct tr_client_group *group = (struct tr_client_group *)arg;
	uint32_t i;

	(void)event;
	(void)status;
	if (!group)
		return;

	for (i = 0; i < group->data_capacity; ++i) {
		int cancel_ret = TR_OK;

		if (group->data[i].state != TR_CLIENT_GROUP_DATA_ACTIVE ||
		    !tr_client_group_conn_equal(
			    group->data[i].connection, connection))
			continue;
		if (!group->closing && group->control.reactor)
			cancel_ret = tr_client_group_send_cancel_on_owner(
				group, &group->data[i].route,
				group->data[i].offer_message_id);
		tr_client_group_invalidate_data_transfers(
			group, i, group->data[i].route.member_generation);
		memset(&group->data[i], 0, sizeof(group->data[i]));
		group->data[i].state = TR_CLIENT_GROUP_DATA_FREE;
		if (cancel_ret != TR_OK && group->control.reactor) {
			(void)tr_reactor_abort_on_owner(
				group->control,
				cancel_ret < 0 ? cancel_ret : TR_ERR_STATE);
			return;
		}
		break;
	}
	if (!group->closing)
		tr_client_group_start_next_on_owner(group);
}

static void tr_client_group_connector_complete(
	int status, int fd, void *arg)
{
	struct tr_client_group *group = (struct tr_client_group *)arg;
	struct tr_client_group_data *data;
	struct tr_conn_handle connection;
	struct tr_pipeline_route_preface route;
	uint64_t message_id;
	uint32_t slot;
	int ret;
	int cancel_ret = TR_OK;

	if (!group) {
		tr_socket_close(&fd);
		return;
	}

	slot = group->connector_slot;
	if (slot >= group->data_capacity) {
		tr_socket_close(&fd);
		return;
	}

	data = &group->data[slot];
	route = data->route;
	message_id = data->offer_message_id;
	group->connector_slot = TR_CLIENT_GROUP_NO_SLOT;

	if (status != TR_OK) {
		memset(data, 0, sizeof(*data));
		data->state = TR_CLIENT_GROUP_DATA_FREE;
		if (group->control.reactor)
			cancel_ret = tr_client_group_send_cancel_on_owner(
				group, &route, message_id);
		if (cancel_ret != TR_OK && group->control.reactor) {
			(void)tr_reactor_abort_on_owner(
				group->control,
				cancel_ret < 0 ? cancel_ret : TR_ERR_STATE);
			return;
		}
		if (!group->closing && !group->draining)
			tr_client_group_start_next_on_owner(group);
		return;
	}

	/*
	 * TR_OK completion 把 connected fd ownership 转移给本 callback。
	 * adopt 成功后 ownership 再转移给 Reactor；其他失败路径必须在此 close。
	 */
	memset(&connection, 0, sizeof(connection));
	ret = tr_reactor_adopt_fd(group->config.owner, fd, &connection);
	if (ret != TR_OK) {
		tr_socket_close(&fd);
		goto fail;
	}
	fd = -1;

	ret = tr_reactor_set_handler(
		connection, tr_client_group_data_frame,
		tr_client_group_data_event, group);
	if (ret != TR_OK) {
		(void)tr_reactor_close_on_owner(connection);
		goto fail;
	}

	data->connection = connection;
	data->state = TR_CLIENT_GROUP_DATA_ACTIVE;
	tr_client_group_start_next_on_owner(group);
	return;

fail:
	memset(data, 0, sizeof(*data));
	data->state = TR_CLIENT_GROUP_DATA_FREE;
	if (group->control.reactor)
		cancel_ret = tr_client_group_send_cancel_on_owner(
			group, &route, message_id);
	if (cancel_ret != TR_OK && group->control.reactor) {
		(void)tr_reactor_abort_on_owner(
			group->control,
			cancel_ret < 0 ? cancel_ret : TR_ERR_STATE);
		return;
	}
	if (!group->closing && !group->draining)
		tr_client_group_start_next_on_owner(group);
}

static int tr_client_group_begin_connector_on_owner(
	struct tr_client_group *group, uint32_t slot)
{
	struct tr_client_group_data *data = &group->data[slot];
	uint8_t preface[TR_PIPELINE_ROUTE_PREFACE_SIZE];
	int ret;

	ret = tr_pipeline_route_preface_encode(preface, &data->route);
	if (ret != TR_OK)
		return ret;

	group->connector_slot = slot;
	data->state = TR_CLIENT_GROUP_DATA_CONNECTING;
	ret = tr_connector_start(
		group->connector, group->address, group->port,
		preface, sizeof(preface));
	return ret;
}

static void tr_client_group_start_next_on_owner(struct tr_client_group *group)
{
	uint32_t i;

	if (!group || group->closing || group->draining ||
	    !group->control.reactor ||
	    group->connector_slot != TR_CLIENT_GROUP_NO_SLOT)
		return;

	for (i = 0; i < group->data_capacity; ++i) {
		struct tr_pipeline_route_preface route;
		uint64_t message_id;
		int ret;
		int cancel_ret;

		if (group->data[i].state != TR_CLIENT_GROUP_DATA_QUEUED)
			continue;

		ret = tr_client_group_begin_connector_on_owner(group, i);
		if (ret == TR_OK)
			return;

		/*
		 * connector 进入 active 后的失败会通过 completion callback 收敛。
		 * 如果 callback 在 start() 内同步执行，它可能已经启动了下一条 offer；
		 * 此时 connector_slot 不再指向 i，外层不能重复处理。
		 */
		if (group->connector_slot != i)
			return;

		route = group->data[i].route;
		message_id = group->data[i].offer_message_id;
		group->connector_slot = TR_CLIENT_GROUP_NO_SLOT;
		memset(&group->data[i], 0, sizeof(group->data[i]));
		group->data[i].state = TR_CLIENT_GROUP_DATA_FREE;

		cancel_ret = tr_client_group_send_cancel_on_owner(
			group, &route, message_id);
		if (cancel_ret != TR_OK && group->control.reactor) {
			(void)tr_reactor_abort_on_owner(
				group->control,
				cancel_ret < 0 ? cancel_ret : TR_ERR_STATE);
			return;
		}
	}
}

static int tr_client_group_offer_duplicate(
	const struct tr_client_group *group,
	const struct tr_pipeline_route_preface *route)
{
	uint32_t i;

	for (i = 0; i < group->data_capacity; ++i) {
		const struct tr_pipeline_route_preface *existing =
			&group->data[i].route;

		if (group->data[i].state == TR_CLIENT_GROUP_DATA_FREE)
			continue;
		if (existing->member_index == route->member_index)
			return 1;
	}
	return 0;
}

static int tr_client_group_queue_offer_on_owner(
	struct tr_client_group *group,
	const struct tr_pipeline_control_wire_message *message,
	uint64_t message_id)
{
	struct tr_pipeline_route_preface route;
	uint32_t i;
	int ret;

	memset(&route, 0, sizeof(route));
	ret = tr_pipeline_control_wire_data_route(message, &route);
	if (ret != TR_OK)
		return ret;
	if (tr_client_group_offer_duplicate(group, &route))
		return TR_ERR_STATE;

	for (i = 0; i < group->data_capacity; ++i) {
		if (group->data[i].state != TR_CLIENT_GROUP_DATA_FREE)
			continue;
		group->data[i].route = route;
		group->data[i].offer_message_id = message_id;
		group->data[i].state = TR_CLIENT_GROUP_DATA_QUEUED;
		tr_client_group_start_next_on_owner(group);
		return TR_OK;
	}

	return tr_client_group_send_cancel_on_owner(group, &route, message_id);
}

static int tr_client_group_ready_data_slot(
	const struct tr_client_group *group,
	const struct tr_pipeline_control_wire_message *message,
	uint32_t *slot_out)
{
	uint32_t i;

	if (!group || !message || !slot_out)
		return TR_ERR_INVALID;

	for (i = 0; i < group->data_capacity; ++i) {
		const struct tr_pipeline_route_preface *route =
			&group->data[i].route;

		if (group->data[i].state != TR_CLIENT_GROUP_DATA_ACTIVE)
			continue;
		if (route->member_index != message->data_index ||
		    route->member_generation != message->data_generation)
			continue;
		if (route->owner_shard_id != message->owner_shard_id ||
		    route->pipeline_id != message->pipeline_id ||
		    route->epoch != message->epoch)
			continue;
		*slot_out = i;
		return TR_OK;
	}
	return TR_ERR_STALE;
}

static uint32_t tr_client_group_stream_hash(uint32_t stream_id)
{
	uint32_t value = stream_id;

	value ^= value >> 16;
	value *= UINT32_C(0x7feb352d);
	value ^= value >> 15;
	value *= UINT32_C(0x846ca68b);
	value ^= value >> 16;
	return value;
}

static int tr_client_group_transfer_find(
	struct tr_client_group *group, uint32_t stream_id,
	uint32_t *slot_out, uint32_t *insert_out)
{
	uint32_t start;
	uint32_t tombstone = UINT32_MAX;
	uint32_t offset;

	if (!group || stream_id == 0U || group->transfer_capacity == 0U)
		return 0;
	start = tr_client_group_stream_hash(stream_id) %
		group->transfer_capacity;
	for (offset = 0; offset < group->transfer_capacity; ++offset) {
		uint32_t index =
			(start + offset) % group->transfer_capacity;
		struct tr_client_group_transfer *transfer =
			&group->transfers[index];

		if (transfer->state == TR_CLIENT_GROUP_TRANSFER_USED) {
			if (transfer->stream_id == stream_id) {
				if (slot_out)
					*slot_out = index;
				return 1;
			}
			continue;
		}
		if (transfer->state == TR_CLIENT_GROUP_TRANSFER_TOMBSTONE) {
			if (tombstone == UINT32_MAX)
				tombstone = index;
			continue;
		}

		if (insert_out)
			*insert_out =
				tombstone != UINT32_MAX ? tombstone : index;
		return 0;
	}
	if (insert_out)
		*insert_out = tombstone;
	return 0;
}

static void tr_client_group_clear_transfers(struct tr_client_group *group)
{
	if (!group || !group->transfers)
		return;
	memset(group->transfers, 0,
	       (size_t)group->transfer_capacity * sizeof(*group->transfers));
	group->transfer_count = 0U;
	tr_client_group_publish_drain_progress_on_owner(group);
}

static void tr_client_group_invalidate_data_transfers(
	struct tr_client_group *group, uint32_t data_slot,
	uint32_t data_generation)
{
	uint32_t i;

	if (!group || !group->transfers)
		return;
	for (i = 0; i < group->transfer_capacity; ++i) {
		struct tr_client_group_transfer *transfer =
			&group->transfers[i];

		if (transfer->state != TR_CLIENT_GROUP_TRANSFER_USED ||
		    transfer->data_slot != data_slot ||
		    transfer->data_generation != data_generation)
			continue;
		transfer->state = TR_CLIENT_GROUP_TRANSFER_TOMBSTONE;
		if (group->transfer_count != 0U)
			group->transfer_count--;
	}
	tr_client_group_publish_drain_progress_on_owner(group);
}

static void tr_client_group_send_buffer_release(
	struct tr_buffer *buffer)
{
	struct tr_client_group_send_buffer *owned =
		(struct tr_client_group_send_buffer *)buffer;
	struct tr_client_group *group = owned->group;

	if (group) {
		if (group->send_bytes_inflight >= owned->accounted_bytes)
			group->send_bytes_inflight -= owned->accounted_bytes;
		else
			group->send_bytes_inflight = 0U;
		tr_client_group_publish_drain_progress_on_owner(group);
	}
	free(owned);
}

static int tr_client_group_transfer_connection_on_owner(
	struct tr_client_group *group, uint32_t stream_id,
	struct tr_conn_handle *connection_out)
{
	struct tr_client_group_transfer *transfer;
	struct tr_client_group_data *data;
	uint32_t slot = UINT32_MAX;

	if (!group || !connection_out || stream_id == 0U)
		return TR_ERR_INVALID;
	if (!tr_client_group_transfer_find(group, stream_id, &slot, NULL))
		return TR_ERR_STALE;

	transfer = &group->transfers[slot];
	if (transfer->data_slot >= group->data_capacity)
		return TR_ERR_STATE;
	data = &group->data[transfer->data_slot];
	if (data->state != TR_CLIENT_GROUP_DATA_ACTIVE ||
	    data->route.member_generation != transfer->data_generation ||
	    !data->connection.reactor) {
		transfer->state = TR_CLIENT_GROUP_TRANSFER_TOMBSTONE;
		if (group->transfer_count != 0U)
			group->transfer_count--;
		tr_client_group_publish_drain_progress_on_owner(group);
		return TR_ERR_STALE;
	}

	*connection_out = data->connection;
	return TR_OK;
}

static int tr_client_group_accept_transfer_ready_on_owner(
	struct tr_client_group *group,
	const struct tr_pipeline_control_wire_message *message,
	uint64_t message_id)
{
	struct tr_client_group_transfer *transfer;
	struct tr_connection_group_id public_group;
	uint32_t data_slot;
	uint32_t existing = UINT32_MAX;
	uint32_t insert = UINT32_MAX;
	int ret;

	if (!group || !message ||
	    message->type != TR_PIPELINE_CONTROL_TRANSFER_READY ||
	    message->stream_id == 0U)
		return TR_ERR_INVALID;
	if (group->transfer_capacity == 0U || !group->transfers)
		return TR_ERR_STATE;
	if (tr_client_group_transfer_find(
		    group, message->stream_id, &existing, &insert))
		return TR_ERR_STATE;
	if (group->transfer_count >= group->transfer_limit ||
	    insert == UINT32_MAX)
		return TR_ERR_STATE;

	ret = tr_client_group_ready_data_slot(group, message, &data_slot);
	if (ret != TR_OK)
		return ret;

	transfer = &group->transfers[insert];
	memset(transfer, 0, sizeof(*transfer));
	transfer->stream_id = message->stream_id;
	transfer->data_slot = data_slot;
	transfer->data_generation =
		group->data[data_slot].route.member_generation;
	transfer->state = TR_CLIENT_GROUP_TRANSFER_USED;
	group->transfer_count++;

	if (group->config.on_transfer_ready) {
		public_group.group_id = group->control_route.pipeline_id;
		public_group.epoch = group->control_route.epoch;
		group->config.on_transfer_ready(
			&public_group, message->stream_id, message_id,
			group->config.callback_arg);
	}
	return TR_OK;
}

static enum tr_frame_disposition tr_client_group_control_frame(
	struct tr_conn_handle connection, struct tr_frame *frame, void *arg)
{
	struct tr_client_group *group = (struct tr_client_group *)arg;
	struct tr_pipeline_control_wire_message message;
	int ret;

	if (!group || !frame ||
	    frame->header.type != TR_FRAME_PIPELINE_CONTROL ||
	    frame->header.flags != 0U || frame->header.stream_id != 0U ||
	    !frame->payload ||
	    frame->payload->len != TR_PIPELINE_CONTROL_WIRE_SIZE) {
		(void)tr_reactor_abort_on_owner(connection, TR_ERR_BAD_TYPE);
		return TR_FRAME_RELEASE;
	}

	memset(&message, 0, sizeof(message));
	ret = tr_pipeline_control_wire_decode(
		frame->payload->data, frame->payload->len, &message);
	if (ret != TR_OK || !tr_client_group_message_matches(group, &message)) {
		if (ret == TR_OK)
			ret = TR_ERR_STALE;
		(void)tr_reactor_abort_on_owner(connection, ret);
		return TR_FRAME_RELEASE;
	}

	if (message.type == TR_PIPELINE_CONTROL_DATA_OFFER) {
		if (group->data_capacity == 0U || group->draining) {
			struct tr_pipeline_route_preface route;

			memset(&route, 0, sizeof(route));
			ret = tr_pipeline_control_wire_data_route(
				&message, &route);
			if (ret == TR_OK)
				ret = tr_client_group_send_cancel_on_owner(
					group, &route,
					frame->header.message_id);
		} else {
			ret = tr_client_group_queue_offer_on_owner(
				group, &message, frame->header.message_id);
		}
		if (ret == TR_OK)
			return TR_FRAME_RELEASE;
	} else if (message.type == TR_PIPELINE_CONTROL_TRANSFER_READY) {
		/*
		 * begin_drain() is a monotonic local admission barrier. A READY
		 * observed after it returns must never create new local work, or
		 * wait_drained() could report quiescence and later become non-drained.
		 * The Server owns its affinity independently and may release it; final
		 * CONTROL close also fences remaining remote soft state.
		 */
		if (group->draining)
			return TR_FRAME_RELEASE;
		ret = tr_client_group_accept_transfer_ready_on_owner(
			group, &message, frame->header.message_id);
		if (ret == TR_OK)
			return TR_FRAME_RELEASE;
	} else {
		ret = TR_ERR_BAD_TYPE;
	}

	(void)tr_reactor_abort_on_owner(
		connection, ret < 0 ? ret : TR_ERR_STATE);
	return TR_FRAME_RELEASE;
}

static void tr_client_group_stop_data_on_owner(struct tr_client_group *group)
{
	uint32_t i;

	tr_client_group_clear_transfers(group);
	if (group->connector)
		(void)tr_connector_cancel(group->connector);
	group->connector_slot = TR_CLIENT_GROUP_NO_SLOT;

	for (i = 0; i < group->data_capacity; ++i) {
		struct tr_conn_handle connection = group->data[i].connection;

		if (group->data[i].state == TR_CLIENT_GROUP_DATA_ACTIVE &&
		    connection.reactor)
			(void)tr_reactor_close_on_owner(connection);
		memset(&group->data[i], 0, sizeof(group->data[i]));
		group->data[i].state = TR_CLIENT_GROUP_DATA_FREE;
	}
}

static void tr_client_group_control_event(
	struct tr_conn_handle connection, enum tr_connection_event event,
	int status, void *arg)
{
	struct tr_client_group *group = (struct tr_client_group *)arg;
	int was_closing;

	(void)event;
	(void)status;
	if (!group ||
	    !tr_client_group_conn_equal(group->control, connection))
		return;

	was_closing = group->closing;
	group->closing = 1;
	tr_client_group_stop_data_on_owner(group);
	memset(&group->control, 0, sizeof(group->control));
	memset(&group->control_route, 0, sizeof(group->control_route));
	memset(group->address, 0, sizeof(group->address));
	group->port = 0U;
	group->closing = was_closing;
}

struct tr_client_group_prepare_connect_request {
	struct tr_client_group *group;
	uint32_t generation;
};

static int tr_client_group_prepare_connect_on_owner(void *arg)
{
	struct tr_client_group_prepare_connect_request *request =
		(struct tr_client_group_prepare_connect_request *)arg;
	struct tr_client_group *group = request->group;

	if (group->control.reactor || group->control_connecting)
		return TR_ERR_STATE;

	group->control_connecting = 1;
	request->generation = tr_client_group_next_generation(group);
	return TR_OK;
}

static int tr_client_group_cancel_prepare_connect_on_owner(void *arg)
{
	struct tr_client_group_prepare_connect_request *request =
		(struct tr_client_group_prepare_connect_request *)arg;

	request->group->control_connecting = 0;
	return TR_OK;
}

struct tr_client_group_adopt_request {
	struct tr_client_group *group;
	int fd;
	int fd_consumed;
	int executed;
	struct tr_conn_handle connection;
	struct tr_pipeline_route_preface route;
	char address[TR_CLIENT_GROUP_ADDRESS_CAPACITY];
	uint16_t port;
};

static int tr_client_group_adopt_control_on_owner(void *arg)
{
	struct tr_client_group_adopt_request *request =
		(struct tr_client_group_adopt_request *)arg;
	struct tr_client_group *group = request->group;
	int ret;

	request->executed = 1;
	if (!group->control_connecting || group->control.reactor) {
		group->control_connecting = 0;
		return TR_ERR_STATE;
	}

	ret = tr_reactor_adopt_fd(
		group->config.owner, request->fd,
		&request->connection);
	if (ret != TR_OK)
		goto fail;
	request->fd_consumed = 1;

	ret = tr_reactor_set_handler(
		request->connection, tr_client_group_control_frame,
		tr_client_group_control_event, group);
	if (ret != TR_OK) {
		(void)tr_reactor_close_on_owner(request->connection);
		memset(&request->connection, 0, sizeof(request->connection));
		goto fail;
	}

	/*
	 * Handler publication and Group state publication are one owner turn.
	 * No callback can observe a partially initialized control route.
	 */
	group->control = request->connection;
	group->control_route = request->route;
	memcpy(group->address, request->address, sizeof(group->address));
	group->port = request->port;
	group->draining = 0;
	group->control_connecting = 0;
	tr_client_group_publish_connected_on_owner(group);
	return TR_OK;

fail:
	group->control_connecting = 0;
	return ret;
}

struct tr_client_group_close_request {
	struct tr_client_group *group;
};

static int tr_client_group_close_on_owner(void *arg)
{
	struct tr_client_group_close_request *request =
		(struct tr_client_group_close_request *)arg;
	struct tr_client_group *group = request->group;
	struct tr_conn_handle control;
	int ret;

	if (!group->control.reactor)
		return TR_ERR_STATE;

	group->closing = 1;
	tr_client_group_stop_data_on_owner(group);
	control = group->control;
	ret = tr_reactor_close_on_owner(control);
	if (ret == TR_ERR_STALE)
		ret = TR_OK;
	if (group->control.reactor &&
	    tr_client_group_conn_equal(group->control, control)) {
		memset(&group->control, 0, sizeof(group->control));
		memset(&group->control_route, 0,
		       sizeof(group->control_route));
		memset(group->address, 0, sizeof(group->address));
		group->port = 0U;
	}
	group->closing = 0;
	return ret;
}

int tr_client_group_create(const struct tr_client_group_config *config,
			   struct tr_client_group **out)
{
	struct tr_client_group *group;
	uint32_t control_buffers;
	int ret;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	if (!config || !config->owner || config->connect_timeout_ms == 0U ||
	    (config->tcp_nodelay != 0 && config->tcp_nodelay != 1) ||
	    config->max_data_connections >
		    UINT32_MAX - TR_CLIENT_GROUP_CONTROL_BUFFER_BASE ||
	    (config->max_data_connections != 0U &&
	     (config->max_transfers == 0U ||
	      config->max_transfers > UINT32_MAX / 2U ||
	      config->max_message_bytes == 0U ||
	      config->max_frame_payload_bytes == 0U ||
	      config->max_frame_payload_bytes > config->max_message_bytes)))
		return TR_ERR_INVALID;

	group = (struct tr_client_group *)calloc(1, sizeof(*group));
	if (!group)
		return TR_ERR_NOMEM;
	group->config = *config;
	group->data_capacity = config->max_data_connections;
	group->transfer_limit =
		config->max_data_connections != 0U ? config->max_transfers : 0U;
	group->transfer_capacity =
		group->transfer_limit != 0U ? group->transfer_limit * 2U : 0U;
	group->send_bytes_limit =
		(uint64_t)group->data_capacity *
		(uint64_t)config->max_message_bytes;
	group->connector_slot = TR_CLIENT_GROUP_NO_SLOT;

	control_buffers =
		config->max_data_connections + TR_CLIENT_GROUP_CONTROL_BUFFER_BASE;
	ret = tr_buffer_pool_init(
		&group->control_pool, control_buffers,
		TR_PIPELINE_CONTROL_WIRE_SIZE);
	if (ret != TR_OK)
		goto fail;
	group->control_pool_ready = 1;

	if (group->data_capacity != 0U) {
		group->data = (struct tr_client_group_data *)calloc(
			group->data_capacity, sizeof(*group->data));
		group->transfers = (struct tr_client_group_transfer *)calloc(
			group->transfer_capacity, sizeof(*group->transfers));
		if (!group->data || !group->transfers) {
			ret = TR_ERR_NOMEM;
			goto fail;
		}

		{
			struct tr_connector_config connector_config;

			memset(&connector_config, 0, sizeof(connector_config));
			connector_config.owner = group->config.owner;
			connector_config.timeout_ms =
				group->config.connect_timeout_ms;
			connector_config.tcp_nodelay = group->config.tcp_nodelay;
			connector_config.complete_cb =
				tr_client_group_connector_complete;
			connector_config.callback_arg = group;
			ret = tr_connector_create(
				&connector_config, &group->connector);
			if (ret != TR_OK)
				goto fail;
		}
	}

	*out = group;
	return TR_OK;

fail:
	tr_connector_destroy(group->connector);
	group->connector = NULL;
	free(group->transfers);
	free(group->data);
	if (group->control_pool_ready)
		tr_buffer_pool_destroy(&group->control_pool);
	free(group);
	return ret;
}

int tr_client_group_connect(
	struct tr_client_group *group, const char *ipv4_address, uint16_t port,
	const struct tr_connection_group_id *id)
{
	struct tr_client_group_prepare_connect_request prepare;
	struct tr_client_group_adopt_request request;
	struct tr_pipeline_route_preface route;
	uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE];
	size_t address_len;
	int fd = -1;
	int ret;
	int prepared = 0;

	if (!group || !ipv4_address || !id || port == 0U ||
	    id->group_id == 0U || id->epoch == 0U)
		return TR_ERR_INVALID;

	address_len = strlen(ipv4_address);
	if (address_len == 0U || address_len >= TR_CLIENT_GROUP_ADDRESS_CAPACITY)
		return TR_ERR_INVALID;

	memset(&prepare, 0, sizeof(prepare));
	prepare.group = group;
	ret = tr_reactor_call(
		group->config.owner,
		tr_client_group_prepare_connect_on_owner, &prepare);
	if (ret != TR_OK)
		return ret;
	prepared = 1;

	ret = tr_client_group_connect_fd(
		ipv4_address, port, group->config.connect_timeout_ms, &fd);
	if (ret != TR_OK)
		goto fail;
	if (group->config.tcp_nodelay) {
		ret = tr_tcp_set_nodelay(fd, 1);
		if (ret != TR_OK)
			goto fail;
	}

	memset(&route, 0, sizeof(route));
	route.version = TR_PIPELINE_ROUTE_VERSION;
	route.role = TR_PIPELINE_ROUTE_CONTROL;
	route.owner_shard_id = 0U;
	route.pipeline_id = id->group_id;
	route.epoch = id->epoch;
	route.member_index = TR_PIPELINE_ROUTE_MEMBER_CONTROL;
	route.member_generation = prepare.generation;
	ret = tr_pipeline_route_preface_encode(raw, &route);
	if (ret != TR_OK)
		goto fail;
	ret = tr_client_group_send_all_fd(
		fd, raw, sizeof(raw), group->config.connect_timeout_ms);
	if (ret != TR_OK)
		goto fail;

	memset(&request, 0, sizeof(request));
	request.group = group;
	request.fd = fd;
	request.route = route;
	memcpy(request.address, ipv4_address, address_len + 1U);
	request.port = port;
	ret = tr_reactor_call(
		group->config.owner,
		tr_client_group_adopt_control_on_owner, &request);
	if (request.executed)
		prepared = 0; /* owner adopt path consumes or clears reservation */
	if (request.fd_consumed)
		fd = -1;
	if (ret != TR_OK)
		goto fail;
	return TR_OK;

fail:
	tr_socket_close(&fd);
	if (prepared)
		(void)tr_reactor_call(
			group->config.owner,
			tr_client_group_cancel_prepare_connect_on_owner, &prepare);
	return ret;
}

int tr_client_group_close(struct tr_client_group *group)
{
	struct tr_client_group_close_request request;

	if (!group)
		return TR_ERR_INVALID;
	request.group = group;
	return tr_reactor_call(
		group->config.owner, tr_client_group_close_on_owner, &request);
}

struct tr_client_group_drain_request {
	struct tr_client_group *group;
};

static int tr_client_group_begin_drain_on_owner(void *arg)
{
	struct tr_client_group_drain_request *request =
		(struct tr_client_group_drain_request *)arg;
	struct tr_client_group *group = request->group;
	uint32_t i;

	if (!group->control.reactor)
		return TR_ERR_STATE;
	if (group->draining)
		return TR_OK;

	group->draining = 1;

	if (group->connector_slot != TR_CLIENT_GROUP_NO_SLOT) {
		uint32_t slot = group->connector_slot;
		struct tr_pipeline_route_preface route;
		uint64_t message_id;
		int ret;

		if (slot >= group->data_capacity)
			return TR_ERR_STATE;
		route = group->data[slot].route;
		message_id = group->data[slot].offer_message_id;
		(void)tr_connector_cancel(group->connector);
		group->connector_slot = TR_CLIENT_GROUP_NO_SLOT;
		memset(&group->data[slot], 0, sizeof(group->data[slot]));
		group->data[slot].state = TR_CLIENT_GROUP_DATA_FREE;
		ret = tr_client_group_send_cancel_on_owner(
			group, &route, message_id);
		if (ret != TR_OK) {
			(void)tr_reactor_abort_on_owner(
				group->control,
				ret < 0 ? ret : TR_ERR_STATE);
			return ret;
		}
	}

	for (i = 0; i < group->data_capacity; ++i) {
		struct tr_client_group_data *data = &group->data[i];
		int ret;

		if (data->state != TR_CLIENT_GROUP_DATA_QUEUED)
			continue;
		ret = tr_client_group_send_cancel_on_owner(
			group, &data->route, data->offer_message_id);
		memset(data, 0, sizeof(*data));
		data->state = TR_CLIENT_GROUP_DATA_FREE;
		if (ret != TR_OK) {
			(void)tr_reactor_abort_on_owner(
				group->control,
				ret < 0 ? ret : TR_ERR_STATE);
			return ret;
		}
	}
	return TR_OK;
}

int tr_client_group_begin_drain(struct tr_client_group *group)
{
	struct tr_client_group_drain_request request;

	if (!group)
		return TR_ERR_INVALID;
	request.group = group;
	return tr_reactor_call(
		group->config.owner,
		tr_client_group_begin_drain_on_owner, &request);
}

struct tr_client_group_stats_request {
	struct tr_client_group *group;
	struct tr_connection_group_client_stats *out;
};

static int tr_client_group_stats_on_owner(void *arg)
{
	struct tr_client_group_stats_request *request =
		(struct tr_client_group_stats_request *)arg;
	struct tr_client_group *group = request->group;
	struct tr_connection_group_client_stats *out = request->out;
	uint32_t i;

	memset(out, 0, sizeof(*out));
	out->group.group_id = group->control_route.pipeline_id;
	out->group.epoch = group->control_route.epoch;
	out->control_connected = group->control.reactor ? 1U : 0U;
	out->draining = group->draining ? 1U : 0U;
	out->active_transfers = group->transfer_count;
	out->active_transfer_limit = group->transfer_limit;
	out->send_bytes_inflight = group->send_bytes_inflight;
	out->send_bytes_limit = group->send_bytes_limit;
	for (i = 0; i < group->data_capacity; ++i)
		if (group->data[i].state == TR_CLIENT_GROUP_DATA_ACTIVE)
			out->data_connections++;
	return TR_OK;
}

int tr_client_group_get_stats(
	struct tr_client_group *group,
	struct tr_connection_group_client_stats *out)
{
	struct tr_client_group_stats_request request;

	if (!group || !out)
		return TR_ERR_INVALID;
	request.group = group;
	request.out = out;
	return tr_reactor_call(
		group->config.owner, tr_client_group_stats_on_owner, &request);
}

struct tr_client_group_release_transfer_request {
	struct tr_client_group *group;
	uint32_t stream_id;
};

static int tr_client_group_release_transfer_on_owner(void *arg)
{
	struct tr_client_group_release_transfer_request *request =
		(struct tr_client_group_release_transfer_request *)arg;
	struct tr_client_group *group = request->group;
	uint32_t slot = UINT32_MAX;

	if (!tr_client_group_transfer_find(
		    group, request->stream_id, &slot, NULL))
		return TR_ERR_STALE;
	group->transfers[slot].state =
		TR_CLIENT_GROUP_TRANSFER_TOMBSTONE;
	if (group->transfer_count != 0U)
		group->transfer_count--;
	tr_client_group_publish_drain_progress_on_owner(group);
	return TR_OK;
}

int tr_client_group_release_transfer(
	struct tr_client_group *group, uint32_t stream_id)
{
	struct tr_client_group_release_transfer_request request;

	if (!group || stream_id == 0U)
		return TR_ERR_INVALID;
	request.group = group;
	request.stream_id = stream_id;
	return tr_reactor_call(
		group->config.owner,
		tr_client_group_release_transfer_on_owner, &request);
}

struct tr_client_group_send_request {
	struct tr_client_group *group;
	uint32_t stream_id;
	uint64_t message_id;
	const struct tr_transport_bytes *bytes;
};

static int tr_client_group_send_on_owner(void *arg)
{
	struct tr_client_group_send_request *request =
		(struct tr_client_group_send_request *)arg;
	struct tr_client_group *group = request->group;
	const struct tr_transport_bytes *bytes = request->bytes;
	struct tr_client_group_send_buffer *owned = NULL;
	struct tr_conn_handle connection;
	struct tr_buffer *payload = NULL;
	size_t allocation_size;
	int ret;

	if (!group->control.reactor)
		return TR_ERR_CLOSED;
	if (bytes->len > group->config.max_message_bytes)
		return TR_ERR_BAD_LENGTH;

	ret = tr_client_group_transfer_connection_on_owner(
		group, request->stream_id, &connection);
	if (ret != TR_OK)
		return ret;

	if (bytes->len != 0U) {
		if (group->send_bytes_inflight > group->send_bytes_limit ||
		    (uint64_t)bytes->len >
			    group->send_bytes_limit -
				    group->send_bytes_inflight)
			return TR_AGAIN;
#if SIZE_MAX <= UINT32_MAX
		if ((size_t)bytes->len >
		    SIZE_MAX - sizeof(*owned))
			return TR_ERR_BAD_LENGTH;
#endif
		allocation_size = sizeof(*owned) + (size_t)bytes->len;
		owned = (struct tr_client_group_send_buffer *)malloc(
			allocation_size);
		if (!owned)
			return TR_ERR_NOMEM;
		memset(owned, 0, sizeof(*owned));
		owned->group = group;
		owned->accounted_bytes = bytes->len;
		owned->buffer.data = owned->storage;
		owned->buffer.capacity = bytes->len;
		owned->buffer.len = bytes->len;
			owned->buffer.release_cb =
			tr_client_group_send_buffer_release;
		memcpy(owned->storage, bytes->data, bytes->len);
		group->send_bytes_inflight += bytes->len;
		payload = &owned->buffer;
	}

	ret = tr_reactor_sendv_limited(
		connection, TR_FRAME_DATA, 0U, request->stream_id,
		request->message_id, payload ? &payload : NULL,
		payload ? 1U : 0U, group->config.max_frame_payload_bytes);
	if (ret != TR_OK && payload)
		tr_buffer_release(payload);
	return ret;
}

int tr_client_group_send(
	struct tr_client_group *group, uint32_t stream_id,
	uint64_t message_id, const struct tr_transport_bytes *bytes)
{
	struct tr_client_group_send_request request;

	if (!group || stream_id == 0U || !bytes ||
	    (bytes->len != 0U && !bytes->data))
		return TR_ERR_INVALID;
	request.group = group;
	request.stream_id = stream_id;
	request.message_id = message_id;
	request.bytes = bytes;
	return tr_reactor_call(
		group->config.owner, tr_client_group_send_on_owner, &request);
}

static int tr_client_group_detach_on_owner(void *arg)
{
	struct tr_client_group *group = (struct tr_client_group *)arg;
	struct tr_conn_handle control;
	int ret = TR_OK;

	/*
	 * destroy 的 owner barrier 必须无条件执行，不能通过外部线程读取
	 * group->control.reactor 来猜测 callback 是否已经退出。
	 */
	group->closing = 1;
	group->draining = 1;
	group->control_connecting = 0;
	tr_client_group_stop_data_on_owner(group);

	control = group->control;
	if (control.reactor) {
		ret = tr_reactor_close_on_owner(control);
		if (ret == TR_ERR_STALE)
			ret = TR_OK;
	}

	memset(&group->control, 0, sizeof(group->control));
	memset(&group->control_route, 0, sizeof(group->control_route));
	memset(group->address, 0, sizeof(group->address));
	group->port = 0U;

	/*
	 * Every Group-owned DATA payload carries a raw group pointer in its release
	 * callback. Closing all DATA connections above must synchronously release
	 * their TX queues before this owner barrier returns. Never free Group
	 * storage while such a callback can still exist.
	 */
	if (group->send_bytes_inflight != 0U && ret == TR_OK)
		ret = TR_ERR_STATE;
	return ret;
}

void tr_client_group_destroy(struct tr_client_group *group)
{
	int ret;

	if (!group)
		return;

	/*
	 * Client facade guarantees destroy runs before Runtime stop. Use a
	 * synchronous owner barrier so every callback that can reference group has
	 * returned before storage is released. Fail closed if ownership cannot
	 * converge.
	 */
	ret = tr_reactor_call(
		group->config.owner, tr_client_group_detach_on_owner, group);
#ifndef NDEBUG
	assert(ret == TR_OK);
#endif
	if (ret != TR_OK)
		return;

	tr_connector_destroy(group->connector);
	group->connector = NULL;
	free(group->transfers);
	free(group->data);
	if (group->control_pool_ready)
		tr_buffer_pool_destroy(&group->control_pool);
	free(group);
}
