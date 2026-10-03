#include "client_group_internal.h"

#include <errno.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>

#include "pipeline_control_wire_internal.h"
#include "pipeline_route_internal.h"
#include "reactor_internal.h"
#include "socket_internal.h"
#include "tr/buffer.h"
#include "tr/socket.h"
#include "tr/status.h"
#include "tr/wire.h"

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

struct tr_client_group {
	struct tr_client_group_config config;
	struct tr_conn_handle control;
	struct tr_pipeline_route_preface control_route;
	uint32_t control_generation;

	char address[TR_CLIENT_GROUP_ADDRESS_CAPACITY];
	uint16_t port;
	int closing;

	struct tr_buffer_pool control_pool;
	int control_pool_ready;

	struct tr_client_group_data *data;
	uint32_t data_capacity;

	uint32_t connector_slot;
	int connector_fd;
	int connector_connecting;
	int connector_watched;
	uint8_t connector_preface[TR_PIPELINE_ROUTE_PREFACE_SIZE];
	size_t connector_preface_sent;

	struct tr_reactor_timer_handle connector_timer;
	int connector_timer_ready;
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

static void tr_client_group_connector_unwatch_on_owner(
	struct tr_client_group *group)
{
	if (!group->connector_watched)
		return;
	(void)tr_reactor_aux_event_unregister(
		group->config.owner, group->connector_fd);
	group->connector_watched = 0;
}

static void tr_client_group_connector_disarm_on_owner(
	struct tr_client_group *group)
{
	if (group->connector_timer_ready)
		(void)tr_reactor_timer_arm(group->connector_timer, 0U);
}

static void tr_client_group_connector_reset_on_owner(
	struct tr_client_group *group, int close_fd)
{
	tr_client_group_connector_unwatch_on_owner(group);
	tr_client_group_connector_disarm_on_owner(group);
	if (close_fd)
		tr_socket_close(&group->connector_fd);
	else
		group->connector_fd = -1;
	group->connector_slot = TR_CLIENT_GROUP_NO_SLOT;
	group->connector_connecting = 0;
	group->connector_preface_sent = 0U;
	memset(group->connector_preface, 0, sizeof(group->connector_preface));
}

static int tr_client_group_connector_watch_on_owner(
	struct tr_client_group *group);

static void tr_client_group_fail_connector_on_owner(
	struct tr_client_group *group, int cancel_reservation)
{
	uint32_t slot = group->connector_slot;
	struct tr_pipeline_route_preface route;
	uint64_t message_id = 0U;
	int cancel_ret = TR_OK;

	memset(&route, 0, sizeof(route));
	if (slot < group->data_capacity) {
		route = group->data[slot].route;
		message_id = group->data[slot].offer_message_id;
	}
	tr_client_group_connector_reset_on_owner(group, 1);

	if (slot < group->data_capacity) {
		memset(&group->data[slot], 0, sizeof(group->data[slot]));
		group->data[slot].state = TR_CLIENT_GROUP_DATA_FREE;
	}
	if (cancel_reservation && group->control.reactor)
		cancel_ret = tr_client_group_send_cancel_on_owner(
			group, &route, message_id);
	if (cancel_ret != TR_OK) {
		(void)tr_reactor_abort_on_owner(group->control, cancel_ret < 0 ?
						      cancel_ret :
						      TR_ERR_STATE);
		return;
	}
	if (!group->closing)
		tr_client_group_start_next_on_owner(group);
}

static enum tr_frame_disposition tr_client_group_data_frame(
	struct tr_conn_handle connection, struct tr_frame *frame, void *arg)
{
	(void)frame;
	(void)arg;
	/*
	 * Server-to-Client DATA delivery is not part of this P3 slice. Fail
	 * explicitly instead of silently discarding application payload.
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
		if (group->data[i].state != TR_CLIENT_GROUP_DATA_ACTIVE ||
		    !tr_client_group_conn_equal(
			    group->data[i].connection, connection))
			continue;
		memset(&group->data[i], 0, sizeof(group->data[i]));
		group->data[i].state = TR_CLIENT_GROUP_DATA_FREE;
		break;
	}
	if (!group->closing)
		tr_client_group_start_next_on_owner(group);
}

static int tr_client_group_connector_finish_on_owner(
	struct tr_client_group *group)
{
	struct tr_client_group_data *data;
	struct tr_conn_handle connection;
	uint32_t slot = group->connector_slot;
	int fd;
	int ret;

	if (slot >= group->data_capacity)
		return TR_ERR_STATE;
	data = &group->data[slot];

	tr_client_group_connector_unwatch_on_owner(group);
	tr_client_group_connector_disarm_on_owner(group);
	fd = group->connector_fd;
	group->connector_fd = -1;

	memset(&connection, 0, sizeof(connection));
	ret = tr_reactor_adopt_fd(group->config.owner, fd, &connection);
	if (ret != TR_OK) {
		tr_socket_close(&fd);
		tr_client_group_connector_reset_on_owner(group, 0);
		memset(data, 0, sizeof(*data));
		data->state = TR_CLIENT_GROUP_DATA_FREE;
		tr_client_group_start_next_on_owner(group);
		return ret;
	}

	ret = tr_reactor_set_handler(
		connection, tr_client_group_data_frame,
		tr_client_group_data_event, group);
	if (ret != TR_OK) {
		(void)tr_reactor_close_on_owner(connection);
		tr_client_group_connector_reset_on_owner(group, 0);
		memset(data, 0, sizeof(*data));
		data->state = TR_CLIENT_GROUP_DATA_FREE;
		tr_client_group_start_next_on_owner(group);
		return ret;
	}

	data->connection = connection;
	data->state = TR_CLIENT_GROUP_DATA_ACTIVE;
	group->connector_slot = TR_CLIENT_GROUP_NO_SLOT;
	group->connector_connecting = 0;
	group->connector_preface_sent = 0U;
	memset(group->connector_preface, 0, sizeof(group->connector_preface));
	tr_client_group_start_next_on_owner(group);
	return TR_OK;
}

static int tr_client_group_connector_send_preface_on_owner(
	struct tr_client_group *group)
{
	while (group->connector_preface_sent <
	       sizeof(group->connector_preface)) {
		ssize_t n = send(
			group->connector_fd,
			group->connector_preface +
				group->connector_preface_sent,
			sizeof(group->connector_preface) -
				group->connector_preface_sent,
			MSG_NOSIGNAL);

		if (n > 0) {
			group->connector_preface_sent += (size_t)n;
			continue;
		}
		if (n < 0 && errno == EINTR)
			continue;
		if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
			int ret = tr_client_group_connector_watch_on_owner(group);

			if (ret != TR_OK)
				tr_client_group_fail_connector_on_owner(group, 1);
			return ret;
		}
		tr_client_group_fail_connector_on_owner(group, 1);
		return TR_ERR_SYS;
	}

	/*
	 * Once the complete route is on the wire, Server owns the decision:
	 * successful attach retires membership on DATA close; rejected attach
	 * exact-cancels the still-RESERVED capability. Do not send DATA_CANCEL
	 * from this point because the route may already be ATTACHED.
	 */
	return tr_client_group_connector_finish_on_owner(group);
}

static int tr_client_group_connector_progress_on_owner(
	struct tr_client_group *group)
{
	int ret;

	if (group->connector_fd < 0 ||
	    group->connector_slot >= group->data_capacity)
		return TR_ERR_STATE;

	if (group->connector_connecting) {
		ret = tr_tcp_finish_connect(group->connector_fd);
		if (ret != TR_OK) {
			tr_client_group_fail_connector_on_owner(group, 1);
			return ret;
		}
		group->connector_connecting = 0;
		if (group->config.tcp_nodelay) {
			ret = tr_tcp_set_nodelay(group->connector_fd, 1);
			if (ret != TR_OK) {
				tr_client_group_fail_connector_on_owner(group, 1);
				return ret;
			}
		}
	}
	return tr_client_group_connector_send_preface_on_owner(group);
}

static void tr_client_group_connector_event(
	int fd, uint32_t events, void *arg)
{
	struct tr_client_group *group = (struct tr_client_group *)arg;

	if (!group || fd != group->connector_fd)
		return;
	if (events & (EPOLLERR | EPOLLHUP)) {
		(void)tr_client_group_connector_progress_on_owner(group);
		return;
	}
	if (events & EPOLLOUT)
		(void)tr_client_group_connector_progress_on_owner(group);
}

static int tr_client_group_connector_watch_on_owner(
	struct tr_client_group *group)
{
	int ret;

	if (group->connector_watched)
		return TR_OK;
	ret = tr_reactor_aux_event_register(
		group->config.owner, group->connector_fd, EPOLLOUT,
		tr_client_group_connector_event, group);
	if (ret == TR_OK)
		group->connector_watched = 1;
	return ret;
}

static uint64_t tr_client_group_connector_timeout(
	void *arg, uint64_t now_ns)
{
	struct tr_client_group *group = (struct tr_client_group *)arg;

	(void)now_ns;
	if (group && group->connector_slot != TR_CLIENT_GROUP_NO_SLOT)
		tr_client_group_fail_connector_on_owner(group, 1);
	return 0U;
}

static int tr_client_group_connector_arm_on_owner(
	struct tr_client_group *group)
{
	uint64_t now_ns;
	uint64_t delay_ns;
	uint64_t deadline_ns;

	if (!group->connector_timer_ready)
		return TR_ERR_STATE;
	now_ns = tr_client_group_now_ns();
	if (now_ns == 0U)
		return TR_ERR_SYS;
	delay_ns =
		(uint64_t)group->config.connect_timeout_ms * UINT64_C(1000000);
	deadline_ns = UINT64_MAX - now_ns < delay_ns ?
			      UINT64_MAX :
			      now_ns + delay_ns;
	return tr_reactor_timer_arm(group->connector_timer, deadline_ns);
}

static int tr_client_group_begin_connector_on_owner(
	struct tr_client_group *group, uint32_t slot)
{
	struct tr_client_group_data *data = &group->data[slot];
	int fd = -1;
	int ret;

	ret = tr_pipeline_route_preface_encode(
		group->connector_preface, &data->route);
	if (ret != TR_OK)
		return ret;

	ret = tr_tcp_connect_ipv4(group->address, group->port, &fd);
	if (ret != TR_OK && ret != TR_IN_PROGRESS)
		return ret;

	group->connector_slot = slot;
	group->connector_fd = fd;
	group->connector_connecting = ret == TR_IN_PROGRESS;
	group->connector_preface_sent = 0U;
	data->state = TR_CLIENT_GROUP_DATA_CONNECTING;

	ret = tr_client_group_connector_arm_on_owner(group);
	if (ret != TR_OK) {
		tr_client_group_fail_connector_on_owner(group, 1);
		return ret;
	}

	if (group->connector_connecting) {
		ret = tr_client_group_connector_watch_on_owner(group);
		if (ret != TR_OK)
			tr_client_group_fail_connector_on_owner(group, 1);
		return ret;
	}

	if (group->config.tcp_nodelay) {
		ret = tr_tcp_set_nodelay(group->connector_fd, 1);
		if (ret != TR_OK) {
			tr_client_group_fail_connector_on_owner(group, 1);
			return ret;
		}
	}
	return tr_client_group_connector_send_preface_on_owner(group);
}

static void tr_client_group_start_next_on_owner(struct tr_client_group *group)
{
	uint32_t i;

	if (!group || group->closing || !group->control.reactor ||
	    group->connector_slot != TR_CLIENT_GROUP_NO_SLOT)
		return;

	for (i = 0; i < group->data_capacity; ++i) {
		int ret;

		if (group->data[i].state != TR_CLIENT_GROUP_DATA_QUEUED)
			continue;
		ret = tr_client_group_begin_connector_on_owner(group, i);
		if (ret == TR_OK)
			return;
		if (group->data[i].state == TR_CLIENT_GROUP_DATA_FREE)
			return;

		/*
		 * Failures before connector ownership is published still need exact
		 * cancellation. If cancellation succeeds, continue with the next
		 * queued offer instead of leaving it stranded.
		 */
		{
			int cancel_ret = tr_client_group_send_cancel_on_owner(
				group, &group->data[i].route,
				group->data[i].offer_message_id);

			memset(&group->data[i], 0, sizeof(group->data[i]));
			group->data[i].state = TR_CLIENT_GROUP_DATA_FREE;
			if (cancel_ret != TR_OK && group->control.reactor) {
				(void)tr_reactor_abort_on_owner(
					group->control,
					cancel_ret < 0 ? cancel_ret :
							 TR_ERR_STATE);
				return;
			}
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
		if (group->data_capacity == 0U) {
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
	} else {
		/* TRANSFER_READY consumption is the next P3 slice. */
		ret = TR_ERR_UNSUPPORTED;
	}

	(void)tr_reactor_abort_on_owner(
		connection, ret < 0 ? ret : TR_ERR_STATE);
	return TR_FRAME_RELEASE;
}

static void tr_client_group_stop_data_on_owner(struct tr_client_group *group)
{
	uint32_t i;

	tr_client_group_connector_unwatch_on_owner(group);
	tr_client_group_connector_disarm_on_owner(group);
	tr_socket_close(&group->connector_fd);
	group->connector_slot = TR_CLIENT_GROUP_NO_SLOT;
	group->connector_connecting = 0;
	group->connector_preface_sent = 0U;

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

struct tr_client_group_adopt_request {
	struct tr_client_group *group;
	int fd;
	int fd_consumed;
	struct tr_conn_handle connection;
};

static int tr_client_group_adopt_control_on_owner(void *arg)
{
	struct tr_client_group_adopt_request *request =
		(struct tr_client_group_adopt_request *)arg;
	int ret;

	if (request->group->control.reactor)
		return TR_ERR_STATE;

	ret = tr_reactor_adopt_fd(
		request->group->config.owner, request->fd,
		&request->connection);
	if (ret != TR_OK)
		return ret;
	request->fd_consumed = 1;

	ret = tr_reactor_set_handler(
		request->connection, tr_client_group_control_frame,
		tr_client_group_control_event, request->group);
	if (ret != TR_OK) {
		(void)tr_reactor_close_on_owner(request->connection);
		memset(&request->connection, 0, sizeof(request->connection));
		return ret;
	}

	request->group->control = request->connection;
	return TR_OK;
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
		    UINT32_MAX - TR_CLIENT_GROUP_CONTROL_BUFFER_BASE)
		return TR_ERR_INVALID;

	group = (struct tr_client_group *)calloc(1, sizeof(*group));
	if (!group)
		return TR_ERR_NOMEM;
	group->config = *config;
	group->data_capacity = config->max_data_connections;
	group->connector_slot = TR_CLIENT_GROUP_NO_SLOT;
	group->connector_fd = -1;

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
		if (!group->data) {
			ret = TR_ERR_NOMEM;
			goto fail;
		}

		ret = tr_reactor_timer_register(
			group->config.owner,
			tr_client_group_connector_timeout, group,
			&group->connector_timer);
		if (ret != TR_OK)
			goto fail;
		group->connector_timer_ready = 1;
	}

	*out = group;
	return TR_OK;

fail:
	if (group->connector_timer_ready)
		(void)tr_reactor_timer_unregister(group->connector_timer);
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
	struct tr_client_group_adopt_request request;
	struct tr_pipeline_route_preface route;
	uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE];
	size_t address_len;
	int fd = -1;
	int ret;

	if (!group || !ipv4_address || !id || port == 0U ||
	    id->group_id == 0U || id->epoch == 0U)
		return TR_ERR_INVALID;
	if (group->control.reactor)
		return TR_ERR_STATE;
	address_len = strlen(ipv4_address);
	if (address_len == 0U || address_len >= sizeof(group->address))
		return TR_ERR_INVALID;

	ret = tr_client_group_connect_fd(
		ipv4_address, port, group->config.connect_timeout_ms, &fd);
	if (ret != TR_OK)
		return ret;
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
	route.member_generation = tr_client_group_next_generation(group);
	ret = tr_pipeline_route_preface_encode(raw, &route);
	if (ret != TR_OK)
		goto fail;
	ret = tr_client_group_send_all_fd(
		fd, raw, sizeof(raw), group->config.connect_timeout_ms);
	if (ret != TR_OK)
		goto fail;

	group->control_route = route;
	memcpy(group->address, ipv4_address, address_len + 1U);
	group->port = port;

	memset(&request, 0, sizeof(request));
	request.group = group;
	request.fd = fd;
	ret = tr_reactor_call(
		group->config.owner,
		tr_client_group_adopt_control_on_owner, &request);
	if (request.fd_consumed)
		fd = -1;
	if (ret != TR_OK) {
		memset(&group->control_route, 0, sizeof(group->control_route));
		memset(group->address, 0, sizeof(group->address));
		group->port = 0U;
		goto fail;
	}
	return TR_OK;

fail:
	tr_socket_close(&fd);
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

void tr_client_group_destroy(struct tr_client_group *group)
{
	if (!group)
		return;

	if (group->control.reactor)
		(void)tr_client_group_close(group);
	if (group->connector_timer_ready)
		(void)tr_reactor_timer_unregister(group->connector_timer);
	free(group->data);
	if (group->control_pool_ready)
		tr_buffer_pool_destroy(&group->control_pool);
	free(group);
}
