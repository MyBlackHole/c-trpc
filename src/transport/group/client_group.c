#define _GNU_SOURCE
#include "client_group_internal.h"

#include <assert.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>

#include "../../io/connector_internal.h"
#include "../../group/pipeline_control_wire_internal.h"
#include "../../group/pipeline_route_internal.h"
#include "../../execution/reactor_internal.h"
#include "../../execution/deadline_internal.h"
#include "../../memory_budget.h"
#include "../../message/builder.h"
#include "../../io/peer_identity_internal.h"
#include "../../io/socket_internal.h"
#include "../../io/tls_openssl.h"
#include "../../execution/buffer.h"
#include "../../io/socket.h"
#include "message_budget.h"
#include "tr/status.h"
#include "../protocol/message_caps.h"
#include "../protocol/message_envelope.h"
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
	uint32_t peer_receive_algorithm_bits;
	uint32_t peer_max_encoded_message_bytes;
	enum tr_client_group_data_state state;
	unsigned message_caps_request_sent;
	unsigned message_caps_ready;
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
	struct tr_client_group_compression_job *compression_job;
	enum tr_client_group_transfer_state state;
};

struct tr_client_group {
	struct tr_client_group_config config;
	struct tr_tls_openssl_profile *pending_tls_profile;
	struct tr_conn_handle control;
	struct tr_pipeline_route_preface control_route;
	struct tr_peer_identity_snapshot *control_peer_identity;
	uint32_t control_generation;

	char address[TR_CLIENT_GROUP_ADDRESS_CAPACITY];
	uint16_t port;
	int control_connecting;
	int closing;
	int draining;

	/* 协议状态仍严格属于反应器所有者。这里仅向外部等待者发布排空生命周期代次，
	 * 不允许等待者借此锁直接读取或修改传输与数据状态。
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
	uint32_t compression_jobs_current;
	uint32_t compressed_wires_current;

	uint32_t connector_slot;
	struct tr_connector *connector;
};

struct tr_client_group_send_buffer {
	struct tr_buffer buffer;
	struct tr_client_group *group;
	uint32_t accounted_bytes;
	uint8_t storage[];
};

struct tr_client_group_compressed_wire {
	struct tr_buffer envelope;
	struct tr_buffer encoded;
	struct tr_message encoded_message;
	struct tr_client_group *group;
	struct tr_memory_budget *budget;
	uint64_t allocation_bytes;
	uint32_t accounted_bytes;
	uint32_t remaining_buffers;
	uint8_t envelope_bytes[TR_MESSAGE_ENVELOPE_WIRE_SIZE];
};

struct tr_client_group_compression_job {
	struct tr_client_group *group;
	struct tr_conn_handle connection;
	struct tr_task_handle task;
	uint32_t transfer_slot;
	uint32_t data_slot;
	uint32_t data_generation;
	uint32_t stream_id;
	uint32_t accounted_bytes;
	uint64_t message_id;
	int submitted;
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

static void
tr_client_group_release_control_identity(struct tr_client_group *group)
{
	if (!group)
		return;
	tr_peer_identity_snapshot_release(group->control_peer_identity);
	group->control_peer_identity = NULL;
}

static int
tr_client_group_identity_equal(const struct tr_peer_identity_view *left,
                               const struct tr_peer_identity_view *right)
{
	return left && right &&
	       left->authentication == TR_PEER_IDENTITY_TLS_CERTIFICATE &&
	       right->authentication == TR_PEER_IDENTITY_TLS_CERTIFICATE &&
	       left->uri_san && right->uri_san && left->uri_san_len != 0U &&
	       left->uri_san_len == right->uri_san_len &&
	       memcmp(left->uri_san, right->uri_san, left->uri_san_len) == 0;
}

static int tr_client_group_tls_authenticated(
        struct tr_conn_handle connection,
        const struct tr_peer_identity_view *peer_identity, void *arg)
{
	struct tr_client_group *group = (struct tr_client_group *)arg;
	struct tr_peer_identity_view control_identity;
	uint32_t i;

	if (!group || !peer_identity ||
	    peer_identity->authentication != TR_PEER_IDENTITY_TLS_CERTIFICATE ||
	    !peer_identity->uri_san || peer_identity->uri_san_len == 0U)
		return TR_ERR_INVALID;

	if (group->control_connecting ||
	    tr_client_group_conn_equal(group->control, connection)) {
		struct tr_peer_identity_snapshot *snapshot = NULL;
		int ret;

		if (group->control_peer_identity)
			return TR_ERR_STATE;
		ret = tr_reactor_peer_identity_snapshot_on_owner(connection,
		                                                 &snapshot);
		if (ret != TR_OK || !snapshot)
			return ret != TR_OK ? ret : TR_ERR_INVALID;
		ret = tr_peer_identity_snapshot_retain(snapshot);
		if (ret != TR_OK)
			return ret;
		group->control_peer_identity = snapshot;
		return TR_OK;
	}

	if (!group->control_peer_identity ||
	    tr_peer_identity_snapshot_view(group->control_peer_identity,
	                                   &control_identity) != TR_OK)
		return TR_ERR_STATE;
	if (!tr_client_group_identity_equal(peer_identity, &control_identity))
		return TR_ERR_INVALID;

	for (i = 0; i < group->data_capacity; ++i) {
		struct tr_client_group_data *data = &group->data[i];

		if (data->state != TR_CLIENT_GROUP_DATA_CONNECTING ||
		    !tr_client_group_conn_equal(data->connection, connection))
			continue;
		return TR_OK;
	}
	return TR_ERR_STALE;
}

static void
tr_client_group_publish_connected_on_owner(struct tr_client_group *group);

static int tr_client_group_send_message_caps_request_on_owner(
	struct tr_client_group *group, struct tr_client_group_data *data)
{
	struct tr_message_caps local_caps = {0U, 0U};
	struct tr_buffer *buffer = NULL;
	int ret;

	if (!group || !data)
		return TR_ERR_INVALID;
	if (group->config.send_compression_algorithms == 0U)
		return TR_OK;
	if (data->state != TR_CLIENT_GROUP_DATA_ACTIVE ||
	    !data->connection.reactor || data->message_caps_request_sent)
		return TR_ERR_STATE;

	ret = tr_buffer_acquire(&group->control_pool,
	                        TR_MESSAGE_CAPS_WIRE_SIZE, &buffer);
	if (ret != TR_OK)
		return ret;
	ret = tr_message_caps_encode(buffer->data, &local_caps);
	if (ret == TR_OK) {
		buffer->len = TR_MESSAGE_CAPS_WIRE_SIZE;
		ret = tr_reactor_send(data->connection, TR_FRAME_MESSAGE_CAPS,
		                      0U, 0U, 0U, buffer);
	}
	if (ret == TR_OK) {
		data->message_caps_request_sent = 1U;
		return TR_OK;
	}
	tr_buffer_release(buffer);
	return ret;
}

static int tr_client_group_tls_preface_sent(struct tr_conn_handle connection,
                                            void *arg)
{
	struct tr_client_group *group = (struct tr_client_group *)arg;
	uint32_t i;

	if (!group)
		return TR_ERR_INVALID;
	if (group->control_connecting &&
	    tr_client_group_conn_equal(group->control, connection)) {
		group->control_connecting = 0;
		tr_client_group_publish_connected_on_owner(group);
		return TR_OK;
	}
	for (i = 0; i < group->data_capacity; ++i) {
		struct tr_client_group_data *data = &group->data[i];

		if (data->state != TR_CLIENT_GROUP_DATA_CONNECTING ||
		    !tr_client_group_conn_equal(data->connection, connection))
			continue;
		data->state = TR_CLIENT_GROUP_DATA_ACTIVE;
		return tr_client_group_send_message_caps_request_on_owner(group,
		                                                         data);
	}
	return TR_ERR_STALE;
}

static int
tr_client_group_drain_complete_on_owner(const struct tr_client_group *group)
{
	uint32_t i;

	if (!group || !group->draining || group->control_connecting ||
	    group->transfer_count != 0U || group->send_bytes_inflight != 0U ||
	    group->compression_jobs_current != 0U ||
	    group->compressed_wires_current != 0U ||
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
	if (group->drain_waiters != 0U) {
		pthread_cond_broadcast(&group->drain_wait_cond);
		ret = TR_ERR_STATE;
	}
	pthread_mutex_unlock(&group->drain_wait_lock);
	return ret;
}

static int tr_client_group_connect_fd(const char *address, uint16_t port,
				      const struct tr_deadline *deadline, int *out_fd)
{
	int fd = -1;
	int ret;

	if (!address || !deadline || !out_fd || port == 0U)
		return TR_ERR_INVALID;
	*out_fd = -1;

	ret = tr_tcp_connect_ipv4(address, port, &fd);
	if (ret == TR_OK) {
		*out_fd = fd;
		return TR_OK;
	}
	if (ret != TR_IN_PROGRESS)
		return ret;

	ret = tr_deadline_poll_fd(fd, POLLOUT, deadline, 0);
	if (ret != TR_OK) {
		tr_socket_close(&fd);
		return ret;
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
				       const struct tr_deadline *deadline)
{
	int expired;
	int ret;

	if (fd < 0 || !deadline || (len != 0U && !data))
		return TR_ERR_INVALID;

	while (len != 0U) {
		ssize_t n;

		/* 成功分段发送或连续 EINTR 都不得越过整个连接时限。 */
		ret = tr_deadline_expired(deadline, &expired);
		if (ret != TR_OK)
			return ret;
		if (expired)
			return TR_ERR_TIMEOUT;

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

		ret = tr_deadline_poll_fd(fd, POLLOUT, deadline, 0);
		if (ret != TR_OK)
			return ret;
	}
	ret = tr_deadline_expired(deadline, &expired);
	if (ret != TR_OK)
		return ret;
	return expired ? TR_ERR_TIMEOUT : TR_OK;
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
static int tr_client_group_invalidate_data_transfers(
	struct tr_client_group *group, uint32_t data_slot,
	uint32_t data_generation);

static enum tr_frame_disposition tr_client_group_data_frame(
	struct tr_conn_handle connection, struct tr_frame *frame, void *arg)
{
	struct tr_client_group *group = (struct tr_client_group *)arg;
	struct tr_message_caps peer_caps;
	uint32_t i;
	int ret;

	if (group && frame && frame->header.type == TR_FRAME_MESSAGE_CAPS) {
		for (i = 0; i < group->data_capacity; ++i) {
			struct tr_client_group_data *data = &group->data[i];

			if ((data->state != TR_CLIENT_GROUP_DATA_ACTIVE &&
			     data->state != TR_CLIENT_GROUP_DATA_CONNECTING) ||
			    !tr_client_group_conn_equal(data->connection, connection))
				continue;
			if (!data->message_caps_request_sent ||
			    data->message_caps_ready || frame->header.flags != 0U ||
			    frame->header.stream_id != 0U ||
			    frame->header.message_id != 0U || !frame->payload ||
			    frame->payload->len != TR_MESSAGE_CAPS_WIRE_SIZE) {
				(void)tr_reactor_abort_on_owner(connection, TR_ERR_BAD_TYPE);
				return TR_FRAME_RELEASE;
			}
			ret = tr_message_caps_decode(frame->payload->data,
			                            frame->payload->len, &peer_caps);
			if (ret != TR_OK) {
				(void)tr_reactor_abort_on_owner(connection, ret);
				return TR_FRAME_RELEASE;
			}
			data->peer_receive_algorithm_bits =
			        peer_caps.receive_algorithm_bits;
			data->peer_max_encoded_message_bytes =
			        peer_caps.max_encoded_wire_bytes;
			data->message_caps_ready = 1U;
			return TR_FRAME_RELEASE;
		}
		(void)tr_reactor_abort_on_owner(connection, TR_ERR_STALE);
		return TR_FRAME_RELEASE;
	}
	/* 服务端到客户端的 DATA 交付尚未进入当前公开能力范围。
	 * 收到这类负载时明确终止 DATA 通道，避免静默丢弃业务数据。
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

		if ((group->data[i].state != TR_CLIENT_GROUP_DATA_ACTIVE &&
		     group->data[i].state != TR_CLIENT_GROUP_DATA_CONNECTING) ||
		    !tr_client_group_conn_equal(group->data[i].connection,
		                                connection))
			continue;
		if (!group->closing && group->control.reactor)
			cancel_ret = tr_client_group_send_cancel_on_owner(
				group, &group->data[i].route,
				group->data[i].offer_message_id);
		{
			int invalidate_ret =
				tr_client_group_invalidate_data_transfers(
					group, i,
					group->data[i].route.member_generation);

			if (cancel_ret == TR_OK)
				cancel_ret = invalidate_ret;
		}
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

static void tr_client_group_connector_complete(int status, int fd,
                                               uint64_t deadline_ns, void *arg)
{
	struct tr_client_group *group = (struct tr_client_group *)arg;
	struct tr_client_group_data *data;
	struct tr_conn_handle connection;
	struct tr_pipeline_route_preface route;
	uint8_t preface[TR_PIPELINE_ROUTE_PREFACE_SIZE];
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
		tr_client_group_publish_drain_progress_on_owner(group);
		return;
	}

	/* TR_OK 完成结果把已连接 fd 所有权转移给本回调。
	 * 接管成功后所有权再转移给反应器；其他失败路径必须在此关闭。
	 */
	ret = tr_pipeline_route_preface_encode(preface, &route);
	if (ret != TR_OK)
		goto fail;
	memset(&connection, 0, sizeof(connection));
	if (group->config.tls_profile)
		ret = tr_reactor_adopt_tls_fd_outbound_prefaced_on_owner(
		        group->config.owner, fd, group->config.tls_profile,
		        deadline_ns, preface, sizeof(preface),
		        tr_client_group_tls_authenticated, group,
		        tr_client_group_tls_preface_sent, group, &connection);
	else
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
	data->state = group->config.tls_profile
	                      ? TR_CLIENT_GROUP_DATA_CONNECTING
	                      : TR_CLIENT_GROUP_DATA_ACTIVE;
	if (!group->config.tls_profile) {
		ret = tr_client_group_send_message_caps_request_on_owner(group,
		                                                         data);
		if (ret != TR_OK) {
			(void)tr_reactor_abort_on_owner(connection, ret);
			return;
		}
	}
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
	tr_client_group_publish_drain_progress_on_owner(group);
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
	ret = tr_connector_start(group->connector, group->address, group->port,
	                         group->config.tls_profile ? NULL : preface,
	                         group->config.tls_profile ? 0U
	                                                   : sizeof(preface));
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

		/* connector 进入活跃状态后的失败会通过完成回调收敛。
		 * 如果回调在 start() 内同步执行，它可能已经启动了下一条提议；
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
		/* 服务端可撤销待定预留并递增代次复用槽位；旧套接字可能仍在关闭。
		 */
		if (existing->member_index == route->member_index &&
		    existing->member_generation == route->member_generation)
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
		if (transfer->state == TR_CLIENT_GROUP_TRANSFER_TOMBSTONE &&
		    !transfer->compression_job) {
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

static int tr_client_group_cancel_compression_job_on_owner(
	struct tr_client_group *group,
	struct tr_client_group_transfer *transfer)
{
	int ret;

	if (!group || !transfer || !transfer->compression_job)
		return TR_OK;
	if (!group->config.message_cancel)
		return TR_ERR_UNSUPPORTED;
	ret = group->config.message_cancel(
	        group->config.message_context,
	        transfer->compression_job->task);
	/* STATE means the transform's terminal result already won; STALE means
	 * its completion has retired the service handle. Both still converge via
	 * the accepted completion callback owned by this job.
	 */
	if (ret == TR_ERR_STATE || ret == TR_ERR_STALE)
		return TR_OK;
	return ret;
}

static int tr_client_group_clear_transfers(struct tr_client_group *group)
{
	uint32_t i;
	int result = TR_OK;

	if (!group || !group->transfers)
		return TR_OK;
	for (i = 0; i < group->transfer_capacity; ++i) {
		struct tr_client_group_transfer *transfer = &group->transfers[i];
		int ret;

		if (transfer->state == TR_CLIENT_GROUP_TRANSFER_USED) {
			transfer->state = TR_CLIENT_GROUP_TRANSFER_TOMBSTONE;
			if (group->transfer_count != 0U)
				group->transfer_count--;
		}
		ret = tr_client_group_cancel_compression_job_on_owner(
		        group, transfer);
		if (ret != TR_OK && result == TR_OK)
			result = ret;
		if (!transfer->compression_job)
			memset(transfer, 0, sizeof(*transfer));
	}
	tr_client_group_publish_drain_progress_on_owner(group);
	return result;
}

static int tr_client_group_invalidate_data_transfers(
	struct tr_client_group *group, uint32_t data_slot,
	uint32_t data_generation)
{
	uint32_t i;
	int result = TR_OK;

	if (!group || !group->transfers)
		return TR_OK;
	for (i = 0; i < group->transfer_capacity; ++i) {
		struct tr_client_group_transfer *transfer =
			&group->transfers[i];
		int ret;

		if (transfer->state != TR_CLIENT_GROUP_TRANSFER_USED ||
		    transfer->data_slot != data_slot ||
		    transfer->data_generation != data_generation)
			continue;
		transfer->state = TR_CLIENT_GROUP_TRANSFER_TOMBSTONE;
		if (group->transfer_count != 0U)
			group->transfer_count--;
		ret = tr_client_group_cancel_compression_job_on_owner(
		        group, transfer);
		if (ret != TR_OK && result == TR_OK)
			result = ret;
	}
	tr_client_group_publish_drain_progress_on_owner(group);
	return result;
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

static void tr_client_group_compressed_wire_release_one(
	struct tr_client_group_compressed_wire *wire)
{
	struct tr_client_group *group;

	if (!wire || wire->remaining_buffers == 0U)
		return;
	wire->remaining_buffers--;
	if (wire->remaining_buffers != 0U)
		return;
	group = wire->group;
	(void)tr_message_release(&wire->encoded_message);
	if (group) {
		if (group->send_bytes_inflight >= wire->accounted_bytes)
			group->send_bytes_inflight -= wire->accounted_bytes;
		else
			group->send_bytes_inflight = 0U;
		if (group->compressed_wires_current != 0U)
			group->compressed_wires_current--;
		tr_client_group_publish_drain_progress_on_owner(group);
	}
	(void)tr_memory_budget_release(wire->budget,
	                               wire->allocation_bytes);
	free(wire);
}

static void tr_client_group_compressed_envelope_release(
	struct tr_buffer *buffer)
{
	struct tr_client_group_compressed_wire *wire =
	        (struct tr_client_group_compressed_wire *)(
	                (uint8_t *)buffer -
	                offsetof(struct tr_client_group_compressed_wire,
	                         envelope));

	tr_client_group_compressed_wire_release_one(wire);
}

static void tr_client_group_compressed_encoded_release(
	struct tr_buffer *buffer)
{
	struct tr_client_group_compressed_wire *wire =
	        (struct tr_client_group_compressed_wire *)(
	                (uint8_t *)buffer -
	                offsetof(struct tr_client_group_compressed_wire,
	                         encoded));

	tr_client_group_compressed_wire_release_one(wire);
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
		/* begin_drain() 是单调的本地准入屏障。它返回后观察到的 READY
		 * 绝不能创建新本地工作，否则 wait_drained() 可能先报告静止，
		 * 之后又变成未排空。服务端独立拥有其亲和绑定，可以释放它；
		 * 最终 CONTROL 关闭也会隔断剩余远端软状态。
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

static int tr_client_group_stop_data_on_owner(struct tr_client_group *group)
{
	uint32_t i;
	int ret;

	/* Connector 事件源解绑是第一个可能失败的屏障。在其成功前不要清除
	 * 传输或 DATA 所有权；否则后续连接器回调可能指向已被声明为空闲的状态。
	 */
	if (group->connector) {
		ret = tr_connector_cancel(group->connector);
		if (ret != TR_OK)
			return ret;
	}
	group->connector_slot = TR_CLIENT_GROUP_NO_SLOT;
	ret = tr_client_group_clear_transfers(group);
	if (ret != TR_OK)
		return ret;

	for (i = 0; i < group->data_capacity; ++i) {
		struct tr_conn_handle connection = group->data[i].connection;

		if ((group->data[i].state == TR_CLIENT_GROUP_DATA_ACTIVE ||
		     group->data[i].state == TR_CLIENT_GROUP_DATA_CONNECTING) &&
		    connection.reactor) {
			ret = tr_reactor_close_on_owner(connection);
			if (ret != TR_OK && ret != TR_ERR_STALE)
				return ret;
		}
		memset(&group->data[i], 0, sizeof(group->data[i]));
		group->data[i].state = TR_CLIENT_GROUP_DATA_FREE;
	}
	tr_client_group_publish_drain_progress_on_owner(group);
	return TR_OK;
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
	if (tr_client_group_stop_data_on_owner(group) != TR_OK) {
		/* 保留陈旧 CONTROL
		 * 能力凭证与终止关闭状态，作为显式关闭或销毁的重试依据。
		 * 只要连接器回调源仍可能引用 Group，就不要释放所有者状态。
		 */
		return;
	}
	memset(&group->control, 0, sizeof(group->control));
	memset(&group->control_route, 0, sizeof(group->control_route));
	memset(group->address, 0, sizeof(group->address));
	group->port = 0U;
	group->control_connecting = 0;
	tr_client_group_release_control_identity(group);
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
	if (group->pending_tls_profile) {
		struct tr_tls_openssl_profile *old_profile =
		        group->config.tls_profile;
		group->config.tls_profile = group->pending_tls_profile;
		group->pending_tls_profile = NULL;
		tr_tls_openssl_profile_release(old_profile);
	}

	group->control_connecting = 1;
	request->generation = tr_client_group_next_generation(group);
	return TR_OK;
}

static int tr_client_group_cancel_prepare_connect_on_owner(void *arg)
{
	struct tr_client_group_prepare_connect_request *request =
		(struct tr_client_group_prepare_connect_request *)arg;

	request->group->control_connecting = 0;
	tr_client_group_release_control_identity(request->group);
	return TR_OK;
}

struct tr_client_group_adopt_request {
	struct tr_client_group *group;
	int fd;
	int fd_consumed;
	int executed;
	uint64_t handshake_deadline_ns;
	struct tr_conn_handle connection;
	struct tr_pipeline_route_preface route;
	uint8_t preface[TR_PIPELINE_ROUTE_PREFACE_SIZE];
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

	if (group->config.tls_profile)
		ret = tr_reactor_adopt_tls_fd_outbound_prefaced_on_owner(
		        group->config.owner, request->fd,
		        group->config.tls_profile,
		        request->handshake_deadline_ns, request->preface,
		        sizeof(request->preface),
		        tr_client_group_tls_authenticated, group,
		        tr_client_group_tls_preface_sent, group,
		        &request->connection);
	else
		ret = tr_reactor_adopt_fd(group->config.owner, request->fd,
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

	/* 处理函数发布与 Group 状态发布处于一个所有者处理轮次。
	 * 任何回调都不能观察到部分初始化的控制路由。
	 */
	group->control = request->connection;
	group->control_route = request->route;
	memcpy(group->address, request->address, sizeof(group->address));
	group->port = request->port;
	group->draining = 0;
	if (!group->config.tls_profile) {
		group->control_connecting = 0;
		tr_client_group_publish_connected_on_owner(group);
	}
	return TR_OK;

fail:
	group->control_connecting = 0;
	tr_client_group_release_control_identity(group);
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
	ret = tr_client_group_stop_data_on_owner(group);
	if (ret != TR_OK)
		return ret;

	control = group->control;
	ret = tr_reactor_close_on_owner(control);
	if (ret == TR_ERR_STALE)
		ret = TR_OK;
	if (ret != TR_OK) {
		group->closing = 0;
		return ret;
	}
	if (group->control.reactor &&
	    tr_client_group_conn_equal(group->control, control)) {
		memset(&group->control, 0, sizeof(group->control));
		memset(&group->control_route, 0,
		       sizeof(group->control_route));
		memset(group->address, 0, sizeof(group->address));
		group->port = 0U;
	}
	tr_client_group_release_control_identity(group);
	group->closing = 0;
	return ret;
}

int tr_client_group_create(const struct tr_client_group_config *config,
			   struct tr_client_group **out)
{
	struct tr_client_group *group;
	uint32_t control_buffers;
	int drain_wait_lock_ready = 0;
	int drain_wait_cond_ready = 0;
	int ret;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	if (!config || !config->owner || config->connect_timeout_ms == 0U ||
	    (config->tcp_nodelay != 0 && config->tcp_nodelay != 1) ||
	    (config->send_compression_algorithms &
	     ~TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD) ||
	    (!config->send_compression_algorithms &&
	     config->max_encoded_message_bytes != 0U) ||
	    (config->send_compression_algorithms &&
	     (config->max_data_connections == 0U ||
	      config->max_encoded_message_bytes < 17U ||
	      !config->message_submit || !config->message_cancel ||
	      !config->message_context || !config->message_budget)) ||
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
	if (group->config.tls_profile) {
		ret = tr_tls_openssl_profile_retain(group->config.tls_profile);
		if (ret != TR_OK) {
			free(group);
			return ret;
		}
	}
	group->data_capacity = config->max_data_connections;
	group->transfer_limit =
		config->max_data_connections != 0U ? config->max_transfers : 0U;
	group->transfer_capacity =
		group->transfer_limit != 0U ? group->transfer_limit * 2U : 0U;
	group->send_bytes_limit =
		(uint64_t)group->data_capacity *
		(uint64_t)config->max_message_bytes;
	group->connector_slot = TR_CLIENT_GROUP_NO_SLOT;
	if (pthread_mutex_init(&group->drain_wait_lock, NULL) != 0) {
		ret = TR_ERR_SYS;
		goto fail;
	}
	drain_wait_lock_ready = 1;
	{
		pthread_condattr_t attr;

		if (pthread_condattr_init(&attr) != 0) {
			ret = TR_ERR_SYS;
			goto fail;
		}
		if (pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) != 0) {
			pthread_condattr_destroy(&attr);
			ret = TR_ERR_SYS;
			goto fail;
		}
		if (pthread_cond_init(&group->drain_wait_cond, &attr) != 0) {
			pthread_condattr_destroy(&attr);
			ret = TR_ERR_SYS;
			goto fail;
		}
		pthread_condattr_destroy(&attr);
		drain_wait_cond_ready = 1;
	}

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
	/* connector_create() 是构造函数的最后一个可失败步骤；一旦成功，
	 * 函数立即提交 Group 所有权，因此失败路径不可能拥有 connector。
	 * 这里的回滚只处理此前建立的本地软状态。
	 */
	if (group->control_pool_ready) {
		int pool_ret = tr_buffer_pool_destroy(&group->control_pool);

		if (pool_ret != TR_OK) {
			/* 池中尚未归还的所有权使终止回滚无法完成。
			 * 保留整个 Group。
			 * 调用方拿到部分构造对象的所有权后可继续销毁。
			 */
			*out = group;
			return pool_ret;
		}
		group->control_pool_ready = 0;
	}
	free(group->transfers);
	free(group->data);
	tr_tls_openssl_profile_release(group->config.tls_profile);
	tr_tls_openssl_profile_release(group->pending_tls_profile);
	if (drain_wait_cond_ready)
		pthread_cond_destroy(&group->drain_wait_cond);
	if (drain_wait_lock_ready)
		pthread_mutex_destroy(&group->drain_wait_lock);
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
	struct tr_deadline connect_deadline;
	uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE];
	uint64_t handshake_deadline_ns;
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

	/* 只在所有者准备完成后开始计时；TCP 和前导数据共用期限。 */
	ret = tr_deadline_init_ms(&connect_deadline,
				  group->config.connect_timeout_ms);
	if (ret != TR_OK)
		goto fail;
	ret = tr_client_group_connect_fd(
		ipv4_address, port, &connect_deadline, &fd);
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
	if (!group->config.tls_profile) {
		ret = tr_client_group_send_all_fd(fd, raw, sizeof(raw),
		                                  &connect_deadline);
		if (ret != TR_OK)
			goto fail;
	}
	handshake_deadline_ns = tr_deadline_to_ns(&connect_deadline);
	if (handshake_deadline_ns == 0U) {
		ret = TR_ERR_SYS;
		goto fail;
	}

	memset(&request, 0, sizeof(request));
	request.group = group;
	request.fd = fd;
	request.handshake_deadline_ns = handshake_deadline_ns;
	request.route = route;
	memcpy(request.preface, raw, sizeof(request.preface));
	memcpy(request.address, ipv4_address, address_len + 1U);
	request.port = port;
	ret = tr_reactor_call(
		group->config.owner,
		tr_client_group_adopt_control_on_owner, &request);
	if (request.executed)
		prepared = 0; /* 所有者接管路径消耗或清除预留 */
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
	int final = TR_OK;

	if (!group->control.reactor)
		return TR_ERR_STATE;
	if (!group->draining) {
		group->draining = 1;
		tr_client_group_publish_drain_start_on_owner(group);
	}

	if (group->connector_slot != TR_CLIENT_GROUP_NO_SLOT) {
		uint32_t slot = group->connector_slot;
		struct tr_pipeline_route_preface route;
		uint64_t message_id;
		int ret;

		if (slot >= group->data_capacity) {
			final = TR_ERR_STATE;
			goto out;
		}
		route = group->data[slot].route;
		message_id = group->data[slot].offer_message_id;
		ret = tr_connector_cancel(group->connector);
		if (ret != TR_OK) {
			final = ret;
			goto out;
		}
		group->connector_slot = TR_CLIENT_GROUP_NO_SLOT;
		memset(&group->data[slot], 0, sizeof(group->data[slot]));
		group->data[slot].state = TR_CLIENT_GROUP_DATA_FREE;
		ret = tr_client_group_send_cancel_on_owner(
			group, &route, message_id);
		if (ret != TR_OK) {
			(void)tr_reactor_abort_on_owner(
				group->control,
				ret < 0 ? ret : TR_ERR_STATE);
			final = ret;
			goto out;
		}
	}

	for (i = 0; i < group->data_capacity; ++i) {
		struct tr_client_group_data *data = &group->data[i];
		struct tr_pipeline_route_preface route = data->route;
		uint64_t message_id = data->offer_message_id;
		int ret;

		if (data->state == TR_CLIENT_GROUP_DATA_QUEUED) {
			ret = tr_client_group_send_cancel_on_owner(
			        group, &route, message_id);
		} else if (data->state == TR_CLIENT_GROUP_DATA_CONNECTING &&
		           data->connection.reactor) {
			ret = tr_reactor_close_on_owner(data->connection);
			if (ret == TR_ERR_STALE) {
				ret = tr_client_group_send_cancel_on_owner(
				        group, &route, message_id);
			} else if (ret == TR_OK) {
				/* 成功关闭会同步触发 DATA event，由其回调撤销
				 * route。 */
				continue;
			}
		} else {
			continue;
		}
		if (ret != TR_OK) {
			if (group->control.reactor)
				(void)tr_reactor_abort_on_owner(
				        group->control,
				        ret < 0 ? ret : TR_ERR_STATE);
			final = ret;
			goto out;
		}
		memset(data, 0, sizeof(*data));
		data->state = TR_CLIENT_GROUP_DATA_FREE;
	}

out:
	tr_client_group_publish_drain_progress_on_owner(group);
	return final;
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
int tr_client_group_wait_drained(
	struct tr_client_group *group, uint32_t timeout_ms)
{
	struct tr_deadline deadline;
	uint64_t generation;
	int timed = timeout_ms != 0U;
	int result = TR_OK;

	if (!group)
		return TR_ERR_INVALID;
	if (tr_reactor_in_owner_context())
		return TR_ERR_STATE;

	/* 0ms 仍表示无限等待；有限值统一使用 time_t 安全的绝对截止时间。 */
	if (timed) {
		int ret = tr_deadline_init_ms(&deadline, timeout_ms);

		if (ret != TR_OK)
			return ret;
	}

	pthread_mutex_lock(&group->drain_wait_lock);
	if (group->drain_wait_closed) {
		pthread_mutex_unlock(&group->drain_wait_lock);
		return TR_ERR_CLOSED;
	}
	if (!group->drain_wait_active || group->drain_generation == 0U) {
		pthread_mutex_unlock(&group->drain_wait_lock);
		return TR_ERR_STATE;
	}

	generation = group->drain_generation;
	if (group->drained_generation == generation) {
		pthread_mutex_unlock(&group->drain_wait_lock);
		return TR_OK;
	}
	if (group->drain_waiters == UINT32_MAX) {
		pthread_mutex_unlock(&group->drain_wait_lock);
		return TR_ERR_STATE;
	}
	group->drain_waiters++;

	while (group->drained_generation != generation) {
		int ret;

		if (group->drain_wait_closed) {
			result = TR_ERR_CLOSED;
			break;
		}
		if (!group->drain_wait_active ||
		    group->drain_generation != generation) {
			result = TR_ERR_STALE;
			break;
		}

		if (timed)
			ret = pthread_cond_timedwait(
				&group->drain_wait_cond,
				&group->drain_wait_lock, &deadline.absolute);
		else
			ret = pthread_cond_wait(
				&group->drain_wait_cond,
				&group->drain_wait_lock);
		if (ret == 0)
			continue;
		if (timed && ret == ETIMEDOUT) {
			result = TR_ERR_TIMEOUT;
			break;
		}
		result = TR_ERR_SYS;
		break;
	}

	assert(group->drain_waiters != 0U);
	group->drain_waiters--;
	pthread_mutex_unlock(&group->drain_wait_lock);
	return result;
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
	out->control_connected =
	        group->control.reactor && !group->control_connecting ? 1U : 0U;
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
	struct tr_client_group_transfer *transfer;
	uint32_t slot = UINT32_MAX;
	int ret;

	if (!tr_client_group_transfer_find(
		    group, request->stream_id, &slot, NULL))
		return TR_ERR_STALE;
	transfer = &group->transfers[slot];
	transfer->state = TR_CLIENT_GROUP_TRANSFER_TOMBSTONE;
	if (group->transfer_count != 0U)
		group->transfer_count--;
	ret = tr_client_group_cancel_compression_job_on_owner(group, transfer);
	tr_client_group_publish_drain_progress_on_owner(group);
	return ret;
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
	enum tr_connection_group_compression compression;
};

static struct tr_client_group_transfer *
tr_client_group_compression_job_transfer(
	struct tr_client_group *group,
	struct tr_client_group_compression_job *job)
{
	uint32_t i;

	for (i = 0; i < group->transfer_capacity; ++i)
		if (group->transfers[i].compression_job == job)
			return &group->transfers[i];
	return NULL;
}

static int tr_client_group_transform_error(
	enum tr_message_transform_outcome outcome)
{
	switch (outcome) {
	case TR_MESSAGE_TRANSFORM_CANCELLED:
		return TR_ERR_CLOSED;
	case TR_MESSAGE_TRANSFORM_LIMIT:
		return TR_ERR_BAD_LENGTH;
	case TR_MESSAGE_TRANSFORM_NOMEM:
		return TR_ERR_NOMEM;
	case TR_MESSAGE_TRANSFORM_BAD_DATA:
		return TR_ERR_BAD_TYPE;
	case TR_MESSAGE_TRANSFORM_OK:
		return TR_OK;
	case TR_MESSAGE_TRANSFORM_INTERNAL:
	default:
		return TR_ERR_STATE;
	}
}

static void tr_client_group_compression_done(
	void *arg, struct tr_task_handle handle,
	struct tr_message_service_result result, struct tr_message *message)
{
	struct tr_client_group_compression_job *job = arg;
	struct tr_client_group *group;
	struct tr_client_group_transfer *transfer;
	struct tr_client_group_data *data = NULL;
	struct tr_client_group_compressed_wire *wire = NULL;
	struct tr_message_view encoded_view;
	struct tr_message_envelope envelope;
	struct tr_buffer *payloads[2];
	uint32_t wire_limit = 0U;
	uint64_t wire_allocation = sizeof(*wire);
	int current = 0;
	int abort_status = TR_OK;
	int ret;

	if (!job || !job->group)
		return;
	group = job->group;
	transfer = tr_client_group_compression_job_transfer(group, job);
	if (result.delivery == TR_MESSAGE_SERVICE_ON_OWNER && transfer &&
	    transfer->state == TR_CLIENT_GROUP_TRANSFER_USED &&
	    transfer->data_slot == job->data_slot &&
	    transfer->data_generation == job->data_generation &&
	    transfer->stream_id == job->stream_id &&
	    job->data_slot < group->data_capacity) {
		data = &group->data[job->data_slot];
		current = !group->closing &&
		          data->state == TR_CLIENT_GROUP_DATA_ACTIVE &&
		          data->route.member_generation == job->data_generation &&
		          data->message_caps_ready &&
		          tr_client_group_conn_equal(data->connection,
		                                     job->connection) &&
		          transfer->compression_job == job;
	}
	if (handle.instance != job->task.instance ||
	    handle.generation != job->task.generation)
		current = 0;

	if (current && result.outcome == TR_MESSAGE_TRANSFORM_OK && message) {
		struct tr_message_limits limits;
		uint32_t local_limit =
		        group->config.max_encoded_message_bytes;

		wire_limit = data->peer_max_encoded_message_bytes;
		if (wire_limit > local_limit)
			wire_limit = local_limit;
		if (wire_limit < TR_MESSAGE_ENVELOPE_WIRE_SIZE + 1U ||
		    !(data->peer_receive_algorithm_bits &
		      TR_MESSAGE_ALGORITHM_ZSTD)) {
			abort_status = TR_ERR_UNSUPPORTED;
		} else {
			ret = tr_message_view(message, &encoded_view);
			if (ret != TR_OK || encoded_view.len == 0U ||
			    encoded_view.len > UINT32_MAX ||
			    encoded_view.len >
			            wire_limit - TR_MESSAGE_ENVELOPE_WIRE_SIZE) {
				abort_status = ret == TR_OK ? TR_ERR_BAD_LENGTH : ret;
			} else {
				ret = tr_memory_budget_reserve(
				        group->config.message_budget,
				        wire_allocation);
				if (ret == TR_OK) {
					wire = calloc(1, sizeof(*wire));
					if (!wire) {
						(void)tr_memory_budget_release(
						        group->config.message_budget,
						        wire_allocation);
						ret = TR_ERR_NOMEM;
					}
				}
				if (ret != TR_OK) {
					abort_status = ret;
				} else {
					wire->group = group;
					wire->budget =
					        group->config.message_budget;
					wire->allocation_bytes = wire_allocation;
					wire->accounted_bytes =
					        job->accounted_bytes;
					limits.max_decoded_bytes =
					        local_limit;
					limits.max_encoded_bytes =
					        wire_limit;
					ret = tr_message_none_move(
					        message, &limits,
					        &wire->encoded_message);
					if (ret == TR_OK)
						ret = tr_message_view(
						        &wire->encoded_message,
						        &encoded_view);
					if (ret == TR_OK) {
						envelope = (struct tr_message_envelope){
						        .codec_id =
						                TR_MESSAGE_CODEC_ZSTD,
						        .encoded_body_bytes =
						                (uint32_t)encoded_view.len,
						        .decoded_message_bytes =
						                job->accounted_bytes};
						ret = tr_message_envelope_encode(
						        wire->envelope_bytes,
						        &envelope);
					}
					if (ret != TR_OK) {
						(void)tr_message_release(
						        &wire->encoded_message);
						(void)tr_memory_budget_release(
						        wire->budget,
						        wire->allocation_bytes);
						free(wire);
						wire = NULL;
						abort_status = ret;
					} else {
						wire->remaining_buffers = 2U;
						wire->envelope.data =
						        wire->envelope_bytes;
						wire->envelope.capacity =
						        TR_MESSAGE_ENVELOPE_WIRE_SIZE;
						wire->envelope.len =
						        TR_MESSAGE_ENVELOPE_WIRE_SIZE;
						wire->envelope.release_cb =
						        tr_client_group_compressed_envelope_release;
						wire->encoded.data =
						        (uint8_t *)encoded_view.data;
						wire->encoded.capacity =
						        (uint32_t)encoded_view.len;
						wire->encoded.len =
						        (uint32_t)encoded_view.len;
						wire->encoded.release_cb =
						        tr_client_group_compressed_encoded_release;
						payloads[0] = &wire->envelope;
						payloads[1] = &wire->encoded;
						group->compressed_wires_current++;
						job->accounted_bytes = 0U;
						transfer->compression_job = NULL;
						ret = tr_reactor_sendv_limited(
						        job->connection,
						        TR_FRAME_DATA,
						        TR_FRAME_F_COMPRESSED,
						        job->stream_id,
						        job->message_id, payloads,
						        2U,
						        group->config
					                .max_frame_payload_bytes);
						if (ret != TR_OK) {
							tr_buffer_release(
							        &wire->envelope);
							tr_buffer_release(
							        &wire->encoded);
							wire = NULL;
							abort_status = ret == TR_AGAIN
							                       ? TR_ERR_STATE
							                       : ret;
						}
					}
				}
			}
		}
	} else if (current) {
		abort_status = result.outcome == TR_MESSAGE_TRANSFORM_OK
		                       ? TR_ERR_STATE
		                       : tr_client_group_transform_error(
		                                 result.outcome);
	}

	if (transfer && transfer->compression_job == job) {
		transfer->compression_job = NULL;
		if (transfer->state == TR_CLIENT_GROUP_TRANSFER_TOMBSTONE)
			memset(transfer, 0, sizeof(*transfer));
	}
	if (abort_status != TR_OK && current)
		(void)tr_reactor_abort_on_owner(
		        job->connection,
		        abort_status < 0 ? abort_status : TR_ERR_STATE);
	if (job->accounted_bytes != 0U) {
		if (group->send_bytes_inflight >= job->accounted_bytes)
			group->send_bytes_inflight -= job->accounted_bytes;
		else
			group->send_bytes_inflight = 0U;
	}
	if (group->compression_jobs_current != 0U)
		group->compression_jobs_current--;
	(void)tr_memory_budget_release(group->config.message_budget,
	                               sizeof(*job));
	free(job);
	tr_client_group_publish_drain_progress_on_owner(group);
}

static int tr_client_group_send_compressed_on_owner(
	struct tr_client_group *group, uint32_t stream_id,
	uint64_t message_id, const struct tr_transport_bytes *bytes)
{
	struct tr_client_group_transfer *transfer;
	struct tr_client_group_data *data;
	struct tr_message_builder *builder = NULL;
	struct tr_message input = {0};
	struct tr_message_transform_config transform_config;
	struct tr_client_group_compression_job *job = NULL;
	struct tr_conn_handle connection;
	uint32_t transfer_slot = UINT32_MAX;
	uint32_t encoded_limit;
	uint32_t data_slot;
	int ret;

	if (!(group->config.send_compression_algorithms &
	      TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD))
		return TR_ERR_UNSUPPORTED;
	if (!group->config.message_submit || !group->config.message_cancel ||
	    !group->config.message_context || !group->config.message_budget)
		return TR_ERR_UNSUPPORTED;
	if (group->closing || group->draining)
		return TR_ERR_CLOSED;
	if (!tr_client_group_transfer_find(group, stream_id, &transfer_slot,
	                                   NULL))
		return TR_ERR_STALE;
	transfer = &group->transfers[transfer_slot];
	ret = tr_client_group_transfer_connection_on_owner(
	        group, stream_id, &connection);
	if (ret != TR_OK)
		return ret;
	if (transfer->data_slot >= group->data_capacity)
		return TR_ERR_STATE;
	data_slot = transfer->data_slot;
	data = &group->data[data_slot];
	if (!data->message_caps_ready)
		return TR_AGAIN;
	if (!(data->peer_receive_algorithm_bits &
	      TR_MESSAGE_ALGORITHM_ZSTD))
		return TR_ERR_UNSUPPORTED;
	if (transfer->compression_job)
		return TR_AGAIN;
	if (group->send_bytes_inflight > group->send_bytes_limit ||
	    (uint64_t)bytes->len >
	            group->send_bytes_limit - group->send_bytes_inflight)
		return TR_AGAIN;
	encoded_limit = group->config.max_encoded_message_bytes;
	if (data->peer_max_encoded_message_bytes < encoded_limit)
		encoded_limit = data->peer_max_encoded_message_bytes;
	if (encoded_limit < TR_MESSAGE_ENVELOPE_WIRE_SIZE + 1U)
		return TR_ERR_UNSUPPORTED;

	ret = tr_memory_budget_reserve(group->config.message_budget,
	                               sizeof(*job));
	if (ret != TR_OK)
		return ret;
	job = calloc(1, sizeof(*job));
	if (!job) {
		(void)tr_memory_budget_release(group->config.message_budget,
		                               sizeof(*job));
		return TR_ERR_NOMEM;
	}
	ret = tr_message_builder_create(group->config.message_budget,
	                                bytes->len, &builder);
	if (ret == TR_OK && bytes->len != 0U)
		ret = tr_message_builder_append(builder, bytes->data, bytes->len);
	if (ret == TR_OK)
		ret = tr_message_builder_publish(&builder, &input);
	if (ret != TR_OK) {
		if (builder)
			(void)tr_message_builder_cancel(&builder);
		(void)tr_message_release(&input);
		(void)tr_memory_budget_release(group->config.message_budget,
		                               sizeof(*job));
		free(job);
		return ret;
	}

	job->group = group;
	job->connection = connection;
	job->transfer_slot = transfer_slot;
	job->data_slot = data_slot;
	job->data_generation = data->route.member_generation;
	job->stream_id = stream_id;
	job->message_id = message_id;
	job->accounted_bytes = bytes->len;
	memset(&transform_config, 0, sizeof(transform_config));
	transform_config.kind = TR_MESSAGE_ZSTD_ENCODE;
	transform_config.limits.max_decoded_bytes =
	        group->config.max_message_bytes;
	transform_config.limits.max_encoded_bytes =
	        encoded_limit - TR_MESSAGE_ENVELOPE_WIRE_SIZE;
	transform_config.max_workspace_bytes =
	        TR_GROUP_MESSAGE_WORKSPACE_BYTES;
	transform_config.max_window_log = TR_GROUP_MESSAGE_WINDOW_LOG;
	transform_config.compression_level = 3;
	transfer->compression_job = job;
	ret = group->config.message_submit(
	        group->config.message_context, &input, &transform_config,
	        tr_client_group_compression_done, job, &job->task);
	if (ret != TR_OK) {
		transfer->compression_job = NULL;
		(void)tr_message_release(&input);
		(void)tr_memory_budget_release(group->config.message_budget,
		                               sizeof(*job));
		free(job);
		return ret;
	}
	job->submitted = 1;
	group->compression_jobs_current++;
	group->send_bytes_inflight += bytes->len;
	return TR_OK;
}

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
	if (request->compression == TR_CONNECTION_GROUP_COMPRESSION_ZSTD) {
		return tr_client_group_send_compressed_on_owner(
		        group, request->stream_id, request->message_id, bytes);
	}

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
	return tr_client_group_send_with_compression(
	        group, stream_id, message_id, bytes,
	        TR_CONNECTION_GROUP_COMPRESSION_NONE);
}

int tr_client_group_send_with_compression(
	struct tr_client_group *group, uint32_t stream_id,
	uint64_t message_id, const struct tr_transport_bytes *bytes,
	enum tr_connection_group_compression compression)
{
	struct tr_client_group_send_request request;

	if (!group || stream_id == 0U || !bytes ||
	    (bytes->len != 0U && !bytes->data) ||
	    (compression != TR_CONNECTION_GROUP_COMPRESSION_NONE &&
	     compression != TR_CONNECTION_GROUP_COMPRESSION_ZSTD))
		return TR_ERR_INVALID;
	request.group = group;
	request.stream_id = stream_id;
	request.message_id = message_id;
	request.bytes = bytes;
	request.compression = compression;
	return tr_reactor_call(
		group->config.owner, tr_client_group_send_on_owner, &request);
}

static int tr_client_group_detach_on_owner(void *arg)
{
	struct tr_client_group *group = (struct tr_client_group *)arg;
	struct tr_conn_handle control;
	int ret = TR_OK;

	/* 销毁的所有者屏障必须无条件执行，不能通过外部线程读取
	 * group->control.reactor 来猜测回调是否已经退出。
	 */
	group->closing = 1;
	group->draining = 1;
	group->control_connecting = 0;
	ret = tr_client_group_stop_data_on_owner(group);
	if (ret != TR_OK)
		return ret;

	control = group->control;
	if (control.reactor) {
		ret = tr_reactor_close_on_owner(control);
		if (ret == TR_ERR_STALE)
			ret = TR_OK;
		if (ret != TR_OK)
			return ret;
	}

	memset(&group->control, 0, sizeof(group->control));
	memset(&group->control_route, 0, sizeof(group->control_route));
	memset(group->address, 0, sizeof(group->address));
	group->port = 0U;
	tr_client_group_release_control_identity(group);

	/* 每个 Group 拥有的 DATA 负载在其释放回调中携带原始 group 指针。
	 * 在此所有者屏障返回前，上方全部 DATA 连接关闭时
	 * 必须同步释放其发送队列。只要此类回调仍可能存在，
	 * 就绝不能释放 Group 存储。
	 */
	if ((group->send_bytes_inflight != 0U ||
	     group->compression_jobs_current != 0U ||
	     group->compressed_wires_current != 0U) &&
	    ret == TR_OK)
		ret = TR_ERR_STATE;
	return ret;
}

int tr_client_group_destroy(struct tr_client_group *group)
{
	int ret;

	if (!group)
		return TR_OK;

	ret = tr_client_group_close_wait_admission(group);
	if (ret != TR_OK)
		return ret;

	/* 客户端门面保证销毁发生在运行时停止之前。使用同步所有者屏障，
	 * 确保释放存储前，所有可能引用 group 的回调都已返回。
	 * 若所有权无法收敛，则保守拒绝。
	 */
	ret = tr_reactor_call(
		group->config.owner, tr_client_group_detach_on_owner, group);
	if (ret != TR_OK)
		return ret;

	if (group->connector) {
		ret = tr_connector_destroy(group->connector);
		if (ret != TR_OK)
			return ret;
		group->connector = NULL;
	}

	if (group->control_pool_ready) {
		ret = tr_buffer_pool_destroy(&group->control_pool);
		if (ret != TR_OK)
			return ret;
		group->control_pool_ready = 0;
	}

	free(group->transfers);
	free(group->data);
	tr_tls_openssl_profile_release(group->config.tls_profile);
	tr_tls_openssl_profile_release(group->pending_tls_profile);
	pthread_cond_destroy(&group->drain_wait_cond);
	pthread_mutex_destroy(&group->drain_wait_lock);
	free(group);
	return TR_OK;
}

struct tr_client_group_tls_profile_request {
	struct tr_client_group *group;
	struct tr_tls_openssl_profile *profile;
};

static int tr_client_group_schedule_tls_profile_on_owner(void *arg)
{
	struct tr_client_group_tls_profile_request *request = arg;
	struct tr_client_group *group;
	int ret;

	if (!request || !request->group || !request->profile)
		return TR_ERR_INVALID;
	group = request->group;
	if (!group->config.tls_profile || group->closing || group->draining)
		return TR_ERR_STATE;
	ret = tr_tls_openssl_profile_retain(request->profile);
	if (ret != TR_OK)
		return ret;
	tr_tls_openssl_profile_release(group->pending_tls_profile);
	group->pending_tls_profile = request->profile;
	return TR_OK;
}

int tr_client_group_schedule_tls_profile(struct tr_client_group *group,
                                         struct tr_tls_openssl_profile *profile)
{
	struct tr_client_group_tls_profile_request request;

	if (!group || !profile)
		return TR_ERR_INVALID;
	request.group = group;
	request.profile = profile;
	return tr_reactor_call_or_stopped(
	        group->config.owner,
	        tr_client_group_schedule_tls_profile_on_owner, &request);
}
