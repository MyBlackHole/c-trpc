#define _GNU_SOURCE
#include "pipeline_listener_internal.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <time.h>
#include <unistd.h>

#include "../../group/pipeline_control_internal.h"
#include "pipeline_control_transport_internal.h"
#include "pipeline_ingress_internal.h"
#include "../../group/pipeline_registry_internal.h"
#include "../../execution/reactor_internal.h"
#include "../../execution/deadline_internal.h"
#include "../../execution/buffer.h"
#include "../../memory_budget.h"
#include "../../message/builder.h"
#include "message_budget.h"
#include "../../io/peer_identity_internal.h"
#include "../../io/tls_openssl.h"
#include "../../io/socket.h"
#include "tr/status.h"
#include "../protocol/message_caps.h"
#include "../protocol/message_envelope.h"

#define TR_PIPELINE_LISTENER_ACCEPT_BATCH 16U

enum tr_pipeline_listener_connection_role {
	TR_PIPELINE_LISTENER_CONN_PENDING = 0,
	TR_PIPELINE_LISTENER_CONN_CONTROL = 1,
	TR_PIPELINE_LISTENER_CONN_DATA = 2
};

struct tr_pipeline_listener_connection {
	struct tr_pipeline_listener *listener;
	struct tr_conn_handle handle;
	struct tr_pipeline_route_preface route;
	struct tr_deadline handshake_deadline;
	struct tr_tls_provider_operation authorization_operation;
	uint32_t generation;
	enum tr_pipeline_listener_connection_role role;
	int authorization_pending;
	unsigned message_caps_received;
	int used;
};

struct tr_pipeline_listener_message_job {
	struct tr_pipeline_listener *listener;
	struct tr_conn_handle connection;
	struct tr_pipeline_route_preface route;
	struct tr_task_handle task;
	uint32_t connection_index;
	uint32_t connection_generation;
	uint32_t stream_id;
	uint32_t decoded_message_bytes;
	uint64_t message_id;
	int used;
	int submitted;
	int cancelled;
};

struct tr_pipeline_listener_rx_slot {
	struct tr_message_builder *builder;
	struct tr_pipeline_route_preface route;
	uint32_t connection_index;
	uint32_t connection_generation;
	uint32_t stream_id;
	uint32_t encoded_body_bytes;
	uint32_t received_body_bytes;
	uint32_t decoded_message_bytes;
	uint32_t job_index;
	uint64_t message_id;
	int used;
};

struct tr_pipeline_listener_decoded_buffer {
	struct tr_buffer buffer;
	struct tr_message message;
	struct tr_pipeline_listener *listener;
	struct tr_memory_budget *budget;
};

_Static_assert(sizeof(struct tr_pipeline_listener_rx_slot) <=
                       TR_GROUP_MESSAGE_RX_SLOT_OVERHEAD_BYTES,
               "Pipeline 压缩接收槽超出固定预算");
_Static_assert(sizeof(struct tr_pipeline_listener_message_job) <=
                       TR_GROUP_MESSAGE_JOB_SLOT_OVERHEAD_BYTES,
               "Pipeline 压缩任务槽超出固定预算");

struct tr_pipeline_listener_session {
	struct tr_pipeline_listener *listener;
	struct tr_pipeline_control_transport *transport;
	struct tr_tls_openssl_profile *tls_profile;
	struct tr_peer_identity_snapshot *peer_identity;
	uint64_t pipeline_id;
	uint64_t epoch;
	uint64_t tls_generation;
	uint32_t connection_index;
	uint32_t connection_generation;
	uint32_t generation;
	int used;
	int closing;
	int authorization_reserved;
};

struct tr_pipeline_listener_preface;

struct tr_pipeline_listener_authorization {
	struct tr_tls_provider_operation operation;
	struct tr_conn_handle connection;
	struct tr_pipeline_route_preface route;
	struct tr_pipeline_listener_preface *preface;
	struct tr_peer_identity_snapshot *peer_identity;
	struct tr_peer_identity_snapshot *control_identity;
	struct tr_tls_openssl_profile *tls_profile;
	uint64_t tls_generation;
	uint32_t connection_index;
	uint32_t connection_generation;
	uint32_t session_index;
	uint32_t session_generation;
	int used;
	int cancelled;
	int cancel_notified;
};

struct tr_pipeline_listener {
	struct tr_pipeline_listener_config config;
	struct tr_pipeline_registry *registry;
	struct tr_buffer_pool message_pool;
	int message_pool_ready;

	struct tr_pipeline_listener_session *sessions;
	struct tr_pipeline_listener_connection *connections;
	struct tr_pipeline_listener_authorization *authorizations;
	int authorization_fd;
	int authorization_event_registered;
	struct tr_pipeline_listener_rx_slot *rx_slots;
	struct tr_pipeline_listener_message_job *message_jobs;
	uint32_t rx_slot_capacity;
	uint32_t message_jobs_current;
	uint32_t rx_messages_current;
	uint64_t message_state_reserved_bytes;
	atomic_uint_fast32_t decoded_buffers_current;

	int listen_fd;
	int retained_listen_fd;
	uint16_t bound_port;
	int listener_registered;
	int draining;
	struct tr_reactor_timer_handle handshake_timer;
	int handshake_timer_registered;

	/* 监听器与 Pipeline 计数器仍仅由反应器所有者访问。
	 * 此锁只向外部等待者发布排空生命周期代次。
	 */
	pthread_mutex_t drain_wait_lock;
	pthread_cond_t drain_wait_cond;
	uint64_t drain_generation;
	uint64_t drained_generation;
	uint32_t drain_waiters;
	int drain_wait_active;
	int drain_wait_closed;

	uint32_t pipelines_current;
	uint32_t authorizations_current;
	uint32_t pipelines_peak;
	uint32_t connections_current;
	uint32_t connections_peak;
	uint64_t control_accepts;
	uint64_t data_accepts;
	uint64_t route_rejections;
	uint64_t capacity_rejections;
};

struct tr_pipeline_listener_preface {
	struct tr_pipeline_listener *listener;
	struct tr_pipeline_route_parser parser;
	struct tr_tls_provider_operation authorization_operation;
	uint32_t connection_index;
	uint32_t connection_generation;
	int authorization_pending;
	int handed_off;
};

static int
tr_pipeline_listener_operation_equal(struct tr_tls_provider_operation left,
                                     struct tr_tls_provider_operation right)
{
	return left.instance == right.instance &&
	       left.generation == right.generation && left.slot == right.slot &&
	       left.kind == right.kind;
}

static struct tr_pipeline_listener_authorization *
tr_pipeline_listener_authorization_find(
        struct tr_pipeline_listener *listener,
        struct tr_tls_provider_operation operation)
{
	uint32_t i;

	if (!listener)
		return NULL;
	for (i = 0U; i < listener->config.authorization_capacity; ++i) {
		struct tr_pipeline_listener_authorization *authorization =
		        &listener->authorizations[i];

		if (authorization->used &&
		    tr_pipeline_listener_operation_equal(
		            authorization->operation, operation))
			return authorization;
	}
	return NULL;
}

static void tr_pipeline_listener_authorization_release(
        struct tr_pipeline_listener_authorization *authorization)
{
	if (!authorization)
		return;
	tr_peer_identity_snapshot_release(authorization->peer_identity);
	tr_peer_identity_snapshot_release(authorization->control_identity);
	tr_tls_openssl_profile_release(authorization->tls_profile);
	authorization->peer_identity = NULL;
	authorization->control_identity = NULL;
	authorization->tls_profile = NULL;
}

static int tr_pipeline_listener_authorization_cancel_on_owner(
        struct tr_pipeline_listener *listener,
        struct tr_pipeline_listener_authorization *authorization, int reason)
{
	int notify = 0;
	int ret;

	if (!listener || !authorization || !authorization->used || reason >= 0)
		return TR_ERR_INVALID;
	authorization->cancelled = 1;
	authorization->preface = NULL;
	if (authorization->route.role == TR_PIPELINE_ROUTE_DATA)
		(void)tr_pipeline_registry_cancel_data_route(
		        listener->registry, &authorization->route);
	ret = tr_tls_provider_operation_request_cancel(
	        listener->config.authorization_operations,
	        authorization->operation, reason, &notify);
	if (ret != TR_OK)
		return ret;
	if (notify && !authorization->cancel_notified) {
		authorization->cancel_notified = 1;
		listener->config.authorize_cancel(
		        authorization->operation,
		        listener->config.authorize_async_arg);
	}
	return TR_OK;
}

static void tr_pipeline_listener_preface_release(void *arg);
static void tr_pipeline_listener_authorization_ready(int fd, uint32_t events,
                                                     void *arg);

static int tr_pipeline_listener_drain_complete_on_owner(
	const struct tr_pipeline_listener *listener)
{
	return listener && listener->draining &&
	       listener->connections_current == 0U &&
	       listener->pipelines_current == 0U &&
	       listener->authorizations_current == 0U &&
	       listener->message_jobs_current == 0U &&
	       listener->rx_messages_current == 0U;
}

static void tr_pipeline_listener_publish_drain_progress_on_owner(
	struct tr_pipeline_listener *listener)
{
	if (!tr_pipeline_listener_drain_complete_on_owner(listener))
		return;

	pthread_mutex_lock(&listener->drain_wait_lock);
	if (listener->drain_wait_active &&
	    listener->drained_generation != listener->drain_generation) {
		listener->drained_generation = listener->drain_generation;
		pthread_cond_broadcast(&listener->drain_wait_cond);
	}
	pthread_mutex_unlock(&listener->drain_wait_lock);
}

static void tr_pipeline_listener_publish_drain_start_on_owner(
	struct tr_pipeline_listener *listener)
{
	pthread_mutex_lock(&listener->drain_wait_lock);
	listener->drain_generation++;
	if (listener->drain_generation == 0U)
		listener->drain_generation = 1U;
	listener->drained_generation = 0U;
	listener->drain_wait_active = 1;
	pthread_mutex_unlock(&listener->drain_wait_lock);

	tr_pipeline_listener_publish_drain_progress_on_owner(listener);
}

static void tr_pipeline_listener_publish_listening_on_owner(
	struct tr_pipeline_listener *listener)
{
	pthread_mutex_lock(&listener->drain_wait_lock);
	listener->drain_wait_active = 0;
	pthread_cond_broadcast(&listener->drain_wait_cond);
	pthread_mutex_unlock(&listener->drain_wait_lock);
}

static int tr_pipeline_listener_close_wait_admission(
	struct tr_pipeline_listener *listener)
{
	int ret = TR_OK;

	pthread_mutex_lock(&listener->drain_wait_lock);
	listener->drain_wait_closed = 1;
	if (listener->drain_waiters != 0U) {
		pthread_cond_broadcast(&listener->drain_wait_cond);
		ret = TR_ERR_STATE;
	}
	pthread_mutex_unlock(&listener->drain_wait_lock);
	return ret;
}

static uint32_t tr_pipeline_listener_next_generation(uint32_t generation)
{
	generation++;
	if (generation == 0U)
		generation = 1U;
	return generation;
}

static struct tr_pipeline_listener_connection *
tr_pipeline_listener_connection_exact(
	struct tr_pipeline_listener *listener, uint32_t index,
	uint32_t generation)
{
	struct tr_pipeline_listener_connection *connection;

	if (!listener || index >= listener->config.connection_capacity)
		return NULL;
	connection = &listener->connections[index];
	if (!connection->used || connection->generation != generation)
		return NULL;
	return connection;
}

static int tr_pipeline_listener_conn_equal(struct tr_conn_handle left,
                                           struct tr_conn_handle right)
{
	return left.reactor == right.reactor && left.slot == right.slot &&
	       left.generation == right.generation;
}

static int
tr_pipeline_listener_route_equal(const struct tr_pipeline_route_preface *left,
                                 const struct tr_pipeline_route_preface *right)
{
	return left && right && left->version == right->version &&
	       left->role == right->role &&
	       left->owner_shard_id == right->owner_shard_id &&
	       left->pipeline_id == right->pipeline_id &&
	       left->epoch == right->epoch &&
	       left->member_index == right->member_index &&
	       left->member_generation == right->member_generation;
}

static struct tr_pipeline_listener_rx_slot *tr_pipeline_listener_rx_find(
        struct tr_pipeline_listener *listener, uint32_t connection_index,
        uint32_t connection_generation,
        const struct tr_pipeline_route_preface *route, uint32_t stream_id)
{
	uint32_t i;

	for (i = 0; i < listener->rx_slot_capacity; ++i) {
		struct tr_pipeline_listener_rx_slot *slot =
		        &listener->rx_slots[i];

		if (slot->used && slot->connection_index == connection_index &&
		    slot->connection_generation == connection_generation &&
		    slot->stream_id == stream_id &&
		    tr_pipeline_listener_route_equal(&slot->route, route))
			return slot;
	}
	return NULL;
}

static struct tr_pipeline_listener_rx_slot *
tr_pipeline_listener_rx_free(struct tr_pipeline_listener *listener)
{
	uint32_t i;

	for (i = 0; i < listener->rx_slot_capacity; ++i)
		if (!listener->rx_slots[i].used)
			return &listener->rx_slots[i];
	return NULL;
}

static struct tr_pipeline_listener_message_job *
tr_pipeline_listener_job_reserve(
        struct tr_pipeline_listener *listener,
        struct tr_pipeline_listener_connection *tracked,
        uint32_t connection_index, struct tr_conn_handle connection,
        uint32_t stream_id, uint64_t message_id, uint32_t *job_index_out)
{
	uint32_t i;

	for (i = 0; i < listener->config.message_task_capacity; ++i) {
		struct tr_pipeline_listener_message_job *job =
		        &listener->message_jobs[i];

		if (job->used)
			continue;
		memset(job, 0, sizeof(*job));
		job->listener = listener;
		job->connection = connection;
		job->route = tracked->route;
		job->connection_index = connection_index;
		job->connection_generation = tracked->generation;
		job->stream_id = stream_id;
		job->message_id = message_id;
		job->used = 1;
		listener->message_jobs_current++;
		if (job_index_out)
			*job_index_out = i;
		return job;
	}
	return NULL;
}

static void
tr_pipeline_listener_job_clear(struct tr_pipeline_listener_message_job *job)
{
	struct tr_pipeline_listener *listener;

	if (!job || !job->used)
		return;
	listener = job->listener;
	memset(job, 0, sizeof(*job));
	if (listener && listener->message_jobs_current != 0U)
		listener->message_jobs_current--;
	if (listener)
		tr_pipeline_listener_publish_drain_progress_on_owner(listener);
}

static void
tr_pipeline_listener_rx_clear(struct tr_pipeline_listener *listener,
                              struct tr_pipeline_listener_rx_slot *slot,
                              int clear_job)
{
	if (!listener || !slot || !slot->used)
		return;
	if (slot->builder && tr_message_builder_cancel(&slot->builder) != TR_OK)
		return;
	if (clear_job &&
	    slot->job_index < listener->config.message_task_capacity) {
		struct tr_pipeline_listener_message_job *job =
		        &listener->message_jobs[slot->job_index];
		if (job->used && !job->submitted)
			tr_pipeline_listener_job_clear(job);
	}
	memset(slot, 0, sizeof(*slot));
	if (listener->rx_messages_current != 0U)
		listener->rx_messages_current--;
	tr_pipeline_listener_publish_drain_progress_on_owner(listener);
}

static void tr_pipeline_listener_retire_compression_route(
        struct tr_pipeline_listener *listener, uint32_t connection_index,
        uint32_t connection_generation)
{
	uint32_t i;

	if (!listener)
		return;
	for (i = 0; i < listener->rx_slot_capacity; ++i) {
		struct tr_pipeline_listener_rx_slot *slot =
		        &listener->rx_slots[i];

		if (slot->used && slot->connection_index == connection_index &&
		    slot->connection_generation == connection_generation)
			tr_pipeline_listener_rx_clear(listener, slot, 1);
	}
	for (i = 0; i < listener->config.message_task_capacity; ++i) {
		struct tr_pipeline_listener_message_job *job =
		        &listener->message_jobs[i];

		if (!job->used || job->connection_index != connection_index ||
		    job->connection_generation != connection_generation)
			continue;
		job->cancelled = 1;
		if (!job->submitted) {
			tr_pipeline_listener_job_clear(job);
			continue;
		}
		if (listener->config.message_cancel)
			(void)listener->config.message_cancel(
			        listener->config.message_context, job->task);
	}
}

static void
tr_pipeline_listener_decoded_buffer_release(struct tr_buffer *buffer)
{
	struct tr_pipeline_listener_decoded_buffer *decoded =
	        (struct tr_pipeline_listener_decoded_buffer *)buffer;
	struct tr_pipeline_listener *listener = decoded->listener;
	struct tr_memory_budget *budget = decoded->budget;

	(void)tr_message_release(&decoded->message);
	(void)tr_memory_budget_release(budget, sizeof(*decoded));
	free(decoded);
	atomic_fetch_sub_explicit(&listener->decoded_buffers_current, 1U,
	                          memory_order_acq_rel);
}

static int
tr_pipeline_listener_transform_error(enum tr_message_transform_outcome outcome)
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

static struct tr_pipeline_listener_connection *
tr_pipeline_listener_job_connection(
        struct tr_pipeline_listener_message_job *job)
{
	struct tr_pipeline_listener_connection *tracked;
	struct tr_pipeline_listener *listener = job->listener;

	tracked = tr_pipeline_listener_connection_exact(
	        listener, job->connection_index, job->connection_generation);
	if (!tracked || tracked->role != TR_PIPELINE_LISTENER_CONN_DATA ||
	    !tracked->message_caps_received || job->cancelled ||
	    !tr_pipeline_listener_conn_equal(tracked->handle,
	                                     job->connection) ||
	    !tr_pipeline_listener_route_equal(&tracked->route, &job->route))
		return NULL;
	return tracked;
}

static void
tr_pipeline_listener_message_done(void *arg, struct tr_task_handle handle,
                                  struct tr_message_service_result result,
                                  struct tr_message *message)
{
	struct tr_pipeline_listener_message_job *job =
	        (struct tr_pipeline_listener_message_job *)arg;
	struct tr_pipeline_listener *listener;
	struct tr_pipeline_listener_connection *tracked = NULL;
	struct tr_message decoded = {0};
	struct tr_message_view view;
	struct tr_message_limits message_limits;
	struct tr_pipeline_listener_decoded_buffer *owned = NULL;
	struct tr_frame frame;
	uint64_t wrapper_bytes = sizeof(*owned);
	int abort_status = TR_OK;
	int current = 0;
	int wrapper_reserved = 0;
	int ret;

	if (!job || !job->used || !job->listener)
		return;
	listener = job->listener;
	if (handle.instance != job->task.instance ||
	    handle.generation != job->task.generation ||
	    handle.slot != job->task.slot) {
		tr_pipeline_listener_job_clear(job);
		return;
	}
	if (result.delivery == TR_MESSAGE_SERVICE_OWNER_STOPPED) {
		/* 父 owner 已 join；这里只退休槽位，不操作连接或应用协议状态。
		 */
		tr_pipeline_listener_job_clear(job);
		return;
	}

	tracked = tr_pipeline_listener_job_connection(job);
	current = tracked != NULL && !listener->draining;
	if (current && result.outcome == TR_MESSAGE_TRANSFORM_OK && message) {
		ret = tr_message_view(message, &view);
		if (ret != TR_OK || view.len != job->decoded_message_bytes ||
		    view.len > listener->config.max_message_bytes ||
		    view.len > UINT32_MAX) {
			abort_status = ret == TR_OK ? TR_ERR_BAD_LENGTH : ret;
		} else {
			ret = tr_memory_budget_reserve(
			        listener->config.message_budget, wrapper_bytes);
			if (ret == TR_OK) {
				wrapper_reserved = 1;
				owned = calloc(1, sizeof(*owned));
				if (!owned) {
					(void)tr_memory_budget_release(
					        listener->config.message_budget,
					        wrapper_bytes);
					wrapper_reserved = 0;
					ret = TR_ERR_NOMEM;
				}
			}
			if (ret == TR_OK)
				ret = tr_message_clone(message, &decoded);
			if (ret == TR_OK) {
				message_limits.max_decoded_bytes =
				        listener->config.max_message_bytes;
				message_limits.max_encoded_bytes =
				        listener->config.max_message_bytes;
				ret = tr_message_none_move(&decoded,
				                           &message_limits,
				                           &owned->message);
			}
			if (ret == TR_OK)
				ret = tr_message_view(&owned->message, &view);
			if (ret != TR_OK) {
				(void)tr_message_release(&decoded);
				if (owned) {
					(void)tr_message_release(
					        &owned->message);
					(void)tr_memory_budget_release(
					        listener->config.message_budget,
					        wrapper_bytes);
					wrapper_reserved = 0;
					free(owned);
					owned = NULL;
				} else if (wrapper_reserved) {
					(void)tr_memory_budget_release(
					        listener->config.message_budget,
					        wrapper_bytes);
					wrapper_reserved = 0;
				}
				abort_status = ret;
			} else {
				owned->listener = listener;
				owned->budget = listener->config.message_budget;
				owned->buffer.data = (uint8_t *)view.data;
				owned->buffer.capacity = (uint32_t)view.len;
				owned->buffer.len = (uint32_t)view.len;
				owned->buffer.release_cb =
				        tr_pipeline_listener_decoded_buffer_release;
				atomic_fetch_add_explicit(
				        &listener->decoded_buffers_current, 1U,
				        memory_order_release);

				memset(&frame, 0, sizeof(frame));
				frame.header.version = TR_WIRE_ENV_VERSION;
				frame.header.type = TR_FRAME_DATA;
				frame.header.flags =
				        TR_FRAME_F_FIRST | TR_FRAME_F_LAST;
				frame.header.stream_id = job->stream_id;
				frame.header.message_id = job->message_id;
				frame.header.payload_len = (uint32_t)view.len;
				frame.payload = &owned->buffer;
				if (listener->config.data_frame_cb(
				            &tracked->route, job->connection,
				            &frame,
				            listener->config
				                    .data_callback_arg) !=
				    TR_FRAME_TAKE_OWNERSHIP) {
					tr_buffer_release(&owned->buffer);
				}
				owned = NULL;
			}
		}
	} else if (current) {
		abort_status = result.outcome == TR_MESSAGE_TRANSFORM_OK
		                       ? TR_ERR_STATE
		                       : tr_pipeline_listener_transform_error(
		                                 result.outcome);
	}

	if (abort_status != TR_OK && current)
		(void)tr_reactor_abort_on_owner(
		        job->connection,
		        abort_status < 0 ? abort_status : TR_ERR_STATE);
	tr_pipeline_listener_job_clear(job);
}

static int
tr_pipeline_listener_submit_decode(struct tr_pipeline_listener *listener,
                                   struct tr_pipeline_listener_rx_slot *slot)
{
	struct tr_pipeline_listener_message_job *job;
	struct tr_message input = {0};
	struct tr_message_transform_config transform;
	int ret;

	if (!listener || !slot || !slot->used || !slot->builder ||
	    slot->job_index >= listener->config.message_task_capacity)
		return TR_ERR_STATE;
	job = &listener->message_jobs[slot->job_index];
	if (!job->used || job->submitted ||
	    job->connection_index != slot->connection_index ||
	    job->connection_generation != slot->connection_generation ||
	    job->stream_id != slot->stream_id ||
	    job->message_id != slot->message_id)
		return TR_ERR_STATE;

	ret = tr_message_builder_publish(&slot->builder, &input);
	if (ret != TR_OK)
		return ret;
	job->decoded_message_bytes = slot->decoded_message_bytes;
	tr_pipeline_listener_rx_clear(listener, slot, 0);

	memset(&transform, 0, sizeof(transform));
	transform.kind = TR_MESSAGE_ZSTD_DECODE;
	transform.limits.max_decoded_bytes = listener->config.max_message_bytes;
	transform.limits.max_encoded_bytes =
	        listener->config.max_encoded_message_bytes -
	        TR_MESSAGE_ENVELOPE_WIRE_SIZE;
	transform.decoded_size = job->decoded_message_bytes;
	transform.max_workspace_bytes = TR_GROUP_MESSAGE_WORKSPACE_BYTES;
	transform.max_window_log = TR_GROUP_MESSAGE_WINDOW_LOG;
	ret = listener->config.message_submit(
	        listener->config.message_context, &input, &transform,
	        tr_pipeline_listener_message_done, job, &job->task);
	if (ret != TR_OK) {
		(void)tr_message_release(&input);
		tr_pipeline_listener_job_clear(job);
		return ret;
	}
	job->submitted = 1;
	return TR_OK;
}

static int tr_pipeline_listener_receive_compressed(
        struct tr_pipeline_listener *listener,
        struct tr_pipeline_listener_connection *tracked,
        struct tr_conn_handle connection, struct tr_frame *frame)
{
	struct tr_pipeline_listener_rx_slot *slot;
	struct tr_pipeline_listener_rx_slot *free_slot;
	struct tr_pipeline_listener_message_job *job;
	struct tr_message_envelope envelope;
	struct tr_message_builder *builder = NULL;
	uint32_t connection_index;
	uint32_t job_index = UINT32_MAX;
	uint32_t payload_offset = 0U;
	uint32_t payload_bytes;
	uint32_t append_bytes;
	int first;
	int last;
	int ret;

	if (!listener || !tracked || !frame || !frame->payload)
		return TR_ERR_INVALID;
	if (frame->payload->len == 0U)
		return TR_ERR_BAD_LENGTH;
	if (listener->rx_slot_capacity == 0U ||
	    listener->config.message_task_capacity == 0U ||
	    !listener->config.message_submit ||
	    !listener->config.message_cancel ||
	    !listener->config.message_context ||
	    !listener->config.message_budget)
		return TR_ERR_UNSUPPORTED;
	if (tracked->role != TR_PIPELINE_LISTENER_CONN_DATA ||
	    !tracked->message_caps_received || frame->header.stream_id == 0U ||
	    frame->header.message_id == 0U ||
	    !(listener->config.receive_compression_algorithms &
	      TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD) ||
	    (frame->header.flags & ~(TR_FRAME_F_COMPRESSED | TR_FRAME_F_FIRST |
	                             TR_FRAME_F_LAST)) != 0U ||
	    !(frame->header.flags & TR_FRAME_F_COMPRESSED))
		return TR_ERR_BAD_TYPE;

	first = (frame->header.flags & TR_FRAME_F_FIRST) != 0U;
	last = (frame->header.flags & TR_FRAME_F_LAST) != 0U;
	connection_index = (uint32_t)(tracked - listener->connections);
	slot = tr_pipeline_listener_rx_find(
	        listener, connection_index, tracked->generation,
	        &tracked->route, frame->header.stream_id);
	payload_bytes = frame->payload->len;

	if (first) {
		if (slot || payload_bytes < TR_MESSAGE_ENVELOPE_WIRE_SIZE)
			return slot ? TR_ERR_BAD_TYPE : TR_ERR_BAD_LENGTH;
		ret = tr_message_envelope_decode(frame->payload->data,
		                                 TR_MESSAGE_ENVELOPE_WIRE_SIZE,
		                                 &envelope);
		if (ret != TR_OK)
			return ret;
		if (envelope.decoded_message_bytes >
		            listener->config.max_message_bytes ||
		    envelope.encoded_body_bytes >
		            listener->config.max_encoded_message_bytes -
		                    TR_MESSAGE_ENVELOPE_WIRE_SIZE)
			return TR_ERR_BAD_LENGTH;

		free_slot = tr_pipeline_listener_rx_free(listener);
		if (!free_slot)
			return TR_ERR_STATE;
		job = tr_pipeline_listener_job_reserve(
		        listener, tracked, connection_index, connection,
		        frame->header.stream_id, frame->header.message_id,
		        &job_index);
		if (!job)
			return TR_ERR_STATE;
		ret = tr_message_builder_create(listener->config.message_budget,
		                                envelope.encoded_body_bytes,
		                                &builder);
		if (ret != TR_OK) {
			tr_pipeline_listener_job_clear(job);
			return ret;
		}

		memset(free_slot, 0, sizeof(*free_slot));
		free_slot->builder = builder;
		free_slot->route = tracked->route;
		free_slot->connection_index = connection_index;
		free_slot->connection_generation = tracked->generation;
		free_slot->stream_id = frame->header.stream_id;
		free_slot->encoded_body_bytes = envelope.encoded_body_bytes;
		free_slot->decoded_message_bytes =
		        envelope.decoded_message_bytes;
		free_slot->job_index = job_index;
		free_slot->message_id = frame->header.message_id;
		free_slot->used = 1;
		listener->rx_messages_current++;
		slot = free_slot;
		payload_offset = TR_MESSAGE_ENVELOPE_WIRE_SIZE;
	} else if (!slot || slot->message_id != frame->header.message_id) {
		return TR_ERR_BAD_TYPE;
	}

	append_bytes = payload_bytes - payload_offset;
	if (append_bytes > slot->encoded_body_bytes - slot->received_body_bytes)
		return TR_ERR_BAD_LENGTH;
	ret = tr_message_builder_append(slot->builder,
	                                frame->payload->data + payload_offset,
	                                append_bytes);
	if (ret != TR_OK)
		return ret;
	slot->received_body_bytes += append_bytes;
	if (last) {
		if (slot->received_body_bytes != slot->encoded_body_bytes)
			return TR_ERR_BAD_LENGTH;
		return tr_pipeline_listener_submit_decode(listener, slot);
	}
	if (slot->received_body_bytes == slot->encoded_body_bytes)
		return TR_ERR_BAD_LENGTH;
	return TR_OK;
}

static void tr_pipeline_listener_connection_clear(
	struct tr_pipeline_listener_connection *connection)
{
	struct tr_pipeline_listener *listener;

	if (!connection || !connection->used)
		return;
	listener = connection->listener;
	memset(&connection->handle, 0, sizeof(connection->handle));
	memset(&connection->route, 0, sizeof(connection->route));
	memset(&connection->authorization_operation, 0,
	       sizeof(connection->authorization_operation));
	connection->authorization_pending = 0;
	connection->role = TR_PIPELINE_LISTENER_CONN_PENDING;
	connection->message_caps_received = 0U;
	connection->used = 0;
	if (listener && listener->connections_current != 0U)
		listener->connections_current--;
	if (listener)
		tr_pipeline_listener_publish_drain_progress_on_owner(listener);
}

static struct tr_pipeline_listener_connection *
tr_pipeline_listener_connection_reserve(
	struct tr_pipeline_listener *listener, uint32_t *index_out)
{
	uint32_t i;

	for (i = 0; i < listener->config.connection_capacity; ++i) {
		struct tr_pipeline_listener_connection *connection =
			&listener->connections[i];

		if (connection->used)
			continue;
		connection->generation =
			tr_pipeline_listener_next_generation(connection->generation);
		connection->listener = listener;
		connection->used = 1;
		connection->role = TR_PIPELINE_LISTENER_CONN_PENDING;
		memset(&connection->handle, 0, sizeof(connection->handle));
		memset(&connection->route, 0, sizeof(connection->route));
		listener->connections_current++;
		if (listener->connections_current > listener->connections_peak)
			listener->connections_peak = listener->connections_current;
		if (index_out)
			*index_out = i;
		return connection;
	}

	listener->capacity_rejections++;
	return NULL;
}

static struct tr_pipeline_listener_session *
tr_pipeline_listener_session_find(
	struct tr_pipeline_listener *listener, uint64_t pipeline_id,
	uint64_t epoch)
{
	uint32_t i;

	for (i = 0; i < listener->config.pipeline_capacity; ++i) {
		struct tr_pipeline_listener_session *session =
		        &listener->sessions[i];

		if (session->used && !session->closing &&
		    !session->authorization_reserved &&
		    session->pipeline_id == pipeline_id &&
		    session->epoch == epoch)
			return session;
	}
	return NULL;
}

static struct tr_pipeline_listener_session *
tr_pipeline_listener_session_find_id(struct tr_pipeline_listener *listener,
                                     uint64_t pipeline_id)
{
	uint32_t i;

	for (i = 0; i < listener->config.pipeline_capacity; ++i) {
		struct tr_pipeline_listener_session *session =
		        &listener->sessions[i];

		if (session->used && session->pipeline_id == pipeline_id)
			return session;
	}
	return NULL;
}

static struct tr_pipeline_listener_session *
tr_pipeline_listener_session_reserve(struct tr_pipeline_listener *listener)
{
	uint32_t i;

	for (i = 0; i < listener->config.pipeline_capacity; ++i) {
		if (!listener->sessions[i].used)
			return &listener->sessions[i];
	}
	listener->capacity_rejections++;
	return NULL;
}

static int tr_pipeline_listener_session_reserve_authorization(
        struct tr_pipeline_listener *listener,
        const struct tr_pipeline_route_preface *route, uint32_t *out_index,
        uint32_t *out_generation)
{
	struct tr_pipeline_listener_session *session;
	uint32_t generation;

	if (!listener || !route || !out_index || !out_generation)
		return TR_ERR_INVALID;
	if (tr_pipeline_listener_session_find_id(listener, route->pipeline_id))
		return TR_ERR_STALE;
	session = tr_pipeline_listener_session_reserve(listener);
	if (!session)
		return TR_AGAIN;
	generation = tr_pipeline_listener_next_generation(session->generation);
	memset(session, 0, sizeof(*session));
	session->listener = listener;
	session->pipeline_id = route->pipeline_id;
	session->epoch = route->epoch;
	session->generation = generation;
	session->used = 1;
	session->authorization_reserved = 1;
	*out_index = (uint32_t)(session - listener->sessions);
	*out_generation = generation;
	return TR_OK;
}

static void tr_pipeline_listener_session_release_authorization(
        struct tr_pipeline_listener *listener, uint32_t index,
        uint32_t generation)
{
	struct tr_pipeline_listener_session *session;

	if (!listener || index >= listener->config.pipeline_capacity)
		return;
	session = &listener->sessions[index];
	if (!session->used || !session->authorization_reserved ||
	    session->generation != generation)
		return;
	memset(session, 0, sizeof(*session));
	session->generation = generation;
}

static int tr_pipeline_listener_identity_view(
        const struct tr_peer_identity_snapshot *snapshot,
        struct tr_peer_identity_view *out)
{
	memset(out, 0, sizeof(*out));
	out->authentication = TR_PEER_IDENTITY_NONE;
	if (snapshot)
		return tr_peer_identity_snapshot_view(snapshot, out);
	return TR_OK;
}

static int
tr_pipeline_listener_identity_equal(const struct tr_peer_identity_view *left,
                                    const struct tr_peer_identity_view *right)
{
	if (!left || !right || left->authentication != right->authentication ||
	    left->uri_san_len != right->uri_san_len)
		return 0;
	if (left->uri_san_len == 0U)
		return left->uri_san == NULL && right->uri_san == NULL;
	return left->uri_san && right->uri_san &&
	       memcmp(left->uri_san, right->uri_san, left->uri_san_len) == 0;
}

static void tr_pipeline_listener_control_closing(void *arg)
{
	struct tr_pipeline_listener_session *session =
		(struct tr_pipeline_listener_session *)arg;

	if (session && session->used)
		session->closing = 1;
}

static void tr_pipeline_listener_control_closed(
	uint64_t pipeline_id, uint64_t epoch, int teardown_status, void *arg)
{
	struct tr_pipeline_listener_session *session =
		(struct tr_pipeline_listener_session *)arg;
	struct tr_pipeline_listener *listener;
	struct tr_pipeline_listener_connection *connection;

	(void)teardown_status;
	if (!session || !session->used)
		return;
	listener = session->listener;
	if (!listener || session->pipeline_id != pipeline_id ||
	    session->epoch != epoch)
		return;

	connection = tr_pipeline_listener_connection_exact(
		listener, session->connection_index,
		session->connection_generation);
	if (connection)
		tr_pipeline_listener_connection_clear(connection);

	if (teardown_status != TR_OK) {
		session->closing = 1;
		return;
	}

	{
		uint32_t generation = session->generation;

		tr_peer_identity_snapshot_release(session->peer_identity);
		session->peer_identity = NULL;
		tr_tls_openssl_profile_release(session->tls_profile);
		session->tls_profile = NULL;
		memset(session, 0, sizeof(*session));
		session->generation = generation;
	}
	if (listener->pipelines_current != 0U)
		listener->pipelines_current--;
	tr_pipeline_listener_publish_drain_progress_on_owner(listener);
}

static enum tr_frame_disposition tr_pipeline_listener_data_frame(
	struct tr_conn_handle connection, struct tr_frame *frame, void *arg)
{
	struct tr_pipeline_listener_connection *tracked =
		(struct tr_pipeline_listener_connection *)arg;
	struct tr_pipeline_listener *listener;
	struct tr_message_caps peer_caps;
	struct tr_message_caps local_caps;
	struct tr_buffer *buffer = NULL;
	int ret;

	if (!tracked || !tracked->used)
		return TR_FRAME_RELEASE;
	listener = tracked->listener;
	if (!listener || !listener->config.data_frame_cb)
		return TR_FRAME_RELEASE;
	if (frame && frame->header.type == TR_FRAME_MESSAGE_CAPS) {
		if (tracked->role != TR_PIPELINE_LISTENER_CONN_DATA ||
		    frame->header.flags != 0U ||
		    frame->header.stream_id != 0U ||
		    frame->header.message_id != 0U || !frame->payload ||
		    frame->payload->len != TR_MESSAGE_CAPS_WIRE_SIZE ||
		    tracked->message_caps_received) {
			(void)tr_reactor_abort_on_owner(connection,
			                                TR_ERR_BAD_TYPE);
			return TR_FRAME_RELEASE;
		}
		memset(&peer_caps, 0, sizeof(peer_caps));
		ret = tr_message_caps_decode(frame->payload->data,
		                             frame->payload->len, &peer_caps);
		if (ret != TR_OK || peer_caps.receive_algorithm_bits != 0U ||
		    peer_caps.max_encoded_wire_bytes != 0U) {
			if (ret == TR_OK)
				ret = TR_ERR_BAD_TYPE;
			(void)tr_reactor_abort_on_owner(connection, ret);
			return TR_FRAME_RELEASE;
		}
		local_caps.receive_algorithm_bits =
		        listener->config.receive_compression_algorithms;
		local_caps.max_encoded_wire_bytes =
		        listener->config.max_encoded_message_bytes;
		ret = tr_buffer_acquire(&listener->message_pool,
		                        TR_MESSAGE_CAPS_WIRE_SIZE, &buffer);
		if (ret == TR_OK)
			ret = tr_message_caps_encode(buffer->data, &local_caps);
		if (ret == TR_OK) {
			buffer->len = TR_MESSAGE_CAPS_WIRE_SIZE;
			ret = tr_reactor_send(connection, TR_FRAME_MESSAGE_CAPS,
			                      0U, 0U, 0U, buffer);
			if (ret == TR_OK)
				buffer = NULL;
		}
		if (buffer)
			tr_buffer_release(buffer);
		if (ret != TR_OK) {
			(void)tr_reactor_abort_on_owner(connection, ret);
			return TR_FRAME_RELEASE;
		}
		tracked->message_caps_received = 1U;
		return TR_FRAME_RELEASE;
	}
	if (frame && frame->header.type == TR_FRAME_DATA) {
		uint32_t connection_index =
		        (uint32_t)(tracked - listener->connections);

		if (frame->header.flags & TR_FRAME_F_COMPRESSED) {
			ret = tr_pipeline_listener_receive_compressed(
			        listener, tracked, connection, frame);
			if (ret != TR_OK) {
				tr_pipeline_listener_retire_compression_route(
				        listener, connection_index,
				        tracked->generation);
				(void)tr_reactor_abort_on_owner(
				        connection,
				        ret < 0 ? ret : TR_ERR_STATE);
			}
			return TR_FRAME_RELEASE;
		}
		if (tr_pipeline_listener_rx_find(
		            listener, connection_index, tracked->generation,
		            &tracked->route, frame->header.stream_id)) {
			tr_pipeline_listener_retire_compression_route(
			        listener, connection_index,
			        tracked->generation);
			(void)tr_reactor_abort_on_owner(connection,
			                                TR_ERR_BAD_TYPE);
			return TR_FRAME_RELEASE;
		}
	}
	return listener->config.data_frame_cb(
		&tracked->route, connection, frame,
		listener->config.data_callback_arg);
}

static void tr_pipeline_listener_data_event(
	struct tr_conn_handle connection, enum tr_connection_event event,
	int status, void *arg)
{
	struct tr_pipeline_listener_connection *tracked =
		(struct tr_pipeline_listener_connection *)arg;
	struct tr_pipeline_listener *listener = NULL;
	tr_pipeline_listener_data_event_cb callback = NULL;
	struct tr_pipeline_route_preface route;
	void *callback_arg = NULL;
	int have_route = 0;

	memset(&route, 0, sizeof(route));
	if (tracked && tracked->used) {
		listener = tracked->listener;
		route = tracked->route;
		have_route = 1;
		if (listener) {
			callback = listener->config.data_event_cb;
			callback_arg = listener->config.data_callback_arg;
			tr_pipeline_listener_retire_compression_route(
			        listener,
			        (uint32_t)(tracked - listener->connections),
			        tracked->generation);
		}
		tr_pipeline_listener_connection_clear(tracked);
	}

	if (callback && have_route)
		callback(&route, connection, event, status, callback_arg);
}

static int tr_pipeline_listener_accept_control(
        struct tr_pipeline_listener *listener,
        struct tr_pipeline_listener_connection *tracked,
        const struct tr_pipeline_route_preface *route,
        struct tr_conn_handle connection, int run_authorize,
        uint32_t reserved_session_index, uint32_t reserved_session_generation)
{
	struct tr_pipeline_listener_session *session;
	struct tr_peer_identity_snapshot *peer_identity = NULL;
	struct tr_tls_openssl_profile *tls_profile = NULL;
	struct tr_connection_group_authorization authorization;
	uint64_t tls_generation = 0U;

	if (listener->draining)
		return TR_ERR_CLOSED;
	struct tr_pipeline_control_config control_config;
	struct tr_pipeline_control *control = NULL;
	struct tr_pipeline_control_transport_config transport_config;
	struct tr_pipeline_control_transport *transport = NULL;
	int ret;

	if (route->owner_shard_id != listener->config.owner_shard_id)
		return TR_ERR_STALE;
	ret = tr_reactor_peer_identity_snapshot_on_owner(connection,
	                                                 &peer_identity);
	if (ret != TR_OK)
		return ret;
	ret = tr_reactor_tls_generation_on_owner(connection, &tls_generation);
	if (ret != TR_OK)
		return ret;
	ret = tr_reactor_tls_profile_retain_on_owner(connection, &tls_profile);
	if (ret != TR_OK)
		return ret;
	if (peer_identity) {
		ret = tr_peer_identity_snapshot_retain(peer_identity);
		if (ret != TR_OK) {
			tr_tls_openssl_profile_release(tls_profile);
			return ret;
		}
	}
	memset(&authorization, 0, sizeof(authorization));
	authorization.group.group_id = route->pipeline_id;
	authorization.group.epoch = route->epoch;
	authorization.role = TR_CONNECTION_GROUP_ROLE_CONTROL;
	ret = tr_pipeline_listener_identity_view(peer_identity,
	                                         &authorization.peer_identity);
	if (ret == TR_OK)
		ret = tr_pipeline_listener_identity_view(
		        NULL, &authorization.control_identity);
	if (ret != TR_OK) {
		tr_peer_identity_snapshot_release(peer_identity);
		tr_tls_openssl_profile_release(tls_profile);
		return ret;
	}
	if (run_authorize) {
		ret = listener->config.authorize_control(
		        &authorization, listener->config.authorize_arg);
		if (ret != TR_OK) {
			tr_peer_identity_snapshot_release(peer_identity);
			tr_tls_openssl_profile_release(tls_profile);
			return ret;
		}
	}

	if (reserved_session_index != UINT32_MAX) {
		if (reserved_session_index >=
		    listener->config.pipeline_capacity) {
			tr_peer_identity_snapshot_release(peer_identity);
			tr_tls_openssl_profile_release(tls_profile);
			return TR_ERR_STALE;
		}
		session = &listener->sessions[reserved_session_index];
		if (!session->used || !session->authorization_reserved ||
		    session->generation != reserved_session_generation ||
		    session->pipeline_id != route->pipeline_id ||
		    session->epoch != route->epoch) {
			tr_peer_identity_snapshot_release(peer_identity);
			tr_tls_openssl_profile_release(tls_profile);
			return TR_ERR_STALE;
		}
	} else {
		session = tr_pipeline_listener_session_reserve(listener);
		if (!session) {
			tr_peer_identity_snapshot_release(peer_identity);
			tr_tls_openssl_profile_release(tls_profile);
			return TR_AGAIN;
		}
	}

	memset(&control_config, 0, sizeof(control_config));
	control_config.registry = listener->registry;
	control_config.pipeline_id = route->pipeline_id;
	control_config.epoch = route->epoch;
	control_config.data_capacity =
		listener->config.data_capacity_per_pipeline;
	control_config.stream_affinity_capacity =
		listener->config.stream_affinity_capacity_per_pipeline;
	ret = tr_pipeline_control_create(
		&control_config, connection, &control);
	if (ret != TR_OK) {
		tr_peer_identity_snapshot_release(peer_identity);
		tr_tls_openssl_profile_release(tls_profile);
		return ret;
	}

	{
		uint32_t generation =
		        session->authorization_reserved
		                ? session->generation
		                : tr_pipeline_listener_next_generation(
		                          session->generation);

		memset(session, 0, sizeof(*session));
		session->generation = generation;
	}
	session->listener = listener;
	session->peer_identity = peer_identity;
	session->tls_profile = tls_profile;
	session->pipeline_id = route->pipeline_id;
	session->epoch = route->epoch;
	session->tls_generation = tls_generation;
	session->connection_index =
		(uint32_t)(tracked - listener->connections);
	session->connection_generation = tracked->generation;
	session->used = 1;

	memset(&transport_config, 0, sizeof(transport_config));
	transport_config.control = control;
	transport_config.connection = connection;
	transport_config.message_pool = &listener->message_pool;
	transport_config.control_route = *route;
	transport_config.closing_cb =
		tr_pipeline_listener_control_closing;
	transport_config.closed_cb =
		tr_pipeline_listener_control_closed;
	transport_config.closed_arg = session;
	ret = tr_pipeline_control_transport_create(
		&transport_config, &transport);
	if (ret != TR_OK) {
		tr_peer_identity_snapshot_release(session->peer_identity);
		session->peer_identity = NULL;
		tr_tls_openssl_profile_release(session->tls_profile);
		session->tls_profile = NULL;
		session->used = 0;
		(void)tr_pipeline_control_close(control, connection);
		return ret;
	}

	session->transport = transport;
	tracked->route = *route;
	tracked->role = TR_PIPELINE_LISTENER_CONN_CONTROL;
	listener->pipelines_current++;
	if (listener->pipelines_current > listener->pipelines_peak)
		listener->pipelines_peak = listener->pipelines_current;
	return TR_OK;
}

static int tr_pipeline_listener_accept_data(
        struct tr_pipeline_listener *listener,
        struct tr_pipeline_listener_connection *tracked,
        const struct tr_pipeline_route_preface *route,
        struct tr_conn_handle connection, int run_authorize)
{
	struct tr_pipeline_ingress_config ingress;
	struct tr_pipeline_listener_session *session;
	struct tr_peer_identity_snapshot *peer_identity = NULL;
	struct tr_connection_group_authorization authorization;
	struct tr_peer_identity_view peer_view;
	struct tr_peer_identity_view control_view;
	uint64_t tls_generation = 0U;
	int ret;

	/* Route identity and the exact reserved member generation are checked
	 * before the DATA certificate is compared with its CONTROL session. */
	ret = tr_pipeline_registry_validate_data_route_on_owner(
	        listener->registry, route);
	if (ret != TR_OK)
		return ret;
	session = tr_pipeline_listener_session_find(
	        listener, route->pipeline_id, route->epoch);
	if (!session)
		return TR_ERR_STALE;
	ret = tr_reactor_tls_generation_on_owner(connection, &tls_generation);
	if (ret != TR_OK)
		return ret;
	if (tls_generation != session->tls_generation)
		return TR_ERR_STALE;
	ret = tr_reactor_peer_identity_snapshot_on_owner(connection,
	                                                 &peer_identity);
	if (ret != TR_OK)
		return ret;
	if (peer_identity) {
		ret = tr_peer_identity_snapshot_retain(peer_identity);
		if (ret != TR_OK)
			return ret;
	}
	ret = tr_pipeline_listener_identity_view(peer_identity, &peer_view);
	if (ret == TR_OK)
		ret = tr_pipeline_listener_identity_view(session->peer_identity,
		                                         &control_view);
	if (ret != TR_OK) {
		tr_peer_identity_snapshot_release(peer_identity);
		return ret;
	}
	if (!tr_pipeline_listener_identity_equal(&peer_view, &control_view)) {
		tr_peer_identity_snapshot_release(peer_identity);
		return TR_ERR_INVALID;
	}
	memset(&authorization, 0, sizeof(authorization));
	authorization.group.group_id = route->pipeline_id;
	authorization.group.epoch = route->epoch;
	authorization.role = TR_CONNECTION_GROUP_ROLE_DATA;
	authorization.peer_identity = peer_view;
	authorization.control_identity = control_view;
	ret = run_authorize
	              ? listener->config.authorize_control(
	                        &authorization, listener->config.authorize_arg)
	              : TR_OK;
	tr_peer_identity_snapshot_release(peer_identity);
	if (ret != TR_OK)
		return ret;

	memset(&ingress, 0, sizeof(ingress));
	ingress.registry = listener->registry;
	ingress.frame_cb = tr_pipeline_listener_data_frame;
	ingress.event_cb = tr_pipeline_listener_data_event;
	ingress.callback_arg = tracked;

	ret = tr_pipeline_ingress_attach_data_route_on_owner(
		&ingress, route, connection);
	if (ret != TR_OK)
		return ret;

	tracked->route = *route;
	tracked->role = TR_PIPELINE_LISTENER_CONN_DATA;
	return TR_OK;
}

static int tr_pipeline_listener_start_async_authorization(
        struct tr_pipeline_listener *listener,
        struct tr_pipeline_listener_connection *tracked,
        struct tr_pipeline_listener_preface *preface,
        const struct tr_pipeline_route_preface *route,
        struct tr_conn_handle connection)
{
	struct tr_pipeline_listener_authorization *pending;
	struct tr_connection_group_authorization request;
	struct tr_peer_identity_snapshot *peer_identity = NULL;
	struct tr_peer_identity_snapshot *control_identity = NULL;
	struct tr_tls_openssl_profile *tls_profile = NULL;
	struct tr_pipeline_listener_session *session = NULL;
	struct tr_peer_identity_view peer_view;
	struct tr_peer_identity_view control_view;
	struct tr_tls_provider_operation operation;
	uint64_t tls_generation = 0U;
	uint32_t session_index = UINT32_MAX;
	uint32_t session_generation = 0U;
	uint32_t authorization_index;
	int control_reservation_held = 0;
	int ret;

	if (!listener || !tracked || !preface || !route ||
	    !listener->config.authorize_async ||
	    !listener->config.authorize_cancel ||
	    !listener->config.authorization_operations)
		return TR_ERR_INVALID;
	if (listener->draining)
		return TR_ERR_CLOSED;
	if (route->role == TR_PIPELINE_ROUTE_CONTROL) {
		if (route->owner_shard_id != listener->config.owner_shard_id)
			return TR_ERR_STALE;
		if (tr_pipeline_listener_session_find_id(listener,
		                                         route->pipeline_id))
			return TR_ERR_STALE;
	} else if (route->role == TR_PIPELINE_ROUTE_DATA) {
		ret = tr_pipeline_registry_validate_data_route_on_owner(
		        listener->registry, route);
		if (ret != TR_OK)
			return ret;
		session = tr_pipeline_listener_session_find(
		        listener, route->pipeline_id, route->epoch);
		if (!session)
			return TR_ERR_STALE;
		session_index = (uint32_t)(session - listener->sessions);
		session_generation = session->generation;
	} else {
		return TR_ERR_BAD_TYPE;
	}

	ret = tr_reactor_tls_generation_on_owner(connection, &tls_generation);
	if (ret != TR_OK)
		return ret;
	ret = tr_reactor_tls_profile_retain_on_owner(connection, &tls_profile);
	if (ret != TR_OK)
		return ret;
	ret = tr_reactor_peer_identity_snapshot_on_owner(connection,
	                                                 &peer_identity);
	if (ret != TR_OK)
		goto fail;
	if (peer_identity) {
		ret = tr_peer_identity_snapshot_retain(peer_identity);
		if (ret != TR_OK)
			goto fail;
	}
	if (session) {
		if (tls_generation != session->tls_generation) {
			ret = TR_ERR_STALE;
			goto fail;
		}
		control_identity = session->peer_identity;
		if (control_identity) {
			ret = tr_peer_identity_snapshot_retain(
			        control_identity);
			if (ret != TR_OK)
				goto fail;
		}
	}
	ret = tr_pipeline_listener_identity_view(peer_identity, &peer_view);
	if (ret == TR_OK)
		ret = tr_pipeline_listener_identity_view(control_identity,
		                                         &control_view);
	if (ret != TR_OK)
		goto fail;
	if (session &&
	    !tr_pipeline_listener_identity_equal(&peer_view, &control_view)) {
		ret = TR_ERR_INVALID;
		goto fail;
	}

	memset(&request, 0, sizeof(request));
	request.group.group_id = route->pipeline_id;
	request.group.epoch = route->epoch;
	request.role = route->role == TR_PIPELINE_ROUTE_CONTROL
	                       ? TR_CONNECTION_GROUP_ROLE_CONTROL
	                       : TR_CONNECTION_GROUP_ROLE_DATA;
	request.peer_identity = peer_view;
	request.control_identity = control_view;
	request.tls_profile_generation = tls_generation;
	request.deadline_monotonic_ns =
	        tr_deadline_to_ns(&tracked->handshake_deadline);
	ret = listener->config.authorize_control(
	        &request, listener->config.authorize_arg);
	if (ret != TR_OK)
		goto fail;
	if (route->role == TR_PIPELINE_ROUTE_CONTROL) {
		ret = tr_pipeline_listener_session_reserve_authorization(
		        listener, route, &session_index, &session_generation);
		if (ret != TR_OK)
			goto fail;
		control_reservation_held = 1;
	}

	ret = tr_tls_provider_operation_reserve(
	        listener->config.authorization_operations,
	        TR_TLS_PROVIDER_OPERATION_AUTHORIZATION,
	        (uint32_t)sizeof(
	                enum tr_connection_group_authorization_decision),
	        &operation);
	if (ret != TR_OK)
		goto fail;
	for (authorization_index = 0U;
	     authorization_index < listener->config.authorization_capacity;
	     ++authorization_index)
		if (!listener->authorizations[authorization_index].used)
			break;
	if (authorization_index == listener->config.authorization_capacity) {
		int ignored = 0;

		(void)tr_tls_provider_operation_request_cancel(
		        listener->config.authorization_operations, operation,
		        TR_ERR_CANCELLED, &ignored);
		(void)tr_tls_provider_operation_retire(
		        listener->config.authorization_operations, operation);
		ret = TR_AGAIN;
		goto fail;
	}
	ret = tr_tls_provider_operation_accept(
	        listener->config.authorization_operations, operation);
	if (ret != TR_OK) {
		int ignored = 0;

		(void)tr_tls_provider_operation_request_cancel(
		        listener->config.authorization_operations, operation,
		        TR_ERR_CANCELLED, &ignored);
		(void)tr_tls_provider_operation_retire(
		        listener->config.authorization_operations, operation);
		goto fail;
	}

	pending = &listener->authorizations[authorization_index];
	memset(pending, 0, sizeof(*pending));
	pending->operation = operation;
	pending->connection = connection;
	pending->route = *route;
	pending->preface = preface;
	pending->peer_identity = peer_identity;
	pending->control_identity = control_identity;
	pending->tls_profile = tls_profile;
	pending->tls_generation = tls_generation;
	pending->connection_index = (uint32_t)(tracked - listener->connections);
	pending->connection_generation = tracked->generation;
	pending->session_index = session_index;
	pending->session_generation = session_generation;
	pending->used = 1;
	control_reservation_held = 0;
	listener->authorizations_current++;
	tracked->route = *route;
	tracked->authorization_operation = operation;
	tracked->authorization_pending = 1;
	preface->authorization_operation = operation;
	preface->authorization_pending = 1;
	listener->config.authorize_async(&request, operation,
	                                 listener->config.authorize_async_arg);
	return TR_IN_PROGRESS;

fail:
	if (control_reservation_held)
		tr_pipeline_listener_session_release_authorization(
		        listener, session_index, session_generation);
	tr_peer_identity_snapshot_release(peer_identity);
	tr_peer_identity_snapshot_release(control_identity);
	tr_tls_openssl_profile_release(tls_profile);
	return ret;
}

static int tr_pipeline_listener_authorization_revalidate(
        struct tr_pipeline_listener *listener,
        struct tr_pipeline_listener_authorization *authorization,
        struct tr_pipeline_listener_connection **out_tracked)
{
	struct tr_pipeline_listener_connection *tracked;
	struct tr_pipeline_listener_session *session = NULL;
	struct tr_peer_identity_snapshot *peer_identity = NULL;
	struct tr_tls_openssl_profile *tls_profile = NULL;
	struct tr_peer_identity_view peer_view;
	struct tr_peer_identity_view control_view;
	uint64_t tls_generation = 0U;
	int expired;
	int ret;

	if (!listener || !authorization || !authorization->used || !out_tracked)
		return TR_ERR_INVALID;
	*out_tracked = NULL;
	tracked = tr_pipeline_listener_connection_exact(
	        listener, authorization->connection_index,
	        authorization->connection_generation);
	if (!tracked || !tracked->authorization_pending ||
	    !tr_pipeline_listener_operation_equal(
	            tracked->authorization_operation,
	            authorization->operation) ||
	    tracked->handle.reactor != authorization->connection.reactor ||
	    tracked->handle.slot != authorization->connection.slot ||
	    tracked->handle.generation != authorization->connection.generation)
		return TR_ERR_STALE;
	ret = tr_deadline_expired(&tracked->handshake_deadline, &expired);
	if (ret != TR_OK)
		return ret;
	if (expired)
		return TR_ERR_TIMEOUT;
	ret = tr_reactor_tls_generation_on_owner(authorization->connection,
	                                         &tls_generation);
	if (ret != TR_OK)
		return ret;
	if (tls_generation != authorization->tls_generation)
		return TR_ERR_STALE;
	ret = tr_reactor_tls_profile_retain_on_owner(authorization->connection,
	                                             &tls_profile);
	if (ret != TR_OK)
		return ret;
	if (tls_profile != authorization->tls_profile) {
		tr_tls_openssl_profile_release(tls_profile);
		return TR_ERR_STALE;
	}
	tr_tls_openssl_profile_release(tls_profile);
	ret = tr_reactor_peer_identity_snapshot_on_owner(
	        authorization->connection, &peer_identity);
	if (ret != TR_OK)
		return ret;
	ret = tr_pipeline_listener_identity_view(peer_identity, &peer_view);
	if (ret == TR_OK)
		ret = tr_pipeline_listener_identity_view(
		        authorization->peer_identity, &control_view);
	if (ret == TR_OK &&
	    !tr_pipeline_listener_identity_equal(&peer_view, &control_view))
		ret = TR_ERR_STALE;
	if (ret != TR_OK)
		return ret;

	if (authorization->route.role == TR_PIPELINE_ROUTE_CONTROL) {
		if (authorization->route.owner_shard_id !=
		            listener->config.owner_shard_id ||
		    authorization->session_index >=
		            listener->config.pipeline_capacity)
			return TR_ERR_STALE;
		session = &listener->sessions[authorization->session_index];
		if (!session->used || !session->authorization_reserved ||
		    session->generation != authorization->session_generation ||
		    session->pipeline_id != authorization->route.pipeline_id ||
		    session->epoch != authorization->route.epoch)
			return TR_ERR_STALE;
	} else if (authorization->route.role == TR_PIPELINE_ROUTE_DATA) {
		ret = tr_pipeline_registry_validate_data_route_on_owner(
		        listener->registry, &authorization->route);
		if (ret != TR_OK)
			return ret;
		if (authorization->session_index >=
		    listener->config.pipeline_capacity)
			return TR_ERR_STALE;
		session = &listener->sessions[authorization->session_index];
		if (!session->used || session->closing ||
		    session->generation != authorization->session_generation ||
		    session->pipeline_id != authorization->route.pipeline_id ||
		    session->epoch != authorization->route.epoch ||
		    session->tls_generation != authorization->tls_generation)
			return TR_ERR_STALE;
		ret = tr_pipeline_listener_identity_view(session->peer_identity,
		                                         &control_view);
		if (ret != TR_OK)
			return ret;
		if (!tr_pipeline_listener_identity_equal(&peer_view,
		                                         &control_view))
			return TR_ERR_STALE;
	} else {
		return TR_ERR_BAD_TYPE;
	}
	*out_tracked = tracked;
	return TR_OK;
}

static int tr_pipeline_listener_authorization_retire_on_owner(
        struct tr_pipeline_listener *listener,
        struct tr_pipeline_listener_authorization *authorization)
{
	int ret;

	if (!listener || !authorization || !authorization->used)
		return TR_ERR_INVALID;
	ret = tr_tls_provider_operation_retire(
	        listener->config.authorization_operations,
	        authorization->operation);
	if (ret != TR_OK)
		return ret;
	if (authorization->route.role == TR_PIPELINE_ROUTE_CONTROL)
		tr_pipeline_listener_session_release_authorization(
		        listener, authorization->session_index,
		        authorization->session_generation);
	tr_pipeline_listener_authorization_release(authorization);
	memset(authorization, 0, sizeof(*authorization));
	if (listener->authorizations_current != 0U)
		listener->authorizations_current--;
	tr_pipeline_listener_publish_drain_progress_on_owner(listener);
	return TR_OK;
}

static void tr_pipeline_listener_authorization_process_on_owner(
        struct tr_pipeline_listener *listener,
        struct tr_pipeline_listener_authorization *authorization)
{
	struct tr_pipeline_listener_connection *tracked = NULL;
	struct tr_pipeline_listener_preface *preface;
	struct tr_conn_handle connection;
	enum tr_connection_group_authorization_decision decision =
	        TR_CONNECTION_GROUP_AUTHORIZATION_DENY;
	int provider_status = TR_AGAIN;
	size_t result_len = 0U;
	int ret;

	ret = tr_tls_provider_operation_result(
	        listener->config.authorization_operations,
	        authorization->operation, &provider_status, &decision,
	        sizeof(decision), &result_len);
	if (ret == TR_AGAIN)
		return;
	if (ret != TR_OK) {
		(void)tr_pipeline_listener_authorization_cancel_on_owner(
		        listener, authorization, TR_ERR_CANCELLED);
		return;
	}
	if (authorization->cancelled) {
		(void)tr_pipeline_listener_authorization_retire_on_owner(
		        listener, authorization);
		return;
	}
	preface = authorization->preface;
	if (!preface || !preface->authorization_pending ||
	    !tr_pipeline_listener_operation_equal(
	            preface->authorization_operation,
	            authorization->operation)) {
		(void)tr_pipeline_listener_authorization_retire_on_owner(
		        listener, authorization);
		return;
	}
	connection = authorization->connection;
	ret = tr_pipeline_listener_authorization_revalidate(
	        listener, authorization, &tracked);
	if (ret == TR_OK && provider_status == TR_OK &&
	    result_len == sizeof(decision) &&
	    decision == TR_CONNECTION_GROUP_AUTHORIZATION_ALLOW) {
		if (authorization->route.role == TR_PIPELINE_ROUTE_CONTROL)
			ret = tr_pipeline_listener_accept_control(
			        listener, tracked, &authorization->route,
			        authorization->connection, 0,
			        authorization->session_index,
			        authorization->session_generation);
		else
			ret = tr_pipeline_listener_accept_data(
			        listener, tracked, &authorization->route,
			        authorization->connection, 0);
	} else if (ret == TR_OK && provider_status == TR_OK &&
	           result_len == sizeof(decision) &&
	           decision == TR_CONNECTION_GROUP_AUTHORIZATION_DENY) {
		ret = TR_ERR_CLOSED;
	} else if (ret == TR_OK) {
		ret = provider_status < 0 ? provider_status : TR_ERR_INVALID;
	}
	if (ret != TR_OK) {
		listener->route_rejections++;
		if (authorization->route.role == TR_PIPELINE_ROUTE_DATA)
			(void)tr_pipeline_registry_cancel_data_route(
			        listener->registry, &authorization->route);
	}
	if (tracked && tracked->authorization_pending &&
	    tr_pipeline_listener_operation_equal(
	            tracked->authorization_operation,
	            authorization->operation)) {
		tracked->authorization_pending = 0;
		memset(&tracked->authorization_operation, 0,
		       sizeof(tracked->authorization_operation));
	}
	preface->authorization_pending = 0;
	memset(&preface->authorization_operation, 0,
	       sizeof(preface->authorization_operation));
	if (ret == TR_OK) {
		preface->handed_off = 1;
		if (authorization->route.role == TR_PIPELINE_ROUTE_CONTROL)
			listener->control_accepts++;
		else
			listener->data_accepts++;
	}
	if (tr_pipeline_listener_authorization_retire_on_owner(
	            listener, authorization) != TR_OK) {
		if (ret == TR_OK)
			ret = TR_ERR_STATE;
	}
	(void)tr_reactor_preface_resume_on_owner(connection, ret);
}

static void tr_pipeline_listener_authorization_scan_on_owner(
        struct tr_pipeline_listener *listener)
{
	uint32_t i;

	if (!listener || !listener->authorizations)
		return;
	for (i = 0U; i < listener->config.authorization_capacity; ++i) {
		struct tr_pipeline_listener_authorization *authorization =
		        &listener->authorizations[i];

		if (authorization->used)
			tr_pipeline_listener_authorization_process_on_owner(
			        listener, authorization);
	}
}

static void tr_pipeline_listener_authorization_ready(int fd, uint32_t events,
                                                     void *arg)
{
	struct tr_pipeline_listener *listener =
	        (struct tr_pipeline_listener *)arg;
	uint64_t value;

	(void)events;
	if (!listener || fd != listener->authorization_fd)
		return;
	while (read(fd, &value, sizeof(value)) == (ssize_t)sizeof(value))
		;
	tr_pipeline_listener_authorization_scan_on_owner(listener);
}

int tr_pipeline_listener_authorize_complete(
        struct tr_pipeline_listener *listener,
        struct tr_tls_provider_operation operation, int status,
        enum tr_connection_group_authorization_decision decision)
{
	uint64_t value = 1U;
	const void *result = NULL;
	size_t result_len = 0U;
	ssize_t written;
	int ret;

	if (!listener || !listener->config.authorization_operations ||
	    operation.kind != TR_TLS_PROVIDER_OPERATION_AUTHORIZATION ||
	    status > TR_OK ||
	    (decision != TR_CONNECTION_GROUP_AUTHORIZATION_ALLOW &&
	     decision != TR_CONNECTION_GROUP_AUTHORIZATION_DENY))
		return TR_ERR_INVALID;
	if (status == TR_OK) {
		result = &decision;
		result_len = sizeof(decision);
	}
	ret = tr_tls_provider_operation_complete(
	        listener->config.authorization_operations, operation, status,
	        result, result_len);
	if (ret != TR_OK)
		return ret;
	do {
		written = write(listener->authorization_fd, &value,
		                sizeof(value));
	} while (written < 0 && errno == EINTR);
	if (written == (ssize_t)sizeof(value) ||
	    (written < 0 && errno == EAGAIN))
		return TR_OK;
	return TR_ERR_SYS;
}

int tr_pipeline_listener_set_tls_profile_on_owner(
        struct tr_pipeline_listener *listener,
        struct tr_tls_openssl_profile *profile)
{
	if (!listener)
		return TR_ERR_INVALID;
	if (listener->draining)
		return TR_ERR_CLOSED;
	listener->config.tls_profile = profile;
	return TR_OK;
}

/* 单个所有者定时器扫描有界连接表；成功交接的 CONTROL/DATA 不再占租约。 */
static uint64_t tr_pipeline_listener_handshake_timer_main(void *arg,
							  uint64_t now_ns)
{
	struct tr_pipeline_listener *listener = arg;
	uint64_t next = 0U;
	uint32_t i;

	for (i = 0; i < listener->config.connection_capacity; ++i) {
		struct tr_pipeline_listener_connection *tracked =
			&listener->connections[i];
		uint64_t deadline;
		int ret;

		if (!tracked->used ||
		    tracked->role != TR_PIPELINE_LISTENER_CONN_PENDING)
			continue;
		deadline = tr_deadline_to_ns(&tracked->handshake_deadline);
		if (deadline <= now_ns) {
			ret = tr_reactor_abort_on_owner(tracked->handle,
							TR_ERR_TIMEOUT);
			if (ret == TR_OK || ret == TR_ERR_STALE) {
				listener->route_rejections++;
				tr_pipeline_listener_connection_clear(tracked);
				continue;
			}
			deadline = now_ns == UINT64_MAX ? now_ns : now_ns + 1U;
		}
		if (next == 0U || deadline < next)
			next = deadline;
	}
	/* 作为 eventfd 的恢复轮次：即使一次 provider completion 无法写入，
	 * 已准备结果仍会在下一次有界握手期限扫描中被 owner 消费。 */
	tr_pipeline_listener_authorization_scan_on_owner(listener);
	return next;
}

static int tr_pipeline_listener_arm_handshake_on_owner(
	struct tr_pipeline_listener *listener)
{
	uint64_t next = 0U;
	uint32_t i;
	int ret;

	if (!listener->handshake_timer_registered) {
		ret = tr_reactor_timer_register(
			listener->config.owner,
			tr_pipeline_listener_handshake_timer_main, listener,
			&listener->handshake_timer);
		if (ret != TR_OK)
			return ret;
		listener->handshake_timer_registered = 1;
	}
	for (i = 0; i < listener->config.connection_capacity; ++i) {
		struct tr_pipeline_listener_connection *tracked =
			&listener->connections[i];
		uint64_t deadline;

		if (!tracked->used ||
		    tracked->role != TR_PIPELINE_LISTENER_CONN_PENDING)
			continue;
		deadline = tr_deadline_to_ns(&tracked->handshake_deadline);
		if (next == 0U || deadline < next)
			next = deadline;
	}
	return tr_reactor_timer_arm(listener->handshake_timer, next);
}

static int tr_pipeline_listener_preface_feed(
	struct tr_conn_handle connection, const uint8_t *data, size_t len,
	int *done, void *arg)
{
	struct tr_pipeline_listener_preface *preface =
		(struct tr_pipeline_listener_preface *)arg;
	struct tr_pipeline_listener *listener;
	struct tr_pipeline_listener_connection *tracked;
	struct tr_pipeline_route_preface route;
	size_t consumed = 0U;
	int ready = 0;
	int ret;

	if (!preface || !done)
		return TR_ERR_INVALID;
	*done = 0;
	listener = preface->listener;
	tracked = tr_pipeline_listener_connection_exact(
		listener, preface->connection_index,
		preface->connection_generation);
	if (!tracked)
		return TR_ERR_STALE;

	{
		int expired;

		/* 即使同一轮先分发接收事件，过期的最后一个字节也不能完成交接。
		 */
		ret = tr_deadline_expired(&tracked->handshake_deadline,
					  &expired);
		if (ret != TR_OK || expired) {
			if (ret == TR_OK)
				ret = TR_ERR_TIMEOUT;
			goto reject;
		}
	}
	memset(&route, 0, sizeof(route));
	ret = tr_pipeline_route_parser_feed(
		&preface->parser, data, len, &consumed, &route, &ready);
	if (ret != TR_OK)
		goto reject;
	if (consumed != len) {
		ret = TR_ERR_STATE;
		goto reject;
	}
	if (!ready)
		return TR_OK;

	if (listener->config.authorize_async) {
		ret = tr_pipeline_listener_start_async_authorization(
		        listener, tracked, preface, &route, connection);
		if (ret == TR_IN_PROGRESS) {
			*done = 1;
			return TR_IN_PROGRESS;
		}
	} else if (route.role == TR_PIPELINE_ROUTE_CONTROL) {
		ret = tr_pipeline_listener_accept_control(listener, tracked,
		                                          &route, connection, 1,
		                                          UINT32_MAX, 0U);
	} else if (route.role == TR_PIPELINE_ROUTE_DATA) {
		ret = tr_pipeline_listener_accept_data(listener, tracked,
		                                       &route, connection, 1);
	} else {
		ret = TR_ERR_BAD_TYPE;
	}
	if (ret != TR_OK) {
		if (route.role == TR_PIPELINE_ROUTE_DATA)
			(void)tr_pipeline_registry_cancel_data_route(
				listener->registry, &route);
		goto reject;
	}

	/* 协议处理函数已拥有连接的终止清理。即使下面的最终期限检查失败，
	 * preface_release 也不能先清空 tracked，否则 DATA/CONTROL 的关闭回调
	 * 会失去精确路由与会话所有权。返回错误后由反应器统一关闭连接。
	 */
	preface->handed_off = 1;
	{
		int expired;

		ret = tr_deadline_expired(&tracked->handshake_deadline,
		                          &expired);
		if (ret != TR_OK || expired) {
			if (ret == TR_OK)
				ret = TR_ERR_TIMEOUT;
			goto reject;
		}
	}
	if (route.role == TR_PIPELINE_ROUTE_CONTROL)
		listener->control_accepts++;
	else
		listener->data_accepts++;
	*done = 1;
	return TR_OK;

reject:
	listener->route_rejections++;
	return ret;
}

static void tr_pipeline_listener_preface_release(void *arg)
{
	struct tr_pipeline_listener_preface *preface =
		(struct tr_pipeline_listener_preface *)arg;
	struct tr_pipeline_listener_connection *tracked;

	if (!preface)
		return;
	if (!preface->handed_off && preface->listener) {
		tracked = tr_pipeline_listener_connection_exact(
			preface->listener, preface->connection_index,
			preface->connection_generation);
		if (tracked && preface->authorization_pending) {
			struct tr_pipeline_listener_authorization *authorization =
			        tr_pipeline_listener_authorization_find(
			                preface->listener,
			                preface->authorization_operation);

			if (authorization &&
			    authorization->connection_index ==
			            preface->connection_index &&
			    authorization->connection_generation ==
			            preface->connection_generation)
				(void)tr_pipeline_listener_authorization_cancel_on_owner(
				        preface->listener, authorization,
				        TR_ERR_CANCELLED);
		}
		if (tracked)
			tr_pipeline_listener_connection_clear(tracked);
	}
	free(preface);
}

static int tr_pipeline_listener_adopt_fd(
	struct tr_pipeline_listener *listener, int fd)
{
	struct tr_pipeline_listener_connection *tracked;
	struct tr_pipeline_listener_preface *preface;
	struct tr_reactor_preface_handler handler;
	struct tr_conn_handle connection;
	uint32_t index = 0U;
	int ret;

	tracked = tr_pipeline_listener_connection_reserve(listener, &index);
	if (!tracked)
		return TR_AGAIN;

	ret = tr_deadline_init_ms(&tracked->handshake_deadline,
				  listener->config.handshake_timeout_ms);
	if (ret == TR_OK)
		ret = tr_pipeline_listener_arm_handshake_on_owner(listener);
	if (ret != TR_OK) {
		/* fd 尚未交给反应器；调用者关闭它，不能留下无到期唤醒的槽位。
		 */
		tr_pipeline_listener_connection_clear(tracked);
		return ret;
	}
	preface = (struct tr_pipeline_listener_preface *)calloc(
		1, sizeof(*preface));
	if (!preface) {
		tr_pipeline_listener_connection_clear(tracked);
		return TR_ERR_NOMEM;
	}
	preface->listener = listener;
	preface->connection_index = index;
	preface->connection_generation = tracked->generation;
	tr_pipeline_route_parser_init(&preface->parser);

	memset(&handler, 0, sizeof(handler));
	handler.byte_count = TR_PIPELINE_ROUTE_PREFACE_SIZE;
	handler.feed = tr_pipeline_listener_preface_feed;
	handler.release = tr_pipeline_listener_preface_release;
	handler.arg = preface;

	memset(&connection, 0, sizeof(connection));
	if (listener->config.tls_profile)
		ret = tr_reactor_adopt_tls_fd_prefaced_on_owner(
		        listener->config.owner, fd,
		        listener->config.tls_profile,
		        tr_deadline_to_ns(&tracked->handshake_deadline),
		        &handler, &connection);
	else
		ret = tr_reactor_adopt_fd_prefaced_on_owner(
		        listener->config.owner, fd, &handler, &connection);
	if (ret != TR_OK) {
		tr_pipeline_listener_preface_release(preface);
		return ret;
	}
	tracked->handle = connection;
	return TR_OK;
}

static void tr_pipeline_listener_on_ready(
	int fd, uint32_t events, void *arg)
{
	struct tr_pipeline_listener *listener =
		(struct tr_pipeline_listener *)arg;
	uint32_t accepted = 0U;

	if (!listener || listener->retained_listen_fd >= 0 ||
	    !(events & EPOLLIN))
		return;

	while (accepted < TR_PIPELINE_LISTENER_ACCEPT_BATCH) {
		int accepted_fd = -1;
		int ret = tr_tcp_accept(fd, &accepted_fd);

		if (ret == TR_AGAIN)
			break;
		if (ret != TR_OK)
			break;

		ret = tr_pipeline_listener_adopt_fd(listener, accepted_fd);
		if (ret != TR_OK)
			tr_socket_close(&accepted_fd);
		accepted++;
	}
}

int tr_pipeline_listener_create(
	const struct tr_pipeline_listener_config *config,
	struct tr_pipeline_listener **out)
{
	struct tr_pipeline_listener *listener;
	struct tr_pipeline_registry_config registry_config;
	uint64_t rx_slot_capacity64 = 0U;
	uint64_t rx_allocation_bytes = 0U;
	uint64_t job_allocation_bytes = 0U;
	int drain_wait_lock_ready = 0;
	int drain_wait_cond_ready = 0;
	int message_service_configured;
	int ret;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	if (!config || !config->owner || config->owner_shard_id == UINT32_MAX ||
	    (config->receive_compression_algorithms &
	     ~TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD) ||
	    (!config->receive_compression_algorithms &&
	     config->max_encoded_message_bytes != 0U) ||
	    (config->receive_compression_algorithms &&
	     config->max_encoded_message_bytes < 17U) ||
	    config->pipeline_capacity == 0U ||
	    config->connection_capacity == 0U ||
	    config->data_capacity_per_pipeline == 0U ||
	    config->stream_affinity_capacity_per_pipeline == 0U ||
	    config->control_message_count == 0U || !config->authorize_control ||
	    ((config->authorize_async == NULL) !=
	     (config->authorize_cancel == NULL)) ||
	    (config->authorize_async && (!config->authorization_operations ||
	                                 config->authorization_capacity == 0U)))
		return TR_ERR_INVALID;
	message_service_configured =
	        config->message_submit || config->message_cancel ||
	        config->message_context || config->message_budget ||
	        config->message_task_capacity;
	if (message_service_configured &&
	    (!config->receive_compression_algorithms ||
	     config->max_message_bytes == 0U ||
	     config->message_task_capacity == 0U || !config->message_submit ||
	     !config->message_cancel || !config->message_context ||
	     !config->message_budget))
		return TR_ERR_INVALID;
	if (config->message_task_capacity != 0U) {
		rx_slot_capacity64 =
		        (uint64_t)config->pipeline_capacity *
		        config->stream_affinity_capacity_per_pipeline;
		rx_allocation_bytes =
		        rx_slot_capacity64 *
		        sizeof(struct tr_pipeline_listener_rx_slot);
		job_allocation_bytes =
		        (uint64_t)config->message_task_capacity *
		        sizeof(struct tr_pipeline_listener_message_job);
		if (rx_slot_capacity64 == 0U ||
		    rx_slot_capacity64 > UINT32_MAX ||
		    rx_allocation_bytes > SIZE_MAX ||
		    job_allocation_bytes > SIZE_MAX)
			return TR_ERR_BAD_LENGTH;
	}

	listener = (struct tr_pipeline_listener *)calloc(
		1, sizeof(*listener));
	if (!listener)
		return TR_ERR_NOMEM;
	listener->listen_fd = -1;
	listener->retained_listen_fd = -1;
	listener->authorization_fd = -1;
	listener->config = *config;
	atomic_init(&listener->decoded_buffers_current, 0U);
	if (listener->config.handshake_timeout_ms == 0U)
		listener->config.handshake_timeout_ms = 10000U;
	if (config->message_task_capacity != 0U) {
		uint64_t rx_overhead = rx_slot_capacity64 *
		                       TR_GROUP_MESSAGE_RX_SLOT_OVERHEAD_BYTES;
		uint64_t job_overhead =
		        (uint64_t)config->message_task_capacity *
		        TR_GROUP_MESSAGE_JOB_SLOT_OVERHEAD_BYTES;

		if (UINT64_MAX - rx_overhead < job_overhead) {
			ret = TR_ERR_BAD_LENGTH;
			goto fail;
		}
		uint64_t state_bytes = rx_overhead + job_overhead;

		ret = tr_memory_budget_reserve(config->message_budget,
		                               state_bytes);
		if (ret != TR_OK)
			goto fail;
		listener->message_state_reserved_bytes = state_bytes;
		listener->rx_slot_capacity = (uint32_t)rx_slot_capacity64;
		listener->rx_slots = calloc(listener->rx_slot_capacity,
		                            sizeof(*listener->rx_slots));
		listener->message_jobs =
		        calloc(config->message_task_capacity,
		               sizeof(*listener->message_jobs));
		if (!listener->rx_slots || !listener->message_jobs) {
			ret = TR_ERR_NOMEM;
			goto fail;
		}
	}

	if (pthread_mutex_init(&listener->drain_wait_lock, NULL) != 0) {
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
		if (pthread_cond_init(&listener->drain_wait_cond, &attr) != 0) {
			pthread_condattr_destroy(&attr);
			ret = TR_ERR_SYS;
			goto fail;
		}
		pthread_condattr_destroy(&attr);
		drain_wait_cond_ready = 1;
	}

	listener->sessions = (struct tr_pipeline_listener_session *)calloc(
		config->pipeline_capacity, sizeof(*listener->sessions));
	listener->connections =
		(struct tr_pipeline_listener_connection *)calloc(
			config->connection_capacity,
			sizeof(*listener->connections));
	if (config->authorize_async)
		listener->authorizations =
		        (struct tr_pipeline_listener_authorization *)calloc(
		                config->authorization_capacity,
		                sizeof(*listener->authorizations));
	if (!listener->sessions || !listener->connections ||
	    (config->authorize_async && !listener->authorizations)) {
		ret = TR_ERR_NOMEM;
		goto fail;
	}

	memset(&registry_config, 0, sizeof(registry_config));
	registry_config.owner = config->owner;
	registry_config.owner_shard_id = config->owner_shard_id;
	registry_config.capacity = config->pipeline_capacity;
	ret = tr_pipeline_registry_create(
		&registry_config, &listener->registry);
	if (ret != TR_OK)
		goto fail;

	ret = tr_buffer_pool_init(
		&listener->message_pool, config->control_message_count,
		TR_PIPELINE_CONTROL_WIRE_SIZE);
	if (ret != TR_OK)
		goto fail;
	listener->message_pool_ready = 1;
	if (config->authorize_async) {
		listener->authorization_fd =
		        eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
		if (listener->authorization_fd < 0) {
			ret = TR_ERR_SYS;
			goto fail;
		}
		ret = tr_reactor_aux_event_register(
		        config->owner, listener->authorization_fd, EPOLLIN,
		        tr_pipeline_listener_authorization_ready, listener);
		if (ret != TR_OK)
			goto fail;
		listener->authorization_event_registered = 1;
	}

	*out = listener;
	return TR_OK;

fail:
	/* eventfd 注册之后没有其他构造步骤；失败回滚仍按就绪标志释放池与资源。
	 */
	if (listener->message_pool_ready)
		(void)tr_buffer_pool_destroy(&listener->message_pool);
	if (listener->registry)
		tr_pipeline_registry_destroy(listener->registry);
	if (listener->authorization_event_registered)
		(void)tr_reactor_aux_event_unregister(
		        config->owner, listener->authorization_fd);
	if (listener->authorization_fd >= 0)
		(void)close(listener->authorization_fd);
	free(listener->authorizations);
	free(listener->message_jobs);
	free(listener->rx_slots);
	free(listener->connections);
	free(listener->sessions);
	if (listener->message_state_reserved_bytes != 0U)
		(void)tr_memory_budget_release(
		        config->message_budget,
		        listener->message_state_reserved_bytes);
	if (drain_wait_cond_ready)
		pthread_cond_destroy(&listener->drain_wait_cond);
	if (drain_wait_lock_ready)
		pthread_mutex_destroy(&listener->drain_wait_lock);
	free(listener);
	return ret;
}

struct tr_pipeline_listener_listen_request {
	struct tr_pipeline_listener *listener;
	int fd;
	uint16_t bound_port;
};

static int tr_pipeline_listener_publish_listen(void *arg)
{
	struct tr_pipeline_listener_listen_request *request =
		(struct tr_pipeline_listener_listen_request *)arg;
	struct tr_pipeline_listener *listener = request->listener;

	/* 这个检查与事件源注册处于同一个反应器所有权事务。
	 * 不能在外部线程先读 listener_registered/listen_fd，
	 * 否则会重新引入生命周期数据竞争。
	 */
	if (listener->listen_fd >= 0 || listener->listener_registered)
		return TR_ERR_STATE;

	pthread_mutex_lock(&listener->drain_wait_lock);
	if (listener->drain_wait_closed) {
		pthread_mutex_unlock(&listener->drain_wait_lock);
		return TR_ERR_CLOSED;
	}
	pthread_mutex_unlock(&listener->drain_wait_lock);

	listener->listen_fd = request->fd;
	listener->bound_port = request->bound_port;
	listener->listener_registered = 1;
	listener->draining = 0;
	tr_pipeline_listener_publish_listening_on_owner(listener);
	return TR_OK;
}

static void tr_pipeline_listener_retain_failed_listen(void *arg)
{
	struct tr_pipeline_listener_listen_request *request =
		(struct tr_pipeline_listener_listen_request *)arg;
	struct tr_pipeline_listener *listener = request->listener;

	/* DEL 回滚失败时，等待后续排空跨过解绑屏障后再关闭该 fd。 */
	listener->retained_listen_fd = request->fd;
}

int tr_pipeline_listener_listen_ipv4(
	struct tr_pipeline_listener *listener, const char *address,
	uint16_t port, int backlog, uint16_t *out_bound_port)
{
	struct tr_pipeline_listener_listen_request request;
	int fd = -1;
	int retained = 0;
	uint16_t bound = 0U;
	int ret;

	if (!listener || !address || backlog <= 0)
		return TR_ERR_INVALID;

	ret = tr_tcp_listen_ipv4(
		address, port, backlog, &fd, &bound);
	if (ret != TR_OK)
		return ret;

	request.listener = listener;
	request.fd = fd;
	request.bound_port = bound;

	/* 事件源对 epoll 可见与监听器所有者状态发布必须是同一个
	 * 生命周期事务。函数成功返回前，回调已经能安全观察
	 * listen_fd/bound_port/listener_registered/draining 的完整状态。
	 */
	ret = tr_reactor_listener_register_publish(
		listener->config.owner, fd,
		tr_pipeline_listener_on_ready, listener,
		tr_pipeline_listener_publish_listen, &request,
		tr_pipeline_listener_retain_failed_listen, &request, &retained);
	if (ret != TR_OK) {
		if (!retained)
			tr_socket_close(&fd);
		return ret;
	}

	if (out_bound_port)
		*out_bound_port = bound;
	return TR_OK;
}

static int tr_pipeline_listener_publish_drained_admission(void *arg)
{
	struct tr_pipeline_listener *listener =
		(struct tr_pipeline_listener *)arg;
	int was_draining;
	int listen_fd;

	was_draining = listener->draining;
	listen_fd = listener->listen_fd;
	listener->listener_registered = 0;
	if (listener->retained_listen_fd >= 0 &&
	    listener->retained_listen_fd != listen_fd)
		tr_socket_close(&listener->retained_listen_fd);
	else
		listener->retained_listen_fd = -1;
	if (listen_fd >= 0) {
		tr_socket_close(&listener->listen_fd);
		listener->bound_port = 0U;
	}
	listener->draining = 1;
	if (!was_draining)
		tr_pipeline_listener_publish_drain_start_on_owner(listener);
	else
		tr_pipeline_listener_publish_drain_progress_on_owner(listener);
	return TR_OK;
}

static int tr_pipeline_listener_begin_drain_on_owner(void *arg)
{
	struct tr_pipeline_listener *listener =
		(struct tr_pipeline_listener *)arg;

	if (!listener)
		return TR_ERR_INVALID;

	/* 即使已经 draining 也经过反应器监听器所有权检查。事件源已
	 * 解绑时 unregister_call 仍执行发布，因此该操作天然幂等。
	 */
	return tr_reactor_listener_unregister_call(
		listener->config.owner, tr_pipeline_listener_on_ready, listener,
		tr_pipeline_listener_publish_drained_admission, listener);
}

int tr_pipeline_listener_begin_drain(struct tr_pipeline_listener *listener)
{
	if (!listener)
		return TR_ERR_INVALID;

	/* 运行中或已停止时，都只在反应器的串行化域读取和修改监听器
	 * 生命周期状态；外部线程不直接读取 listener_registered/listen_fd。
	 */
	return tr_reactor_listener_unregister_call(
		listener->config.owner, tr_pipeline_listener_on_ready, listener,
		tr_pipeline_listener_publish_drained_admission, listener);
}

int tr_pipeline_listener_wait_drained(
	struct tr_pipeline_listener *listener, uint32_t timeout_ms)
{
	struct tr_deadline deadline;
	uint64_t generation;
	int timed = timeout_ms != 0U;
	int result = TR_OK;

	if (!listener)
		return TR_ERR_INVALID;
	if (tr_reactor_in_owner_context())
		return TR_ERR_STATE;

	/* 0ms 仍表示无限等待；有限值统一使用 time_t 安全的绝对截止时间。 */
	if (timed) {
		int ret = tr_deadline_init_ms(&deadline, timeout_ms);

		if (ret != TR_OK)
			return ret;
	}

	pthread_mutex_lock(&listener->drain_wait_lock);
	if (listener->drain_wait_closed) {
		pthread_mutex_unlock(&listener->drain_wait_lock);
		return TR_ERR_CLOSED;
	}
	if (!listener->drain_wait_active || listener->drain_generation == 0U) {
		pthread_mutex_unlock(&listener->drain_wait_lock);
		return TR_ERR_STATE;
	}

	generation = listener->drain_generation;
	if (listener->drained_generation == generation) {
		pthread_mutex_unlock(&listener->drain_wait_lock);
		return TR_OK;
	}
	if (listener->drain_waiters == UINT32_MAX) {
		pthread_mutex_unlock(&listener->drain_wait_lock);
		return TR_ERR_STATE;
	}
	listener->drain_waiters++;

	while (listener->drained_generation != generation) {
		int ret;

		if (listener->drain_wait_closed) {
			result = TR_ERR_CLOSED;
			break;
		}
		if (!listener->drain_wait_active ||
		    listener->drain_generation != generation) {
			result = TR_ERR_STALE;
			break;
		}

		if (timed)
			ret = pthread_cond_timedwait(
				&listener->drain_wait_cond,
				&listener->drain_wait_lock, &deadline.absolute);
		else
			ret = pthread_cond_wait(
				&listener->drain_wait_cond,
				&listener->drain_wait_lock);
		if (ret == 0)
			continue;
		if (timed && ret == ETIMEDOUT) {
			result = TR_ERR_TIMEOUT;
			break;
		}
		result = TR_ERR_SYS;
		break;
	}

	assert(listener->drain_waiters != 0U);
	listener->drain_waiters--;
	pthread_mutex_unlock(&listener->drain_wait_lock);
	return result;
}

struct tr_pipeline_listener_stop_request {
	struct tr_pipeline_listener *listener;
};

static int tr_pipeline_listener_stop_on_owner(void *arg)
{
	struct tr_pipeline_listener_stop_request *request =
		(struct tr_pipeline_listener_stop_request *)arg;
	struct tr_pipeline_listener *listener = request->listener;
	uint32_t i;
	int result;

	/* 停止的整个状态转换都由所有者串行化：先关闭监听器准入，
	 * 再关闭已有连接与传输对象，最后在同一个所有者处理轮次检查计数是否归零。
	 * 外部线程不直接观察仅限所有者访问的计数器。
	 */
	result = tr_pipeline_listener_begin_drain_on_owner(listener);

	for (i = 0; i < listener->config.connection_capacity; ++i) {
		struct tr_pipeline_listener_connection *tracked =
			&listener->connections[i];
		struct tr_conn_handle handle;
		int ret;

		if (!tracked->used || !tracked->handle.reactor)
			continue;
		handle = tracked->handle;
		ret = tr_reactor_close_on_owner(handle);
		if (ret != TR_OK && ret != TR_ERR_STALE && result == TR_OK)
			result = ret;
	}
	for (i = 0; i < listener->rx_slot_capacity; ++i)
		if (listener->rx_slots[i].used)
			tr_pipeline_listener_rx_clear(
			        listener, &listener->rx_slots[i], 1);
	for (i = 0; i < listener->config.message_task_capacity; ++i) {
		struct tr_pipeline_listener_message_job *job =
		        &listener->message_jobs[i];
		int ret;

		if (!job->used)
			continue;
		job->cancelled = 1;
		if (!job->submitted) {
			tr_pipeline_listener_job_clear(job);
			continue;
		}
		ret = listener->config.message_cancel(
		        listener->config.message_context, job->task);
		if (ret != TR_OK && ret != TR_ERR_STATE &&
		    ret != TR_ERR_STALE && result == TR_OK)
			result = ret;
	}
	/* close_on_owner 会同步触发 preface release 和 provider cancel。
	 * 先消费其中已经确认的结果，再判断排空是否仍有 provider 引用。 */
	tr_pipeline_listener_authorization_scan_on_owner(listener);

	/* 连接关闭回调通常会完成 CONTROL 致命拆除。若某次
	 * 拆除在关闭屏障之后返回错误，连接槽位可能已经清空，
	 * 但会话仍保留传输对象。stop() 必须显式重试这些残留会话，
	 * 不能仅依赖 connections_current 判断是否还需要所有者侧清理。
	 */
	for (i = 0; i < listener->config.pipeline_capacity; ++i) {
		struct tr_pipeline_listener_session *session =
			&listener->sessions[i];
		int ret;

		if (!session->used || !session->transport)
			continue;
		ret = tr_pipeline_control_transport_abort(session->transport);
		if (ret != TR_OK && result == TR_OK)
			result = ret;
	}

	if (listener->authorizations_current != 0U)
		return result == TR_OK ? TR_AGAIN : result;
	if (listener->connections_current != 0U ||
	    listener->pipelines_current != 0U ||
	    listener->message_jobs_current != 0U ||
	    listener->rx_messages_current != 0U)
		return result == TR_OK ? TR_ERR_STATE : result;
	return result;
}

static int tr_pipeline_listener_stop_stopped(void *arg)
{
	struct tr_pipeline_listener *listener =
		(struct tr_pipeline_listener *)arg;
	int ret;

	ret = tr_pipeline_listener_publish_drained_admission(listener);
	if (ret != TR_OK)
		return ret;
	tr_pipeline_listener_authorization_scan_on_owner(listener);
	if (listener->authorizations_current != 0U)
		return TR_AGAIN;
	if (listener->connections_current != 0U ||
	    listener->pipelines_current != 0U ||
	    listener->message_jobs_current != 0U ||
	    listener->rx_messages_current != 0U)
		return TR_ERR_STATE;
	return TR_OK;
}

int tr_pipeline_listener_stop(struct tr_pipeline_listener *listener)
{
	struct tr_pipeline_listener_stop_request request;
	int ret;

	if (!listener)
		return TR_ERR_INVALID;

	request.listener = listener;
	ret = tr_reactor_call(
		listener->config.owner,
		tr_pipeline_listener_stop_on_owner, &request);
	if (ret != TR_ERR_CLOSED)
		return ret;

	/* tr_reactor_call() 的 CLOSED 同时覆盖“未启动/已停止”和“正在停止”。
	 * unregister_call 只允许前者进入 ctl_lock 直接路径；正在停止时继续
	 * 返回 CLOSED，绝不越过所有者拆除屏障。解绑、状态发布和
	 * 计数器检查在同一串行化区间完成。
	 */
	return tr_reactor_listener_unregister_call(
		listener->config.owner, tr_pipeline_listener_on_ready, listener,
		tr_pipeline_listener_stop_stopped, listener);
}

static int tr_pipeline_listener_verify_destroy(void *arg)
{
	struct tr_pipeline_listener *listener =
		(struct tr_pipeline_listener *)arg;

	if (!listener)
		return TR_ERR_INVALID;
	if (listener->listener_registered || listener->listen_fd >= 0 ||
	    listener->retained_listen_fd >= 0 ||
	    listener->connections_current != 0U ||
	    listener->pipelines_current != 0U ||
	    listener->message_jobs_current != 0U ||
	    listener->rx_messages_current != 0U ||
	    atomic_load_explicit(&listener->decoded_buffers_current,
	                         memory_order_acquire) != 0U)
		return TR_ERR_STATE;
	if (listener->authorizations_current != 0U)
		return TR_AGAIN;

	return tr_pipeline_listener_close_wait_admission(listener);
}

int tr_pipeline_listener_destroy(struct tr_pipeline_listener *listener)
{
	int ret;

	if (!listener)
		return TR_OK;

	/* 在监听器与事件源拆除所用的同一串行化域验证仅限所有者访问的生命周期状态。
	 * 完全停止的反应器使用 ctl_lock 直接路径；停止进行中则返回 CLOSED，
	 * 而不是允许释放与所有者竞争。
	 */
	ret = tr_reactor_call_or_stopped(
		listener->config.owner,
		tr_pipeline_listener_verify_destroy, listener);
	/* verify_destroy 是可恢复的终止屏障：仍有监听器、会话或等待者
	 * 时必须把错误返回给调用方，而不是在调试构建先中止。
	 */
	if (ret != TR_OK)
		return ret;

	/* verify_destroy 已在所有者上关闭准入；先解绑定时器才能释放回调参数。
	 */
	if (listener->handshake_timer_registered) {
		ret = tr_reactor_timer_unregister(listener->handshake_timer);
		if (ret != TR_OK)
			return ret;
		listener->handshake_timer_registered = 0;
	}
	if (listener->authorization_event_registered) {
		ret = tr_reactor_aux_event_unregister(
		        listener->config.owner, listener->authorization_fd);
		if (ret != TR_OK)
			return ret;
		listener->authorization_event_registered = 0;
	}
	if (listener->authorization_fd >= 0) {
		if (close(listener->authorization_fd) != 0)
			return TR_ERR_SYS;
		listener->authorization_fd = -1;
	}
	if (listener->message_pool_ready) {
		ret = tr_buffer_pool_destroy(&listener->message_pool);
		if (ret != TR_OK)
			return ret;
		listener->message_pool_ready = 0;
	}
	if (listener->message_state_reserved_bytes != 0U) {
		ret = tr_memory_budget_release(
		        listener->config.message_budget,
		        listener->message_state_reserved_bytes);
		if (ret != TR_OK)
			return ret;
		listener->message_state_reserved_bytes = 0U;
	}
	if (listener->registry)
		tr_pipeline_registry_destroy(listener->registry);
	free(listener->authorizations);
	free(listener->message_jobs);
	free(listener->rx_slots);
	free(listener->connections);
	free(listener->sessions);
	(void)pthread_cond_destroy(&listener->drain_wait_cond);
	pthread_mutex_destroy(&listener->drain_wait_lock);
	free(listener);
	return TR_OK;
}

uint16_t tr_pipeline_listener_bound_port(
	const struct tr_pipeline_listener *listener)
{
	return listener ? listener->bound_port : 0U;
}

struct tr_pipeline_listener_offer_request {
	struct tr_pipeline_listener *listener;
	uint64_t pipeline_id;
	uint64_t epoch;
	uint64_t message_id;
	struct tr_pipeline_route_preface *route_out;
};

static int tr_pipeline_listener_send_offer_on_owner(void *arg)
{
	struct tr_pipeline_listener_offer_request *request =
		(struct tr_pipeline_listener_offer_request *)arg;
	struct tr_pipeline_listener_session *session;

	if (request->listener->draining)
		return TR_ERR_CLOSED;
	session =
		tr_pipeline_listener_session_find(
			request->listener, request->pipeline_id, request->epoch);

	if (!session || !session->transport)
		return TR_ERR_STALE;
	return tr_pipeline_control_transport_send_data_offer(
		session->transport, request->message_id, request->route_out);
}

int tr_pipeline_listener_send_data_offer(
	struct tr_pipeline_listener *listener, uint64_t pipeline_id,
	uint64_t epoch, uint64_t message_id,
	struct tr_pipeline_route_preface *route_out)
{
	struct tr_pipeline_listener_offer_request request;

	if (!listener || pipeline_id == 0U || epoch == 0U)
		return TR_ERR_INVALID;
	if (route_out)
		memset(route_out, 0, sizeof(*route_out));
	request.listener = listener;
	request.pipeline_id = pipeline_id;
	request.epoch = epoch;
	request.message_id = message_id;
	request.route_out = route_out;
	return tr_reactor_call(
		listener->config.owner,
		tr_pipeline_listener_send_offer_on_owner, &request);
}

struct tr_pipeline_listener_transfer_request {
	struct tr_pipeline_listener *listener;
	uint64_t pipeline_id;
	uint64_t epoch;
	uint32_t stream_id;
	uint64_t message_id;
};

static int tr_pipeline_listener_send_ready_on_owner(void *arg)
{
	struct tr_pipeline_listener_transfer_request *request =
		(struct tr_pipeline_listener_transfer_request *)arg;
	struct tr_pipeline_listener_session *session;

	if (request->listener->draining)
		return TR_ERR_CLOSED;
	session =
		tr_pipeline_listener_session_find(
			request->listener, request->pipeline_id, request->epoch);

	if (!session || !session->transport)
		return TR_ERR_STALE;
	return tr_pipeline_control_transport_send_transfer_ready(
		session->transport, request->stream_id, request->message_id);
}

int tr_pipeline_listener_send_transfer_ready(
	struct tr_pipeline_listener *listener, uint64_t pipeline_id,
	uint64_t epoch, uint32_t stream_id, uint64_t message_id)
{
	struct tr_pipeline_listener_transfer_request request;

	if (!listener || pipeline_id == 0U || epoch == 0U ||
	    stream_id == 0U)
		return TR_ERR_INVALID;
	request.listener = listener;
	request.pipeline_id = pipeline_id;
	request.epoch = epoch;
	request.stream_id = stream_id;
	request.message_id = message_id;
	return tr_reactor_call(
		listener->config.owner,
		tr_pipeline_listener_send_ready_on_owner, &request);
}

static int tr_pipeline_listener_release_transfer_on_owner(void *arg)
{
	struct tr_pipeline_listener_transfer_request *request =
		(struct tr_pipeline_listener_transfer_request *)arg;
	struct tr_pipeline_listener_session *session =
		tr_pipeline_listener_session_find(
			request->listener, request->pipeline_id, request->epoch);

	if (!session || !session->transport)
		return TR_ERR_STALE;
	return tr_pipeline_control_transport_release_transfer(
		session->transport, request->stream_id);
}

int tr_pipeline_listener_release_transfer(
	struct tr_pipeline_listener *listener, uint64_t pipeline_id,
	uint64_t epoch, uint32_t stream_id)
{
	struct tr_pipeline_listener_transfer_request request;

	if (!listener || pipeline_id == 0U || epoch == 0U ||
	    stream_id == 0U)
		return TR_ERR_INVALID;
	memset(&request, 0, sizeof(request));
	request.listener = listener;
	request.pipeline_id = pipeline_id;
	request.epoch = epoch;
	request.stream_id = stream_id;
	return tr_reactor_call(
		listener->config.owner,
		tr_pipeline_listener_release_transfer_on_owner, &request);
}

struct tr_pipeline_listener_stats_request {
	struct tr_pipeline_listener *listener;
	struct tr_pipeline_listener_stats *out;
};

static int tr_pipeline_listener_stats_on_owner(void *arg)
{
	struct tr_pipeline_listener_stats_request *request =
		(struct tr_pipeline_listener_stats_request *)arg;
	struct tr_pipeline_listener *listener = request->listener;

	memset(request->out, 0, sizeof(*request->out));
	request->out->pipeline_capacity =
		listener->config.pipeline_capacity;
	request->out->pipelines_current = listener->pipelines_current;
	request->out->pipelines_peak = listener->pipelines_peak;
	request->out->connection_capacity =
		listener->config.connection_capacity;
	request->out->connections_current = listener->connections_current;
	request->out->connections_peak = listener->connections_peak;
	request->out->draining = listener->draining ? 1U : 0U;
	{
		uint32_t i;

		for (i = 0; i < listener->config.connection_capacity; ++i)
			if (listener->connections[i].used &&
			    listener->connections[i].role ==
				    TR_PIPELINE_LISTENER_CONN_DATA)
				request->out->data_connections_current++;

		for (i = 0; i < listener->config.pipeline_capacity; ++i) {
			struct tr_pipeline_listener_session *session =
				&listener->sessions[i];
			struct tr_pipeline_stats stats;
			int ret;

			if (!session->used || session->closing ||
			    !session->transport)
				continue;
			memset(&stats, 0, sizeof(stats));
			ret = tr_pipeline_control_transport_get_stats(
				session->transport, &stats);
			if (ret == TR_OK)
				request->out->active_transfers +=
					stats.stream_affinity_count;
		}
	}
	request->out->control_accepts = listener->control_accepts;
	request->out->data_accepts = listener->data_accepts;
	request->out->route_rejections = listener->route_rejections;
	request->out->capacity_rejections = listener->capacity_rejections;
	return TR_OK;
}

int tr_pipeline_listener_get_stats(
	struct tr_pipeline_listener *listener,
	struct tr_pipeline_listener_stats *out)
{
	struct tr_pipeline_listener_stats_request request;

	if (!listener || !out)
		return TR_ERR_INVALID;
	request.listener = listener;
	request.out = out;
	return tr_reactor_call(
		listener->config.owner,
		tr_pipeline_listener_stats_on_owner, &request);
}
