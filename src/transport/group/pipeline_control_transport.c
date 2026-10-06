#include "pipeline_control_transport_internal.h"

#include <stdlib.h>
#include <string.h>

#include "../../group/pipeline_control_wire_internal.h"
#include "../../execution/reactor_internal.h"
#include "tr/status.h"
#include "../protocol/wire.h"

struct tr_pipeline_control_transport {
	struct tr_pipeline_control *control;
	struct tr_conn_handle connection;
	struct tr_buffer_pool *message_pool;
	struct tr_pipeline_route_preface control_route;
	tr_pipeline_control_transport_closing_cb closing_cb;
	tr_pipeline_control_transport_closed_cb closed_cb;
	void *closed_arg;
	int closing;
};

static int tr_pipeline_control_transport_route_valid(
	const struct tr_pipeline_control_transport_config *config)
{
	const struct tr_pipeline_route_preface *route;

	if (!config)
		return 0;
	route = &config->control_route;
	if (tr_pipeline_route_preface_validate_fields(route) != TR_OK)
		return 0;
	return route->role == TR_PIPELINE_ROUTE_CONTROL &&
	       route->owner_shard_id != UINT32_MAX &&
	       route->pipeline_id != 0U && route->epoch != 0U &&
	       config->connection.reactor != NULL;
}

static int tr_pipeline_control_transport_message_matches(
	const struct tr_pipeline_control_transport *transport,
	const struct tr_pipeline_control_wire_message *message)
{
	return message->owner_shard_id ==
		       transport->control_route.owner_shard_id &&
	       message->pipeline_id == transport->control_route.pipeline_id &&
	       message->epoch == transport->control_route.epoch;
}

static enum tr_frame_disposition tr_pipeline_control_transport_frame(
	struct tr_conn_handle connection, struct tr_frame *frame, void *arg)
{
	struct tr_pipeline_control_transport *transport =
		(struct tr_pipeline_control_transport *)arg;
	struct tr_pipeline_control_wire_message message;
	int ret;

	if (!transport || transport->closing)
		return TR_FRAME_RELEASE;

	if (!frame || frame->header.type != TR_FRAME_PIPELINE_CONTROL ||
	    frame->header.flags != 0U || frame->header.stream_id != 0U ||
	    !frame->payload ||
	    frame->payload->len != TR_PIPELINE_CONTROL_WIRE_SIZE) {
		(void)tr_reactor_abort_on_owner(connection, TR_ERR_BAD_TYPE);
		return TR_FRAME_RELEASE;
	}

	memset(&message, 0, sizeof(message));
	ret = tr_pipeline_control_wire_decode(
		frame->payload->data, frame->payload->len, &message);
	if (ret != TR_OK ||
	    !tr_pipeline_control_transport_message_matches(
		    transport, &message) ||
	    message.type != TR_PIPELINE_CONTROL_DATA_CANCEL) {
		if (ret == TR_OK)
			ret = message.type == TR_PIPELINE_CONTROL_DATA_CANCEL ?
				      TR_ERR_STALE : TR_ERR_BAD_TYPE;
		(void)tr_reactor_abort_on_owner(connection, ret);
		return TR_FRAME_RELEASE;
	}

	ret = tr_pipeline_control_cancel_data_wire(
		transport->control, frame->payload->data, frame->payload->len);
	if (ret != TR_OK) {
		(void)tr_reactor_abort_on_owner(connection, ret);
		return TR_FRAME_RELEASE;
	}

	return TR_FRAME_RELEASE;
}

static int tr_pipeline_control_transport_abort_on_owner(void *arg)
{
	struct tr_pipeline_control_transport *transport =
		(struct tr_pipeline_control_transport *)arg;
	tr_pipeline_control_transport_closed_cb closed_cb;
	void *closed_arg;
	uint64_t pipeline_id;
	uint64_t epoch;
	int teardown_status;

	if (!transport || !transport->control)
		return TR_ERR_INVALID;

	pipeline_id = transport->control_route.pipeline_id;
	epoch = transport->control_route.epoch;
	closed_cb = transport->closed_cb;
	closed_arg = transport->closed_arg;

	/*
	 * closing 是 admission fence，不代表 teardown 已经完成。第一次进入时先
	 * 禁止新的 CONTROL work；若之前的 abort 因内部状态暂时未收敛而失败，
	 * listener stop 可以再次调用本函数完成剩余 teardown。
	 */
	if (!transport->closing) {
		transport->closing = 1;
		if (transport->closing_cb)
			transport->closing_cb(closed_arg);
	}

	teardown_status = tr_pipeline_control_abort(
		transport->control, transport->connection);
	if (teardown_status != TR_OK) {
		if (closed_cb)
			closed_cb(
				pipeline_id, epoch, teardown_status, closed_arg);
		return teardown_status;
	}
	transport->control = NULL;

	if (closed_cb)
		closed_cb(pipeline_id, epoch, TR_OK, closed_arg);
	free(transport);
	return TR_OK;
}

static void tr_pipeline_control_transport_event(
	struct tr_conn_handle connection, enum tr_connection_event event,
	int status, void *arg)
{
	struct tr_pipeline_control_transport *transport =
		(struct tr_pipeline_control_transport *)arg;

	(void)connection;
	(void)event;
	(void)status;
	if (!transport)
		return;

	(void)tr_pipeline_control_transport_abort_on_owner(transport);
}

int tr_pipeline_control_transport_create(
	const struct tr_pipeline_control_transport_config *config,
	struct tr_pipeline_control_transport **out)
{
	struct tr_pipeline_control_transport *transport;
	int ret;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	if (!config || !config->control || !config->message_pool ||
	    !tr_pipeline_control_transport_route_valid(config))
		return TR_ERR_INVALID;

	transport = (struct tr_pipeline_control_transport *)calloc(
		1, sizeof(*transport));
	if (!transport)
		return TR_ERR_NOMEM;

	transport->control = config->control;
	transport->connection = config->connection;
	transport->message_pool = config->message_pool;
	transport->control_route = config->control_route;
	transport->closing_cb = config->closing_cb;
	transport->closed_cb = config->closed_cb;
	transport->closed_arg = config->closed_arg;

	ret = tr_reactor_set_handler(
		config->connection, tr_pipeline_control_transport_frame,
		tr_pipeline_control_transport_event, transport);
	if (ret != TR_OK) {
		free(transport);
		return ret;
	}

	*out = transport;
	return TR_OK;
}

static int tr_pipeline_control_transport_send_wire(
	struct tr_pipeline_control_transport *transport,
	struct tr_buffer *buffer, uint64_t message_id)
{
	int ret;

	ret = tr_reactor_send(
		transport->connection, TR_FRAME_PIPELINE_CONTROL, 0U, 0U,
		message_id, buffer);
	return ret;
}

struct tr_pipeline_control_transport_offer_request {
	struct tr_pipeline_control_transport *transport;
	uint64_t message_id;
	struct tr_pipeline_route_preface *route_out;
};

static int tr_pipeline_control_transport_send_data_offer_on_owner(void *arg)
{
	struct tr_pipeline_control_transport_offer_request *request =
		(struct tr_pipeline_control_transport_offer_request *)arg;
	struct tr_pipeline_control_transport *transport = request->transport;
	struct tr_pipeline_data_offer offer;
	struct tr_buffer *buffer = NULL;
	int rollback_ret;
	int ret;

	if (!transport->control || transport->closing)
		return TR_ERR_STATE;
	memset(&offer, 0, sizeof(offer));

	ret = tr_buffer_acquire(
		transport->message_pool, TR_PIPELINE_CONTROL_WIRE_SIZE, &buffer);
	if (ret != TR_OK)
		return ret;

	/*
	 * reserve_data_wire_ex() returns the exact capability created by the same
	 * owner transaction. No encode->decode round trip is needed to reconstruct
	 * rollback identity.
	 */
	ret = tr_pipeline_control_reserve_data_wire_ex(
		transport->control, &offer, buffer->data);
	if (ret != TR_OK)
		goto fail;
	buffer->len = TR_PIPELINE_CONTROL_WIRE_SIZE;

	ret = tr_pipeline_control_transport_send_wire(
		transport, buffer, request->message_id);
	if (ret != TR_OK) {
		/*
		 * SEND admission and reservation rollback execute in the same owner turn.
		 * A failed send never transfers buffer ownership, so both capability and
		 * buffer remain locally recoverable here.
		 */
		rollback_ret = tr_pipeline_control_cancel_data(
			transport->control, &offer);
		if (rollback_ret != TR_OK)
			ret = rollback_ret;
		goto fail;
	}

	if (request->route_out)
		*request->route_out = offer.route;
	return TR_OK;

fail:
	tr_buffer_release(buffer);
	return ret;
}

int tr_pipeline_control_transport_send_data_offer(
	struct tr_pipeline_control_transport *transport, uint64_t message_id,
	struct tr_pipeline_route_preface *route_out)
{
	struct tr_pipeline_control_transport_offer_request request;

	if (!transport || !transport->connection.reactor)
		return TR_ERR_INVALID;
	if (route_out)
		memset(route_out, 0, sizeof(*route_out));

	request.transport = transport;
	request.message_id = message_id;
	request.route_out = route_out;
	return tr_reactor_call(
		transport->connection.reactor,
		tr_pipeline_control_transport_send_data_offer_on_owner, &request);
}

struct tr_pipeline_control_transport_transfer_request {
	struct tr_pipeline_control_transport *transport;
	uint32_t stream_id;
	uint64_t message_id;
};

static int tr_pipeline_control_transport_send_transfer_ready_on_owner(void *arg)
{
	struct tr_pipeline_control_transport_transfer_request *request =
		(struct tr_pipeline_control_transport_transfer_request *)arg;
	struct tr_pipeline_control_transport *transport = request->transport;
	struct tr_buffer *buffer = NULL;
	int rollback_ret;
	int ret;

	if (!transport->control || transport->closing ||
	    request->stream_id == 0U)
		return TR_ERR_INVALID;

	ret = tr_buffer_acquire(
		transport->message_pool, TR_PIPELINE_CONTROL_WIRE_SIZE, &buffer);
	if (ret != TR_OK)
		return ret;

	ret = tr_pipeline_control_prepare_transfer_wire(
		transport->control, request->stream_id, buffer->data);
	if (ret != TR_OK)
		goto fail;
	buffer->len = TR_PIPELINE_CONTROL_WIRE_SIZE;

	ret = tr_pipeline_control_transport_send_wire(
		transport, buffer, request->message_id);
	if (ret != TR_OK) {
		rollback_ret = tr_pipeline_control_release_transfer(
			transport->control, request->stream_id);
		if (rollback_ret != TR_OK)
			ret = rollback_ret;
		goto fail;
	}

	return TR_OK;

fail:
	tr_buffer_release(buffer);
	return ret;
}

int tr_pipeline_control_transport_send_transfer_ready(
	struct tr_pipeline_control_transport *transport, uint32_t stream_id,
	uint64_t message_id)
{
	struct tr_pipeline_control_transport_transfer_request request;

	if (!transport || !transport->connection.reactor || stream_id == 0U)
		return TR_ERR_INVALID;

	request.transport = transport;
	request.stream_id = stream_id;
	request.message_id = message_id;
	return tr_reactor_call(
		transport->connection.reactor,
		tr_pipeline_control_transport_send_transfer_ready_on_owner,
		&request);
}

static int tr_pipeline_control_transport_release_transfer_on_owner(void *arg)
{
	struct tr_pipeline_control_transport_transfer_request *request =
		(struct tr_pipeline_control_transport_transfer_request *)arg;
	struct tr_pipeline_control_transport *transport = request->transport;

	if (!transport->control || transport->closing)
		return TR_ERR_STATE;
	return tr_pipeline_control_release_transfer(
		transport->control, request->stream_id);
}

int tr_pipeline_control_transport_release_transfer(
	struct tr_pipeline_control_transport *transport, uint32_t stream_id)
{
	struct tr_pipeline_control_transport_transfer_request request;

	if (!transport || !transport->connection.reactor || stream_id == 0U)
		return TR_ERR_INVALID;

	memset(&request, 0, sizeof(request));
	request.transport = transport;
	request.stream_id = stream_id;
	return tr_reactor_call(
		transport->connection.reactor,
		tr_pipeline_control_transport_release_transfer_on_owner,
		&request);
}

int tr_pipeline_control_transport_abort(
	struct tr_pipeline_control_transport *transport)
{
	if (!transport || !transport->connection.reactor)
		return TR_ERR_INVALID;

	return tr_reactor_call(
		transport->connection.reactor,
		tr_pipeline_control_transport_abort_on_owner, transport);
}

int tr_pipeline_control_transport_get_stats(
	struct tr_pipeline_control_transport *transport,
	struct tr_pipeline_stats *out)
{
	if (!transport || !transport->control || !out)
		return TR_ERR_INVALID;
	if (transport->closing)
		return TR_ERR_STATE;
	return tr_pipeline_control_get_stats(
		transport->control, out);
}

uint64_t tr_pipeline_control_transport_pipeline_id(
	const struct tr_pipeline_control_transport *transport)
{
	return transport ? transport->control_route.pipeline_id : 0U;
}

uint64_t tr_pipeline_control_transport_epoch(
	const struct tr_pipeline_control_transport *transport)
{
	return transport ? transport->control_route.epoch : 0U;
}
