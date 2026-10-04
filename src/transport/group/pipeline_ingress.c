#include "pipeline_ingress_internal.h"

#include <stdlib.h>
#include <string.h>

#include "../../group/pipeline_route_internal.h"
#include "../../reactor_internal.h"
#include "tr/status.h"

struct tr_pipeline_ingress_member {
	struct tr_pipeline_registry *registry;
	struct tr_pipeline *pipeline;
	struct tr_pipeline_route_preface preface;
	struct tr_pipeline_data_ref data;
	tr_reactor_frame_cb frame_cb;
	tr_reactor_event_cb event_cb;
	void *callback_arg;
};

struct tr_pipeline_ingress_preface {
	struct tr_pipeline_registry *registry;
	struct tr_pipeline_route_parser parser;
	tr_reactor_frame_cb frame_cb;
	tr_reactor_event_cb event_cb;
	void *callback_arg;
};

static enum tr_frame_disposition tr_pipeline_ingress_frame(
	struct tr_conn_handle connection, struct tr_frame *frame, void *arg)
{
	struct tr_pipeline_ingress_member *member =
		(struct tr_pipeline_ingress_member *)arg;

	if (!member)
		return TR_FRAME_RELEASE;

	if (frame && frame->header.type == TR_FRAME_DATA) {
		struct tr_pipeline_data_ref data;
		struct tr_conn_handle bound;
		int ret;

		memset(&data, 0, sizeof(data));
		memset(&bound, 0, sizeof(bound));
		ret = tr_pipeline_stream_data(
			member->pipeline, frame->header.stream_id,
			&data, &bound);
		if (ret != TR_OK ||
		    data.index != member->data.index ||
		    data.generation != member->data.generation ||
		    bound.reactor != connection.reactor ||
		    bound.slot != connection.slot ||
		    bound.generation != connection.generation) {
			if (ret == TR_OK)
				ret = TR_ERR_STALE;
			(void)tr_reactor_abort_on_owner(
				connection, ret < 0 ? ret : TR_ERR_STATE);
			return TR_FRAME_RELEASE;
		}
	}

	if (!member->frame_cb)
		return TR_FRAME_RELEASE;
	return member->frame_cb(connection, frame, member->callback_arg);
}

static void tr_pipeline_ingress_event(
	struct tr_conn_handle connection, enum tr_connection_event event,
	int status, void *arg)
{
	struct tr_pipeline_ingress_member *member =
		(struct tr_pipeline_ingress_member *)arg;
	tr_reactor_event_cb event_cb = NULL;
	void *callback_arg = NULL;

	if (!member)
		return;

	/*
	 * Connection close/error is the owner-side membership retirement point.
	 * Exact capability+connection matching prevents a stale close callback
	 * from detaching a replacement DATA membership.
	 */
	(void)tr_pipeline_registry_detach_data_route(
		member->registry, &member->preface, connection);
	event_cb = member->event_cb;
	callback_arg = member->callback_arg;
	free(member);

	if (event_cb)
		event_cb(connection, event, status, callback_arg);
}

int tr_pipeline_ingress_attach_data_route_on_owner(
	const struct tr_pipeline_ingress_config *config,
	const struct tr_pipeline_route_preface *preface,
	struct tr_conn_handle connection)
{
	struct tr_pipeline_ingress_member *member;
	struct tr_pipeline_data_ref attached;
	int ret;

	if (!config || !config->registry || !preface ||
	    connection.reactor != tr_pipeline_registry_owner(config->registry))
		return TR_ERR_INVALID;

	member = (struct tr_pipeline_ingress_member *)calloc(
		1, sizeof(*member));
	if (!member)
		return TR_ERR_NOMEM;

	memset(&attached, 0, sizeof(attached));
	ret = tr_pipeline_registry_attach_data_route_local_on_owner(
		config->registry, preface, connection, &attached,
		&member->pipeline);
	if (ret != TR_OK) {
		free(member);
		return ret;
	}

	member->registry = config->registry;
	member->preface = *preface;
	member->data = attached;
	member->frame_cb = config->frame_cb;
	member->event_cb = config->event_cb;
	member->callback_arg = config->callback_arg;

	ret = tr_reactor_set_handler(
		connection, tr_pipeline_ingress_frame,
		tr_pipeline_ingress_event, member);
	if (ret != TR_OK) {
		(void)tr_pipeline_registry_detach_data_route(
			config->registry, preface, connection);
		free(member);
		return ret;
	}

	return TR_OK;
}

static int tr_pipeline_ingress_preface_feed(
	struct tr_conn_handle connection, const uint8_t *data, size_t len,
	int *done, void *arg)
{
	struct tr_pipeline_ingress_preface *ingress =
		(struct tr_pipeline_ingress_preface *)arg;
	struct tr_pipeline_ingress_config config;
	struct tr_pipeline_route_preface preface;
	size_t consumed = 0U;
	int ready = 0;
	int ret;

	if (!ingress || !done)
		return TR_ERR_INVALID;
	*done = 0;

	memset(&preface, 0, sizeof(preface));
	ret = tr_pipeline_route_parser_feed(
		&ingress->parser, data, len, &consumed, &preface, &ready);
	if (ret != TR_OK)
		return ret;
	if (consumed != len)
		return TR_ERR_STATE;
	if (!ready)
		return TR_OK;

	memset(&config, 0, sizeof(config));
	config.registry = ingress->registry;
	config.frame_cb = ingress->frame_cb;
	config.event_cb = ingress->event_cb;
	config.callback_arg = ingress->callback_arg;
	ret = tr_pipeline_ingress_attach_data_route_on_owner(
		&config, &preface, connection);
	if (ret != TR_OK)
		return ret;

	*done = 1;
	return TR_OK;
}

static void tr_pipeline_ingress_preface_release(void *arg)
{
	struct tr_pipeline_ingress_preface *ingress =
		(struct tr_pipeline_ingress_preface *)arg;

	if (!ingress)
		return;
	free(ingress);
}

int tr_pipeline_ingress_adopt_data_fd_on_owner(
	const struct tr_pipeline_ingress_config *config, int fd,
	struct tr_conn_handle *out)
{
	struct tr_pipeline_ingress_preface *ingress;
	struct tr_reactor_preface_handler preface;
	struct tr_reactor *owner;
	int ret;

	if (!config || !config->registry || fd < 0 || !out)
		return TR_ERR_INVALID;

	owner = tr_pipeline_registry_owner(config->registry);
	if (!owner)
		return TR_ERR_INVALID;

	ingress = (struct tr_pipeline_ingress_preface *)calloc(
		1, sizeof(*ingress));
	if (!ingress)
		return TR_ERR_NOMEM;

	ingress->registry = config->registry;
	ingress->frame_cb = config->frame_cb;
	ingress->event_cb = config->event_cb;
	ingress->callback_arg = config->callback_arg;
	tr_pipeline_route_parser_init(&ingress->parser);

	memset(&preface, 0, sizeof(preface));
	preface.byte_count = TR_PIPELINE_ROUTE_PREFACE_SIZE;
	preface.feed = tr_pipeline_ingress_preface_feed;
	preface.release = tr_pipeline_ingress_preface_release;
	preface.arg = ingress;

	ret = tr_reactor_adopt_fd_prefaced_on_owner(owner, fd, &preface, out);
	if (ret != TR_OK)
		tr_pipeline_ingress_preface_release(ingress);
	return ret;
}
