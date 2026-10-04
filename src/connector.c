#include "connector_internal.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>

#include "reactor_internal.h"
#include "socket_internal.h"
#include "tr/socket.h"
#include "tr/status.h"

#define TR_CONNECTOR_ADDRESS_CAPACITY 64U
#define TR_CONNECTOR_MAX_PREFACE_BYTES 256U

struct tr_connector {
	struct tr_connector_config config;

	char address[TR_CONNECTOR_ADDRESS_CAPACITY];
	uint16_t port;
	int fd;
	int active;
	int connecting;
	int watched;
	int socket_ready;

	uint8_t preface[TR_CONNECTOR_MAX_PREFACE_BYTES];
	uint32_t preface_len;
	uint32_t preface_sent;

	struct tr_reactor_timer_handle timer;
	int timer_registered;
};

static uint64_t tr_connector_now_ns(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
		return 0U;
	return (uint64_t)ts.tv_sec * UINT64_C(1000000000) +
	       (uint64_t)ts.tv_nsec;
}

static void tr_connector_unwatch_on_owner(struct tr_connector *connector)
{
	if (!connector->watched)
		return;
	(void)tr_reactor_aux_event_unregister(
		connector->config.owner, connector->fd);
	connector->watched = 0;
}

static void tr_connector_disarm_on_owner(struct tr_connector *connector)
{
	if (connector->timer_registered)
		(void)tr_reactor_timer_arm(connector->timer, 0U);
}

static void tr_connector_reset_state(struct tr_connector *connector)
{
	connector->fd = -1;
	connector->active = 0;
	connector->connecting = 0;
	connector->watched = 0;
	connector->socket_ready = 0;
	connector->port = 0U;
	connector->preface_len = 0U;
	connector->preface_sent = 0U;
	memset(connector->address, 0, sizeof(connector->address));
	memset(connector->preface, 0, sizeof(connector->preface));
}

static void tr_connector_complete_on_owner(
	struct tr_connector *connector, int status)
{
	tr_connector_complete_cb callback;
	void *callback_arg;
	int fd = -1;

	if (!connector->active)
		return;

	tr_connector_unwatch_on_owner(connector);
	tr_connector_disarm_on_owner(connector);

	callback = connector->config.complete_cb;
	callback_arg = connector->config.callback_arg;
	if (status == TR_OK) {
		fd = connector->fd;
		connector->fd = -1;
	} else {
		tr_socket_close(&connector->fd);
	}
	tr_connector_reset_state(connector);

	/*
	 * TR_OK 时 fd ownership 从 connector 转移给 callback。callback 可以在
	 * 返回前 adopt/close fd，甚至销毁上层对象，因此之后不能再访问 connector。
	 */
	callback(status, fd, callback_arg);
}

static int tr_connector_watch_on_owner(struct tr_connector *connector);

static int tr_connector_send_preface_on_owner(
	struct tr_connector *connector)
{
	while (connector->preface_sent < connector->preface_len) {
		ssize_t n = send(
			connector->fd,
			connector->preface + connector->preface_sent,
			connector->preface_len - connector->preface_sent,
			MSG_NOSIGNAL);

		if (n > 0) {
			connector->preface_sent += (uint32_t)n;
			continue;
		}
		if (n < 0 && errno == EINTR)
			continue;
		if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
			return tr_connector_watch_on_owner(connector);

		tr_connector_complete_on_owner(connector, TR_ERR_SYS);
		return TR_ERR_SYS;
	}

	tr_connector_complete_on_owner(connector, TR_OK);
	return TR_OK;
}

static int tr_connector_progress_on_owner(struct tr_connector *connector)
{
	int ret;

	if (!connector->active || connector->fd < 0)
		return TR_ERR_STATE;

	if (connector->connecting) {
		ret = tr_tcp_finish_connect(connector->fd);
		if (ret != TR_OK) {
			tr_connector_complete_on_owner(connector, ret);
			return ret;
		}
		connector->connecting = 0;
	}

	if (!connector->socket_ready) {
		if (connector->config.tcp_nodelay) {
			ret = tr_tcp_set_nodelay(connector->fd, 1);
			if (ret != TR_OK) {
				tr_connector_complete_on_owner(connector, ret);
				return ret;
			}
		}
		connector->socket_ready = 1;
	}

	return tr_connector_send_preface_on_owner(connector);
}

static void tr_connector_event(
	int fd, uint32_t events, void *arg)
{
	struct tr_connector *connector = (struct tr_connector *)arg;

	(void)events;
	if (!connector || !connector->active || fd != connector->fd)
		return;
	(void)tr_connector_progress_on_owner(connector);
}

static int tr_connector_watch_on_owner(struct tr_connector *connector)
{
	int ret;

	if (connector->watched)
		return TR_OK;

	ret = tr_reactor_aux_event_register(
		connector->config.owner, connector->fd, EPOLLOUT,
		tr_connector_event, connector);
	if (ret == TR_OK)
		connector->watched = 1;
	return ret;
}

static uint64_t tr_connector_timeout(void *arg, uint64_t now_ns)
{
	struct tr_connector *connector = (struct tr_connector *)arg;

	(void)now_ns;
	if (connector && connector->active)
		tr_connector_complete_on_owner(connector, TR_ERR_TIMEOUT);
	return 0U;
}

static int tr_connector_arm_timeout_on_owner(struct tr_connector *connector)
{
	uint64_t now_ns;
	uint64_t delay_ns;
	uint64_t deadline_ns;

	now_ns = tr_connector_now_ns();
	if (now_ns == 0U)
		return TR_ERR_SYS;

	delay_ns =
		(uint64_t)connector->config.timeout_ms * UINT64_C(1000000);
	deadline_ns = UINT64_MAX - now_ns < delay_ns ?
			      UINT64_MAX : now_ns + delay_ns;
	return tr_reactor_timer_arm(connector->timer, deadline_ns);
}

struct tr_connector_start_request {
	struct tr_connector *connector;
	const char *address;
	uint16_t port;
	const uint8_t *preface;
	uint32_t preface_len;
};

static int tr_connector_start_on_owner(void *arg)
{
	struct tr_connector_start_request *request =
		(struct tr_connector_start_request *)arg;
	struct tr_connector *connector = request->connector;
	size_t address_len;
	int fd = -1;
	int ret;

	if (connector->active)
		return TR_ERR_STATE;

	address_len = strlen(request->address);
	if (address_len == 0U ||
	    address_len >= sizeof(connector->address) ||
	    request->port == 0U ||
	    request->preface_len > sizeof(connector->preface) ||
	    (request->preface_len != 0U && !request->preface))
		return TR_ERR_INVALID;

	ret = tr_tcp_connect_ipv4(request->address, request->port, &fd);
	if (ret != TR_OK && ret != TR_IN_PROGRESS)
		return ret;

	memcpy(connector->address, request->address, address_len + 1U);
	connector->port = request->port;
	connector->fd = fd;
	connector->active = 1;
	connector->connecting = ret == TR_IN_PROGRESS;
	connector->socket_ready = 0;
	connector->preface_len = request->preface_len;
	connector->preface_sent = 0U;
	if (request->preface_len != 0U)
		memcpy(connector->preface, request->preface,
		       request->preface_len);

	ret = tr_connector_arm_timeout_on_owner(connector);
	if (ret != TR_OK) {
		tr_connector_complete_on_owner(connector, ret);
		return ret;
	}

	if (connector->connecting) {
		ret = tr_connector_watch_on_owner(connector);
		if (ret != TR_OK) {
			tr_connector_complete_on_owner(connector, ret);
			return ret;
		}
		return TR_OK;
	}

	return tr_connector_progress_on_owner(connector);
}

struct tr_connector_cancel_request {
	struct tr_connector *connector;
};

static int tr_connector_cancel_on_owner(void *arg)
{
	struct tr_connector_cancel_request *request =
		(struct tr_connector_cancel_request *)arg;
	struct tr_connector *connector = request->connector;

	if (!connector->active)
		return TR_OK;

	tr_connector_unwatch_on_owner(connector);
	tr_connector_disarm_on_owner(connector);
	tr_socket_close(&connector->fd);
	tr_connector_reset_state(connector);
	return TR_OK;
}

int tr_connector_create(
	const struct tr_connector_config *config,
	struct tr_connector **out)
{
	struct tr_connector *connector;
	int ret;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	if (!config || !config->owner || config->timeout_ms == 0U ||
	    (config->tcp_nodelay != 0 && config->tcp_nodelay != 1) ||
	    !config->complete_cb)
		return TR_ERR_INVALID;

	connector = (struct tr_connector *)calloc(1, sizeof(*connector));
	if (!connector)
		return TR_ERR_NOMEM;
	connector->config = *config;
	connector->fd = -1;

	ret = tr_reactor_timer_register(
		config->owner, tr_connector_timeout, connector,
		&connector->timer);
	if (ret != TR_OK) {
		free(connector);
		return ret;
	}
	connector->timer_registered = 1;

	*out = connector;
	return TR_OK;
}

void tr_connector_destroy(struct tr_connector *connector)
{
	if (!connector)
		return;

	(void)tr_connector_cancel(connector);
	if (connector->timer_registered)
		(void)tr_reactor_timer_unregister(connector->timer);
	free(connector);
}

int tr_connector_start(
	struct tr_connector *connector, const char *ipv4_address, uint16_t port,
	const uint8_t *preface, uint32_t preface_len)
{
	struct tr_connector_start_request request;

	if (!connector || !ipv4_address)
		return TR_ERR_INVALID;

	request.connector = connector;
	request.address = ipv4_address;
	request.port = port;
	request.preface = preface;
	request.preface_len = preface_len;
	return tr_reactor_call(
		connector->config.owner, tr_connector_start_on_owner, &request);
}

int tr_connector_cancel(struct tr_connector *connector)
{
	struct tr_connector_cancel_request request;

	if (!connector)
		return TR_ERR_INVALID;
	request.connector = connector;
	return tr_reactor_call(
		connector->config.owner, tr_connector_cancel_on_owner, &request);
}
