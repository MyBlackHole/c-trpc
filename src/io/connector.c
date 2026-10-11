#include "connector_internal.h"

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>

#include "../execution/reactor_internal.h"
#include "socket_internal.h"
#include "socket.h"
#include "tr/status.h"

#define TR_CONNECTOR_ADDRESS_CAPACITY 64U
#define TR_CONNECTOR_MAX_PREFACE_BYTES 256U
#define TR_CONNECTOR_TEARDOWN_RETRY_NS UINT64_C(10000000)

enum tr_connector_terminal_action {
	TR_CONNECTOR_TERMINAL_NONE = 0,
	TR_CONNECTOR_TERMINAL_COMPLETE,
	TR_CONNECTOR_TERMINAL_CANCEL
};

struct tr_connector {
	struct tr_connector_config config;

	char address[TR_CONNECTOR_ADDRESS_CAPACITY];
	uint16_t port;
	int fd;
	int active;
	enum tr_connector_terminal_action terminal_action;
	int terminal_status;
	int connecting;
	int watched;
	int socket_ready;
	/* 单次尝试的定时器与 I/O 共用此绝对时间，0 表示未成功设置定时。 */
	uint64_t deadline_ns;

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

static int tr_connector_unwatch_on_owner(struct tr_connector *connector)
{
	int ret;

	if (!connector->watched)
		return TR_OK;

	ret = tr_reactor_aux_event_unregister(
		connector->config.owner, connector->fd);
	/* STALE 表示此 fd 已从辅助事件源表移除，足以保证 Connector
	 * 生命周期安全。 */
	if (ret != TR_OK && ret != TR_ERR_STALE)
		return ret;

	connector->watched = 0;
	return TR_OK;
}

static int tr_connector_disarm_on_owner(struct tr_connector *connector)
{
	int ret;

	if (!connector->timer_registered)
		return TR_OK;

	ret = tr_reactor_timer_arm(connector->timer, 0U);
	if (ret == TR_ERR_STALE) {
		/* 定时器事件源已不存在，在本地同步记录该状态。 */
		connector->timer_registered = 0;
		memset(&connector->timer, 0, sizeof(connector->timer));
		return TR_OK;
	}
	return ret;
}

static void tr_connector_reset_state(struct tr_connector *connector)
{
	connector->fd = -1;
	connector->active = 0;
	connector->terminal_action = TR_CONNECTOR_TERMINAL_NONE;
	connector->terminal_status = TR_OK;
	connector->connecting = 0;
	connector->watched = 0;
	connector->socket_ready = 0;
	connector->deadline_ns = 0U;
	connector->port = 0U;
	connector->preface_len = 0U;
	connector->preface_sent = 0U;
	memset(connector->address, 0, sizeof(connector->address));
	memset(connector->preface, 0, sizeof(connector->preface));
}

static int tr_connector_complete_on_owner(
	struct tr_connector *connector, int status)
{
	tr_connector_complete_cb callback;
	void *callback_arg;
	int fd = -1;
	int ret;

	if (!connector->active)
		return TR_OK;
	if (connector->terminal_action == TR_CONNECTOR_TERMINAL_CANCEL)
		return TR_ERR_STATE;
	if (connector->terminal_action == TR_CONNECTOR_TERMINAL_NONE) {
		connector->terminal_action = TR_CONNECTOR_TERMINAL_COMPLETE;
		connector->terminal_status = status;
	}
	status = connector->terminal_status;

	/* 只有拆除所有仍持有 Connector 指针的回调源后，才能向上层移交 fd
	 * 所有权。 */
	ret = tr_connector_unwatch_on_owner(connector);
	if (ret != TR_OK)
		return ret;
	ret = tr_connector_disarm_on_owner(connector);
	if (ret != TR_OK)
		return ret;

	callback = connector->config.complete_cb;
	callback_arg = connector->config.callback_arg;
	if (status == TR_OK) {
		fd = connector->fd;
		connector->fd = -1;
	} else {
		tr_socket_close(&connector->fd);
	}
	tr_connector_reset_state(connector);

	/* TR_OK 时 fd 所有权从 connector 转移给回调。回调可以在
	 * 返回前接管或关闭 fd，甚至销毁上层对象，因此之后不能再访问 connector。
	 */
	callback(status, fd, callback_arg);
	return TR_OK;
}

static int tr_connector_cancel_cleanup_on_owner(
	struct tr_connector *connector)
{
	int ret;

	ret = tr_connector_unwatch_on_owner(connector);
	if (ret != TR_OK)
		return ret;
	ret = tr_connector_disarm_on_owner(connector);
	if (ret != TR_OK)
		return ret;

	tr_socket_close(&connector->fd);
	tr_connector_reset_state(connector);
	return TR_OK;
}

static int tr_connector_watch_on_owner(struct tr_connector *connector);

/* 反应器定时器回调与可写 I/O 回调都在同一所有者上执行，但
 * EPOLLOUT 可以在同一轮定时器分派前完成。成功也必须确认截止时间
 * 尚未到期，不能仅依赖稍后才得到执行的定时器回调。
 */
static int tr_connector_deadline_status_on_owner(
	const struct tr_connector *connector)
{
	uint64_t now_ns;

	if (connector->deadline_ns == 0U)
		return TR_ERR_STATE;
	now_ns = tr_connector_now_ns();
	if (now_ns == 0U)
		return TR_ERR_SYS;
	return now_ns >= connector->deadline_ns ? TR_ERR_TIMEOUT : TR_OK;
}

static int tr_connector_send_preface_on_owner(
	struct tr_connector *connector)
{
	for (;;) {
		ssize_t n;
		int ret = tr_connector_deadline_status_on_owner(connector);

		if (ret != TR_OK)
			return tr_connector_complete_on_owner(connector, ret);
		/* 每段发送后在下一轮先校验期限，避免发送已完成但跨越
		 * 截止时间时误把本次尝试提交为成功。
		 */
		if (connector->preface_sent == connector->preface_len)
			return tr_connector_complete_on_owner(connector, TR_OK);

		n = send(connector->fd,
			 connector->preface + connector->preface_sent,
			 connector->preface_len - connector->preface_sent,
			 MSG_NOSIGNAL);
		if (n > 0) {
			connector->preface_sent += (uint32_t)n;
			continue;
		}
		if (n < 0 && (errno == EINTR || errno == EAGAIN ||
			      errno == EWOULDBLOCK)) {
			/* EINTR 同 EAGAIN 一样让出本次所有者处理轮次，依赖
			 * EPOLLOUT 继续，而不是无限重试使超时或命令无法被调度。
			 * 已注册的辅助事件源保持不变，无额外工作线程或定时器。
			 */
			ret = tr_connector_watch_on_owner(connector);
			if (ret == TR_OK)
				return TR_OK;
			return tr_connector_complete_on_owner(connector, ret);
		}

		/* 本次尝试已经接管。若完成结果能成功发布，启动或推进操作本身仍返回成功；
		 * 网络错误通过 complete_cb 恰好报告一次。
		 */
		return tr_connector_complete_on_owner(connector, TR_ERR_SYS);
	}
}

static int tr_connector_progress_on_owner(struct tr_connector *connector)
{
	int ret;

	if (!connector->active || connector->fd < 0)
		return TR_ERR_STATE;

	/* 到期的可写事件不得早于本轮仍待调度的定时器提交成功。 */
	ret = tr_connector_deadline_status_on_owner(connector);
	if (ret != TR_OK)
		return tr_connector_complete_on_owner(connector, ret);

	if (connector->connecting) {
		ret = tr_tcp_finish_connect(connector->fd);
		if (ret != TR_OK)
			return tr_connector_complete_on_owner(connector, ret);
		connector->connecting = 0;
	}

	if (!connector->socket_ready) {
		if (connector->config.tcp_nodelay) {
			ret = tr_tcp_set_nodelay(connector->fd, 1);
			if (ret != TR_OK)
				return tr_connector_complete_on_owner(connector, ret);
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
	if (connector->terminal_action == TR_CONNECTOR_TERMINAL_COMPLETE) {
		(void)tr_connector_complete_on_owner(
			connector, connector->terminal_status);
		return;
	}
	if (connector->terminal_action == TR_CONNECTOR_TERMINAL_CANCEL) {
		(void)tr_connector_cancel_cleanup_on_owner(connector);
		return;
	}
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
	int ret;

	if (!connector || !connector->active)
		return 0U;

	if (connector->terminal_action == TR_CONNECTOR_TERMINAL_CANCEL)
		ret = tr_connector_cancel_cleanup_on_owner(connector);
	else
		ret = tr_connector_complete_on_owner(
			connector,
			connector->terminal_action == TR_CONNECTOR_TERMINAL_COMPLETE ?
				connector->terminal_status : TR_ERR_TIMEOUT);
	if (ret == TR_OK)
		return 0U;

	/* 首次终态已经锁存，但回调源解绑仍可能暂时失败。保持现有
	 * 发布不变，用同一个定时器短暂延迟重试，直到终态清理完成。
	 */
	if (UINT64_MAX - now_ns < TR_CONNECTOR_TEARDOWN_RETRY_NS)
		return UINT64_MAX;
	return now_ns + TR_CONNECTOR_TEARDOWN_RETRY_NS;
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
	{
		int ret = tr_reactor_timer_arm(connector->timer, deadline_ns);

		if (ret == TR_OK)
			connector->deadline_ns = deadline_ns;
		return ret;
	}
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
	if (ret != TR_OK)
		return tr_connector_complete_on_owner(connector, ret);

	if (connector->connecting) {
		ret = tr_connector_watch_on_owner(connector);
		if (ret != TR_OK)
			return tr_connector_complete_on_owner(connector, ret);
		return TR_OK;
	}

	/* 从 active=1 开始，尝试的所有权已经转移给 connector。
	 * 推进操作可能同步完成，且回调允许销毁上层或 Connector；
	 * 因此本调用返回后绝不能再解引用 connector。
	 */
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

	if (connector->active) {
		if (connector->terminal_action ==
		    TR_CONNECTOR_TERMINAL_COMPLETE)
			return TR_ERR_STATE;
		if (connector->terminal_action == TR_CONNECTOR_TERMINAL_NONE)
			connector->terminal_action = TR_CONNECTOR_TERMINAL_CANCEL;
	}

	/* 即使尝试已不活跃，也必须确认不会遗留任何反应器回调源。 */
	return tr_connector_cancel_cleanup_on_owner(connector);
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

static int tr_connector_destroy_on_owner(void *arg)
{
	struct tr_connector *connector = (struct tr_connector *)arg;
	struct tr_connector_cancel_request request;
	int ret;

	request.connector = connector;
	ret = tr_connector_cancel_on_owner(&request);
	if (ret != TR_OK)
		return ret;

	if (connector->timer_registered) {
		ret = tr_reactor_timer_unregister(connector->timer);
		if (ret != TR_OK && ret != TR_ERR_STALE)
			return ret;
		connector->timer_registered = 0;
		memset(&connector->timer, 0, sizeof(connector->timer));
	}
	return TR_OK;
}

int tr_connector_destroy(struct tr_connector *connector)
{
	int ret;

	if (!connector)
		return TR_OK;

	/* Connector
	 * 生命周期依附于反应器。父对象必须在反应器停止前销毁它；所有者
	 * 正在停止或已经停止时保守返回错误，不提前释放 callback_arg 所属存储。
	 */
	ret = tr_reactor_call(
		connector->config.owner, tr_connector_destroy_on_owner, connector);
	if (ret != TR_OK)
		return ret;

	free(connector);
	return TR_OK;
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
