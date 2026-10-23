#include "byte_endpoint.h"

#include <errno.h>
#include <limits.h>
#include <string.h>
#include <sys/socket.h>

#define TR_BYTE_ENDPOINT_EVENT_MASK                                            \
	(TR_BYTE_ENDPOINT_EVENT_READ | TR_BYTE_ENDPOINT_EVENT_WRITE)

static int tr_byte_endpoint_valid_wait(uint32_t wait_events)
{
	return wait_events != 0U &&
	       (wait_events & ~TR_BYTE_ENDPOINT_EVENT_MASK) == 0U;
}

static void tr_byte_endpoint_clear_io(struct tr_byte_endpoint *endpoint)
{
	endpoint->read_wait = 0U;
	endpoint->write_wait = 0U;
	endpoint->read_buffer = NULL;
	endpoint->read_capacity = 0U;
	endpoint->write_buffer = NULL;
	endpoint->write_length = 0U;
}

static int tr_byte_endpoint_fail(struct tr_byte_endpoint *endpoint, int status)
{
	if (status >= 0)
		status = TR_ERR_SYS;
	endpoint->state = TR_BYTE_ENDPOINT_CLOSING;
	endpoint->terminal_status = status;
	endpoint->handshake_wait = 0U;
	endpoint->shutdown_wait = 0U;
	endpoint->shutdown_done = 1;
	tr_byte_endpoint_clear_io(endpoint);
	return status;
}

static int tr_byte_endpoint_plain_io_status(struct tr_byte_endpoint *endpoint,
                                            int status)
{
	return status < 0 ? tr_byte_endpoint_fail(endpoint, status) : status;
}

int tr_byte_endpoint_init(struct tr_byte_endpoint *endpoint,
                          enum tr_byte_endpoint_mode mode, int fd,
                          uint64_t handshake_deadline_ns,
                          const struct tr_byte_endpoint_driver *driver,
                          void *driver_arg)
{
	if (!endpoint)
		return TR_ERR_INVALID;
	memset(endpoint, 0, sizeof(*endpoint));
	if (fd < 0)
		return TR_ERR_INVALID;
	if (mode == TR_BYTE_ENDPOINT_TLS_REQUIRED) {
		if (!driver)
			return TR_ERR_UNSUPPORTED;
		if (!driver->handshake || !driver->read || !driver->write ||
		    !driver->shutdown || !driver->release ||
		    handshake_deadline_ns == 0U)
			return TR_ERR_INVALID;
		endpoint->state = TR_BYTE_ENDPOINT_TCP_READY;
		endpoint->state = TR_BYTE_ENDPOINT_NEGOTIATING;
		endpoint->driver = driver;
		endpoint->driver_arg = driver_arg;
		endpoint->handshake_deadline_ns = handshake_deadline_ns;
	} else if (mode == TR_BYTE_ENDPOINT_PLAIN) {
		if (driver)
			return TR_ERR_INVALID;
		endpoint->state = TR_BYTE_ENDPOINT_TCP_READY;
		endpoint->state = TR_BYTE_ENDPOINT_AUTHENTICATED;
		endpoint->shutdown_done = 1;
	} else {
		return TR_ERR_INVALID;
	}
	endpoint->fd = fd;
	endpoint->mode = mode;
	return TR_OK;
}

int tr_byte_endpoint_event_bind(struct tr_byte_endpoint *endpoint)
{
	if (!endpoint || endpoint->events_bound ||
	    (endpoint->state != TR_BYTE_ENDPOINT_TCP_READY &&
	     endpoint->state != TR_BYTE_ENDPOINT_NEGOTIATING &&
	     endpoint->state != TR_BYTE_ENDPOINT_AUTHENTICATED))
		return TR_ERR_STATE;
	endpoint->events_bound = 1;
	return TR_OK;
}

static int tr_byte_endpoint_drive_handshake(struct tr_byte_endpoint *endpoint,
                                            uint32_t ready_events,
                                            uint64_t now_ns)
{
	uint32_t wait_events = 0U;
	int status;
	if (now_ns >= endpoint->handshake_deadline_ns)
		return tr_byte_endpoint_fail(endpoint, TR_ERR_TIMEOUT);
	if (endpoint->handshake_wait != 0U &&
	    (ready_events & endpoint->handshake_wait) == 0U)
		return TR_AGAIN;
	status =
	        endpoint->driver->handshake(endpoint->driver_arg, &wait_events);
	if (status == TR_OK) {
		if (wait_events != 0U)
			return tr_byte_endpoint_fail(endpoint, TR_ERR_STATE);
		endpoint->handshake_wait = 0U;
		endpoint->state = TR_BYTE_ENDPOINT_AUTHENTICATED;
		return TR_OK;
	}
	if (status == TR_AGAIN) {
		if (!tr_byte_endpoint_valid_wait(wait_events))
			return tr_byte_endpoint_fail(endpoint, TR_ERR_STATE);
		endpoint->handshake_wait = wait_events;
		return TR_AGAIN;
	}
	return tr_byte_endpoint_fail(endpoint,
	                             status < 0 ? status : TR_ERR_SYS);
}

static int tr_byte_endpoint_drive_shutdown(struct tr_byte_endpoint *endpoint,
                                           uint32_t ready_events)
{
	uint32_t wait_events = 0U;
	int status;
	if (endpoint->shutdown_done)
		return endpoint->terminal_status;
	if (endpoint->shutdown_wait != 0U &&
	    (ready_events & endpoint->shutdown_wait) == 0U)
		return TR_AGAIN;
	status = endpoint->driver->shutdown(endpoint->driver_arg, &wait_events);
	if (status == TR_OK) {
		if (wait_events != 0U)
			return tr_byte_endpoint_fail(endpoint, TR_ERR_STATE);
		endpoint->shutdown_done = 1;
		endpoint->shutdown_wait = 0U;
		if (!endpoint->events_bound)
			endpoint->state = TR_BYTE_ENDPOINT_DETACHED;
		return endpoint->terminal_status;
	}
	if (status == TR_AGAIN) {
		if (!tr_byte_endpoint_valid_wait(wait_events))
			return tr_byte_endpoint_fail(endpoint, TR_ERR_STATE);
		endpoint->shutdown_wait = wait_events;
		return TR_AGAIN;
	}
	return tr_byte_endpoint_fail(endpoint,
	                             status < 0 ? status : TR_ERR_SYS);
}

int tr_byte_endpoint_step(struct tr_byte_endpoint *endpoint,
                          uint32_t ready_events, uint64_t now_ns)
{
	if (!endpoint)
		return TR_ERR_INVALID;
	if ((ready_events & ~TR_BYTE_ENDPOINT_EVENT_MASK) != 0U)
		return TR_ERR_INVALID;
	if (endpoint->state == TR_BYTE_ENDPOINT_NEGOTIATING) {
		if (!endpoint->events_bound)
			return TR_ERR_STATE;
		return tr_byte_endpoint_drive_handshake(endpoint, ready_events,
		                                        now_ns);
	}
	if (endpoint->state == TR_BYTE_ENDPOINT_CLOSING &&
	    !endpoint->shutdown_done && !endpoint->events_bound)
		return TR_ERR_STATE;
	if (endpoint->state == TR_BYTE_ENDPOINT_CLOSING &&
	    !endpoint->shutdown_done && endpoint->shutdown_deadline_ns != 0U &&
	    now_ns >= endpoint->shutdown_deadline_ns) {
		endpoint->shutdown_done = 1;
		endpoint->shutdown_wait = 0U;
		if (endpoint->terminal_status == TR_OK)
			endpoint->terminal_status = TR_ERR_TIMEOUT;
		if (!endpoint->events_bound)
			endpoint->state = TR_BYTE_ENDPOINT_DETACHED;
		return endpoint->terminal_status;
	}
	if (endpoint->state == TR_BYTE_ENDPOINT_CLOSING &&
	    endpoint->mode == TR_BYTE_ENDPOINT_TLS_REQUIRED &&
	    !endpoint->shutdown_done && endpoint->shutdown_deadline_ns == 0U)
		return TR_ERR_STATE;
	if (endpoint->state == TR_BYTE_ENDPOINT_CLOSING)
		return tr_byte_endpoint_drive_shutdown(endpoint, ready_events);
	return TR_ERR_STATE;
}

uint32_t tr_byte_endpoint_interest(const struct tr_byte_endpoint *endpoint)
{
	if (!endpoint)
		return 0U;
	return endpoint->handshake_wait | endpoint->read_wait |
	       endpoint->write_wait | endpoint->shutdown_wait;
}

static int tr_byte_endpoint_retry_ready(uint32_t wait_events,
                                        uint32_t ready_events)
{
	return wait_events == 0U || (wait_events & ready_events) != 0U;
}

static int tr_byte_endpoint_plain_read(struct tr_byte_endpoint *endpoint,
                                       void *buffer, size_t capacity,
                                       size_t *read_bytes,
                                       uint32_t *wait_events)
{
	ssize_t result;
	do {
		result = recv(endpoint->fd, buffer,
		              capacity > (size_t)SSIZE_MAX ? (size_t)SSIZE_MAX
		                                           : capacity,
		              MSG_DONTWAIT);
	} while (result < 0 && errno == EINTR);
	if (result > 0) {
		*read_bytes = (size_t)result;
		return TR_OK;
	}
	if (result == 0)
		return tr_byte_endpoint_plain_io_status(endpoint,
		                                        TR_ERR_CLOSED);
	if (errno == EAGAIN || errno == EWOULDBLOCK) {
		*wait_events = TR_BYTE_ENDPOINT_EVENT_READ;
		return TR_AGAIN;
	}
	return tr_byte_endpoint_plain_io_status(endpoint, TR_ERR_SYS);
}

int tr_byte_endpoint_read(struct tr_byte_endpoint *endpoint, void *buffer,
                          size_t capacity, uint32_t ready_events,
                          size_t *read_bytes, uint32_t *wait_events)
{
	int status;
	if (!endpoint || !buffer || capacity == 0U || !read_bytes ||
	    !wait_events || (ready_events & ~TR_BYTE_ENDPOINT_EVENT_MASK) != 0U)
		return TR_ERR_INVALID;
	*read_bytes = 0U;
	*wait_events = 0U;
	if (endpoint->state != TR_BYTE_ENDPOINT_AUTHENTICATED)
		return TR_ERR_STATE;
	if (!endpoint->events_bound)
		return TR_ERR_STATE;
	if (endpoint->mode == TR_BYTE_ENDPOINT_TLS_REQUIRED &&
	    endpoint->peer_closed)
		return TR_ERR_CLOSED;
	if (endpoint->read_buffer && (endpoint->read_buffer != buffer ||
	                              endpoint->read_capacity != capacity))
		return TR_ERR_STATE;
	if (!tr_byte_endpoint_retry_ready(endpoint->read_wait, ready_events)) {
		*wait_events = endpoint->read_wait;
		return TR_AGAIN;
	}
	if (endpoint->mode == TR_BYTE_ENDPOINT_PLAIN)
		status = tr_byte_endpoint_plain_read(endpoint, buffer, capacity,
		                                     read_bytes, wait_events);
	else
		status = endpoint->driver->read(endpoint->driver_arg, buffer,
		                                capacity, read_bytes,
		                                wait_events);
	if (status == TR_AGAIN) {
		if (!tr_byte_endpoint_valid_wait(*wait_events) ||
		    *read_bytes != 0U)
			return tr_byte_endpoint_fail(endpoint, TR_ERR_STATE);
		endpoint->read_buffer = buffer;
		endpoint->read_capacity = capacity;
		endpoint->read_wait = *wait_events;
		return TR_AGAIN;
	}
	if (status != TR_OK) {
		endpoint->read_wait = 0U;
		endpoint->read_buffer = NULL;
		endpoint->read_capacity = 0U;
		if (endpoint->mode == TR_BYTE_ENDPOINT_TLS_REQUIRED &&
		    status == TR_ERR_CLOSED) {
			if (*read_bytes != 0U || *wait_events != 0U)
				return tr_byte_endpoint_fail(endpoint,
				                             TR_ERR_STATE);
			/* 保留发送方向，供 Reactor 先排空已有 TX 项。 */
			endpoint->peer_closed = 1;
			endpoint->read_wait = 0U;
			endpoint->read_buffer = NULL;
			endpoint->read_capacity = 0U;
			return status;
		}
		if (endpoint->mode == TR_BYTE_ENDPOINT_TLS_REQUIRED)
			return tr_byte_endpoint_fail(endpoint, status);
		return status;
	}
	if (*read_bytes == 0U || *read_bytes > capacity || *wait_events != 0U)
		return tr_byte_endpoint_fail(endpoint, TR_ERR_STATE);
	endpoint->read_wait = 0U;
	endpoint->read_buffer = NULL;
	endpoint->read_capacity = 0U;
	return TR_OK;
}

static int tr_byte_endpoint_plain_write(struct tr_byte_endpoint *endpoint,
                                        const void *buffer, size_t length,
                                        size_t *written_bytes,
                                        uint32_t *wait_events)
{
	ssize_t result;
	do {
		result = send(endpoint->fd, buffer,
		              length > (size_t)SSIZE_MAX ? (size_t)SSIZE_MAX
		                                         : length,
		              MSG_DONTWAIT | MSG_NOSIGNAL);
	} while (result < 0 && errno == EINTR);
	if (result > 0) {
		*written_bytes = (size_t)result;
		return TR_OK;
	}
	if (result == 0)
		return tr_byte_endpoint_plain_io_status(endpoint,
		                                        TR_ERR_CLOSED);
	if (errno == EAGAIN || errno == EWOULDBLOCK) {
		*wait_events = TR_BYTE_ENDPOINT_EVENT_WRITE;
		return TR_AGAIN;
	}
	return tr_byte_endpoint_plain_io_status(endpoint, TR_ERR_SYS);
}

int tr_byte_endpoint_write(struct tr_byte_endpoint *endpoint,
                           const void *buffer, size_t length,
                           uint32_t ready_events, size_t *written_bytes,
                           uint32_t *wait_events)
{
	int status;
	if (!endpoint || !buffer || length == 0U || !written_bytes ||
	    !wait_events || (ready_events & ~TR_BYTE_ENDPOINT_EVENT_MASK) != 0U)
		return TR_ERR_INVALID;
	*written_bytes = 0U;
	*wait_events = 0U;
	if (endpoint->state != TR_BYTE_ENDPOINT_AUTHENTICATED)
		return TR_ERR_STATE;
	if (!endpoint->events_bound)
		return TR_ERR_STATE;
	if (endpoint->write_buffer && (endpoint->write_buffer != buffer ||
	                               endpoint->write_length != length))
		return TR_ERR_STATE;
	if (!tr_byte_endpoint_retry_ready(endpoint->write_wait, ready_events)) {
		*wait_events = endpoint->write_wait;
		return TR_AGAIN;
	}
	if (endpoint->mode == TR_BYTE_ENDPOINT_PLAIN)
		status = tr_byte_endpoint_plain_write(
		        endpoint, buffer, length, written_bytes, wait_events);
	else
		status = endpoint->driver->write(endpoint->driver_arg, buffer,
		                                 length, written_bytes,
		                                 wait_events);
	if (status == TR_AGAIN) {
		if (!tr_byte_endpoint_valid_wait(*wait_events) ||
		    *written_bytes != 0U)
			return tr_byte_endpoint_fail(endpoint, TR_ERR_STATE);
		endpoint->write_buffer = buffer;
		endpoint->write_length = length;
		endpoint->write_wait = *wait_events;
		return TR_AGAIN;
	}
	if (status != TR_OK) {
		endpoint->write_wait = 0U;
		endpoint->write_buffer = NULL;
		endpoint->write_length = 0U;
		if (endpoint->mode == TR_BYTE_ENDPOINT_TLS_REQUIRED)
			return tr_byte_endpoint_fail(endpoint, status);
		return status;
	}
	if (*written_bytes == 0U || *written_bytes > length ||
	    *wait_events != 0U)
		return tr_byte_endpoint_fail(endpoint, TR_ERR_STATE);
	endpoint->write_wait = 0U;
	endpoint->write_buffer = NULL;
	endpoint->write_length = 0U;
	return TR_OK;
}

int tr_byte_endpoint_begin_close(struct tr_byte_endpoint *endpoint,
                                 int terminal_status,
                                 uint64_t shutdown_deadline_ns)
{
	if (!endpoint)
		return TR_ERR_INVALID;
	if (terminal_status > TR_OK)
		return TR_ERR_INVALID;
	if (endpoint->state == TR_BYTE_ENDPOINT_CLOSING &&
	    endpoint->shutdown_done)
		return TR_OK;
	if (endpoint->mode == TR_BYTE_ENDPOINT_TLS_REQUIRED &&
	    !endpoint->events_bound)
		return TR_ERR_STATE;
	if (endpoint->state == TR_BYTE_ENDPOINT_AUTHENTICATED &&
	    endpoint->write_buffer)
		return TR_AGAIN;
	if (endpoint->state == TR_BYTE_ENDPOINT_CLOSING &&
	    !endpoint->shutdown_done) {
		if (endpoint->mode == TR_BYTE_ENDPOINT_TLS_REQUIRED &&
		    shutdown_deadline_ns == 0U)
			return TR_ERR_INVALID;
		if (endpoint->shutdown_deadline_ns == 0U)
			endpoint->shutdown_deadline_ns = shutdown_deadline_ns;
		return TR_OK;
	}
	if (endpoint->state != TR_BYTE_ENDPOINT_AUTHENTICATED)
		return TR_ERR_STATE;
	if (endpoint->mode == TR_BYTE_ENDPOINT_TLS_REQUIRED &&
	    shutdown_deadline_ns == 0U)
		return TR_ERR_INVALID;
	endpoint->state = TR_BYTE_ENDPOINT_CLOSING;
	endpoint->terminal_status = terminal_status;
	endpoint->shutdown_deadline_ns = shutdown_deadline_ns;
	endpoint->handshake_wait = 0U;
	endpoint->shutdown_wait = 0U;
	endpoint->shutdown_done = endpoint->mode == TR_BYTE_ENDPOINT_PLAIN;
	tr_byte_endpoint_clear_io(endpoint);
	if (endpoint->shutdown_done && !endpoint->events_bound)
		endpoint->state = TR_BYTE_ENDPOINT_DETACHED;
	return TR_OK;
}

int tr_byte_endpoint_abort(struct tr_byte_endpoint *endpoint,
                           int terminal_status)
{
	if (!endpoint)
		return TR_ERR_INVALID;
	if (terminal_status > TR_OK)
		return TR_ERR_INVALID;
	if (endpoint->state == TR_BYTE_ENDPOINT_CLOSING) {
		if (!endpoint->shutdown_done &&
		    endpoint->terminal_status == TR_OK &&
		    terminal_status < TR_OK)
			endpoint->terminal_status = terminal_status;
		endpoint->shutdown_done = 1;
		endpoint->shutdown_wait = 0U;
		if (!endpoint->events_bound)
			endpoint->state = TR_BYTE_ENDPOINT_DETACHED;
		return TR_OK;
	}
	if (endpoint->state != TR_BYTE_ENDPOINT_AUTHENTICATED &&
	    endpoint->state != TR_BYTE_ENDPOINT_NEGOTIATING)
		return TR_ERR_STATE;
	endpoint->state = TR_BYTE_ENDPOINT_CLOSING;
	endpoint->terminal_status = terminal_status;
	endpoint->handshake_wait = 0U;
	endpoint->shutdown_wait = 0U;
	endpoint->shutdown_done = 1;
	tr_byte_endpoint_clear_io(endpoint);
	if (!endpoint->events_bound)
		endpoint->state = TR_BYTE_ENDPOINT_DETACHED;
	return TR_OK;
}

int tr_byte_endpoint_event_unbind(struct tr_byte_endpoint *endpoint)
{
	if (!endpoint || !endpoint->events_bound ||
	    endpoint->state != TR_BYTE_ENDPOINT_CLOSING)
		return TR_ERR_STATE;
	if (!endpoint->shutdown_done)
		return TR_AGAIN;
	endpoint->events_bound = 0;
	if (endpoint->shutdown_done)
		endpoint->state = TR_BYTE_ENDPOINT_DETACHED;
	return TR_OK;
}

int tr_byte_endpoint_release(struct tr_byte_endpoint *endpoint)
{
	if (!endpoint || endpoint->state != TR_BYTE_ENDPOINT_DETACHED ||
	    endpoint->events_bound)
		return TR_ERR_STATE;
	if (endpoint->driver && endpoint->driver->release)
		endpoint->driver->release(endpoint->driver_arg);
	endpoint->state = TR_BYTE_ENDPOINT_RELEASED;
	endpoint->driver = NULL;
	endpoint->driver_arg = NULL;
	return TR_OK;
}
