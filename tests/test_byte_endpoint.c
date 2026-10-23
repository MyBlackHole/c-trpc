#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "../src/io/byte_endpoint.h"
#include "../include/tr/status.h"

struct fake_tls {
	int handshake_results[3];
	uint32_t handshake_waits[3];
	size_t handshake_count;
	size_t handshake_calls;
	int write_result;
	uint32_t write_wait;
	size_t write_calls;
	size_t shutdown_calls;
	size_t release_calls;
	int read_result;
	uint32_t read_wait;
	size_t read_calls;
};

static int fake_handshake(void *arg, uint32_t *wait_events)
{
	struct fake_tls *fake = (struct fake_tls *)arg;
	size_t index = fake->handshake_calls++;
	assert(index < fake->handshake_count);
	*wait_events = fake->handshake_waits[index];
	return fake->handshake_results[index];
}

static int fake_read(void *arg, void *buffer, size_t capacity,
                     size_t *read_bytes, uint32_t *wait_events)
{
	(void)arg;
	(void)buffer;
	(void)capacity;
	struct fake_tls *fake = (struct fake_tls *)arg;
	++fake->read_calls;
	*read_bytes = 0U;
	*wait_events = fake->read_result == TR_OK ? 0U : fake->read_wait;
	if (fake->read_result == TR_OK) {
		assert(capacity > 0U);
		((unsigned char *)buffer)[0] = 0x42U;
		*read_bytes = 1U;
	}
	return fake->read_result;
}

static int fake_write(void *arg, const void *buffer, size_t length,
                      size_t *written_bytes, uint32_t *wait_events)
{
	struct fake_tls *fake = (struct fake_tls *)arg;
	(void)buffer;
	++fake->write_calls;
	*written_bytes = fake->write_result == TR_OK ? length : 0U;
	*wait_events = fake->write_result == TR_OK ? 0U : fake->write_wait;
	return fake->write_result;
}

static int fake_shutdown(void *arg, uint32_t *wait_events)
{
	struct fake_tls *fake = (struct fake_tls *)arg;
	++fake->shutdown_calls;
	if (fake->shutdown_calls == 1) {
		*wait_events = TR_BYTE_ENDPOINT_EVENT_READ;
		return TR_AGAIN;
	}
	*wait_events = 0U;
	return TR_OK;
}

static void fake_release(void *arg)
{
	struct fake_tls *fake = (struct fake_tls *)arg;
	++fake->release_calls;
}

static const struct tr_byte_endpoint_driver fake_driver = {
        .handshake = fake_handshake,
        .read = fake_read,
        .write = fake_write,
        .shutdown = fake_shutdown,
        .release = fake_release,
};

static void test_tls_required_without_backend_is_rejected(void)
{
	struct tr_byte_endpoint endpoint;
	assert(tr_byte_endpoint_init(&endpoint, TR_BYTE_ENDPOINT_TLS_REQUIRED,
	                             17, 1000, NULL,
	                             NULL) == TR_ERR_UNSUPPORTED);
	assert(endpoint.state == TR_BYTE_ENDPOINT_UNINITIALIZED);
}

static void test_handshake_waits_for_the_requested_direction(void)
{
	struct tr_byte_endpoint endpoint;
	struct fake_tls fake = {
	        .handshake_results = {TR_AGAIN, TR_AGAIN, TR_OK},
	        .handshake_waits = {TR_BYTE_ENDPOINT_EVENT_READ,
	                            TR_BYTE_ENDPOINT_EVENT_WRITE, 0},
	        .handshake_count = 3,
	};
	assert(tr_byte_endpoint_init(&endpoint, TR_BYTE_ENDPOINT_TLS_REQUIRED,
	                             17, 1000, &fake_driver, &fake) == TR_OK);
	assert(tr_byte_endpoint_step(&endpoint, 0U, 1U) == TR_ERR_STATE);
	assert(fake.handshake_calls == 0U);
	assert(tr_byte_endpoint_event_bind(&endpoint) == TR_OK);
	assert(tr_byte_endpoint_step(&endpoint, 0, 1) == TR_AGAIN);
	assert(tr_byte_endpoint_interest(&endpoint) ==
	       TR_BYTE_ENDPOINT_EVENT_READ);
	assert(tr_byte_endpoint_step(&endpoint, TR_BYTE_ENDPOINT_EVENT_WRITE,
	                             2) == TR_AGAIN);
	assert(fake.handshake_calls == 1);
	assert(tr_byte_endpoint_step(&endpoint, TR_BYTE_ENDPOINT_EVENT_READ,
	                             3) == TR_AGAIN);
	assert(tr_byte_endpoint_interest(&endpoint) ==
	       TR_BYTE_ENDPOINT_EVENT_WRITE);
	assert(tr_byte_endpoint_step(&endpoint, TR_BYTE_ENDPOINT_EVENT_READ,
	                             4) == TR_AGAIN);
	assert(fake.handshake_calls == 2);
	assert(tr_byte_endpoint_step(&endpoint, TR_BYTE_ENDPOINT_EVENT_WRITE,
	                             5) == TR_OK);
	assert(endpoint.state == TR_BYTE_ENDPOINT_AUTHENTICATED);
	assert(tr_byte_endpoint_interest(&endpoint) == 0);
	assert(tr_byte_endpoint_begin_close(&endpoint, 0, 20) == TR_OK);
	assert(tr_byte_endpoint_step(&endpoint, TR_BYTE_ENDPOINT_EVENT_READ,
	                             6) == TR_AGAIN);
	assert(tr_byte_endpoint_interest(&endpoint) ==
	       TR_BYTE_ENDPOINT_EVENT_READ);
	assert(tr_byte_endpoint_step(&endpoint, TR_BYTE_ENDPOINT_EVENT_READ,
	                             7) == TR_OK);
	assert(fake.shutdown_calls == 2);
	assert(tr_byte_endpoint_release(&endpoint) == TR_ERR_STATE);
	assert(tr_byte_endpoint_event_unbind(&endpoint) == TR_OK);
	assert(endpoint.state == TR_BYTE_ENDPOINT_DETACHED);
	assert(tr_byte_endpoint_release(&endpoint) == TR_OK);
	assert(fake.release_calls == 1);
	assert(tr_byte_endpoint_release(&endpoint) == TR_ERR_STATE);
	assert(fake.release_calls == 1);
}

static void test_shutdown_deadline_allows_detach(void)
{
	struct tr_byte_endpoint endpoint;
	struct fake_tls fake = {
	        .handshake_results = {TR_OK},
	        .handshake_waits = {0},
	        .handshake_count = 1,
	};
	assert(tr_byte_endpoint_init(&endpoint, TR_BYTE_ENDPOINT_TLS_REQUIRED,
	                             17, 100, &fake_driver, &fake) == TR_OK);
	assert(tr_byte_endpoint_event_bind(&endpoint) == TR_OK);
	assert(tr_byte_endpoint_step(&endpoint, 0U, 1U) == TR_OK);
	assert(tr_byte_endpoint_begin_close(&endpoint, TR_OK, 10U) == TR_OK);
	assert(tr_byte_endpoint_step(&endpoint, TR_BYTE_ENDPOINT_EVENT_READ,
	                             2U) == TR_AGAIN);
	assert(tr_byte_endpoint_event_unbind(&endpoint) == TR_AGAIN);
	assert(tr_byte_endpoint_step(&endpoint, 0U, 10U) == TR_ERR_TIMEOUT);
	assert(endpoint.shutdown_done == 1);
	assert(tr_byte_endpoint_event_unbind(&endpoint) == TR_OK);
	assert(tr_byte_endpoint_release(&endpoint) == TR_OK);
	assert(fake.shutdown_calls == 1U);
	assert(fake.release_calls == 1U);
}

static void test_handshake_deadline_and_no_progress_fail_closed(void)
{
	struct tr_byte_endpoint endpoint;
	struct fake_tls fake = {
	        .handshake_results = {TR_AGAIN},
	        .handshake_waits = {0},
	        .handshake_count = 1,
	};
	assert(tr_byte_endpoint_init(&endpoint, TR_BYTE_ENDPOINT_TLS_REQUIRED,
	                             17, 10, &fake_driver, &fake) == TR_OK);
	assert(tr_byte_endpoint_event_bind(&endpoint) == TR_OK);
	assert(tr_byte_endpoint_step(&endpoint, 0, 10) == TR_ERR_TIMEOUT);
	assert(fake.handshake_calls == 0);
	assert(tr_byte_endpoint_abort(&endpoint, TR_ERR_TIMEOUT) == TR_OK);
	assert(tr_byte_endpoint_event_unbind(&endpoint) == TR_OK);
	assert(tr_byte_endpoint_release(&endpoint) == TR_OK);

	fake.handshake_results[0] = TR_AGAIN;
	fake.handshake_waits[0] = 0;
	fake.handshake_calls = 0;
	assert(tr_byte_endpoint_init(&endpoint, TR_BYTE_ENDPOINT_TLS_REQUIRED,
	                             17, 100, &fake_driver, &fake) == TR_OK);
	assert(tr_byte_endpoint_event_bind(&endpoint) == TR_OK);
	assert(tr_byte_endpoint_step(&endpoint, 0, 1) == TR_ERR_STATE);
	assert(endpoint.state == TR_BYTE_ENDPOINT_CLOSING);
	assert(tr_byte_endpoint_event_unbind(&endpoint) == TR_OK);
	assert(tr_byte_endpoint_release(&endpoint) == TR_OK);
}

static void test_abort_cancels_negotiation_without_shutdown(void)
{
	struct tr_byte_endpoint endpoint;
	struct fake_tls fake = {
	        .handshake_results = {TR_AGAIN},
	        .handshake_waits = {TR_BYTE_ENDPOINT_EVENT_READ},
	        .handshake_count = 1U,
	};
	assert(tr_byte_endpoint_init(&endpoint, TR_BYTE_ENDPOINT_TLS_REQUIRED,
	                             17, 100U, &fake_driver, &fake) == TR_OK);
	assert(tr_byte_endpoint_event_bind(&endpoint) == TR_OK);
	assert(tr_byte_endpoint_abort(&endpoint, TR_ERR_CLOSED) == TR_OK);
	assert(endpoint.state == TR_BYTE_ENDPOINT_CLOSING);
	assert(fake.shutdown_calls == 0U);
	assert(tr_byte_endpoint_event_unbind(&endpoint) == TR_OK);
	assert(tr_byte_endpoint_release(&endpoint) == TR_OK);
	assert(fake.release_calls == 1U);
}

static void test_tls_write_keeps_buffer_across_want(void)
{
	struct tr_byte_endpoint endpoint;
	struct fake_tls fake = {
	        .handshake_results = {TR_OK},
	        .handshake_waits = {0},
	        .handshake_count = 1,
	        .write_result = TR_AGAIN,
	        .write_wait = TR_BYTE_ENDPOINT_EVENT_READ,
	        .read_result = TR_AGAIN,
	        .read_wait = TR_BYTE_ENDPOINT_EVENT_WRITE,
	};
	static const unsigned char payload[] = {1, 2, 3, 4};
	static const unsigned char other[] = {1, 2, 3, 4};
	static unsigned char receive_buffer[8];
	static unsigned char other_receive_buffer[8];
	size_t written = 99;
	size_t read_bytes = 99;
	uint32_t wait_events = 0;
	assert(tr_byte_endpoint_init(&endpoint, TR_BYTE_ENDPOINT_TLS_REQUIRED,
	                             17, 100, &fake_driver, &fake) == TR_OK);
	assert(tr_byte_endpoint_event_bind(&endpoint) == TR_OK);
	assert(tr_byte_endpoint_step(&endpoint, 0, 1) == TR_OK);
	assert(tr_byte_endpoint_read(&endpoint, receive_buffer,
	                             sizeof(receive_buffer),
	                             TR_BYTE_ENDPOINT_EVENT_READ, &read_bytes,
	                             &wait_events) == TR_AGAIN);
	assert(read_bytes == 0U);
	assert(wait_events == TR_BYTE_ENDPOINT_EVENT_WRITE);
	assert(tr_byte_endpoint_write(&endpoint, payload, sizeof(payload), 0U,
	                              &written, &wait_events) == TR_AGAIN);
	assert(wait_events == TR_BYTE_ENDPOINT_EVENT_READ);
	assert(tr_byte_endpoint_interest(&endpoint) ==
	       (TR_BYTE_ENDPOINT_EVENT_READ | TR_BYTE_ENDPOINT_EVENT_WRITE));
	assert(tr_byte_endpoint_begin_close(&endpoint, TR_OK, 10U) == TR_AGAIN);
	assert(endpoint.state == TR_BYTE_ENDPOINT_AUTHENTICATED);
	assert(tr_byte_endpoint_read(&endpoint, other_receive_buffer,
	                             sizeof(other_receive_buffer),
	                             TR_BYTE_ENDPOINT_EVENT_WRITE, &read_bytes,
	                             &wait_events) == TR_ERR_STATE);
	assert(fake.read_calls == 1U);
	fake.read_result = TR_OK;
	assert(tr_byte_endpoint_read(&endpoint, receive_buffer,
	                             sizeof(receive_buffer),
	                             TR_BYTE_ENDPOINT_EVENT_WRITE, &read_bytes,
	                             &wait_events) == TR_OK);
	assert(read_bytes == 1U && receive_buffer[0] == 0x42U);
	assert(wait_events == 0U);
	assert(tr_byte_endpoint_interest(&endpoint) ==
	       TR_BYTE_ENDPOINT_EVENT_READ);
	assert(tr_byte_endpoint_write(&endpoint, other, sizeof(other),
	                              TR_BYTE_ENDPOINT_EVENT_READ, &written,
	                              &wait_events) == TR_ERR_STATE);
	assert(fake.write_calls == 1U);
	fake.write_result = TR_OK;
	fake.write_wait = 0;
	assert(tr_byte_endpoint_write(&endpoint, payload, sizeof(payload),
	                              TR_BYTE_ENDPOINT_EVENT_READ, &written,
	                              &wait_events) == TR_OK);
	assert(written == sizeof(payload));
	assert(wait_events == 0);
	fake.read_result = TR_ERR_CLOSED;
	fake.read_wait = 0U;
	assert(tr_byte_endpoint_read(&endpoint, receive_buffer,
	                             sizeof(receive_buffer), 0U, &read_bytes,
	                             &wait_events) == TR_ERR_CLOSED);
	assert(endpoint.state == TR_BYTE_ENDPOINT_AUTHENTICATED);
	assert(endpoint.peer_closed == 1);
	assert(fake.shutdown_calls == 0U);
	assert(tr_byte_endpoint_write(&endpoint, payload, sizeof(payload), 0U,
	                              &written, &wait_events) == TR_OK);
	assert(written == sizeof(payload));
	assert(tr_byte_endpoint_begin_close(&endpoint, TR_ERR_CLOSED, 10U) ==
	       TR_OK);
	assert(tr_byte_endpoint_step(&endpoint, TR_BYTE_ENDPOINT_EVENT_READ,
	                             2) == TR_AGAIN);
	assert(tr_byte_endpoint_step(&endpoint, TR_BYTE_ENDPOINT_EVENT_READ,
	                             3) == TR_ERR_CLOSED);
	assert(tr_byte_endpoint_event_unbind(&endpoint) == TR_OK);
	assert(tr_byte_endpoint_release(&endpoint) == TR_OK);
}

static void test_plain_endpoint_adapts_nonblocking_socket_io(void)
{
	struct tr_byte_endpoint endpoint;
	unsigned char input = 0x31U;
	unsigned char output = 0U;
	size_t count = 0U;
	uint32_t wait_events = 0U;
	int fd[2];
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
	                  0, fd) == 0);
	assert(tr_byte_endpoint_init(&endpoint, TR_BYTE_ENDPOINT_PLAIN, fd[0],
	                             0U, NULL, NULL) == TR_OK);
	assert(endpoint.state == TR_BYTE_ENDPOINT_AUTHENTICATED);
	assert(tr_byte_endpoint_event_bind(&endpoint) == TR_OK);
	assert(tr_byte_endpoint_read(&endpoint, &output, sizeof(output), 0U,
	                             &count, &wait_events) == TR_AGAIN);
	assert(count == 0U && wait_events == TR_BYTE_ENDPOINT_EVENT_READ);
	assert(tr_byte_endpoint_interest(&endpoint) ==
	       TR_BYTE_ENDPOINT_EVENT_READ);
	assert(send(fd[1], &input, sizeof(input), MSG_NOSIGNAL) ==
	       (ssize_t)sizeof(input));
	assert(tr_byte_endpoint_read(&endpoint, &output, sizeof(output),
	                             TR_BYTE_ENDPOINT_EVENT_READ, &count,
	                             &wait_events) == TR_OK);
	assert(count == 1U && output == input && wait_events == 0U);
	assert(tr_byte_endpoint_write(&endpoint, &input, sizeof(input), 0U,
	                              &count, &wait_events) == TR_OK);
	assert(count == 1U && wait_events == 0U);
	output = 0U;
	assert(recv(fd[1], &output, sizeof(output), 0) ==
	       (ssize_t)sizeof(output));
	assert(output == input);
	assert(shutdown(fd[1], SHUT_WR) == 0);
	assert(tr_byte_endpoint_read(&endpoint, &output, sizeof(output),
	                             TR_BYTE_ENDPOINT_EVENT_READ, &count,
	                             &wait_events) == TR_ERR_CLOSED);
	assert(endpoint.state == TR_BYTE_ENDPOINT_CLOSING);
	assert(endpoint.shutdown_done == 1);
	assert(tr_byte_endpoint_event_unbind(&endpoint) == TR_OK);
	assert(tr_byte_endpoint_release(&endpoint) == TR_OK);
	assert(close(fd[1]) == 0);
	assert(close(fd[0]) == 0);
}

int main(void)
{
	test_tls_required_without_backend_is_rejected();
	test_handshake_waits_for_the_requested_direction();
	test_shutdown_deadline_allows_detach();
	test_handshake_deadline_and_no_progress_fail_closed();
	test_abort_cancels_negotiation_without_shutdown();
	test_tls_write_keeps_buffer_across_want();
	test_plain_endpoint_adapts_nonblocking_socket_io();
	return 0;
}
