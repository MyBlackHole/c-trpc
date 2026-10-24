#define _POSIX_C_SOURCE 200809L

#include "tr/trpc.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

#define BENCH_SCHEMA 1U
#define BENCH_GROUP_BASE UINT64_C(0xc7a00000)
#define BENCH_GROUP_EPOCH UINT64_C(1)
#define BENCH_STREAM_ID 1U
#define BENCH_READY_MESSAGE_ID UINT64_C(1)
#define BENCH_HEADER_SIZE 24U
#define BENCH_SMALL_BODY 32U
#define BENCH_BULK_BODY (64U * 1024U)
#define BENCH_MAX_CLIENTS 2U
#define BENCH_MAX_REQUESTS 100000U
#define BENCH_MESSAGE_LIMIT (128U * 1024U)
#define BENCH_TIMEOUT_MS 30000U

static _Atomic uint64_t bench_alloc_calls;
static _Atomic uint64_t bench_alloc_requested_bytes;

static int bench_debug_enabled(void)
{
	return getenv("TR_BENCH_DEBUG") != NULL;
}

void *__real_malloc(size_t size);
void *__real_calloc(size_t count, size_t size);
void *__real_realloc(void *ptr, size_t size);

static void bench_count_allocation(size_t size)
{
	atomic_fetch_add_explicit(&bench_alloc_calls, 1U, memory_order_relaxed);
	atomic_fetch_add_explicit(&bench_alloc_requested_bytes, (uint64_t)size,
	                          memory_order_relaxed);
}

void *__wrap_malloc(size_t size)
{
	void *ptr = __real_malloc(size);
	if (ptr)
		bench_count_allocation(size);
	return ptr;
}

void *__wrap_calloc(size_t count, size_t size)
{
	void *ptr = __real_calloc(count, size);
	if (ptr && size != 0U && count <= SIZE_MAX / size)
		bench_count_allocation(count * size);
	return ptr;
}

void *__wrap_realloc(void *ptr, size_t size)
{
	void *result = __real_realloc(ptr, size);
	if (result)
		bench_count_allocation(size);
	return result;
}

enum bench_scenario {
	BENCH_SMALL,
	BENCH_BULK_COMPRESSIBLE,
	BENCH_BULK_INCOMPRESSIBLE,
	BENCH_MIXED
};

enum bench_compression { BENCH_COMPRESSION_NONE, BENCH_COMPRESSION_ZSTD };

struct bench_options {
	int server;
	int tls;
	uint32_t port;
	uint32_t index;
	uint32_t clients;
	uint32_t warmup;
	uint32_t requests;
	enum bench_scenario scenario;
	enum bench_compression compression;
	const char *host;
	const char *ca_file;
	const char *server_cert;
	const char *server_key;
	const char *client_cert;
	const char *client_key;
};

struct bench_usage {
	double cpu_s;
	long peak_rss_kib;
	uint64_t alloc_calls;
	uint64_t alloc_requested_bytes;
};

struct bench_server_state {
	pthread_mutex_t lock;
	pthread_cond_t changed;
	uint32_t clients;
	uint32_t warmup;
	uint32_t requests;
	uint32_t warmup_received;
	uint32_t measure_received;
	uint32_t verified;
	uint32_t measure_verified;
	uint32_t invalid;
	uint8_t *seen[BENCH_MAX_CLIENTS];
	uint8_t *partial_message[BENCH_MAX_CLIENTS];
	uint32_t partial_bytes[BENCH_MAX_CLIENTS];
	uint64_t partial_message_id[BENCH_MAX_CLIENTS];
	uint8_t partial_active[BENCH_MAX_CLIENTS];
	uint64_t *latencies_ns;
	uint32_t latency_count;
	uint64_t first_submit_ns;
	uint64_t last_receive_ns;
	int measure_started;
	int tls;
	enum bench_scenario scenario;
	int failure_status;
};

struct bench_client_state {
	pthread_mutex_t lock;
	pthread_cond_t changed;
	struct tr_connection_group_id group;
	uint32_t index;
	int ready;
	int failed;
};

static void bench_fail(const char *message, int status)
{
	fprintf(stderr, "bench_connection_group: %s (status=%d)\n", message,
	        status);
	exit(EXIT_FAILURE);
}

static uint64_t bench_now_ns(void)
{
	struct timespec now;
	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
		bench_fail("clock_gettime failed", errno);
	return (uint64_t)now.tv_sec * UINT64_C(1000000000) +
	       (uint64_t)now.tv_nsec;
}

static void bench_sleep_ms(uint32_t milliseconds)
{
	struct timespec delay;
	delay.tv_sec = (time_t)(milliseconds / 1000U);
	delay.tv_nsec = (long)(milliseconds % 1000U) * 1000000L;
	while (nanosleep(&delay, &delay) != 0 && errno == EINTR)
		;
}

static int bench_cond_init(pthread_cond_t *condition)
{
	pthread_condattr_t attributes;
	int ret = pthread_condattr_init(&attributes);
	if (ret != 0)
		return ret;
	ret = pthread_condattr_setclock(&attributes, CLOCK_MONOTONIC);
	if (ret == 0)
		ret = pthread_cond_init(condition, &attributes);
	(void)pthread_condattr_destroy(&attributes);
	return ret;
}

static struct timespec bench_deadline(uint32_t timeout_ms)
{
	struct timespec deadline;
	if (clock_gettime(CLOCK_MONOTONIC, &deadline) != 0)
		bench_fail("clock_gettime failed", errno);
	deadline.tv_sec += (time_t)(timeout_ms / 1000U);
	deadline.tv_nsec += (long)(timeout_ms % 1000U) * 1000000L;
	if (deadline.tv_nsec >= 1000000000L) {
		deadline.tv_sec++;
		deadline.tv_nsec -= 1000000000L;
	}
	return deadline;
}

static void bench_put_u64(uint8_t *bytes, uint64_t value)
{
	unsigned index;
	for (index = 0U; index < 8U; ++index)
		bytes[index] = (uint8_t)(value >> (index * 8U));
}

static uint64_t bench_get_u64(const uint8_t *bytes)
{
	uint64_t value = 0U;
	unsigned index;
	for (index = 0U; index < 8U; ++index)
		value |= (uint64_t)bytes[index] << (index * 8U);
	return value;
}

static int bench_body_is_incompressible(enum bench_scenario scenario)
{
	return scenario == BENCH_BULK_INCOMPRESSIBLE;
}

static uint32_t bench_body_size(enum bench_scenario scenario, uint64_t sequence)
{
	if (scenario == BENCH_SMALL)
		return BENCH_SMALL_BODY;
	if (scenario == BENCH_MIXED && (sequence % 4U) != 0U)
		return BENCH_SMALL_BODY;
	return BENCH_BULK_BODY;
}

static void bench_fill_body(uint8_t *body, enum bench_scenario scenario,
                            uint64_t sequence, uint32_t body_size)
{
	uint32_t index;
	uint32_t state;
	if (bench_body_is_incompressible(scenario)) {
		state = (uint32_t)sequence ^ UINT32_C(0x9e3779b9);
		if (state == 0U)
			state = UINT32_C(0xa341316c);
		for (index = 0U; index < body_size; ++index) {
			state ^= state << 13;
			state ^= state >> 17;
			state ^= state << 5;
			body[index] = (uint8_t)(state >> 24);
		}
		return;
	}
	for (index = 0U; index < body_size; ++index) {
		if (scenario == BENCH_BULK_COMPRESSIBLE ||
		    (scenario == BENCH_MIXED && (sequence % 4U) == 0U))
			body[index] = (uint8_t)('A' + (index % 7U));
		else
			body[index] =
			        (uint8_t)(sequence * 31U + index * 17U + 9U);
	}
}

static int bench_body_matches(const uint8_t *body, enum bench_scenario scenario,
                              uint64_t sequence, uint32_t body_size)
{
	uint32_t index;
	uint32_t state;
	if (bench_body_is_incompressible(scenario)) {
		state = (uint32_t)sequence ^ UINT32_C(0x9e3779b9);
		if (state == 0U)
			state = UINT32_C(0xa341316c);
		for (index = 0U; index < body_size; ++index) {
			state ^= state << 13;
			state ^= state >> 17;
			state ^= state << 5;
			if (body[index] != (uint8_t)(state >> 24))
				return 0;
		}
		return 1;
	}
	for (index = 0U; index < body_size; ++index) {
		uint8_t expected;
		if (scenario == BENCH_BULK_COMPRESSIBLE ||
		    (scenario == BENCH_MIXED && (sequence % 4U) == 0U))
			expected = (uint8_t)('A' + (index % 7U));
		else
			expected = (uint8_t)(sequence * 31U + index * 17U + 9U);
		if (body[index] != expected)
			return 0;
	}
	return 1;
}

static uint64_t bench_hash(const uint8_t *bytes, size_t length)
{
	uint64_t hash = UINT64_C(1469598103934665603);
	size_t index;
	for (index = 0U; index < length; ++index) {
		hash ^= bytes[index];
		hash *= UINT64_C(1099511628211);
	}
	return hash;
}

static uint32_t bench_build_message(uint8_t *message,
                                    enum bench_scenario scenario,
                                    uint64_t sequence, uint64_t submitted_ns)
{
	uint32_t body_size = bench_body_size(scenario, sequence);
	uint8_t *body = message + BENCH_HEADER_SIZE;
	uint64_t hash;
	bench_fill_body(body, scenario, sequence, body_size);
	hash = bench_hash(body, body_size);
	bench_put_u64(message, sequence);
	bench_put_u64(message + 8U, submitted_ns);
	bench_put_u64(message + 16U, hash);
	return BENCH_HEADER_SIZE + body_size;
}

static int bench_scenario_parse(const char *name, enum bench_scenario *out)
{
	if (!name || !out)
		return 0;
	if (strcmp(name, "small") == 0)
		*out = BENCH_SMALL;
	else if (strcmp(name, "bulk-compressible") == 0)
		*out = BENCH_BULK_COMPRESSIBLE;
	else if (strcmp(name, "bulk-incompressible") == 0)
		*out = BENCH_BULK_INCOMPRESSIBLE;
	else if (strcmp(name, "mixed") == 0)
		*out = BENCH_MIXED;
	else
		return 0;
	return 1;
}

static const char *bench_scenario_name(enum bench_scenario scenario)
{
	switch (scenario) {
	case BENCH_SMALL:
		return "small";
	case BENCH_BULK_COMPRESSIBLE:
		return "bulk-compressible";
	case BENCH_BULK_INCOMPRESSIBLE:
		return "bulk-incompressible";
	case BENCH_MIXED:
		return "mixed";
	}
	return "invalid";
}

static int bench_parse_u32(const char *value, uint32_t *out)
{
	char *end = NULL;
	unsigned long parsed;
	if (!value || !out || value[0] == '\0' || value[0] == '-')
		return 0;
	errno = 0;
	parsed = strtoul(value, &end, 10);
	if (errno || !end || *end != '\0' || parsed > UINT32_MAX)
		return 0;
	*out = (uint32_t)parsed;
	return 1;
}

static int bench_option_value(int argc, char **argv, int *index,
                              const char **out)
{
	if (*index + 1 >= argc)
		return 0;
	*out = argv[++(*index)];
	return 1;
}

static int bench_parse_options(int argc, char **argv,
                               struct bench_options *options)
{
	int index;
	memset(options, 0, sizeof(*options));
	if (argc < 2)
		return 0;
	if (strcmp(argv[1], "server") == 0)
		options->server = 1;
	else if (strcmp(argv[1], "client") == 0)
		options->server = 0;
	else
		return 0;
	options->port = options->server ? 0U : UINT32_MAX;
	options->clients = 1U;
	options->warmup = 100U;
	options->requests = 1000U;
	options->host = "127.0.0.1";
	options->scenario = BENCH_SMALL;
	options->compression = BENCH_COMPRESSION_NONE;
	for (index = 2; index < argc; ++index) {
		const char *value;
		if (!bench_option_value(argc, argv, &index, &value))
			return 0;
		if (strcmp(argv[index - 1], "--port") == 0) {
			if (!bench_parse_u32(value, &options->port) ||
			    options->port > UINT16_MAX)
				return 0;
		} else if (strcmp(argv[index - 1], "--clients") == 0) {
			if (!bench_parse_u32(value, &options->clients))
				return 0;
		} else if (strcmp(argv[index - 1], "--index") == 0) {
			if (!bench_parse_u32(value, &options->index))
				return 0;
		} else if (strcmp(argv[index - 1], "--warmup") == 0) {
			if (!bench_parse_u32(value, &options->warmup))
				return 0;
		} else if (strcmp(argv[index - 1], "--requests") == 0) {
			if (!bench_parse_u32(value, &options->requests))
				return 0;
		} else if (strcmp(argv[index - 1], "--tls") == 0) {
			uint32_t enabled;
			if (!bench_parse_u32(value, &enabled) || enabled > 1U)
				return 0;
			options->tls = (int)enabled;
		} else if (strcmp(argv[index - 1], "--compression") == 0) {
			if (strcmp(value, "none") == 0)
				options->compression = BENCH_COMPRESSION_NONE;
			else if (strcmp(value, "zstd") == 0)
				options->compression = BENCH_COMPRESSION_ZSTD;
			else
				return 0;
		} else if (strcmp(argv[index - 1], "--scenario") == 0) {
			if (!bench_scenario_parse(value, &options->scenario))
				return 0;
		} else if (strcmp(argv[index - 1], "--host") == 0) {
			options->host = value;
		} else if (strcmp(argv[index - 1], "--ca") == 0) {
			options->ca_file = value;
		} else if (strcmp(argv[index - 1], "--server-cert") == 0) {
			options->server_cert = value;
		} else if (strcmp(argv[index - 1], "--server-key") == 0) {
			options->server_key = value;
		} else if (strcmp(argv[index - 1], "--client-cert") == 0) {
			options->client_cert = value;
		} else if (strcmp(argv[index - 1], "--client-key") == 0) {
			options->client_key = value;
		} else {
			return 0;
		}
	}
	if (!options->requests || options->requests > BENCH_MAX_REQUESTS ||
	    options->warmup > BENCH_MAX_REQUESTS || options->clients == 0U ||
	    options->clients > BENCH_MAX_CLIENTS ||
	    (!options->server && options->index >= BENCH_MAX_CLIENTS) ||
	    (!options->server && options->port == UINT32_MAX))
		return 0;
	if (options->tls) {
		if (!options->ca_file)
			return 0;
		if (options->server &&
		    (!options->server_cert || !options->server_key))
			return 0;
		if (!options->server &&
		    (!options->client_cert || !options->client_key))
			return 0;
	}
	return 1;
}

static double bench_timeval_seconds(const struct timeval *time)
{
	return (double)time->tv_sec + (double)time->tv_usec / 1000000.0;
}

static void bench_usage_snapshot(struct bench_usage *out)
{
	struct rusage usage;
	if (getrusage(RUSAGE_SELF, &usage) != 0)
		bench_fail("getrusage failed", errno);
	out->cpu_s = bench_timeval_seconds(&usage.ru_utime) +
	             bench_timeval_seconds(&usage.ru_stime);
	out->peak_rss_kib = usage.ru_maxrss;
	out->alloc_calls =
	        atomic_load_explicit(&bench_alloc_calls, memory_order_relaxed);
	out->alloc_requested_bytes = atomic_load_explicit(
	        &bench_alloc_requested_bytes, memory_order_relaxed);
}

static void bench_usage_delta(const struct bench_usage *start,
                              struct bench_usage *end)
{
	bench_usage_snapshot(end);
	end->cpu_s -= start->cpu_s;
	end->alloc_calls -= start->alloc_calls;
	end->alloc_requested_bytes -= start->alloc_requested_bytes;
}

static uint32_t bench_group_index(const struct tr_connection_group_id *group,
                                  uint32_t clients)
{
	if (!group || group->group_id < BENCH_GROUP_BASE ||
	    group->group_id >= BENCH_GROUP_BASE + clients ||
	    group->epoch != BENCH_GROUP_EPOCH)
		return UINT32_MAX;
	return (uint32_t)(group->group_id - BENCH_GROUP_BASE);
}

static int bench_identity_is_tls(const struct tr_peer_identity_view *identity)
{
	return identity &&
	       identity->authentication == TR_PEER_IDENTITY_TLS_CERTIFICATE &&
	       identity->uri_san != NULL && identity->uri_san_len != 0U;
}

static int
bench_authorize(const struct tr_connection_group_authorization *authorization,
                void *arg)
{
	struct bench_server_state *state = arg;
	uint32_t index;
	int identity_ok;
	if (!authorization || !state)
		return TR_ERR_INVALID;
	if (bench_debug_enabled())
		fprintf(stderr,
		        "bench_debug authorize role=%d group=%llu epoch=%llu "
		        "peer_auth=%d peer_uri_len=%u control_auth=%d "
		        "control_uri_len=%u\n",
		        authorization->role,
		        (unsigned long long)authorization->group.group_id,
		        (unsigned long long)authorization->group.epoch,
		        authorization->peer_identity.authentication,
		        authorization->peer_identity.uri_san_len,
		        authorization->control_identity.authentication,
		        authorization->control_identity.uri_san_len);
	index = bench_group_index(&authorization->group, state->clients);
	if (index == UINT32_MAX ||
	    (authorization->role != TR_CONNECTION_GROUP_ROLE_CONTROL &&
	     authorization->role != TR_CONNECTION_GROUP_ROLE_DATA))
		return TR_ERR_STALE;
	identity_ok =
	        state->tls
	                ? bench_identity_is_tls(&authorization->peer_identity)
	                : authorization->peer_identity.authentication ==
	                          TR_PEER_IDENTITY_NONE;
	if (!identity_ok)
		return TR_ERR_INVALID;
	if (authorization->role == TR_CONNECTION_GROUP_ROLE_CONTROL) {
		if (authorization->control_identity.authentication !=
		    TR_PEER_IDENTITY_NONE)
			return TR_ERR_INVALID;
	} else if (state->tls) {
		if (!bench_identity_is_tls(&authorization->control_identity))
			return TR_ERR_INVALID;
	} else if (authorization->control_identity.authentication !=
	           TR_PEER_IDENTITY_NONE) {
		return TR_ERR_INVALID;
	}
	pthread_mutex_lock(&state->lock);
	pthread_cond_broadcast(&state->changed);
	pthread_mutex_unlock(&state->lock);
	return TR_OK;
}

static void bench_record_complete_message_locked(
        struct bench_server_state *state, uint32_t index, uint64_t message_id,
        const uint8_t *bytes, uint32_t length, uint64_t received_ns)
{
	uint32_t expected_body_size;
	uint32_t body_size;
	uint64_t sequence;
	uint64_t submitted_ns;
	uint64_t expected_hash;
	uint64_t observed_hash;
	int valid = 1;
	if (!bytes || length < BENCH_HEADER_SIZE) {
		valid = 0;
		sequence = UINT64_MAX;
		submitted_ns = 0U;
		expected_hash = 0U;
	} else {
		sequence = bench_get_u64(bytes);
		submitted_ns = bench_get_u64(bytes + 8U);
		expected_hash = bench_get_u64(bytes + 16U);
		body_size = length - BENCH_HEADER_SIZE;
		if (sequence >= (uint64_t)state->warmup + state->requests ||
		    message_id != sequence + 1U)
			valid = 0;
		expected_body_size =
		        valid ? bench_body_size(state->scenario, sequence) : 0U;
		if (valid && body_size != expected_body_size)
			valid = 0;
		observed_hash =
		        bench_hash(bytes + BENCH_HEADER_SIZE, body_size);
		if (valid && observed_hash != expected_hash)
			valid = 0;
		if (valid &&
		    !bench_body_matches(bytes + BENCH_HEADER_SIZE,
		                        state->scenario, sequence, body_size))
			valid = 0;
	}
	if (index == UINT32_MAX) {
		state->invalid++;
	} else if (sequence >= (uint64_t)state->warmup + state->requests) {
		state->invalid++;
		if (state->measure_started)
			state->measure_received++;
		else
			state->warmup_received++;
	} else if (state->seen[index][sequence]) {
		state->invalid++;
		if (state->measure_started)
			state->measure_received++;
		else
			state->warmup_received++;
	} else {
		state->seen[index][sequence] = 1U;
		if ((state->measure_started && sequence < state->warmup) ||
		    (!state->measure_started && sequence >= state->warmup))
			valid = 0;
		if (!valid)
			state->invalid++;
		else {
			state->verified++;
			if (state->measure_started)
				state->measure_verified++;
		}
		if (state->measure_started) {
			uint32_t sample =
			        index * state->requests +
			        (uint32_t)(sequence >= state->warmup
			                           ? sequence - state->warmup
			                           : 0U);
			state->measure_received++;
			if (sequence < state->warmup ||
			    sample >= state->clients * state->requests) {
				state->invalid++;
			} else if (valid && submitted_ns <= received_ns &&
			           state->latency_count <
			                   state->clients * state->requests) {
				state->latencies_ns[sample] =
				        received_ns - submitted_ns;
				state->latency_count++;
				if (!state->first_submit_ns ||
				    submitted_ns < state->first_submit_ns)
					state->first_submit_ns = submitted_ns;
				if (received_ns > state->last_receive_ns)
					state->last_receive_ns = received_ns;
			}
		} else {
			state->warmup_received++;
		}
	}
	pthread_cond_broadcast(&state->changed);
}

static enum tr_connection_group_message_disposition
bench_on_message(const struct tr_connection_group_message *message, void *arg)
{
	struct bench_server_state *state = arg;
	uint32_t index;
	uint32_t flags;
	uint32_t length;
	uint64_t received_ns;
	int invalid_fragment = 0;
	if (!state || !message)
		return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
	if (bench_debug_enabled())
		fprintf(stderr,
		        "bench_debug server message group=%llu stream=%u "
		        "id=%llu "
		        "flags=%u bytes=%u\n",
		        (unsigned long long)message->group.group_id,
		        message->stream_id,
		        (unsigned long long)message->message_id, message->flags,
		        message->bytes.len);
	index = bench_group_index(&message->group, state->clients);
	if (index == UINT32_MAX)
		return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
	received_ns = bench_now_ns();
	flags = message->flags;
	if ((flags & ~(TR_CONNECTION_GROUP_DATA_FIRST |
	               TR_CONNECTION_GROUP_DATA_LAST)) != 0U ||
	    message->stream_id != BENCH_STREAM_ID ||
	    (message->bytes.len && !message->bytes.data))
		invalid_fragment = 1;

	pthread_mutex_lock(&state->lock);
	if (invalid_fragment)
		goto fragment_error;
	if (flags & TR_CONNECTION_GROUP_DATA_FIRST) {
		if (state->partial_active[index])
			goto fragment_error;
		state->partial_active[index] = 1U;
		state->partial_message_id[index] = message->message_id;
		state->partial_bytes[index] = 0U;
	} else if (!state->partial_active[index] ||
	           state->partial_message_id[index] != message->message_id) {
		goto fragment_error;
	}
	length = message->bytes.len;
	if (length > BENCH_MESSAGE_LIMIT - state->partial_bytes[index])
		goto fragment_error;
	if (length)
		memcpy(state->partial_message[index] +
		               state->partial_bytes[index],
		       message->bytes.data, length);
	state->partial_bytes[index] += length;
	if (flags & TR_CONNECTION_GROUP_DATA_LAST) {
		bench_record_complete_message_locked(
		        state, index, message->message_id,
		        state->partial_message[index],
		        state->partial_bytes[index], received_ns);
		state->partial_active[index] = 0U;
		state->partial_bytes[index] = 0U;
	}
	pthread_mutex_unlock(&state->lock);
	return TR_CONNECTION_GROUP_MESSAGE_RELEASE;

fragment_error:
	state->invalid++;
	state->failure_status = TR_ERR_INVALID;
	pthread_cond_broadcast(&state->changed);
	pthread_mutex_unlock(&state->lock);
	return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
}

static void bench_on_data_event(const struct tr_connection_group_id *group,
                                enum tr_connection_group_data_event event,
                                int status, void *arg)
{
	struct bench_server_state *state = arg;
	if (bench_debug_enabled())
		fprintf(stderr,
		        "bench_debug server data_event group=%llu epoch=%llu "
		        "event=%d status=%d\n",
		        (unsigned long long)(group ? group->group_id : 0U),
		        (unsigned long long)(group ? group->epoch : 0U), event,
		        status);
	if (!state || !group || event != TR_CONNECTION_GROUP_DATA_ERROR)
		return;
	pthread_mutex_lock(&state->lock);
	if (!state->failure_status)
		state->failure_status = status ? status : TR_ERR_SYS;
	pthread_cond_broadcast(&state->changed);
	pthread_mutex_unlock(&state->lock);
}

static int bench_wait_server_counter(struct bench_server_state *state,
                                     int waiting_for_warmup, uint32_t expected)
{
	struct timespec deadline = bench_deadline(BENCH_TIMEOUT_MS);
	int ret = 0;
	pthread_mutex_lock(&state->lock);
	for (;;) {
		uint32_t current = waiting_for_warmup ? state->warmup_received
		                                      : state->measure_received;
		if (current >= expected)
			break;
		if (state->failure_status) {
			ret = state->failure_status;
			break;
		}
		ret = pthread_cond_timedwait(&state->changed, &state->lock,
		                             &deadline);
		if (ret == ETIMEDOUT) {
			ret = TR_ERR_TIMEOUT;
			break;
		}
		if (ret != 0)
			break;
	}
	pthread_mutex_unlock(&state->lock);
	return ret;
}

static int bench_wait_server_ready(struct tr_server *server, uint32_t clients,
                                   uint32_t want_data)
{
	struct tr_connection_group_server_stats stats;
	struct timespec deadline = bench_deadline(BENCH_TIMEOUT_MS);
	for (;;) {
		struct timespec now;
		int ret = tr_server_connection_group_get_stats(server, &stats);
		if (ret != TR_OK)
			return ret;
		if ((!want_data && stats.groups_current >= clients) ||
		    (want_data && stats.data_connections_current >= clients))
			return TR_OK;
		if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
			return TR_ERR_SYS;
		if (now.tv_sec > deadline.tv_sec ||
		    (now.tv_sec == deadline.tv_sec &&
		     now.tv_nsec >= deadline.tv_nsec))
			return TR_ERR_TIMEOUT;
		bench_sleep_ms(1U);
	}
}

static int bench_send_data_offer(struct tr_server *server,
                                 struct tr_connection_group_id group,
                                 uint64_t message_id)
{
	uint64_t deadline =
	        bench_now_ns() + (uint64_t)BENCH_TIMEOUT_MS * 1000000U;
	for (;;) {
		int ret = tr_server_connection_group_send_data_offer(
		        server, group.group_id, group.epoch, message_id);
		if (ret != TR_AGAIN)
			return ret;
		if (bench_now_ns() >= deadline)
			return TR_ERR_TIMEOUT;
		bench_sleep_ms(1U);
	}
}

static int bench_send_transfer_ready(struct tr_server *server,
                                     struct tr_connection_group_id group,
                                     uint64_t message_id)
{
	uint64_t deadline =
	        bench_now_ns() + (uint64_t)BENCH_TIMEOUT_MS * 1000000U;
	for (;;) {
		int ret = tr_server_connection_group_send_transfer_ready(
		        server, group.group_id, group.epoch, BENCH_STREAM_ID,
		        message_id);
		if (ret != TR_AGAIN)
			return ret;
		if (bench_now_ns() >= deadline)
			return TR_ERR_TIMEOUT;
		bench_sleep_ms(1U);
	}
}

static int bench_server_mode(const struct bench_options *options)
{
	struct tr_server_config config;
	struct tr_server *server = NULL;
	struct bench_server_state state;
	struct bench_usage usage_start;
	struct bench_usage usage_end;
	uint16_t port = 0U;
	uint32_t index;
	uint32_t total_measure;
	uint64_t elapsed_ns;
	int ret;
	char command[64];
	memset(&state, 0, sizeof(state));
	state.clients = options->clients;
	state.warmup = options->warmup;
	state.requests = options->requests;
	state.tls = options->tls;
	state.scenario = options->scenario;
	if (pthread_mutex_init(&state.lock, NULL) != 0 ||
	    bench_cond_init(&state.changed) != 0)
		bench_fail("server synchronization init failed", TR_ERR_SYS);
	for (index = 0U; index < state.clients; ++index) {
		state.seen[index] =
		        calloc((size_t)state.warmup + state.requests,
		               sizeof(*state.seen[index]));
		state.partial_message[index] = malloc(BENCH_MESSAGE_LIMIT);
		if (!state.seen[index] || !state.partial_message[index])
			bench_fail("server sequence bitmap allocation failed",
			           TR_ERR_NOMEM);
	}
	state.latencies_ns = calloc((size_t)state.clients * state.requests,
	                            sizeof(*state.latencies_ns));
	if (!state.latencies_ns)
		bench_fail("server latency allocation failed", TR_ERR_NOMEM);

	tr_server_config_init(&config);
	config.keepalive_interval_ms = 0U;
	config.max_peers = state.clients * 2U;
	config.listen_backlog = (int)(state.clients * 4U);
	config.limits.max_frame_payload_bytes = 16384U;
	config.limits.max_message_bytes = BENCH_MESSAGE_LIMIT;
	config.connection_groups.max_groups = state.clients;
	config.connection_groups.max_connections = state.clients * 2U;
	config.connection_groups.max_data_connections_per_group = 1U;
	config.connection_groups.max_streams_per_group = 1U;
	config.connection_groups.authorize = bench_authorize;
	config.connection_groups.on_message = bench_on_message;
	config.connection_groups.on_data_event = bench_on_data_event;
	config.connection_groups.callback_arg = &state;
	if (options->compression == BENCH_COMPRESSION_ZSTD) {
		config.connection_groups.receive_compression_algorithms =
		        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
		config.connection_groups.max_encoded_message_bytes =
		        BENCH_MESSAGE_LIMIT;
		/* 与默认 Runtime MessageService 队列容量一致，覆盖单客户端
		 * 默认的 1000 条测量突发，同时仍受任务数和内存预算约束。
		 */
		config.connection_groups.max_inflight_compressed_messages =
		        1024U;
	}
	if (options->tls) {
		config.tls.mode = TR_TLS_REQUIRED;
		config.tls.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
		config.tls.certificate_file = options->server_cert;
		config.tls.private_key_file = options->server_key;
		config.tls.client_ca_file = options->ca_file;
	}
	ret = tr_server_create_transport(&config, &server);
	if (ret != TR_OK)
		bench_fail("tr_server_create_transport failed", ret);
	ret = tr_server_connection_group_listen(
	        server, "127.0.0.1", (uint16_t)options->port,
	        (int)(state.clients * 4U), &port);
	if (ret != TR_OK || port == 0U)
		bench_fail("connection-group listen failed", ret);
	ret = tr_server_start(server);
	if (ret != TR_OK)
		bench_fail("tr_server_start failed", ret);
	printf("{\"type\":\"server_ready\",\"schema\":%u,"
	       "\"api_generation\":\"connection_group_v2\","
	       "\"pid\":%ld,\"port\":%u}\n",
	       BENCH_SCHEMA, (long)getpid(), port);
	fflush(stdout);

	ret = bench_wait_server_ready(server, state.clients, 0U);
	if (ret != TR_OK)
		bench_fail("timed out waiting for CONTROL groups", ret);
	for (index = 0U; index < state.clients; ++index) {
		struct tr_connection_group_id group = {BENCH_GROUP_BASE + index,
		                                       BENCH_GROUP_EPOCH};
		ret = bench_send_data_offer(server, group,
		                            UINT64_C(1000) + index);
		if (ret != TR_OK)
			bench_fail("sending DATA_OFFER failed", ret);
	}
	ret = bench_wait_server_ready(server, state.clients, 1U);
	if (ret != TR_OK)
		bench_fail("timed out waiting for DATA routes", ret);
	for (index = 0U; index < state.clients; ++index) {
		struct tr_connection_group_id group = {BENCH_GROUP_BASE + index,
		                                       BENCH_GROUP_EPOCH};
		ret = bench_send_transfer_ready(server, group,
		                                BENCH_READY_MESSAGE_ID);
		if (ret != TR_OK)
			bench_fail("sending TRANSFER_READY failed", ret);
	}

	ret = bench_wait_server_counter(&state, 1,
	                                state.clients * state.warmup);
	if (ret != 0)
		bench_fail("timed out waiting for warmup messages", ret);
	bench_usage_snapshot(&usage_start);
	pthread_mutex_lock(&state.lock);
	state.measure_started = 1;
	pthread_mutex_unlock(&state.lock);
	printf("{\"type\":\"server_phase_ready\",\"schema\":%u,"
	       "\"api_generation\":\"connection_group_v2\","
	       "\"phase\":\"warmup_done\"}\n",
	       BENCH_SCHEMA);
	fflush(stdout);
	total_measure = state.clients * state.requests;
	ret = bench_wait_server_counter(&state, 0, total_measure);
	if (ret != 0)
		bench_fail("timed out waiting for measured messages", ret);
	bench_usage_delta(&usage_start, &usage_end);
	if (state.latency_count != total_measure)
		state.invalid++;
	pthread_mutex_lock(&state.lock);
	if (state.failure_status)
		ret = state.failure_status;
	else
		ret = TR_OK;
	pthread_mutex_unlock(&state.lock);
	if (ret != TR_OK)
		bench_fail("DATA route reported an error", ret);

	/* Sorting with an explicit integer comparator keeps latency statistics
	 * stable for both 32-bit and 64-bit platforms.
	 */
	if (!state.latency_count || !state.first_submit_ns ||
	    state.last_receive_ns <= state.first_submit_ns)
		bench_fail("server latency sample set is incomplete",
		           TR_ERR_STATE);
	{
		uint32_t p50_index = (state.latency_count + 1U) / 2U - 1U;
		uint32_t p99_index =
		        (state.latency_count * 99U + 99U) / 100U - 1U;
		uint64_t p50_ns;
		uint64_t p99_ns;
		uint32_t measure_valid_count;
		uint32_t invalid_count;
		pthread_mutex_lock(&state.lock);
		measure_valid_count = state.measure_verified;
		invalid_count = state.invalid;
		pthread_mutex_unlock(&state.lock);
		/* Insertion sort is bounded by the configured request count and
		 * avoids an additional comparator dependency in the callback
		 * path.
		 */
		for (index = 1U; index < state.latency_count; ++index) {
			uint64_t value = state.latencies_ns[index];
			uint32_t cursor = index;
			while (cursor > 0U &&
			       state.latencies_ns[cursor - 1U] > value) {
				state.latencies_ns[cursor] =
				        state.latencies_ns[cursor - 1U];
				--cursor;
			}
			state.latencies_ns[cursor] = value;
		}
		p50_ns = state.latencies_ns[p50_index];
		p99_ns = state.latencies_ns[p99_index];
		elapsed_ns = state.last_receive_ns - state.first_submit_ns;
		printf("{\"type\":\"server_phase\",\"schema\":%u,"
		       "\"api_generation\":\"connection_group_v2\","
		       "\"phase\":\"measure\",\"scenario\":\"%s\","
		       "\"expected\":%u,\"received\":%u,\"verified\":%u,"
		       "\"invalid\":%u,\"start_ns\":%llu,\"end_ns\":%llu,"
		       "\"elapsed_s\":%.9f,\"receive_rps\":%.3f,"
		       "\"latency_p50_us\":%.3f,\"latency_p99_us\":%.3f,"
		       "\"server_cpu_s\":%.6f,\"server_peak_rss_kib\":%ld,"
		       "\"server_alloc_calls\":%llu,"
		       "\"server_alloc_requested_bytes\":%llu}\n",
		       BENCH_SCHEMA, bench_scenario_name(options->scenario),
		       total_measure, state.measure_received,
		       measure_valid_count, invalid_count,
		       (unsigned long long)state.first_submit_ns,
		       (unsigned long long)state.last_receive_ns,
		       (double)elapsed_ns / 1e9,
		       (double)total_measure * 1e9 / (double)elapsed_ns,
		       (double)p50_ns / 1000.0, (double)p99_ns / 1000.0,
		       usage_end.cpu_s, usage_end.peak_rss_kib,
		       (unsigned long long)usage_end.alloc_calls,
		       (unsigned long long)usage_end.alloc_requested_bytes);
		fflush(stdout);
	}
	for (index = 0U; index < state.clients; ++index) {
		struct tr_connection_group_id group = {BENCH_GROUP_BASE + index,
		                                       BENCH_GROUP_EPOCH};
		if (bench_debug_enabled())
			fprintf(stderr,
			        "bench_debug release transfer begin index=%u\n",
			        index);
		ret = tr_server_connection_group_release_transfer(
		        server, group.group_id, group.epoch, BENCH_STREAM_ID);
		if (bench_debug_enabled())
			fprintf(stderr,
			        "bench_debug release transfer end index=%u "
			        "status=%d\n",
			        index, ret);
		if (ret != TR_OK)
			bench_fail("server transfer release failed", ret);
	}
	if (bench_debug_enabled())
		fprintf(stderr, "bench_debug release transfer loop complete\n");
	printf("{\"type\":\"server_release_ready\",\"schema\":%u,"
	       "\"api_generation\":\"connection_group_v2\","
	       "\"released_transfers\":%u}\n",
	       BENCH_SCHEMA, state.clients);
	fflush(stdout);
	if (bench_debug_enabled())
		fprintf(stderr, "bench_debug release ready emitted\n");
	if (!fgets(command, sizeof(command), stdin) || command[0] != 'Q')
		bench_fail("missing server shutdown gate", TR_ERR_INVALID);
	ret = tr_server_connection_group_stop(server);
	if (ret != TR_OK)
		bench_fail("connection-group stop failed", ret);
	ret = tr_server_drain(server, 10000U);
	if (ret != TR_OK)
		bench_fail("server drain failed", ret);
	ret = tr_server_destroy(server);
	if (ret != TR_OK)
		bench_fail("server destroy failed", ret);
	printf("{\"type\":\"server_exit\",\"schema\":%u,"
	       "\"api_generation\":\"connection_group_v2\","
	       "\"drain_status\":0,\"received\":%u,\"verified\":%u,"
	       "\"invalid\":%u}\n",
	       BENCH_SCHEMA, state.warmup_received + state.measure_received,
	       state.verified, state.invalid);
	fflush(stdout);
	for (index = 0U; index < state.clients; ++index) {
		free(state.seen[index]);
		free(state.partial_message[index]);
	}
	free(state.latencies_ns);
	(void)pthread_cond_destroy(&state.changed);
	(void)pthread_mutex_destroy(&state.lock);
	return 0;
}

static void bench_on_transfer_ready(const struct tr_connection_group_id *group,
                                    uint32_t stream_id, uint64_t message_id,
                                    void *arg)
{
	struct bench_client_state *state = arg;
	if (!state || !group || stream_id != BENCH_STREAM_ID ||
	    message_id != BENCH_READY_MESSAGE_ID ||
	    group->group_id != state->group.group_id ||
	    group->epoch != state->group.epoch) {
		if (state) {
			pthread_mutex_lock(&state->lock);
			state->failed = 1;
			pthread_cond_broadcast(&state->changed);
			pthread_mutex_unlock(&state->lock);
		}
		return;
	}
	pthread_mutex_lock(&state->lock);
	state->ready = 1;
	pthread_cond_broadcast(&state->changed);
	pthread_mutex_unlock(&state->lock);
}

static int bench_read_gate(char expected)
{
	char line[64];
	if (!fgets(line, sizeof(line), stdin) || line[0] != expected)
		return TR_ERR_INVALID;
	return TR_OK;
}

static int bench_wait_tx_drained(struct tr_client *client)
{
	uint64_t deadline =
	        bench_now_ns() + (uint64_t)BENCH_TIMEOUT_MS * 1000000U;
	for (;;) {
		struct tr_connection_group_client_stats stats;
		int ret = tr_client_connection_group_get_stats(client, &stats);
		if (ret != TR_OK)
			return ret;
		if (stats.send_bytes_inflight == 0U)
			return TR_OK;
		if (bench_now_ns() >= deadline)
			return TR_ERR_TIMEOUT;
		bench_sleep_ms(1U);
	}
}

static int bench_send_messages(struct tr_client *client,
                               const struct bench_options *options,
                               uint32_t start_sequence, uint32_t count,
                               uint32_t *attempted, uint32_t *accepted,
                               uint32_t *submit_again, uint64_t *start_ns,
                               uint64_t *end_ns)
{
	uint8_t *message = malloc(BENCH_HEADER_SIZE + BENCH_BULK_BODY);
	uint32_t offset;
	if (!message)
		return TR_ERR_NOMEM;
	for (offset = 0U; offset < count; ++offset) {
		uint64_t sequence = (uint64_t)start_sequence + offset;
		uint64_t retry_deadline =
		        bench_now_ns() + (uint64_t)BENCH_TIMEOUT_MS * 1000000U;
		for (;;) {
			struct tr_transport_bytes bytes;
			uint64_t now = bench_now_ns();
			uint32_t length = bench_build_message(
			        message, options->scenario, sequence, now);
			int ret;
			if (!*start_ns)
				*start_ns = now;
			(*attempted)++;
			bytes.data = message;
			bytes.len = length;
			if (options->compression == BENCH_COMPRESSION_NONE)
				ret = tr_client_connection_group_send(
				        client, BENCH_STREAM_ID, sequence + 1U,
				        &bytes);
			else
				ret = tr_client_connection_group_send_with_compression(
				        client, BENCH_STREAM_ID, sequence + 1U,
				        &bytes,
				        TR_CONNECTION_GROUP_COMPRESSION_ZSTD);
			if (ret == TR_OK) {
				(*accepted)++;
				*end_ns = bench_now_ns();
				break;
			}
			if (ret != TR_AGAIN) {
				free(message);
				return ret;
			}
			(*submit_again)++;
			if (bench_now_ns() >= retry_deadline) {
				free(message);
				return TR_ERR_TIMEOUT;
			}
			bench_sleep_ms(1U);
		}
	}
	free(message);
	return TR_OK;
}

static int bench_wait_client_ready(struct bench_client_state *state)
{
	struct timespec deadline = bench_deadline(BENCH_TIMEOUT_MS);
	int ret = 0;
	pthread_mutex_lock(&state->lock);
	while (!state->ready && !state->failed) {
		ret = pthread_cond_timedwait(&state->changed, &state->lock,
		                             &deadline);
		if (ret == ETIMEDOUT) {
			ret = TR_ERR_TIMEOUT;
			break;
		}
		if (ret != 0)
			break;
	}
	if (state->failed)
		ret = TR_ERR_STALE;
	else if (state->ready)
		ret = TR_OK;
	pthread_mutex_unlock(&state->lock);
	return ret;
}

static int bench_client_mode(const struct bench_options *options)
{
	struct tr_client_config config;
	struct tr_client *client = NULL;
	struct bench_client_state state;
	struct bench_usage usage_start;
	struct bench_usage usage_end;
	uint32_t attempted = 0U;
	uint32_t accepted = 0U;
	uint32_t submit_again = 0U;
	uint32_t submit_errors = 0U;
	uint64_t measure_start_ns = 0U;
	uint64_t measure_end_ns = 0U;
	uint64_t elapsed_ns;
	int ret;
	memset(&state, 0, sizeof(state));
	state.index = options->index;
	state.group.group_id = BENCH_GROUP_BASE + options->index;
	state.group.epoch = BENCH_GROUP_EPOCH;
	if (pthread_mutex_init(&state.lock, NULL) != 0 ||
	    bench_cond_init(&state.changed) != 0)
		bench_fail("client synchronization init failed", TR_ERR_SYS);
	tr_client_config_init(&config);
	config.keepalive_interval_ms = 0U;
	config.connect_timeout_ms = BENCH_TIMEOUT_MS;
	config.limits.max_frame_payload_bytes = 16384U;
	config.limits.max_message_bytes = BENCH_MESSAGE_LIMIT;
	config.connection_groups.max_data_connections = 1U;
	config.connection_groups.max_active_transfers = 1U;
	config.connection_groups.on_transfer_ready = bench_on_transfer_ready;
	config.connection_groups.callback_arg = &state;
	if (options->compression == BENCH_COMPRESSION_ZSTD) {
		config.connection_groups.send_compression_algorithms =
		        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
		config.connection_groups.max_encoded_message_bytes =
		        BENCH_MESSAGE_LIMIT;
	}
	if (options->tls) {
		config.tls.mode = TR_TLS_REQUIRED;
		config.tls.ca_file = options->ca_file;
		config.tls.expected_peer_name = "server.example.test";
		config.tls.sni_name = "server.example.test";
		config.tls.client_certificate_file = options->client_cert;
		config.tls.client_private_key_file = options->client_key;
	}
	ret = tr_client_create_transport(&config, &client);
	if (ret != TR_OK)
		bench_fail("tr_client_create_transport failed", ret);
	ret = tr_client_connection_group_connect(
	        client, options->host, (uint16_t)options->port, &state.group);
	if (ret != TR_OK)
		bench_fail("connection-group connect failed", ret);
	ret = bench_wait_client_ready(&state);
	if (ret != TR_OK)
		bench_fail("timed out waiting for TRANSFER_READY", ret);
	printf("{\"type\":\"client_ready\",\"schema\":%u,"
	       "\"api_generation\":\"connection_group_v2\","
	       "\"pid\":%ld,\"client_index\":%u}\n",
	       BENCH_SCHEMA, (long)getpid(), options->index);
	fflush(stdout);
	ret = bench_read_gate('W');
	if (ret != TR_OK)
		bench_fail("missing warmup gate", ret);
	ret = bench_send_messages(client, options, 0U, options->warmup,
	                          &attempted, &accepted, &submit_again,
	                          &measure_start_ns, &measure_end_ns);
	if (ret != TR_OK)
		bench_fail("sending warmup messages failed", ret);
	ret = bench_wait_tx_drained(client);
	if (ret != TR_OK)
		bench_fail("warmup TX drain failed", ret);
	if (bench_debug_enabled())
		fprintf(stderr,
		        "bench_debug client warmup index=%u attempted=%u "
		        "accepted=%u "
		        "submit_again=%u\n",
		        options->index, attempted, accepted, submit_again);
	printf("{\"type\":\"client_phase_ready\",\"schema\":%u,"
	       "\"api_generation\":\"connection_group_v2\","
	       "\"phase\":\"warmup_sent\",\"client_index\":%u}\n",
	       BENCH_SCHEMA, options->index);
	fflush(stdout);
	ret = bench_read_gate('M');
	if (ret != TR_OK)
		bench_fail("missing measure gate", ret);
	attempted = 0U;
	accepted = 0U;
	submit_again = 0U;
	submit_errors = 0U;
	measure_start_ns = 0U;
	measure_end_ns = 0U;
	bench_usage_snapshot(&usage_start);
	ret = bench_send_messages(
	        client, options, options->warmup, options->requests, &attempted,
	        &accepted, &submit_again, &measure_start_ns, &measure_end_ns);
	if (ret != TR_OK) {
		submit_errors++;
		bench_fail("sending measured messages failed", ret);
	}
	ret = bench_wait_tx_drained(client);
	if (ret != TR_OK)
		bench_fail("measured TX drain failed", ret);
	measure_end_ns = bench_now_ns();
	bench_usage_delta(&usage_start, &usage_end);
	elapsed_ns = measure_end_ns - measure_start_ns;
	if (!elapsed_ns || accepted != options->requests)
		bench_fail("client accepted count or timer is invalid",
		           TR_ERR_STATE);
	printf("{\"type\":\"client_phase\",\"schema\":%u,"
	       "\"api_generation\":\"connection_group_v2\","
	       "\"phase\":\"measure\",\"scenario\":\"%s\","
	       "\"client_index\":%u,\"attempted\":%u,\"accepted\":%u,"
	       "\"submit_again\":%u,\"submit_errors\":%u,"
	       "\"start_ns\":%llu,\"end_ns\":%llu,\"elapsed_s\":%.9f,"
	       "\"accepted_rps\":%.3f,\"client_cpu_s\":%.6f,"
	       "\"client_peak_rss_kib\":%ld,\"client_alloc_calls\":%llu,"
	       "\"client_alloc_requested_bytes\":%llu}\n",
	       BENCH_SCHEMA, bench_scenario_name(options->scenario),
	       options->index, attempted, accepted, submit_again, submit_errors,
	       (unsigned long long)measure_start_ns,
	       (unsigned long long)measure_end_ns, (double)elapsed_ns / 1e9,
	       (double)accepted * 1e9 / (double)elapsed_ns, usage_end.cpu_s,
	       usage_end.peak_rss_kib,
	       (unsigned long long)usage_end.alloc_calls,
	       (unsigned long long)usage_end.alloc_requested_bytes);
	fflush(stdout);
	ret = bench_read_gate('Q');
	if (ret != TR_OK)
		bench_fail("missing client shutdown gate", ret);
	ret = tr_client_connection_group_release_transfer(client,
	                                                  BENCH_STREAM_ID);
	if (ret != TR_OK)
		bench_fail("client transfer release failed", ret);
	ret = tr_client_connection_group_close(client);
	if (ret != TR_OK)
		bench_fail("client connection-group close failed", ret);
	ret = tr_client_destroy(client);
	if (ret != TR_OK)
		bench_fail("client destroy failed", ret);
	(void)pthread_cond_destroy(&state.changed);
	(void)pthread_mutex_destroy(&state.lock);
	return 0;
}

int main(int argc, char **argv)
{
	struct bench_options options;
	if (!bench_parse_options(argc, argv, &options)) {
		fprintf(stderr,
		        "usage: %s server|client [--port N] [--clients 1|2] "
		        "[--index N] [--host IPv4] [--warmup N] [--requests N] "
		        "[--scenario "
		        "small|bulk-compressible|bulk-incompressible|mixed] "
		        "[--compression none|zstd] [--tls 0|1] "
		        "TLS_FILE_OPTIONS\n",
		        argv[0]);
		return EXIT_FAILURE;
	}
	if (options.server)
		return bench_server_mode(&options);
	return bench_client_mode(&options);
}
