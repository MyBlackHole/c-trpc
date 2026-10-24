#define _POSIX_C_SOURCE 200809L
#include "tr/client.h"
#include "tr/rpc_v2.h"
#include "tr/server.h"
#include "tr/status.h"
#include "../src/facade_diagnostics_internal.h"

#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>
#include <zstd.h>

/* Standalone diagnostic; open-loop mode is bounded by explicit in-flight slots. */
#define SERVICE 1U
#define METHOD_SMALL 1U
#define METHOD_BULK 2U
#define METHOD_SLOW 3U
#define MAX_PAYLOAD (1024U * 1024U)
#define PHASE_GUARD_NS UINT64_C(60000000000)
#define BENCH_COMPRESS_REQUEST 1U
#define BENCH_COMPRESS_RESPONSE 2U

struct options {
	const char *role;
	const char *host;
	const char *scenario;
	const char *tls_ca_file, *tls_cert_file, *tls_key_file;
	uint32_t port, requests, window, warmup, small_bytes, bulk_bytes;
	uint32_t bulk_every, timeout_ms, slow_ms, capacity, start_gate;
	uint32_t rate_rps;
	uint32_t compression, tls;
};

struct method_context {
	uint32_t delay_ms;
	uint32_t compression;
};

extern void *__real_malloc(size_t size);
extern void *__real_calloc(size_t count, size_t size);
extern void *__real_realloc(void *pointer, size_t size);

static atomic_bool allocation_tracking;
static atomic_uint_fast64_t tracked_allocation_calls;
static atomic_uint_fast64_t tracked_allocation_bytes;

void *__wrap_malloc(size_t size)
{
	void *pointer = __real_malloc(size);
	if (atomic_load_explicit(&allocation_tracking, memory_order_relaxed)) {
		atomic_fetch_add_explicit(&tracked_allocation_calls, 1U,
		                          memory_order_relaxed);
		atomic_fetch_add_explicit(&tracked_allocation_bytes, size,
		                          memory_order_relaxed);
	}
	return pointer;
}

void *__wrap_calloc(size_t count, size_t size)
{
	void *pointer = __real_calloc(count, size);
	if (atomic_load_explicit(&allocation_tracking, memory_order_relaxed)) {
		atomic_fetch_add_explicit(&tracked_allocation_calls, 1U,
		                          memory_order_relaxed);
		if (size == 0U || count <= SIZE_MAX / size)
			atomic_fetch_add_explicit(&tracked_allocation_bytes,
			                          (uint64_t)count * size,
			                          memory_order_relaxed);
	}
	return pointer;
}

void *__wrap_realloc(void *old_pointer, size_t size)
{
	void *pointer = __real_realloc(old_pointer, size);
	if (atomic_load_explicit(&allocation_tracking, memory_order_relaxed)) {
		atomic_fetch_add_explicit(&tracked_allocation_calls, 1U,
		                          memory_order_relaxed);
		atomic_fetch_add_explicit(&tracked_allocation_bytes, size,
		                          memory_order_relaxed);
	}
	return pointer;
}

static void allocation_tracking_begin(void)
{
	atomic_store_explicit(&tracked_allocation_calls, 0U, memory_order_relaxed);
	atomic_store_explicit(&tracked_allocation_bytes, 0U, memory_order_relaxed);
	atomic_store_explicit(&allocation_tracking, true, memory_order_relaxed);
}

static void allocation_tracking_end(uint64_t *calls, uint64_t *bytes)
{
	atomic_store_explicit(&allocation_tracking, false, memory_order_relaxed);
	*calls = atomic_load_explicit(&tracked_allocation_calls, memory_order_relaxed);
	*bytes = atomic_load_explicit(&tracked_allocation_bytes, memory_order_relaxed);
}

struct sample {
	uint64_t scheduled_ns, start_ns, submit_ns, end_ns;
	uint32_t bytes, method;
	int submit_status, rpc_status, valid, scheduler_dropped;
};

struct client_run;
struct slot {
	struct client_run *run;
	struct sample *sample;
	unsigned char *payload;
	int message_seen, response_valid;
	int busy, done;
};

struct client_run {
	struct tr_client *client;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct slot *slots;
	uint32_t window;
	uint64_t scheduled_begin_ns;
	int bad_callback;
};

static void fatal(const char *operation, int code)
{
	fprintf(stderr, "%s failed (%d)\n", operation, code);
	exit(EXIT_FAILURE);
}

static void check(int code, const char *operation)
{
	if (code != 0)
		fatal(operation, code);
}

static void destroy_server_retry(struct tr_server *server)
{
	struct timespec pause = {0, 1000000L};
	unsigned attempt;

	for (attempt = 0; attempt < 10000U; ++attempt) {
		int ret = tr_server_destroy(server);
		if (ret == TR_OK)
			return;
		if (ret != TR_AGAIN)
			fatal("server destroy", ret);
		while (nanosleep(&pause, &pause) != 0 && errno == EINTR)
			;
		pause.tv_sec = 0;
		pause.tv_nsec = 1000000L;
	}
	fatal("server destroy retry limit", TR_AGAIN);
}

static uint64_t now_ns(void)
{
	struct timespec ts;
	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
		fatal("clock_gettime", errno);
	return (uint64_t)ts.tv_sec * UINT64_C(1000000000) + (uint64_t)ts.tv_nsec;
}

static void sleep_until_ns(uint64_t target_ns)
{
	struct timespec target = {
		(time_t)(target_ns / UINT64_C(1000000000)),
		(long)(target_ns % UINT64_C(1000000000))
	};
	int ret;

	do {
		ret = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &target, NULL);
	} while (ret == EINTR);
	if (ret != 0)
		fatal("clock_nanosleep", ret);
}

static double cpu_seconds(const struct rusage *usage)
{
	return (double)usage->ru_utime.tv_sec + (double)usage->ru_stime.tv_sec +
	       ((double)usage->ru_utime.tv_usec + (double)usage->ru_stime.tv_usec) / 1e6;
}

static uint32_t number(const char *text)
{
	char *end;
	unsigned long value;
	if (!*text || *text < '0' || *text > '9')
		fatal("expected unsigned decimal argument", 0);
	errno = 0;
	value = strtoul(text, &end, 10);
	if (errno || *end || value > UINT32_MAX)
		fatal("numeric argument out of range", 0);
	return (uint32_t)value;
}

static void usage(const char *program)
{
	fprintf(stderr,
	        "Usage: %s server|client [--host IPv4] [--port N]\n"
	        "  --scenario small|bulk|mixed|pressure|open --requests N "
	        "--window N\n"
	        "  --warmup N --small-bytes N --bulk-bytes N --bulk-every N\n"
	        "  --timeout-ms N --slow-ms N --capacity N --start-gate 0|1\n"
	        "  --rate-rps N --compression none|request|response|zstd --tls 0|1\n"
	        "  TLS paths: --tls-ca FILE --tls-cert FILE --tls-key FILE\n"
	        "Server binds loopback by default, prints readiness JSON, and "
	        "exits on stdin EOF.\n"
	        "pressure uses the delayed method, then measures recovery on "
	        "the SAME client.\n"
	        "open schedules METHOD_SLOW arrivals independently of callback "
	        "completion.\n",
	        program);
}

static struct options parse_options(int argc, char **argv)
{
	struct options o = {.host = "127.0.0.1",
	                    .scenario = "small",
	                    .port = 0,
	                    .requests = 2000,
	                    .window = 8,
	                    .warmup = 100,
	                    .small_bytes = 32,
	                    .bulk_bytes = 65536,
	                    .bulk_every = 4,
	                    .timeout_ms = 5000,
	                    .slow_ms = 25,
	                    .capacity = 64,
	                    .rate_rps = 200};
	struct in_addr address;
	int i;
	if (argc < 2 || (strcmp(argv[1], "server") && strcmp(argv[1], "client"))) {
		usage(argv[0]);
		exit(EXIT_FAILURE);
	}
	o.role = argv[1];
	for (i = 2; i < argc; i += 2) {
		if (i + 1 == argc) {
			usage(argv[0]);
			exit(EXIT_FAILURE);
		}
		if (!strcmp(argv[i], "--host")) { o.host = argv[i + 1]; continue; }
		if (!strcmp(argv[i], "--scenario")) { o.scenario = argv[i + 1]; continue; }
		if (!strcmp(argv[i], "--tls-ca")) { o.tls_ca_file = argv[i + 1]; continue; }
		if (!strcmp(argv[i], "--tls-cert")) { o.tls_cert_file = argv[i + 1]; continue; }
		if (!strcmp(argv[i], "--tls-key")) { o.tls_key_file = argv[i + 1]; continue; }
		if (!strcmp(argv[i], "--compression")) {
			if (!strcmp(argv[i + 1], "none")) o.compression = 0U;
			else if (!strcmp(argv[i + 1], "request"))
				o.compression = BENCH_COMPRESS_REQUEST;
			else if (!strcmp(argv[i + 1], "response"))
				o.compression = BENCH_COMPRESS_RESPONSE;
			else if (!strcmp(argv[i + 1], "zstd"))
				o.compression = BENCH_COMPRESS_REQUEST |
				                BENCH_COMPRESS_RESPONSE;
			else fatal("invalid compression direction", 0);
			continue;
		}
#define ARG(name, field) if (!strcmp(argv[i], name)) { o.field = number(argv[i + 1]); continue; }
		ARG("--port", port)
		ARG("--requests", requests)
		ARG("--window", window)
		ARG("--warmup", warmup)
		ARG("--small-bytes", small_bytes)
		ARG("--bulk-bytes", bulk_bytes)
		ARG("--bulk-every", bulk_every)
		ARG("--timeout-ms", timeout_ms)
		ARG("--slow-ms", slow_ms)
		ARG("--capacity", capacity)
		ARG("--start-gate", start_gate)
		ARG("--rate-rps", rate_rps)
		ARG("--tls", tls)
#undef ARG
		usage(argv[0]);
		fatal("unknown argument", 0);
	}
	if (inet_pton(AF_INET, o.host, &address) != 1 || o.port > 65535U ||
	    (!strcmp(o.role, "client") && o.port == 0) || o.requests == 0 ||
	    o.requests > 1000000U || o.warmup > 100000U || o.window == 0 ||
	    o.window > 256U || o.capacity < o.window || o.capacity > 256U ||
	    o.rate_rps == 0 || o.rate_rps > 1000000U || o.small_bytes < 8U ||
	    o.bulk_bytes < o.small_bytes || o.bulk_bytes > MAX_PAYLOAD ||
	    o.bulk_every == 0 || o.timeout_ms == 0 || o.timeout_ms > 30000U ||
	    o.slow_ms > 1000U || o.start_gate > 1U || o.tls > 1U ||
	    (o.tls && (!o.tls_ca_file || !o.tls_cert_file || !o.tls_key_file)) ||
	    (uint64_t)o.capacity * (o.bulk_bytes + 512U) > UINT64_C(67108864) ||
	    (strcmp(o.scenario, "small") && strcmp(o.scenario, "bulk") &&
	     strcmp(o.scenario, "mixed") && strcmp(o.scenario, "pressure") &&
	     strcmp(o.scenario, "open")))
		fatal("invalid configuration or memory limit", 0);
	return o;
}

static void configure_limits(
	struct tr_facade_limits *limits, const struct options *o)
{
	tr_facade_limits_init(limits);
	limits->max_streams = 2U * o->capacity;
	limits->max_calls = 2U * o->capacity;
	limits->max_methods = 4U;
	limits->max_frame_payload_bytes = 16384U;
	limits->max_message_bytes = o->bulk_bytes + 512U;
	if (limits->max_message_bytes < limits->max_frame_payload_bytes)
		limits->max_message_bytes = limits->max_frame_payload_bytes;
	limits->initial_window_bytes =
		(uint64_t)limits->max_message_bytes * 4U;
	limits->window_update_threshold_bytes = limits->max_message_bytes;
}

static struct tr_rpc_method_desc method(uint32_t id, uint32_t max_bytes)
{
	struct tr_rpc_method_desc m = {
		.service_id = SERVICE, .method_id = id,
		.request_cardinality = TR_RPC_ONE, .response_cardinality = TR_RPC_ONE,
		.request_codec_id = TR_RPC_CODEC_RAW, .response_codec_id = TR_RPC_CODEC_RAW,
		.max_request_bytes = max_bytes, .max_response_bytes = max_bytes
	};
	m.lane = id == METHOD_BULK ? TR_RPC_LANE_BULK : TR_RPC_LANE_CONTROL;
	return m;
}

static void method_enable_compression(struct tr_rpc_method_desc *descriptor,
	                              const struct options *options)
{
	if (!descriptor || !options || options->compression == 0U)
		return;
	if (options->compression & BENCH_COMPRESS_REQUEST)
		descriptor->request_compression_algorithms =
		        TR_RPC_COMPRESSION_MASK_ZSTD;
	if (options->compression & BENCH_COMPRESS_RESPONSE)
		descriptor->response_compression_algorithms =
		        TR_RPC_COMPRESSION_MASK_ZSTD;
}

static const char *compression_name(uint32_t compression)
{
	switch (compression) {
	case 0U: return "none";
	case BENCH_COMPRESS_REQUEST: return "request";
	case BENCH_COMPRESS_RESPONSE: return "response";
	default: return "zstd";
	}
}

static enum tr_rpc_status echo_v2(struct tr_rpc_v2_call_id call,
                                  const struct tr_rpc_v2_request *request,
                                  const struct tr_rpc_v2_cancel *cancel,
                                 struct tr_rpc_v2_reply *reply, void *arg)
{
	const struct method_context *context = arg;
	(void)call;
	(void)cancel;
	if (request->kind == TR_RPC_V2_REQUEST_MESSAGE && context->delay_ms) {
		struct timespec delay = { (time_t)(context->delay_ms / 1000U),
			(long)(context->delay_ms % 1000U) * 1000000L };
		while (nanosleep(&delay, &delay) != 0)
			if (errno != EINTR)
				return TR_RPC_STATUS_INTERNAL;
	}
	if (request->kind == TR_RPC_V2_REQUEST_MESSAGE) {
		int ret = context->compression
			? tr_rpc_v2_reply_append_with_compression(
				reply, &request->body, TR_RPC_COMPRESSION_ZSTD)
			: tr_rpc_v2_reply_append(reply, &request->body);
		if (ret != TR_OK)
			return TR_RPC_STATUS_RESOURCE_EXHAUSTED;
	}
	return TR_RPC_STATUS_OK;
}

static int run_server(const struct options *o)
{
	struct tr_server_config config;
	struct tr_server *server = NULL;
	struct tr_server_stats stats;
	struct rusage usage_after;
	struct tr_rpc_v2_method methods[3] = {0};
	struct method_context contexts[] = {
		{0U, !!(o->compression & BENCH_COMPRESS_RESPONSE)},
		{0U, !!(o->compression & BENCH_COMPRESS_RESPONSE)},
		{o->slow_ms, !!(o->compression & BENCH_COMPRESS_RESPONSE)}};
	uint64_t allocation_calls = 0U, allocation_bytes = 0U;
	uint16_t port;
	int status;
	tr_server_config_init(&config);
	configure_limits(&config.limits, o);
	config.max_peers = 32U;
	config.keepalive_interval_ms = 0;
	if (o->tls) {
		config.tls.mode = TR_TLS_REQUIRED;
		config.tls.certificate_file = o->tls_cert_file;
		config.tls.private_key_file = o->tls_key_file;
	}
	for (uint32_t id = 1U; id <= 3U; ++id) {
		methods[id - 1U].descriptor = method(id, o->bulk_bytes);
		method_enable_compression(&methods[id - 1U].descriptor, o);
		methods[id - 1U].work = echo_v2;
		methods[id - 1U].arg = &contexts[id - 1U];
	}
	check(tr_server_create(&config, methods, 3U, &server), "server create");
	check(tr_server_listen(server, o->host, (uint16_t)o->port, &port), "listen");
	check(tr_server_start(server), "server start");
	allocation_tracking_begin();
	printf("{\"schema\":2,\"api_generation\":\"rpc_v2\","
	       "\"type\":\"ready\",\"port\":%u,\"pid\":%ld,"
	       "\"capacity\":%u,\"slow_ms\":%u,\"compression\":\"%s\","
	       "\"tls_enabled\":%u}\n",
	       port, (long)getpid(), o->capacity, o->slow_ms,
	       compression_name(o->compression), o->tls);
	fflush(stdout);
	/* No global signal policy: the harness closes this process's stdin. */
	while (getchar() != EOF)
		;
	status = tr_server_drain(server, 5000U);
	allocation_tracking_end(&allocation_calls, &allocation_bytes);
	check(tr_server_get_stats(server, &stats), "server stats");
	check(getrusage(RUSAGE_SELF, &usage_after), "server getrusage");
	{
		printf("{\"schema\":2,\"api_generation\":\"rpc_v2\","
		       "\"type\":\"server_exit\",\"drain_status\":%d,"
		       "\"cpu_s\":%.9f,\"peak_rss_kib\":%ld,"
		       "\"alloc_calls\":%" PRIu64 ","
		       "\"alloc_requested_bytes\":%" PRIu64 ","
		       "\"reactor_turns\":%" PRIu64 ","
		       "\"command_budget_limit\":%" PRIu64 ","
		       "\"command_processed_total\":%" PRIu64 ","
		       "\"command_max_per_turn\":%" PRIu64 ","
		       "\"command_budget_hits\":%" PRIu64 ","
		       "\"epoll_polls\":%" PRIu64 ",\"epoll_waits\":%" PRIu64
		       ","
		       "\"command_send_enqueued\":%" PRIu64 ","
		       "\"command_send_full\":%" PRIu64 ","
		       "\"command_resume_rx_enqueued\":%" PRIu64 ","
		       "\"command_resume_rx_full\":%" PRIu64 ","
		       "\"command_call_enqueued\":%" PRIu64 ","
		       "\"command_call_full\":%" PRIu64 ","
		       "\"command_other_enqueued\":%" PRIu64 ","
		       "\"command_other_full\":%" PRIu64 ","
		       "\"command_queue_capacity\":%u,"
		       "\"command_queue_peak\":%u,\"completion_queue_peak\":%u,"
		       "\"command_queue_full_events\":%" PRIu64 ","
		       "\"completion_queue_full_events\":%" PRIu64 ","
		       "\"rx_pool_capacity\":%u,\"rx_pool_peak\":%u,"
		       "\"tx_pool_capacity\":%u,\"tx_pool_peak\":%u,"
		       "\"control_tx_pool_capacity\":%u,"
		       "\"control_tx_pool_peak\":%u,"
		       "\"rx_pool_exhausted_events\":%" PRIu64 ","
		       "\"tx_pool_exhausted_events\":%" PRIu64 ","
		       "\"control_tx_pool_exhausted_events\":%" PRIu64 ","
		       "\"rpc_executor_threads\":%u,"
		       "\"rpc_task_queue_capacity\":%" PRIu64 ","
		       "\"rpc_tasks_queued\":%" PRIu64 ","
		       "\"rpc_tasks_running\":%" PRIu64 ","
		       "\"rpc_calls_started\":%" PRIu64 ","
		       "\"rpc_calls_finished\":%" PRIu64 ","
		       "\"rpc_unavailable\":%" PRIu64 ","
		       "\"rpc_resource_exhausted\":%" PRIu64 ","
		       "\"reassembly_pool_capacity\":%u,"
		       "\"reassembly_pool_peak\":%u,"
		       "\"reassembly_pool_exhausted_events\":%" PRIu64 ","
		       "\"peers_ready_total\":%" PRIu64 ","
		       "\"peers_reaped_total\":%" PRIu64 "}\n",
		       status, cpu_seconds(&usage_after), usage_after.ru_maxrss,
		       allocation_calls, allocation_bytes,
		       stats.reactor.turns, stats.reactor.limits.commands,
		       stats.reactor.total.commands,
		       stats.reactor.max_per_turn.commands,
		       stats.reactor.budget_hits.commands,
		       stats.reactor.epoll_polls, stats.reactor.epoll_waits,
		       stats.reactor.command_send.enqueued,
		       stats.reactor.command_send.full_events,
		       stats.reactor.command_resume_rx.enqueued,
		       stats.reactor.command_resume_rx.full_events,
		       stats.reactor.command_call.enqueued,
		       stats.reactor.command_call.full_events,
		       stats.reactor.command_other.enqueued,
		       stats.reactor.command_other.full_events,
		       stats.reactor.command_queue.capacity,
		       stats.reactor.command_queue.peak,
		       stats.reactor.completion_queue.peak,
		       stats.reactor.command_queue.full_events,
		       stats.reactor.completion_queue.full_events,
		       stats.reactor.rx_buffer_pool.capacity,
		       stats.reactor.rx_buffer_pool.peak,
		       stats.reactor.tx_item_pool.capacity,
		       stats.reactor.tx_item_pool.peak,
		       stats.reactor.control_tx_item_pool.capacity,
		       stats.reactor.control_tx_item_pool.peak,
		       stats.reactor.rx_buffer_pool.exhausted_events,
		       stats.reactor.tx_item_pool.exhausted_events,
		       stats.reactor.control_tx_item_pool.exhausted_events,
		       stats.rpc.executor_threads,
		       stats.rpc.executor_queue_capacity_current,
		       stats.rpc.executor_queued_tasks_current,
		       stats.rpc.executor_running_tasks_current,
		       stats.rpc.semantic.calls_started,
		       stats.rpc.semantic.calls_finished,
		       stats.rpc.semantic.final_status[TR_RPC_STATUS_UNAVAILABLE],
		       stats.rpc.semantic.final_status[TR_RPC_STATUS_RESOURCE_EXHAUSTED],
		       stats.reassembly_pool.capacity,
		       stats.reassembly_pool.peak,
		       stats.reassembly_pool.exhausted_events,
		       stats.peers_ready_total, stats.peers_reaped_total);
	}
	destroy_server_retry(server);
	return status == TR_OK ? EXIT_SUCCESS : EXIT_FAILURE;
}

static void result_message(struct tr_rpc_v2_call call,
                           const struct tr_rpc_bytes *response, void *arg)
{
	struct slot *slot = arg;
	struct client_run *run = slot->run;
	(void)call;
	check(pthread_mutex_lock(&run->lock), "result lock");
	if (!slot->busy || slot->done || slot->message_seen) {
		run->bad_callback = 1;
	} else {
		slot->message_seen = 1;
		slot->response_valid =
		        response && response->len == slot->sample->bytes &&
		        response->data &&
		        !memcmp(response->data, slot->payload, response->len);
	}
	check(pthread_cond_broadcast(&run->cond), "result signal");
	check(pthread_mutex_unlock(&run->lock), "result unlock");
}

static void result_terminal(struct tr_rpc_v2_call call,
                            enum tr_rpc_status status,
                            enum tr_rpc_v2_delivery delivery, void *arg)
{
	struct slot *slot = arg;
	struct client_run *run = slot->run;
	(void)call;
	(void)delivery;
	check(pthread_mutex_lock(&run->lock), "terminal lock");
	if (!slot->busy || slot->done) {
		run->bad_callback = 1;
	} else {
		slot->sample->end_ns = now_ns();
		slot->sample->rpc_status = (int)status;
		slot->sample->valid =
		        status != TR_RPC_STATUS_OK ||
		        (slot->message_seen && slot->response_valid);
		slot->done = 1;
	}
	check(pthread_cond_broadcast(&run->cond), "terminal signal");
	check(pthread_mutex_unlock(&run->lock), "terminal unlock");
	/* The issuer may reuse this slot after the terminal callback returns.
	 */
}

static struct tr_rpc_v2_client_callbacks callbacks_for(struct slot *slot)
{
	struct tr_rpc_v2_client_callbacks callbacks = {
	        .message = result_message,
	        .terminal = result_terminal,
	        .arg = slot};
	return callbacks;
}

static int compare_u64(const void *a, const void *b)
{
	uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
	return (x > y) - (x < y);
}

static double percentile(const uint64_t *values, uint32_t count, uint32_t p)
{
	uint64_t rank;
	if (!count) return 0.0;
	rank = ((uint64_t)count * p + 99U) / 100U; /* nearest rank */
	return (double)values[rank - 1U] / 1000.0;
}

static size_t zstd_reference_encoded_size(uint32_t bytes)
{
	ZSTD_CCtx *context;
	uint8_t *source;
	uint8_t *destination;
	size_t capacity, encoded;
	if (bytes == 0U)
		return 0U;
	source = malloc(bytes);
	capacity = ZSTD_compressBound(bytes);
	destination = malloc(capacity);
	context = ZSTD_createCCtx();
	if (!source || !destination || !context) {
		free(source);
		free(destination);
		ZSTD_freeCCtx(context);
		return bytes;
	}
	memset(source, 0x5a, bytes);
	if (bytes >= 8U)
		memset(source, 0, 8U);
	if (ZSTD_isError(ZSTD_CCtx_setParameter(context, ZSTD_c_compressionLevel, 3)) ||
	    ZSTD_isError(ZSTD_CCtx_setParameter(context, ZSTD_c_checksumFlag, 1))) {
		free(source);
		free(destination);
		ZSTD_freeCCtx(context);
		return bytes;
	}
	encoded = ZSTD_compress2(context, destination, capacity, source, bytes);
	free(source);
	free(destination);
	ZSTD_freeCCtx(context);
	return ZSTD_isError(encoded) ? bytes : encoded;
}

static void report_class(const char *name, const struct sample *samples,
			 uint32_t count, uint32_t selected, double elapsed)
{
	uint64_t *ok_lat = malloc((size_t)count * sizeof(*ok_lat));
	uint64_t *all_lat = malloc((size_t)count * sizeof(*all_lat));
	uint64_t bytes = 0, reference_encoded_bytes = 0;
	uint32_t i, attempts = 0, accepted = 0, ok = 0, deadlines = 0;
	uint32_t rpc_errors = 0, again = 0, submit_errors = 0, invalid = 0;
	if (!ok_lat || !all_lat) fatal("latency allocation", errno);
	for (i = 0; i < count; ++i) {
		const struct sample *s = &samples[i];
		if (selected && s->method != selected) continue;
		attempts++;
		if (s->submit_status != TR_OK) {
			if (s->submit_status == TR_AGAIN) again++;
			else submit_errors++;
			continue;
		}
		all_lat[accepted++] = s->end_ns - s->start_ns;
		if (!s->valid) invalid++;
		else if (s->rpc_status == TR_RPC_STATUS_OK) {
			ok_lat[ok++] = s->end_ns - s->start_ns;
			bytes += 2U * (uint64_t)s->bytes;
			reference_encoded_bytes +=
			        2U * zstd_reference_encoded_size(s->bytes);
		} else if (s->rpc_status == TR_RPC_STATUS_DEADLINE_EXCEEDED) deadlines++;
		else rpc_errors++;
	}
	qsort(ok_lat, ok, sizeof(*ok_lat), compare_u64);
	qsort(all_lat, accepted, sizeof(*all_lat), compare_u64);
	printf("\"%s\":{\"attempted\":%u,\"accepted\":%u,\"ok\":%u,"
	       "\"completed\":%u,\"submit_again\":%u,\"submit_errors\":%u,\"deadlines\":%u,"
	       "\"rpc_errors\":%u,\"invalid_responses\":%u,\"ok_rps\":%.3f,"
	       "\"payload_MiB_s\":%.6f,\"ok_p50_us\":%.3f,\"ok_p99_us\":%.3f,"
	       "\"accepted_p99_us\":%.3f,\"reference_zstd_payload_ratio\":%.6f}",
	       name, attempts, accepted, ok,
	       accepted, again, submit_errors, deadlines, rpc_errors, invalid, ok / elapsed,
	       (double)bytes / (1048576.0 * elapsed), percentile(ok_lat, ok, 50U),
	       percentile(ok_lat, ok, 99U), percentile(all_lat, accepted, 99U),
	       bytes ? (double)reference_encoded_bytes / (double)bytes : 0.0);
	free(ok_lat);
	free(all_lat);
}

static int phase(struct client_run *run, const struct options *o, const char *name,
		 const char *scenario, uint32_t count, uint32_t timeout_ms, int report)
{
	struct sample *samples = calloc(count, sizeof(*samples));
	struct rusage before, after;
	uint64_t begin, end, guard;
	uint64_t allocation_calls = 0U, allocation_bytes = 0U;
	uint32_t sent = 0, completed = 0, i;
	int failed = 0;
	if (!samples) fatal("sample allocation", errno);
	check(getrusage(RUSAGE_SELF, &before), "client getrusage");
	allocation_tracking_begin();
	begin = now_ns();
	guard = begin + PHASE_GUARD_NS;
	check(pthread_mutex_lock(&run->lock), "phase lock");
	while (completed < count && !failed) {
		for (i = 0; i < run->window; ++i) {
			struct slot *slot = &run->slots[i];
			struct tr_rpc_call_options call_options = {
				.timeout_ms = timeout_ms,
				.request_compression = (o->compression & BENCH_COMPRESS_REQUEST)
					? TR_RPC_COMPRESSION_ZSTD : TR_RPC_COMPRESSION_NONE};
			struct tr_rpc_v2_call call;
			struct tr_rpc_v2_client_callbacks callbacks;
			struct tr_rpc_bytes request;
			struct sample *s;
			uint64_t sequence;
			unsigned j;
			int ret;
			if (slot->busy && slot->done) {
				if (!slot->sample->valid) failed = 1;
				slot->busy = 0;
				completed++;
			}
			if (failed || slot->busy || sent == count) continue;
			s = &samples[sent];
			s->method = !strcmp(scenario, "pressure") ? METHOD_SLOW :
				(!strcmp(scenario, "bulk") || (!strcmp(scenario, "mixed") &&
				 sent % o->bulk_every == 0) ? METHOD_BULK : METHOD_SMALL);
			s->bytes = s->method == METHOD_BULK ? o->bulk_bytes : o->small_bytes;
			memset(slot->payload, s->method == METHOD_BULK ? 0x5a : 0xa5, s->bytes);
			sequence = sent++;
			for (j = 0; j < 8U; ++j) slot->payload[j] = (unsigned char)(sequence >> (8U * j));
			slot->sample = s;
			slot->busy = 1;
			slot->done = 0;
			s->valid = 1;
			request.data = slot->payload;
			request.len = s->bytes;
			s->start_ns = now_ns();
			slot->message_seen = 0;
			slot->response_valid = 0;
			callbacks = callbacks_for(slot);
			/* Callback may run before submit returns. Never hold its mutex here. */
			check(pthread_mutex_unlock(&run->lock), "submit unlock");
			ret = tr_client_unary_call(
			        run->client, SERVICE, s->method, &request,
			        &call_options, &callbacks, &call);
			check(pthread_mutex_lock(&run->lock), "submit lock");
			s->submit_status = ret;
			if (ret != TR_OK) {
				if (slot->done) run->bad_callback = 1;
				s->end_ns = now_ns();
				slot->done = 1;
			}
		}
		if (completed == count || failed) break;
		if (now_ns() >= guard || run->bad_callback) { failed = 1; break; }
		/* No polling retry on TR_AGAIN: rejected attempts are recorded once. */
		for (i = 0; i < run->window; ++i)
			if (run->slots[i].busy && run->slots[i].done) break;
		if (i == run->window) {
			struct timespec until = { (time_t)(guard / UINT64_C(1000000000)),
				(long)(guard % UINT64_C(1000000000)) };
			int ret = pthread_cond_timedwait(&run->cond, &run->lock, &until);
			if (ret != 0) failed = 1;
		}
	}
	check(pthread_mutex_unlock(&run->lock), "phase unlock");
	end = now_ns();
	allocation_tracking_end(&allocation_calls, &allocation_bytes);
	check(getrusage(RUSAGE_SELF, &after), "client getrusage");
	if (failed || run->bad_callback) {
		fprintf(stderr, "phase %s: incomplete, duplicate callback or corrupt response (%u/%u)\n",
			name, completed, count);
		/* Keep samples and payloads alive until all library callbacks are joined. */
		check(tr_client_destroy(run->client), "client destroy");
		run->client = NULL;
		free(samples);
		return -1;
	}
	if (report) {
		double elapsed = (double)(end - begin) / 1e9;
		printf("{\"type\":\"phase\",\"schema\":2,"
		       "\"api_generation\":\"rpc_v2\",\"phase\":\"%s\","
		       "\"scenario\":\"%s\","
		       "\"pid\":%ld,\"window\":%u,\"start_ns\":%" PRIu64
		       ",\"end_ns\":%" PRIu64 ","
		       "\"elapsed_s\":%.9f,\"client_cpu_s\":%.9f,"
		       "\"client_peak_rss_kib\":%ld,\"compression\":\"%s\","
		       "\"tls_enabled\":%u,\"client_alloc_calls\":%" PRIu64
		       ",\"client_alloc_requested_bytes\":%" PRIu64 ",",
		       name, scenario, (long)getpid(), run->window, begin, end,
		       elapsed, cpu_seconds(&after) - cpu_seconds(&before),
		       after.ru_maxrss, compression_name(o->compression), o->tls,
		       allocation_calls, allocation_bytes);
		report_class("all", samples, count, 0U, elapsed);
		putchar(','); report_class("small", samples, count, METHOD_SMALL, elapsed);
		putchar(','); report_class("bulk", samples, count, METHOD_BULK, elapsed);
		putchar(','); report_class("slow", samples, count, METHOD_SLOW, elapsed);
		puts("}");
		fflush(stdout);
	}
	if (!report) {
		for (i = 0; i < count; ++i)
			if (samples[i].submit_status != TR_OK ||
			    samples[i].rpc_status != TR_RPC_STATUS_OK) {
				fprintf(stderr,
				        "phase %s: warmup sample %u submit=%d rpc=%d valid=%d\n",
				        name, i, samples[i].submit_status,
				        samples[i].rpc_status, samples[i].valid);
				failed = 1;
			}
	}
	free(samples);
	return failed ? -1 : 0;
}


static void report_open_phase(const struct sample *samples, uint32_t count,
			      uint32_t rate_rps, uint64_t begin,
			      uint64_t horizon, uint64_t end,
			      double client_cpu_s, long peak_rss_kib,
			      uint32_t window, uint64_t allocation_calls,
			      uint64_t allocation_bytes,
			      const struct options *options)
{
	uint64_t *ok_lat = malloc((size_t)count * sizeof(*ok_lat));
	uint64_t *all_lat = malloc((size_t)count * sizeof(*all_lat));
	uint64_t *lag = malloc((size_t)count * sizeof(*lag));
	uint64_t *submit_lag = malloc((size_t)count * sizeof(*submit_lag));
	uint64_t first_submit = 0, last_submit = 0;
	uint32_t submitted_in_window = 0, completed_in_window = 0;
	uint32_t accepted_in_window = 0;
	uint64_t bytes = 0;
	uint32_t i, attempted = 0, accepted = 0, ok = 0, deadlines = 0;
	uint32_t rpc_errors = 0, unavailable = 0, resource_exhausted = 0;
	uint32_t again = 0, submit_errors = 0, invalid = 0, dropped = 0;
	double arrival_window;
	double drain_elapsed;
	double drain_tail_ms;

	if (!ok_lat || !all_lat || !lag || !submit_lag)
		fatal("open-loop report allocation", errno);

	for (i = 0; i < count; ++i) {
		const struct sample *s = &samples[i];
		lag[i] = s->start_ns > s->scheduled_ns ?
			 s->start_ns - s->scheduled_ns : 0;
		if (s->scheduler_dropped) {
			dropped++;
			continue;
		}
		submit_lag[attempted++] =
		        s->submit_ns > s->scheduled_ns
		                ? s->submit_ns - s->scheduled_ns
		                : 0;
		if (!first_submit)
			first_submit = s->submit_ns;
		last_submit = s->submit_ns;
		if (s->submit_ns >= begin && s->submit_ns < horizon)
			submitted_in_window++;
		if (s->submit_status != TR_OK) {
			if (s->submit_status == TR_AGAIN)
				again++;
			else
				submit_errors++;
			continue;
		}
		if (s->submit_ns >= begin && s->submit_ns < horizon)
			accepted_in_window++;
		if (s->end_ns >= begin && s->end_ns < horizon)
			completed_in_window++;
		all_lat[accepted++] = s->end_ns - s->start_ns;
		if (!s->valid) {
			invalid++;
		} else if (s->rpc_status == TR_RPC_STATUS_OK) {
			ok_lat[ok++] = s->end_ns - s->start_ns;
			bytes += 2U * (uint64_t)s->bytes;
		} else if (s->rpc_status == TR_RPC_STATUS_DEADLINE_EXCEEDED) {
			deadlines++;
		} else {
			rpc_errors++;
			if (s->rpc_status == TR_RPC_STATUS_UNAVAILABLE)
				unavailable++;
			else if (s->rpc_status == TR_RPC_STATUS_RESOURCE_EXHAUSTED)
				resource_exhausted++;
		}
	}

	qsort(ok_lat, ok, sizeof(*ok_lat), compare_u64);
	qsort(all_lat, accepted, sizeof(*all_lat), compare_u64);
	qsort(lag, count, sizeof(*lag), compare_u64);
	qsort(submit_lag, attempted, sizeof(*submit_lag), compare_u64);
	arrival_window = (double)(horizon - begin) / 1e9;
	drain_elapsed = (double)(end - begin) / 1e9;
	drain_tail_ms = end > horizon ? (double)(end - horizon) / 1e6 : 0.0;

	printf("{\"type\":\"open_phase\",\"schema\":2,"
	       "\"api_generation\":\"rpc_v2\",\"phase\":\"measure\","
	       "\"scenario\":\"open\",\"pid\":%ld,\"window\":%u,"
	       "\"compression\":\"%s\",\"tls_enabled\":%u,"
	       "\"client_alloc_calls\":%" PRIu64
	       ",\"client_alloc_requested_bytes\":%" PRIu64 ","
	       "\"rate_rps\":%u,\"start_ns\":%" PRIu64
	       ",\"schedule_horizon_ns\":%" PRIu64 ","
	       "\"end_ns\":%" PRIu64
	       ",\"arrival_window_s\":%.9f,\"drain_elapsed_s\":%.9f,"
	       "\"drain_tail_ms\":%.6f,\"client_cpu_s\":%.9f,\"client_peak_rss_"
	       "kib\":%ld,"
	       "\"offered\":%u,\"scheduler_dropped\":%u,\"attempted\":%u,"
	       "\"accepted\":%u,\"completed\":%u,\"ok\":%u,\"submit_again\":%u,"
	       "\"submit_errors\":%u,\"deadlines\":%u,\"rpc_errors\":%u,"
	       "\"unavailable\":%u,\"resource_exhausted\":%u,"
	       "\"invalid_responses\":%u,\"ok_ratio\":%.9f,"
	       "\"ok_rps\":%.3f,\"payload_MiB_s\":%.6f,"
	       "\"ok_p50_us\":%.3f,\"ok_p99_us\":%.3f,\"accepted_p99_us\":%.3f,"
	       "\"scheduler_late_p50_us\":%.3f,\"scheduler_late_p99_us\":%.3f,"
	       "\"scheduler_late_max_us\":%.3f,"
	       "\"submission_schema\":1,\"first_submit_ns\":%" PRIu64 ","
	       "\"last_submit_ns\":%" PRIu64 ",\"submitted_in_window\":%u,"
	       "\"accepted_in_window\":%u,\"completed_in_window\":%u,\"submit_"
	       "late_p99_us\":%.3f,"
	       "\"submit_late_max_us\":%.3f}\n",
	       (long)getpid(), window,
	       compression_name(options->compression), options->tls,
	       allocation_calls, allocation_bytes, rate_rps, begin, horizon, end,
	       arrival_window, drain_elapsed, drain_tail_ms, client_cpu_s,
	       peak_rss_kib, count, dropped, attempted, accepted, accepted, ok,
	       again, submit_errors, deadlines, rpc_errors, unavailable,
	       resource_exhausted, invalid, (double)ok / (double)count,
	       ok / drain_elapsed, (double)bytes / (1048576.0 * drain_elapsed),
	       percentile(ok_lat, ok, 50U), percentile(ok_lat, ok, 99U),
	       percentile(all_lat, accepted, 99U), percentile(lag, count, 50U),
	       percentile(lag, count, 99U), (double)lag[count - 1U] / 1000.0,
	       first_submit, last_submit, submitted_in_window,
	       accepted_in_window, completed_in_window,
	       percentile(submit_lag, attempted, 99U),
	       attempted ? (double)submit_lag[attempted - 1U] / 1000.0 : 0.0);
	free(submit_lag);
	free(lag);
	free(all_lat);
	free(ok_lat);
}

static int open_phase(struct client_run *run, const struct options *o)
{
	struct sample *samples = calloc(o->requests, sizeof(*samples));
	struct rusage before, after;
	uint64_t begin, horizon, guard, end;
	uint64_t allocation_calls = 0U, allocation_bytes = 0U;
	uint32_t i;
	int failed = 0;

	if (!samples)
		fatal("open-loop sample allocation", errno);
	check(getrusage(RUSAGE_SELF, &before), "client getrusage");
	allocation_tracking_begin();
	begin = run->scheduled_begin_ns ? run->scheduled_begin_ns : now_ns();
	horizon = begin + ((uint64_t)o->requests * UINT64_C(1000000000)) /
			     o->rate_rps;
	guard = horizon > UINT64_MAX - PHASE_GUARD_NS ?
		UINT64_MAX : horizon + PHASE_GUARD_NS;

	for (i = 0; i < o->requests && !failed; ++i) {
		struct sample *s = &samples[i];
		struct slot *slot = NULL;
		struct tr_rpc_call_options call_options = {
			.timeout_ms = o->timeout_ms,
			.request_compression = (o->compression & BENCH_COMPRESS_REQUEST)
				? TR_RPC_COMPRESSION_ZSTD : TR_RPC_COMPRESSION_NONE
		};
		struct tr_rpc_v2_call call;
		struct tr_rpc_v2_client_callbacks callbacks;
		struct tr_rpc_bytes request;
		uint64_t sequence = i;
		uint32_t j;
		int ret;

		s->method = METHOD_SLOW;
		s->bytes = o->small_bytes;
		s->scheduled_ns = begin +
			((uint64_t)i * UINT64_C(1000000000)) / o->rate_rps;
		sleep_until_ns(s->scheduled_ns);
		s->start_ns = now_ns();

		check(pthread_mutex_lock(&run->lock), "open lock");
		for (j = 0; j < run->window; ++j) {
			if (run->slots[j].busy && run->slots[j].done)
				run->slots[j].busy = 0;
			if (!slot && !run->slots[j].busy)
				slot = &run->slots[j];
		}
		if (run->bad_callback) {
			failed = 1;
			check(pthread_mutex_unlock(&run->lock), "open unlock");
			break;
		}
		if (!slot) {
			s->scheduler_dropped = 1;
			s->end_ns = s->start_ns;
			s->valid = 1;
			check(pthread_mutex_unlock(&run->lock), "open drop unlock");
			continue;
		}

		memset(slot->payload, 0x3c, s->bytes);
		for (j = 0; j < 8U; ++j)
			slot->payload[j] = (unsigned char)(sequence >> (8U * j));
		slot->sample = s;
		slot->busy = 1;
		slot->done = 0;
		slot->message_seen = 0;
		slot->response_valid = 0;
		s->valid = 1;
		request.data = slot->payload;
		request.len = s->bytes;
		callbacks = callbacks_for(slot);
		check(pthread_mutex_unlock(&run->lock), "open submit unlock");

		/* 近似 API 入口，不是网络发送或服务端到达时间。 */
		s->submit_ns = now_ns();
		ret = tr_client_unary_call(run->client, SERVICE, METHOD_SLOW,
		                           &request, &call_options, &callbacks,
		                           &call);

		check(pthread_mutex_lock(&run->lock), "open submit lock");
		s->submit_status = ret;
		if (ret != TR_OK) {
			if (slot->done)
				run->bad_callback = 1;
			s->end_ns = now_ns();
			slot->done = 1;
		}
		check(pthread_mutex_unlock(&run->lock), "open submit done unlock");
	}

	check(pthread_mutex_lock(&run->lock), "open drain lock");
	while (!failed) {
		uint32_t busy = 0;

		for (i = 0; i < run->window; ++i) {
			if (run->slots[i].busy && run->slots[i].done)
				run->slots[i].busy = 0;
			if (run->slots[i].busy)
				busy++;
		}
		if (busy == 0)
			break;
		if (run->bad_callback || now_ns() >= guard) {
			failed = 1;
			break;
		}
		{
			struct timespec until = {
				(time_t)(guard / UINT64_C(1000000000)),
				(long)(guard % UINT64_C(1000000000))
			};
			int wait_ret =
				pthread_cond_timedwait(&run->cond, &run->lock, &until);
			if (wait_ret != 0 && wait_ret != EINTR)
				failed = 1;
		}
	}
	check(pthread_mutex_unlock(&run->lock), "open drain unlock");
	end = now_ns();
	allocation_tracking_end(&allocation_calls, &allocation_bytes);
	check(getrusage(RUSAGE_SELF, &after), "client getrusage");

	if (failed || run->bad_callback) {
		fputs("open phase: incomplete or duplicate callback\n", stderr);
		check(tr_client_destroy(run->client), "client destroy");
		run->client = NULL;
		free(samples);
		return -1;
	}

	report_open_phase(samples, o->requests, o->rate_rps, begin, horizon, end,
			  cpu_seconds(&after) - cpu_seconds(&before),
			  after.ru_maxrss, run->window, allocation_calls,
			  allocation_bytes, o);
	fflush(stdout);
	free(samples);
	return 0;
}

static int run_client(const struct options *o)
{
	struct tr_client_config config;
	struct tr_rpc_method_desc methods[3];
	struct client_run run = {0};
	pthread_condattr_t attr;
	uint32_t i;
	int status = EXIT_SUCCESS;
	tr_client_config_init(&config);
	configure_limits(&config.limits, o);
	config.keepalive_interval_ms = 0;
	config.enable_reconnect = 0;
	if (o->tls) {
		config.tls.mode = TR_TLS_REQUIRED;
		config.tls.ca_file = o->tls_ca_file;
		config.tls.expected_peer_name = "server.example.test";
	}
	check(pthread_mutex_init(&run.lock, NULL), "mutex init");
	check(pthread_condattr_init(&attr), "cond attr");
	check(pthread_condattr_setclock(&attr, CLOCK_MONOTONIC), "cond clock");
	check(pthread_cond_init(&run.cond, &attr), "cond init");
	check(pthread_condattr_destroy(&attr), "cond attr destroy");
	run.window = o->window;
	run.slots = calloc(run.window, sizeof(*run.slots));
	if (!run.slots) fatal("slots allocation", errno);
	for (i = 0; i < run.window; ++i) {
		run.slots[i].run = &run;
		run.slots[i].payload = malloc(o->bulk_bytes);
		if (!run.slots[i].payload) fatal("payload allocation", errno);
	}
	for (i = 1U; i <= 3U; ++i) {
		methods[i - 1U] = method(i, o->bulk_bytes);
		method_enable_compression(&methods[i - 1U], o);
	}
	check(tr_client_create(&config, methods, 3U, &run.client),
	      "V2 client create");
	check(tr_client_connect(run.client, o->host, (uint16_t)o->port),
	      "V2 client connect");
	check(tr_client_wait_ready(run.client, 5000U), "V2 client ready");
	if (o->warmup) {
		uint32_t measure_window = run.window;

		/*
		 * Keep warmup bounded to a single outstanding call in open-loop
		 * mode.
		 */
		if (!strcmp(o->scenario, "open"))
			run.window = 1U;
		if (phase(&run, o, "warmup", "small", o->warmup, 5000U, 0))
			status = EXIT_FAILURE;
		run.window = measure_window;
	}
	if (status == EXIT_SUCCESS && o->start_gate) {
		int gate_kind;

		printf("{\"schema\":2,\"api_generation\":\"rpc_v2\","
		       "\"type\":\"client_ready\",\"pid\":%ld}\n",
		       (long)getpid());
		fflush(stdout);
		gate_kind = getchar();
		if (gate_kind == EOF) {
			fputs("client start gate was not released\n", stderr);
			status = EXIT_FAILURE;
		} else if (gate_kind == 'g') {
			run.scheduled_begin_ns = 0;
		} else if (gate_kind == 't') {
			char timestamp[94];
			char *end = NULL;
			unsigned long long value;

			if (getchar() != ':' ||
			    !fgets(timestamp, sizeof(timestamp), stdin)) {
				fputs("client start gate timestamp was incomplete\n",
				      stderr);
				status = EXIT_FAILURE;
			} else {
				errno = 0;
				value = strtoull(timestamp, &end, 10);
				if (errno || end == timestamp ||
				    (*end != '\n' && *end != '\0') || value == 0) {
					fputs("client start gate timestamp was invalid\n",
					      stderr);
					status = EXIT_FAILURE;
				} else {
					run.scheduled_begin_ns = (uint64_t)value;
				}
			}
		} else {
			fputs("client start gate was invalid\n", stderr);
			status = EXIT_FAILURE;
		}
	}
	if (status == EXIT_SUCCESS) {
		if (!strcmp(o->scenario, "open")) {
			if (open_phase(&run, o))
				status = EXIT_FAILURE;
		} else if (phase(&run, o, "measure", o->scenario, o->requests,
				 o->timeout_ms, 1) ||
			   (!strcmp(o->scenario, "pressure") &&
			    phase(&run, o, "recovery", "small", 64U, 5000U, 1))) {
			status = EXIT_FAILURE;
		}
	}
	/* Pressure and recovery share one V2 client and one real TCP
	 * connection. */
	if (run.client) {
		check(tr_client_begin_drain(run.client),
		      "V2 client begin drain");
		check(tr_client_wait_drained(run.client, 5000U),
		      "V2 client drain");
		check(tr_client_destroy(run.client), "V2 client destroy");
	}
	for (i = 0; i < run.window; ++i) free(run.slots[i].payload);
	free(run.slots);
	check(pthread_cond_destroy(&run.cond), "cond destroy");
	check(pthread_mutex_destroy(&run.lock), "mutex destroy");
	return status;
}

int main(int argc, char **argv)
{
	struct options options = parse_options(argc, argv);
	return !strcmp(options.role, "server") ? run_server(&options) : run_client(&options);
}
