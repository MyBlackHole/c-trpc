#define _POSIX_C_SOURCE 200809L
#include "tr/client.h"
#include "tr/server.h"
#include "tr/status.h"

#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

/* Standalone diagnostic, not a production server or an open-loop load source. */
#define SERVICE 1U
#define METHOD_SMALL 1U
#define METHOD_BULK 2U
#define METHOD_SLOW 3U
#define MAX_PAYLOAD (1024U * 1024U)
#define PHASE_GUARD_NS UINT64_C(60000000000)

struct options {
	const char *role;
	const char *host;
	const char *scenario;
	uint32_t port, requests, window, warmup, small_bytes, bulk_bytes;
	uint32_t bulk_every, timeout_ms, slow_ms, workers, capacity, start_gate;
};

struct sample {
	uint64_t start_ns, end_ns;
	uint32_t bytes, method;
	int submit_status, rpc_status, valid;
};

struct client_run;
struct slot {
	struct client_run *run;
	struct sample *sample;
	unsigned char *payload;
	int busy, done;
};

struct client_run {
	struct tr_client *client;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct slot *slots;
	uint32_t window;
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

static uint64_t now_ns(void)
{
	struct timespec ts;
	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
		fatal("clock_gettime", errno);
	return (uint64_t)ts.tv_sec * UINT64_C(1000000000) + (uint64_t)ts.tv_nsec;
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
		"  --scenario small|bulk|mixed|pressure --requests N --window N\n"
		"  --warmup N --small-bytes N --bulk-bytes N --bulk-every N\n"
		"  --timeout-ms N --slow-ms N --workers N --capacity N --start-gate 0|1\n"
		"Server binds loopback by default, prints readiness JSON, and exits on stdin EOF.\n"
		"pressure uses the delayed method, then measures recovery on the SAME client.\n",
		program);
}

static struct options parse_options(int argc, char **argv)
{
	struct options o = {
		.host = "127.0.0.1", .scenario = "small", .port = 0,
		.requests = 2000, .window = 8, .warmup = 100,
		.small_bytes = 32, .bulk_bytes = 65536, .bulk_every = 4,
		.timeout_ms = 5000, .slow_ms = 25, .workers = 4, .capacity = 64
	};
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
		ARG("--workers", workers)
		ARG("--capacity", capacity)
		ARG("--start-gate", start_gate)
#undef ARG
		usage(argv[0]);
		fatal("unknown argument", 0);
	}
	if (inet_pton(AF_INET, o.host, &address) != 1 || o.port > 65535U ||
	    (!strcmp(o.role, "client") && o.port == 0) ||
	    o.requests == 0 || o.requests > 1000000U || o.warmup > 100000U ||
	    o.window == 0 || o.window > 64U || o.capacity < o.window || o.capacity > 256U ||
	    o.workers == 0 || o.workers > 32U || o.small_bytes < 8U ||
	    o.bulk_bytes < o.small_bytes || o.bulk_bytes > MAX_PAYLOAD ||
	    o.bulk_every == 0 || o.timeout_ms == 0 || o.timeout_ms > 30000U ||
	    o.slow_ms > 1000U || o.start_gate > 1U ||
	    (uint64_t)o.capacity * (o.bulk_bytes + 512U) > UINT64_C(67108864) ||
	    (strcmp(o.scenario, "small") && strcmp(o.scenario, "bulk") &&
	     strcmp(o.scenario, "mixed") && strcmp(o.scenario, "pressure")))
		fatal("invalid configuration or memory limit", 0);
	return o;
}

static void configure_limits(struct tr_facade_limits *limits, const struct options *o)
{
	tr_facade_limits_init(limits);
	limits->max_streams = 2U * o->capacity;
	limits->max_calls = 2U * o->capacity;
	limits->max_methods = 4U;
	limits->max_frame_payload_bytes = 16384U;
	limits->max_message_bytes = o->bulk_bytes + 512U;
	if (limits->max_message_bytes < limits->max_frame_payload_bytes)
		limits->max_message_bytes = limits->max_frame_payload_bytes;
	limits->rpc_message_buffer_bytes = limits->max_message_bytes;
	limits->rpc_message_pool_count = 2U * o->capacity + 16U;
	limits->reassembly_pool_count = o->capacity + 8U;
	limits->rx_buffer_count = 2U * o->capacity + 16U;
	limits->initial_window_bytes = (uint64_t)limits->max_message_bytes * 4U;
	limits->window_update_threshold_bytes = limits->max_message_bytes;
	limits->executor_threads = o->workers;
	limits->executor_queue_capacity = 4U * o->capacity;
}

static struct tr_rpc_method_desc method(uint32_t id, uint32_t max_bytes)
{
	struct tr_rpc_method_desc m = {
		.service_id = SERVICE, .method_id = id,
		.request_cardinality = TR_RPC_ONE, .response_cardinality = TR_RPC_ONE,
		.request_codec_id = TR_RPC_CODEC_RAW, .response_codec_id = TR_RPC_CODEC_RAW,
		.max_request_bytes = max_bytes, .max_response_bytes = max_bytes
	};
	m.lane = id == METHOD_BULK ? TR_LANE_BULK : TR_LANE_CONTROL;
	return m;
}

static int echo(struct tr_rpc_call_handle call, const struct tr_rpc_bytes *request,
		struct tr_rpc_unary_response *response, void *arg)
{
	const uint32_t *delay_ms = arg;
	(void)call;
	if (*delay_ms) {
		struct timespec delay = { (time_t)(*delay_ms / 1000U),
			(long)(*delay_ms % 1000U) * 1000000L };
		while (nanosleep(&delay, &delay) != 0)
			if (errno != EINTR)
				return TR_ERR_SYS;
	}
	/* Unary runtime copies this borrowed request before releasing its task. */
	response->status = TR_RPC_STATUS_OK;
	response->message = *request;
	return TR_OK;
}

static int run_server(const struct options *o)
{
	struct tr_server_config config;
	struct tr_server *server = NULL;
	struct rusage usage_after;
	uint32_t delays[] = {0U, 0U, o->slow_ms};
	uint16_t port;
	uint32_t id;
	int status;
	tr_server_config_init(&config);
	configure_limits(&config.limits, o);
	config.max_peers = 32U;
	config.keepalive_interval_ms = 0;
	check(tr_server_create(&config, &server), "server create");
	for (id = 1; id <= 3; ++id) {
		struct tr_rpc_method_desc m = method(id, o->bulk_bytes);
		check(tr_server_register_method(server, &m, echo, &delays[id - 1U]), "server method");
	}
	check(tr_server_listen(server, o->host, (uint16_t)o->port, &port), "listen");
	check(tr_server_start(server), "server start");
	printf("{\"type\":\"ready\",\"port\":%u,\"pid\":%ld,\"workers\":%u,\"capacity\":%u,\"slow_ms\":%u}\n",
	       port, (long)getpid(), o->workers, o->capacity, o->slow_ms);
	fflush(stdout);
	/* No global signal policy: the harness closes this process's stdin. */
	while (getchar() != EOF)
		;
	status = tr_server_drain(server, 5000U);
	tr_server_destroy(server);
	check(getrusage(RUSAGE_SELF, &usage_after), "server getrusage");
	printf("{\"type\":\"server_exit\",\"drain_status\":%d,\"cpu_s\":%.9f,\"peak_rss_kib\":%ld}\n",
	       status, cpu_seconds(&usage_after), usage_after.ru_maxrss);
	return status == TR_OK ? EXIT_SUCCESS : EXIT_FAILURE;
}

static void result(struct tr_rpc_call_handle call, int status,
		   const struct tr_rpc_bytes *response, void *arg)
{
	struct slot *slot = arg;
	struct client_run *run = slot->run;
	uint64_t end = now_ns();
	(void)call;
	check(pthread_mutex_lock(&run->lock), "result lock");
	if (!slot->busy || slot->done) {
		run->bad_callback = 1;
	} else {
		slot->sample->end_ns = end;
		slot->sample->rpc_status = status;
		slot->sample->valid = status != TR_RPC_STATUS_OK ||
			(response && response->len == slot->sample->bytes &&
			 response->data && !memcmp(response->data, slot->payload, response->len));
		slot->done = 1;
	}
	check(pthread_cond_broadcast(&run->cond), "result signal");
	check(pthread_mutex_unlock(&run->lock), "result unlock");
	/* No slot access after unlock: the issuer may now reuse it. */
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

static void report_class(const char *name, const struct sample *samples,
			 uint32_t count, uint32_t selected, double elapsed)
{
	uint64_t *ok_lat = malloc((size_t)count * sizeof(*ok_lat));
	uint64_t *all_lat = malloc((size_t)count * sizeof(*all_lat));
	uint64_t bytes = 0;
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
		} else if (s->rpc_status == TR_RPC_STATUS_DEADLINE_EXCEEDED) deadlines++;
		else rpc_errors++;
	}
	qsort(ok_lat, ok, sizeof(*ok_lat), compare_u64);
	qsort(all_lat, accepted, sizeof(*all_lat), compare_u64);
	printf("\"%s\":{\"attempted\":%u,\"accepted\":%u,\"ok\":%u,"
	       "\"completed\":%u,\"submit_again\":%u,\"submit_errors\":%u,\"deadlines\":%u,"
	       "\"rpc_errors\":%u,\"invalid_responses\":%u,\"ok_rps\":%.3f,"
	       "\"payload_MiB_s\":%.6f,\"ok_p50_us\":%.3f,\"ok_p99_us\":%.3f,"
	       "\"accepted_p99_us\":%.3f}", name, attempts, accepted, ok,
	       accepted, again, submit_errors, deadlines, rpc_errors, invalid, ok / elapsed,
	       (double)bytes / (1048576.0 * elapsed), percentile(ok_lat, ok, 50U),
	       percentile(ok_lat, ok, 99U), percentile(all_lat, accepted, 99U));
	free(ok_lat);
	free(all_lat);
}

static int phase(struct client_run *run, const struct options *o, const char *name,
		 const char *scenario, uint32_t count, uint32_t timeout_ms, int report)
{
	struct sample *samples = calloc(count, sizeof(*samples));
	struct rusage before, after;
	uint64_t begin, end, guard;
	uint32_t sent = 0, completed = 0, i;
	int failed = 0;
	if (!samples) fatal("sample allocation", errno);
	check(getrusage(RUSAGE_SELF, &before), "client getrusage");
	begin = now_ns();
	guard = begin + PHASE_GUARD_NS;
	check(pthread_mutex_lock(&run->lock), "phase lock");
	while (completed < count && !failed) {
		for (i = 0; i < run->window; ++i) {
			struct slot *slot = &run->slots[i];
			struct tr_rpc_call_options call_options = { .timeout_ms = timeout_ms };
			struct tr_rpc_call_handle handle;
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
			/* Callback may run before submit returns. Never hold its mutex here. */
			check(pthread_mutex_unlock(&run->lock), "submit unlock");
			ret = tr_client_unary_call_ex(run->client, SERVICE, s->method,
				&request, &call_options, result, slot, &handle);
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
	check(getrusage(RUSAGE_SELF, &after), "client getrusage");
	if (failed || run->bad_callback) {
		fprintf(stderr, "phase %s: incomplete, duplicate callback or corrupt response (%u/%u)\n",
			name, completed, count);
		/* Keep samples and payloads alive until all library callbacks are joined. */
		tr_client_destroy(run->client);
		run->client = NULL;
		free(samples);
		return -1;
	}
	if (report) {
		double elapsed = (double)(end - begin) / 1e9;
		printf("{\"type\":\"phase\",\"schema\":1,\"phase\":\"%s\",\"scenario\":\"%s\","
		       "\"pid\":%ld,\"window\":%u,\"start_ns\":%" PRIu64 ",\"end_ns\":%" PRIu64 ","
		       "\"elapsed_s\":%.9f,\"client_cpu_s\":%.9f,"
		       "\"client_peak_rss_kib\":%ld,", name, scenario, (long)getpid(), run->window, begin, end,
		       elapsed, cpu_seconds(&after) - cpu_seconds(&before), after.ru_maxrss);
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
				fprintf(stderr, "phase %s: warmup did not fully succeed\n", name);
				failed = 1;
			}
	}
	free(samples);
	return failed ? -1 : 0;
}

static int run_client(const struct options *o)
{
	struct tr_client_config config;
	struct client_run run = {0};
	pthread_condattr_t attr;
	uint32_t i;
	int status = EXIT_SUCCESS;
	tr_client_config_init(&config);
	configure_limits(&config.limits, o);
	config.keepalive_interval_ms = 0;
	config.enable_reconnect = 0;
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
	check(tr_client_create(&config, &run.client), "client create");
	check(tr_client_connect(run.client, o->host, (uint16_t)o->port), "client connect");
	check(tr_client_wait_ready(run.client, 5000U), "client ready");
	for (i = 1U; i <= 3U; ++i) {
		struct tr_rpc_method_desc m = method(i, o->bulk_bytes);
		check(tr_client_register_method(run.client, &m), "client method");
	}
	if (o->warmup && phase(&run, o, "warmup", "small", o->warmup, 5000U, 0))
		status = EXIT_FAILURE;
	if (status == EXIT_SUCCESS && o->start_gate) {
		printf("{\"type\":\"client_ready\",\"pid\":%ld}\n", (long)getpid());
		fflush(stdout);
		if (getchar() != 'g') {
			fputs("client start gate was not released\n", stderr);
			status = EXIT_FAILURE;
		}
	}
	if (status == EXIT_SUCCESS &&
	    (phase(&run, o, "measure", o->scenario, o->requests, o->timeout_ms, 1) ||
	     (!strcmp(o->scenario, "pressure") &&
	      phase(&run, o, "recovery", "small", 64U, 5000U, 1))))
		status = EXIT_FAILURE;
	/* No reconnect/recreate between pressure and recovery. Teardown joins callbacks. */
	tr_client_destroy(run.client);
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
