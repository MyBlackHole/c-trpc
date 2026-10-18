#define _POSIX_C_SOURCE 200809L
#include "tr/rpc_v2.h"
#include "tr/status.h"

#include <errno.h>
#include <libintl.h>
#include <limits.h>
#include <locale.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define ECHO_DOMAIN "c-trpc-examples"
#define ECHO_USAGE                                                             \
	"Usage: echo_client [--lang=auto|en|zh_CN] [--] [address [port "       \
	"[payload]]]\n"

/* Linux 示例将翻译资源与可执行文件一起部署，不依赖启动目录。 */
static void bind_messages(void)
{
	char directory[PATH_MAX];
	ssize_t length =
	        readlink("/proc/self/exe", directory, sizeof(directory) - 1U);
	char *slash;
	static const char suffix[] = "/locales";

	if (length < 0 || (size_t)length >= sizeof(directory) - 1U)
		return;
	directory[length] = '\0';
	slash = strrchr(directory, '/');
	if (!slash ||
	    (size_t)(slash - directory) + sizeof(suffix) > sizeof(directory))
		return;
	memcpy(slash, suffix, sizeof(suffix));
	(void)bindtextdomain(ECHO_DOMAIN, directory);
	(void)bind_textdomain_codeset(ECHO_DOMAIN, "UTF-8");
}

/* 仅独立 CLI 在建线程前初始化消息 locale；库与其他 locale 类别不变。 */
static void initialize_messages(const char *language)
{
	const char *selected = NULL;

	bind_messages();
	if (language && strcmp(language, "en") == 0) {
		(void)setlocale(LC_MESSAGES, "C");
		return;
	}
	if (language && strcmp(language, "zh_CN") == 0) {
		/* 显式选项优先于 LANGUAGE；仅修改本进程的环境副本。 */
		if (setenv("LANGUAGE", "zh_CN", 1) == 0) {
			selected = setlocale(LC_MESSAGES, "zh_CN.UTF-8");
			if (!selected)
				selected = setlocale(LC_MESSAGES, "zh_CN.utf8");
			if (!selected)
				selected = setlocale(LC_MESSAGES, "");
		}
	} else if (!language || !*language || strcmp(language, "auto") == 0) {
		selected = setlocale(LC_MESSAGES, "");
	}
	/* 老版本 glibc 在 C.UTF-8 下仍应用 LANGUAGE；统一 C 家族为原文。 */
	if (!selected || strncmp(selected, "C.", 2) == 0)
		(void)setlocale(LC_MESSAGES, "C");
}

struct result_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int done;
	int status;
	int copy_status;
	int messages;
	char response[1024];
	uint32_t response_len;
};

/* 正文只借用至回调返回，复制到有界应用存储；不能把正文当作终局。 */
static void on_message(struct tr_rpc_v2_call call,
                       const struct tr_rpc_bytes *response, void *arg)
{
	struct result_ctx *ctx = arg;
	(void)call;

	pthread_mutex_lock(&ctx->lock);
	if (ctx->messages++ || response->len >= sizeof(ctx->response)) {
		ctx->copy_status = TR_RPC_STATUS_RESOURCE_EXHAUSTED;
	} else {
		ctx->response_len = response->len;
		if (response->len)
			memcpy(ctx->response, response->data, response->len);
		ctx->response[response->len] = '\0';
	}
	pthread_mutex_unlock(&ctx->lock);
}

static void on_terminal(struct tr_rpc_v2_call call, enum tr_rpc_status status,
                        enum tr_rpc_v2_delivery delivery, void *arg)
{
	struct result_ctx *ctx = arg;
	(void)call;
	(void)delivery;

	pthread_mutex_lock(&ctx->lock);
	ctx->status = status == TR_RPC_STATUS_OK && ctx->copy_status
	                      ? ctx->copy_status
	                      : (int)status;
	ctx->done = 1;
	pthread_cond_signal(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

/* 只由外部控制线程收敛；AGAIN 不是成功，也不能先销毁回调存储。 */
static int destroy_client(struct tr_client *client)
{
	struct timespec deadline, now;
	const struct timespec pause_time = {0, 1000000L};
	int ret = tr_client_destroy(client);

	if (ret != TR_AGAIN)
		return ret;
	if (clock_gettime(CLOCK_MONOTONIC, &deadline) != 0)
		return TR_ERR_SYS;
	deadline.tv_sec += 5;
	for (;;) {
		if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
			return TR_ERR_SYS;
		if (now.tv_sec > deadline.tv_sec ||
		    (now.tv_sec == deadline.tv_sec &&
		     now.tv_nsec >= deadline.tv_nsec))
			return TR_ERR_TIMEOUT;
		if (nanosleep(&pause_time, NULL) != 0 && errno != EINTR)
			return TR_ERR_SYS;
		ret = tr_client_destroy(client);
		if (ret != TR_AGAIN)
			return ret;
	}
}

int main(int argc, char **argv)
{
	struct tr_client_config config;
	struct tr_client *client = NULL;
	struct tr_rpc_method_desc method;
	struct tr_rpc_bytes request;
	struct tr_rpc_v2_call call;
	struct tr_rpc_v2_client_callbacks callbacks;
	static struct result_ctx result;
	struct timespec deadline;
	const char *language = getenv("CTRPC_LANG");
	const char *address;
	const char *text;
	uint16_t port;
	int first = 1;
	int wait_ret = 0;

	/* 只在地址之前识别选项，不解释端口或负载中的选项样式文本。 */
	if (argc > first && strncmp(argv[first], "--lang=", 7) == 0) {
		language = argv[first++] + 7;
		if (strcmp(language, "auto") != 0 &&
		    strcmp(language, "en") != 0 &&
		    strcmp(language, "zh_CN") != 0) {
			initialize_messages(getenv("CTRPC_LANG"));
			fputs(dgettext(ECHO_DOMAIN, ECHO_USAGE), stderr);
			return 2;
		}
	}
	initialize_messages(language);
	if (argc > first && strcmp(argv[first], "--") == 0)
		++first;
	address = argc > first ? argv[first] : "127.0.0.1";
	port = argc > first + 1 ? (uint16_t)strtoul(argv[first + 1], NULL, 10)
	                        : 9000U;
	text = argc > first + 2 ? argv[first + 2] : "hello";

	memset(&result, 0, sizeof(result));
	pthread_mutex_init(&result.lock, NULL);
	pthread_cond_init(&result.cond, NULL);

	memset(&method, 0, sizeof(method));
	method.service_id = 1U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_ONE;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_RPC_LANE_CONTROL;
	method.max_request_bytes = 64U * 1024U;
	method.max_response_bytes = 64U * 1024U;

	tr_client_config_init(&config);
	if (tr_client_create(&config, &method, 1, &client) != TR_OK ||
	    tr_client_connect(client, address, port) != TR_OK) {
		fputs(dgettext(ECHO_DOMAIN, "connect failed\n"), stderr);
		if (destroy_client(client) != TR_OK)
			fputs(dgettext(ECHO_DOMAIN, "client destroy failed\n"),
			      stderr);
		return 1;
	}
	if (tr_client_wait_ready(client, 5000) != TR_OK) {
		if (destroy_client(client) != TR_OK)
			fputs(dgettext(ECHO_DOMAIN, "client destroy failed\n"),
			      stderr);
		return 1;
	}
	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.message = on_message;
	callbacks.terminal = on_terminal;
	callbacks.arg = &result;

	request.data = (const uint8_t *)text;
	request.len = (uint32_t)strlen(text);
	if (tr_client_unary_call(client, 1U, 1U, &request, NULL, &callbacks,
	                         &call) != TR_OK) {
		if (destroy_client(client) != TR_OK)
			fputs(dgettext(ECHO_DOMAIN, "client destroy failed\n"),
			      stderr);
		return 1;
	}

	clock_gettime(CLOCK_REALTIME, &deadline);
	deadline.tv_sec += 5;
	pthread_mutex_lock(&result.lock);
	while (!result.done && wait_ret == 0)
		wait_ret = pthread_cond_timedwait(&result.cond, &result.lock,
		                                  &deadline);
	if (result.done && result.status == TR_RPC_STATUS_OK)
		printf("response: %s\n", result.response);
	else
		fprintf(stderr, dgettext(ECHO_DOMAIN, "RPC failed: %d\n"),
		        result.done ? result.status : wait_ret);
	pthread_mutex_unlock(&result.lock);

	if (destroy_client(client) != TR_OK) {
		fputs(dgettext(ECHO_DOMAIN, "client destroy failed\n"), stderr);
		/* 销毁失败尚未证明回调退出。进程退出前保留静态回调存储及同步对象。
		 */
		return 1;
	}
	pthread_cond_destroy(&result.cond);
	pthread_mutex_destroy(&result.lock);
	return result.done && result.status == TR_RPC_STATUS_OK ? 0 : 1;
}
