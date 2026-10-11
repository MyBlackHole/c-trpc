#define _POSIX_C_SOURCE 200809L
/* 仅测试示例入口：桩不进入 trcore，也不安装到 SDK。 */
#include <locale.h>
static int missing_locale;
static char *test_setlocale(int category, const char *locale);
#define setlocale test_setlocale
#define main echo_client_main
#include "../examples/echo_client.c"
#undef main
#undef setlocale

static char *test_setlocale(int category, const char *locale)
{
	if (missing_locale && locale && strcmp(locale, "C") != 0)
		return NULL;
	return setlocale(category, locale);
}

#include <assert.h>
#include <sys/wait.h>
#include <unistd.h>

struct tr_client {
	int unused;
};
static struct tr_client instance;
static int scenario;
static const char *expected_address = "127.0.0.1";
static uint16_t expected_port = 9000U;

void tr_client_config_init(struct tr_client_config *config)
{
	memset(config, 0, sizeof(*config));
}

int tr_client_create(const struct tr_client_config *config,
                     struct tr_client **out)
{
	(void)config;
	*out = &instance;
	return TR_OK;
}

int tr_client_connect(struct tr_client *client, const char *address,
                      uint16_t port)
{
	(void)client;
	assert(strcmp(address, expected_address) == 0);
	assert(port == expected_port);
	return scenario == 1 || scenario == 2 ? TR_ERR_STATE : TR_OK;
}

int tr_client_register_method(struct tr_client *client,
                              const struct tr_rpc_method_desc *method)
{
	(void)client;
	(void)method;
	return scenario == 3 ? TR_ERR_STATE : TR_OK;
}

int tr_client_unary_call(struct tr_client *client, uint32_t service,
                         uint32_t method, const struct tr_rpc_bytes *request,
                         tr_rpc_unary_result_cb callback, void *arg,
                         struct tr_rpc_call_handle *out)
{
	(void)client;
	(void)service;
	(void)method;
	if (scenario == 4)
		return TR_ERR_STATE;
	memset(out, 0, sizeof(*out));
	callback(*out, scenario == 5 ? 17 : TR_RPC_STATUS_OK, request, arg);
	return TR_OK;
}

int tr_client_destroy(struct tr_client *client)
{
	(void)client;
	return scenario == 2 || scenario == 3 || scenario == 4 || scenario == 6
	               ? TR_ERR_STATE
	               : TR_OK;
}

static void reset_environment(void)
{
	const char *names[] = {"CTRPC_LANG", "LC_ALL", "LC_MESSAGES", "LANG",
	                       "LANGUAGE"};
	size_t i;

	for (i = 0; i < sizeof(names) / sizeof(names[0]); ++i)
		assert(unsetenv(names[i]) == 0);
}

static void check_output(int argc, char **argv, int code, const char *output,
                         const char *error)
{
	FILE *out = tmpfile();
	FILE *err = tmpfile();
	char actual[1024];
	size_t size;
	int status;
	pid_t child;

	assert(out && err);
	assert(fflush(NULL) == 0);
	child = fork();
	assert(child >= 0);
	if (child == 0) {
		int result;

		assert(dup2(fileno(out), STDOUT_FILENO) >= 0);
		assert(dup2(fileno(err), STDERR_FILENO) >= 0);
		result = echo_client_main(argc, argv);
		assert(strcmp(setlocale(LC_NUMERIC, NULL), "C") == 0);
		assert(strcmp(setlocale(LC_CTYPE, NULL), "C") == 0);
		assert(fflush(NULL) == 0);
		_exit(result);
	}
	assert(waitpid(child, &status, 0) == child);
	assert(WIFEXITED(status) && WEXITSTATUS(status) == code);
	rewind(out);
	size = fread(actual, 1, sizeof(actual) - 1U, out);
	assert(!ferror(out));
	actual[size] = '\0';
	assert(size == strlen(output) && strcmp(actual, output) == 0);
	rewind(err);
	size = fread(actual, 1, sizeof(actual) - 1U, err);
	assert(!ferror(err));
	actual[size] = '\0';
	if (size != strlen(error) || strcmp(actual, error) != 0) {
		const char *names[] = {"LC_ALL", "LC_MESSAGES", "LANG",
		                       "LANGUAGE"};
		size_t i;

		for (i = 0; i < sizeof(names) / sizeof(names[0]); ++i)
			fprintf(stderr, "%s=%s\n", names[i],
			        getenv(names[i]) ? getenv(names[i])
			                         : "<unset>");
		fprintf(stderr,
		        "scenario=%d argument=%s expected=%s actual=%s\n",
		        scenario, argc > 1 ? argv[1] : "", error, actual);
	}
	assert(size == strlen(error) && strcmp(actual, error) == 0);
	assert(fclose(out) == 0);
	assert(fclose(err) == 0);
}

static void check_scenarios(const char *option, int chinese)
{
	char *args[] = {"echo_client", (char *)option, "127.0.0.1",
	                "9000",        "xmake-smoke",  NULL};
	const char *en[] = {"",
	                    "connect failed\n",
	                    "connect failed\nclient destroy failed\n",
	                    "client destroy failed\n",
	                    "client destroy failed\n",
	                    "RPC failed: 17\n",
	                    "client destroy failed\n"};
	const char *zh[] = {"",
	                    "连接失败\n",
	                    "连接失败\n客户端销毁失败\n",
	                    "客户端销毁失败\n",
	                    "客户端销毁失败\n",
	                    "RPC 失败：17\n",
	                    "客户端销毁失败\n"};

	for (scenario = 0; scenario < 7; ++scenario)
		check_output(5, args, scenario ? 1 : 0,
		             scenario == 0 || scenario == 6
		                     ? "response: xmake-smoke\n"
		                     : "",
		             chinese ? zh[scenario] : en[scenario]);
}

int main(void)
{
	char *invalid[] = {"echo_client", "--lang=invalid", NULL};
	char *literal[] = {"echo_client", "127.0.0.1", "9000", "--lang=zh_CN",
	                   NULL};
	char *escaped[] = {"echo_client", "--",      "--lang=literal",
	                   "9000",        "你好 %s", NULL};
	char *defaults[] = {"echo_client", NULL};
	char *empty[] = {"echo_client", "--lang=", NULL};
	const char *c_locales[] = {"C", "POSIX", "C.UTF-8", "C.utf8",
	                           "xx_YY.UTF-8"};
	size_t i;

	reset_environment();
	/* 中文路径必须真实执行；CI 在私有 LOCPATH 生成消息 locale。 */
	assert(setlocale(LC_MESSAGES, "zh_CN.UTF-8"));
	assert(setlocale(LC_MESSAGES, "C"));
	check_scenarios("--lang=auto", 0);
	check_scenarios("--lang=en", 0);
	check_scenarios("--lang=zh_CN", 1);
	for (i = 0; i < sizeof(c_locales) / sizeof(c_locales[0]); ++i) {
		reset_environment();
		assert(setenv("LC_ALL", c_locales[i], 1) == 0);
		assert(setenv("LANGUAGE", "zh_CN", 1) == 0);
		check_scenarios("--lang=auto", 0);
	}
	reset_environment();
	assert(setenv("LANG", "zh_CN.UTF-8", 1) == 0);
	check_scenarios("--lang=auto", 1);
	assert(setenv("LC_MESSAGES", "C", 1) == 0);
	check_scenarios("--lang=auto", 0);
	assert(setenv("LC_ALL", "zh_CN.UTF-8", 1) == 0);
	check_scenarios("--lang=auto", 1);
	assert(setenv("LANGUAGE", "en", 1) == 0);
	check_scenarios("--lang=auto", 0);
	assert(setenv("LANGUAGE", "en:zh_CN", 1) == 0);
	check_scenarios("--lang=auto", 1);
	assert(setenv("LANGUAGE", "fr:zh_CN:en", 1) == 0);
	check_scenarios("--lang=auto", 1);
	assert(setenv("LANGUAGE", "zh_TW:en", 1) == 0);
	check_scenarios("--lang=auto", 0);
	assert(setenv("LANGUAGE", "zh_Hant:en", 1) == 0);
	check_scenarios("--lang=auto", 0);
	assert(setenv("CTRPC_LANG", "zh_CN", 1) == 0);
	assert(setenv("LANGUAGE", "zh_CN", 1) == 0);
	check_scenarios("--lang=en", 0);
	assert(setenv("CTRPC_LANG", "en", 1) == 0);
	assert(setenv("LANGUAGE", "en", 1) == 0);
	check_scenarios("--lang=zh_CN", 1);
	check_output(2, invalid, 2, "", ECHO_USAGE);
	check_output(2, empty, 2, "", ECHO_USAGE);
	scenario = 1;
	check_output(1, defaults, 1, "", "connect failed\n");
	assert(setenv("CTRPC_LANG", "zh_CN", 1) == 0);
	check_output(1, defaults, 1, "", "连接失败\n");
	assert(setenv("CTRPC_LANG", "unknown", 1) == 0);
	check_output(1, defaults, 1, "", "connect failed\n");
	/* 注入所有非 C locale 初始化失败，验证回退而非假跳过。 */
	missing_locale = 1;
	check_scenarios("--lang=zh_CN", 0);
	missing_locale = 0;
	scenario = 0;
	check_output(4, literal, 0, "response: --lang=zh_CN\n", "");
	expected_address = "--lang=literal";
	check_output(5, escaped, 0, "response: 你好 %s\n", "");
	expected_address = "127.0.0.1";
	check_output(1, defaults, 0, "response: hello\n", "");
	assert(strcmp(setlocale(LC_NUMERIC, NULL), "C") == 0);
	assert(strcmp(setlocale(LC_CTYPE, NULL), "C") == 0);
	puts("echo_client gettext: bilingual scenarios, precedence and "
	     "fallback passed");
	return 0;
}
