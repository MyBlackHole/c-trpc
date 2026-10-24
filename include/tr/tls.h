#ifndef TR_TLS_H
#define TR_TLS_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// 备注: 普通 V2 的 required 表示 TLS 1.3
// 握手必须成功；任何失败都不能改用明文传输。
enum tr_tls_mode { TR_TLS_DISABLED = 0, TR_TLS_REQUIRED = 1 };

// 备注: 服务端请求或强制客户端证书的策略；optional 接受无证书连接，
// 备注: 但提供的证书仍须通过链、用途和唯一 URI SAN 校验。
enum tr_tls_client_auth_policy {
	TR_TLS_CLIENT_AUTH_NONE = 0,
	TR_TLS_CLIENT_AUTH_OPTIONAL = 1,
	TR_TLS_CLIENT_AUTH_REQUIRED = 2
};

/* 配置轮换后，旧会话可自然排空，或立即撤销旧 profile 上的连接。 */
enum tr_tls_rotation_policy {
	TR_TLS_ROTATE_GRACEFUL = 0,
	TR_TLS_ROTATE_REVOKE_OLD = 1
};

enum tr_tls_provider_operation_kind {
	TR_TLS_PROVIDER_OPERATION_CREDENTIALS = 1,
	TR_TLS_PROVIDER_OPERATION_AUTHORIZATION = 2
};

/* 异步 provider 的值句柄。复制句柄不会延长 facade 生命周期或获得对象所有权。 */
struct tr_tls_provider_operation {
	uint64_t instance;
	uint64_t generation;
	uint32_t slot;
	enum tr_tls_provider_operation_kind kind;
};

/* 仅在调用处借用的数据视图；长度以字节计，不依赖 NUL 终止。 */
struct tr_tls_bytes {
	const uint8_t *data;
	size_t len;
};

/* complete 调用期间借用；成功时库在返回前复制全部数据。 */
struct tr_tls_credential_bundle {
	struct tr_tls_bytes certificate_chain_pem;
	struct tr_tls_bytes private_key_pem;
	struct tr_tls_bytes private_key_password;
};

struct tr_tls_credential_request {
	/* CLOCK_MONOTONIC 纳秒绝对期限。 */
	uint64_t deadline_monotonic_ns;
	enum tr_tls_rotation_policy rotation_policy;
};

struct tr_tls_credential_result {
	/* 凭证获取、解析或 profile 发布的终态；成功为 TR_OK。 */
	int status;
	/* 仅成功发布时非零。 */
	uint64_t profile_generation;
};

/* fetch/cancel 必须快速返回，不能等待外部 I/O。一次 fetch 调用即表示已接受；
 * request 只在 fetch 回调期间有效。收到 cancel 后，provider 必须停止再访问
 * 原始数据，并通过 complete 确认该句柄；取消确认前 facade 不会回收其预留槽。
 */
typedef void (*tr_tls_credentials_fetch_cb)(
        const struct tr_tls_credential_request *request,
        struct tr_tls_provider_operation operation, void *arg);
typedef void (*tr_tls_credentials_cancel_cb)(
        struct tr_tls_provider_operation operation, void *arg);

struct tr_tls_credentials_provider {
	tr_tls_credentials_fetch_cb fetch;
	tr_tls_credentials_cancel_cb cancel;
	void *arg;
};

// 备注: Client 的证书信任与服务端身份配置。字符串仅借用到创建函数返回，
// 备注: 调用方应在 TLS 必需模式下显式提供 expected_peer_name。
struct tr_tls_client_config {
	enum tr_tls_mode mode;
	// 备注: ca_file/ca_directory
	// 均为空时使用系统信任库；指定后只信任显式来源。
	const char *ca_file;
	const char *ca_directory;
	// 备注: DNS 只匹配 dNSName SAN，IP 只匹配 iPAddress SAN，不回退到
	// Common Name。
	const char *expected_peer_name;
	// 备注: 可选 DNS SNI 名称，独立于证书验证目标；传 IP 字面量无效。
	const char *sni_name;
	// 备注: mTLS 客户端证书与私钥必须同时提供，并在 TLS 1.3 握手中
	// 备注: 发送；证书文件和私钥匹配关系在 Client 创建时校验。
	const char *client_certificate_file;
	const char *client_private_key_file;
};

// 备注: Server 的服务端身份和客户端证书策略。证书与私钥必须成对配置。
struct tr_tls_server_config {
	enum tr_tls_mode mode;
	const char *certificate_file;
	const char *private_key_file;
	// 备注: client_auth 为 none 时必须为空；optional/required 时必须指定
	// 备注: 验证客户端证书链的 CA 文件。
	const char *client_ca_file;
	enum tr_tls_client_auth_policy client_auth;
};

#ifdef __cplusplus
}
#endif

#endif
