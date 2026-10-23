#ifndef TR_TLS_H
#define TR_TLS_H

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
