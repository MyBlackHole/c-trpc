#ifndef TR_PEER_IDENTITY_H
#define TR_PEER_IDENTITY_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum tr_peer_identity_authentication {
	TR_PEER_IDENTITY_NONE = 0,
	TR_PEER_IDENTITY_TLS_CERTIFICATE = 1
};

/* 借用视图；uri_san 只在提供此视图的回调期间有效。 */
struct tr_peer_identity_view {
	enum tr_peer_identity_authentication authentication;
	const uint8_t *uri_san;
	uint32_t uri_san_len;
};

#ifdef __cplusplus
}
#endif

#endif
