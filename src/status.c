#include "tr/status.h"

const char *tr_status_str(int status)
{
	switch (status) {
	case TR_OK:
		return "ok";
	case TR_AGAIN:
		return "again";
	case TR_FRAME_READY:
		return "frame-ready";
	case TR_IN_PROGRESS:
		return "in-progress";
	case TR_ERR_INVALID:
		return "invalid";
	case TR_ERR_NOMEM:
		return "no-memory";
	case TR_ERR_BAD_MAGIC:
		return "bad-magic";
	case TR_ERR_BAD_VERSION:
		return "bad-version";
	case TR_ERR_BAD_TYPE:
		return "bad-type";
	case TR_ERR_BAD_FLAGS:
		return "bad-flags";
	case TR_ERR_BAD_LENGTH:
		return "bad-length";
	case TR_ERR_HEADER_CRC:
		return "header-crc";
	case TR_ERR_PAYLOAD_CRC:
		return "payload-crc";
	case TR_ERR_RESERVED:
		return "reserved";
	case TR_ERR_STATE:
		return "bad-state";
	case TR_ERR_SYS:
		return "system-error";
	case TR_ERR_CLOSED:
		return "closed";
	case TR_ERR_STALE:
		return "stale";
	case TR_ERR_UNSUPPORTED:
		return "unsupported";
	case TR_ERR_TIMEOUT:
		return "timeout";
	default:
		return "unknown";
	}
}
