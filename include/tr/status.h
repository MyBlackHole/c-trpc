#ifndef TR_STATUS_H
#define TR_STATUS_H

#ifdef __cplusplus
extern "C" {
#endif

enum tr_status {
	TR_OK = 0,
	TR_AGAIN = 1,
	TR_FRAME_READY = 2,
	TR_IN_PROGRESS = 3,

	TR_ERR_INVALID = -1,
	TR_ERR_NOMEM = -2,
	TR_ERR_BAD_MAGIC = -3,
	TR_ERR_BAD_VERSION = -4,
	TR_ERR_BAD_TYPE = -5,
	TR_ERR_BAD_FLAGS = -6,
	TR_ERR_BAD_LENGTH = -7,
	TR_ERR_HEADER_CRC = -8,
	TR_ERR_PAYLOAD_CRC = -9,
	TR_ERR_RESERVED = -10,
	TR_ERR_STATE = -11,
	TR_ERR_SYS = -12,
	TR_ERR_CLOSED = -13,
	TR_ERR_STALE = -14,
	TR_ERR_UNSUPPORTED = -15,
	TR_ERR_TIMEOUT = -16,
	TR_ERR_CANCELLED = -17
};

const char *tr_status_str(int status);

#ifdef __cplusplus
}
#endif

#endif
