CC ?= cc
AR ?= ar
CFLAGS ?= -O2 -g
CFLAGS += -std=c11 -Wall -Wextra -Werror -pedantic -Iinclude -pthread
LDFLAGS += -pthread

SRC := \
	src/status.c \
	src/crc32c.c \
	src/buffer.c \
	src/wire.c \
	src/frame.c \
	src/parser.c \
	src/command_queue.c \
	src/completion_queue.c \
	src/timer_queue.c \
	src/socket.c \
	src/reactor.c \
	src/channel.c \
	src/rpc_codec.c \
	src/rpc_wire.c \
	src/rpc.c \
	src/facade.c \
	src/client.c \
	src/server.c

OBJ := $(SRC:.c=.o)
LIB := libtrcore.a
TEST := tests/test_transport
EXAMPLES := examples/echo_server examples/echo_client

.PHONY: all test clean

all: $(LIB) $(TEST) $(EXAMPLES)

$(LIB): $(OBJ)
	$(AR) rcs $@ $^

$(TEST): tests/test_transport.o $(LIB)
	$(CC) $(CFLAGS) -o $@ tests/test_transport.o $(LIB) $(LDFLAGS)

examples/echo_server: examples/echo_server.o $(LIB)
	$(CC) $(CFLAGS) -o $@ examples/echo_server.o $(LIB) $(LDFLAGS)

examples/echo_client: examples/echo_client.o $(LIB)
	$(CC) $(CFLAGS) -o $@ examples/echo_client.o $(LIB) $(LDFLAGS)

test: $(TEST)
	./$(TEST)

clean:
	rm -f $(OBJ) tests/*.o examples/*.o $(LIB) $(TEST) $(EXAMPLES)
