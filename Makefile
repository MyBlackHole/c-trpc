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
	src/maintenance.c \
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
TIMER_TEST := tests/test_timer_queue
EXAMPLES := examples/echo_server examples/echo_client

.PHONY: all test test-timer clean

all: $(LIB) $(TEST) $(TIMER_TEST) $(EXAMPLES)

$(LIB): $(OBJ)
	$(AR) rcs $@ $^

$(TEST): tests/test_transport.o $(LIB)
	$(CC) $(CFLAGS) -o $@ tests/test_transport.o $(LIB) $(LDFLAGS)

$(TIMER_TEST): tests/test_timer_queue.o src/timer_queue.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

src/reactor.o src/timer_queue.o tests/test_transport.o \
	tests/test_timer_queue.o: src/timer_queue.h

examples/echo_server: examples/echo_server.o $(LIB)
	$(CC) $(CFLAGS) -o $@ examples/echo_server.o $(LIB) $(LDFLAGS)

examples/echo_client: examples/echo_client.o $(LIB)
	$(CC) $(CFLAGS) -o $@ examples/echo_client.o $(LIB) $(LDFLAGS)

test: $(TEST) $(TIMER_TEST)
	./$(TEST)
	./$(TIMER_TEST)

test-timer: $(TIMER_TEST)
	./$(TIMER_TEST)

clean:
	rm -f $(OBJ) tests/*.o examples/*.o $(LIB) $(TEST) $(TIMER_TEST) $(EXAMPLES)
