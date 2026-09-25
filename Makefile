# Compatibility entry points only. xmake.lua owns the entire build graph.
XMAKE ?= xmake
MODE ?= release
XMAKE_CONFIG ?=

# Do not let GNU Make's built-in CC=cc / AR=ar override an Xmake toolchain.
ifneq ($(filter-out default undefined,$(origin CC)),)
XMAKE_TOOLS += --cc="$(CC)" --ld="$(CC)"
endif
ifneq ($(filter-out default undefined,$(origin AR)),)
XMAKE_TOOLS += --ar="$(AR)"
endif

.PHONY: all configure test test-timer clean
.NOTPARALLEL:

all: configure
	$(XMAKE) build --all

configure:
	$(XMAKE) f -y -m "$(MODE)" $(XMAKE_TOOLS) \
		--cflags="$(CPPFLAGS) $(CFLAGS)" --ldflags="$(LDFLAGS)" $(XMAKE_CONFIG)

test: configure
	$(XMAKE) test -v -j1

test-timer: configure
	$(XMAKE) test -v -j1 'test_timer_queue/*'

clean:
	$(XMAKE) clean --all
