# Compatibility entry points only. xmake.lua owns the entire build graph.
XMAKE ?= xmake
MODE ?= release
XMAKE_CONFIG ?=

# Preserve each value as one shell argument, including quotes and literal '$'.
# xmake.lua parses compiler/linker flags once into an atomic argument group.
sh_quote = '$(subst ','"'"',$(1))'

# Do not let GNU Make's built-in CC=cc / AR=ar override an Xmake toolchain.
ifneq ($(filter-out default undefined,$(origin CC)),)
XMAKE_TOOLS += --cc=$(call sh_quote,$(CC)) --ld=$(call sh_quote,$(CC))
endif
ifneq ($(filter-out default undefined,$(origin AR)),)
XMAKE_TOOLS += --ar=$(call sh_quote,$(AR))
endif

.PHONY: all configure test test-timer clean
.NOTPARALLEL:

all: configure
	$(call sh_quote,$(XMAKE)) build --all

configure:
	$(call sh_quote,$(XMAKE)) f -y -m $(call sh_quote,$(MODE)) $(XMAKE_TOOLS) \
		--cflags= --ldflags= \
		--make_cflags=$(call sh_quote,$(CPPFLAGS) $(CFLAGS)) \
		--make_ldflags=$(call sh_quote,$(LDFLAGS)) $(XMAKE_CONFIG)

test: configure
	$(call sh_quote,$(XMAKE)) test -v -j1

test-timer: configure
	$(call sh_quote,$(XMAKE)) test -v -j1 'test_timer_queue/*'

clean:
	$(call sh_quote,$(XMAKE)) clean --all
