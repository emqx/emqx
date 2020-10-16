REBAR = $(CURDIR)/rebar3

PROFILE ?= emqx
PROFILES := emqx emqx-edge
PKG_PROFILES := emqx-pkg emqx-edge-pkg

export DEFAULT_VSN ?= $(shell ./get-lastest-tag.escript)
ifneq ($(shell echo $(DEFAULT_VSN) | grep -oE "^[ev0-9]+\.[0-9]+(\.[0-9]+)?"),)
	export PKG_VSN := $(patsubst v%,%,$(patsubst e%,%,$(DEFAULT_VSN)))
else
	export PKG_VSN := $(patsubst v%,%,$(DEFAULT_VSN))
endif

.PHONY: default
default: $(REBAR) $(PROFILE)

.PHONY: all
all: $(REBAR) $(PROFILES)

.PHONY: xref
xref:
	$(REBAR) xref

.PHONY: ct
ct:
	$(REBAR) ct

.PHONY: dialyzer
dialyzer:
	$(REBAR) dialyzer

.PHONY: distclean
distclean:
	@rm -rf _build

.PHONY: $(PROFILES)
$(PROFILES:%=%): $(REBAR)
ifneq ($(shell echo $(@) |grep edge),)
	export EMQX_DESC="EMQ X Edge"
else
	export EMQX_DESC="EMQ X Broker"
endif
	$(REBAR) as $(@) release

.PHONY: $(PROFILES:%=build-%)
$(PROFILES:%=build-%): $(REBAR)
	$(REBAR) as $(@:build-%=%) compile

.PHONY: clean $(PROFILES:%=clean-%)
clean: $(PROFILES:%=clean-%)
$(PROFILES:%=clean-%): $(REBAR)
	@rm -rf _build/$(@:clean-%=%)
	@rm -rf _build/$(@:clean-%=%)+test

.PHONY: deps-all
deps-all: $(REBAR) $(PROFILES:%=deps-%) $(PKG_PROFILES:%=deps-%)

.PHONY: $(PROFILES:%=deps-%) $(PKG_PROFILES:%=deps-%)
$(PROFILES:%=deps-%) $(PKG_PROFILES:%=deps-%): $(REBAR)
ifneq ($(shell echo $(@) |grep edge),)
	export EMQX_DESC="EMQ X Edge"
else
	export EMQX_DESC="EMQ X Broker"
endif
	$(REBAR) as $(@:deps-%=%) get-deps

include packages.mk
include docker.mk
