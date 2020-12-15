REBAR_VERSION = 3.14.3-emqx-3
REBAR = $(CURDIR)/rebar3
export PKG_VSN ?= $(shell git describe --tags --match '[0-9]*' 2>/dev/null || git describe --always)
# comma separated versions
export RELUP_BASE_VERSIONS ?=

PROFILE ?= emqx
PROFILES := emqx emqx-edge check test
PKG_PROFILES := emqx-pkg emqx-edge-pkg

export REBAR_GIT_CLONE_OPTIONS += --depth=1

.PHONY: default
default: $(REBAR) $(PROFILE)

.PHONY: all
all: $(REBAR) $(PROFILES)

.PHONY: ensure-rebar3
ensure-rebar3:
	$(CURDIR)/ensure-rebar3.sh $(REBAR_VERSION)

$(REBAR): ensure-rebar3

.PHONY: eunit
eunit: $(REBAR)
	$(REBAR) eunit

.PHONY: ct
ct: $(REBAR)
	$(REBAR) ct

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

# rebar clean
.PHONY: clean $(PROFILES:%=clean-%)
clean: $(PROFILES:%=clean-%)
$(PROFILES:%=clean-%): $(REBAR)
	$(REBAR) as $(@:clean-%=%) clean

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

.PHONY: xref
xref: $(REBAR)
	$(REBAR) as check xref

.PHONY: dialyzer
dialyzer: $(REBAR)
	$(REBAR) as check dialyzer

include packages.mk
include docker.mk
