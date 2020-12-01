REBAR_VERSION = 3.13.2-emqx-2
REBAR = ./rebar3

PROFILE ?= emqx
PROFILES := emqx emqx-edge
PKG_PROFILES := emqx-pkg emqx-edge-pkg

.PHONY: default
default: $(REBAR) $(PROFILE)

.PHONY: all
all: $(REBAR) $(PROFILES)

.PHONY: ensure-rebar3
ensure-rebar3:
	@./ensure-rebar3.sh $(REBAR_VERSION)

$(REBAR): ensure-rebar3

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

# rebar clean
.PHONY: clean $(PROFILES:%=clean-%)
clean: $(PROFILES:%=clean-%) clean-stamps
$(PROFILES:%=clean-%): $(REBAR)
	$(REBAR) as $(@:clean-%=%) clean

.PHONY: clean-stamps
clean-stamps:
	find -L _build -name '.stamp' -type f | xargs rm -f

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
