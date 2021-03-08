$(shell scripts/git-hooks-init.sh)
REBAR_VERSION = 3.14.3-emqx-4
REBAR = $(CURDIR)/rebar3
BUILD = $(CURDIR)/build
SCRIPTS = $(CURDIR)/scripts
export PKG_VSN ?= $(shell $(CURDIR)/pkg-vsn.sh)
export EMQX_DESC ?= EMQ X
export EMQX_CE_DASHBOARD_VERSION ?= v4.3.0-beta.1

PROFILE ?= emqx
REL_PROFILES := emqx emqx-edge
PKG_PROFILES := emqx-pkg emqx-edge-pkg
PROFILES := $(REL_PROFILES) $(PKG_PROFILES) default

export REBAR_GIT_CLONE_OPTIONS += --depth=1

.PHONY: default
default: $(REBAR) $(PROFILE)

.PHONY: all
all: $(REBAR) $(PROFILES)

.PHONY: ensure-rebar3
ensure-rebar3:
	@$(SCRIPTS)/fail-on-old-otp-version.escript
	@$(SCRIPTS)/ensure-rebar3.sh $(REBAR_VERSION)

$(REBAR): ensure-rebar3

.PHONY: get-dashboard
get-dashboard:
	@$(SCRIPTS)/get-dashboard.sh

.PHONY: eunit
eunit: $(REBAR)
	@$(REBAR) eunit -v -c

.PHONY: proper
proper: $(REBAR)
	@$(REBAR) as test proper -d test/props -c

.PHONY: ct
ct: $(REBAR)
	@$(REBAR) ct --name 'test@127.0.0.1' -c -v

.PHONY: cover
cover: $(REBAR)
	@$(REBAR) cover

.PHONY: coveralls
coveralls: $(REBAR)
	@$(REBAR) as test coveralls send

.PHONY: $(REL_PROFILES)
$(REL_PROFILES:%=%): $(REBAR) get-dashboard
ifneq ($(shell echo $(@) |grep edge),)
	@export EMQX_DESC="$${EMQX_DESC} Edge"
else
	@export EMQX_DESC="$${EMQX_DESC} Broker"
endif
	@$(REBAR) as $(@) release

## Not calling rebar3 clean because
## 1. rebar3 clean relies on rebar3, meaning it reads config, fetches dependencies etc.
## 2. it's slow
## NOTE: this does not force rebar3 to fetch new version dependencies
## make clean-all to delete all fetched dependencies for a fresh start-over
.PHONY: clean $(PROFILES:%=clean-%)
clean: $(PROFILES:%=clean-%)
$(PROFILES:%=clean-%):
	@if [ -d _build/$(@:clean-%=%) ]; then \
		rm -rf _build/$(@:clean-%=%)/rel; \
		find _build/$(@:clean-%=%) -name '*.beam' -o -name '*.so' -o -name '*.app' -o -name '*.appup' -o -name '*.o' -o -name '*.d' -type f | xargs rm -f; \
	fi

.PHONY: clean-all
clean-all:
	@rm -rf _build

.PHONY: deps-all
deps-all: $(REBAR) $(PROFILES:%=deps-%)

.PHONY: $(PROFILES:%=deps-%)
$(PROFILES:%=deps-%): $(REBAR) get-dashboard
ifneq ($(shell echo $(@) |grep edge),)
	@export EMQX_DESC="$${EMQX_DESC} Edge"
else
	@export EMQX_DESC="$${EMQX_DESC} Broker"
endif
	@$(REBAR) as $(@:deps-%=%) get-deps

.PHONY: xref
xref: $(REBAR)
	@$(REBAR) as check xref

.PHONY: dialyzer
dialyzer: $(REBAR)
	@$(REBAR) as check dialyzer

.PHONY: $(REL_PROFILES:%=relup-%)
$(REL_PROFILES:%=relup-%): $(REBAR)
ifneq ($(OS),Windows_NT)
	@$(BUILD) $(@:relup-%=%) relup
endif

.PHONY: $(REL_PROFILES:%=%-tar) $(PKG_PROFILES:%=%-tar)
$(REL_PROFILES:%=%-tar) $(PKG_PROFILES:%=%-tar): $(REBAR) get-dashboard
	@$(BUILD) $(subst -tar,,$(@)) tar

## zip targets depend on the corresponding relup and tar artifacts
.PHONY: $(REL_PROFILES:%=%-zip)
define gen-zip-target
$1-zip: relup-$1 $1-tar
	@$(BUILD) $1 zip
endef
ALL_ZIPS = $(REL_PROFILES) $(PKG_PROFILES)
$(foreach zt,$(ALL_ZIPS),$(eval $(call gen-zip-target,$(zt))))

## A pkg target depend on a regular release profile zip to include relup,
## and also a -pkg suffixed profile tar (without relup) for making deb/rpm package
.PHONY: $(PKG_PROFILES)
define gen-pkg-target
$1: $(subst -pkg,,$1)-zip $1-tar
	@$(BUILD) $1 pkg
endef
$(foreach pt,$(PKG_PROFILES),$(eval $(call gen-pkg-target,$(pt))))

include docker.mk
