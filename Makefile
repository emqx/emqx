$(shell $(CURDIR)/scripts/git-hooks-init.sh)
REBAR_VERSION = 3.16.1-emqx-1
REBAR = $(CURDIR)/rebar3
BUILD = $(CURDIR)/build
SCRIPTS = $(CURDIR)/scripts
export EMQX_RELUP ?= true
export EMQX_DEFAULT_BUILDER = ghcr.io/emqx/emqx-builder/5.0-10:1.13.3-24.2.1-1-alpine3.15.1
export EMQX_DEFAULT_RUNNER = alpine:3.15.1
export OTP_VSN ?= $(shell $(CURDIR)/scripts/get-otp-vsn.sh)
export ELIXIR_VSN ?= $(shell $(CURDIR)/scripts/get-elixir-vsn.sh)
export EMQX_DASHBOARD_VERSION ?= v0.26.0
export DOCKERFILE := deploy/docker/Dockerfile
export EMQX_REL_FORM ?= tgz
ifeq ($(OS),Windows_NT)
	export REBAR_COLOR=none
	FIND=/usr/bin/find
else
	FIND=find
endif

PROFILE ?= emqx
REL_PROFILES := emqx emqx-edge emqx-enterprise
PKG_PROFILES := emqx-pkg emqx-edge-pkg emqx-enterprise-pkg
PROFILES := $(REL_PROFILES) $(PKG_PROFILES) default

CT_NODE_NAME ?= 'test@127.0.0.1'

export REBAR_GIT_CLONE_OPTIONS += --depth=1

.PHONY: default
default: $(REBAR) $(PROFILE)

.PHONY: all
all: $(REBAR) $(PROFILES)

.PHONY: ensure-rebar3
ensure-rebar3:
	@$(SCRIPTS)/ensure-rebar3.sh $(REBAR_VERSION)

.PHONY: ensure-hex
ensure-hex:
	@mix local.hex --if-missing --force

.PHONY: ensure-mix-rebar3
ensure-mix-rebar3: $(REBAR)
	@mix local.rebar rebar3 $(CURDIR)/rebar3 --if-missing --force

.PHONY: ensure-mix-rebar
ensure-mix-rebar: $(REBAR)
	@mix local.rebar --if-missing --force

.PHONY: mix-deps-get
mix-deps-get: $(ELIXIR_COMMON_DEPS)
	@mix deps.get

$(REBAR): ensure-rebar3

.PHONY: get-dashboard
get-dashboard:
	@$(SCRIPTS)/get-dashboard.sh

.PHONY: eunit
eunit: $(REBAR)
	@ENABLE_COVER_COMPILE=1 $(REBAR) eunit -v -c

.PHONY: proper
proper: $(REBAR)
	@ENABLE_COVER_COMPILE=1 $(REBAR) proper -d test/props -c

.PHONY: ct
ct: $(REBAR) conf-segs
	@ENABLE_COVER_COMPILE=1 $(REBAR) ct --name $(CT_NODE_NAME) -c -v

.PHONY: static_checks
static_checks:
	@$(REBAR) as check do xref, dialyzer, ct --suite apps/emqx/test/emqx_static_checks --readable false

APPS=$(shell $(CURDIR)/scripts/find-apps.sh)

## app/name-ct targets are intended for local tests hence cover is not enabled
.PHONY: $(APPS:%=%-ct)
define gen-app-ct-target
$1-ct: $(REBAR) conf-segs
	@ENABLE_COVER_COMPILE=1 $(REBAR) ct --name $(CT_NODE_NAME) -c -v --cover_export_name $(subst /,-,$1) --suite $(shell $(CURDIR)/scripts/find-suites.sh $1)
endef
$(foreach app,$(APPS),$(eval $(call gen-app-ct-target,$(app))))

## apps/name-prop targets
.PHONY: $(APPS:%=%-prop)
define gen-app-prop-target
$1-prop:
	$(REBAR) proper -d test/props -v -m $(shell $(CURDIR)/scripts/find-props.sh $1)
endef
$(foreach app,$(APPS),$(eval $(call gen-app-prop-target,$(app))))

.PHONY: ct-suite
ct-suite: $(REBAR)
ifneq ($(TESTCASE),)
	$(REBAR) ct -v --readable=false --name $(CT_NODE_NAME) --suite $(SUITE)  --case $(TESTCASE)
else ifneq ($(GROUP),)
	$(REBAR) ct -v --readable=false --name $(CT_NODE_NAME) --suite $(SUITE)  --group $(GROUP)
else
	$(REBAR) ct -v --readable=false --name $(CT_NODE_NAME) --suite $(SUITE)
endif

.PHONY: cover
cover: $(REBAR)
	@ENABLE_COVER_COMPILE=1 $(REBAR) cover

.PHONY: coveralls
coveralls: $(REBAR)
	@ENABLE_COVER_COMPILE=1 $(REBAR) as test coveralls send

COMMON_DEPS := $(REBAR) get-dashboard conf-segs
ELIXIR_COMMON_DEPS := ensure-hex ensure-mix-rebar3 ensure-mix-rebar

.PHONY: $(REL_PROFILES)
$(REL_PROFILES:%=%): $(COMMON_DEPS)
	@$(REBAR) as $(@) do release

## Not calling rebar3 clean because
## 1. rebar3 clean relies on rebar3, meaning it reads config, fetches dependencies etc.
## 2. it's slow
## NOTE: this does not force rebar3 to fetch new version dependencies
## make clean-all to delete all fetched dependencies for a fresh start-over
.PHONY: clean $(PROFILES:%=clean-%)
clean: $(PROFILES:%=clean-%)
$(PROFILES:%=clean-%):
	@if [ -d _build/$(@:clean-%=%) ]; then \
		rm rebar.lock \
		rm -rf _build/$(@:clean-%=%)/rel; \
		$(FIND) _build/$(@:clean-%=%) -name '*.beam' -o -name '*.so' -o -name '*.app' -o -name '*.appup' -o -name '*.o' -o -name '*.d' -type f | xargs rm -f; \
		$(FIND) _build/$(@:clean-%=%) -type l -delete; \
	fi

.PHONY: clean-all
clean-all:
	@rm -f rebar.lock
	@rm -rf _build

.PHONY: deps-all
deps-all: $(REBAR) $(PROFILES:%=deps-%)
	@make clean # ensure clean at the end

## deps-<profile> is used in CI scripts to download deps and the
## share downloads between CI steps and/or copied into containers
## which may not have the right credentials
.PHONY: $(PROFILES:%=deps-%)
$(PROFILES:%=deps-%): $(COMMON_DEPS)
	@$(REBAR) as $(@:deps-%=%) get-deps
	@rm -f rebar.lock

.PHONY: xref
xref: $(REBAR)
	@$(REBAR) as check xref

.PHONY: dialyzer
dialyzer: $(REBAR)
	@$(REBAR) as check dialyzer

## rel target is to create release package without relup
.PHONY: $(REL_PROFILES:%=%-rel) $(PKG_PROFILES:%=%-rel)
$(REL_PROFILES:%=%-rel) $(PKG_PROFILES:%=%-rel): $(COMMON_DEPS)
	@$(BUILD) $(subst -rel,,$(@)) rel

## download relup base packages
.PHONY: $(REL_PROFILES:%=%-relup-downloads)
define download-relup-packages
$1-relup-downloads:
	@if [ "$${EMQX_RELUP}" = "true" ]; then $(CURDIR)/scripts/relup-base-packages.sh $1; fi
endef
ALL_ZIPS = $(REL_PROFILES)
$(foreach zt,$(ALL_ZIPS),$(eval $(call download-relup-packages,$(zt))))

## relup target is to create relup instructions
.PHONY: $(REL_PROFILES:%=%-relup)
define gen-relup-target
$1-relup: $1-relup-downloads $(COMMON_DEPS)
	@$(BUILD) $1 relup
endef
ALL_TGZS = $(REL_PROFILES)
$(foreach zt,$(ALL_TGZS),$(eval $(call gen-relup-target,$(zt))))

## tgz target is to create a release package .tar.gz with relup
.PHONY: $(REL_PROFILES:%=%-tgz)
define gen-tgz-target
$1-tgz: $1-relup
	@$(BUILD) $1 tgz
endef
ALL_TGZS = $(REL_PROFILES)
$(foreach zt,$(ALL_TGZS),$(eval $(call gen-tgz-target,$(zt))))

## A pkg target depend on a regular release
.PHONY: $(PKG_PROFILES)
define gen-pkg-target
$1: $(COMMON_DEPS)
	@$(BUILD) $1 pkg
endef
$(foreach pt,$(PKG_PROFILES),$(eval $(call gen-pkg-target,$(pt))))

.PHONY: run
run: $(PROFILE) quickrun

.PHONY: quickrun
quickrun:
	./_build/$(PROFILE)/rel/emqx/bin/emqx console

## docker target is to create docker instructions
.PHONY: $(REL_PROFILES:%=%-docker) $(REL_PROFILES:%=%-elixir-docker)
define gen-docker-target
$1-docker: $(COMMON_DEPS)
	@$(BUILD) $1 docker
endef
ALL_DOCKERS = $(REL_PROFILES) $(REL_PROFILES:%=%-elixir)
$(foreach zt,$(ALL_DOCKERS),$(eval $(call gen-docker-target,$(zt))))

.PHONY:
conf-segs:
	@scripts/merge-config.escript

## elixir target is to create release packages using Elixir's Mix
.PHONY: $(REL_PROFILES:%=%-elixir) $(PKG_PROFILES:%=%-elixir)
$(REL_PROFILES:%=%-elixir) $(PKG_PROFILES:%=%-elixir): $(COMMON_DEPS) $(ELIXIR_COMMON_DEPS) mix-deps-get
	@$(BUILD) $(subst -elixir,,$(@)) elixir

.PHONY: $(REL_PROFILES:%=%-elixir-pkg)
define gen-elixir-pkg-target
# the Elixir places the tar in a different path than Rebar3
$1-elixir-pkg: $(COMMON_DEPS) $(ELIXIR_COMMON_DEPS) mix-deps-get
	@env TAR_PKG_DIR=_build/$1-pkg \
	     IS_ELIXIR=yes \
	     $(BUILD) $1-pkg pkg
endef
$(foreach pt,$(REL_PROFILES),$(eval $(call gen-elixir-pkg-target,$(pt))))

.PHONY: $(REL_PROFILES:%=%-elixir-tgz)
define gen-elixir-tgz-target
$1-elixir-tgz: $(COMMON_DEPS) $(ELIXIR_COMMON_DEPS) mix-deps-get
	@env IS_ELIXIR=yes $(BUILD) $1 tgz
endef
ALL_ELIXIR_TGZS = $(REL_PROFILES)
$(foreach tt,$(ALL_ELIXIR_TGZS),$(eval $(call gen-elixir-tgz-target,$(tt))))
