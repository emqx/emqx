$(shell $(CURDIR)/scripts/git-hooks-init.sh)
REBAR_VERSION = 3.16.1-emqx-1
REBAR = $(CURDIR)/rebar3
BUILD = $(CURDIR)/build
SCRIPTS = $(CURDIR)/scripts
export EMQX_DEFAULT_BUILDER = ghcr.io/emqx/emqx-builder/4.4-2:23.3.4.9-3-alpine3.14
export EMQX_DEFAULT_RUNNER = alpine:3.14
export OTP_VSN ?= $(shell $(CURDIR)/scripts/get-otp-vsn.sh)
export PKG_VSN ?= $(shell $(CURDIR)/pkg-vsn.sh)
export EMQX_DASHBOARD_VERSION ?= v5.0.0-beta.18
export DOCKERFILE := deploy/docker/Dockerfile
export DOCKERFILE_TESTING := deploy/docker/Dockerfile.testing
ifeq ($(OS),Windows_NT)
	export REBAR_COLOR=none
endif

PROFILE ?= emqx
REL_PROFILES := emqx emqx-edge emqx-ee
PKG_PROFILES := emqx-pkg emqx-edge-pkg emqx-ee-pkg
PROFILES := $(REL_PROFILES) $(PKG_PROFILES) default

CT_NODE_NAME ?= 'test@127.0.0.1'

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
	@ENABLE_COVER_COMPILE=1 $(REBAR) eunit -v -c

.PHONY: proper
proper: $(REBAR)
	@ENABLE_COVER_COMPILE=1 $(REBAR) proper -d test/props -c

.PHONY: ct
ct: $(REBAR) conf-segs
	@ENABLE_COVER_COMPILE=1 $(REBAR) ct --name $(CT_NODE_NAME) -c -v

APPS=$(shell $(CURDIR)/scripts/find-apps.sh)

## app/name-ct targets are intended for local tests hence cover is not enabled
.PHONY: $(APPS:%=%-ct)
define gen-app-ct-target
$1-ct:
	$(REBAR) ct --name $(CT_NODE_NAME) -v --suite $(shell $(CURDIR)/scripts/find-suites.sh $1)
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

.PHONY: $(REL_PROFILES)
$(REL_PROFILES:%=%): $(REBAR) get-dashboard conf-segs
	@$(REBAR) as $(@) do compile,release

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
		find _build/$(@:clean-%=%) -name '*.beam' -o -name '*.so' -o -name '*.app' -o -name '*.appup' -o -name '*.o' -o -name '*.d' -type f | xargs rm -f; \
		find _build/$(@:clean-%=%)  -type l -delete; \
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
$(PROFILES:%=deps-%): $(REBAR) get-dashboard
	@$(REBAR) as $(@:deps-%=%) get-deps
	@rm -f rebar.lock

.PHONY: xref
xref: $(REBAR)
	@$(REBAR) as check xref

.PHONY: dialyzer
dialyzer: $(REBAR)
	@$(REBAR) as check dialyzer

COMMON_DEPS := $(REBAR) get-dashboard conf-segs

## rel target is to create release package without relup
.PHONY: $(REL_PROFILES:%=%-rel) $(PKG_PROFILES:%=%-rel)
$(REL_PROFILES:%=%-rel) $(PKG_PROFILES:%=%-rel): $(COMMON_DEPS)
	@$(BUILD) $(subst -rel,,$(@)) rel

## relup target is to create relup instructions
.PHONY: $(REL_PROFILES:%=%-relup)
define gen-relup-target
$1-relup: $(COMMON_DEPS)
	@$(BUILD) $1 relup
endef
ALL_ZIPS = $(REL_PROFILES)
$(foreach zt,$(ALL_ZIPS),$(eval $(call gen-relup-target,$(zt))))

## zip target is to create a release package .zip with relup
.PHONY: $(REL_PROFILES:%=%-zip)
define gen-zip-target
$1-zip: $1-relup
	@$(BUILD) $1 zip
endef
ALL_ZIPS = $(REL_PROFILES)
$(foreach zt,$(ALL_ZIPS),$(eval $(call gen-zip-target,$(zt))))

## A pkg target depend on a regular release
.PHONY: $(PKG_PROFILES)
define gen-pkg-target
$1: $1-rel
	@$(BUILD) $1 pkg
endef
$(foreach pt,$(PKG_PROFILES),$(eval $(call gen-pkg-target,$(pt))))

.PHONY: run
run: $(PROFILE) quickrun

.PHONY: quickrun
quickrun:
	./_build/$(PROFILE)/rel/emqx/bin/emqx console

## docker target is to create docker instructions
.PHONY: $(REL_PROFILES:%=%-docker)
define gen-docker-target
$1-docker: $(COMMON_DEPS)
	@$(BUILD) $1 docker
endef
ALL_ZIPS = $(REL_PROFILES)
$(foreach zt,$(ALL_ZIPS),$(eval $(call gen-docker-target,$(zt))))

## emqx-docker-testing
## emqx-ee-docker-testing
## is to directly copy a unzipped zip-package to a
## base image such as ubuntu20.04. Mostly for testing
.PHONY: $(REL_PROFILES:%=%-docker-testing)
define gen-docker-target-testing
$1-docker-testing: $(COMMON_DEPS)
	@$(BUILD) $1 docker-testing
endef
ALL_ZIPS = $(REL_PROFILES)
$(foreach zt,$(ALL_ZIPS),$(eval $(call gen-docker-target-testing,$(zt))))

conf-segs:
	@scripts/merge-config.escript
