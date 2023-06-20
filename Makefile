REBAR = $(CURDIR)/rebar3
BUILD = $(CURDIR)/build
SCRIPTS = $(CURDIR)/scripts
export EMQX_RELUP ?= true
export EMQX_DEFAULT_BUILDER = ghcr.io/emqx/emqx-builder/5.1-0:1.14.5-25.3.2-1-debian11
export EMQX_DEFAULT_RUNNER = debian:11-slim
export EMQX_REL_FORM ?= tgz
export QUICER_DOWNLOAD_FROM_RELEASE = 1
ifeq ($(OS),Windows_NT)
	export REBAR_COLOR=none
	FIND=/usr/bin/find
else
	FIND=find
endif

# Dashboard version
# from https://github.com/emqx/emqx-dashboard5
export EMQX_DASHBOARD_VERSION ?= v1.3.0-1
export EMQX_EE_DASHBOARD_VERSION ?= e1.1.0

# `:=` should be used here, otherwise the `$(shell ...)` will be executed every time when the variable is used
# In make 4.4+, for backward-compatibility the value from the original environment is used.
# so the shell script will be executed tons of times.
# https://github.com/emqx/emqx/pull/10627
ifeq ($(strip $(OTP_VSN)),)
	export OTP_VSN := $(shell $(SCRIPTS)/get-otp-vsn.sh)
endif
ifeq ($(strip $(ELIXIR_VSN)),)
	export ELIXIR_VSN := $(shell $(SCRIPTS)/get-elixir-vsn.sh)
endif

PROFILE ?= emqx
REL_PROFILES := emqx emqx-enterprise
PKG_PROFILES := emqx-pkg emqx-enterprise-pkg
PROFILES := $(REL_PROFILES) $(PKG_PROFILES) default

CT_NODE_NAME ?= 'test@127.0.0.1'
CT_READABLE ?= true
CT_COVER_EXPORT_PREFIX ?= $(PROFILE)

export REBAR_GIT_CLONE_OPTIONS += --depth=1

.PHONY: default
default: $(REBAR) $(PROFILE)

.prepare:
	@$(SCRIPTS)/git-hooks-init.sh # this is no longer needed since 5.0 but we keep it anyway
	@$(SCRIPTS)/prepare-build-deps.sh
	@touch .prepare

.PHONY: all
all: $(REBAR) $(PROFILES)

.PHONY: ensure-rebar3
ensure-rebar3:
	@$(SCRIPTS)/ensure-rebar3.sh

$(REBAR): .prepare ensure-rebar3

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

.PHONY: eunit
eunit: $(REBAR) merge-config
	@ENABLE_COVER_COMPILE=1 $(REBAR) eunit -v -c --cover_export_name $(CT_COVER_EXPORT_PREFIX)-eunit

.PHONY: proper
proper: $(REBAR)
	@ENABLE_COVER_COMPILE=1 $(REBAR) proper -d test/props -c

.PHONY: test-compile
test-compile: $(REBAR) merge-config
	$(REBAR) as test compile

.PHONY: $(REL_PROFILES:%=%-compile)
$(REL_PROFILES:%=%-compile): $(REBAR) merge-config
	$(REBAR) as $(@:%-compile=%) compile

.PHONY: ct
ct: $(REBAR) merge-config
	@ENABLE_COVER_COMPILE=1 $(REBAR) ct --name $(CT_NODE_NAME) -c -v --cover_export_name $(CT_COVER_EXPORT_PREFIX)-ct

## only check bpapi for enterprise profile because it's a super-set.
.PHONY: static_checks
static_checks:
	@$(REBAR) as check do xref, dialyzer
	@if [ "$${PROFILE}" = 'emqx-enterprise' ]; then $(REBAR) ct --suite apps/emqx/test/emqx_static_checks --readable $(CT_READABLE); fi
	./scripts/check-i18n-style.sh

APPS=$(shell $(SCRIPTS)/find-apps.sh)

.PHONY: $(APPS:%=%-ct)
define gen-app-ct-target
$1-ct: $(REBAR) merge-config clean-test-cluster-config
	$(eval SUITES := $(shell $(SCRIPTS)/find-suites.sh $1))
ifneq ($(SUITES),)
		@ENABLE_COVER_COMPILE=1 $(REBAR) ct -c -v \
			--readable=$(CT_READABLE) \
			--name $(CT_NODE_NAME) \
			--cover_export_name $(CT_COVER_EXPORT_PREFIX)-$(subst /,-,$1) \
			--suite $(SUITES)
else
		@echo 'No suites found for $1'
endif
endef
$(foreach app,$(APPS),$(eval $(call gen-app-ct-target,$(app))))

## apps/name-prop targets
.PHONY: $(APPS:%=%-prop)
define gen-app-prop-target
$1-prop:
	$(REBAR) proper -d test/props -v -m $(shell $(SCRIPTS)/find-props.sh $1)
endef
$(foreach app,$(APPS),$(eval $(call gen-app-prop-target,$(app))))

.PHONY: ct-suite
ct-suite: $(REBAR) merge-config clean-test-cluster-config
ifneq ($(TESTCASE),)
ifneq ($(GROUP),)
	$(REBAR) ct -v --readable=$(CT_READABLE) --name $(CT_NODE_NAME) --suite $(SUITE)  --case $(TESTCASE) --group $(GROUP)
else
	$(REBAR) ct -v --readable=$(CT_READABLE) --name $(CT_NODE_NAME) --suite $(SUITE)  --case $(TESTCASE)
endif
else ifneq ($(GROUP),)
	$(REBAR) ct -v --readable=$(CT_READABLE) --name $(CT_NODE_NAME) --suite $(SUITE)  --group $(GROUP)
else
	$(REBAR) ct -v --readable=$(CT_READABLE) --name $(CT_NODE_NAME) --suite $(SUITE)
endif

.PHONY: cover
cover: $(REBAR)
	@ENABLE_COVER_COMPILE=1 $(REBAR) cover

.PHONY: coveralls
coveralls: $(REBAR)
	@ENABLE_COVER_COMPILE=1 $(REBAR) as test coveralls send

COMMON_DEPS := $(REBAR)

.PHONY: $(REL_PROFILES)
$(REL_PROFILES:%=%): $(COMMON_DEPS)
	@$(BUILD) $(@) rel

.PHONY: compile $(PROFILES:%=compile-%)
compile: $(PROFILES:%=compile-%)
$(PROFILES:%=compile-%):
	@$(BUILD) $(@:compile-%=%) apps

.PHONY: $(PROFILES:%=compile-%-elixir)
$(PROFILES:%=compile-%-elixir):
	@env IS_ELIXIR=yes $(BUILD) $(@:compile-%-elixir=%) apps

## Not calling rebar3 clean because
## 1. rebar3 clean relies on rebar3, meaning it reads config, fetches dependencies etc.
## 2. it's slow
## NOTE: this does not force rebar3 to fetch new version dependencies
## make clean-all to delete all fetched dependencies for a fresh start-over
.PHONY: clean $(PROFILES:%=clean-%)
clean: $(PROFILES:%=clean-%)
$(PROFILES:%=clean-%):
	@if [ -d _build/$(@:clean-%=%) ]; then \
		rm -f rebar.lock; \
		rm -rf _build/$(@:clean-%=%)/rel; \
		$(FIND) _build/$(@:clean-%=%) -name '*.beam' -o -name '*.so' -o -name '*.app' -o -name '*.appup' -o -name '*.o' -o -name '*.d' -type f | xargs rm -f; \
		$(FIND) _build/$(@:clean-%=%) -type l -delete; \
	fi

.PHONY: clean-all
clean-all:
	@rm -f rebar.lock
	@rm -rf deps
	@rm -rf _build
	@rm -f emqx_dialyzer_*_plt

.PHONY: deps-all
deps-all: $(REBAR) $(PROFILES:%=deps-%)
	@make clean # ensure clean at the end

## deps-<profile> is used in CI scripts to download deps and the
## share downloads between CI steps and/or copied into containers
## which may not have the right credentials
.PHONY: $(PROFILES:%=deps-%)
$(PROFILES:%=deps-%): $(COMMON_DEPS)
	@$(SCRIPTS)/pre-compile.sh $(@:deps-%=%)
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
	@if [ "$${EMQX_RELUP}" = "true" ]; then $(SCRIPTS)/relup-build/download-base-packages.sh $1; fi
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
run: compile-$(PROFILE) quickrun

.PHONY: quickrun
quickrun:
	./dev -p $(PROFILE)

## Take the currently set PROFILE
docker:
	@$(BUILD) $(PROFILE) docker

## docker target is to create docker instructions
.PHONY: $(REL_PROFILES:%=%-docker) $(REL_PROFILES:%=%-elixir-docker)
define gen-docker-target
$1-docker: $(COMMON_DEPS)
	@$(BUILD) $1 docker
endef
ALL_DOCKERS = $(REL_PROFILES) $(REL_PROFILES:%=%-elixir)
$(foreach zt,$(ALL_DOCKERS),$(eval $(call gen-docker-target,$(zt))))

.PHONY:
merge-config:
	@$(SCRIPTS)/merge-config.escript

## elixir target is to create release packages using Elixir's Mix
.PHONY: $(REL_PROFILES:%=%-elixir) $(PKG_PROFILES:%=%-elixir)
$(REL_PROFILES:%=%-elixir) $(PKG_PROFILES:%=%-elixir): $(COMMON_DEPS)
	@env IS_ELIXIR=yes $(BUILD) $(subst -elixir,,$(@)) elixir

.PHONY: $(REL_PROFILES:%=%-elixir-pkg)
define gen-elixir-pkg-target
# the Elixir places the tar in a different path than Rebar3
$1-elixir-pkg: $(COMMON_DEPS)
	@env TAR_PKG_DIR=_build/$1-pkg \
		IS_ELIXIR=yes \
		$(BUILD) $1-pkg pkg
endef
$(foreach pt,$(REL_PROFILES),$(eval $(call gen-elixir-pkg-target,$(pt))))

.PHONY: $(REL_PROFILES:%=%-elixir-tgz)
define gen-elixir-tgz-target
$1-elixir-tgz: $(COMMON_DEPS)
	@env IS_ELIXIR=yes $(BUILD) $1 tgz
endef
ALL_ELIXIR_TGZS = $(REL_PROFILES)
$(foreach tt,$(ALL_ELIXIR_TGZS),$(eval $(call gen-elixir-tgz-target,$(tt))))

.PHONY: fmt
fmt: $(REBAR)
	@$(SCRIPTS)/erlfmt -w '{apps,lib-ee}/*/{src,include,test}/**/*.{erl,hrl,app.src}'
	@$(SCRIPTS)/erlfmt -w 'rebar.config.erl'
	@mix format

.PHONY: clean-test-cluster-config
clean-test-cluster-config:
	@rm -f apps/emqx_conf/data/configs/cluster.hocon || true
