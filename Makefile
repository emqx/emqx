ifeq ($(DEBUG),1)
DEBUG_INFO = $(info $1)
else
DEBUG_INFO = @:
endif
REBAR = $(CURDIR)/rebar3
MIX = $(CURDIR)/mix
BUILD = $(CURDIR)/build
SCRIPTS = $(CURDIR)/scripts
include env.sh

# Dashboard version
# from https://github.com/emqx/emqx-dashboard5
export EMQX_DASHBOARD_VERSION ?= v1.10.6
export EMQX_EE_DASHBOARD_VERSION ?= 2.0.0-M1.202507

export EMQX_RELUP ?= true
export EMQX_REL_FORM ?= tgz
export QUICER_TLS_VER ?= sys

-include default-profile.mk
PROFILE ?= emqx-enterprise
export PROFILE
REL_PROFILES := emqx-enterprise
PKG_PROFILES := emqx-enterprise-pkg
PROFILES := $(REL_PROFILES) $(PKG_PROFILES) default

CT_NODE_NAME ?= 'test@127.0.0.1'
CT_READABLE ?= true
CT_COVER_EXPORT_PREFIX ?= $(PROFILE)

export REBAR_GIT_CLONE_OPTIONS += --depth=1

ELIXIR_COMMON_DEPS := ensure-hex ensure-mix-rebar3 ensure-mix-rebar

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
	@if [ "$(shell uname -m)" = "aarch64" ] && [ "$(shell ./scripts/get-distro.sh)" = "el7" ] ; then \
	    mix archive.install github hexpm/hex branch latest --force; \
	else \
	    mix local.hex 2.2.1 --if-missing --force; \
	fi

.PHONY: ensure-mix-rebar3
ensure-mix-rebar3: $(REBAR)
	@mix local.rebar rebar3 $(CURDIR)/rebar3 --if-missing --force

.PHONY: ensure-mix-rebar
ensure-mix-rebar: $(REBAR)
	@mix local.rebar --if-missing --force


.PHONY: elixir-common-deps
elixir-common-deps: $(ELIXIR_COMMON_DEPS)

.PHONY: mix-deps-get
mix-deps-get: elixir-common-deps
	@mix deps.get

.PHONY: eunit
eunit: $(ELIXIR_COMMON_DEPS) merge-config
	@env PROFILE=$(PROFILE)-test $(MIX) eunit --cover-export-name $(CT_COVER_EXPORT_PREFIX)-eunit

.PHONY: proper
proper: $(ELIXIR_COMMON_DEPS)
	@env PROFILE=$(PROFILE)-test $(MIX) proper

.PHONY: test-compile
test-compile: $(REBAR) merge-config
	env PROFILE=$(PROFILE)-test $(MIX) do deps.get, compile

.PHONY: $(REL_PROFILES:%=%-compile)
$(REL_PROFILES:%=%-compile): $(REBAR) merge-config
	env PROFILE=$(@:%-compile=%) $(MIX) do deps.get, compile

.PHONY: ct
ct: $(REBAR) merge-config
	@env ERL_FLAGS="-kernel prevent_overlapping_partitions false" $(MIX) ct --cover-export-name $(CT_COVER_EXPORT_PREFIX)-ct

## only check bpapi for enterprise profile because it's a super-set.
.PHONY: static_checks
static_checks: $(ELIXIR_COMMON_DEPS)
	@env BPAPI_BUILD_PROFILE=$(PROFILE:%-test=%) \
	    $(MIX) do \
	    emqx.xref, dialyzer --mode classic \
	    emqx.static_checks
	./scripts/check-i18n-style.sh
	./scripts/check_missing_reboot_apps.exs

# Allow user-set CASES environment variable
ifneq ($(CASES),)
CASES_ARG := --cases $(CASES)
endif

# Allow user-set GROUPS environment variable
ifneq ($(GROUPS),)
GROUPS_ARG := --group-paths $(GROUPS)
endif

ifeq ($(ENABLE_COVER_COMPILE),1)
cover_args = --cover-export-name $(CT_COVER_EXPORT_PREFIX)-$(subst /,-,$1)
else
cover_args =
endif

## example:
## env SUITES=apps/appname/test/test_SUITE.erl CASES=t_foo make apps/appname-ct
define gen-app-ct-target
$1-ct: $(REBAR) $(ELIXIR_COMMON_DEPS) merge-config clean-test-cluster-config
	$(eval SUITES := $(shell $(SCRIPTS)/find-suites.sh $1))
ifneq ($(SUITES),)
	env ERL_FLAGS="-kernel prevent_overlapping_partitions false" \
	    PROFILE=$(PROFILE)-test \
	        $(MIX) ct \
		$(call cover_args,$1) \
		--suites $(SUITES) \
		$(GROUPS_ARG) \
		$(CASES_ARG)
else
	@echo 'No suites found for $1'
endif
endef

ifneq ($(filter %-ct,$(MAKECMDGOALS)),)
app_to_test := $(patsubst %-ct,%,$(filter %-ct,$(MAKECMDGOALS)))
$(call DEBUG_INFO,app_to_test $(app_to_test))
$(eval $(call gen-app-ct-target,$(app_to_test)))
endif

## apps/name-prop targets
define gen-app-prop-target
$1-prop:
	$(REBAR) proper -d test/props -v -m $(shell $(SCRIPTS)/find-props.sh $1)
endef
ifneq ($(filter %-prop,$(MAKECMDGOALS)),)
app_to_test := $(patsubst %-prop,%,$(filter %-prop,$(MAKECMDGOALS)))
$(call DEBUG_INFO,app_to_test $(app_to_test))
$(eval $(call gen-app-prop-target,$(app_to_test)))
endif

.PHONY: ct-suite
ct-suite: $(REBAR) merge-config clean-test-cluster-config
ifneq ($(TESTCASE),)
ifneq ($(GROUP),)
	env PROFILE=$(PROFILE)-test $(MIX) do deps.get, ct --suites $(SUITE) --cases $(TESTCASE) --group-paths $(GROUP)
else
	env PROFILE=$(PROFILE)-test $(MIX) do deps.get,  ct --suites $(SUITE)  --cases $(TESTCASE)
endif
else ifneq ($(GROUP),)
	env PROFILE=$(PROFILE)-test $(MIX) do deps.get,  ct --suites $(SUITE)  --group-paths $(GROUP)
else
	env PROFILE=$(PROFILE)-test $(MIX) do deps.get,  ct --suites $(SUITE)
endif

.PHONY: cover
cover:
	@env PROFILE=$(PROFILE)-test mix cover

COMMON_DEPS := $(REBAR)

.PHONY: $(REL_PROFILES)
$(REL_PROFILES:%=%): $(COMMON_DEPS) $(ELIXIR_COMMON_DEPS) mix-deps-get
	@$(BUILD) $(@) rel

.PHONY: clean $(PROFILES:%=clean-%)
clean: $(PROFILES:%=clean-%)
$(PROFILES:%=clean-%):
	@rm -rf _build/$(@:clean-%=%)

.PHONY: clean-all
clean-all:
	@rm -f rebar.lock
	@rm -f mix.lock
	@rm -rf deps
	@rm -rf _build
	@rm -f emqx_dialyzer_*_plt*
	@rm -rf apps/emqx_dashboard/priv

.PHONY: deps-all
deps-all: $(REBAR) $(PROFILES:%=deps-%)
	@make clean # ensure clean at the end

.PHONY: xref
xref:
	@$(MIX) emqx.xref

.PHONY: dialyzer
dialyzer:
	@$(MIX) dialyzer --mode incremental

## rel target is to create release package without relup
.PHONY: $(REL_PROFILES:%=%-rel) $(PKG_PROFILES:%=%-rel)
$(REL_PROFILES:%=%-rel) $(PKG_PROFILES:%=%-rel): $(COMMON_DEPS) $(ELIXIR_COMMON_DEPS)
	@env ELIXIR_MAKE_TAR=yes PROFILE=$(subst -rel,,$(@)) $(BUILD) $(subst -rel,,$(@)) rel

## download relup base packages
.PHONY: $(REL_PROFILES:%=%-relup-downloads)
define download-relup-packages
$1-relup-downloads:
	@if [ "$(shell uname -s)" = "Darwin" ]; then \
		echo "relup is not supported on macOS"; \
	elif [ "$(EMQX_RELUP)" = "true" ]; then \
		$(SCRIPTS)/relup-build/download-base-packages.sh $1; \
	else \
		echo "Skipping relup base packages download for $1"; \
	fi
endef
ALL_ZIPS = $(REL_PROFILES)
$(foreach zt,$(ALL_ZIPS),$(eval $(call download-relup-packages,$(zt))))

## relup target is to create relup instructions
.PHONY: $(REL_PROFILES:%=%-relup)
define gen-relup-target
$1-relup: $(COMMON_DEPS)
	@$(BUILD) $1 relup
endef
ALL_TGZS = $(REL_PROFILES)
$(foreach zt,$(ALL_TGZS),$(eval $(call gen-relup-target,$(zt))))

## tgz target is to create a release package .tar.gz with relup
.PHONY: $(REL_PROFILES:%=%-tgz)
define gen-tgz-target
$1-tgz: $(COMMON_DEPS) $(ELIXIR_COMMON_DEPS) mix-deps-get merge-config
	@env $(BUILD) $1 tgz
endef
ALL_TGZS = $(REL_PROFILES)
$(foreach zt,$(ALL_TGZS),$(eval $(call gen-tgz-target,$(zt))))

## A pkg target depend on a regular release
.PHONY: $(PKG_PROFILES)
define gen-pkg-target
$1: $(COMMON_DEPS) $(ELIXIR_COMMON_DEPS) merge-config
	@env TAR_PKG_DIR=_build/$1 \
		$(BUILD) $1 pkg
endef
$(foreach pt,$(PKG_PROFILES),$(eval $(call gen-pkg-target,$(pt))))

.PHONY: run
run: $(PROFILE) run-console

.PHONY: run-iex
run-iex: $(PROFILE) run-console-iex

.PHONY: run-console
run-console:
	_build/$(PROFILE)/rel/emqx/bin/emqx console

.PHONY: run-console-iex
run-console-iex:
	env EMQX_CONSOLE_FLAVOR=iex _build/$(PROFILE)/rel/emqx/bin/emqx console

.PHONY: repl
repl:
	iex -S mix app.start --no-start

## Take the currently set PROFILE
.PHONY: docker
docker:
	@$(BUILD) $(PROFILE) docker

## docker target is to create docker instructions
.PHONY: $(REL_PROFILES:%=%-docker)
define gen-docker-target
$1-docker: $(COMMON_DEPS)
	@$(BUILD) $1 docker
endef
ALL_DOCKERS = $(REL_PROFILES)
$(foreach zt,$(ALL_DOCKERS),$(eval $(call gen-docker-target,$(zt))))

.PHONY:
merge-config:
	@$(SCRIPTS)/merge-config.escript

.PHONY: fmt
fmt: $(REBAR)
	@find . \( -name '*.app.src' -o \
						 -name '*.erl' -o \
					   -name '*.hrl' -o \
			  		 -name 'rebar.config' -o \
			  		 -name '*.eterm' -o \
			  		 -name '*.escript' \) \
	                          -not -path '*/_build/*' \
	                          -not -path '*/deps/*' \
	                          -not -path '*/_checkouts/*' \
	                          -type f \
		| xargs $(SCRIPTS)/erlfmt -w
	@$(SCRIPTS)/erlfmt -w 'elvis.config'
	@$(SCRIPTS)/erlfmt -w 'bin/nodetool'
	@mix format

.PHONY: fmt-diff
fmt-diff:
	@env ERLFMT_WRITE=true ./scripts/git-hook-pre-commit.sh

.PHONY: clean-test-cluster-config
clean-test-cluster-config:
	@rm -f apps/emqx_conf/data/configs/cluster.hocon || true

.PHONY: spellcheck
spellcheck:
	./scripts/spellcheck/spellcheck.sh _build/docgen/$(PROFILE)/schema-en.json

.PHONY: nothing
nothing:
	@:
