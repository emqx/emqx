# Copyright (c) 2013-2015, Loïc Hoguin <essen@ninenines.eu>
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

.PHONY: all app apps deps search rel docs install-docs check tests clean distclean help erlang-mk

ERLANG_MK_FILENAME := $(realpath $(lastword $(MAKEFILE_LIST)))

ERLANG_MK_VERSION = 2.0.0-pre.2-130-gc6fe5ea

# Core configuration.

PROJECT ?= $(notdir $(CURDIR))
PROJECT := $(strip $(PROJECT))

PROJECT_VERSION ?= rolling
PROJECT_MOD ?= $(PROJECT)_app

# Verbosity.

V ?= 0

verbose_0 = @
verbose_2 = set -x;
verbose = $(verbose_$(V))

gen_verbose_0 = @echo " GEN   " $@;
gen_verbose_2 = set -x;
gen_verbose = $(gen_verbose_$(V))

# Temporary files directory.

ERLANG_MK_TMP ?= $(CURDIR)/.erlang.mk
export ERLANG_MK_TMP

# "erl" command.

ERL = erl +A0 -noinput -boot start_clean

# Platform detection.

ifeq ($(PLATFORM),)
UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Linux)
PLATFORM = linux
else ifeq ($(UNAME_S),Darwin)
PLATFORM = darwin
else ifeq ($(UNAME_S),SunOS)
PLATFORM = solaris
else ifeq ($(UNAME_S),GNU)
PLATFORM = gnu
else ifeq ($(UNAME_S),FreeBSD)
PLATFORM = freebsd
else ifeq ($(UNAME_S),NetBSD)
PLATFORM = netbsd
else ifeq ($(UNAME_S),OpenBSD)
PLATFORM = openbsd
else ifeq ($(UNAME_S),DragonFly)
PLATFORM = dragonfly
else ifeq ($(shell uname -o),Msys)
PLATFORM = msys2
else
$(error Unable to detect platform. Please open a ticket with the output of uname -a.)
endif

export PLATFORM
endif

# Core targets.

all:: deps app rel

# Noop to avoid a Make warning when there's nothing to do.
rel::
	$(verbose) :

check:: tests

clean:: clean-crashdump

clean-crashdump:
ifneq ($(wildcard erl_crash.dump),)
	$(gen_verbose) rm -f erl_crash.dump
endif

distclean:: clean distclean-tmp

distclean-tmp:
	$(gen_verbose) rm -rf $(ERLANG_MK_TMP)

help::
	$(verbose) printf "%s\n" \
		"erlang.mk (version $(ERLANG_MK_VERSION)) is distributed under the terms of the ISC License." \
		"Copyright (c) 2013-2015 Loïc Hoguin <essen@ninenines.eu>" \
		"" \
		"Usage: [V=1] $(MAKE) [target]..." \
		"" \
		"Core targets:" \
		"  all           Run deps, app and rel targets in that order" \
		"  app           Compile the project" \
		"  deps          Fetch dependencies (if needed) and compile them" \
		"  search q=...  Search for a package in the built-in index" \
		"  rel           Build a release for this project, if applicable" \
		"  docs          Build the documentation for this project" \
		"  install-docs  Install the man pages for this project" \
		"  check         Compile and run all tests and analysis for this project" \
		"  tests         Run the tests for this project" \
		"  clean         Delete temporary and output files from most targets" \
		"  distclean     Delete all temporary and output files" \
		"  help          Display this help and exit" \
		"  erlang-mk     Update erlang.mk to the latest version"

# Core functions.

empty :=
space := $(empty) $(empty)
tab := $(empty)	$(empty)
comma := ,

define newline


endef

define comma_list
$(subst $(space),$(comma),$(strip $(1)))
endef

# Adding erlang.mk to make Erlang scripts who call init:get_plain_arguments() happy.
define erlang
$(ERL) $(2) -pz $(ERLANG_MK_TMP)/rebar/ebin -eval "$(subst $(newline),,$(subst ",\",$(1)))" -- erlang.mk
endef

ifeq ($(PLATFORM),msys2)
core_native_path = $(subst \,\\\\,$(shell cygpath -w $1))
else
core_native_path = $1
endif

ifeq ($(shell which wget 2>/dev/null | wc -l), 1)
define core_http_get
	wget --no-check-certificate -O $(1) $(2)|| rm $(1)
endef
else
define core_http_get.erl
	ssl:start(),
	inets:start(),
	case httpc:request(get, {"$(2)", []}, [{autoredirect, true}], []) of
		{ok, {{_, 200, _}, _, Body}} ->
			case file:write_file("$(1)", Body) of
				ok -> ok;
				{error, R1} -> halt(R1)
			end;
		{error, R2} ->
			halt(R2)
	end,
	halt(0).
endef

define core_http_get
	$(call erlang,$(call core_http_get.erl,$(call core_native_path,$1),$2))
endef
endif

core_eq = $(and $(findstring $(1),$(2)),$(findstring $(2),$(1)))

core_find = $(if $(wildcard $1),$(shell find $(1:%/=%) -type f -name $(subst *,\*,$2)))

core_lc = $(subst A,a,$(subst B,b,$(subst C,c,$(subst D,d,$(subst E,e,$(subst F,f,$(subst G,g,$(subst H,h,$(subst I,i,$(subst J,j,$(subst K,k,$(subst L,l,$(subst M,m,$(subst N,n,$(subst O,o,$(subst P,p,$(subst Q,q,$(subst R,r,$(subst S,s,$(subst T,t,$(subst U,u,$(subst V,v,$(subst W,w,$(subst X,x,$(subst Y,y,$(subst Z,z,$(1)))))))))))))))))))))))))))

core_ls = $(filter-out $(1),$(shell echo $(1)))

# @todo Use a solution that does not require using perl.
core_relpath = $(shell perl -e 'use File::Spec; print File::Spec->abs2rel(@ARGV) . "\n"' $1 $2)

# Automated update.

ERLANG_MK_REPO ?= https://github.com/ninenines/erlang.mk
ERLANG_MK_COMMIT ?=
ERLANG_MK_BUILD_CONFIG ?= build.config
ERLANG_MK_BUILD_DIR ?= .erlang.mk.build

erlang-mk:
	git clone $(ERLANG_MK_REPO) $(ERLANG_MK_BUILD_DIR)
ifdef ERLANG_MK_COMMIT
	cd $(ERLANG_MK_BUILD_DIR) && git checkout $(ERLANG_MK_COMMIT)
endif
	if [ -f $(ERLANG_MK_BUILD_CONFIG) ]; then cp $(ERLANG_MK_BUILD_CONFIG) $(ERLANG_MK_BUILD_DIR)/build.config; fi
	$(MAKE) -C $(ERLANG_MK_BUILD_DIR)
	cp $(ERLANG_MK_BUILD_DIR)/erlang.mk ./erlang.mk
	rm -rf $(ERLANG_MK_BUILD_DIR)

# Copyright (c) 2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: search

define pkg_print
	$(verbose) printf "%s\n" \
		$(if $(call core_eq,$(1),$(pkg_$(1)_name)),,"Pkg name:    $(1)") \
		"App name:    $(pkg_$(1)_name)" \
		"Description: $(pkg_$(1)_description)" \
		"Home page:   $(pkg_$(1)_homepage)" \
		"Fetch with:  $(pkg_$(1)_fetch)" \
		"Repository:  $(pkg_$(1)_repo)" \
		"Commit:      $(pkg_$(1)_commit)" \
		""

endef

search:
ifdef q
	$(foreach p,$(PACKAGES), \
		$(if $(findstring $(call core_lc,$(q)),$(call core_lc,$(pkg_$(p)_name) $(pkg_$(p)_description))), \
			$(call pkg_print,$(p))))
else
	$(foreach p,$(PACKAGES),$(call pkg_print,$(p)))
endif

# Copyright (c) 2013-2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: distclean-deps

# Configuration.

ifdef OTP_DEPS
$(warning The variable OTP_DEPS is deprecated in favor of LOCAL_DEPS.)
endif

IGNORE_DEPS ?=
export IGNORE_DEPS

APPS_DIR ?= $(CURDIR)/apps
export APPS_DIR

DEPS_DIR ?= $(CURDIR)/deps
export DEPS_DIR

REBAR_DEPS_DIR = $(DEPS_DIR)
export REBAR_DEPS_DIR

dep_name = $(if $(dep_$(1)),$(1),$(if $(pkg_$(1)_name),$(pkg_$(1)_name),$(1)))
dep_repo = $(patsubst git://github.com/%,https://github.com/%, \
	$(if $(dep_$(1)),$(word 2,$(dep_$(1))),$(pkg_$(1)_repo)))
dep_commit = $(if $(dep_$(1)_commit),$(dep_$(1)_commit),$(if $(dep_$(1)),$(word 3,$(dep_$(1))),$(pkg_$(1)_commit)))

ALL_APPS_DIRS = $(if $(wildcard $(APPS_DIR)/),$(filter-out $(APPS_DIR),$(shell find $(APPS_DIR) -maxdepth 1 -type d)))
ALL_DEPS_DIRS = $(addprefix $(DEPS_DIR)/,$(foreach dep,$(filter-out $(IGNORE_DEPS),$(BUILD_DEPS) $(DEPS)),$(call dep_name,$(dep))))

ifeq ($(filter $(APPS_DIR) $(DEPS_DIR),$(subst :, ,$(ERL_LIBS))),)
ifeq ($(ERL_LIBS),)
	ERL_LIBS = $(APPS_DIR):$(DEPS_DIR)
else
	ERL_LIBS := $(ERL_LIBS):$(APPS_DIR):$(DEPS_DIR)
endif
endif
export ERL_LIBS

export NO_AUTOPATCH

# Verbosity.

dep_verbose_0 = @echo " DEP   " $(1);
dep_verbose_2 = set -x;
dep_verbose = $(dep_verbose_$(V))

# Core targets.

ifdef IS_APP
apps::
else
apps:: $(ALL_APPS_DIRS)
ifeq ($(IS_APP)$(IS_DEP),)
	$(verbose) rm -f $(ERLANG_MK_TMP)/apps.log
endif
	$(verbose) mkdir -p $(ERLANG_MK_TMP)
# Create ebin directory for all apps to make sure Erlang recognizes them
# as proper OTP applications when using -include_lib. This is a temporary
# fix, a proper fix would be to compile apps/* in the right order.
	$(verbose) for dep in $(ALL_APPS_DIRS) ; do \
		mkdir -p $$dep/ebin || exit $$?; \
	done
	$(verbose) for dep in $(ALL_APPS_DIRS) ; do \
		if grep -qs ^$$dep$$ $(ERLANG_MK_TMP)/apps.log; then \
			:; \
		else \
			echo $$dep >> $(ERLANG_MK_TMP)/apps.log; \
			$(MAKE) -C $$dep IS_APP=1 || exit $$?; \
		fi \
	done
endif

ifneq ($(SKIP_DEPS),)
deps::
else
deps:: $(ALL_DEPS_DIRS) apps
ifeq ($(IS_APP)$(IS_DEP),)
	$(verbose) rm -f $(ERLANG_MK_TMP)/deps.log
endif
	$(verbose) mkdir -p $(ERLANG_MK_TMP)
	$(verbose) for dep in $(ALL_DEPS_DIRS) ; do \
		if grep -qs ^$$dep$$ $(ERLANG_MK_TMP)/deps.log; then \
			:; \
		else \
			echo $$dep >> $(ERLANG_MK_TMP)/deps.log; \
			if [ -f $$dep/GNUmakefile ] || [ -f $$dep/makefile ] || [ -f $$dep/Makefile ]; then \
				$(MAKE) -C $$dep IS_DEP=1 || exit $$?; \
			else \
				echo "Error: No Makefile to build dependency $$dep."; \
				exit 2; \
			fi \
		fi \
	done
endif

# Deps related targets.

# @todo rename GNUmakefile and makefile into Makefile first, if they exist
# While Makefile file could be GNUmakefile or makefile,
# in practice only Makefile is needed so far.
define dep_autopatch
	if [ -f $(DEPS_DIR)/$(1)/erlang.mk ]; then \
		$(call erlang,$(call dep_autopatch_appsrc.erl,$(1))); \
		$(call dep_autopatch_erlang_mk,$(1)); \
	elif [ -f $(DEPS_DIR)/$(1)/Makefile ]; then \
		if [ 0 != `grep -c "include ../\w*\.mk" $(DEPS_DIR)/$(1)/Makefile` ]; then \
			$(call dep_autopatch2,$(1)); \
		elif [ 0 != `grep -ci rebar $(DEPS_DIR)/$(1)/Makefile` ]; then \
			$(call dep_autopatch2,$(1)); \
		elif [ -n "`find $(DEPS_DIR)/$(1)/ -type f -name \*.mk -not -name erlang.mk -exec grep -i rebar '{}' \;`" ]; then \
			$(call dep_autopatch2,$(1)); \
		else \
			$(call erlang,$(call dep_autopatch_app.erl,$(1))); \
		fi \
	else \
		if [ ! -d $(DEPS_DIR)/$(1)/src/ ]; then \
			$(call dep_autopatch_noop,$(1)); \
		else \
			$(call dep_autopatch2,$(1)); \
		fi \
	fi
endef

define dep_autopatch2
	if [ -f $(DEPS_DIR)/$1/src/$1.app.src.script ]; then \
		$(call erlang,$(call dep_autopatch_appsrc_script.erl,$(1))); \
	fi; \
	$(call erlang,$(call dep_autopatch_appsrc.erl,$(1))); \
	if [ -f $(DEPS_DIR)/$(1)/rebar -o -f $(DEPS_DIR)/$(1)/rebar.config -o -f $(DEPS_DIR)/$(1)/rebar.config.script ]; then \
		$(call dep_autopatch_fetch_rebar); \
		$(call dep_autopatch_rebar,$(1)); \
	else \
		$(call dep_autopatch_gen,$(1)); \
	fi
endef

define dep_autopatch_noop
	printf "noop:\n" > $(DEPS_DIR)/$(1)/Makefile
endef

# Overwrite erlang.mk with the current file by default.
ifeq ($(NO_AUTOPATCH_ERLANG_MK),)
define dep_autopatch_erlang_mk
	echo "include $(call core_relpath,$(dir $(ERLANG_MK_FILENAME)),$(DEPS_DIR)/app)/erlang.mk" \
		> $(DEPS_DIR)/$1/erlang.mk
endef
else
define dep_autopatch_erlang_mk
	:
endef
endif

define dep_autopatch_gen
	printf "%s\n" \
		"ERLC_OPTS = +debug_info" \
		"include ../../erlang.mk" > $(DEPS_DIR)/$(1)/Makefile
endef

define dep_autopatch_fetch_rebar
	mkdir -p $(ERLANG_MK_TMP); \
	if [ ! -d $(ERLANG_MK_TMP)/rebar ]; then \
		git clone -q -n -- https://github.com/rebar/rebar $(ERLANG_MK_TMP)/rebar; \
		cd $(ERLANG_MK_TMP)/rebar; \
		git checkout -q 791db716b5a3a7671e0b351f95ddf24b848ee173; \
		$(MAKE); \
		cd -; \
	fi
endef

define dep_autopatch_rebar
	if [ -f $(DEPS_DIR)/$(1)/Makefile ]; then \
		mv $(DEPS_DIR)/$(1)/Makefile $(DEPS_DIR)/$(1)/Makefile.orig.mk; \
	fi; \
	$(call erlang,$(call dep_autopatch_rebar.erl,$(1))); \
	rm -f $(DEPS_DIR)/$(1)/ebin/$(1).app
endef

define dep_autopatch_rebar.erl
	application:load(rebar),
	application:set_env(rebar, log_level, debug),
	Conf1 = case file:consult("$(call core_native_path,$(DEPS_DIR)/$1/rebar.config)") of
		{ok, Conf0} -> Conf0;
		_ -> []
	end,
	{Conf, OsEnv} = fun() ->
		case filelib:is_file("$(call core_native_path,$(DEPS_DIR)/$1/rebar.config.script)") of
			false -> {Conf1, []};
			true ->
				Bindings0 = erl_eval:new_bindings(),
				Bindings1 = erl_eval:add_binding('CONFIG', Conf1, Bindings0),
				Bindings = erl_eval:add_binding('SCRIPT', "$(call core_native_path,$(DEPS_DIR)/$1/rebar.config.script)", Bindings1),
				Before = os:getenv(),
				{ok, Conf2} = file:script("$(call core_native_path,$(DEPS_DIR)/$1/rebar.config.script)", Bindings),
				{Conf2, lists:foldl(fun(E, Acc) -> lists:delete(E, Acc) end, os:getenv(), Before)}
		end
	end(),
	Write = fun (Text) ->
		file:write_file("$(call core_native_path,$(DEPS_DIR)/$1/Makefile)", Text, [append])
	end,
	Escape = fun (Text) ->
		re:replace(Text, "\\\\$$", "\$$$$", [global, {return, list}])
	end,
	Write("IGNORE_DEPS += edown eper eunit_formatters meck node_package "
		"rebar_lock_deps_plugin rebar_vsn_plugin reltool_util\n"),
	Write("C_SRC_DIR = /path/do/not/exist\n"),
	Write("C_SRC_TYPE = rebar\n"),
	Write("DRV_CFLAGS = -fPIC\nexport DRV_CFLAGS\n"),
	Write(["ERLANG_ARCH = ", rebar_utils:wordsize(), "\nexport ERLANG_ARCH\n"]),
	fun() ->
		Write("ERLC_OPTS = +debug_info\nexport ERLC_OPTS\n"),
		case lists:keyfind(erl_opts, 1, Conf) of
			false -> ok;
			{_, ErlOpts} ->
				lists:foreach(fun
					({d, D}) ->
						Write("ERLC_OPTS += -D" ++ atom_to_list(D) ++ "=1\n");
					({i, I}) ->
						Write(["ERLC_OPTS += -I ", I, "\n"]);
					({platform_define, Regex, D}) ->
						case rebar_utils:is_arch(Regex) of
							true -> Write("ERLC_OPTS += -D" ++ atom_to_list(D) ++ "=1\n");
							false -> ok
						end;
					({parse_transform, PT}) ->
						Write("ERLC_OPTS += +'{parse_transform, " ++ atom_to_list(PT) ++ "}'\n");
					(_) -> ok
				end, ErlOpts)
		end,
		Write("\n")
	end(),
	fun() ->
		File = case lists:keyfind(deps, 1, Conf) of
			false -> [];
			{_, Deps} ->
				[begin case case Dep of
							{N, S} when is_atom(N), is_list(S) -> {N, {hex, S}};
							{N, S} when is_tuple(S) -> {N, S};
							{N, _, S} -> {N, S};
							{N, _, S, _} -> {N, S};
							_ -> false
						end of
					false -> ok;
					{Name, Source} ->
						{Method, Repo, Commit} = case Source of
							{hex, V} -> {hex, V, undefined};
							{git, R} -> {git, R, master};
							{M, R, {branch, C}} -> {M, R, C};
							{M, R, {ref, C}} -> {M, R, C};
							{M, R, {tag, C}} -> {M, R, C};
							{M, R, C} -> {M, R, C}
						end,
						Write(io_lib:format("DEPS += ~s\ndep_~s = ~s ~s ~s~n", [Name, Name, Method, Repo, Commit]))
				end end || Dep <- Deps]
		end
	end(),
	fun() ->
		case lists:keyfind(erl_first_files, 1, Conf) of
			false -> ok;
			{_, Files} ->
				Names = [[" ", case lists:reverse(F) of
					"lre." ++ Elif -> lists:reverse(Elif);
					Elif -> lists:reverse(Elif)
				end] || "src/" ++ F <- Files],
				Write(io_lib:format("COMPILE_FIRST +=~s\n", [Names]))
		end
	end(),
	Write("\n\nrebar_dep: preprocess pre-deps deps pre-app app\n"),
	Write("\npreprocess::\n"),
	Write("\npre-deps::\n"),
	Write("\npre-app::\n"),
	PatchHook = fun(Cmd) ->
		case Cmd of
			"make -C" ++ Cmd1 -> "$$\(MAKE) -C" ++ Escape(Cmd1);
			"gmake -C" ++ Cmd1 -> "$$\(MAKE) -C" ++ Escape(Cmd1);
			"make " ++ Cmd1 -> "$$\(MAKE) -f Makefile.orig.mk " ++ Escape(Cmd1);
			"gmake " ++ Cmd1 -> "$$\(MAKE) -f Makefile.orig.mk " ++ Escape(Cmd1);
			_ -> Escape(Cmd)
		end
	end,
	fun() ->
		case lists:keyfind(pre_hooks, 1, Conf) of
			false -> ok;
			{_, Hooks} ->
				[case H of
					{'get-deps', Cmd} ->
						Write("\npre-deps::\n\t" ++ PatchHook(Cmd) ++ "\n");
					{compile, Cmd} ->
						Write("\npre-app::\n\tCC=$$\(CC) " ++ PatchHook(Cmd) ++ "\n");
					{Regex, compile, Cmd} ->
						case rebar_utils:is_arch(Regex) of
							true -> Write("\npre-app::\n\tCC=$$\(CC) " ++ PatchHook(Cmd) ++ "\n");
							false -> ok
						end;
					_ -> ok
				end || H <- Hooks]
		end
	end(),
	ShellToMk = fun(V) ->
		re:replace(re:replace(V, "(\\\\$$)(\\\\w*)", "\\\\1(\\\\2)", [global]),
			"-Werror\\\\b", "", [{return, list}, global])
	end,
	PortSpecs = fun() ->
		case lists:keyfind(port_specs, 1, Conf) of
			false ->
				case filelib:is_dir("$(call core_native_path,$(DEPS_DIR)/$1/c_src)") of
					false -> [];
					true ->
						[{"priv/" ++ proplists:get_value(so_name, Conf, "$(1)_drv.so"),
							proplists:get_value(port_sources, Conf, ["c_src/*.c"]), []}]
				end;
			{_, Specs} ->
				lists:flatten([case S of
					{Output, Input} -> {ShellToMk(Output), Input, []};
					{Regex, Output, Input} ->
						case rebar_utils:is_arch(Regex) of
							true -> {ShellToMk(Output), Input, []};
							false -> []
						end;
					{Regex, Output, Input, [{env, Env}]} ->
						case rebar_utils:is_arch(Regex) of
							true -> {ShellToMk(Output), Input, Env};
							false -> []
						end
				end || S <- Specs])
		end
	end(),
	PortSpecWrite = fun (Text) ->
		file:write_file("$(call core_native_path,$(DEPS_DIR)/$1/c_src/Makefile.erlang.mk)", Text, [append])
	end,
	case PortSpecs of
		[] -> ok;
		_ ->
			Write("\npre-app::\n\t$$\(MAKE) -f c_src/Makefile.erlang.mk\n"),
			PortSpecWrite(io_lib:format("ERL_CFLAGS = -finline-functions -Wall -fPIC -I \\"~s/erts-~s/include\\" -I \\"~s\\"\n",
				[code:root_dir(), erlang:system_info(version), code:lib_dir(erl_interface, include)])),
			PortSpecWrite(io_lib:format("ERL_LDFLAGS = -L \\"~s\\" -lerl_interface -lei\n",
				[code:lib_dir(erl_interface, lib)])),
			[PortSpecWrite(["\n", E, "\n"]) || E <- OsEnv],
			FilterEnv = fun(Env) ->
				lists:flatten([case E of
					{_, _} -> E;
					{Regex, K, V} ->
						case rebar_utils:is_arch(Regex) of
							true -> {K, V};
							false -> []
						end
				end || E <- Env])
			end,
			MergeEnv = fun(Env) ->
				lists:foldl(fun ({K, V}, Acc) ->
					case lists:keyfind(K, 1, Acc) of
						false -> [{K, rebar_utils:expand_env_variable(V, K, "")}|Acc];
						{_, V0} -> [{K, rebar_utils:expand_env_variable(V, K, V0)}|Acc]
					end
				end, [], Env)
			end,
			PortEnv = case lists:keyfind(port_env, 1, Conf) of
				false -> [];
				{_, PortEnv0} -> FilterEnv(PortEnv0)
			end,
			PortSpec = fun ({Output, Input0, Env}) ->
				filelib:ensure_dir("$(call core_native_path,$(DEPS_DIR)/$1/)" ++ Output),
				Input = [[" ", I] || I <- Input0],
				PortSpecWrite([
					[["\n", K, " = ", ShellToMk(V)] || {K, V} <- lists:reverse(MergeEnv(PortEnv))],
					case $(PLATFORM) of
						darwin -> "\n\nLDFLAGS += -flat_namespace -undefined suppress";
						_ -> ""
					end,
					"\n\nall:: ", Output, "\n\n",
					"%.o: %.c\n\t$$\(CC) -c -o $$\@ $$\< $$\(CFLAGS) $$\(ERL_CFLAGS) $$\(DRV_CFLAGS) $$\(EXE_CFLAGS)\n\n",
					"%.o: %.C\n\t$$\(CXX) -c -o $$\@ $$\< $$\(CXXFLAGS) $$\(ERL_CFLAGS) $$\(DRV_CFLAGS) $$\(EXE_CFLAGS)\n\n",
					"%.o: %.cc\n\t$$\(CXX) -c -o $$\@ $$\< $$\(CXXFLAGS) $$\(ERL_CFLAGS) $$\(DRV_CFLAGS) $$\(EXE_CFLAGS)\n\n",
					"%.o: %.cpp\n\t$$\(CXX) -c -o $$\@ $$\< $$\(CXXFLAGS) $$\(ERL_CFLAGS) $$\(DRV_CFLAGS) $$\(EXE_CFLAGS)\n\n",
					[[Output, ": ", K, " = ", ShellToMk(V), "\n"] || {K, V} <- lists:reverse(MergeEnv(FilterEnv(Env)))],
					Output, ": $$\(foreach ext,.c .C .cc .cpp,",
						"$$\(patsubst %$$\(ext),%.o,$$\(filter %$$\(ext),$$\(wildcard", Input, "))))\n",
					"\t$$\(CC) -o $$\@ $$\? $$\(LDFLAGS) $$\(ERL_LDFLAGS) $$\(DRV_LDFLAGS) $$\(EXE_LDFLAGS)",
					case {filename:extension(Output), $(PLATFORM)} of
					    {[], _} -> "\n";
					    {_, darwin} -> "\n";
					    _ -> " -shared\n"
					end])
			end,
			[PortSpec(S) || S <- PortSpecs]
	end,
	Write("\ninclude $(call core_relpath,$(dir $(ERLANG_MK_FILENAME)),$(DEPS_DIR)/app)/erlang.mk"),
	RunPlugin = fun(Plugin, Step) ->
		case erlang:function_exported(Plugin, Step, 2) of
			false -> ok;
			true ->
				c:cd("$(call core_native_path,$(DEPS_DIR)/$1/)"),
				Ret = Plugin:Step({config, "", Conf, dict:new(), dict:new(), dict:new(),
					dict:store(base_dir, "", dict:new())}, undefined),
				io:format("rebar plugin ~p step ~p ret ~p~n", [Plugin, Step, Ret])
		end
	end,
	fun() ->
		case lists:keyfind(plugins, 1, Conf) of
			false -> ok;
			{_, Plugins} ->
				[begin
					case lists:keyfind(deps, 1, Conf) of
						false -> ok;
						{_, Deps} ->
							case lists:keyfind(P, 1, Deps) of
								false -> ok;
								_ ->
									Path = "$(call core_native_path,$(DEPS_DIR)/)" ++ atom_to_list(P),
									io:format("~s", [os:cmd("$(MAKE) -C $(call core_native_path,$(DEPS_DIR)/$1) " ++ Path)]),
									io:format("~s", [os:cmd("$(MAKE) -C " ++ Path ++ " IS_DEP=1")]),
									code:add_patha(Path ++ "/ebin")
							end
					end
				end || P <- Plugins],
				[case code:load_file(P) of
					{module, P} -> ok;
					_ ->
						case lists:keyfind(plugin_dir, 1, Conf) of
							false -> ok;
							{_, PluginsDir} ->
								ErlFile = "$(call core_native_path,$(DEPS_DIR)/$1/)" ++ PluginsDir ++ "/" ++ atom_to_list(P) ++ ".erl",
								{ok, P, Bin} = compile:file(ErlFile, [binary]),
								{module, P} = code:load_binary(P, ErlFile, Bin)
						end
				end || P <- Plugins],
				[RunPlugin(P, preprocess) || P <- Plugins],
				[RunPlugin(P, pre_compile) || P <- Plugins],
				[RunPlugin(P, compile) || P <- Plugins]
		end
	end(),
	halt()
endef

define dep_autopatch_app.erl
	UpdateModules = fun(App) ->
		case filelib:is_regular(App) of
			false -> ok;
			true ->
				{ok, [{application, '$(1)', L0}]} = file:consult(App),
				Mods = filelib:fold_files("$(call core_native_path,$(DEPS_DIR)/$1/src)", "\\\\.erl$$", true,
					fun (F, Acc) -> [list_to_atom(filename:rootname(filename:basename(F)))|Acc] end, []),
				L = lists:keystore(modules, 1, L0, {modules, Mods}),
				ok = file:write_file(App, io_lib:format("~p.~n", [{application, '$(1)', L}]))
		end
	end,
	UpdateModules("$(call core_native_path,$(DEPS_DIR)/$1/ebin/$1.app)"),
	halt()
endef

define dep_autopatch_appsrc_script.erl
	AppSrc = "$(call core_native_path,$(DEPS_DIR)/$1/src/$1.app.src)",
	AppSrcScript = AppSrc ++ ".script",
	Bindings = erl_eval:new_bindings(),
	{ok, Conf} = file:script(AppSrcScript, Bindings),
	ok = file:write_file(AppSrc, io_lib:format("~p.~n", [Conf])),
	halt()
endef

define dep_autopatch_appsrc.erl
	AppSrcOut = "$(call core_native_path,$(DEPS_DIR)/$1/src/$1.app.src)",
	AppSrcIn = case filelib:is_regular(AppSrcOut) of false -> "$(call core_native_path,$(DEPS_DIR)/$1/ebin/$1.app)"; true -> AppSrcOut end,
	case filelib:is_regular(AppSrcIn) of
		false -> ok;
		true ->
			{ok, [{application, $(1), L0}]} = file:consult(AppSrcIn),
			L1 = lists:keystore(modules, 1, L0, {modules, []}),
			L2 = case lists:keyfind(vsn, 1, L1) of {_, git} -> lists:keyreplace(vsn, 1, L1, {vsn, "git"}); _ -> L1 end,
			L3 = case lists:keyfind(registered, 1, L2) of false -> [{registered, []}|L2]; _ -> L2 end,
			ok = file:write_file(AppSrcOut, io_lib:format("~p.~n", [{application, $(1), L3}])),
			case AppSrcOut of AppSrcIn -> ok; _ -> ok = file:delete(AppSrcIn) end
	end,
	halt()
endef

define dep_fetch_git
	git clone -q -n -- $(call dep_repo,$(1)) $(DEPS_DIR)/$(call dep_name,$(1)); \
	cd $(DEPS_DIR)/$(call dep_name,$(1)) && git checkout -q $(call dep_commit,$(1));
endef

define dep_fetch_git-submodule
	git submodule update --init -- $(DEPS_DIR)/$1;
endef

define dep_fetch_hg
	hg clone -q -U $(call dep_repo,$(1)) $(DEPS_DIR)/$(call dep_name,$(1)); \
	cd $(DEPS_DIR)/$(call dep_name,$(1)) && hg update -q $(call dep_commit,$(1));
endef

define dep_fetch_svn
	svn checkout -q $(call dep_repo,$(1)) $(DEPS_DIR)/$(call dep_name,$(1));
endef

define dep_fetch_cp
	cp -R $(call dep_repo,$(1)) $(DEPS_DIR)/$(call dep_name,$(1));
endef

define dep_fetch_hex.erl
	ssl:start(),
	inets:start(),
	{ok, {{_, 200, _}, _, Body}} = httpc:request(get,
		{"https://s3.amazonaws.com/s3.hex.pm/tarballs/$(1)-$(2).tar", []},
		[], [{body_format, binary}]),
	{ok, Files} = erl_tar:extract({binary, Body}, [memory]),
	{_, Source} = lists:keyfind("contents.tar.gz", 1, Files),
	ok = erl_tar:extract({binary, Source}, [{cwd, "$(call core_native_path,$(DEPS_DIR)/$1)"}, compressed]),
	halt()
endef

# Hex only has a package version. No need to look in the Erlang.mk packages.
define dep_fetch_hex
	$(call erlang,$(call dep_fetch_hex.erl,$(1),$(strip $(word 2,$(dep_$(1))))));
endef

define dep_fetch_fail
	echo "Error: Unknown or invalid dependency: $(1)." >&2; \
	exit 78;
endef

# Kept for compatibility purposes with older Erlang.mk configuration.
define dep_fetch_legacy
	$(warning WARNING: '$(1)' dependency configuration uses deprecated format.) \
	git clone -q -n -- $(word 1,$(dep_$(1))) $(DEPS_DIR)/$(1); \
	cd $(DEPS_DIR)/$(1) && git checkout -q $(if $(word 2,$(dep_$(1))),$(word 2,$(dep_$(1))),master);
endef

define dep_fetch
	$(if $(dep_$(1)), \
		$(if $(dep_fetch_$(word 1,$(dep_$(1)))), \
			$(word 1,$(dep_$(1))), \
			$(if $(IS_DEP),legacy,fail)), \
		$(if $(filter $(1),$(PACKAGES)), \
			$(pkg_$(1)_fetch), \
			fail))
endef

define dep_target
$(DEPS_DIR)/$(call dep_name,$1):
	$(eval DEP_NAME := $(call dep_name,$1))
	$(eval DEP_STR := $(if $(filter-out $1,$(DEP_NAME)),$1,"$1 ($(DEP_NAME))"))
	$(verbose) if test -d $(APPS_DIR)/$(DEP_NAME); then \
		echo "Error: Dependency" $(DEP_STR) "conflicts with application found in $(APPS_DIR)/$(DEP_NAME)."; \
		exit 17; \
	fi
	$(verbose) mkdir -p $(DEPS_DIR)
	$(dep_verbose) $(call dep_fetch_$(strip $(call dep_fetch,$(1))),$(1))
	$(verbose) if [ -f $(DEPS_DIR)/$(1)/configure.ac -o -f $(DEPS_DIR)/$(1)/configure.in ] \
			&& [ ! -f $(DEPS_DIR)/$(1)/configure ]; then \
		echo " AUTO  " $(1); \
		cd $(DEPS_DIR)/$(1) && autoreconf -Wall -vif -I m4; \
	fi
	- $(verbose) if [ -f $(DEPS_DIR)/$(DEP_NAME)/configure ]; then \
		echo " CONF  " $(DEP_STR); \
		cd $(DEPS_DIR)/$(DEP_NAME) && ./configure; \
	fi
ifeq ($(filter $(1),$(NO_AUTOPATCH)),)
	$(verbose) if [ "$(1)" = "amqp_client" -a "$(RABBITMQ_CLIENT_PATCH)" ]; then \
		if [ ! -d $(DEPS_DIR)/rabbitmq-codegen ]; then \
			echo " PATCH  Downloading rabbitmq-codegen"; \
			git clone https://github.com/rabbitmq/rabbitmq-codegen.git $(DEPS_DIR)/rabbitmq-codegen; \
		fi; \
		if [ ! -d $(DEPS_DIR)/rabbitmq-server ]; then \
			echo " PATCH  Downloading rabbitmq-server"; \
			git clone https://github.com/rabbitmq/rabbitmq-server.git $(DEPS_DIR)/rabbitmq-server; \
		fi; \
		ln -s $(DEPS_DIR)/amqp_client/deps/rabbit_common-0.0.0 $(DEPS_DIR)/rabbit_common; \
	elif [ "$(1)" = "rabbit" -a "$(RABBITMQ_SERVER_PATCH)" ]; then \
		if [ ! -d $(DEPS_DIR)/rabbitmq-codegen ]; then \
			echo " PATCH  Downloading rabbitmq-codegen"; \
			git clone https://github.com/rabbitmq/rabbitmq-codegen.git $(DEPS_DIR)/rabbitmq-codegen; \
		fi \
	else \
		$$(call dep_autopatch,$(DEP_NAME)) \
	fi
endif
endef

$(foreach dep,$(BUILD_DEPS) $(DEPS),$(eval $(call dep_target,$(dep))))

ifndef IS_APP
clean:: clean-apps

clean-apps:
	$(verbose) for dep in $(ALL_APPS_DIRS) ; do \
		$(MAKE) -C $$dep clean IS_APP=1 || exit $$?; \
	done

distclean:: distclean-apps

distclean-apps:
	$(verbose) for dep in $(ALL_APPS_DIRS) ; do \
		$(MAKE) -C $$dep distclean IS_APP=1 || exit $$?; \
	done
endif

ifndef SKIP_DEPS
distclean:: distclean-deps

distclean-deps:
	$(gen_verbose) rm -rf $(DEPS_DIR)
endif

# External plugins.

DEP_PLUGINS ?=

define core_dep_plugin
-include $(DEPS_DIR)/$(1)

$(DEPS_DIR)/$(1): $(DEPS_DIR)/$(2) ;
endef

$(foreach p,$(DEP_PLUGINS),\
	$(eval $(if $(findstring /,$p),\
		$(call core_dep_plugin,$p,$(firstword $(subst /, ,$p))),\
		$(call core_dep_plugin,$p/plugins.mk,$p))))

# Copyright (c) 2013-2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

# Configuration.

DTL_FULL_PATH ?=
DTL_PATH ?= templates/
DTL_SUFFIX ?= _dtl
DTL_OPTS ?=

# Verbosity.

dtl_verbose_0 = @echo " DTL   " $(filter %.dtl,$(?F));
dtl_verbose = $(dtl_verbose_$(V))

# Core targets.

DTL_FILES = $(sort $(call core_find,$(DTL_PATH),*.dtl))

ifneq ($(DTL_FILES),)

ifdef DTL_FULL_PATH
BEAM_FILES += $(addprefix ebin/,$(patsubst %.dtl,%_dtl.beam,$(subst /,_,$(DTL_FILES:$(DTL_PATH)%=%))))
else
BEAM_FILES += $(addprefix ebin/,$(patsubst %.dtl,%_dtl.beam,$(notdir $(DTL_FILES))))
endif

# Rebuild templates when the Makefile changes.
$(DTL_FILES): $(MAKEFILE_LIST)
	@touch $@

define erlydtl_compile.erl
	[begin
		Module0 = case "$(strip $(DTL_FULL_PATH))" of
			"" ->
				filename:basename(F, ".dtl");
			_ ->
				"$(DTL_PATH)" ++ F2 = filename:rootname(F, ".dtl"),
				re:replace(F2, "/",  "_",  [{return, list}, global])
		end,
		Module = list_to_atom(string:to_lower(Module0) ++ "$(DTL_SUFFIX)"),
		case erlydtl:compile(F, Module, [$(DTL_OPTS)] ++ [{out_dir, "ebin/"}, return_errors, {doc_root, "templates"}]) of
			ok -> ok;
			{ok, _} -> ok
		end
	end || F <- string:tokens("$(1)", " ")],
	halt().
endef

ebin/$(PROJECT).app:: $(DTL_FILES) | ebin/
	$(if $(strip $?),\
		$(dtl_verbose) $(call erlang,$(call erlydtl_compile.erl,$?),-pa ebin/ $(DEPS_DIR)/erlydtl/ebin/))

endif

# Copyright (c) 2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

# Verbosity.

proto_verbose_0 = @echo " PROTO " $(filter %.proto,$(?F));
proto_verbose = $(proto_verbose_$(V))

# Core targets.

define compile_proto
	$(verbose) mkdir -p ebin/ include/
	$(proto_verbose) $(call erlang,$(call compile_proto.erl,$(1)))
	$(proto_verbose) erlc +debug_info -o ebin/ ebin/*.erl
	$(verbose) rm ebin/*.erl
endef

define compile_proto.erl
	[begin
		Dir = filename:dirname(filename:dirname(F)),
		protobuffs_compile:generate_source(F,
			[{output_include_dir, Dir ++ "/include"},
				{output_src_dir, Dir ++ "/ebin"}])
	end || F <- string:tokens("$(1)", " ")],
	halt().
endef

ifneq ($(wildcard src/),)
ebin/$(PROJECT).app:: $(sort $(call core_find,src/,*.proto))
	$(if $(strip $?),$(call compile_proto,$?))
endif

# Copyright (c) 2013-2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: clean-app

# Configuration.

ERLC_OPTS ?= -Werror +debug_info +warn_export_vars +warn_shadow_vars \
	+warn_obsolete_guard # +bin_opt_info +warn_export_all +warn_missing_spec
COMPILE_FIRST ?=
COMPILE_FIRST_PATHS = $(addprefix src/,$(addsuffix .erl,$(COMPILE_FIRST)))
ERLC_EXCLUDE ?=
ERLC_EXCLUDE_PATHS = $(addprefix src/,$(addsuffix .erl,$(ERLC_EXCLUDE)))

ERLC_MIB_OPTS ?=
COMPILE_MIB_FIRST ?=
COMPILE_MIB_FIRST_PATHS = $(addprefix mibs/,$(addsuffix .mib,$(COMPILE_MIB_FIRST)))

# Verbosity.

app_verbose_0 = @echo " APP   " $(PROJECT);
app_verbose_2 = set -x;
app_verbose = $(app_verbose_$(V))

appsrc_verbose_0 = @echo " APP   " $(PROJECT).app.src;
appsrc_verbose_2 = set -x;
appsrc_verbose = $(appsrc_verbose_$(V))

makedep_verbose_0 = @echo " DEPEND" $(PROJECT).d;
makedep_verbose_2 = set -x;
makedep_verbose = $(makedep_verbose_$(V))

erlc_verbose_0 = @echo " ERLC  " $(filter-out $(patsubst %,%.erl,$(ERLC_EXCLUDE)),\
	$(filter %.erl %.core,$(?F)));
erlc_verbose_2 = set -x;
erlc_verbose = $(erlc_verbose_$(V))

xyrl_verbose_0 = @echo " XYRL  " $(filter %.xrl %.yrl,$(?F));
xyrl_verbose_2 = set -x;
xyrl_verbose = $(xyrl_verbose_$(V))

asn1_verbose_0 = @echo " ASN1  " $(filter %.asn1,$(?F));
asn1_verbose_2 = set -x;
asn1_verbose = $(asn1_verbose_$(V))

mib_verbose_0 = @echo " MIB   " $(filter %.bin %.mib,$(?F));
mib_verbose_2 = set -x;
mib_verbose = $(mib_verbose_$(V))

ifneq ($(wildcard src/),)

# Targets.

ifeq ($(wildcard ebin/test),)
app:: deps $(PROJECT).d
	$(verbose) $(MAKE) --no-print-directory app-build
else
app:: clean deps $(PROJECT).d
	$(verbose) $(MAKE) --no-print-directory app-build
endif

ifeq ($(wildcard src/$(PROJECT_MOD).erl),)
define app_file
{application, $(PROJECT), [
	{description, "$(PROJECT_DESCRIPTION)"},
	{vsn, "$(PROJECT_VERSION)"},$(if $(IS_DEP),
	{id$(comma)$(space)"$(1)"}$(comma))
	{modules, [$(call comma_list,$(2))]},
	{registered, []},
	{applications, [$(call comma_list,kernel stdlib $(OTP_DEPS) $(LOCAL_DEPS) $(DEPS))]}
]}.
endef
else
define app_file
{application, $(PROJECT), [
	{description, "$(PROJECT_DESCRIPTION)"},
	{vsn, "$(PROJECT_VERSION)"},$(if $(IS_DEP),
	{id$(comma)$(space)"$(1)"}$(comma))
	{modules, [$(call comma_list,$(2))]},
	{registered, [$(call comma_list,$(PROJECT)_sup $(PROJECT_REGISTERED))]},
	{applications, [$(call comma_list,kernel stdlib $(OTP_DEPS) $(LOCAL_DEPS) $(DEPS))]},
	{mod, {$(PROJECT_MOD), []}}
]}.
endef
endif

app-build: ebin/$(PROJECT).app
	$(verbose) :

# Source files.

ERL_FILES = $(sort $(call core_find,src/,*.erl))
CORE_FILES = $(sort $(call core_find,src/,*.core))

# ASN.1 files.

ifneq ($(wildcard asn1/),)
ASN1_FILES = $(sort $(call core_find,asn1/,*.asn1))
ERL_FILES += $(addprefix src/,$(patsubst %.asn1,%.erl,$(notdir $(ASN1_FILES))))

define compile_asn1
	$(verbose) mkdir -p include/
	$(asn1_verbose) erlc -v -I include/ -o asn1/ +noobj $(1)
	$(verbose) mv asn1/*.erl src/
	$(verbose) mv asn1/*.hrl include/
	$(verbose) mv asn1/*.asn1db include/
endef

$(PROJECT).d:: $(ASN1_FILES)
	$(if $(strip $?),$(call compile_asn1,$?))
endif

# SNMP MIB files.

ifneq ($(wildcard mibs/),)
MIB_FILES = $(sort $(call core_find,mibs/,*.mib))

$(PROJECT).d:: $(COMPILE_MIB_FIRST_PATHS) $(MIB_FILES)
	$(verbose) mkdir -p include/ priv/mibs/
	$(mib_verbose) erlc -v $(ERLC_MIB_OPTS) -o priv/mibs/ -I priv/mibs/ $?
	$(mib_verbose) erlc -o include/ -- $(addprefix priv/mibs/,$(patsubst %.mib,%.bin,$(notdir $?)))
endif

# Leex and Yecc files.

XRL_FILES = $(sort $(call core_find,src/,*.xrl))
XRL_ERL_FILES = $(addprefix src/,$(patsubst %.xrl,%.erl,$(notdir $(XRL_FILES))))
ERL_FILES += $(XRL_ERL_FILES)

YRL_FILES = $(sort $(call core_find,src/,*.yrl))
YRL_ERL_FILES = $(addprefix src/,$(patsubst %.yrl,%.erl,$(notdir $(YRL_FILES))))
ERL_FILES += $(YRL_ERL_FILES)

$(PROJECT).d:: $(XRL_FILES) $(YRL_FILES)
	$(if $(strip $?),$(xyrl_verbose) erlc -v -o src/ $?)

# Erlang and Core Erlang files.

define makedep.erl
	E = ets:new(makedep, [bag]),
	G = digraph:new([acyclic]),
	ErlFiles = lists:usort(string:tokens("$(ERL_FILES)", " ")),
	Modules = [{list_to_atom(filename:basename(F, ".erl")), F} || F <- ErlFiles],
	Add = fun (Mod, Dep) ->
		case lists:keyfind(Dep, 1, Modules) of
			false -> ok;
			{_, DepFile} ->
				{_, ModFile} = lists:keyfind(Mod, 1, Modules),
				ets:insert(E, {ModFile, DepFile}),
				digraph:add_vertex(G, Mod),
				digraph:add_vertex(G, Dep),
				digraph:add_edge(G, Mod, Dep)
		end
	end,
	AddHd = fun (F, Mod, DepFile) ->
		case file:open(DepFile, [read]) of
			{error, enoent} -> ok;
			{ok, Fd} ->
				F(F, Fd, Mod),
				{_, ModFile} = lists:keyfind(Mod, 1, Modules),
				ets:insert(E, {ModFile, DepFile})
		end
	end,
	Attr = fun
		(F, Mod, behavior, Dep) -> Add(Mod, Dep);
		(F, Mod, behaviour, Dep) -> Add(Mod, Dep);
		(F, Mod, compile, {parse_transform, Dep}) -> Add(Mod, Dep);
		(F, Mod, compile, Opts) when is_list(Opts) ->
			case proplists:get_value(parse_transform, Opts) of
				undefined -> ok;
				Dep -> Add(Mod, Dep)
			end;
		(F, Mod, include, Hrl) ->
			case filelib:is_file("include/" ++ Hrl) of
				true -> AddHd(F, Mod, "include/" ++ Hrl);
				false ->
					case filelib:is_file("src/" ++ Hrl) of
						true -> AddHd(F, Mod, "src/" ++ Hrl);
						false -> false
					end
			end;
		(F, Mod, include_lib, "$1/include/" ++ Hrl) -> AddHd(F, Mod, "include/" ++ Hrl);
		(F, Mod, include_lib, Hrl) -> AddHd(F, Mod, "include/" ++ Hrl);
		(F, Mod, import, {Imp, _}) ->
			case filelib:is_file("src/" ++ atom_to_list(Imp) ++ ".erl") of
				false -> ok;
				true -> Add(Mod, Imp)
			end;
		(_, _, _, _) -> ok
	end,
	MakeDepend = fun(F, Fd, Mod) ->
		case io:parse_erl_form(Fd, undefined) of
			{ok, {attribute, _, Key, Value}, _} ->
				Attr(F, Mod, Key, Value),
				F(F, Fd, Mod);
			{eof, _} ->
				file:close(Fd);
			_ ->
				F(F, Fd, Mod)
		end
	end,
	[begin
		Mod = list_to_atom(filename:basename(F, ".erl")),
		{ok, Fd} = file:open(F, [read]),
		MakeDepend(MakeDepend, Fd, Mod)
	end || F <- ErlFiles],
	Depend = sofs:to_external(sofs:relation_to_family(sofs:relation(ets:tab2list(E)))),
	CompileFirst = [X || X <- lists:reverse(digraph_utils:topsort(G)), [] =/= digraph:in_neighbours(G, X)],
	ok = file:write_file("$(1)", [
		[[F, "::", [[" ", D] || D <- Deps], "; @touch \$$@\n"] || {F, Deps} <- Depend],
		"\nCOMPILE_FIRST +=", [[" ", atom_to_list(CF)] || CF <- CompileFirst], "\n"
	]),
	halt()
endef

ifeq ($(if $(NO_MAKEDEP),$(wildcard $(PROJECT).d),),)
$(PROJECT).d:: $(ERL_FILES) $(call core_find,include/,*.hrl)
	$(makedep_verbose) $(call erlang,$(call makedep.erl,$@))
endif

# Rebuild everything when the Makefile changes.
$(ERL_FILES) $(CORE_FILES) $(ASN1_FILES) $(MIB_FILES) $(XRL_FILES) $(YRL_FILES):: $(MAKEFILE_LIST)
	@touch $@

-include $(PROJECT).d

ebin/$(PROJECT).app:: ebin/

ebin/:
	$(verbose) mkdir -p ebin/

define compile_erl
	$(erlc_verbose) erlc -v $(if $(IS_DEP),$(filter-out -Werror,$(ERLC_OPTS)),$(ERLC_OPTS)) -o ebin/ \
		-pa ebin/ -I include/ $(filter-out $(ERLC_EXCLUDE_PATHS),$(COMPILE_FIRST_PATHS) $(1))
endef

ebin/$(PROJECT).app:: $(ERL_FILES) $(CORE_FILES) $(wildcard src/$(PROJECT).app.src)
	$(eval FILES_TO_COMPILE := $(filter-out src/$(PROJECT).app.src,$?))
	$(if $(strip $(FILES_TO_COMPILE)),$(call compile_erl,$(FILES_TO_COMPILE)))
	$(eval GITDESCRIBE := $(shell git describe --dirty --abbrev=7 --tags --always --first-parent 2>/dev/null || true))
	$(eval MODULES := $(patsubst %,'%',$(sort $(notdir $(basename \
		$(filter-out $(ERLC_EXCLUDE_PATHS),$(ERL_FILES) $(CORE_FILES) $(BEAM_FILES)))))))
ifeq ($(wildcard src/$(PROJECT).app.src),)
	$(app_verbose) printf "$(subst $(newline),\n,$(subst ",\",$(call app_file,$(GITDESCRIBE),$(MODULES))))" \
		> ebin/$(PROJECT).app
else
	$(verbose) if [ -z "$$(grep -e '^[^%]*{\s*modules\s*,' src/$(PROJECT).app.src)" ]; then \
		echo "Empty modules entry not found in $(PROJECT).app.src. Please consult the erlang.mk README for instructions." >&2; \
		exit 1; \
	fi
	$(appsrc_verbose) cat src/$(PROJECT).app.src \
		| sed "s/{[[:space:]]*modules[[:space:]]*,[[:space:]]*\[\]}/{modules, \[$(call comma_list,$(MODULES))\]}/" \
		| sed "s/{id,[[:space:]]*\"git\"}/{id, \"$(subst /,\/,$(GITDESCRIBE))\"}/" \
		> ebin/$(PROJECT).app
endif

clean:: clean-app

clean-app:
	$(gen_verbose) rm -rf $(PROJECT).d ebin/ priv/mibs/ $(XRL_ERL_FILES) $(YRL_ERL_FILES) \
		$(addprefix include/,$(patsubst %.mib,%.hrl,$(notdir $(MIB_FILES)))) \
		$(addprefix include/,$(patsubst %.asn1,%.hrl,$(notdir $(ASN1_FILES)))) \
		$(addprefix include/,$(patsubst %.asn1,%.asn1db,$(notdir $(ASN1_FILES)))) \
		$(addprefix src/,$(patsubst %.asn1,%.erl,$(notdir $(ASN1_FILES))))

endif

# Copyright (c) 2015, Viktor Söderqvist <viktor@zuiderkwast.se>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: docs-deps

# Configuration.

ALL_DOC_DEPS_DIRS = $(addprefix $(DEPS_DIR)/,$(DOC_DEPS))

# Targets.

$(foreach dep,$(DOC_DEPS),$(eval $(call dep_target,$(dep))))

ifneq ($(SKIP_DEPS),)
doc-deps:
else
doc-deps: $(ALL_DOC_DEPS_DIRS)
	$(verbose) for dep in $(ALL_DOC_DEPS_DIRS) ; do $(MAKE) -C $$dep; done
endif

# Copyright (c) 2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: rel-deps

# Configuration.

ALL_REL_DEPS_DIRS = $(addprefix $(DEPS_DIR)/,$(REL_DEPS))

# Targets.

$(foreach dep,$(REL_DEPS),$(eval $(call dep_target,$(dep))))

ifneq ($(SKIP_DEPS),)
rel-deps:
else
rel-deps: $(ALL_REL_DEPS_DIRS)
	$(verbose) for dep in $(ALL_REL_DEPS_DIRS) ; do $(MAKE) -C $$dep; done
endif

# Copyright (c) 2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: test-deps test-dir test-build clean-test-dir

# Configuration.

TEST_DIR ?= $(CURDIR)/test

ALL_TEST_DEPS_DIRS = $(addprefix $(DEPS_DIR)/,$(TEST_DEPS))

TEST_ERLC_OPTS ?= +debug_info +warn_export_vars +warn_shadow_vars +warn_obsolete_guard
TEST_ERLC_OPTS += -DTEST=1

# Targets.

$(foreach dep,$(TEST_DEPS),$(eval $(call dep_target,$(dep))))

ifneq ($(SKIP_DEPS),)
test-deps:
else
test-deps: $(ALL_TEST_DEPS_DIRS)
	$(verbose) for dep in $(ALL_TEST_DEPS_DIRS) ; do $(MAKE) -C $$dep IS_DEP=1; done
endif

ifneq ($(wildcard $(TEST_DIR)),)
test-dir:
	$(gen_verbose) erlc -v $(TEST_ERLC_OPTS) -I include/ -o $(TEST_DIR) \
		$(call core_find,$(TEST_DIR)/,*.erl) -pa ebin/
endif

ifeq ($(wildcard src),)
test-build:: ERLC_OPTS=$(TEST_ERLC_OPTS)
test-build:: clean deps test-deps
	$(verbose) $(MAKE) --no-print-directory test-dir ERLC_OPTS="$(TEST_ERLC_OPTS)"
else
ifeq ($(wildcard ebin/test),)
test-build:: ERLC_OPTS=$(TEST_ERLC_OPTS)
test-build:: clean deps test-deps $(PROJECT).d
	$(verbose) $(MAKE) --no-print-directory app-build test-dir ERLC_OPTS="$(TEST_ERLC_OPTS)"
	$(gen_verbose) touch ebin/test
else
test-build:: ERLC_OPTS=$(TEST_ERLC_OPTS)
test-build:: deps test-deps $(PROJECT).d
	$(verbose) $(MAKE) --no-print-directory app-build test-dir ERLC_OPTS="$(TEST_ERLC_OPTS)"
endif

clean:: clean-test-dir

clean-test-dir:
ifneq ($(wildcard $(TEST_DIR)/*.beam),)
	$(gen_verbose) rm -f $(TEST_DIR)/*.beam
endif
endif

# Copyright (c) 2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: rebar.config

# We strip out -Werror because we don't want to fail due to
# warnings when used as a dependency.

compat_prepare_erlc_opts = $(shell echo "$1" | sed 's/, */,/g')

define compat_convert_erlc_opts
$(if $(filter-out -Werror,$1),\
	$(if $(findstring +,$1),\
		$(shell echo $1 | cut -b 2-)))
endef

define compat_erlc_opts_to_list
[$(call comma_list,$(foreach o,$(call compat_prepare_erlc_opts,$1),$(call compat_convert_erlc_opts,$o)))]
endef

define compat_rebar_config
{deps, [
$(call comma_list,$(foreach d,$(DEPS),\
	$(if $(filter hex,$(call dep_fetch,$d)),\
		{$(call dep_name,$d)$(comma)"$(call dep_repo,$d)"},\
		{$(call dep_name,$d)$(comma)".*"$(comma){git,"$(call dep_repo,$d)"$(comma)"$(call dep_commit,$d)"}})))
]}.
{erl_opts, $(call compat_erlc_opts_to_list,$(ERLC_OPTS))}.
endef

$(eval _compat_rebar_config = $$(compat_rebar_config))
$(eval export _compat_rebar_config)

rebar.config:
	$(gen_verbose) echo "$${_compat_rebar_config}" > rebar.config

# Copyright (c) 2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: asciidoc asciidoc-guide asciidoc-manual install-asciidoc distclean-asciidoc

MAN_INSTALL_PATH ?= /usr/local/share/man
MAN_SECTIONS ?= 3 7

docs:: asciidoc

asciidoc: asciidoc-guide asciidoc-manual

ifeq ($(wildcard doc/src/guide/book.asciidoc),)
asciidoc-guide:
else
asciidoc-guide: distclean-asciidoc doc-deps
	a2x -v -f pdf doc/src/guide/book.asciidoc && mv doc/src/guide/book.pdf doc/guide.pdf
	a2x -v -f chunked doc/src/guide/book.asciidoc && mv doc/src/guide/book.chunked/ doc/html/
endif

ifeq ($(wildcard doc/src/manual/*.asciidoc),)
asciidoc-manual:
else
asciidoc-manual: distclean-asciidoc doc-deps
	for f in doc/src/manual/*.asciidoc ; do \
		a2x -v -f manpage $$f ; \
	done
	for s in $(MAN_SECTIONS); do \
		mkdir -p doc/man$$s/ ; \
		mv doc/src/manual/*.$$s doc/man$$s/ ; \
		gzip doc/man$$s/*.$$s ; \
	done

install-docs:: install-asciidoc

install-asciidoc: asciidoc-manual
	for s in $(MAN_SECTIONS); do \
		mkdir -p $(MAN_INSTALL_PATH)/man$$s/ ; \
		install -g `id -u` -o `id -g` -m 0644 doc/man$$s/*.gz $(MAN_INSTALL_PATH)/man$$s/ ; \
	done
endif

distclean:: distclean-asciidoc

distclean-asciidoc:
	$(gen_verbose) rm -rf doc/html/ doc/guide.pdf doc/man3/ doc/man7/

# Copyright (c) 2014-2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: bootstrap bootstrap-lib bootstrap-rel new list-templates

# Core targets.

help::
	$(verbose) printf "%s\n" "" \
		"Bootstrap targets:" \
		"  bootstrap          Generate a skeleton of an OTP application" \
		"  bootstrap-lib      Generate a skeleton of an OTP library" \
		"  bootstrap-rel      Generate the files needed to build a release" \
		"  new-app in=NAME    Create a new local OTP application NAME" \
		"  new-lib in=NAME    Create a new local OTP library NAME" \
		"  new t=TPL n=NAME   Generate a module NAME based on the template TPL" \
		"  new t=T n=N in=APP Generate a module NAME based on the template TPL in APP" \
		"  list-templates     List available templates"

# Bootstrap templates.

define bs_appsrc
{application, $p, [
	{description, ""},
	{vsn, "0.1.0"},
	{id, "git"},
	{modules, []},
	{registered, []},
	{applications, [
		kernel,
		stdlib
	]},
	{mod, {$p_app, []}},
	{env, []}
]}.
endef

define bs_appsrc_lib
{application, $p, [
	{description, ""},
	{vsn, "0.1.0"},
	{id, "git"},
	{modules, []},
	{registered, []},
	{applications, [
		kernel,
		stdlib
	]}
]}.
endef

# To prevent autocompletion issues with ZSH, we add "include erlang.mk"
# separately during the actual bootstrap.
ifdef SP
define bs_Makefile
PROJECT = $p
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.0.1

# Whitespace to be used when creating files from templates.
SP = $(SP)

endef
else
define bs_Makefile
PROJECT = $p
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.0.1

endef
endif

define bs_apps_Makefile
PROJECT = $p
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.0.1

include $(call core_relpath,$(dir $(ERLANG_MK_FILENAME)),$(APPS_DIR)/app)/erlang.mk
endef

define bs_app
-module($p_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
	$p_sup:start_link().

stop(_State) ->
	ok.
endef

define bs_relx_config
{release, {$p_release, "1"}, [$p]}.
{extended_start_script, true}.
{sys_config, "rel/sys.config"}.
{vm_args, "rel/vm.args"}.
endef

define bs_sys_config
[
].
endef

define bs_vm_args
-name $p@127.0.0.1
-setcookie $p
-heart
endef

# Normal templates.

define tpl_supervisor
-module($(n)).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
	Procs = [],
	{ok, {{one_for_one, 1, 5}, Procs}}.
endef

define tpl_gen_server
-module($(n)).
-behaviour(gen_server).

%% API.
-export([start_link/0]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-record(state, {
}).

%% API.

-spec start_link() -> {ok, pid()}.
start_link() ->
	gen_server:start_link(?MODULE, [], []).

%% gen_server.

init([]) ->
	{ok, #state{}}.

handle_call(_Request, _From, State) ->
	{reply, ignored, State}.

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
endef

define tpl_module
-module($(n)).
-export([]).
endef

define tpl_cowboy_http
-module($(n)).
-behaviour(cowboy_http_handler).

-export([init/3]).
-export([handle/2]).
-export([terminate/3]).

-record(state, {
}).

init(_, Req, _Opts) ->
	{ok, Req, #state{}}.

handle(Req, State=#state{}) ->
	{ok, Req2} = cowboy_req:reply(200, Req),
	{ok, Req2, State}.

terminate(_Reason, _Req, _State) ->
	ok.
endef

define tpl_gen_fsm
-module($(n)).
-behaviour(gen_fsm).

%% API.
-export([start_link/0]).

%% gen_fsm.
-export([init/1]).
-export([state_name/2]).
-export([handle_event/3]).
-export([state_name/3]).
-export([handle_sync_event/4]).
-export([handle_info/3]).
-export([terminate/3]).
-export([code_change/4]).

-record(state, {
}).

%% API.

-spec start_link() -> {ok, pid()}.
start_link() ->
	gen_fsm:start_link(?MODULE, [], []).

%% gen_fsm.

init([]) ->
	{ok, state_name, #state{}}.

state_name(_Event, StateData) ->
	{next_state, state_name, StateData}.

handle_event(_Event, StateName, StateData) ->
	{next_state, StateName, StateData}.

state_name(_Event, _From, StateData) ->
	{reply, ignored, state_name, StateData}.

handle_sync_event(_Event, _From, StateName, StateData) ->
	{reply, ignored, StateName, StateData}.

handle_info(_Info, StateName, StateData) ->
	{next_state, StateName, StateData}.

terminate(_Reason, _StateName, _StateData) ->
	ok.

code_change(_OldVsn, StateName, StateData, _Extra) ->
	{ok, StateName, StateData}.
endef

define tpl_cowboy_loop
-module($(n)).
-behaviour(cowboy_loop_handler).

-export([init/3]).
-export([info/3]).
-export([terminate/3]).

-record(state, {
}).

init(_, Req, _Opts) ->
	{loop, Req, #state{}, 5000, hibernate}.

info(_Info, Req, State) ->
	{loop, Req, State, hibernate}.

terminate(_Reason, _Req, _State) ->
	ok.
endef

define tpl_cowboy_rest
-module($(n)).

-export([init/3]).
-export([content_types_provided/2]).
-export([get_html/2]).

init(_, _Req, _Opts) ->
	{upgrade, protocol, cowboy_rest}.

content_types_provided(Req, State) ->
	{[{{<<"text">>, <<"html">>, '*'}, get_html}], Req, State}.

get_html(Req, State) ->
	{<<"<html><body>This is REST!</body></html>">>, Req, State}.
endef

define tpl_cowboy_ws
-module($(n)).
-behaviour(cowboy_websocket_handler).

-export([init/3]).
-export([websocket_init/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).
-export([websocket_terminate/3]).

-record(state, {
}).

init(_, _, _) ->
	{upgrade, protocol, cowboy_websocket}.

websocket_init(_, Req, _Opts) ->
	Req2 = cowboy_req:compact(Req),
	{ok, Req2, #state{}}.

websocket_handle({text, Data}, Req, State) ->
	{reply, {text, Data}, Req, State};
websocket_handle({binary, Data}, Req, State) ->
	{reply, {binary, Data}, Req, State};
websocket_handle(_Frame, Req, State) ->
	{ok, Req, State}.

websocket_info(_Info, Req, State) ->
	{ok, Req, State}.

websocket_terminate(_Reason, _Req, _State) ->
	ok.
endef

define tpl_ranch_protocol
-module($(n)).
-behaviour(ranch_protocol).

-export([start_link/4]).
-export([init/4]).

-type opts() :: [].
-export_type([opts/0]).

-record(state, {
	socket :: inet:socket(),
	transport :: module()
}).

start_link(Ref, Socket, Transport, Opts) ->
	Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
	{ok, Pid}.

-spec init(ranch:ref(), inet:socket(), module(), opts()) -> ok.
init(Ref, Socket, Transport, _Opts) ->
	ok = ranch:accept_ack(Ref),
	loop(#state{socket=Socket, transport=Transport}).

loop(State) ->
	loop(State).
endef

# Plugin-specific targets.

define render_template
	$(verbose) printf -- '$(subst $(newline),\n,$(subst %,%%,$(subst ','\'',$(subst $(tab),$(WS),$(call $(1))))))\n' > $(2)
endef

ifndef WS
ifdef SP
WS = $(subst a,,a $(wordlist 1,$(SP),a a a a a a a a a a a a a a a a a a a a))
else
WS = $(tab)
endif
endif

bootstrap:
ifneq ($(wildcard src/),)
	$(error Error: src/ directory already exists)
endif
	$(eval p := $(PROJECT))
	$(eval n := $(PROJECT)_sup)
	$(call render_template,bs_Makefile,Makefile)
	$(verbose) echo "include erlang.mk" >> Makefile
	$(verbose) mkdir src/
ifdef LEGACY
	$(call render_template,bs_appsrc,src/$(PROJECT).app.src)
endif
	$(call render_template,bs_app,src/$(PROJECT)_app.erl)
	$(call render_template,tpl_supervisor,src/$(PROJECT)_sup.erl)

bootstrap-lib:
ifneq ($(wildcard src/),)
	$(error Error: src/ directory already exists)
endif
	$(eval p := $(PROJECT))
	$(call render_template,bs_Makefile,Makefile)
	$(verbose) echo "include erlang.mk" >> Makefile
	$(verbose) mkdir src/
ifdef LEGACY
	$(call render_template,bs_appsrc_lib,src/$(PROJECT).app.src)
endif

bootstrap-rel:
ifneq ($(wildcard relx.config),)
	$(error Error: relx.config already exists)
endif
ifneq ($(wildcard rel/),)
	$(error Error: rel/ directory already exists)
endif
	$(eval p := $(PROJECT))
	$(call render_template,bs_relx_config,relx.config)
	$(verbose) mkdir rel/
	$(call render_template,bs_sys_config,rel/sys.config)
	$(call render_template,bs_vm_args,rel/vm.args)

new-app:
ifndef in
	$(error Usage: $(MAKE) new-app in=APP)
endif
ifneq ($(wildcard $(APPS_DIR)/$in),)
	$(error Error: Application $in already exists)
endif
	$(eval p := $(in))
	$(eval n := $(in)_sup)
	$(verbose) mkdir -p $(APPS_DIR)/$p/src/
	$(call render_template,bs_apps_Makefile,$(APPS_DIR)/$p/Makefile)
ifdef LEGACY
	$(call render_template,bs_appsrc,$(APPS_DIR)/$p/src/$p.app.src)
endif
	$(call render_template,bs_app,$(APPS_DIR)/$p/src/$p_app.erl)
	$(call render_template,tpl_supervisor,$(APPS_DIR)/$p/src/$p_sup.erl)

new-lib:
ifndef in
	$(error Usage: $(MAKE) new-lib in=APP)
endif
ifneq ($(wildcard $(APPS_DIR)/$in),)
	$(error Error: Application $in already exists)
endif
	$(eval p := $(in))
	$(verbose) mkdir -p $(APPS_DIR)/$p/src/
	$(call render_template,bs_apps_Makefile,$(APPS_DIR)/$p/Makefile)
ifdef LEGACY
	$(call render_template,bs_appsrc_lib,$(APPS_DIR)/$p/src/$p.app.src)
endif

new:
ifeq ($(wildcard src/)$(in),)
	$(error Error: src/ directory does not exist)
endif
ifndef t
	$(error Usage: $(MAKE) new t=TEMPLATE n=NAME [in=APP])
endif
ifndef tpl_$(t)
	$(error Unknown template)
endif
ifndef n
	$(error Usage: $(MAKE) new t=TEMPLATE n=NAME [in=APP])
endif
ifdef in
	$(verbose) $(MAKE) -C $(APPS_DIR)/$(in)/ new t=$t n=$n in=
else
	$(call render_template,tpl_$(t),src/$(n).erl)
endif

list-templates:
	$(verbose) echo Available templates: $(sort $(patsubst tpl_%,%,$(filter tpl_%,$(.VARIABLES))))

# Copyright (c) 2014-2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: clean-c_src distclean-c_src-env

# Configuration.

C_SRC_DIR ?= $(CURDIR)/c_src
C_SRC_ENV ?= $(C_SRC_DIR)/env.mk
C_SRC_OUTPUT ?= $(CURDIR)/priv/$(PROJECT)
C_SRC_TYPE ?= shared

# System type and C compiler/flags.

ifeq ($(PLATFORM),msys2)
	C_SRC_OUTPUT_EXECUTABLE_EXTENSION ?= .exe
	C_SRC_OUTPUT_SHARED_EXTENSION ?= .dll
else
	C_SRC_OUTPUT_EXECUTABLE_EXTENSION ?=
	C_SRC_OUTPUT_SHARED_EXTENSION ?= .so
endif

ifeq ($(C_SRC_TYPE),shared)
	C_SRC_OUTPUT_FILE = $(C_SRC_OUTPUT)$(C_SRC_OUTPUT_SHARED_EXTENSION)
else
	C_SRC_OUTPUT_FILE = $(C_SRC_OUTPUT)$(C_SRC_OUTPUT_EXECUTABLE_EXTENSION)
endif

ifeq ($(PLATFORM),msys2)
# We hardcode the compiler used on MSYS2. The default CC=cc does
# not produce working code. The "gcc" MSYS2 package also doesn't.
	CC = /mingw64/bin/gcc
	export CC
	CFLAGS ?= -O3 -std=c99 -finline-functions -Wall -Wmissing-prototypes
	CXXFLAGS ?= -O3 -finline-functions -Wall
else ifeq ($(PLATFORM),darwin)
	CC ?= cc
	CFLAGS ?= -O3 -std=c99 -arch x86_64 -finline-functions -Wall -Wmissing-prototypes
	CXXFLAGS ?= -O3 -arch x86_64 -finline-functions -Wall
	LDFLAGS ?= -arch x86_64 -flat_namespace -undefined suppress
else ifeq ($(PLATFORM),freebsd)
	CC ?= cc
	CFLAGS ?= -O3 -std=c99 -finline-functions -Wall -Wmissing-prototypes
	CXXFLAGS ?= -O3 -finline-functions -Wall
else ifeq ($(PLATFORM),linux)
	CC ?= gcc
	CFLAGS ?= -O3 -std=c99 -finline-functions -Wall -Wmissing-prototypes
	CXXFLAGS ?= -O3 -finline-functions -Wall
endif

ifneq ($(PLATFORM),msys2)
	CFLAGS += -fPIC
	CXXFLAGS += -fPIC
endif

CFLAGS += -I"$(ERTS_INCLUDE_DIR)" -I"$(ERL_INTERFACE_INCLUDE_DIR)"
CXXFLAGS += -I"$(ERTS_INCLUDE_DIR)" -I"$(ERL_INTERFACE_INCLUDE_DIR)"

LDLIBS += -L"$(ERL_INTERFACE_LIB_DIR)" -lerl_interface -lei

# Verbosity.

c_verbose_0 = @echo " C     " $(?F);
c_verbose = $(c_verbose_$(V))

cpp_verbose_0 = @echo " CPP   " $(?F);
cpp_verbose = $(cpp_verbose_$(V))

link_verbose_0 = @echo " LD    " $(@F);
link_verbose = $(link_verbose_$(V))

# Targets.

ifeq ($(wildcard $(C_SRC_DIR)),)
else ifneq ($(wildcard $(C_SRC_DIR)/Makefile),)
app:: app-c_src

test-build:: app-c_src

app-c_src:
	$(MAKE) -C $(C_SRC_DIR)

clean::
	$(MAKE) -C $(C_SRC_DIR) clean

else

ifeq ($(SOURCES),)
SOURCES := $(sort $(foreach pat,*.c *.C *.cc *.cpp,$(call core_find,$(C_SRC_DIR)/,$(pat))))
endif
OBJECTS = $(addsuffix .o, $(basename $(SOURCES)))

COMPILE_C = $(c_verbose) $(CC) $(CFLAGS) $(CPPFLAGS) -c
COMPILE_CPP = $(cpp_verbose) $(CXX) $(CXXFLAGS) $(CPPFLAGS) -c

app:: $(C_SRC_ENV) $(C_SRC_OUTPUT_FILE)

test-build:: $(C_SRC_ENV) $(C_SRC_OUTPUT_FILE)

$(C_SRC_OUTPUT_FILE): $(OBJECTS)
	$(verbose) mkdir -p priv/
	$(link_verbose) $(CC) $(OBJECTS) \
		$(LDFLAGS) $(if $(filter $(C_SRC_TYPE),shared),-shared) $(LDLIBS) \
		-o $(C_SRC_OUTPUT_FILE)

%.o: %.c
	$(COMPILE_C) $(OUTPUT_OPTION) $<

%.o: %.cc
	$(COMPILE_CPP) $(OUTPUT_OPTION) $<

%.o: %.C
	$(COMPILE_CPP) $(OUTPUT_OPTION) $<

%.o: %.cpp
	$(COMPILE_CPP) $(OUTPUT_OPTION) $<

clean:: clean-c_src

clean-c_src:
	$(gen_verbose) rm -f $(C_SRC_OUTPUT_FILE) $(OBJECTS)

endif

ifneq ($(wildcard $(C_SRC_DIR)),)
$(C_SRC_ENV):
	$(verbose) $(ERL) -eval "file:write_file(\"$(call core_native_path,$(C_SRC_ENV))\", \
		io_lib:format( \
			\"ERTS_INCLUDE_DIR ?= ~s/erts-~s/include/~n\" \
			\"ERL_INTERFACE_INCLUDE_DIR ?= ~s~n\" \
			\"ERL_INTERFACE_LIB_DIR ?= ~s~n\", \
			[code:root_dir(), erlang:system_info(version), \
			code:lib_dir(erl_interface, include), \
			code:lib_dir(erl_interface, lib)])), \
		halt()."

distclean:: distclean-c_src-env

distclean-c_src-env:
	$(gen_verbose) rm -f $(C_SRC_ENV)

-include $(C_SRC_ENV)
endif

# Templates.

define bs_c_nif
#include "erl_nif.h"

static int loads = 0;

static int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
	/* Initialize private data. */
	*priv_data = NULL;

	loads++;

	return 0;
}

static int upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info)
{
	/* Convert the private data to the new version. */
	*priv_data = *old_priv_data;

	loads++;

	return 0;
}

static void unload(ErlNifEnv* env, void* priv_data)
{
	if (loads == 1) {
		/* Destroy the private data. */
	}

	loads--;
}

static ERL_NIF_TERM hello(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
	if (enif_is_atom(env, argv[0])) {
		return enif_make_tuple2(env,
			enif_make_atom(env, "hello"),
			argv[0]);
	}

	return enif_make_tuple2(env,
		enif_make_atom(env, "error"),
		enif_make_atom(env, "badarg"));
}

static ErlNifFunc nif_funcs[] = {
	{"hello", 1, hello}
};

ERL_NIF_INIT($n, nif_funcs, load, NULL, upgrade, unload)
endef

define bs_erl_nif
-module($n).

-export([hello/1]).

-on_load(on_load/0).
on_load() ->
	PrivDir = case code:priv_dir(?MODULE) of
		{error, _} ->
			AppPath = filename:dirname(filename:dirname(code:which(?MODULE))),
			filename:join(AppPath, "priv");
		Path ->
			Path
	end,
	erlang:load_nif(filename:join(PrivDir, atom_to_list(?MODULE)), 0).

hello(_) ->
	erlang:nif_error({not_loaded, ?MODULE}).
endef

new-nif:
ifneq ($(wildcard $(C_SRC_DIR)/$n.c),)
	$(error Error: $(C_SRC_DIR)/$n.c already exists)
endif
ifneq ($(wildcard src/$n.erl),)
	$(error Error: src/$n.erl already exists)
endif
ifdef in
	$(verbose) $(MAKE) -C $(APPS_DIR)/$(in)/ new-nif n=$n in=
else
	$(verbose) mkdir -p $(C_SRC_DIR) src/
	$(call render_template,bs_c_nif,$(C_SRC_DIR)/$n.c)
	$(call render_template,bs_erl_nif,src/$n.erl)
endif

# Copyright (c) 2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: ci ci-setup distclean-kerl

KERL ?= $(CURDIR)/kerl
export KERL

KERL_URL ?= https://raw.githubusercontent.com/yrashk/kerl/master/kerl

OTP_GIT ?= https://github.com/erlang/otp

CI_INSTALL_DIR ?= $(HOME)/erlang
CI_OTP ?=

ifeq ($(strip $(CI_OTP)),)
ci::
else
ci:: $(addprefix ci-,$(CI_OTP))

ci-prepare: $(addprefix $(CI_INSTALL_DIR)/,$(CI_OTP))

ci-setup::

ci_verbose_0 = @echo " CI    " $(1);
ci_verbose = $(ci_verbose_$(V))

define ci_target
ci-$(1): $(CI_INSTALL_DIR)/$(1)
	$(ci_verbose) \
		PATH="$(CI_INSTALL_DIR)/$(1)/bin:$(PATH)" \
		CI_OTP_RELEASE="$(1)" \
		CT_OPTS="-label $(1)" \
		$(MAKE) clean ci-setup tests
endef

$(foreach otp,$(CI_OTP),$(eval $(call ci_target,$(otp))))

define ci_otp_target
ifeq ($(wildcard $(CI_INSTALL_DIR)/$(1)),)
$(CI_INSTALL_DIR)/$(1): $(KERL)
	$(KERL) build git $(OTP_GIT) $(1) $(1)
	$(KERL) install $(1) $(CI_INSTALL_DIR)/$(1)
endif
endef

$(foreach otp,$(CI_OTP),$(eval $(call ci_otp_target,$(otp))))

$(KERL):
	$(gen_verbose) $(call core_http_get,$(KERL),$(KERL_URL))
	$(verbose) chmod +x $(KERL)

help::
	$(verbose) printf "%s\n" "" \
		"Continuous Integration targets:" \
		"  ci          Run '$(MAKE) tests' on all configured Erlang versions." \
		"" \
		"The CI_OTP variable must be defined with the Erlang versions" \
		"that must be tested. For example: CI_OTP = OTP-17.3.4 OTP-17.5.3"

distclean:: distclean-kerl

distclean-kerl:
	$(gen_verbose) rm -rf $(KERL)
endif

# Copyright (c) 2013-2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: ct apps-ct distclean-ct

# Configuration.

CT_OPTS ?=
ifneq ($(wildcard $(TEST_DIR)),)
	CT_SUITES ?= $(sort $(subst _SUITE.erl,,$(notdir $(call core_find,$(TEST_DIR)/,*_SUITE.erl))))
else
	CT_SUITES ?=
endif

# Core targets.

tests:: ct

distclean:: distclean-ct

help::
	$(verbose) printf "%s\n" "" \
		"Common_test targets:" \
		"  ct          Run all the common_test suites for this project" \
		"" \
		"All your common_test suites have their associated targets." \
		"A suite named http_SUITE can be ran using the ct-http target."

# Plugin-specific targets.

CT_RUN = ct_run \
	-no_auto_compile \
	-noinput \
	-pa $(CURDIR)/ebin $(DEPS_DIR)/*/ebin $(APPS_DIR)/*/ebin $(TEST_DIR) \
	-dir $(TEST_DIR) \
	-logdir $(CURDIR)/logs

ifeq ($(CT_SUITES),)
ct: $(if $(IS_APP),,apps-ct)
else
ct: test-build $(if $(IS_APP),,apps-ct)
	$(verbose) mkdir -p $(CURDIR)/logs/
	$(gen_verbose) $(CT_RUN) -sname ct_$(PROJECT) -suite $(addsuffix _SUITE,$(CT_SUITES)) $(CT_OPTS)
endif

ifneq ($(ALL_APPS_DIRS),)
define ct_app_target
apps-ct-$1:
	$(MAKE) -C $1 ct IS_APP=1
endef

$(foreach app,$(ALL_APPS_DIRS),$(eval $(call ct_app_target,$(app))))

apps-ct: test-build $(addprefix apps-ct-,$(ALL_APPS_DIRS))
endif

ifndef t
CT_EXTRA =
else
ifeq (,$(findstring :,$t))
CT_EXTRA = -group $t
else
t_words = $(subst :, ,$t)
CT_EXTRA = -group $(firstword $(t_words)) -case $(lastword $(t_words))
endif
endif

define ct_suite_target
ct-$(1): test-build
	$(verbose) mkdir -p $(CURDIR)/logs/
	$(gen_verbose) $(CT_RUN) -sname ct_$(PROJECT) -suite $(addsuffix _SUITE,$(1)) $(CT_EXTRA) $(CT_OPTS)
endef

$(foreach test,$(CT_SUITES),$(eval $(call ct_suite_target,$(test))))

distclean-ct:
	$(gen_verbose) rm -rf $(CURDIR)/logs/

# Copyright (c) 2013-2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: plt distclean-plt dialyze

# Configuration.

DIALYZER_PLT ?= $(CURDIR)/.$(PROJECT).plt
export DIALYZER_PLT

PLT_APPS ?=
DIALYZER_DIRS ?= --src -r $(wildcard src) $(ALL_APPS_DIRS)
DIALYZER_OPTS ?= -Werror_handling -Wrace_conditions -Wunmatched_returns # -Wunderspecs

# Core targets.

check:: dialyze

distclean:: distclean-plt

help::
	$(verbose) printf "%s\n" "" \
		"Dialyzer targets:" \
		"  plt         Build a PLT file for this project" \
		"  dialyze     Analyze the project using Dialyzer"

# Plugin-specific targets.

define filter_opts.erl
	Opts = binary:split(<<"$1">>, <<"-">>, [global]),
	Filtered = lists:reverse(lists:foldl(fun
		(O = <<"pa ", _/bits>>, Acc) -> [O|Acc];
		(O = <<"D ", _/bits>>, Acc) -> [O|Acc];
		(O = <<"I ", _/bits>>, Acc) -> [O|Acc];
		(_, Acc) -> Acc
	end, [], Opts)),
	io:format("~s~n", [[["-", O] || O <- Filtered]]),
	halt().
endef

$(DIALYZER_PLT): deps app
	$(verbose) dialyzer --build_plt --apps erts kernel stdlib $(PLT_APPS) $(OTP_DEPS) $(LOCAL_DEPS) $(DEPS)

plt: $(DIALYZER_PLT)

distclean-plt:
	$(gen_verbose) rm -f $(DIALYZER_PLT)

ifneq ($(wildcard $(DIALYZER_PLT)),)
dialyze:
else
dialyze: $(DIALYZER_PLT)
endif
	$(verbose) dialyzer --no_native `$(call erlang,$(call filter_opts.erl,$(ERLC_OPTS)))` $(DIALYZER_DIRS) $(DIALYZER_OPTS)

# Copyright (c) 2013-2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: distclean-edoc edoc

# Configuration.

EDOC_OPTS ?=

# Core targets.

ifneq ($(wildcard doc/overview.edoc),)
docs:: edoc
endif

distclean:: distclean-edoc

# Plugin-specific targets.

edoc: distclean-edoc doc-deps
	$(gen_verbose) $(ERL) -eval 'edoc:application($(PROJECT), ".", [$(EDOC_OPTS)]), halt().'

distclean-edoc:
	$(gen_verbose) rm -f doc/*.css doc/*.html doc/*.png doc/edoc-info

# Copyright (c) 2014 Dave Cottlehuber <dch@skunkwerks.at>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: distclean-escript escript

# Configuration.

ESCRIPT_NAME ?= $(PROJECT)
ESCRIPT_FILE ?= $(ESCRIPT_NAME)

ESCRIPT_COMMENT ?= This is an -*- erlang -*- file

ESCRIPT_BEAMS ?= "ebin/*", "deps/*/ebin/*"
ESCRIPT_SYS_CONFIG ?= "rel/sys.config"
ESCRIPT_EMU_ARGS ?= -pa . \
	-sasl errlog_type error \
	-escript main $(ESCRIPT_NAME)
ESCRIPT_SHEBANG ?= /usr/bin/env escript
ESCRIPT_STATIC ?= "deps/*/priv/**", "priv/**"

# Core targets.

distclean:: distclean-escript

help::
	$(verbose) printf "%s\n" "" \
		"Escript targets:" \
		"  escript     Build an executable escript archive" \

# Plugin-specific targets.

# Based on https://github.com/synrc/mad/blob/master/src/mad_bundle.erl
# Copyright (c) 2013 Maxim Sokhatsky, Synrc Research Center
# Modified MIT License, https://github.com/synrc/mad/blob/master/LICENSE :
# Software may only be used for the great good and the true happiness of all
# sentient beings.

define ESCRIPT_RAW
'Read = fun(F) -> {ok, B} = file:read_file(filename:absname(F)), B end,'\
'Files = fun(L) -> A = lists:concat([filelib:wildcard(X)||X<- L ]),'\
'  [F || F <- A, not filelib:is_dir(F) ] end,'\
'Squash = fun(L) -> [{filename:basename(F), Read(F) } || F <- L ] end,'\
'Zip = fun(A, L) -> {ok,{_,Z}} = zip:create(A, L, [{compress,all},memory]), Z end,'\
'Ez = fun(Escript) ->'\
'  Static = Files([$(ESCRIPT_STATIC)]),'\
'  Beams = Squash(Files([$(ESCRIPT_BEAMS), $(ESCRIPT_SYS_CONFIG)])),'\
'  Archive = Beams ++ [{ "static.gz", Zip("static.gz", Static)}],'\
'  escript:create(Escript, [ $(ESCRIPT_OPTIONS)'\
'    {archive, Archive, [memory]},'\
'    {shebang, "$(ESCRIPT_SHEBANG)"},'\
'    {comment, "$(ESCRIPT_COMMENT)"},'\
'    {emu_args, " $(ESCRIPT_EMU_ARGS)"}'\
'  ]),'\
'  file:change_mode(Escript, 8#755)'\
'end,'\
'Ez("$(ESCRIPT_FILE)"),'\
'halt().'
endef

ESCRIPT_COMMAND = $(subst ' ',,$(ESCRIPT_RAW))

escript:: distclean-escript deps app
	$(gen_verbose) $(ERL) -eval $(ESCRIPT_COMMAND)

distclean-escript:
	$(gen_verbose) rm -f $(ESCRIPT_NAME)

# Copyright (c) 2014, Enrique Fernandez <enrique.fernandez@erlang-solutions.com>
# Copyright (c) 2015, Loïc Hoguin <essen@ninenines.eu>
# This file is contributed to erlang.mk and subject to the terms of the ISC License.

.PHONY: eunit apps-eunit

# Configuration

EUNIT_OPTS ?=
EUNIT_ERL_OPTS ?=

# Core targets.

tests:: eunit

help::
	$(verbose) printf "%s\n" "" \
		"EUnit targets:" \
		"  eunit       Run all the EUnit tests for this project"

# Plugin-specific targets.

define eunit.erl
	case "$(COVER)" of
		"" -> ok;
		_ ->
			case cover:compile_beam_directory("ebin") of
				{error, _} -> halt(1);
				_ -> ok
			end
	end,
	case eunit:test($1, [$(EUNIT_OPTS)]) of
		ok -> ok;
		error -> halt(2)
	end,
	case "$(COVER)" of
		"" -> ok;
		_ ->
			cover:export("eunit.coverdata")
	end,
	halt()
endef

EUNIT_ERL_OPTS += -pa $(TEST_DIR) $(DEPS_DIR)/*/ebin $(APPS_DIR)/*/ebin $(CURDIR)/ebin

ifdef t
ifeq (,$(findstring :,$(t)))
eunit: test-build
	$(gen_verbose) $(call erlang,$(call eunit.erl,['$(t)']),$(EUNIT_ERL_OPTS))
else
eunit: test-build
	$(gen_verbose) $(call erlang,$(call eunit.erl,fun $(t)/0),$(EUNIT_ERL_OPTS))
endif
else
EUNIT_EBIN_MODS = $(notdir $(basename $(ERL_FILES) $(BEAM_FILES)))
EUNIT_TEST_MODS = $(notdir $(basename $(call core_find,$(TEST_DIR)/,*.erl)))

EUNIT_MODS = $(foreach mod,$(EUNIT_EBIN_MODS) $(filter-out \
	$(patsubst %,%_tests,$(EUNIT_EBIN_MODS)),$(EUNIT_TEST_MODS)),'$(mod)')

eunit: test-build $(if $(IS_APP),,apps-eunit)
	$(gen_verbose) $(call erlang,$(call eunit.erl,[$(call comma_list,$(EUNIT_MODS))]),$(EUNIT_ERL_OPTS))

ifneq ($(ALL_APPS_DIRS),)
apps-eunit:
	$(verbose) for app in $(ALL_APPS_DIRS); do $(MAKE) -C $$app eunit IS_APP=1; done
endif
endif

# Copyright (c) 2013-2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: relx-rel distclean-relx-rel distclean-relx run

# Configuration.

RELX ?= $(CURDIR)/relx
RELX_CONFIG ?= $(CURDIR)/relx.config

RELX_URL ?= https://github.com/erlware/relx/releases/download/v3.19.0/relx
RELX_OPTS ?=
RELX_OUTPUT_DIR ?= _rel

ifeq ($(firstword $(RELX_OPTS)),-o)
	RELX_OUTPUT_DIR = $(word 2,$(RELX_OPTS))
else
	RELX_OPTS += -o $(RELX_OUTPUT_DIR)
endif

# Core targets.

ifeq ($(IS_DEP),)
ifneq ($(wildcard $(RELX_CONFIG)),)
rel:: relx-rel
endif
endif

distclean:: distclean-relx-rel distclean-relx

# Plugin-specific targets.

$(RELX):
	$(gen_verbose) $(call core_http_get,$(RELX),$(RELX_URL))
	$(verbose) chmod +x $(RELX)

relx-rel: $(RELX) rel-deps app
	$(verbose) $(RELX) -c $(RELX_CONFIG) $(RELX_OPTS)

distclean-relx-rel:
	$(gen_verbose) rm -rf $(RELX_OUTPUT_DIR)

distclean-relx:
	$(gen_verbose) rm -rf $(RELX)

# Run target.

ifeq ($(wildcard $(RELX_CONFIG)),)
run:
else

define get_relx_release.erl
	{ok, Config} = file:consult("$(RELX_CONFIG)"),
	{release, {Name, _}, _} = lists:keyfind(release, 1, Config),
	io:format("~s", [Name]),
	halt(0).
endef

RELX_RELEASE = `$(call erlang,$(get_relx_release.erl))`

run: all
	$(verbose) $(RELX_OUTPUT_DIR)/$(RELX_RELEASE)/bin/$(RELX_RELEASE) console

help::
	$(verbose) printf "%s\n" "" \
		"Relx targets:" \
		"  run         Compile the project, build the release and run it"

endif

# Copyright (c) 2014, M Robert Martin <rob@version2beta.com>
# Copyright (c) 2015, Loïc Hoguin <essen@ninenines.eu>
# This file is contributed to erlang.mk and subject to the terms of the ISC License.

.PHONY: shell

# Configuration.

SHELL_ERL ?= erl
SHELL_PATHS ?= $(CURDIR)/ebin $(APPS_DIR)/*/ebin $(DEPS_DIR)/*/ebin
SHELL_OPTS ?=

ALL_SHELL_DEPS_DIRS = $(addprefix $(DEPS_DIR)/,$(SHELL_DEPS))

# Core targets

help::
	$(verbose) printf "%s\n" "" \
		"Shell targets:" \
		"  shell       Run an erlang shell with SHELL_OPTS or reasonable default"

# Plugin-specific targets.

$(foreach dep,$(SHELL_DEPS),$(eval $(call dep_target,$(dep))))

build-shell-deps: $(ALL_SHELL_DEPS_DIRS)
	$(verbose) for dep in $(ALL_SHELL_DEPS_DIRS) ; do $(MAKE) -C $$dep ; done

shell: build-shell-deps
	$(gen_verbose) $(SHELL_ERL) -pa $(SHELL_PATHS) $(SHELL_OPTS)

# Copyright (c) 2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(filter triq,$(DEPS) $(TEST_DEPS)),triq)
.PHONY: triq

# Targets.

tests:: triq

define triq_check.erl
	code:add_pathsa(["$(CURDIR)/ebin", "$(DEPS_DIR)/*/ebin"]),
	try
		case $(1) of
			all -> [true] =:= lists:usort([triq:check(M) || M <- [$(call comma_list,$(3))]]);
			module -> triq:check($(2));
			function -> triq:check($(2))
		end
	of
		true -> halt(0);
		_ -> halt(1)
	catch error:undef ->
		io:format("Undefined property or module~n"),
		halt(0)
	end.
endef

ifdef t
ifeq (,$(findstring :,$(t)))
triq: test-build
	$(verbose) $(call erlang,$(call triq_check.erl,module,$(t)))
else
triq: test-build
	$(verbose) echo Testing $(t)/0
	$(verbose) $(call erlang,$(call triq_check.erl,function,$(t)()))
endif
else
triq: test-build
	$(eval MODULES := $(patsubst %,'%',$(sort $(notdir $(basename $(wildcard ebin/*.beam))))))
	$(gen_verbose) $(call erlang,$(call triq_check.erl,all,undefined,$(MODULES)))
endif
endif

# Copyright (c) 2015, Erlang Solutions Ltd.
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: xref distclean-xref

# Configuration.

ifeq ($(XREF_CONFIG),)
	XREF_ARGS :=
else
	XREF_ARGS := -c $(XREF_CONFIG)
endif

XREFR ?= $(CURDIR)/xrefr
export XREFR

XREFR_URL ?= https://github.com/inaka/xref_runner/releases/download/0.2.2/xrefr

# Core targets.

help::
	$(verbose) printf "%s\n" "" \
		"Xref targets:" \
		"  xref        Run Xrefr using $XREF_CONFIG as config file if defined"

distclean:: distclean-xref

# Plugin-specific targets.

$(XREFR):
	$(gen_verbose) $(call core_http_get,$(XREFR),$(XREFR_URL))
	$(verbose) chmod +x $(XREFR)

xref: deps app $(XREFR)
	$(gen_verbose) $(XREFR) $(XREFR_ARGS)

distclean-xref:
	$(gen_verbose) rm -rf $(XREFR)

# Copyright 2015, Viktor Söderqvist <viktor@zuiderkwast.se>
# This file is part of erlang.mk and subject to the terms of the ISC License.

COVER_REPORT_DIR = cover

# Hook in coverage to ct

ifdef COVER
ifdef CT_RUN
# All modules in 'ebin'
COVER_MODS = $(notdir $(basename $(call core_ls,ebin/*.beam)))

test-build:: $(TEST_DIR)/ct.cover.spec

$(TEST_DIR)/ct.cover.spec:
	$(verbose) echo Cover mods: $(COVER_MODS)
	$(gen_verbose) printf "%s\n" \
		'{incl_mods,[$(subst $(space),$(comma),$(COVER_MODS))]}.' \
		'{export,"$(CURDIR)/ct.coverdata"}.' > $@

CT_RUN += -cover $(TEST_DIR)/ct.cover.spec
endif
endif

# Core targets

ifdef COVER
ifneq ($(COVER_REPORT_DIR),)
tests::
	$(verbose) $(MAKE) --no-print-directory cover-report
endif
endif

clean:: coverdata-clean

ifneq ($(COVER_REPORT_DIR),)
distclean:: cover-report-clean
endif

help::
	$(verbose) printf "%s\n" "" \
		"Cover targets:" \
		"  cover-report  Generate a HTML coverage report from previously collected" \
		"                cover data." \
		"  all.coverdata Merge {eunit,ct}.coverdata into one coverdata file." \
		"" \
		"If COVER=1 is set, coverage data is generated by the targets eunit and ct. The" \
		"target tests additionally generates a HTML coverage report from the combined" \
		"coverdata files from each of these testing tools. HTML reports can be disabled" \
		"by setting COVER_REPORT_DIR to empty."

# Plugin specific targets

COVERDATA = $(filter-out all.coverdata,$(wildcard *.coverdata))

.PHONY: coverdata-clean
coverdata-clean:
	$(gen_verbose) rm -f *.coverdata ct.cover.spec

# Merge all coverdata files into one.
all.coverdata: $(COVERDATA)
	$(gen_verbose) $(ERL) -eval ' \
		$(foreach f,$(COVERDATA),cover:import("$(f)") == ok orelse halt(1),) \
		cover:export("$@"), halt(0).'

# These are only defined if COVER_REPORT_DIR is non-empty. Set COVER_REPORT_DIR to
# empty if you want the coverdata files but not the HTML report.
ifneq ($(COVER_REPORT_DIR),)

.PHONY: cover-report-clean cover-report

cover-report-clean:
	$(gen_verbose) rm -rf $(COVER_REPORT_DIR)

ifeq ($(COVERDATA),)
cover-report:
else

# Modules which include eunit.hrl always contain one line without coverage
# because eunit defines test/0 which is never called. We compensate for this.
EUNIT_HRL_MODS = $(subst $(space),$(comma),$(shell \
	grep -e '^\s*-include.*include/eunit\.hrl"' src/*.erl \
	| sed "s/^src\/\(.*\)\.erl:.*/'\1'/" | uniq))

define cover_report.erl
	$(foreach f,$(COVERDATA),cover:import("$(f)") == ok orelse halt(1),)
	Ms = cover:imported_modules(),
	[cover:analyse_to_file(M, "$(COVER_REPORT_DIR)/" ++ atom_to_list(M)
		++ ".COVER.html", [html])  || M <- Ms],
	Report = [begin {ok, R} = cover:analyse(M, module), R end || M <- Ms],
	EunitHrlMods = [$(EUNIT_HRL_MODS)],
	Report1 = [{M, {Y, case lists:member(M, EunitHrlMods) of
		true -> N - 1; false -> N end}} || {M, {Y, N}} <- Report],
	TotalY = lists:sum([Y || {_, {Y, _}} <- Report1]),
	TotalN = lists:sum([N || {_, {_, N}} <- Report1]),
	Perc = fun(Y, N) -> case Y + N of 0 -> 100; S -> round(100 * Y / S) end end,
	TotalPerc = Perc(TotalY, TotalN),
	{ok, F} = file:open("$(COVER_REPORT_DIR)/index.html", [write]),
	io:format(F, "<!DOCTYPE html><html>~n"
		"<head><meta charset=\"UTF-8\">~n"
		"<title>Coverage report</title></head>~n"
		"<body>~n", []),
	io:format(F, "<h1>Coverage</h1>~n<p>Total: ~p%</p>~n", [TotalPerc]),
	io:format(F, "<table><tr><th>Module</th><th>Coverage</th></tr>~n", []),
	[io:format(F, "<tr><td><a href=\"~p.COVER.html\">~p</a></td>"
		"<td>~p%</td></tr>~n",
		[M, M, Perc(Y, N)]) || {M, {Y, N}} <- Report1],
	How = "$(subst $(space),$(comma)$(space),$(basename $(COVERDATA)))",
	Date = "$(shell date -u "+%Y-%m-%dT%H:%M:%SZ")",
	io:format(F, "</table>~n"
		"<p>Generated using ~s and erlang.mk on ~s.</p>~n"
		"</body></html>", [How, Date]),
	halt().
endef

cover-report:
	$(gen_verbose) mkdir -p $(COVER_REPORT_DIR)
	$(gen_verbose) $(call erlang,$(cover_report.erl))

endif
endif # ifneq ($(COVER_REPORT_DIR),)
