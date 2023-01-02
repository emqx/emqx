%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc This module is NOT used in production.
%% It is used to collect coverage data when running blackbox test
-module(emqx_cover).

-include_lib("covertool/include/covertool.hrl").

-ifdef(EMQX_ENTERPRISE).
-define(OUTPUT_APPNAME, 'EMQX Enterprise').
-else.
-define(OUTPUT_APPNAME, 'EMQX').
-endif.

-export([start/0,
         start/1,
         abort/0,
         export_and_stop/1,
         lookup_source/1
        ]).

%% This is a ETS table to keep a mapping of module name (atom) to
%% .erl file path (relative path from project root)
%% We needed this ETS table because the source file information
%% is missing from the .beam metadata sicne we are using 'deterministic'
%% compile flag.
-define(SRC, emqx_cover_module_src).

%% @doc Start cover.
%% All emqx_ modules will be cover-compiled, this may cause
%% some excessive RAM consumption and result in warning logs.
start() ->
    start(#{}).

%% @doc Start cover.
%% All emqx_ modules will be cover-compiled, this may cause
%% some excessive RAM consumption and result in warning logs.
%% Supported options:
%% - project_root: the directory to search for .erl source code
%% - debug_secret_file: only applicable to EMQX Enterprise
start(Opts) ->
    ok = abort(),
    %% spawn a ets table owner
    %% this implementation is kept dead-simple
    %% because there is no concurrency requirement
    Parent = self(),
    {Pid, Ref} =
        erlang:spawn_monitor(
          fun() ->
                  true = register(?SRC, self()),
                  _ = ets:new(?SRC, [named_table, public]),
                  _ = Parent ! {started, self()},
                  receive
                      stop ->
                          ok
                  end
          end),
    receive
        {started, Pid} ->
            ok;
        {'DOWN', Ref, process, Pid, Reason} ->
            throw({failed_to_start, Reason})
    after
        1000 ->
            throw({failed_to_start, timeout})
    end,
    Modules = modules(Opts),
    ok = maybe_set_secret(Opts),
    case cover:start() of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        Other ->
            throw(Other)
    end,
    ok = cover_compile(Modules),
    io:format("cover-compiled ~p modules~n", [length(Modules)]),
    ok = build_source_mapping(Opts, sets:from_list(Modules, [{version, 2}])),
    CachedModulesCount = ets:info(?SRC, size),
    io:format("source-cached ~p modules~n", [CachedModulesCount]),
    ok.

%% @doc Abort cover data collection without exporting.
abort() ->
    _ = cover:stop(),
    case whereis(?SRC) of
        undefined -> ok;
        Pid ->
            Ref = monitor(process, Pid),
            exit(Pid, kill),
            receive
                {'DOWN', Ref, process, Pid, _} ->
                    ok
            end
    end,
    ok.

%% @doc Export coverage report (xml) format.
%% e.g. `emqx_cover:export_and_stop("/tmp/cover.xml").'
export_and_stop(Path) when is_list(Path) ->
    ProjectRoot = get_project_root(),
    Config = #config{appname = ?OUTPUT_APPNAME,
                     sources = [ProjectRoot],
                     output = Path,
                     lookup_source = fun ?MODULE:lookup_source/1
                    },
    covertool:generate_report(Config, cover:modules()).

build_source_mapping(Opts, Modules) ->
    Default = os_env("EMQX_PROJECT_ROOT"),
    case maps:get(project_root, Opts, Default) of
        "" ->
            io:format(standard_error, "EMQX_PROJECT_ROOT is not set", []),
            throw(emqx_project_root_undefined);
        Dir ->
            ok = put_project_root(Dir),
            ok = do_build_source_mapping(Dir, Modules)
    end.

get_project_root() ->
    [{_, Dir}] = ets:lookup(?SRC, {root, ?OUTPUT_APPNAME}),
    Dir.

put_project_root(Dir) ->
    _ = ets:insert(?SRC, {{root, ?OUTPUT_APPNAME}, Dir}),
    ok.

do_build_source_mapping(Dir, Modules) ->
    All = filelib:wildcard("**/*.erl", Dir),
    lists:foreach(
      fun(Path) ->
              ModuleNameStr = filename:basename(Path, ".erl"),
              Module = list_to_atom(ModuleNameStr),
              case sets:is_element(Module, Modules) of
                  true ->
                      ets:insert(?SRC, {Module, Path});
                  false ->
                      ok
              end
      end, All),
    ok.

lookup_source(Module) ->
    case ets:lookup(?SRC, Module) of
        [{_, Path}] ->
            Path;
        [] ->
            false
    end.

maybe_set_secret(Opts) ->
    Default = os_env("EMQX_DEBUG_SECRET_FILE"),
    case maps:get(debug_secret_file, Opts, Default) of
        "" ->
            ok;
        File ->
            ok = emqx:set_debug_secret(File)
    end.

modules(_Opts) ->
    %% TODO better filter based on Opts,
    %% e.g. we may want to see coverage info for ehttpc
    Filter = fun is_emqx_module/1,
    find_modules(Filter).

cover_compile(Modules) ->
    Results = cover:compile_beam(Modules),
    Errors = lists:filter(fun({ok, _}) -> false;
                             (_) -> true
                          end, Results),
    case Errors of
        [] ->
            ok;
        _ ->
            io:format("failed_to_cover_compile:~n~p~n", [Errors]),
            throw(failed_to_cover_compile)
    end.

find_modules(Filter) ->
    All = code:all_loaded(),
    F = fun({M, _BeamPath}) -> Filter(M) andalso {true, M} end,
    lists:filtermap(F, All).

is_emqx_module(?MODULE) ->
    %% do not cover-compile self
    false;
is_emqx_module(Module) ->
    case erlang:atom_to_binary(Module, utf8) of
        <<"emqx", _/binary>> ->
            true;
        _ ->
            false
    end.

os_env(Name) ->
    os:getenv(Name, "").
