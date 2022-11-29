%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([start/0,
         start/1,
         export_and_stop/1
        ]).

start() ->
    start(#{}).

start(Opts) ->
    ok = maybe_set_secret(),
    case cover:start() of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        Other ->
            throw(Other)
    end,
    ok = cover_compile(Opts).

export_and_stop(Path) ->
    ok = cover:export(Path),
    _ = cover:stop(),
    ok.

maybe_set_secret() ->
    case os:getenv("EMQX_DEBUG_SECRET_FILE") of
        false ->
            ok;
        "" ->
            ok;
        File ->
            ok = emqx:set_debug_secret(File)
    end.

cover_compile(_Opts) ->
    %% TODO better filter based on Opts,
    %% e.g. we may want to see coverage info for ehttpc
    Filter = fun is_emqx_module/1,
    Modules = find_modules(Filter),
    Results = cover:compile_beam(Modules),
    Errors = lists:filter(fun({ok, _}) -> false;
                             (_) -> true
                          end, Results),
    case Errors of
        [] ->
            ok;
        _ ->
            io:format(user, "failed_to_cover_compile:~n~p~n", [Errors]),
            throw(failed_to_cover_compile)
    end.

find_modules(Filter) ->
    All = code:all_loaded(),
    F = fun({M, _BeamPath}) -> Filter(M) end,
    lists:filter(F, All).

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
