%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exhook_app).

-behaviour(application).

-include("emqx_exhook.hrl").

-emqx_plugin(extension).

-define(CNTER, emqx_exhook_counter).

-export([ start/2
        , stop/1
        , prep_stop/1
        ]).

%% Internal export
-export([ load_server/2
        , unload_server/1
        , unload_exhooks/0
        , init_hooks_cnter/0
        ]).

%%--------------------------------------------------------------------
%% Application callbacks
%%--------------------------------------------------------------------

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_exhook_sup:start_link(),

    %% Init counter
    init_hooks_cnter(),

    %% Load all dirvers
    load_all_servers(),

    %% Register CLI
    emqx_ctl:register_command(exhook, {emqx_exhook_cli, cli}, []),
    {ok, Sup}.

prep_stop(State) ->
    emqx_ctl:unregister_command(exhook),
    _ = unload_exhooks(),
    ok = unload_all_servers(),
    State.

stop(_State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

load_all_servers() ->
    lists:foreach(fun({Name, Options}) ->
        load_server(Name, Options)
    end, application:get_env(?APP, servers, [])).

unload_all_servers() ->
    emqx_exhook:disable_all().

load_server(Name, Options) ->
    emqx_exhook:enable(Name, Options).

unload_server(Name) ->
    emqx_exhook:disable(Name).

unload_exhooks() ->
    [emqx:unhook(Name, {M, F}) ||
     {Name, {M, F, _A}} <- ?ENABLED_HOOKS].

init_hooks_cnter() ->
    try
        _ = ets:new(?CNTER, [named_table, public]), ok
    catch
        error:badarg:_ ->
            ok
    end.

