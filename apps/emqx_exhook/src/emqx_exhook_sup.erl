%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exhook_sup).

-behaviour(supervisor).

-include("emqx_exhook.hrl").

-export([ start_link/0
        , init/1
        ]).

-export([ start_grpc_client_channel/3
        , stop_grpc_client_channel/1
        ]).

-define(CHILD(Mod, Type, Args),
            #{ id => Mod
             , start => {Mod, start_link, Args}
             , type => Type
             , shutdown => 15000
             }
       ).

%%--------------------------------------------------------------------
%%  Supervisor APIs & Callbacks
%%--------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Mngr = ?CHILD(emqx_exhook_mngr, worker,
                  [servers(), auto_reconnect(), request_options(), hooks_options()]),
    {ok, {{one_for_one, 10, 100}, [Mngr]}}.

servers() ->
    env(servers, []).

auto_reconnect() ->
    env(auto_reconnect, 60000).

request_options() ->
    #{timeout => env(request_timeout, 5000),
      request_failed_action => env(request_failed_action, deny),
      pool_size => env(pool_size, erlang:system_info(schedulers))
     }.

hooks_options() ->
    #{hook_priority => env(hook_priority, ?DEFAULT_HOOK_PRIORITY)
     }.

env(Key, Def) ->
    application:get_env(emqx_exhook, Key, Def).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec start_grpc_client_channel(
        string(),
        uri_string:uri_string(),
        grpc_client_sup:options()) -> {ok, pid()} | {error, term()}.
start_grpc_client_channel(Name, SvrAddr, Options) ->
    grpc_client_sup:create_channel_pool(Name, SvrAddr, Options).

-spec stop_grpc_client_channel(string()) -> ok.
stop_grpc_client_channel(Name) ->
    %% Avoid crash due to hot-upgrade had unloaded
    %% grpc application
    try
        grpc_client_sup:stop_channel_pool(Name)
    catch
        _:_:_ ->
            ok
    end.
