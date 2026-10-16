%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_app).

-behaviour(application).
-emqx_plugin(?MODULE).

-export([start/2, stop/1]).
-export([on_config_changed/2]).
-export([on_handle_api_call/4]).

start(_StartType, _StartArgs) ->
    {ok, _} = application:ensure_all_started(prometheus),
    ok = emqx_bcast_config:load(),
    %% Tables before the supervisor: the index owner rebuilds from mria in
    %% its init and must find the tables ready on first activation.
    ok = emqx_bcast:init_tables(),
    {ok, Sup} = emqx_bcast_sup:start_link(),
    ok = emqx_bcast_metrics:init(),
    ok = emqx_bcast:hook(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_bcast:unhook(),
    ok.

on_config_changed(_OldConf, NewConf) ->
    OldSize = normalized_pool_size(),
    ok = emqx_bcast_config:update(NewConf),
    NewSize = normalized_pool_size(),
    case OldSize =:= NewSize of
        true ->
            ok;
        false ->
            emqx_bcast_sup:restart_pools(NewSize)
    end.

normalized_pool_size() ->
    emqx_bcast_config:get(delivery_pool_size).

on_handle_api_call(Method, PathRemainder, Request, _Context) ->
    emqx_bcast_api:handle(Method, PathRemainder, Request).
