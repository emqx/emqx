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
    %% Use the prometheus app provided by the EMQX release instead of
    %% bundling one in the plugin package: bundling conflicts with nodes
    %% that already load prometheus (e.g. the built-in emqx_prometheus).
    %% ensure_all_started is idempotent: it reuses an already-started
    %% prometheus, or starts the release's copy otherwise.
    {ok, _} = application:ensure_all_started(prometheus),
    ok = emqx_bcast_config:load(),
    {ok, Sup} = emqx_bcast_sup:start_link(),
    ok = emqx_bcast_metrics:init(),
    ok = emqx_bcast:init_tables(),
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
            %% Pool worker count is fixed at supervisor start; restart the
            %% pool child to apply the new size. Queue max is read from
            %% persistent_term on every submit, so it applies immediately.
            emqx_bcast_sup:restart_deliver_pool(NewSize)
    end.

normalized_pool_size() ->
    Config = persistent_term:get({emqx_bcast, config}, #{}),
    maps:get(delivery_pool_size, Config, erlang:system_info(schedulers)).

on_handle_api_call(Method, PathRemainder, Request, _Context) ->
    emqx_bcast_api:handle(Method, PathRemainder, Request).
