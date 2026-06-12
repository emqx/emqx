%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_relup_eredis_upgrade).

-elvis([{elvis_style, invalid_dynamic_call, disable}]).

-export([
    stop_redis_resources/0,
    start_redis_resources/0
]).

-define(REDIS_RESOURCE_IDS, {?MODULE, redis_resource_ids}).

%% One-off helpers for the 5.10.4 -> 5.10.5 relup hop. They run as
%% code_changes, so keep them in the relup plugin rather than emqx_post_upgrade.
stop_redis_resources() ->
    %% Store the stopped set outside process state so a retried relup can restart
    %% resources that were already stopped by a previous failed attempt.
    Mod = emqx_resource,
    ResIds = running_redis_resource_ids(Mod),
    lists:foreach(fun(ResId) -> stop_redis_resource(Mod, ResId) end, ResIds),
    stop_legacy_redis_sentinel().

start_redis_resources() ->
    Mod = emqx_resource,
    ResIds = persistent_term:get(?REDIS_RESOURCE_IDS, []),
    lists:foreach(fun(ResId) -> start_redis_resource(Mod, ResId) end, ResIds),
    persistent_term:erase(?REDIS_RESOURCE_IDS),
    ok.

running_redis_resource_ids(Mod) ->
    lists:filter(
        fun(ResId) -> is_running_resource(Mod, ResId) end,
        lists:append([Mod:list_instances_by_type(Type) || Type <- redis_resource_types()])
    ).

redis_resource_types() ->
    [
        emqx_redis,
        emqx_bridge_redis_connector,
        emqx_offline_messages_redis_connector
    ].

is_running_resource(Mod, ResId) ->
    case Mod:get_instance(ResId) of
        {ok, _, #{status := stopped}} ->
            false;
        {ok, _, _} ->
            true;
        _ ->
            false
    end.

stop_redis_resource(Mod, ResId) ->
    case Mod:stop(ResId) of
        ok ->
            remember_stopped_redis_resource(ResId);
        {error, not_found} ->
            ok;
        {error, Reason} ->
            error({failed_to_stop_redis_resource, ResId, Reason})
    end.

start_redis_resource(Mod, ResId) ->
    case Mod:start(ResId) of
        ok ->
            ok;
        {error, not_found} ->
            ok;
        {error, Reason} ->
            error({failed_to_start_redis_resource, ResId, Reason})
    end.

remember_stopped_redis_resource(ResId) ->
    Stopped = persistent_term:get(?REDIS_RESOURCE_IDS, []),
    persistent_term:put(?REDIS_RESOURCE_IDS, lists:usort([ResId | Stopped])).

stop_legacy_redis_sentinel() ->
    %% Old eredis versions use a process-registered singleton outside the new
    %% eredis supervisor tree. It may already be absent; either case is fine.
    Mod = eredis_sentinel,
    try Mod:stop() of
        _ -> ok
    catch
        _:_ -> ok
    end,
    ok.
