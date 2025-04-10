%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc The MQTT-SN Gateway implement interface
-module(emqx_gateway_mqttsn).

-include_lib("emqx/include/logger.hrl").

%% define a gateway named stomp
-gateway(#{
    name => mqttsn,
    callback_module => ?MODULE,
    config_schema_module => emqx_mqttsn_schema
}).

%% callback_module must implement the emqx_gateway_impl behaviour
-behaviour(emqx_gateway_impl).

%% callback for emqx_gateway_impl
-export([
    on_gateway_load/2,
    on_gateway_update/3,
    on_gateway_unload/2
]).

-import(
    emqx_gateway_utils,
    [
        normalize_config/1,
        start_listeners/4,
        stop_listeners/2,
        update_gateway/5
    ]
).

-define(MOD_CFG, #{
    frame_mod => emqx_mqttsn_frame,
    chann_mod => emqx_mqttsn_channel
}).

%%--------------------------------------------------------------------
%% emqx_gateway_impl callbacks
%%--------------------------------------------------------------------

on_gateway_load(
    _Gateway = #{
        name := GwName,
        config := Config
    },
    Ctx
) ->
    ensure_broadcast_started(Config),
    ensure_predefined_topics(Config),
    NConfig = maps:without([broadcast, predefined], Config),
    Listeners = emqx_gateway_utils:normalize_config(NConfig),
    case
        start_listeners(
            Listeners, GwName, Ctx, ?MOD_CFG
        )
    of
        {ok, ListenerPids} ->
            {ok, ListenerPids, _GwState = #{ctx => Ctx}};
        {error, {Reason, Listener}} ->
            throw(
                {badconf, #{
                    key => listeners,
                    value => Listener,
                    reason => Reason
                }}
            )
    end.

on_gateway_update(Config, Gateway = #{config := OldConfig}, GwState = #{ctx := Ctx}) ->
    GwName = maps:get(name, Gateway),
    try
        ensure_broadcast_started(Config),
        clear_predefined_topics(OldConfig),
        ensure_predefined_topics(Config),
        Config1 = maps:without([broadcast, predefined], Config),
        OldConfig1 = maps:without([broadcast, predefined], OldConfig),
        {ok, NewPids} = update_gateway(Config1, OldConfig1, GwName, Ctx, ?MOD_CFG),
        {ok, NewPids, GwState}
    catch
        Class:Reason:Stk ->
            logger:error(
                "Failed to update ~ts; "
                "reason: {~0p, ~0p} stacktrace: ~0p",
                [GwName, Class, Reason, Stk]
            ),
            {error, Reason}
    end.

on_gateway_unload(
    _Gateway = #{
        name := GwName,
        config := Config
    },
    _GwState
) ->
    ok = clear_predefined_topics(Config),
    Listeners = normalize_config(Config),
    stop_listeners(GwName, Listeners).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

ensure_broadcast_started(Config) ->
    case maps:get(broadcast, Config, false) of
        false ->
            ok;
        true ->
            %% FIXME:
            Port = 1884,
            SnGwId = maps:get(gateway_id, Config, undefined),
            _ = emqx_mqttsn_broadcast:start_link(SnGwId, Port),
            ok
    end.

ensure_predefined_topics(Config) ->
    PredefTopics = maps:get(predefined, Config, []),
    ok = emqx_mqttsn_registry:persist_predefined_topics(PredefTopics).

clear_predefined_topics(Config) ->
    PredefTopics = maps:get(predefined, Config, []),
    ok = emqx_mqttsn_registry:clear_predefined_topics(PredefTopics).
