%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    ListenerConfigs = emqx_gateway_utils_conf:to_rt_listener_configs(
        GwName, NConfig, ?MOD_CFG, Ctx
    ),
    case emqx_gateway_utils:start_listeners(ListenerConfigs) of
        {ok, ListenerPids} ->
            {ok, ListenerPids, _GwState = #{ctx => Ctx}};
        {error, {Reason, #{original_listener_config := ListenerConfig}}} ->
            throw(
                {badconf, #{
                    key => listeners,
                    value => ListenerConfig,
                    reason => Reason
                }}
            )
    end.

on_gateway_update(
    Config, _Gateway = #{config := OldConfig, name := GwName}, GwState = #{ctx := Ctx}
) ->
    try
        ensure_broadcast_started(Config),
        clear_predefined_topics(OldConfig),
        ensure_predefined_topics(Config),
        Config1 = maps:without([broadcast, predefined], Config),
        OldConfig1 = maps:without([broadcast, predefined], OldConfig),
        OldListenerConfigs = emqx_gateway_utils_conf:to_rt_listener_configs(
            GwName, OldConfig1, ?MOD_CFG, Ctx
        ),
        NewListenerConfigs = emqx_gateway_utils_conf:to_rt_listener_configs(
            GwName, Config1, ?MOD_CFG, Ctx
        ),
        case
            emqx_gateway_utils:update_gateway_listeners(
                GwName, OldListenerConfigs, NewListenerConfigs
            )
        of
            {ok, NewPids} ->
                {ok, NewPids, GwState};
            {error, _} = Error ->
                Error
        end
    catch
        Class:Reason:Stk ->
            ?SLOG(error, #{
                msg => "gateway_update_failed",
                class => Class,
                reason => Reason,
                stacktrace => Stk,
                gateway => GwName
            }),
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
    ListenerIds = emqx_gateway_utils_conf:to_rt_listener_ids(GwName, Config),
    emqx_gateway_utils:stop_listeners(ListenerIds).

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
