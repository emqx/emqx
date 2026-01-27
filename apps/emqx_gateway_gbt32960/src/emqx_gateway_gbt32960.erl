%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc The GBT32960 Gateway implement
-module(emqx_gateway_gbt32960).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_gateway/include/emqx_gateway.hrl").

%% define a gateway named gbt32960
-gateway(#{
    name => gbt32960,
    callback_module => ?MODULE,
    config_schema_module => emqx_gbt32960_schema
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
    frame_mod => emqx_gbt32960_frame,
    chann_mod => emqx_gbt32960_channel
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
    ListenerConfigs = emqx_gateway_utils_conf:to_rt_listener_configs(GwName, Config, ?MOD_CFG, Ctx),
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
    OldListenerConfigs = emqx_gateway_utils_conf:to_rt_listener_configs(
        GwName, OldConfig, ?MOD_CFG, Ctx
    ),
    NewListenerConfigs = emqx_gateway_utils_conf:to_rt_listener_configs(
        GwName, Config, ?MOD_CFG, Ctx
    ),
    case
        emqx_gateway_utils:update_gateway_listeners(GwName, OldListenerConfigs, NewListenerConfigs)
    of
        {ok, NewPids} ->
            {ok, NewPids, GwState};
        {error, _} = Error ->
            Error
    end.

on_gateway_unload(
    _Gateway = #{
        name := GwName,
        config := Config
    },
    _GwState
) ->
    ListenerIds = emqx_gateway_utils_conf:to_rt_listener_ids(GwName, Config),
    emqx_gateway_utils:stop_listeners(ListenerIds).
