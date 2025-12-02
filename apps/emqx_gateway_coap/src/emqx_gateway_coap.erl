%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc The CoAP Gateway implement
-module(emqx_gateway_coap).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_gateway/include/emqx_gateway.hrl").

%% define a gateway named coap
-gateway(#{
    name => coap,
    callback_module => ?MODULE,
    config_schema_module => emqx_coap_schema
}).

%% callback_module must implement the emqx_gateway_impl behaviour
-behaviour(emqx_gateway_impl).

%% callback for emqx_gateway_impl
-export([
    on_gateway_load/2,
    on_gateway_update/3,
    on_gateway_unload/2
]).

-define(DEFAULT_MOD_CFG, #{
    frame_mod => emqx_coap_frame,
    chann_mod => emqx_coap_channel
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
    ModConfig = mod_cfg(Config),
    ListenerConfigs = emqx_gateway_utils_conf:to_rt_listener_configs(
        GwName, Config, ModConfig, Ctx
    ),
    case emqx_gateway_utils:start_listeners(ListenerConfigs) of
        {ok, ListenerPids} ->
            {ok, ListenerPids, #{ctx => Ctx}};
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
    Config,
    _Gateway = #{
        name := GwName,
        config := OldConfig
    },
    GwState = #{ctx := Ctx}
) ->
    OldModConfig = mod_cfg(OldConfig),
    OldListenerConfigs = emqx_gateway_utils_conf:to_rt_listener_configs(
        GwName, OldConfig, OldModConfig, Ctx
    ),
    NewModConfig = mod_cfg(Config),
    NewListenerConfigs = emqx_gateway_utils_conf:to_rt_listener_configs(
        GwName, Config, NewModConfig, Ctx
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
    _GwState = #{ctx := Ctx}
) ->
    ModConfig = mod_cfg(Config),
    ListenerConfigs = emqx_gateway_utils_conf:to_rt_listener_configs(
        GwName, Config, ModConfig, Ctx
    ),
    emqx_gateway_utils:stop_listeners(ListenerConfigs).

mod_cfg(#{connection_required := true}) ->
    maps:merge(?DEFAULT_MOD_CFG, #{
        connection_mod => esockd_udp_proxy,
        esockd_proxy_opts => #{
            connection_mod => emqx_coap_proxy_conn
        }
    });
mod_cfg(#{connection_required := false}) ->
    ?DEFAULT_MOD_CFG.
