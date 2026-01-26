%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc The LwM2M Gateway implement
-module(emqx_gateway_lwm2m).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_gateway/include/emqx_gateway.hrl").

%% define a gateway named stomp
-gateway(#{
    name => lwm2m,
    callback_module => ?MODULE,
    config_schema_module => emqx_lwm2m_schema
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
    frame_mod => emqx_coap_frame,
    chann_mod => emqx_lwm2m_channel
}).

%%--------------------------------------------------------------------
%% emqx_gateway_registry callbacks
%%--------------------------------------------------------------------

on_gateway_load(
    _Gateway = #{
        name := GwName,
        config := Config
    },
    Ctx
) ->
    XmlDir = maps:get(xml_dir, Config),
    case emqx_lwm2m_xml_object_db:start_link(XmlDir) of
        {ok, RegPid} ->
            ListenerConfigs = emqx_gateway_utils_conf:to_rt_listener_configs(
                GwName, Config, ?MOD_CFG, Ctx
            ),
            case emqx_gateway_utils:start_listeners(ListenerConfigs) of
                {ok, ListenerPids} ->
                    {ok, ListenerPids, #{ctx => Ctx, registry => RegPid}};
                {error, {Reason, #{original_listener_config := ListenerConfig}}} ->
                    _ = emqx_lwm2m_xml_object_db:stop(),
                    throw(
                        {badconf, #{
                            key => listeners,
                            value => ListenerConfig,
                            reason => Reason
                        }}
                    )
            end;
        {error, Reason} ->
            throw(
                {badconf, #{
                    key => xml_dir,
                    value => XmlDir,
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
    _GwState = #{registry := _RegPid}
) ->
    try
        emqx_lwm2m_xml_object_db:stop()
    catch
        Error:Reason ->
            ?SLOG(error, #{
                msg => "lwm2m_xml_object_db_stop_failed",
                class => Error,
                reason => Reason,
                gateway => GwName
            })
    end,
    ListenerIds = emqx_gateway_utils_conf:to_rt_listener_ids(GwName, Config),
    emqx_gateway_utils:stop_listeners(ListenerIds).
