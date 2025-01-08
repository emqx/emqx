%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-import(
    emqx_gateway_utils,
    [
        normalize_config/1,
        start_listeners/4,
        stop_listeners/2
    ]
).

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
            Listeners = emqx_gateway_utils:normalize_config(Config),
            ModCfg = #{
                frame_mod => emqx_coap_frame,
                chann_mod => emqx_lwm2m_channel
            },
            case
                emqx_gateway_utils:start_listeners(
                    Listeners, GwName, Ctx, ModCfg
                )
            of
                {ok, ListenerPids} ->
                    {ok, ListenerPids, #{ctx => Ctx, registry => RegPid}};
                {error, {Reason, Listener}} ->
                    _ = emqx_lwm2m_xml_object_db:stop(),
                    throw(
                        {badconf, #{
                            key => listeners,
                            value => Listener,
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

on_gateway_update(Config, Gateway, GwState = #{ctx := Ctx}) ->
    GwName = maps:get(name, Gateway),
    try
        %% XXX: 1. How hot-upgrade the changes ???
        %% XXX: 2. Check the New confs first before destroy old instance ???
        on_gateway_unload(Gateway, GwState),
        on_gateway_load(Gateway#{config => Config}, Ctx)
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
    _GwState = #{registry := _RegPid}
) ->
    _ =
        try
            emqx_lwm2m_xml_object_db:stop()
        catch
            _:_ -> ok
        end,
    Listeners = emqx_gateway_utils:normalize_config(Config),
    emqx_gateway_utils:stop_listeners(GwName, Listeners).
