%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_bridge_v2_test_connector).

-behaviour(emqx_resource).

-export([
    query_mode/1,
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3
]).

query_mode(_Config) ->
    sync.

resource_type() -> test_connector.

callback_mode() ->
    always_sync.

on_start(
    _InstId,
    #{on_start_fun := FunRef} = Conf
) ->
    Fun = emqx_bridge_v2_SUITE:unwrap_fun(FunRef),
    Fun(Conf);
on_start(_InstId, Config) ->
    {ok, Config}.

on_add_channel(
    _InstId,
    _State,
    _ChannelId,
    #{on_add_channel_fun := FunRef}
) ->
    Fun = emqx_bridge_v2_SUITE:unwrap_fun(FunRef),
    Fun();
on_add_channel(
    InstId,
    #{on_add_channel_fun := FunRef} = ConnectorState,
    ChannelId,
    ChannelConfig
) ->
    Fun = emqx_bridge_v2_SUITE:unwrap_fun(FunRef),
    Fun(InstId, ConnectorState, ChannelId, ChannelConfig);
on_add_channel(
    _InstId,
    State,
    ChannelId,
    ChannelConfig
) ->
    Channels = maps:get(channels, State, #{}),
    NewChannels = maps:put(ChannelId, ChannelConfig, Channels),
    NewState = maps:put(channels, NewChannels, State),
    {ok, NewState}.

on_stop(_InstanceId, _State) ->
    ok.

on_remove_channel(
    _InstId,
    State,
    ChannelId
) ->
    Channels = maps:get(channels, State, #{}),
    NewChannels = maps:remove(ChannelId, Channels),
    NewState = maps:put(channels, NewChannels, State),
    {ok, NewState}.

on_query(
    _InstId,
    {ChannelId, Message},
    ConnectorState
) ->
    Channels = maps:get(channels, ConnectorState, #{}),
    %% Lookup the channel
    ChannelState = maps:get(ChannelId, Channels, not_found),
    SendTo = maps:get(send_to, ChannelState),
    SendTo ! Message,
    ok.

on_get_channels(ResId) ->
    emqx_bridge_v2:get_channels_for_connector(ResId).

on_query_async(
    _InstId,
    {_MessageTag, _Message},
    _AsyncReplyFn,
    _ConnectorState
) ->
    throw(not_implemented).

on_get_status(
    _InstId,
    #{on_get_status_fun := FunRef}
) ->
    Fun = emqx_bridge_v2_SUITE:unwrap_fun(FunRef),
    Fun();
on_get_status(
    _InstId,
    _State
) ->
    connected.

on_get_channel_status(
    _ResId,
    ChannelId,
    State
) ->
    Channels = maps:get(channels, State, #{}),
    ChannelState = maps:get(ChannelId, Channels, #{}),
    case ChannelState of
        #{on_get_channel_status_fun := FunRef} ->
            Fun = emqx_bridge_v2_SUITE:unwrap_fun(FunRef),
            Fun();
        _ ->
            connected
    end.
