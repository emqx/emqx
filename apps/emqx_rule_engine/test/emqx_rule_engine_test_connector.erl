%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_rule_engine_test_connector).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_batch_query/3,
    on_batch_query_async/4,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3
]).

%% ===================================================================
resource_type() -> test_connector.

callback_mode() -> always_sync.

on_start(
    _InstId,
    _Config
) ->
    {ok, #{installed_channels => #{}}}.

on_stop(_InstId, _State) ->
    ok.

on_add_channel(
    _InstId,
    #{
        installed_channels := InstalledChannels
    } = OldState,
    ChannelId,
    ChannelConfig
) ->
    NewInstalledChannels = maps:put(ChannelId, ChannelConfig, InstalledChannels),
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState}.

on_remove_channel(
    _InstId,
    OldState,
    _ChannelId
) ->
    {ok, OldState}.

on_get_channel_status(
    _ResId,
    _ChannelId,
    _State
) ->
    connected.

on_get_channels(ResId) ->
    emqx_bridge_v2:get_channels_for_connector(ResId).

on_query(
    _InstId,
    _Query,
    _State
) ->
    ok.

on_query_async(
    _InstId,
    _Query,
    _State,
    _Callback
) ->
    ok.

on_batch_query(
    _InstId,
    [{ChannelId, _Req} | _] = Msg,
    #{installed_channels := Channels} = _State
) ->
    #{parameters := #{values := #{send_to_pid := PidBin}}} = maps:get(ChannelId, Channels),
    Pid = binary_to_term(emqx_utils:hexstr_to_bin(PidBin)),
    Pid ! Msg,
    emqx_trace:rendered_action_template(ChannelId, #{nothing_to_render => ok}),
    ok.

on_batch_query_async(
    _InstId,
    _Batch,
    _State,
    _Callback
) ->
    ok.

on_get_status(_InstId, _State) ->
    connected.
