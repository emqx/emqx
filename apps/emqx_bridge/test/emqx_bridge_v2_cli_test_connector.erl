%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Minimal connector for `emqx_bridge_v2_cli_SUITE`: its connector-level status is
%% read directly off its own config (`status`), so tests can deterministically stand
%% up a "connected" and a "disconnected" connector without closures or ETS plumbing.
-module(emqx_bridge_v2_cli_test_connector).

-behaviour(emqx_resource).

-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3
]).

resource_type() -> cli_test_connector.

callback_mode() -> always_sync.

on_start(_InstId, Config) ->
    {ok, Config}.

on_stop(_InstId, _State) ->
    ok.

on_get_status(_InstId, #{status := Status}) ->
    Status.

on_add_channel(_InstId, State, ChannelId, ChannelConfig) ->
    Channels = maps:get(channels, State, #{}),
    {ok, State#{channels => Channels#{ChannelId => ChannelConfig}}}.

on_remove_channel(_InstId, State, ChannelId) ->
    Channels = maps:get(channels, State, #{}),
    {ok, State#{channels => maps:remove(ChannelId, Channels)}}.

on_get_channels(ResId) ->
    emqx_bridge_v2:get_channels_for_connector(ResId).

on_get_channel_status(_ResId, _ChannelId, _State) ->
    connected.
