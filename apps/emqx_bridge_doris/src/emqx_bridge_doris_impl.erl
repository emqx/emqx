%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_doris_impl).

-feature(maybe_expr, enable).

-behaviour(emqx_resource).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_trace.hrl").
-include_lib("mysql/include/protocol.hrl").

%% `emqx_resource` API
-export([
    resource_type/0,
    callback_mode/0,

    on_start/2,
    on_stop/2,
    on_get_status/2,

    on_get_channels/1,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3,

    on_query/3,
    on_batch_query/3
]).

%% API
-export([]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% Allocatable resources

%% Underlying implementation uses these keys.
-define(installed_channels, channels).

-type connector_config() :: #{}.
-type connector_state() :: emqx_bridge_mysql_connector:connector_state().

-type channel_config() :: #{
    parameters := #{}
}.

-type query() :: {_Tag :: channel_id(), _Data :: map()}.

%%------------------------------------------------------------------------------
%% `emqx_resource' API
%%------------------------------------------------------------------------------

-spec resource_type() -> atom().
resource_type() ->
    doris.

-spec callback_mode() -> callback_mode().
callback_mode() ->
    emqx_bridge_mysql_connector:callback_mode().

-spec on_start(connector_resource_id(), connector_config()) ->
    {ok, connector_state()} | {error, _Reason}.
on_start(ConnResId, ConnConfig0) ->
    ConnConfig = ConnConfig0#{
        basic_capabilities => #{?CLIENT_TRANSACTIONS => false},
        parse_server_opts => #{default_port => 9030}
    },
    emqx_bridge_mysql_connector:on_start(ConnResId, ConnConfig).

-spec on_stop(connector_resource_id(), connector_state()) -> ok.
on_stop(ConnResId, ConnState) ->
    Res = emqx_bridge_mysql_connector:on_stop(ConnResId, ConnState),
    ?tp("doris_connector_stop", #{instance_id => ConnResId}),
    Res.

-spec on_get_status(connector_resource_id(), connector_state()) ->
    ?status_connected | ?status_disconnected.
on_get_status(ConnResId, ConnState) ->
    emqx_bridge_mysql_connector:on_get_status(ConnResId, ConnState).

-spec on_get_channels(connector_resource_id()) ->
    [{action_resource_id(), channel_config()}].
on_get_channels(ConnResId) ->
    emqx_bridge_v2:get_channels_for_connector(ConnResId).

-spec on_add_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id(),
    channel_config()
) ->
    {ok, connector_state()}.
on_add_channel(ConnResId, ConnState, ChanResId, ActionConfig) ->
    emqx_bridge_mysql_connector:on_add_channel(ConnResId, ConnState, ChanResId, ActionConfig).

-spec on_remove_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id()
) ->
    {ok, connector_state()}.
on_remove_channel(
    ConnResId,
    ConnState = #{?installed_channels := InstalledChannels},
    ChanResId
) when
    is_map_key(ChanResId, InstalledChannels)
->
    emqx_bridge_mysql_connector:on_remove_channel(ConnResId, ConnState, ChanResId);
on_remove_channel(_ConnResId, ConnState, _ChanResId) ->
    {ok, ConnState}.

-spec on_get_channel_status(
    connector_resource_id(),
    action_resource_id(),
    connector_state()
) ->
    ?status_connected | ?status_disconnected.
on_get_channel_status(
    ConnResId,
    ChanResId,
    ConnState = #{?installed_channels := InstalledChannels}
) when is_map_key(ChanResId, InstalledChannels) ->
    emqx_bridge_mysql_connector:on_get_channel_status(ConnResId, ChanResId, ConnState);
on_get_channel_status(_ConnResId, _ChanResId, _ConnState) ->
    ?status_disconnected.

-spec on_query(connector_resource_id(), query(), connector_state()) ->
    {ok, _Result} | {error, _Reason}.
on_query(ConnResId, Query, ConnState) ->
    emqx_bridge_mysql_connector:on_query(ConnResId, Query, ConnState).

-spec on_batch_query(connector_resource_id(), [query()], connector_state()) ->
    ok | {error, term()}.
on_batch_query(ConnResId, Queries, ConnState) ->
    emqx_bridge_mysql_connector:on_batch_query(ConnResId, Queries, ConnState).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
