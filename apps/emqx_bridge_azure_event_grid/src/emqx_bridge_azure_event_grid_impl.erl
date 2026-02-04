%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_azure_event_grid_impl).

-behaviour(emqx_resource).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_trace.hrl").

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
    on_query_async/4
]).

%% API
-export([]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% Allocatable resources

-define(installed_channels, installed_channels).

-type connector_config() :: #{}.
-type connector_state() :: #{
    ?installed_channels := #{channel_id() => channel_state()}
}.

-type channel_config() :: #{
    parameters := #{}
}.
-type channel_state() :: #{}.

-type query() :: {_Tag :: channel_id(), _Data :: map()}.

%%------------------------------------------------------------------------------
%% `emqx_resource' API
%%------------------------------------------------------------------------------

-spec resource_type() -> atom().
resource_type() ->
    azure_event_grid.

-spec callback_mode() -> callback_mode().
callback_mode() ->
    emqx_bridge_mqtt_connector:callback_mode().

-spec on_start(connector_resource_id(), connector_config()) ->
    {ok, connector_state()} | {error, _Reason}.
on_start(ConnResId, ConnConfig0) ->
    ConnConfig = maybe_set_sni(ConnConfig0),
    emqx_bridge_mqtt_connector:on_start(ConnResId, ConnConfig).

-spec on_stop(connector_resource_id(), connector_state()) -> ok.
on_stop(ConnResId, ConnState) ->
    emqx_bridge_mqtt_connector:on_stop(ConnResId, ConnState).

-spec on_get_status(connector_resource_id(), connector_state()) ->
    ?status_connected | ?status_disconnected.
on_get_status(ConnResId, ConnState) ->
    emqx_bridge_mqtt_connector:on_get_status(ConnResId, ConnState).

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
    emqx_bridge_mqtt_connector:on_add_channel(ConnResId, ConnState, ChanResId, ActionConfig).

-spec on_remove_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id()
) ->
    {ok, connector_state()}.
on_remove_channel(
    ConnResId,
    ConnState = #{?installed_channels := InstalledChannels0},
    ChanResId
) when
    is_map_key(ChanResId, InstalledChannels0)
->
    emqx_bridge_mqtt_connector:on_remove_channel(ConnResId, ConnState, ChanResId);
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
    emqx_bridge_mqtt_connector:on_get_channel_status(ConnResId, ChanResId, ConnState);
on_get_channel_status(_ConnResId, _ChanResId, _ConnState) ->
    ?status_disconnected.

-spec on_query(connector_resource_id(), query(), connector_state()) ->
    {ok, _Result} | {error, _Reason}.
on_query(
    ConnResId,
    {ChanResId, #{} = Data},
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    emqx_bridge_mqtt_connector:on_query(ConnResId, {ChanResId, Data}, ConnState);
on_query(_ConnResId, Query, _ConnState) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

-spec on_query_async(
    connector_resource_id(),
    {message_tag(), map()},
    {ReplyFun :: function(), Args :: list()},
    connector_state()
) -> {ok, pid()} | {error, term()}.
on_query_async(
    ConnResId,
    {ChanResId, #{} = Data},
    ReplyFnAndArgs,
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    emqx_bridge_mqtt_connector:on_query_async(
        ConnResId, {ChanResId, Data}, ReplyFnAndArgs, ConnState
    );
on_query_async(_ConnResId, Query, _ReplyFnAndArgs, _ConnState) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

%% Azure Event Grid requires SNI to be set when connecting.  We copy it from `server` if
%% unset and TLS is enabled.
maybe_set_sni(#{ssl := #{enable := true} = SSLOpts0} = ConnConfig0) when
    not is_map_key(server_name_indication, SSLOpts0)
->
    #{server := Server} = ConnConfig0,
    #{hostname := Hostname} = emqx_schema:parse_server(Server, #{default_port => 8883}),
    SSLOpts = SSLOpts0#{server_name_indication => Hostname},
    ConnConfig0#{ssl := SSLOpts};
maybe_set_sni(ConnConfig) ->
    ConnConfig.
