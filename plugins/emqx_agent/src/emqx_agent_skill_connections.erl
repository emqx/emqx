%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill_connections).

-include_lib("emqx/include/logger.hrl").

-export([
    init/0,
    deinit/0,
    reconcile/0,
    status/1,
    resource_id/1
]).

-define(TYPE_POSTGRESQL, postgresql).
-define(RESOURCE_GROUP, <<"emqx_agent">>).
-define(RESOURCE_PREFIX, "emqx_agent_connection:").

-type connection_id() :: binary().

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

init() ->
    reconcile().

deinit() ->
    stop_all_local().

-spec reconcile() -> ok.
reconcile() ->
    DesiredEnabled = enabled_connections(emqx_agent_config:parsed_config([connections], [])),
    lists:foreach(
        fun(ResourceId) -> maybe_stop_removed(ResourceId, DesiredEnabled) end,
        agent_resource_ids()
    ),
    maps:foreach(fun maybe_start_or_restart/2, DesiredEnabled),
    ok.

-spec resource_id(connection_id()) -> binary().
resource_id(ConnectionId) ->
    <<?RESOURCE_PREFIX, ConnectionId/binary>>.

-spec status(map()) -> map().
status(#{id := _ConnectionId, enable := false}) ->
    status_map(stopped, null);
status(#{id := ConnectionId}) ->
    ResourceId = resource_id(ConnectionId),
    case emqx_resource:get_instance(ResourceId) of
        {ok, ?RESOURCE_GROUP, #{status := Status, error := Error}} ->
            status_map(Status, Error);
        {ok, OtherGroup, _} ->
            status_map(disconnected, {unexpected_group, OtherGroup});
        {error, Reason} ->
            status_map(stopped, Reason)
    end.

%%--------------------------------------------------------------------
%% Internal resource reconciliation
%%--------------------------------------------------------------------

enabled_connections(Connections) ->
    maps:from_list([
        {ConnectionId, Conn}
     || #{id := ConnectionId, enable := true} = Conn <- Connections
    ]).

agent_resource_ids() ->
    lists:filter(fun is_agent_resource_id/1, emqx_resource:list_group_instances(?RESOURCE_GROUP)).

is_agent_resource_id(<<?RESOURCE_PREFIX, _/binary>>) -> true;
is_agent_resource_id(_) -> false.

connection_id_from_resource_id(<<?RESOURCE_PREFIX, ConnectionId/binary>>) ->
    ConnectionId.

maybe_stop_removed(ResourceId, DesiredEnabled) ->
    ConnectionId = connection_id_from_resource_id(ResourceId),
    case maps:is_key(ConnectionId, DesiredEnabled) of
        true -> ok;
        false -> stop_local(ResourceId)
    end.

maybe_start_or_restart(ConnectionId, Conn) ->
    case connector_module(Conn) of
        {ok, ConnectorMod} ->
            ResourceId = resource_id(ConnectionId),
            ResourceConfig = resource_config(Conn),
            ensure_local(ConnectionId, ResourceId, ConnectorMod, ResourceConfig);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "agent_connection_unknown_type",
                connection_id => ConnectionId,
                reason => Reason
            }),
            ok
    end.

ensure_local(ConnectionId, ResourceId, ConnectorMod, ResourceConfig) ->
    case emqx_resource:get_instance(ResourceId) of
        {ok, ?RESOURCE_GROUP, #{mod := ConnectorMod, config := ResourceConfig}} ->
            ok;
        {ok, ?RESOURCE_GROUP, #{mod := ConnectorMod}} ->
            recreate_local(ConnectionId, ResourceId, ConnectorMod, ResourceConfig);
        {ok, ?RESOURCE_GROUP, #{mod := _OtherMod}} ->
            ok = stop_local(ResourceId),
            create_local(ConnectionId, ResourceId, ConnectorMod, ResourceConfig);
        {ok, OtherGroup, _} ->
            ?SLOG(error, #{
                msg => "agent_connection_resource_id_conflict",
                connection_id => ConnectionId,
                resource_id => ResourceId,
                group => OtherGroup
            }),
            ok;
        {error, not_found} ->
            create_local(ConnectionId, ResourceId, ConnectorMod, ResourceConfig);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "agent_connection_lookup_failed",
                connection_id => ConnectionId,
                reason => Reason
            }),
            ok
    end.

create_local(ConnectionId, ResourceId, ConnectorMod, ResourceConfig) ->
    case
        emqx_resource:create_local(
            ResourceId,
            ?RESOURCE_GROUP,
            ConnectorMod,
            ResourceConfig,
            resource_opts()
        )
    of
        {ok, _} ->
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "agent_connection_start_failed",
                connection_id => ConnectionId,
                reason => Reason
            }),
            ok
    end.

recreate_local(ConnectionId, ResourceId, ConnectorMod, ResourceConfig) ->
    case emqx_resource:recreate_local(ResourceId, ConnectorMod, ResourceConfig, resource_opts()) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "agent_connection_recreate_failed",
                connection_id => ConnectionId,
                reason => Reason
            }),
            ok
    end.

stop_all_local() ->
    lists:foreach(fun stop_local/1, agent_resource_ids()).

stop_local(ResourceId) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok.

resource_opts() ->
    #{
        health_check_interval => 1000,
        start_timeout => 5000,
        start_after_created => true
    }.

connector_module(#{type := ?TYPE_POSTGRESQL}) ->
    {ok, emqx_agent_skill_postgresql_connector};
connector_module(Conn) ->
    {error, {unsupported_type, maps:get(type, Conn, undefined)}}.

resource_config(#{config := Config}) ->
    Config.

status_map(Status, Error) ->
    #{
        <<"status">> => status_to_binary(Status),
        <<"error">> => error_to_json(Error)
    }.

status_to_binary(Status) when is_atom(Status) ->
    atom_to_binary(Status, utf8);
status_to_binary(Status) when is_binary(Status) ->
    Status.

error_to_json(null) ->
    null;
error_to_json(undefined) ->
    null;
error_to_json(Error) when is_binary(Error) ->
    Error;
error_to_json(Error) ->
    iolist_to_binary(io_lib:format("~p", [Error])).
