%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_jwks_connector).

-behaviour(emqx_resource).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%% callbacks of behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_get_status/2,
    connect/1
]).

-define(DEFAULT_POOL_SIZE, 8).

resource_type() -> jwks.

callback_mode() -> always_sync.

on_start(InstId, Opts) ->
    PoolOpts = [
        {pool_size, maps:get(pool_size, Opts, ?DEFAULT_POOL_SIZE)},
        {connector_opts, Opts}
    ],
    case emqx_resource_pool:start(InstId, ?MODULE, PoolOpts) of
        ok -> {ok, #{pool_name => InstId}};
        {error, Reason} -> {error, Reason}
    end.

on_stop(_InstId, #{pool_name := PoolName}) ->
    emqx_resource_pool:stop(PoolName).

on_query(InstId, get_jwks, #{pool_name := PoolName}) ->
    Result = ecpool:pick_and_do(PoolName, {emqx_authn_jwks_client, get_jwks, []}, no_handover),
    case Result of
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "emqx_authn_jwks_client_query_failed",
                connector => InstId,
                command => get_jwks,
                reason => Reason
            });
        _ ->
            ok
    end,
    Result.

on_get_status(_InstId, #{pool_name := PoolName}) ->
    case emqx_resource_pool:health_check_workers(PoolName, fun health_check/1) of
        true -> ?status_connected;
        false -> ?status_disconnected
    end.

health_check(Conn) ->
    case emqx_authn_jwks_client:get_jwks(Conn) of
        {ok, _} -> true;
        _ -> false
    end.

connect(Opts) ->
    ConnectorOpts = proplists:get_value(connector_opts, Opts),
    emqx_authn_jwks_client:start_link(ConnectorOpts).
