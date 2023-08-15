%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_redis).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-export([roots/0, fields/1]).

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_get_status/2
]).

-export([do_get_status/1]).

-export([connect/1]).

-export([do_cmd/3]).

%% redis host don't need parse
-define(REDIS_HOST_OPTIONS, #{
    default_port => ?REDIS_DEFAULT_PORT
}).

%%=====================================================================
roots() ->
    [
        {config, #{
            type => hoconsc:union(
                [
                    hoconsc:ref(?MODULE, cluster),
                    hoconsc:ref(?MODULE, single),
                    hoconsc:ref(?MODULE, sentinel)
                ]
            )
        }}
    ].

fields(single) ->
    [
        {server, server()},
        {redis_type, #{
            type => single,
            default => single,
            required => false,
            desc => ?DESC("single")
        }}
    ] ++
        redis_fields() ++
        emqx_connector_schema_lib:ssl_fields();
fields(cluster) ->
    [
        {servers, servers()},
        {redis_type, #{
            type => cluster,
            default => cluster,
            required => false,
            desc => ?DESC("cluster")
        }}
    ] ++
        lists:keydelete(database, 1, redis_fields()) ++
        emqx_connector_schema_lib:ssl_fields();
fields(sentinel) ->
    [
        {servers, servers()},
        {redis_type, #{
            type => sentinel,
            default => sentinel,
            required => false,
            desc => ?DESC("sentinel")
        }},
        {sentinel, #{
            type => string(),
            required => true,
            desc => ?DESC("sentinel_desc")
        }}
    ] ++
        redis_fields() ++
        emqx_connector_schema_lib:ssl_fields().

server() ->
    Meta = #{desc => ?DESC("server")},
    emqx_schema:servers_sc(Meta, ?REDIS_HOST_OPTIONS).

servers() ->
    Meta = #{desc => ?DESC("servers")},
    emqx_schema:servers_sc(Meta, ?REDIS_HOST_OPTIONS).

%% ===================================================================

callback_mode() -> always_sync.

on_start(
    InstId,
    #{
        redis_type := Type,
        pool_size := PoolSize,
        ssl := SSL
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_redis_connector",
        connector => InstId,
        config => emqx_utils:redact(Config)
    }),
    ConfKey =
        case Type of
            single -> server;
            _ -> servers
        end,
    Servers0 = maps:get(ConfKey, Config),
    Servers1 = lists:map(
        fun(#{hostname := Host, port := Port}) ->
            {Host, Port}
        end,
        emqx_schema:parse_servers(Servers0, ?REDIS_HOST_OPTIONS)
    ),
    Servers = [{servers, Servers1}],
    Database =
        case Type of
            cluster -> [];
            _ -> [{database, maps:get(database, Config)}]
        end,
    Opts =
        [
            {pool_size, PoolSize},
            {username, maps:get(username, Config, undefined)},
            {password, eredis_secret:wrap(maps:get(password, Config, ""))},
            {auto_reconnect, ?AUTO_RECONNECT_INTERVAL}
        ] ++ Database ++ Servers,
    Options =
        case maps:get(enable, SSL) of
            true ->
                [
                    {ssl, true},
                    {ssl_options, emqx_tls_lib:to_client_opts(SSL)}
                ];
            false ->
                [{ssl, false}]
        end ++ [{sentinel, maps:get(sentinel, Config, undefined)}],
    State = #{pool_name => InstId, type => Type},
    ok = emqx_resource:allocate_resource(InstId, type, Type),
    ok = emqx_resource:allocate_resource(InstId, pool_name, InstId),
    case Type of
        cluster ->
            case eredis_cluster:start_pool(InstId, Opts ++ [{options, Options}]) of
                {ok, _} ->
                    {ok, State};
                {ok, _, _} ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason}
            end;
        _ ->
            case emqx_resource_pool:start(InstId, ?MODULE, Opts ++ [{options, Options}]) of
                ok -> {ok, State};
                {error, Reason} -> {error, Reason}
            end
    end.

on_stop(InstId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_redis_connector",
        connector => InstId
    }),
    case emqx_resource:get_allocated_resources(InstId) of
        #{pool_name := PoolName, type := cluster} ->
            eredis_cluster:stop_pool(PoolName);
        #{pool_name := PoolName, type := _} ->
            emqx_resource_pool:stop(PoolName);
        _ ->
            ok
    end.

on_query(InstId, {cmd, _} = Query, State) ->
    do_query(InstId, Query, State);
on_query(InstId, {cmds, _} = Query, State) ->
    do_query(InstId, Query, State).

do_query(InstId, Query, #{pool_name := PoolName, type := Type} = State) ->
    ?TRACE(
        "QUERY",
        "redis_connector_received",
        #{connector => InstId, query => Query, state => State}
    ),
    Result =
        case Type of
            cluster -> do_cmd(PoolName, cluster, Query);
            _ -> ecpool:pick_and_do(PoolName, {?MODULE, do_cmd, [Type, Query]}, no_handover)
        end,
    case Result of
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "redis_connector_do_query_failed",
                connector => InstId,
                query => Query,
                reason => Reason
            }),
            case is_unrecoverable_error(Reason) of
                true ->
                    {error, {unrecoverable_error, Reason}};
                false when Reason =:= ecpool_empty ->
                    {error, {recoverable_error, Reason}};
                false ->
                    Result
            end;
        _ ->
            Result
    end.

is_unrecoverable_error(Results) when is_list(Results) ->
    lists:any(fun is_unrecoverable_error/1, Results);
is_unrecoverable_error({error, <<"ERR unknown command ", _/binary>>}) ->
    true;
is_unrecoverable_error({error, invalid_cluster_command}) ->
    true;
is_unrecoverable_error(_) ->
    false.

on_get_status(_InstId, #{type := cluster, pool_name := PoolName}) ->
    case eredis_cluster:pool_exists(PoolName) of
        true ->
            Health = eredis_cluster:ping_all(PoolName),
            status_result(Health);
        false ->
            disconnected
    end;
on_get_status(_InstId, #{pool_name := PoolName}) ->
    Health = emqx_resource_pool:health_check_workers(PoolName, fun ?MODULE:do_get_status/1),
    status_result(Health).

do_get_status(Conn) ->
    case eredis:q(Conn, ["PING"]) of
        {ok, _} -> true;
        _ -> false
    end.

status_result(_Status = true) -> connected;
status_result(_Status = false) -> connecting.

do_cmd(PoolName, cluster, {cmd, Command}) ->
    eredis_cluster:q(PoolName, Command);
do_cmd(Conn, _Type, {cmd, Command}) ->
    eredis:q(Conn, Command);
do_cmd(PoolName, cluster, {cmds, Commands}) ->
    % TODO
    % Cluster mode is currently incompatible with batching.
    wrap_qp_result([eredis_cluster:q(PoolName, Command) || Command <- Commands]);
do_cmd(Conn, _Type, {cmds, Commands}) ->
    wrap_qp_result(eredis:qp(Conn, Commands)).

wrap_qp_result({error, _} = Error) ->
    Error;
wrap_qp_result(Results) when is_list(Results) ->
    AreAllOK = lists:all(
        fun
            ({ok, _}) -> true;
            ({error, _}) -> false
        end,
        Results
    ),
    case AreAllOK of
        true -> {ok, Results};
        false -> {error, Results}
    end.

%% ===================================================================
connect(Opts) ->
    eredis:start_link(Opts).

redis_fields() ->
    [
        {pool_size, fun emqx_connector_schema_lib:pool_size/1},
        {username, fun emqx_connector_schema_lib:username/1},
        {password, fun emqx_connector_schema_lib:password/1},
        {database, #{
            type => non_neg_integer(),
            default => 0,
            desc => ?DESC("database")
        }},
        {auto_reconnect, fun emqx_connector_schema_lib:auto_reconnect/1}
    ].
