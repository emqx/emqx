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
-module(emqx_redis).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-export([namespace/0, roots/0, fields/1, redis_fields/0, desc/1]).

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    resource_type/0,
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
namespace() -> "redis".

roots() ->
    [
        {config, #{
            type => hoconsc:union(
                [
                    ?R_REF(redis_cluster),
                    ?R_REF(redis_single),
                    ?R_REF(redis_sentinel)
                ]
            )
        }}
    ].

fields(redis_single) ->
    fields(redis_single_connector) ++
        emqx_connector_schema_lib:ssl_fields();
fields(redis_single_connector) ->
    [
        {server, server()},
        redis_type(single)
    ] ++ redis_fields();
fields(redis_cluster) ->
    fields(redis_cluster_connector) ++
        emqx_connector_schema_lib:ssl_fields();
fields(redis_cluster_connector) ->
    [
        {servers, servers()},
        redis_type(cluster)
    ] ++ lists:keydelete(database, 1, redis_fields());
fields(redis_sentinel) ->
    fields(redis_sentinel_connector) ++
        emqx_connector_schema_lib:ssl_fields();
fields(redis_sentinel_connector) ->
    [
        {servers, servers()},
        redis_type(sentinel),
        {sentinel, #{
            type => string(),
            required => true,
            desc => ?DESC("sentinel_desc")
        }}
    ] ++ redis_fields().

server() ->
    Meta = #{desc => ?DESC("server")},
    emqx_schema:servers_sc(Meta, ?REDIS_HOST_OPTIONS).

servers() ->
    Meta = #{desc => ?DESC("servers")},
    emqx_schema:servers_sc(Meta, ?REDIS_HOST_OPTIONS).

desc(redis_cluster_connector) ->
    ?DESC(redis_cluster_connector);
desc(redis_single_connector) ->
    ?DESC(redis_single_connector);
desc(redis_sentinel_connector) ->
    ?DESC(redis_sentinel_connector);
desc(_) ->
    undefined.

%% ===================================================================

redis_type(Type) ->
    {redis_type, #{
        type => Type,
        default => Type,
        required => false,
        desc => ?DESC(Type)
    }}.

resource_type() -> redis.

callback_mode() -> always_sync.

on_start(InstId, Config0) ->
    ?SLOG(info, #{
        msg => "starting_redis_connector",
        connector => InstId,
        config => emqx_utils:redact(Config0)
    }),
    Config = config(Config0),
    #{pool_size := PoolSize, ssl := SSL, redis_type := Type} = Config,
    Options = ssl_options(SSL) ++ [{sentinel, maps:get(sentinel, Config, undefined)}],
    Opts =
        [
            {username, maps:get(username, Config, undefined)},
            {password, maps:get(password, Config, "")},
            {servers, servers(Config)},
            {options, Options},
            {pool_size, PoolSize},
            {auto_reconnect, ?AUTO_RECONNECT_INTERVAL}
        ] ++ database(Config),

    State = #{pool_name => InstId, type => Type},
    ok = emqx_resource:allocate_resource(InstId, type, Type),
    ok = emqx_resource:allocate_resource(InstId, pool_name, InstId),
    case Type of
        cluster ->
            case eredis_cluster:start_pool(InstId, Opts) of
                {ok, _} ->
                    {ok, State};
                {ok, _, _} ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason}
            end;
        _ ->
            case emqx_resource_pool:start(InstId, ?MODULE, Opts) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

ssl_options(SSL = #{enable := true}) ->
    [
        {ssl, true},
        {ssl_options, emqx_tls_lib:to_client_opts(SSL)}
    ];
ssl_options(#{enable := false}) ->
    [{ssl, false}].

on_stop(InstId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_redis_connector",
        connector => InstId
    }),
    case emqx_resource:get_allocated_resources(InstId) of
        #{pool_name := PoolName, type := cluster} ->
            case eredis_cluster:stop_pool(PoolName) of
                {error, not_found} -> ok;
                ok -> ok;
                Error -> Error
            end;
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
            %% eredis_cluster has null slot even pool_exists when emqx start before redis cluster.
            %% we need restart eredis_cluster pool when pool_worker(slot) is empty.
            %% If the pool is empty, it means that there are no workers attempting to reconnect.
            %% In this case, we can directly consider it as a disconnect and then proceed to reconnect.
            case eredis_cluster_monitor:get_all_pools(PoolName) of
                [] ->
                    ?status_disconnected;
                [_ | _] ->
                    do_cluster_status_check(PoolName)
            end;
        false ->
            ?status_disconnected
    end;
on_get_status(_InstId, #{pool_name := PoolName}) ->
    HealthCheckResoults = emqx_resource_pool:health_check_workers(
        PoolName,
        fun ?MODULE:do_get_status/1,
        emqx_resource_pool:health_check_timeout(),
        #{return_values => true}
    ),
    case HealthCheckResoults of
        {ok, Results} ->
            sum_worker_results(Results);
        Error ->
            {?status_disconnected, Error}
    end.

do_cluster_status_check(Pool) ->
    Pongs = eredis_cluster:qa(Pool, [<<"PING">>]),
    sum_worker_results(Pongs).

do_get_status(Conn) ->
    eredis:q(Conn, ["PING"]).

sum_worker_results([]) ->
    ?status_connected;
sum_worker_results([{error, <<"NOAUTH Authentication required.">>} = Error | _Rest]) ->
    ?tp(emqx_redis_auth_required_error, #{}),
    %% This requires user action to fix so we set the status to disconnected
    {?status_disconnected, {unhealthy_target, Error}};
sum_worker_results([{ok, _} | Rest]) ->
    sum_worker_results(Rest);
sum_worker_results([Error | _Rest]) ->
    ?SLOG(
        warning,
        #{
            msg => "emqx_redis_check_status_error",
            error => Error
        }
    ),
    {?status_connecting, Error}.

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
%% parameters for connector
config(#{parameters := #{} = Param} = Config) ->
    maps:merge(maps:remove(parameters, Config), Param);
%% is for authn/authz
config(Config) ->
    Config.

servers(#{server := Server}) ->
    servers(Server);
servers(#{servers := Servers}) ->
    servers(Servers);
servers(Servers) ->
    lists:map(
        fun(#{hostname := Host, port := Port}) ->
            {Host, Port}
        end,
        emqx_schema:parse_servers(Servers, ?REDIS_HOST_OPTIONS)
    ).

database(#{redis_type := cluster}) -> [];
database(#{database := Database}) -> [{database, Database}].

connect(Opts) ->
    eredis:start_link(Opts).

redis_fields() ->
    [
        {pool_size, fun emqx_connector_schema_lib:pool_size/1},
        {username, fun emqx_connector_schema_lib:username/1},
        {password, emqx_connector_schema_lib:password_field()},
        {database, #{
            type => non_neg_integer(),
            default => 0,
            desc => ?DESC("database")
        }},
        {auto_reconnect, fun emqx_connector_schema_lib:auto_reconnect/1}
    ].
