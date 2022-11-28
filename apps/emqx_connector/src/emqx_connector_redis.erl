%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_connector_redis).

-include("emqx_connector.hrl").
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
    host_type => hostname,
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
        {server, fun server/1},
        {redis_type, #{
            type => single,
            default => single,
            required => true,
            desc => ?DESC("single")
        }}
    ] ++
        redis_fields() ++
        emqx_connector_schema_lib:ssl_fields();
fields(cluster) ->
    [
        {servers, fun servers/1},
        {redis_type, #{
            type => cluster,
            default => cluster,
            required => true,
            desc => ?DESC("cluster")
        }}
    ] ++
        lists:keydelete(database, 1, redis_fields()) ++
        emqx_connector_schema_lib:ssl_fields();
fields(sentinel) ->
    [
        {servers, fun servers/1},
        {redis_type, #{
            type => sentinel,
            default => sentinel,
            required => true,
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

server(type) -> emqx_schema:host_port();
server(required) -> true;
server(validator) -> [?NOT_EMPTY("the value of the field 'server' cannot be empty")];
server(converter) -> fun to_server_raw/1;
server(desc) -> ?DESC("server");
server(_) -> undefined.

servers(type) -> list();
servers(required) -> true;
servers(validator) -> [?NOT_EMPTY("the value of the field 'servers' cannot be empty")];
servers(converter) -> fun to_servers_raw/1;
servers(desc) -> ?DESC("servers");
servers(_) -> undefined.

%% ===================================================================
callback_mode() -> always_sync.

on_start(
    InstId,
    #{
        redis_type := Type,
        pool_size := PoolSize,
        auto_reconnect := AutoReconn,
        ssl := SSL
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_redis_connector",
        connector => InstId,
        config => Config
    }),
    Servers =
        case Type of
            single -> [{servers, [maps:get(server, Config)]}];
            _ -> [{servers, maps:get(servers, Config)}]
        end,
    Database =
        case Type of
            cluster -> [];
            _ -> [{database, maps:get(database, Config)}]
        end,
    Opts =
        [
            {pool_size, PoolSize},
            {password, maps:get(password, Config, "")},
            {auto_reconnect, reconn_interval(AutoReconn)}
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
    PoolName = emqx_plugin_libs_pool:pool_name(InstId),
    State = #{poolname => PoolName, type => Type, auto_reconnect => AutoReconn},
    case Type of
        cluster ->
            case eredis_cluster:start_pool(PoolName, Opts ++ [{options, Options}]) of
                {ok, _} ->
                    {ok, State};
                {ok, _, _} ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason}
            end;
        _ ->
            case
                emqx_plugin_libs_pool:start_pool(PoolName, ?MODULE, Opts ++ [{options, Options}])
            of
                ok -> {ok, State};
                {error, Reason} -> {error, Reason}
            end
    end.

on_stop(InstId, #{poolname := PoolName, type := Type}) ->
    ?SLOG(info, #{
        msg => "stopping_redis_connector",
        connector => InstId
    }),
    case Type of
        cluster -> eredis_cluster:stop_pool(PoolName);
        _ -> emqx_plugin_libs_pool:stop_pool(PoolName)
    end.

on_query(InstId, {cmd, _} = Query, State) ->
    do_query(InstId, Query, State);
on_query(InstId, {cmds, _} = Query, State) ->
    do_query(InstId, Query, State).

do_query(InstId, Query, #{poolname := PoolName, type := Type} = State) ->
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
            });
        _ ->
            ok
    end,
    Result.

extract_eredis_cluster_workers(PoolName) ->
    lists:flatten([
        gen_server:call(PoolPid, get_all_workers)
     || PoolPid <- eredis_cluster_monitor:get_all_pools(PoolName)
    ]).

eredis_cluster_workers_exist_and_are_connected(Workers) ->
    length(Workers) > 0 andalso
        lists:all(
            fun({_, Pid, _, _}) ->
                eredis_cluster_pool_worker:is_connected(Pid) =:= true
            end,
            Workers
        ).

on_get_status(_InstId, #{type := cluster, poolname := PoolName, auto_reconnect := AutoReconn}) ->
    case eredis_cluster:pool_exists(PoolName) of
        true ->
            Workers = extract_eredis_cluster_workers(PoolName),
            Health = eredis_cluster_workers_exist_and_are_connected(Workers),
            status_result(Health, AutoReconn);
        false ->
            disconnected
    end;
on_get_status(_InstId, #{poolname := Pool, auto_reconnect := AutoReconn}) ->
    Health = emqx_plugin_libs_pool:health_check_ecpool_workers(Pool, fun ?MODULE:do_get_status/1),
    status_result(Health, AutoReconn).

do_get_status(Conn) ->
    case eredis:q(Conn, ["PING"]) of
        {ok, _} -> true;
        _ -> false
    end.

status_result(_Status = true, _AutoReconn) -> connected;
status_result(_Status = false, _AutoReconn = true) -> connecting;
status_result(_Status = false, _AutoReconn = false) -> disconnected.

reconn_interval(true) -> 15;
reconn_interval(false) -> false.

do_cmd(PoolName, cluster, {cmd, Command}) ->
    eredis_cluster:q(PoolName, Command);
do_cmd(Conn, _Type, {cmd, Command}) ->
    eredis:q(Conn, Command);
do_cmd(PoolName, cluster, {cmds, Commands}) ->
    wrap_qp_result(eredis_cluster:qp(PoolName, Commands));
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
        {password, fun emqx_connector_schema_lib:password/1},
        {database, #{
            type => integer(),
            default => 0,
            required => true,
            desc => ?DESC("database")
        }},
        {auto_reconnect, fun emqx_connector_schema_lib:auto_reconnect/1}
    ].

-spec to_server_raw(string()) ->
    {string(), pos_integer()}.
to_server_raw(Server) ->
    emqx_connector_schema_lib:parse_server(Server, ?REDIS_HOST_OPTIONS).

-spec to_servers_raw(string()) ->
    [{string(), pos_integer()}].
to_servers_raw(Servers) ->
    lists:map(
        fun(Server) ->
            emqx_connector_schema_lib:parse_server(Server, ?REDIS_HOST_OPTIONS)
        end,
        string:tokens(str(Servers), ", ")
    ).

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.
