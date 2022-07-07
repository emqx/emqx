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
-module(emqx_connector_mongo).

-include("emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    on_start/2,
    on_stop/2,
    on_query/4,
    on_get_status/2
]).

%% ecpool callback
-export([connect/1]).

-export([roots/0, fields/1, desc/1]).

-export([mongo_query/5, check_worker_health/1]).

-define(HEALTH_CHECK_TIMEOUT, 30000).

%% mongo servers don't need parse
-define(MONGO_HOST_OPTIONS, #{
    host_type => hostname,
    default_port => ?MONGO_DEFAULT_PORT
}).

%%=====================================================================
roots() ->
    [
        {config, #{
            type => hoconsc:union(
                [
                    hoconsc:ref(?MODULE, single),
                    hoconsc:ref(?MODULE, rs),
                    hoconsc:ref(?MODULE, sharded)
                ]
            )
        }}
    ].

fields(single) ->
    [
        {mongo_type, #{
            type => single,
            default => single,
            required => true,
            desc => ?DESC("single_mongo_type")
        }},
        {server, fun server/1},
        {w_mode, fun w_mode/1}
    ] ++ mongo_fields();
fields(rs) ->
    [
        {mongo_type, #{
            type => rs,
            default => rs,
            required => true,
            desc => ?DESC("rs_mongo_type")
        }},
        {servers, fun servers/1},
        {w_mode, fun w_mode/1},
        {r_mode, fun r_mode/1},
        {replica_set_name, fun replica_set_name/1}
    ] ++ mongo_fields();
fields(sharded) ->
    [
        {mongo_type, #{
            type => sharded,
            default => sharded,
            required => true,
            desc => ?DESC("sharded_mongo_type")
        }},
        {servers, fun servers/1},
        {w_mode, fun w_mode/1}
    ] ++ mongo_fields();
fields(topology) ->
    [
        {pool_size, fun emqx_connector_schema_lib:pool_size/1},
        {max_overflow, fun max_overflow/1},
        {overflow_ttl, fun duration/1},
        {overflow_check_period, fun duration/1},
        {local_threshold_ms, fun duration/1},
        {connect_timeout_ms, fun duration/1},
        {socket_timeout_ms, fun duration/1},
        {server_selection_timeout_ms, fun duration/1},
        {wait_queue_timeout_ms, fun duration/1},
        {heartbeat_frequency_ms, fun duration/1},
        {min_heartbeat_frequency_ms, fun duration/1}
    ].

desc(single) ->
    ?DESC("desc_single");
desc(rs) ->
    ?DESC("desc_rs");
desc(sharded) ->
    ?DESC("desc_sharded");
desc(topology) ->
    ?DESC("desc_topology");
desc(_) ->
    undefined.

mongo_fields() ->
    [
        {srv_record, fun srv_record/1},
        {pool_size, fun emqx_connector_schema_lib:pool_size/1},
        {username, fun emqx_connector_schema_lib:username/1},
        {password, fun emqx_connector_schema_lib:password/1},
        {auth_source, #{
            type => binary(),
            required => false,
            desc => ?DESC("auth_source")
        }},
        {database, fun emqx_connector_schema_lib:database/1},
        {topology, #{type => hoconsc:ref(?MODULE, topology), required => false}}
    ] ++
        emqx_connector_schema_lib:ssl_fields().

%% ===================================================================

on_start(
    InstId,
    Config = #{
        mongo_type := Type,
        pool_size := PoolSize,
        ssl := SSL
    }
) ->
    Msg =
        case Type of
            single -> "starting_mongodb_single_connector";
            rs -> "starting_mongodb_replica_set_connector";
            sharded -> "starting_mongodb_sharded_connector"
        end,
    ?SLOG(info, #{msg => Msg, connector => InstId, config => Config}),
    NConfig = #{hosts := Hosts} = may_parse_srv_and_txt_records(Config),
    SslOpts =
        case maps:get(enable, SSL) of
            true ->
                [
                    {ssl, true},
                    {ssl_opts, emqx_tls_lib:to_client_opts(SSL)}
                ];
            false ->
                [{ssl, false}]
        end,
    Topology = maps:get(topology, NConfig, #{}),
    Opts = [
        {mongo_type, init_type(NConfig)},
        {hosts, Hosts},
        {pool_size, PoolSize},
        {options, init_topology_options(maps:to_list(Topology), [])},
        {worker_options, init_worker_options(maps:to_list(NConfig), SslOpts)}
    ],
    PoolName = emqx_plugin_libs_pool:pool_name(InstId),
    case emqx_plugin_libs_pool:start_pool(PoolName, ?MODULE, Opts) of
        ok -> {ok, #{poolname => PoolName, type => Type}};
        {error, Reason} -> {error, Reason}
    end.

on_stop(InstId, #{poolname := PoolName}) ->
    ?SLOG(info, #{
        msg => "stopping_mongodb_connector",
        connector => InstId
    }),
    emqx_plugin_libs_pool:stop_pool(PoolName).

on_query(
    InstId,
    {Action, Collection, Filter, Projector},
    AfterQuery,
    #{poolname := PoolName} = State
) ->
    Request = {Action, Collection, Filter, Projector},
    ?TRACE(
        "QUERY",
        "mongodb_connector_received",
        #{request => Request, connector => InstId, state => State}
    ),
    case
        ecpool:pick_and_do(
            PoolName,
            {?MODULE, mongo_query, [Action, Collection, Filter, Projector]},
            no_handover
        )
    of
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "mongodb_connector_do_query_failed",
                request => Request,
                reason => Reason,
                connector => InstId
            }),
            emqx_resource:query_failed(AfterQuery),
            {error, Reason};
        {ok, Cursor} when is_pid(Cursor) ->
            emqx_resource:query_success(AfterQuery),
            mc_cursor:foldl(fun(O, Acc2) -> [O | Acc2] end, [], Cursor, 1000);
        Result ->
            emqx_resource:query_success(AfterQuery),
            Result
    end.

-dialyzer({nowarn_function, [on_get_status/2]}).
on_get_status(InstId, #{poolname := PoolName} = _State) ->
    case health_check(PoolName) of
        true ->
            ?tp(debug, emqx_connector_mongo_health_check, #{
                instance_id => InstId,
                status => ok
            }),
            connected;
        false ->
            ?tp(warning, emqx_connector_mongo_health_check, #{
                instance_id => InstId,
                status => failed
            }),
            disconnected
    end.

health_check(PoolName) ->
    emqx_plugin_libs_pool:health_check_ecpool_workers(
        PoolName, fun ?MODULE:check_worker_health/1, ?HEALTH_CHECK_TIMEOUT + timer:seconds(1)
    ).

%% ===================================================================

check_worker_health(Conn) ->
    %% we don't care if this returns something or not, we just to test the connection
    try do_test_query(Conn) of
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "mongo_connection_get_status_error",
                reason => Reason
            }),
            false;
        _ ->
            true
    catch
        Class:Error ->
            ?SLOG(warning, #{
                msg => "mongo_connection_get_status_exception",
                class => Class,
                error => Error
            }),
            false
    end.

do_test_query(Conn) ->
    mongoc:transaction_query(
        Conn,
        fun(Conf = #{pool := Worker}) ->
            Query = mongoc:find_one_query(Conf, <<"foo">>, #{}, #{}, 0),
            mc_worker_api:find_one(Worker, Query)
        end,
        #{},
        ?HEALTH_CHECK_TIMEOUT
    ).

connect(Opts) ->
    Type = proplists:get_value(mongo_type, Opts, single),
    Hosts = proplists:get_value(hosts, Opts, []),
    Options = proplists:get_value(options, Opts, []),
    WorkerOptions = proplists:get_value(worker_options, Opts, []),
    mongo_api:connect(Type, Hosts, Options, WorkerOptions).

mongo_query(Conn, find, Collection, Filter, Projector) ->
    mongo_api:find(Conn, Collection, Filter, Projector);
mongo_query(Conn, find_one, Collection, Filter, Projector) ->
    mongo_api:find_one(Conn, Collection, Filter, Projector);
%% Todo xxx
mongo_query(_Conn, _Action, _Collection, _Filter, _Projector) ->
    ok.

init_type(#{mongo_type := rs, replica_set_name := ReplicaSetName}) ->
    {rs, ReplicaSetName};
init_type(#{mongo_type := Type}) ->
    Type.

init_topology_options([{pool_size, Val} | R], Acc) ->
    init_topology_options(R, [{pool_size, Val} | Acc]);
init_topology_options([{max_overflow, Val} | R], Acc) ->
    init_topology_options(R, [{max_overflow, Val} | Acc]);
init_topology_options([{overflow_ttl, Val} | R], Acc) ->
    init_topology_options(R, [{overflow_ttl, Val} | Acc]);
init_topology_options([{overflow_check_period, Val} | R], Acc) ->
    init_topology_options(R, [{overflow_check_period, Val} | Acc]);
init_topology_options([{local_threshold_ms, Val} | R], Acc) ->
    init_topology_options(R, [{'localThresholdMS', Val} | Acc]);
init_topology_options([{connect_timeout_ms, Val} | R], Acc) ->
    init_topology_options(R, [{'connectTimeoutMS', Val} | Acc]);
init_topology_options([{socket_timeout_ms, Val} | R], Acc) ->
    init_topology_options(R, [{'socketTimeoutMS', Val} | Acc]);
init_topology_options([{server_selection_timeout_ms, Val} | R], Acc) ->
    init_topology_options(R, [{'serverSelectionTimeoutMS', Val} | Acc]);
init_topology_options([{wait_queue_timeout_ms, Val} | R], Acc) ->
    init_topology_options(R, [{'waitQueueTimeoutMS', Val} | Acc]);
init_topology_options([{heartbeat_frequency_ms, Val} | R], Acc) ->
    init_topology_options(R, [{'heartbeatFrequencyMS', Val} | Acc]);
init_topology_options([{min_heartbeat_frequency_ms, Val} | R], Acc) ->
    init_topology_options(R, [{'minHeartbeatFrequencyMS', Val} | Acc]);
init_topology_options([_ | R], Acc) ->
    init_topology_options(R, Acc);
init_topology_options([], Acc) ->
    Acc.

init_worker_options([{database, V} | R], Acc) ->
    init_worker_options(R, [{database, V} | Acc]);
init_worker_options([{auth_source, V} | R], Acc) ->
    init_worker_options(R, [{auth_source, V} | Acc]);
init_worker_options([{username, V} | R], Acc) ->
    init_worker_options(R, [{login, V} | Acc]);
init_worker_options([{password, V} | R], Acc) ->
    init_worker_options(R, [{password, V} | Acc]);
init_worker_options([{w_mode, V} | R], Acc) ->
    init_worker_options(R, [{w_mode, V} | Acc]);
init_worker_options([{r_mode, V} | R], Acc) ->
    init_worker_options(R, [{r_mode, V} | Acc]);
init_worker_options([_ | R], Acc) ->
    init_worker_options(R, Acc);
init_worker_options([], Acc) ->
    Acc.

%% ===================================================================
%% Schema funcs

server(type) -> emqx_schema:ip_port();
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

w_mode(type) -> hoconsc:enum([unsafe, safe]);
w_mode(desc) -> ?DESC("w_mode");
w_mode(default) -> unsafe;
w_mode(_) -> undefined.

r_mode(type) -> hoconsc:enum([master, slave_ok]);
r_mode(desc) -> ?DESC("r_mode");
r_mode(default) -> master;
r_mode(_) -> undefined.

duration(type) -> emqx_schema:duration_ms();
duration(desc) -> ?DESC("duration");
duration(required) -> false;
duration(_) -> undefined.

max_overflow(type) -> non_neg_integer();
max_overflow(desc) -> ?DESC("max_overflow");
max_overflow(default) -> 0;
max_overflow(_) -> undefined.

replica_set_name(type) -> binary();
replica_set_name(desc) -> ?DESC("replica_set_name");
replica_set_name(required) -> true;
replica_set_name(_) -> undefined.

srv_record(type) -> boolean();
srv_record(desc) -> ?DESC("srv_record");
srv_record(default) -> false;
srv_record(_) -> undefined.

%% ===================================================================
%% Internal funcs

may_parse_srv_and_txt_records(#{server := Server} = Config) ->
    NConfig = maps:remove(server, Config),
    may_parse_srv_and_txt_records_(NConfig#{servers => [Server]});
may_parse_srv_and_txt_records(Config) ->
    may_parse_srv_and_txt_records_(Config).

may_parse_srv_and_txt_records_(
    #{
        mongo_type := Type,
        srv_record := false,
        servers := Servers
    } = Config
) ->
    case Type =:= rs andalso maps:is_key(replica_set_name, Config) =:= false of
        true ->
            error({missing_parameter, replica_set_name});
        false ->
            Config#{hosts => servers_to_bin(Servers)}
    end;
may_parse_srv_and_txt_records_(
    #{
        mongo_type := Type,
        srv_record := true,
        servers := Servers
    } = Config
) ->
    Hosts = parse_srv_records(Type, Servers),
    ExtraOpts = parse_txt_records(Type, Servers),
    maps:merge(Config#{hosts => Hosts}, ExtraOpts).

parse_srv_records(Type, Servers) ->
    Fun = fun(AccIn, {IpOrHost, _Port}) ->
        case
            inet_res:lookup(
                "_mongodb._tcp." ++
                    ip_or_host_to_string(IpOrHost),
                in,
                srv
            )
        of
            [] ->
                error(service_not_found);
            Services ->
                [
                    [server_to_bin({Host, Port}) || {_, _, Port, Host} <- Services]
                    | AccIn
                ]
        end
    end,
    Res = lists:foldl(Fun, [], Servers),
    case Type of
        single -> lists:nth(1, Res);
        _ -> Res
    end.

parse_txt_records(Type, Servers) ->
    Fields =
        case Type of
            rs -> ["authSource", "replicaSet"];
            _ -> ["authSource"]
        end,
    Fun = fun(AccIn, {IpOrHost, _Port}) ->
        case inet_res:lookup(IpOrHost, in, txt) of
            [] ->
                #{};
            [[QueryString]] ->
                case uri_string:dissect_query(QueryString) of
                    {error, _, _} ->
                        error({invalid_txt_record, invalid_query_string});
                    Options ->
                        maps:merge(AccIn, take_and_convert(Fields, Options))
                end;
            _ ->
                error({invalid_txt_record, multiple_records})
        end
    end,
    lists:foldl(Fun, #{}, Servers).

take_and_convert(Fields, Options) ->
    take_and_convert(Fields, Options, #{}).

take_and_convert([], [_ | _], _Acc) ->
    error({invalid_txt_record, invalid_option});
take_and_convert([], [], Acc) ->
    Acc;
take_and_convert([Field | More], Options, Acc) ->
    case lists:keytake(Field, 1, Options) of
        {value, {"authSource", V}, NOptions} ->
            take_and_convert(More, NOptions, Acc#{auth_source => list_to_binary(V)});
        {value, {"replicaSet", V}, NOptions} ->
            take_and_convert(More, NOptions, Acc#{replica_set_name => list_to_binary(V)});
        {value, _, _} ->
            error({invalid_txt_record, invalid_option});
        false ->
            take_and_convert(More, Options, Acc)
    end.

-spec ip_or_host_to_string(binary() | string() | tuple()) ->
    string().
ip_or_host_to_string(Ip) when is_tuple(Ip) ->
    inet:ntoa(Ip);
ip_or_host_to_string(Host) ->
    str(Host).

servers_to_bin([Server | Rest]) ->
    [server_to_bin(Server) | servers_to_bin(Rest)];
servers_to_bin([]) ->
    [].

server_to_bin({IpOrHost, Port}) ->
    iolist_to_binary(ip_or_host_to_string(IpOrHost) ++ ":" ++ integer_to_list(Port)).

%% ===================================================================
%% typereflt funcs

-spec to_server_raw(string()) ->
    {string(), pos_integer()}.
to_server_raw(Server) ->
    emqx_connector_schema_lib:parse_server(Server, ?MONGO_HOST_OPTIONS).

-spec to_servers_raw(string()) ->
    [{string(), pos_integer()}].
to_servers_raw(Servers) ->
    lists:map(
        fun(Server) ->
            emqx_connector_schema_lib:parse_server(Server, ?MONGO_HOST_OPTIONS)
        end,
        string:tokens(str(Servers), ", ")
    ).

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.
