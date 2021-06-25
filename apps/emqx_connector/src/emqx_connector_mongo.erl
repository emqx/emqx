%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("emqx_resource/include/emqx_resource_behaviour.hrl").

%% callbacks of behaviour emqx_resource
-export([ on_start/2
        , on_stop/2
        , on_query/4
        , on_health_check/2
        , on_jsonify/1
        ]).

-export([connect/1]).

-export([structs/0, fields/1]).

-export([mongo_query/5]).
%%=====================================================================
structs() -> [""].

fields("") ->
    mongodb_fields() ++
    mongodb_topology_fields() ++
    mongodb_rs_set_name_fields() ++
    emqx_connector_schema_lib:ssl_fields().

on_jsonify(Config) ->
    Config.

%% ===================================================================
on_start(InstId, #{servers := Servers,
                   mongo_type := Type,
                   database := Database,
                   pool_size := PoolSize} = Config) ->
    logger:info("starting mongodb connector: ~p, config: ~p", [InstId, Config]),
    SslOpts = init_ssl_opts(Config, InstId),
    Hosts = [string:trim(H) || H <- string:tokens(binary_to_list(Servers), ",")],
    Opts = [{type, init_type(Type, Config)},
            {hosts, Hosts},
            {pool_size, PoolSize},
            {options, init_topology_options(maps:to_list(Config), [])},
            {worker_options, init_worker_options(maps:to_list(Config), SslOpts)}],

    %% test the connection
    TestOpts = [{database, Database}] ++ host_port(hd(Hosts)),
    {ok, TestConn} = mc_worker_api:connect(TestOpts),

    PoolName = emqx_plugin_libs_pool:pool_name(InstId),
    _ = emqx_plugin_libs_pool:start_pool(PoolName, ?MODULE, Opts ++ SslOpts),
    {ok, #{pool => PoolName,
           type => Type,
           test_conn => TestConn,
           test_opts => TestOpts}}.

on_stop(InstId, #{poolname := PoolName}) ->
    logger:info("stopping mongodb connector: ~p", [InstId]),
    emqx_plugin_libs_pool:stop_pool(PoolName).

on_query(InstId, {Action, Collection, Selector, Docs}, AfterQuery, #{poolname := PoolName} = State) ->
    logger:debug("mongodb connector ~p received request: ~p, at state: ~p", [InstId, {Action, Collection, Selector, Docs}, State]),
    case Result = ecpool:pick_and_do(PoolName, {?MODULE, mongo_query, [Action, Collection, Selector, Docs]}, no_handover) of
        {error, Reason} ->
            logger:debug("mongodb connector ~p do sql query failed, request: ~p, reason: ~p", [InstId, {Action, Collection, Selector, Docs}, Reason]),
            emqx_resource:query_failed(AfterQuery);
        _ ->
            emqx_resource:query_success(AfterQuery)
    end,
    Result.

-dialyzer({nowarn_function, [on_health_check/2]}).
on_health_check(_InstId, #{test_opts := TestOpts}) ->
    case mc_worker_api:connect(TestOpts) of
        {ok, TestConn} ->
            mc_worker_api:disconnect(TestConn),
            {ok, true};
        {error, _} ->
            {ok, false}
    end.

%% ===================================================================
connect(Opts) ->
    Type = proplists:get_value(mongo_type, Opts, single),
    Hosts = proplists:get_value(hosts, Opts, []),
    Options = proplists:get_value(options, Opts, []),
    WorkerOptions = proplists:get_value(worker_options, Opts, []),
    mongo_api:connect(Type, Hosts, Options, WorkerOptions).

mongo_query(Conn, find, Collection, Selector, Docs) ->
    mongo_api:find(Conn, Collection, Selector, Docs);

%% Todo xxx
mongo_query(_Conn, _Action, _Collection, _Selector, _Docs) ->
    ok.

init_type(rs, #{rs_set_name := Name}) ->
    {rs, Name};
init_type(Type, _Opts) ->
    Type.

init_topology_options([{pool_size, Val}| R], Acc) ->
    init_topology_options(R, [{pool_size, Val}| Acc]);
init_topology_options([{max_overflow, Val}| R], Acc) ->
    init_topology_options(R, [{max_overflow, Val}| Acc]);
init_topology_options([{overflow_ttl, Val}| R], Acc) ->
    init_topology_options(R, [{overflow_ttl, Val}| Acc]);
init_topology_options([{overflow_check_period, Val}| R], Acc) ->
    init_topology_options(R, [{overflow_check_period, Val}| Acc]);
init_topology_options([{local_threshold_ms, Val}| R], Acc) ->
    init_topology_options(R, [{'localThresholdMS', Val}| Acc]);
init_topology_options([{connect_timeout_ms, Val}| R], Acc) ->
    init_topology_options(R, [{'connectTimeoutMS', Val}| Acc]);
init_topology_options([{socket_timeout_ms, Val}| R], Acc) ->
    init_topology_options(R, [{'socketTimeoutMS', Val}| Acc]);
init_topology_options([{server_selection_timeout_ms, Val}| R], Acc) ->
    init_topology_options(R, [{'serverSelectionTimeoutMS', Val}| Acc]);
init_topology_options([{wait_queue_timeout_ms, Val}| R], Acc) ->
    init_topology_options(R, [{'waitQueueTimeoutMS', Val}| Acc]);
init_topology_options([{heartbeat_frequency_ms, Val}| R], Acc) ->
    init_topology_options(R, [{'heartbeatFrequencyMS', Val}| Acc]);
init_topology_options([{min_heartbeat_frequency_ms, Val}| R], Acc) ->
    init_topology_options(R, [{'minHeartbeatFrequencyMS', Val}| Acc]);
init_topology_options([_| R], Acc) ->
    init_topology_options(R, Acc);
init_topology_options([], Acc) ->
    Acc.

init_worker_options([{database, V} | R], Acc) ->
    init_worker_options(R, [{database, V} | Acc]);
init_worker_options([{auth_source, V} | R], Acc) ->
    init_worker_options(R, [{auth_source, V} | Acc]);
init_worker_options([{login, V} | R], Acc) ->
    init_worker_options(R, [{login, V} | Acc]);
init_worker_options([{password, V} | R], Acc) ->
    init_worker_options(R, [{password, V} | Acc]);
init_worker_options([{w_mode, V} | R], Acc) ->
    init_worker_options(R, [{w_mode, V} | Acc]);
init_worker_options([{r_mode, V} | R], Acc) ->
    init_worker_options(R, [{r_mode, V} | Acc]);
init_worker_options([_ | R], Acc) ->
    init_worker_options(R, Acc);
init_worker_options([], Acc) -> Acc.

init_ssl_opts(#{ssl := true} = Config, InstId) ->
    [{ssl, true},
     {ssl_opts, emqx_plugin_libs_ssl:save_files_return_opts(Config, "connectors", InstId)}
    ];
init_ssl_opts(_Config, _InstId) ->
    [{ssl, false}].

host_port(HostPort) ->
    case string:split(HostPort, ":") of
        [Host, Port] ->
            {ok, Host1} = inet:parse_address(Host),
            [{host, Host1}, {port, list_to_integer(Port)}];
        [Host] ->
            {ok, Host1} = inet:parse_address(Host),
            [{host, Host1}]
    end.

mongodb_fields() ->
    [ {mongo_type, fun mongo_type/1}
    , {servers, fun servers/1}
    , {pool_size, fun emqx_connector_schema_lib:pool_size/1}
    , {login, fun emqx_connector_schema_lib:username/1}
    , {password, fun emqx_connector_schema_lib:password/1}
    , {auth_source, fun auth_source/1}
    , {database, fun emqx_connector_schema_lib:database/1}
    ].

mongodb_topology_fields() ->
    [ {max_overflow, fun emqx_connector_schema_lib:pool_size/1}
    , {overflow_ttl, fun duration/1}
    , {overflow_check_period, fun duration/1}
    , {local_threshold_ms, fun duration/1}
    , {connect_timeout_ms, fun duration/1}
    , {socket_timeout_ms, fun duration/1}
    , {server_selection_timeout_ms, fun duration/1}
    , {wait_queue_timeout_ms, fun duration/1}
    , {heartbeat_frequency_ms, fun duration/1}
    , {min_heartbeat_frequency_ms, fun duration/1}
    ].

mongodb_rs_set_name_fields() ->
    [ {rs_set_name, fun emqx_connector_schema_lib:database/1}
    ].

auth_source(type) -> binary();
auth_source(_) -> undefined.

servers(type) -> binary();
servers(validator) -> [?REQUIRED("the field 'servers' is required")];
servers(_) -> undefined.

mongo_type(type) -> hoconsc:enum([single, unknown, shared, rs]);
mongo_type(default) -> single;
mongo_type(_) -> undefined.

duration(type) -> emqx_schema:duration_ms();
duration(_) -> undefined.
