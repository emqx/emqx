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
-module(emqx_connector_redis).

-include("emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_resource/include/emqx_resource_behaviour.hrl").

-export([ schema/0
        ]).

%% callbacks of behaviour emqx_resource
-export([ on_start/2
        , on_stop/2
        , on_query/4
        , on_health_check/2
        , on_jsonify/1
        , cmd/2
        ]).

-export([do_health_check/1]).

-export([connect/1]).
%%=====================================================================
schema() ->
    redis_fields() ++
    redis_sentinel_fields() ++
    emqx_connector_schema_lib:ssl_fields().

on_jsonify(Config) ->
    Config.

%% ===================================================================
on_start(InstId, #{servers := Servers,
                   redis_type := Type,
                   database := Database,
                   pool_size := PoolSize,
                   auto_reconnect := AutoReconn} = Config) ->
    logger:info("starting redis connector: ~p, config: ~p", [InstId, Config]),
    SslOpts = init_ssl_opts(Config, InstId),
    Opts = [{pool_size, PoolSize},
            {database, Database},
            {password, maps:get(password, Config, "")},
            {auto_reconnect, reconn_interval(AutoReconn)},
            {servers, Servers}],
    Options = [{options, SslOpts}, {sentinel, maps:get(sentinel, Config, undefined)}],
    PoolName = emqx_plugin_libs_pool:pool_name(InstId),
    _ = emqx_plugin_libs_pool:start_pool(PoolName, ?MODULE, Opts ++ Options),
    {ok, #{poolname => PoolName, type => Type}}.

on_stop(InstId, #{poolname := PoolName}) ->
    logger:info("stopping redis connector: ~p", [InstId]),
    emqx_plugin_libs_pool:stop_pool(PoolName).

on_query(InstId, {cmd, Command}, AfterCommand, #{poolname := PoolName, type := Type} = State) ->
    logger:debug("redis connector ~p received cmd query: ~p, at state: ~p", [InstId, Command, State]),
    case Result = ecpool:pick_and_do(PoolName, {?MODULE, cmd, [Type, Command]}, no_handover) of
        {error, Reason} ->
            logger:debug("redis connector ~p do cmd query failed, cmd: ~p, reason: ~p", [InstId, Command, Reason]),
            emqx_resource:query_failed(AfterCommand);
        _ ->
            emqx_resource:query_success(AfterCommand)
    end,
    Result.

on_health_check(_InstId, #{type := cluster, poolname := PoolName}) ->
    Workers = lists:flatten([gen_server:call(PoolPid, get_all_workers) ||
                             PoolPid <- eredis_cluster_monitor:get_all_pools(PoolName)]),
    case length(Workers) > 0 andalso lists:all(
            fun({_, Pid, _, _}) ->
                eredis_cluster_pool_worker:is_connected(Pid) =:= true
            end, Workers) of
        true -> {ok, true};
        false -> {error, false}
    end;
on_health_check(_InstId, #{poolname := PoolName} = State) ->
    emqx_plugin_libs_pool:health_check(PoolName, fun ?MODULE:do_health_check/1, State).

do_health_check(Conn) ->
    case eredis:q(Conn, ["PING"]) of
        {ok, _} -> true;
        _ -> false
    end.

reconn_interval(true) -> 15;
reconn_interval(false) -> false.

cmd(cluster, Command) ->
    eredis_cluster:q(Command);
cmd(_Type, Command) ->
    eredis:q(Command).

%% ===================================================================
connect(Opts) ->
    eredis:start_link(Opts).

init_ssl_opts(#{ssl := true} = Config, InstId) ->
    [{ssl, true},
     {ssl_opts, emqx_plugin_libs_ssl:save_files_return_opts(Config, "connectors", InstId)}
    ];
init_ssl_opts(_Config, _InstId) ->
    [{ssl, false}].

redis_fields() ->
    [ {redis_type, fun redis_type/1}
    , {servers, fun emqx_connector_schema_lib:servers/1}
    , {pool_size, fun emqx_connector_schema_lib:pool_size/1}
    , {password, fun emqx_connector_schema_lib:password/1}
    , {database, fun database/1}
    , {auto_reconnect, fun emqx_connector_schema_lib:auto_reconnect/1}
    ].

redis_sentinel_fields() ->
    [ {sentinel, fun sentinel_name/1}
    ].

sentinel_name(type) -> binary();
sentinel_name(_) -> undefined.

redis_type(type) -> hoconsc:enum([single, sentinel, cluster]);
redis_type(default) -> single;
redis_type(_) -> undefined.

database(type) -> integer();
database(default) -> 0;
database(_) -> undefined.
