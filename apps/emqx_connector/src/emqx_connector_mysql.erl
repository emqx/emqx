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
-module(emqx_connector_mysql).

-include("emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    on_start/2,
    on_stop/2,
    on_query/4,
    on_get_status/2
]).

%% ecpool connect & reconnect
-export([connect/1, prepare_sql_to_conn/2]).

-export([prepare_sql/2]).

-export([roots/0, fields/1]).

-export([do_get_status/1]).

-define(MYSQL_HOST_OPTIONS, #{
    host_type => inet_addr,
    default_port => ?MYSQL_DEFAULT_PORT
}).

%%=====================================================================
%% Hocon schema
roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [{server, fun server/1}] ++
        emqx_connector_schema_lib:relational_db_fields() ++
        emqx_connector_schema_lib:ssl_fields() ++
        emqx_connector_schema_lib:prepare_statement_fields().

server(type) -> emqx_schema:ip_port();
server(required) -> true;
server(validator) -> [?NOT_EMPTY("the value of the field 'server' cannot be empty")];
server(converter) -> fun to_server/1;
server(desc) -> ?DESC("server");
server(_) -> undefined.

%% ===================================================================
on_start(
    InstId,
    #{
        server := {Host, Port},
        database := DB,
        username := User,
        password := Password,
        auto_reconnect := AutoReconn,
        pool_size := PoolSize,
        ssl := SSL
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_mysql_connector",
        connector => InstId,
        config => Config
    }),
    SslOpts =
        case maps:get(enable, SSL) of
            true ->
                [{ssl, emqx_tls_lib:to_client_opts(SSL)}];
            false ->
                []
        end,
    Options = [
        {host, Host},
        {port, Port},
        {user, User},
        {password, Password},
        {database, DB},
        {auto_reconnect, reconn_interval(AutoReconn)},
        {pool_size, PoolSize}
    ],
    PoolName = emqx_plugin_libs_pool:pool_name(InstId),
    Prepares = maps:get(prepare_statement, Config, #{}),
    State = #{poolname => PoolName, prepare_statement => Prepares, auto_reconnect => AutoReconn},
    case emqx_plugin_libs_pool:start_pool(PoolName, ?MODULE, Options ++ SslOpts) of
        ok -> {ok, init_prepare(State)};
        {error, Reason} -> {error, Reason}
    end.

on_stop(InstId, #{poolname := PoolName}) ->
    ?SLOG(info, #{
        msg => "stopping_mysql_connector",
        connector => InstId
    }),
    emqx_plugin_libs_pool:stop_pool(PoolName).

on_query(InstId, {Type, SQLOrKey}, AfterQuery, State) ->
    on_query(InstId, {Type, SQLOrKey, [], default_timeout}, AfterQuery, State);
on_query(InstId, {Type, SQLOrKey, Params}, AfterQuery, State) ->
    on_query(InstId, {Type, SQLOrKey, Params, default_timeout}, AfterQuery, State);
on_query(
    InstId,
    {Type, SQLOrKey, Params, Timeout},
    AfterQuery,
    #{poolname := PoolName, prepare_statement := Prepares} = State
) ->
    LogMeta = #{connector => InstId, sql => SQLOrKey, state => State},
    ?TRACE("QUERY", "mysql_connector_received", LogMeta),
    Worker = ecpool:get_client(PoolName),
    {ok, Conn} = ecpool_worker:client(Worker),
    MySqlFunction = mysql_function(Type),
    Result = erlang:apply(mysql, MySqlFunction, [Conn, SQLOrKey, Params, Timeout]),
    case Result of
        {error, disconnected} ->
            ?SLOG(
                error,
                LogMeta#{msg => "mysql_connector_do_sql_query_failed", reason => disconnected}
            ),
            %% kill the poll worker to trigger reconnection
            _ = exit(Conn, restart),
            emqx_resource:query_failed(AfterQuery),
            Result;
        {error, not_prepared} ->
            ?SLOG(
                warning,
                LogMeta#{msg => "mysql_connector_prepare_query_failed", reason => not_prepared}
            ),
            case prepare_sql(Prepares, PoolName) of
                ok ->
                    %% not return result, next loop will try again
                    on_query(InstId, {Type, SQLOrKey, Params, Timeout}, AfterQuery, State);
                {error, Reason} ->
                    ?SLOG(
                        error,
                        LogMeta#{msg => "mysql_connector_do_prepare_failed", reason => Reason}
                    ),
                    emqx_resource:query_failed(AfterQuery),
                    {error, Reason}
            end;
        {error, Reason} ->
            ?SLOG(
                error,
                LogMeta#{msg => "mysql_connector_do_sql_query_failed", reason => Reason}
            ),
            emqx_resource:query_failed(AfterQuery),
            Result;
        _ ->
            emqx_resource:query_success(AfterQuery),
            Result
    end.

mysql_function(sql) -> query;
mysql_function(prepared_query) -> execute.

on_get_status(_InstId, #{poolname := Pool, auto_reconnect := AutoReconn} = State) ->
    case emqx_plugin_libs_pool:health_check_ecpool_workers(Pool, fun ?MODULE:do_get_status/1) of
        true ->
            case do_check_prepares(State) of
                ok ->
                    connected;
                {ok, NState} ->
                    %% return new state with prepared statements
                    {connected, NState};
                {error, _Reason} ->
                    %% do not log error, it is logged in prepare_sql_to_conn
                    conn_status(AutoReconn)
            end;
        false ->
            conn_status(AutoReconn)
    end.

do_get_status(Conn) ->
    ok == element(1, mysql:query(Conn, <<"SELECT count(1) AS T">>)).

do_check_prepares(#{prepare_statement := Prepares}) when is_map(Prepares) ->
    ok;
do_check_prepares(State = #{poolname := PoolName, prepare_statement := {error, Prepares}}) ->
    %% retry to prepare
    case prepare_sql(Prepares, PoolName) of
        ok ->
            %% remove the error
            {ok, State#{prepare_statement => Prepares}};
        {error, Reason} ->
            {error, Reason}
    end.

%% ===================================================================
conn_status(_AutoReconn = true) -> connecting;
conn_status(_AutoReconn = false) -> disconnected.

reconn_interval(true) -> 15;
reconn_interval(false) -> false.

connect(Options) ->
    mysql:start_link(Options).

-spec to_server(string()) ->
    {inet:ip_address() | inet:hostname(), pos_integer()}.
to_server(Str) ->
    emqx_connector_schema_lib:parse_server(Str, ?MYSQL_HOST_OPTIONS).

init_prepare(State = #{prepare_statement := Prepares, poolname := PoolName}) ->
    case maps:size(Prepares) of
        0 ->
            State;
        _ ->
            case prepare_sql(Prepares, PoolName) of
                ok ->
                    State;
                {error, Reason} ->
                    LogMeta = #{msg => <<"MySQL init prepare statement failed">>, reason => Reason},
                    ?SLOG(error, LogMeta),
                    %% mark the prepare_statement as failed
                    State#{prepare_statement => {error, Prepares}}
            end
    end.

prepare_sql(Prepares, PoolName) when is_map(Prepares) ->
    prepare_sql(maps:to_list(Prepares), PoolName);
prepare_sql(Prepares, PoolName) ->
    case do_prepare_sql(Prepares, PoolName) of
        ok ->
            %% prepare for reconnect
            ecpool:add_reconnect_callback(PoolName, {?MODULE, prepare_sql_to_conn, [Prepares]}),
            ok;
        {error, R} ->
            {error, R}
    end.

do_prepare_sql(Prepares, PoolName) ->
    Conns =
        [
            begin
                {ok, Conn} = ecpool_worker:client(Worker),
                Conn
            end
         || {_Name, Worker} <- ecpool:workers(PoolName)
        ],
    prepare_sql_to_conn_list(Conns, Prepares).

prepare_sql_to_conn_list([], _PrepareList) ->
    ok;
prepare_sql_to_conn_list([Conn | ConnList], PrepareList) ->
    case prepare_sql_to_conn(Conn, PrepareList) of
        ok ->
            prepare_sql_to_conn_list(ConnList, PrepareList);
        {error, R} ->
            %% rollback
            Fun = fun({Key, _}) ->
                _ = unprepare_sql_to_conn(Conn, Key),
                ok
            end,
            lists:foreach(Fun, PrepareList),
            {error, R}
    end.

prepare_sql_to_conn(Conn, []) when is_pid(Conn) -> ok;
prepare_sql_to_conn(Conn, [{Key, SQL} | PrepareList]) when is_pid(Conn) ->
    LogMeta = #{msg => "MySQL Prepare Statement", name => Key, prepare_sql => SQL},
    ?SLOG(info, LogMeta),
    _ = unprepare_sql_to_conn(Conn, Key),
    case mysql:prepare(Conn, Key, SQL) of
        {ok, _Key} ->
            ?SLOG(info, LogMeta#{result => success}),
            prepare_sql_to_conn(Conn, PrepareList);
        {error, Reason} ->
            ?SLOG(error, LogMeta#{result => failed, reason => Reason}),
            {error, Reason}
    end.

unprepare_sql_to_conn(Conn, PrepareSqlKey) ->
    mysql:unprepare(Conn, PrepareSqlKey).
