%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ee_connector_cassa).

-behaviour(emqx_resource).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("emqx_ee_connector/include/emqx_ee_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% schema
-export([roots/0, fields/1]).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_batch_query/3,
    on_batch_query_async/4,
    on_get_status/2
]).

%% callbacks of ecpool
-export([
    connect/1,
    prepare_cql_to_conn/2
]).

%% callbacks for query executing
-export([query/4, prepared_query/4, batch_query/3]).

-export([do_get_status/1]).

-type prepares() :: #{atom() => binary()}.
-type params_tokens() :: #{atom() => list()}.

-type state() ::
    #{
        poolname := atom(),
        prepare_cql := prepares(),
        params_tokens := params_tokens(),
        %% returned by ecql:prepare/2
        prepare_statement := binary()
    }.

-define(DEFAULT_SERVER_OPTION, #{default_port => ?CASSANDRA_DEFAULT_PORT}).

%%--------------------------------------------------------------------
%% schema

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    cassandra_db_fields() ++
        emqx_connector_schema_lib:ssl_fields() ++
        emqx_connector_schema_lib:prepare_statement_fields().

cassandra_db_fields() ->
    [
        {servers, servers()},
        {keyspace, fun keyspace/1},
        {pool_size, fun emqx_connector_schema_lib:pool_size/1},
        {username, fun emqx_connector_schema_lib:username/1},
        {password, fun emqx_connector_schema_lib:password/1},
        {auto_reconnect, fun emqx_connector_schema_lib:auto_reconnect/1}
    ].

servers() ->
    Meta = #{desc => ?DESC("servers")},
    emqx_schema:servers_sc(Meta, ?DEFAULT_SERVER_OPTION).

keyspace(type) -> binary();
keyspace(desc) -> ?DESC("keyspace");
keyspace(required) -> true;
keyspace(_) -> undefined.

%%--------------------------------------------------------------------
%% callbacks for emqx_resource

callback_mode() -> async_if_possible.

-spec on_start(binary(), hoconsc:config()) -> {ok, state()} | {error, _}.
on_start(
    InstId,
    #{
        servers := Servers,
        keyspace := Keyspace,
        username := Username,
        pool_size := PoolSize,
        ssl := SSL
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_cassandra_connector",
        connector => InstId,
        config => emqx_misc:redact(Config)
    }),

    Options = [
        {nodes, emqx_schema:parse_servers(Servers, ?DEFAULT_SERVER_OPTION)},
        {username, Username},
        {password, emqx_secret:wrap(maps:get(password, Config, ""))},
        {keyspace, Keyspace},
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
        {pool_size, PoolSize}
    ],

    SslOpts =
        case maps:get(enable, SSL) of
            true ->
                [
                    %% note: type defined at ecql:option/0
                    {ssl, emqx_tls_lib:to_client_opts(SSL)}
                ];
            false ->
                []
        end,
    %% use InstaId of binary type as Pool name, which is supported in ecpool.
    PoolName = InstId,
    Prepares = parse_prepare_cql(Config),
    InitState = #{poolname => PoolName, prepare_statement => #{}},
    State = maps:merge(InitState, Prepares),
    case emqx_plugin_libs_pool:start_pool(PoolName, ?MODULE, Options ++ SslOpts) of
        ok ->
            {ok, init_prepare(State)};
        {error, Reason} ->
            ?tp(
                cassandra_connector_start_failed,
                #{error => Reason}
            ),
            {error, Reason}
    end.

on_stop(InstId, #{poolname := PoolName}) ->
    ?SLOG(info, #{
        msg => "stopping_cassandra_connector",
        connector => InstId
    }),
    emqx_plugin_libs_pool:stop_pool(PoolName).

-type request() ::
    % emqx_bridge.erl
    {send_message, Params :: map()}
    % common query
    | {query, SQL :: binary()}
    | {query, SQL :: binary(), Params :: map()}.

-spec on_query(
    emqx_resource:resource_id(),
    request(),
    state()
) -> ok | {ok, ecql:cql_result()} | {error, {recoverable_error | unrecoverable_error, term()}}.
on_query(
    InstId,
    Request,
    State
) ->
    do_signle_query(InstId, Request, sync, State).

-spec on_query_async(
    emqx_resource:resource_id(),
    request(),
    {function(), list()},
    state()
) -> ok | {error, {recoverable_error | unrecoverable_error, term()}}.
on_query_async(
    InstId,
    Request,
    Callback,
    State
) ->
    do_signle_query(InstId, Request, {async, Callback}, State).

do_signle_query(
    InstId,
    Request,
    Async,
    #{poolname := PoolName} = State
) ->
    {Type, PreparedKeyOrSQL, Params} = parse_request_to_cql(Request),
    ?tp(
        debug,
        cassandra_connector_received_cql_query,
        #{
            connector => InstId,
            type => Type,
            params => Params,
            prepared_key_or_cql => PreparedKeyOrSQL,
            state => State
        }
    ),
    {PreparedKeyOrSQL1, Data} = proc_cql_params(Type, PreparedKeyOrSQL, Params, State),
    Res = exec_cql_query(InstId, PoolName, Type, Async, PreparedKeyOrSQL1, Data),
    handle_result(Res).

-spec on_batch_query(
    emqx_resource:resource_id(),
    [request()],
    state()
) -> ok | {error, {recoverable_error | unrecoverable_error, term()}}.
on_batch_query(
    InstId,
    Requests,
    State
) ->
    do_batch_query(InstId, Requests, sync, State).

-spec on_batch_query_async(
    emqx_resource:resource_id(),
    [request()],
    {function(), list()},
    state()
) -> ok | {error, {recoverable_error | unrecoverable_error, term()}}.
on_batch_query_async(
    InstId,
    Requests,
    Callback,
    State
) ->
    do_batch_query(InstId, Requests, {async, Callback}, State).

do_batch_query(
    InstId,
    Requests,
    Async,
    #{poolname := PoolName} = State
) ->
    CQLs =
        lists:map(
            fun(Request) ->
                {Type, PreparedKeyOrSQL, Params} = parse_request_to_cql(Request),
                proc_cql_params(Type, PreparedKeyOrSQL, Params, State)
            end,
            Requests
        ),
    ?tp(
        debug,
        cassandra_connector_received_cql_batch_query,
        #{
            connector => InstId,
            cqls => CQLs,
            state => State
        }
    ),
    Res = exec_cql_batch_query(InstId, PoolName, Async, CQLs),
    handle_result(Res).

parse_request_to_cql({send_message, Params}) ->
    {prepared_query, _Key = send_message, Params};
parse_request_to_cql({query, SQL}) ->
    parse_request_to_cql({query, SQL, #{}});
parse_request_to_cql({query, SQL, Params}) ->
    {query, SQL, Params}.

proc_cql_params(
    prepared_query,
    PreparedKey0,
    Params,
    #{prepare_statement := Prepares, params_tokens := ParamsTokens}
) ->
    %% assert
    _PreparedKey = maps:get(PreparedKey0, Prepares),
    Tokens = maps:get(PreparedKey0, ParamsTokens),
    {PreparedKey0, assign_type_for_params(emqx_plugin_libs_rule:proc_sql(Tokens, Params))};
proc_cql_params(query, SQL, Params, _State) ->
    {SQL1, Tokens} = emqx_plugin_libs_rule:preproc_sql(SQL, '?'),
    {SQL1, assign_type_for_params(emqx_plugin_libs_rule:proc_sql(Tokens, Params))}.

exec_cql_query(InstId, PoolName, Type, Async, PreparedKey, Data) when
    Type == query; Type == prepared_query
->
    case ecpool:pick_and_do(PoolName, {?MODULE, Type, [Async, PreparedKey, Data]}, no_handover) of
        {error, Reason} = Result ->
            ?tp(
                error,
                cassandra_connector_query_return,
                #{connector => InstId, error => Reason}
            ),
            Result;
        Result ->
            ?tp(debug, cassandra_connector_query_return, #{result => Result}),
            Result
    end.

exec_cql_batch_query(InstId, PoolName, Async, CQLs) ->
    case ecpool:pick_and_do(PoolName, {?MODULE, batch_query, [Async, CQLs]}, no_handover) of
        {error, Reason} = Result ->
            ?tp(
                error,
                cassandra_connector_query_return,
                #{connector => InstId, error => Reason}
            ),
            Result;
        Result ->
            ?tp(debug, cassandra_connector_query_return, #{result => Result}),
            Result
    end.

on_get_status(_InstId, #{poolname := Pool} = State) ->
    case emqx_plugin_libs_pool:health_check_ecpool_workers(Pool, fun ?MODULE:do_get_status/1) of
        true ->
            case do_check_prepares(State) of
                ok ->
                    connected;
                {ok, NState} ->
                    %% return new state with prepared statements
                    {connected, NState};
                false ->
                    %% do not log error, it is logged in prepare_cql_to_conn
                    connecting
            end;
        false ->
            connecting
    end.

do_get_status(Conn) ->
    ok == element(1, ecql:query(Conn, "SELECT count(1) AS T FROM system.local")).

do_check_prepares(#{prepare_cql := Prepares}) when is_map(Prepares) ->
    ok;
do_check_prepares(State = #{poolname := PoolName, prepare_cql := {error, Prepares}}) ->
    %% retry to prepare
    case prepare_cql(Prepares, PoolName) of
        {ok, Sts} ->
            %% remove the error
            {ok, State#{prepare_cql => Prepares, prepare_statement := Sts}};
        _Error ->
            false
    end.

%%--------------------------------------------------------------------
%% callbacks query

query(Conn, sync, CQL, Params) ->
    ecql:query(Conn, CQL, Params);
query(Conn, {async, Callback}, CQL, Params) ->
    ecql:async_query(Conn, CQL, Params, one, Callback).

prepared_query(Conn, sync, PreparedKey, Params) ->
    ecql:execute(Conn, PreparedKey, Params);
prepared_query(Conn, {async, Callback}, PreparedKey, Params) ->
    ecql:async_execute(Conn, PreparedKey, Params, Callback).

batch_query(Conn, sync, Rows) ->
    ecql:batch(Conn, Rows);
batch_query(Conn, {async, Callback}, Rows) ->
    ecql:async_batch(Conn, Rows, Callback).

%%--------------------------------------------------------------------
%% callbacks for ecpool

connect(Opts) ->
    case ecql:connect(conn_opts(Opts)) of
        {ok, _Conn} = Ok ->
            Ok;
        {error, Reason} ->
            {error, Reason}
    end.

conn_opts(Opts) ->
    conn_opts(Opts, []).

conn_opts([], Acc) ->
    Acc;
conn_opts([{password, Password} | Opts], Acc) ->
    conn_opts(Opts, [{password, emqx_secret:unwrap(Password)} | Acc]);
conn_opts([Opt | Opts], Acc) ->
    conn_opts(Opts, [Opt | Acc]).

%%--------------------------------------------------------------------
%% prepare

%% XXX: hardcode
%% note: the `cql` param is passed by emqx_ee_bridge_cassa
parse_prepare_cql(#{cql := SQL}) ->
    parse_prepare_cql([{send_message, SQL}], #{}, #{});
parse_prepare_cql(_) ->
    #{prepare_cql => #{}, params_tokens => #{}}.

parse_prepare_cql([{Key, H} | T], Prepares, Tokens) ->
    {PrepareSQL, ParamsTokens} = emqx_plugin_libs_rule:preproc_sql(H, '?'),
    parse_prepare_cql(
        T, Prepares#{Key => PrepareSQL}, Tokens#{Key => ParamsTokens}
    );
parse_prepare_cql([], Prepares, Tokens) ->
    #{
        prepare_cql => Prepares,
        params_tokens => Tokens
    }.

init_prepare(State = #{prepare_cql := Prepares, poolname := PoolName}) ->
    case maps:size(Prepares) of
        0 ->
            State;
        _ ->
            case prepare_cql(Prepares, PoolName) of
                {ok, Sts} ->
                    State#{prepare_statement := Sts};
                Error ->
                    ?tp(
                        error,
                        cassandra_prepare_cql_failed,
                        #{prepares => Prepares, reason => Error}
                    ),
                    %% mark the prepare_cql as failed
                    State#{prepare_cql => {error, Prepares}}
            end
    end.

prepare_cql(Prepares, PoolName) when is_map(Prepares) ->
    prepare_cql(maps:to_list(Prepares), PoolName);
prepare_cql(Prepares, PoolName) ->
    case do_prepare_cql(Prepares, PoolName) of
        {ok, _Sts} = Ok ->
            %% prepare for reconnect
            ecpool:add_reconnect_callback(PoolName, {?MODULE, prepare_cql_to_conn, [Prepares]}),
            Ok;
        Error ->
            Error
    end.

do_prepare_cql(Prepares, PoolName) ->
    do_prepare_cql(ecpool:workers(PoolName), Prepares, PoolName, #{}).

do_prepare_cql([{_Name, Worker} | T], Prepares, PoolName, _LastSts) ->
    {ok, Conn} = ecpool_worker:client(Worker),
    case prepare_cql_to_conn(Conn, Prepares) of
        {ok, Sts} ->
            do_prepare_cql(T, Prepares, PoolName, Sts);
        Error ->
            Error
    end;
do_prepare_cql([], _Prepares, _PoolName, LastSts) ->
    {ok, LastSts}.

prepare_cql_to_conn(Conn, Prepares) ->
    prepare_cql_to_conn(Conn, Prepares, #{}).

prepare_cql_to_conn(Conn, [], Statements) when is_pid(Conn) -> {ok, Statements};
prepare_cql_to_conn(Conn, [{Key, SQL} | PrepareList], Statements) when is_pid(Conn) ->
    ?SLOG(info, #{msg => "cassandra_prepare_cql", name => Key, prepare_cql => SQL}),
    case ecql:prepare(Conn, Key, SQL) of
        {ok, Statement} ->
            prepare_cql_to_conn(Conn, PrepareList, Statements#{Key => Statement});
        {error, Error} = Other ->
            ?SLOG(error, #{
                msg => "cassandra_prepare_cql_failed",
                worker_pid => Conn,
                name => Key,
                prepare_cql => SQL,
                error => Error
            }),
            Other
    end.

handle_result({error, disconnected}) ->
    {error, {recoverable_error, disconnected}};
handle_result({error, Error}) ->
    {error, {unrecoverable_error, Error}};
handle_result(Res) ->
    Res.

%%--------------------------------------------------------------------
%% utils

%% see ecql driver requirements
assign_type_for_params(Params) ->
    assign_type_for_params(Params, []).

assign_type_for_params([], Acc) ->
    lists:reverse(Acc);
assign_type_for_params([Param | More], Acc) ->
    assign_type_for_params(More, [maybe_assign_type(Param) | Acc]).

maybe_assign_type(true) ->
    {int, 1};
maybe_assign_type(false) ->
    {int, 0};
maybe_assign_type(V) when is_binary(V); is_list(V); is_atom(V) -> V;
maybe_assign_type(V) when is_integer(V) ->
    %% The max value of signed int(4) is 2147483647
    case V > 2147483647 orelse V < -2147483647 of
        true -> {bigint, V};
        false -> {int, V}
    end;
maybe_assign_type(V) when is_float(V) -> {double, V};
maybe_assign_type(V) ->
    V.
