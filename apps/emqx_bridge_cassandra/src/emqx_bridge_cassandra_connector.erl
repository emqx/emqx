%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_cassandra_connector).

-behaviour(emqx_resource).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include("emqx_bridge_cassandra.hrl").
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
        pool_name := binary(),
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
        servers := Servers0,
        keyspace := Keyspace,
        pool_size := PoolSize,
        ssl := SSL
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_cassandra_connector",
        connector => InstId,
        config => emqx_utils:redact(Config)
    }),
    Servers =
        lists:map(
            fun(#{hostname := Host, port := Port}) ->
                {Host, Port}
            end,
            emqx_schema:parse_servers(Servers0, ?DEFAULT_SERVER_OPTION)
        ),

    Options = [
        {nodes, Servers},
        {keyspace, Keyspace},
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
        {pool_size, PoolSize}
    ],
    Options1 = maybe_add_opt(username, Config, Options),
    Options2 = maybe_add_opt(password, Config, Options1, _IsSensitive = true),

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
    State = parse_prepare_cql(Config),
    case emqx_resource_pool:start(InstId, ?MODULE, Options2 ++ SslOpts) of
        ok ->
            {ok, init_prepare(State#{pool_name => InstId, prepare_statement => #{}})};
        {error, Reason} ->
            ?tp(
                cassandra_connector_start_failed,
                #{error => Reason}
            ),
            {error, Reason}
    end.

on_stop(InstId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_cassandra_connector",
        connector => InstId
    }),
    emqx_resource_pool:stop(InstId).

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
    do_single_query(InstId, Request, sync, State).

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
    do_single_query(InstId, Request, {async, Callback}, State).

do_single_query(
    InstId,
    Request,
    Async,
    #{pool_name := PoolName} = State
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
    #{pool_name := PoolName} = State
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
    {PreparedKey0, assign_type_for_params(emqx_placeholder:proc_sql(Tokens, Params))};
proc_cql_params(query, SQL, Params, _State) ->
    {SQL1, Tokens} = emqx_placeholder:preproc_sql(SQL, '?'),
    {SQL1, assign_type_for_params(emqx_placeholder:proc_sql(Tokens, Params))}.

exec_cql_query(InstId, PoolName, Type, Async, PreparedKey, Data) when
    Type == query; Type == prepared_query
->
    case exec(PoolName, {?MODULE, Type, [Async, PreparedKey, Data]}) of
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
    case exec(PoolName, {?MODULE, batch_query, [Async, CQLs]}) of
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

%% Pick one of the pool members to do the query.
%% Using 'no_handoever' strategy,
%% meaning the buffer worker does the gen_server call or gen_server cast
%% towards the connection process.
exec(PoolName, Query) ->
    ecpool:pick_and_do(PoolName, Query, no_handover).

on_get_status(_InstId, #{pool_name := PoolName} = State) ->
    case emqx_resource_pool:health_check_workers(PoolName, fun ?MODULE:do_get_status/1) of
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
    ok == element(1, ecql:query(Conn, "SELECT cluster_name FROM system.local")).

do_check_prepares(#{prepare_cql := Prepares}) when is_map(Prepares) ->
    ok;
do_check_prepares(State = #{pool_name := PoolName, prepare_cql := {error, Prepares}}) ->
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
    ok = ecql:async_query(Conn, CQL, Params, one, Callback),
    %% return the connection pid for buffer worker to monitor
    {ok, Conn}.

prepared_query(Conn, sync, PreparedKey, Params) ->
    ecql:execute(Conn, PreparedKey, Params);
prepared_query(Conn, {async, Callback}, PreparedKey, Params) ->
    ok = ecql:async_execute(Conn, PreparedKey, Params, Callback),
    %% return the connection pid for buffer worker to monitor
    {ok, Conn}.

batch_query(Conn, sync, Rows) ->
    ecql:batch(Conn, Rows);
batch_query(Conn, {async, Callback}, Rows) ->
    ok = ecql:async_batch(Conn, Rows, Callback),
    %% return the connection pid for buffer worker to monitor
    {ok, Conn}.

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
%% note: the `cql` param is passed by emqx_bridge_cassandra
parse_prepare_cql(#{cql := SQL}) ->
    parse_prepare_cql([{send_message, SQL}], #{}, #{});
parse_prepare_cql(_) ->
    #{prepare_cql => #{}, params_tokens => #{}}.

parse_prepare_cql([{Key, H} | T], Prepares, Tokens) ->
    {PrepareSQL, ParamsTokens} = emqx_placeholder:preproc_sql(H, '?'),
    parse_prepare_cql(
        T, Prepares#{Key => PrepareSQL}, Tokens#{Key => ParamsTokens}
    );
parse_prepare_cql([], Prepares, Tokens) ->
    #{
        prepare_cql => Prepares,
        params_tokens => Tokens
    }.

init_prepare(State = #{prepare_cql := Prepares, pool_name := PoolName}) ->
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
    do_prepare_cql(ecpool:workers(PoolName), Prepares, #{}).

do_prepare_cql([{_Name, Worker} | T], Prepares, _LastSts) ->
    {ok, Conn} = ecpool_worker:client(Worker),
    case prepare_cql_to_conn(Conn, Prepares) of
        {ok, Sts} ->
            do_prepare_cql(T, Prepares, Sts);
        Error ->
            Error
    end;
do_prepare_cql([], _Prepares, LastSts) ->
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
handle_result({error, ecpool_empty}) ->
    {error, {recoverable_error, ecpool_empty}};
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

maybe_add_opt(Key, Conf, Opts) ->
    maybe_add_opt(Key, Conf, Opts, _IsSensitive = false).

maybe_add_opt(Key, Conf, Opts, IsSensitive) ->
    case Conf of
        #{Key := Val} ->
            [{Key, maybe_wrap(IsSensitive, Val)} | Opts];
        _ ->
            Opts
    end.

maybe_wrap(false = _IsSensitive, Val) ->
    Val;
maybe_wrap(true, Val) ->
    emqx_secret:wrap(Val).
