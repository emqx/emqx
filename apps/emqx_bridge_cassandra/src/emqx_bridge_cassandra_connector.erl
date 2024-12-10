%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_cassandra_connector).

-behaviour(emqx_resource).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").
-include("emqx_bridge_cassandra.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_trace.hrl").

%% schema
-export([roots/0, fields/1, desc/1, namespace/0]).

%% callbacks of behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3,
    on_get_channels/1,
    on_query/3,
    on_query_async/4,
    on_batch_query/3,
    on_batch_query_async/4,
    on_get_status/2,
    on_format_query_result/1
]).

%% callbacks of ecpool
-export([
    connect/1,
    prepare_cql_to_conn/2
]).

%% callbacks for query executing
-export([query/4, prepared_query/4, batch_query/3]).

-export([do_get_status/1, get_reconnect_callback_signature/1]).

-type state() ::
    #{
        pool_name := binary(),
        channels := #{}
    }.

-define(DEFAULT_SERVER_OPTION, #{default_port => ?CASSANDRA_DEFAULT_PORT}).

%%--------------------------------------------------------------------
%% schema

namespace() -> cassandra.

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    cassandra_db_fields() ++
        emqx_connector_schema_lib:ssl_fields() ++
        emqx_connector_schema_lib:prepare_statement_fields();
fields("connector") ->
    cassandra_db_fields() ++ emqx_connector_schema_lib:ssl_fields().

cassandra_db_fields() ->
    [
        {servers, servers()},
        {keyspace, fun keyspace/1},
        {pool_size, fun emqx_connector_schema_lib:pool_size/1},
        {username, fun emqx_connector_schema_lib:username/1},
        {password, emqx_connector_schema_lib:password_field()},
        {auto_reconnect, fun emqx_connector_schema_lib:auto_reconnect/1}
    ].

servers() ->
    Meta = #{desc => ?DESC("servers")},
    emqx_schema:servers_sc(Meta, ?DEFAULT_SERVER_OPTION).

keyspace(type) -> binary();
keyspace(desc) -> ?DESC("keyspace");
keyspace(required) -> true;
keyspace(_) -> undefined.

desc(config) ->
    ?DESC("config");
desc("connector") ->
    ?DESC("connector").

%%--------------------------------------------------------------------
%% callbacks for emqx_resource
resource_type() -> cassandra.

callback_mode() -> async_if_possible.

-spec on_start(binary(), hocon:config()) -> {ok, state()} | {error, _}.
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

    Options =
        maps:to_list(maps:with([username, password], Config)) ++
            [
                {nodes, Servers},
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
    case emqx_resource_pool:start(InstId, ?MODULE, Options ++ SslOpts) of
        ok ->
            {ok, #{pool_name => InstId, channels => #{}}};
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

on_add_channel(_InstId, #{channels := Channs} = OldState, ChannId, ChannConf0) ->
    #{parameters := #{cql := CQL}} = ChannConf0,
    {PrepareCQL, ParamsTokens} = emqx_placeholder:preproc_sql(CQL, '?'),
    ParsedCql = #{
        prepare_key => make_prepare_key(ChannId),
        prepare_cql => PrepareCQL,
        params_tokens => ParamsTokens
    },
    NewChanns = Channs#{ChannId => #{parsed_cql => ParsedCql, prepare_result => not_prepared}},
    {ok, OldState#{channels => NewChanns}}.

on_remove_channel(_InstanceId, #{channels := Channels} = State, ChannId) ->
    NewState = State#{channels => maps:remove(ChannId, Channels)},
    {ok, NewState}.

on_get_channel_status(InstanceId, ChannId, #{channels := Channels, pool_name := PoolName} = State) ->
    case on_get_status(InstanceId, State) of
        connected ->
            #{parsed_cql := ParsedCql} = maps:get(ChannId, Channels),
            case prepare_cql_to_cassandra(ParsedCql, PoolName) of
                {ok, _} -> connected;
                {error, Reason} -> {connecting, Reason}
            end;
        _ ->
            connecting
    end.

on_get_channels(InstanceId) ->
    emqx_bridge_v2:get_channels_for_connector(InstanceId).

-type request() ::
    % emqx_bridge.erl
    {ChannId :: binary(), Params :: map()}
    % common query
    | {query, CQL :: binary()}
    | {query, CQL :: binary(), Params :: map()}.

-spec on_query(
    emqx_resource:resource_id(),
    request(),
    state()
) -> ok | {ok, ecql:cql_result()} | {error, {recoverable_error | unrecoverable_error, term()}}.
on_query(InstId, Request, State) ->
    do_single_query(InstId, Request, sync, State).

-spec on_query_async(
    emqx_resource:resource_id(),
    request(),
    {function(), list()},
    state()
) -> ok | {error, {recoverable_error | unrecoverable_error, term()}}.
on_query_async(InstId, Request, Callback, State) ->
    do_single_query(InstId, Request, {async, Callback}, State).

do_single_query(InstId, Request, Async, #{pool_name := PoolName} = State) ->
    {Type, PreparedKeyOrCQL, Params} = parse_request_to_cql(Request),
    ?tp(
        debug,
        cassandra_connector_received_cql_query,
        #{
            connector => InstId,
            type => Type,
            params => Params,
            prepared_key_or_cql => PreparedKeyOrCQL,
            state => State
        }
    ),
    {PreparedKeyOrCQL1, Data} = proc_cql_params(Type, PreparedKeyOrCQL, Params, State),
    emqx_trace:rendered_action_template(PreparedKeyOrCQL, #{
        type => Type,
        key_or_cql => PreparedKeyOrCQL1,
        data => Data
    }),
    Res = exec_cql_query(InstId, PoolName, Type, Async, PreparedKeyOrCQL1, Data),
    handle_result(Res).

-spec on_batch_query(
    emqx_resource:resource_id(),
    [request()],
    state()
) -> ok | {error, {recoverable_error | unrecoverable_error, term()}}.
on_batch_query(InstId, Requests, State) ->
    do_batch_query(InstId, Requests, sync, State).

-spec on_batch_query_async(
    emqx_resource:resource_id(),
    [request()],
    {function(), list()},
    state()
) -> ok | {error, {recoverable_error | unrecoverable_error, term()}}.
on_batch_query_async(InstId, Requests, Callback, State) ->
    do_batch_query(InstId, Requests, {async, Callback}, State).

do_batch_query(InstId, Requests, Async, #{pool_name := PoolName} = State) ->
    CQLs =
        lists:map(
            fun(Request) ->
                {Type, PreparedKeyOrCQL, Params} = parse_request_to_cql(Request),
                proc_cql_params(Type, PreparedKeyOrCQL, Params, State)
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
    ChannelID =
        case Requests of
            [{CID, _} | _] -> CID;
            _ -> none
        end,
    emqx_trace:rendered_action_template(ChannelID, #{
        cqls => #emqx_trace_format_func_data{data = CQLs, function = fun trace_format_cql_tuples/1}
    }),
    Res = exec_cql_batch_query(InstId, PoolName, Async, CQLs),
    handle_result(Res).

trace_format_cql_tuples(Tuples) ->
    [CQL || {_, CQL} <- Tuples].

parse_request_to_cql({query, CQL}) ->
    {query, CQL, #{}};
parse_request_to_cql({query, CQL, Params}) ->
    {query, CQL, Params};
parse_request_to_cql({ChannId, Params}) ->
    {prepared_query, ChannId, Params}.

proc_cql_params(prepared_query, ChannId, Params, #{channels := Channs}) ->
    #{
        parsed_cql := #{
            prepare_key := PrepareKey,
            params_tokens := ParamsTokens
        }
    } = maps:get(ChannId, Channs),
    {PrepareKey, assign_type_for_params(proc_sql(ParamsTokens, Params))};
proc_cql_params(query, CQL, Params, _State) ->
    {CQL1, Tokens} = emqx_placeholder:preproc_sql(CQL, '?'),
    {CQL1, assign_type_for_params(proc_sql(Tokens, Params))}.

proc_sql(Tokens, Params) ->
    VarTrans = fun
        (null) -> null;
        (X) -> emqx_placeholder:sql_data(X)
    end,
    emqx_placeholder:proc_tmpl(
        Tokens,
        Params,
        #{
            return => rawlist,
            var_trans => VarTrans
        }
    ).

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

on_get_status(_InstId, #{pool_name := PoolName}) ->
    case emqx_resource_pool:health_check_workers(PoolName, fun ?MODULE:do_get_status/1) of
        true -> ?status_connected;
        false -> ?status_connecting
    end.

do_get_status(Conn) ->
    ok == element(1, ecql:query(Conn, "SELECT cluster_name FROM system.local")).

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
    %% TODO: teach `ecql` to accept 0-arity closures as passwords.
    conn_opts(Opts, [{password, emqx_secret:unwrap(Password)} | Acc]);
conn_opts([Opt | Opts], Acc) ->
    conn_opts(Opts, [Opt | Acc]).

%% this callback accepts the arg list provided to
%% ecpool:add_reconnect_callback(PoolName, {?MODULE, prepare_sql_to_conn, [Templates]})
%% so ecpool_worker can de-duplicate the callbacks based on the signature.
get_reconnect_callback_signature([#{prepare_key := PrepareKey}]) ->
    PrepareKey.

%%--------------------------------------------------------------------
%% prepare
prepare_cql_to_cassandra(ParsedCql, PoolName) ->
    case prepare_cql_to_cassandra(ecpool:workers(PoolName), ParsedCql, #{}) of
        {ok, Statement} ->
            %% prepare for reconnect
            ecpool:add_reconnect_callback(PoolName, {?MODULE, prepare_cql_to_conn, [ParsedCql]}),
            {ok, Statement};
        Error ->
            ?tp(
                error,
                cassandra_prepare_cql_failed,
                #{parsed_cql => ParsedCql, reason => Error}
            ),
            Error
    end.

prepare_cql_to_cassandra([{_Name, Worker} | T], ParsedCql, _LastSts) ->
    {ok, Conn} = ecpool_worker:client(Worker),
    case prepare_cql_to_conn(Conn, ParsedCql) of
        {ok, Statement} ->
            prepare_cql_to_cassandra(T, ParsedCql, Statement);
        Error ->
            Error
    end;
prepare_cql_to_cassandra([], _ParsedCql, LastSts) ->
    {ok, LastSts}.

prepare_cql_to_conn(Conn, #{prepare_key := PrepareKey, prepare_cql := PrepareCQL}) when
    is_pid(Conn)
->
    ?SLOG(info, #{
        msg => "cassandra_prepare_cql", prepare_key => PrepareKey, prepare_cql => PrepareCQL
    }),
    case ecql:prepare(Conn, PrepareKey, PrepareCQL) of
        {ok, Statement} ->
            {ok, Statement};
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "cassandra_prepare_cql_failed",
                worker_pid => Conn,
                name => PrepareKey,
                prepare_cql => PrepareCQL,
                reason => Reason
            }),
            Error
    end.

handle_result({error, disconnected}) ->
    {error, {recoverable_error, disconnected}};
handle_result({error, ecpool_empty}) ->
    {error, {recoverable_error, ecpool_empty}};
handle_result({error, Error}) ->
    {error, {unrecoverable_error, Error}};
handle_result(Res) ->
    Res.

on_format_query_result({ok, Result}) ->
    #{result => ok, info => Result};
on_format_query_result(Result) ->
    Result.

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

make_prepare_key(ChannId) ->
    ChannId.
