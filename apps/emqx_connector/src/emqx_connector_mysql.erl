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
-module(emqx_connector_mysql).

-include("emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2
]).

%% ecpool connect & reconnect
-export([connect/1, prepare_sql_to_conn/2]).

-export([prepare_sql/2]).

-export([roots/0, fields/1]).

-export([do_get_status/1]).

-define(MYSQL_HOST_OPTIONS, #{
    default_port => ?MYSQL_DEFAULT_PORT
}).

-type prepares() :: #{atom() => binary()}.
-type params_tokens() :: #{atom() => list()}.
-type sqls() :: #{atom() => binary()}.
-type state() ::
    #{
        poolname := atom(),
        prepare_statement := prepares(),
        params_tokens := params_tokens(),
        batch_inserts := sqls(),
        batch_params_tokens := params_tokens()
    }.

%%=====================================================================
%% Hocon schema
roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [{server, server()}] ++
        add_default_username(emqx_connector_schema_lib:relational_db_fields(), []) ++
        emqx_connector_schema_lib:ssl_fields() ++
        emqx_connector_schema_lib:prepare_statement_fields().

add_default_username([{username, OrigUsernameFn} | Tail], Head) ->
    Head ++ [{username, add_default_fn(OrigUsernameFn, <<"root">>)} | Tail];
add_default_username([Field | Tail], Head) ->
    add_default_username(Tail, Head ++ [Field]).

add_default_fn(OrigFn, Default) ->
    fun
        (default) -> Default;
        (Field) -> OrigFn(Field)
    end.

server() ->
    Meta = #{desc => ?DESC("server")},
    emqx_schema:servers_sc(Meta, ?MYSQL_HOST_OPTIONS).

%% ===================================================================
callback_mode() -> always_sync.

-spec on_start(binary(), hoconsc:config()) -> {ok, state()} | {error, _}.
on_start(
    InstId,
    #{
        server := Server,
        database := DB,
        username := Username,
        pool_size := PoolSize,
        ssl := SSL
    } = Config
) ->
    {Host, Port} = emqx_schema:parse_server(Server, ?MYSQL_HOST_OPTIONS),
    ?SLOG(info, #{
        msg => "starting_mysql_connector",
        connector => InstId,
        config => emqx_misc:redact(Config)
    }),
    SslOpts =
        case maps:get(enable, SSL) of
            true ->
                [{ssl, emqx_tls_lib:to_client_opts(SSL)}];
            false ->
                []
        end,
    Options =
        maybe_add_password_opt(
            maps:get(password, Config, undefined),
            [
                {host, Host},
                {port, Port},
                {user, Username},
                {database, DB},
                {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
                {pool_size, PoolSize}
            ]
        ),

    PoolName = emqx_plugin_libs_pool:pool_name(InstId),
    Prepares = parse_prepare_sql(Config),
    State = maps:merge(#{poolname => PoolName}, Prepares),
    case emqx_plugin_libs_pool:start_pool(PoolName, ?MODULE, Options ++ SslOpts) of
        ok ->
            {ok, init_prepare(State)};
        {error, Reason} ->
            ?tp(
                mysql_connector_start_failed,
                #{error => Reason}
            ),
            {error, Reason}
    end.

maybe_add_password_opt(undefined, Options) ->
    Options;
maybe_add_password_opt(Password, Options) ->
    [{password, Password} | Options].

on_stop(InstId, #{poolname := PoolName}) ->
    ?SLOG(info, #{
        msg => "stopping_mysql_connector",
        connector => InstId
    }),
    emqx_plugin_libs_pool:stop_pool(PoolName).

on_query(InstId, {TypeOrKey, SQLOrKey}, State) ->
    on_query(InstId, {TypeOrKey, SQLOrKey, [], default_timeout}, State);
on_query(InstId, {TypeOrKey, SQLOrKey, Params}, State) ->
    on_query(InstId, {TypeOrKey, SQLOrKey, Params, default_timeout}, State);
on_query(
    InstId,
    {TypeOrKey, SQLOrKey, Params, Timeout},
    #{poolname := PoolName, prepare_statement := Prepares} = State
) ->
    MySqlFunction = mysql_function(TypeOrKey),
    {SQLOrKey2, Data} = proc_sql_params(TypeOrKey, SQLOrKey, Params, State),
    case on_sql_query(InstId, MySqlFunction, SQLOrKey2, Data, Timeout, State) of
        {error, not_prepared} ->
            case maybe_prepare_sql(SQLOrKey2, Prepares, PoolName) of
                ok ->
                    ?tp(
                        mysql_connector_on_query_prepared_sql,
                        #{type_or_key => TypeOrKey, sql_or_key => SQLOrKey, params => Params}
                    ),
                    %% not return result, next loop will try again
                    on_query(InstId, {TypeOrKey, SQLOrKey, Params, Timeout}, State);
                {error, Reason} ->
                    LogMeta = #{connector => InstId, sql => SQLOrKey, state => State},
                    ?SLOG(
                        error,
                        LogMeta#{msg => "mysql_connector_do_prepare_failed", reason => Reason}
                    ),
                    {error, Reason}
            end;
        Result ->
            Result
    end.

on_batch_query(
    InstId,
    BatchReq,
    #{batch_inserts := Inserts, batch_params_tokens := ParamsTokens} = State
) ->
    case hd(BatchReq) of
        {Key, _} ->
            case maps:get(Key, Inserts, undefined) of
                undefined ->
                    {error, {unrecoverable_error, batch_select_not_implemented}};
                InsertSQL ->
                    Tokens = maps:get(Key, ParamsTokens),
                    on_batch_insert(InstId, BatchReq, InsertSQL, Tokens, State)
            end;
        Request ->
            LogMeta = #{connector => InstId, first_request => Request, state => State},
            ?SLOG(error, LogMeta#{msg => "invalid request"}),
            {error, {unrecoverable_error, invalid_request}}
    end.

mysql_function(sql) ->
    query;
mysql_function(prepared_query) ->
    execute;
%% for bridge
mysql_function(_) ->
    mysql_function(prepared_query).

on_get_status(_InstId, #{poolname := Pool} = State) ->
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
                    connecting
            end;
        false ->
            connecting
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

connect(Options) ->
    mysql:start_link(Options).

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

maybe_prepare_sql(SQLOrKey, Prepares, PoolName) ->
    case maps:is_key(SQLOrKey, Prepares) of
        true -> prepare_sql(Prepares, PoolName);
        false -> {error, {unrecoverable_error, prepared_statement_invalid}}
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
            % FIXME: we should try to differ on transient failers and
            % syntax failures. Retrying syntax failures is not very productive.
            ?SLOG(error, LogMeta#{result => failed, reason => Reason}),
            {error, Reason}
    end.

unprepare_sql_to_conn(Conn, PrepareSqlKey) ->
    mysql:unprepare(Conn, PrepareSqlKey).

parse_prepare_sql(Config) ->
    SQL =
        case maps:get(prepare_statement, Config, undefined) of
            undefined ->
                case maps:get(sql, Config, undefined) of
                    undefined -> #{};
                    Template -> #{send_message => Template}
                end;
            Any ->
                Any
        end,
    parse_prepare_sql(maps:to_list(SQL), #{}, #{}, #{}, #{}).

parse_prepare_sql([{Key, H} | _] = L, Prepares, Tokens, BatchInserts, BatchTks) ->
    {PrepareSQL, ParamsTokens} = emqx_plugin_libs_rule:preproc_sql(H),
    parse_batch_prepare_sql(
        L, Prepares#{Key => PrepareSQL}, Tokens#{Key => ParamsTokens}, BatchInserts, BatchTks
    );
parse_prepare_sql([], Prepares, Tokens, BatchInserts, BatchTks) ->
    #{
        prepare_statement => Prepares,
        params_tokens => Tokens,
        batch_inserts => BatchInserts,
        batch_params_tokens => BatchTks
    }.

parse_batch_prepare_sql([{Key, H} | T], Prepares, Tokens, BatchInserts, BatchTks) ->
    case emqx_plugin_libs_rule:detect_sql_type(H) of
        {ok, select} ->
            parse_prepare_sql(T, Prepares, Tokens, BatchInserts, BatchTks);
        {ok, insert} ->
            case emqx_plugin_libs_rule:split_insert_sql(H) of
                {ok, {InsertSQL, Params}} ->
                    ParamsTks = emqx_plugin_libs_rule:preproc_tmpl(Params),
                    parse_prepare_sql(
                        T,
                        Prepares,
                        Tokens,
                        BatchInserts#{Key => InsertSQL},
                        BatchTks#{Key => ParamsTks}
                    );
                {error, Reason} ->
                    ?SLOG(error, #{msg => "split sql failed", sql => H, reason => Reason}),
                    parse_prepare_sql(T, Prepares, Tokens, BatchInserts, BatchTks)
            end;
        {error, Reason} ->
            ?SLOG(error, #{msg => "detect sql type failed", sql => H, reason => Reason}),
            parse_prepare_sql(T, Prepares, Tokens, BatchInserts, BatchTks)
    end.

proc_sql_params(query, SQLOrKey, Params, _State) ->
    {SQLOrKey, Params};
proc_sql_params(prepared_query, SQLOrKey, Params, _State) ->
    {SQLOrKey, Params};
proc_sql_params(TypeOrKey, SQLOrData, Params, #{params_tokens := ParamsTokens}) ->
    case maps:get(TypeOrKey, ParamsTokens, undefined) of
        undefined ->
            {SQLOrData, Params};
        Tokens ->
            {TypeOrKey, emqx_plugin_libs_rule:proc_sql(Tokens, SQLOrData)}
    end.

on_batch_insert(InstId, BatchReqs, InsertPart, Tokens, State) ->
    ValuesPart = lists:join($,, [
        emqx_placeholder:proc_param_str(Tokens, Msg, fun emqx_placeholder:quote_mysql/1)
     || {_, Msg} <- BatchReqs
    ]),
    Query = [InsertPart, <<" values ">> | ValuesPart],
    on_sql_query(InstId, query, Query, no_params, default_timeout, State).

on_sql_query(
    InstId,
    SQLFunc,
    SQLOrKey,
    Params,
    Timeout,
    #{poolname := PoolName} = State
) ->
    LogMeta = #{connector => InstId, sql => SQLOrKey, state => State},
    ?TRACE("QUERY", "mysql_connector_received", LogMeta),
    Worker = ecpool:get_client(PoolName),
    case ecpool_worker:client(Worker) of
        {ok, Conn} ->
            ?tp(
                mysql_connector_send_query,
                #{sql_func => SQLFunc, sql_or_key => SQLOrKey, data => Params}
            ),
            do_sql_query(SQLFunc, Conn, SQLOrKey, Params, Timeout, LogMeta);
        {error, disconnected} ->
            ?SLOG(
                error,
                LogMeta#{
                    msg => "mysql_connector_do_sql_query_failed",
                    reason => worker_is_disconnected
                }
            ),
            {error, {recoverable_error, disconnected}}
    end.

do_sql_query(SQLFunc, Conn, SQLOrKey, Params, Timeout, LogMeta) ->
    try mysql:SQLFunc(Conn, SQLOrKey, Params, no_filtermap_fun, Timeout) of
        {error, disconnected} ->
            ?SLOG(
                error,
                LogMeta#{msg => "mysql_connector_do_sql_query_failed", reason => disconnected}
            ),
            %% kill the poll worker to trigger reconnection
            _ = exit(Conn, restart),
            {error, {recoverable_error, disconnected}};
        {error, not_prepared} = Error ->
            ?tp(
                mysql_connector_prepare_query_failed,
                #{error => not_prepared}
            ),
            ?SLOG(
                warning,
                LogMeta#{msg => "mysql_connector_prepare_query_failed", reason => not_prepared}
            ),
            Error;
        {error, {1053, <<"08S01">>, Reason}} ->
            %% mysql sql server shutdown in progress
            ?SLOG(
                error,
                LogMeta#{msg => "mysql_connector_do_sql_query_failed", reason => Reason}
            ),
            {error, {recoverable_error, Reason}};
        {error, Reason} ->
            ?SLOG(
                error,
                LogMeta#{msg => "mysql_connector_do_sql_query_failed", reason => Reason}
            ),
            {error, {unrecoverable_error, Reason}};
        Result ->
            ?tp(
                mysql_connector_query_return,
                #{result => Result}
            ),
            Result
    catch
        error:badarg ->
            ?SLOG(
                error,
                LogMeta#{msg => "mysql_connector_invalid_params", params => Params}
            ),
            {error, {unrecoverable_error, {invalid_params, Params}}}
    end.
