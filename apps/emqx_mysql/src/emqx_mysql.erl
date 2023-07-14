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
-module(emqx_mysql).

-include_lib("emqx_connector/include/emqx_connector.hrl").
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

-type template() :: {unicode:chardata(), emqx_template:str()}.
-type state() ::
    #{
        pool_name := binary(),
        prepares := ok | {error, _},
        templates := #{{atom(), batch | prepstmt} => template()}
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
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, ?MYSQL_HOST_OPTIONS),
    ?SLOG(info, #{
        msg => "starting_mysql_connector",
        connector => InstId,
        config => emqx_utils:redact(Config)
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
    State = parse_prepare_sql(Config),
    case emqx_resource_pool:start(InstId, ?MODULE, Options ++ SslOpts) of
        ok ->
            {ok, init_prepare(State#{pool_name => InstId})};
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

on_stop(InstId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_mysql_connector",
        connector => InstId
    }),
    emqx_resource_pool:stop(InstId).

on_query(InstId, {TypeOrKey, SQLOrKey}, State) ->
    on_query(InstId, {TypeOrKey, SQLOrKey, [], default_timeout}, State);
on_query(InstId, {TypeOrKey, SQLOrKey, Params}, State) ->
    on_query(InstId, {TypeOrKey, SQLOrKey, Params, default_timeout}, State);
on_query(
    InstId,
    {TypeOrKey, SQLOrKey, Params, Timeout},
    State
) ->
    MySqlFunction = mysql_function(TypeOrKey),
    {SQLOrKey2, Data} = proc_sql_params(TypeOrKey, SQLOrKey, Params, State),
    case on_sql_query(InstId, MySqlFunction, SQLOrKey2, Data, Timeout, State) of
        {error, not_prepared} ->
            case maybe_prepare_sql(SQLOrKey2, State) of
                ok ->
                    ?tp(
                        mysql_connector_on_query_prepared_sql,
                        #{type_or_key => TypeOrKey, sql_or_key => SQLOrKey, params => Params}
                    ),
                    %% not return result, next loop will try again
                    on_query(InstId, {TypeOrKey, SQLOrKey, Params, Timeout}, State);
                {error, Reason} ->
                    ?tp(
                        error,
                        "mysql_connector_do_prepare_failed",
                        #{
                            connector => InstId,
                            sql => SQLOrKey,
                            state => State,
                            reason => Reason
                        }
                    ),
                    {error, Reason}
            end;
        Result ->
            Result
    end.

on_batch_query(
    InstId,
    BatchReq = [{Key, _} | _],
    #{query_templates := Templates} = State
) ->
    case maps:get({Key, batch}, Templates, undefined) of
        undefined ->
            {error, {unrecoverable_error, batch_select_not_implemented}};
        Template ->
            on_batch_insert(InstId, BatchReq, Template, State)
    end;
on_batch_query(
    InstId,
    BatchReq,
    State
) ->
    ?SLOG(error, #{
        msg => "invalid request",
        connector => InstId,
        request => BatchReq,
        state => State
    }),
    {error, {unrecoverable_error, invalid_request}}.

mysql_function(sql) ->
    query;
mysql_function(prepared_query) ->
    execute;
%% for bridge
mysql_function(_) ->
    mysql_function(prepared_query).

on_get_status(_InstId, #{pool_name := PoolName} = State) ->
    case emqx_resource_pool:health_check_workers(PoolName, fun ?MODULE:do_get_status/1) of
        true ->
            case do_check_prepares(State) of
                ok ->
                    connected;
                {ok, NState} ->
                    %% return new state with prepared statements
                    {connected, NState};
                {error, undefined_table} ->
                    {disconnected, State, unhealthy_target};
                {error, _Reason} ->
                    %% do not log error, it is logged in prepare_sql_to_conn
                    connecting
            end;
        false ->
            connecting
    end.

do_get_status(Conn) ->
    ok == element(1, mysql:query(Conn, <<"SELECT count(1) AS T">>)).

do_check_prepares(
    #{
        pool_name := PoolName,
        templates := #{{send_message, prepstmt} := SQL}
    }
) ->
    % it's already connected. Verify if target table still exists
    Workers = [Worker || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
    lists:foldl(
        fun
            (WorkerPid, ok) ->
                case ecpool_worker:client(WorkerPid) of
                    {ok, Conn} ->
                        case mysql:prepare(Conn, get_status, SQL) of
                            {error, {1146, _, _}} ->
                                {error, undefined_table};
                            {ok, Statement} ->
                                mysql:unprepare(Conn, Statement);
                            _ ->
                                ok
                        end;
                    _ ->
                        ok
                end;
            (_, Acc) ->
                Acc
        end,
        ok,
        Workers
    );
do_check_prepares(#{prepares := ok}) ->
    ok;
do_check_prepares(#{prepares := {error, _}} = State) ->
    %% retry to prepare
    case prepare_sql(State) of
        ok ->
            %% remove the error
            {ok, State#{prepares => ok}};
        {error, Reason} ->
            {error, Reason}
    end.

%% ===================================================================

connect(Options) ->
    mysql:start_link(Options).

init_prepare(State = #{query_templates := Templates}) ->
    case maps:size(Templates) of
        0 ->
            State#{prepares => ok};
        _ ->
            case prepare_sql(State) of
                ok ->
                    State#{prepares => ok};
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => <<"MySQL init prepare statement failed">>,
                        reason => Reason
                    }),
                    %% mark the prepare_statement as failed
                    State#{prepares => {error, Reason}}
            end
    end.

maybe_prepare_sql(SQLOrKey, State = #{query_templates := Templates}) ->
    case maps:is_key({SQLOrKey, prepstmt}, Templates) of
        true -> prepare_sql(State);
        false -> {error, {unrecoverable_error, prepared_statement_invalid}}
    end.

prepare_sql(#{query_templates := Templates, pool_name := PoolName}) ->
    prepare_sql(maps:to_list(Templates), PoolName).

prepare_sql(Templates, PoolName) ->
    case do_prepare_sql(Templates, PoolName) of
        ok ->
            %% prepare for reconnect
            ecpool:add_reconnect_callback(PoolName, {?MODULE, prepare_sql_to_conn, [Templates]}),
            ok;
        {error, R} ->
            {error, R}
    end.

do_prepare_sql(Templates, PoolName) ->
    Conns =
        [
            begin
                {ok, Conn} = ecpool_worker:client(Worker),
                Conn
            end
         || {_Name, Worker} <- ecpool:workers(PoolName)
        ],
    prepare_sql_to_conn_list(Conns, Templates).

prepare_sql_to_conn_list([], _Templates) ->
    ok;
prepare_sql_to_conn_list([Conn | ConnList], Templates) ->
    case prepare_sql_to_conn(Conn, Templates) of
        ok ->
            prepare_sql_to_conn_list(ConnList, Templates);
        {error, R} ->
            %% rollback
            _ = [unprepare_sql_to_conn(Conn, Template) || Template <- Templates],
            {error, R}
    end.

prepare_sql_to_conn(_Conn, []) ->
    ok;
prepare_sql_to_conn(Conn, [{{Key, prepstmt}, {SQL, _RowTemplate}} | Rest]) ->
    LogMeta = #{msg => "MySQL Prepare Statement", name => Key, prepare_sql => SQL},
    ?SLOG(info, LogMeta),
    _ = unprepare_sql_to_conn(Conn, Key),
    case mysql:prepare(Conn, Key, SQL) of
        {ok, _Key} ->
            ?SLOG(info, LogMeta#{result => success}),
            prepare_sql_to_conn(Conn, Rest);
        {error, {1146, _, _} = Reason} ->
            %% Target table is not created
            ?tp(mysql_undefined_table, #{}),
            ?SLOG(error, LogMeta#{result => failed, reason => Reason}),
            {error, undefined_table};
        {error, Reason} ->
            % FIXME: we should try to differ on transient failures and
            % syntax failures. Retrying syntax failures is not very productive.
            ?SLOG(error, LogMeta#{result => failed, reason => Reason}),
            {error, Reason}
    end;
prepare_sql_to_conn(Conn, [{_Key, _Template} | Rest]) ->
    prepare_sql_to_conn(Conn, Rest).

unprepare_sql_to_conn(Conn, {{Key, prepstmt}, _}) ->
    mysql:unprepare(Conn, Key);
unprepare_sql_to_conn(Conn, Key) when is_atom(Key) ->
    mysql:unprepare(Conn, Key);
unprepare_sql_to_conn(_Conn, _) ->
    ok.

parse_prepare_sql(Config) ->
    Queries =
        case Config of
            #{prepare_statement := Qs} ->
                Qs;
            #{sql := Query} ->
                #{send_message => Query};
            _ ->
                #{}
        end,
    Templates = maps:fold(fun parse_prepare_sql/3, #{}, Queries),
    #{query_templates => Templates}.

parse_prepare_sql(Key, Query, Acc) ->
    Template = emqx_template_sql:parse_prepstmt(Query, #{parameters => '?'}),
    AccNext = Acc#{{Key, prepstmt} => Template},
    parse_batch_sql(Key, Query, AccNext).

parse_batch_sql(Key, Query, Acc) ->
    case emqx_utils_sql:get_statement_type(Query) of
        insert ->
            case emqx_utils_sql:parse_insert(Query) of
                {ok, {Insert, Params}} ->
                    RowTemplate = emqx_template_sql:parse(Params),
                    Acc#{{Key, batch} => {Insert, RowTemplate}};
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "parse insert sql statement failed",
                        sql => Query,
                        reason => Reason
                    }),
                    Acc
            end;
        select ->
            Acc;
        Otherwise ->
            ?SLOG(error, #{
                msg => "invalid sql statement type",
                sql => Query,
                type => Otherwise
            }),
            Acc
    end.

proc_sql_params(query, SQLOrKey, Params, _State) ->
    {SQLOrKey, Params};
proc_sql_params(prepared_query, SQLOrKey, Params, _State) ->
    {SQLOrKey, Params};
proc_sql_params(TypeOrKey, SQLOrData, Params, #{query_templates := Templates}) ->
    case maps:get({TypeOrKey, prepstmt}, Templates, undefined) of
        undefined ->
            {SQLOrData, Params};
        {_InsertPart, RowTemplate} ->
            % NOTE: ignoring errors here, missing variables are set to `null`.
            {Row, _Errors} = emqx_template_sql:render_prepstmt(RowTemplate, SQLOrData),
            {TypeOrKey, Row}
    end.

on_batch_insert(InstId, BatchReqs, {InsertPart, RowTemplate}, State) ->
    Rows = [render_row(RowTemplate, Msg) || {_, Msg} <- BatchReqs],
    Query = [InsertPart, <<" values ">> | lists:join($,, Rows)],
    on_sql_query(InstId, query, Query, no_params, default_timeout, State).

render_row(RowTemplate, Data) ->
    % NOTE: ignoring errors here, missing variables are set to "NULL".
    {Row, _Errors} = emqx_template_sql:render(RowTemplate, Data, #{escaping => mysql}),
    Row.

on_sql_query(
    InstId,
    SQLFunc,
    SQLOrKey,
    Params,
    Timeout,
    #{pool_name := PoolName} = State
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
            ?tp(
                error,
                "mysql_connector_do_sql_query_failed",
                LogMeta#{reason => worker_is_disconnected}
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
            %% kill the pool worker to trigger reconnection
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
