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
-module(emqx_postgresql).

-include("emqx_postgresql.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("epgsql/include/epgsql.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([roots/0, fields/1]).

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

-export([connect/1]).

-export([
    query/3,
    prepared_query/3,
    execute_batch/3
]).

%% for ecpool workers usage
-export([do_get_status/1, prepare_sql_to_conn/2]).

-define(PGSQL_HOST_OPTIONS, #{
    default_port => ?PGSQL_DEFAULT_PORT
}).

-type template() :: {unicode:chardata(), emqx_template_sql:row_template()}.
-type state() ::
    #{
        pool_name := binary(),
        query_templates := #{binary() => template()},
        prepares := #{binary() => epgsql:statement()} | {error, _}
    }.

%% FIXME: add `{error, sync_required}' to `epgsql:execute_batch'
%% We want to be able to call sync if any message from the backend leaves the driver in an
%% inconsistent state needing sync.
-dialyzer({nowarn_function, [execute_batch/3]}).

%%=====================================================================

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [{server, server()}] ++
        adjust_fields(emqx_connector_schema_lib:relational_db_fields()) ++
        emqx_connector_schema_lib:ssl_fields() ++
        emqx_connector_schema_lib:prepare_statement_fields().

server() ->
    Meta = #{desc => ?DESC("server")},
    emqx_schema:servers_sc(Meta, ?PGSQL_HOST_OPTIONS).

adjust_fields(Fields) ->
    lists:map(
        fun
            ({username, Sc}) ->
                %% to please dialyzer...
                Override = #{type => hocon_schema:field_schema(Sc, type), required => true},
                {username, hocon_schema:override(Sc, Override)};
            (Field) ->
                Field
        end,
        Fields
    ).

%% ===================================================================
callback_mode() -> always_sync.

-spec on_start(binary(), hoconsc:config()) -> {ok, state()} | {error, _}.
on_start(
    InstId,
    #{
        server := Server,
        database := DB,
        username := User,
        pool_size := PoolSize,
        ssl := SSL
    } = Config
) ->
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, ?PGSQL_HOST_OPTIONS),
    ?SLOG(info, #{
        msg => "starting_postgresql_connector",
        connector => InstId,
        config => emqx_utils:redact(Config)
    }),
    SslOpts =
        case maps:get(enable, SSL) of
            true ->
                [
                    %% note: this is converted to `required' in
                    %% `conn_opts/2', and there's a boolean guard
                    %% there; if this is set to `required' here,
                    %% that'll require changing `conn_opts/2''s guard.
                    {ssl, true},
                    {ssl_opts, emqx_tls_lib:to_client_opts(SSL)}
                ];
            false ->
                [{ssl, false}]
        end,
    Options = [
        {host, Host},
        {port, Port},
        {username, User},
        {password, emqx_secret:wrap(maps:get(password, Config, ""))},
        {database, DB},
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
        {pool_size, PoolSize}
    ],
    State = parse_prepare_sql(Config),
    case emqx_resource_pool:start(InstId, ?MODULE, Options ++ SslOpts) of
        ok ->
            {ok, init_prepare(State#{pool_name => InstId, prepares => #{}})};
        {error, Reason} ->
            ?tp(
                pgsql_connector_start_failed,
                #{error => Reason}
            ),
            {error, Reason}
    end.

on_stop(InstId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_postgresql_connector",
        connector => InstId
    }),
    emqx_resource_pool:stop(InstId).

on_query(InstId, {TypeOrKey, NameOrSQL}, State) ->
    on_query(InstId, {TypeOrKey, NameOrSQL, []}, State);
on_query(
    InstId,
    {TypeOrKey, NameOrSQL, Params},
    #{pool_name := PoolName} = State
) ->
    ?SLOG(debug, #{
        msg => "postgresql_connector_received_sql_query",
        connector => InstId,
        type => TypeOrKey,
        sql => NameOrSQL,
        state => State
    }),
    Type = pgsql_query_type(TypeOrKey),
    {NameOrSQL2, Data} = proc_sql_params(TypeOrKey, NameOrSQL, Params, State),
    Res = on_sql_query(InstId, PoolName, Type, NameOrSQL2, Data),
    handle_result(Res).

pgsql_query_type(sql) ->
    query;
pgsql_query_type(query) ->
    query;
pgsql_query_type(prepared_query) ->
    prepared_query;
%% for bridge
pgsql_query_type(_) ->
    pgsql_query_type(prepared_query).

on_batch_query(
    InstId,
    [{Key, _} = Request | _] = BatchReq,
    #{pool_name := PoolName, query_templates := Templates, prepares := PrepStatements} = State
) ->
    BinKey = to_bin(Key),
    case maps:get(BinKey, Templates, undefined) of
        undefined ->
            Log = #{
                connector => InstId,
                first_request => Request,
                state => State,
                msg => "batch prepare not implemented"
            },
            ?SLOG(error, Log),
            {error, {unrecoverable_error, batch_prepare_not_implemented}};
        {_Statement, RowTemplate} ->
            PrepStatement = maps:get(BinKey, PrepStatements),
            Rows = [render_prepare_sql_row(RowTemplate, Data) || {_Key, Data} <- BatchReq],
            case on_sql_query(InstId, PoolName, execute_batch, PrepStatement, Rows) of
                {error, _Error} = Result ->
                    handle_result(Result);
                {_Column, Results} ->
                    handle_batch_result(Results, 0)
            end
    end;
on_batch_query(InstId, BatchReq, State) ->
    ?SLOG(error, #{
        connector => InstId,
        request => BatchReq,
        state => State,
        msg => "invalid request"
    }),
    {error, {unrecoverable_error, invalid_request}}.

proc_sql_params(query, SQLOrKey, Params, _State) ->
    {SQLOrKey, Params};
proc_sql_params(prepared_query, SQLOrKey, Params, _State) ->
    {SQLOrKey, Params};
proc_sql_params(TypeOrKey, SQLOrData, Params, #{query_templates := Templates}) ->
    Key = to_bin(TypeOrKey),
    case maps:get(Key, Templates, undefined) of
        undefined ->
            {SQLOrData, Params};
        {_Statement, RowTemplate} ->
            {Key, render_prepare_sql_row(RowTemplate, SQLOrData)}
    end.

on_sql_query(InstId, PoolName, Type, NameOrSQL, Data) ->
    try ecpool:pick_and_do(PoolName, {?MODULE, Type, [NameOrSQL, Data]}, no_handover) of
        {error, Reason} = Result ->
            ?tp(
                pgsql_connector_query_return,
                #{error => Reason}
            ),
            ?SLOG(
                error,
                maps:merge(
                    #{
                        msg => "postgresql_connector_do_sql_query_failed",
                        connector => InstId,
                        type => Type,
                        sql => NameOrSQL
                    },
                    translate_to_log_context(Reason)
                )
            ),
            case Reason of
                sync_required ->
                    {error, {recoverable_error, Reason}};
                ecpool_empty ->
                    {error, {recoverable_error, Reason}};
                {error, error, _, undefined_table, _, _} ->
                    {error, {unrecoverable_error, Reason}};
                _ ->
                    Result
            end;
        Result ->
            ?tp(
                pgsql_connector_query_return,
                #{result => Result}
            ),
            Result
    catch
        error:function_clause:Stacktrace ->
            ?SLOG(error, #{
                msg => "postgresql_connector_do_sql_query_failed",
                connector => InstId,
                type => Type,
                sql => NameOrSQL,
                reason => function_clause,
                stacktrace => Stacktrace
            }),
            {error, {unrecoverable_error, invalid_request}}
    end.

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
                    %% return new state indicating that we are connected but the target table is not created
                    {disconnected, State, unhealthy_target};
                {error, _Reason} ->
                    %% do not log error, it is logged in prepare_sql_to_conn
                    connecting
            end;
        false ->
            connecting
    end.

do_get_status(Conn) ->
    ok == element(1, epgsql:squery(Conn, "SELECT count(1) AS T")).

do_check_prepares(
    #{
        pool_name := PoolName,
        query_templates := #{<<"send_message">> := {SQL, _RowTemplate}}
    }
) ->
    WorkerPids = [Worker || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
    case validate_table_existence(WorkerPids, SQL) of
        ok ->
            ok;
        {error, undefined_table} ->
            {error, {undefined_table, State}}
    end;
do_check_prepares(#{prepares := Prepares}) when is_map(Prepares) ->
    ok;
do_check_prepares(#{prepares := {error, _}} = State) ->
    %% retry to prepare
    case prepare_sql(State) of
        {ok, PrepStatements} ->
            %% remove the error
            {ok, State#{prepares := PrepStatements}};
        {error, Reason} ->
            {error, Reason}
    end.

-spec validate_table_existence([pid()], binary()) -> ok | {error, undefined_table}.
validate_table_existence([WorkerPid | Rest], SQL) ->
    try ecpool_worker:client(WorkerPid) of
        {ok, Conn} ->
            case epgsql:parse2(Conn, "", SQL, []) of
                {error, {_, _, _, undefined_table, _, _}} ->
                    {error, undefined_table};
                Res when is_tuple(Res) andalso ok == element(1, Res) ->
                    ok;
                Res ->
                    ?tp(postgres_connector_bad_parse2, #{result => Res}),
                    validate_table_existence(Rest, SQL)
            end;
        _ ->
            validate_table_existence(Rest, SQL)
    catch
        exit:{noproc, _} ->
            validate_table_existence(Rest, SQL)
    end;
validate_table_existence([], _SQL) ->
    %% All workers either replied an unexpected error; we will retry
    %% on the next health check.
    ok.

%% ===================================================================

connect(Opts) ->
    Host = proplists:get_value(host, Opts),
    Username = proplists:get_value(username, Opts),
    Password = emqx_secret:unwrap(proplists:get_value(password, Opts)),
    case epgsql:connect(Host, Username, Password, conn_opts(Opts)) of
        {ok, _Conn} = Ok ->
            Ok;
        {error, Reason} ->
            {error, Reason}
    end.

query(Conn, SQL, Params) ->
    case epgsql:equery(Conn, SQL, Params) of
        {error, sync_required} = Res ->
            ok = epgsql:sync(Conn),
            Res;
        Res ->
            Res
    end.

prepared_query(Conn, Name, Params) ->
    case epgsql:prepared_query2(Conn, Name, Params) of
        {error, sync_required} = Res ->
            ok = epgsql:sync(Conn),
            Res;
        Res ->
            Res
    end.

execute_batch(Conn, Statement, Params) ->
    case epgsql:execute_batch(Conn, Statement, Params) of
        {error, sync_required} = Res ->
            ok = epgsql:sync(Conn),
            Res;
        Res ->
            Res
    end.

conn_opts(Opts) ->
    conn_opts(Opts, []).
conn_opts([], Acc) ->
    Acc;
conn_opts([Opt = {database, _} | Opts], Acc) ->
    conn_opts(Opts, [Opt | Acc]);
conn_opts([{ssl, Bool} | Opts], Acc) when is_boolean(Bool) ->
    Flag =
        case Bool of
            true -> required;
            false -> false
        end,
    conn_opts(Opts, [{ssl, Flag} | Acc]);
conn_opts([Opt = {port, _} | Opts], Acc) ->
    conn_opts(Opts, [Opt | Acc]);
conn_opts([Opt = {timeout, _} | Opts], Acc) ->
    conn_opts(Opts, [Opt | Acc]);
conn_opts([Opt = {ssl_opts, _} | Opts], Acc) ->
    conn_opts(Opts, [Opt | Acc]);
conn_opts([_Opt | Opts], Acc) ->
    conn_opts(Opts, Acc).

parse_prepare_sql(Config) ->
    Queries =
        case Config of
            #{prepare_statement := Qs} ->
                Qs;
            #{sql := Query} ->
                #{<<"send_message">> => Query};
            #{} ->
                #{}
        end,
    Templates = maps:fold(fun parse_prepare_sql/3, #{}, Queries),
    #{query_templates => Templates}.

parse_prepare_sql(Key, Query, Acc) ->
    Template = emqx_template_sql:parse_prepstmt(Query, #{parameters => '$n'}),
    Acc#{Key => Template}.

render_prepare_sql_row(RowTemplate, Data) ->
    % NOTE: ignoring errors here, missing variables will be replaced with `null`.
    {Row, _Errors} = emqx_template_sql:render_prepstmt(RowTemplate, Data),
    Row.

init_prepare(State = #{query_templates := Templates}) when map_size(Templates) == 0 ->
    State;
init_prepare(State = #{}) ->
    case prepare_sql(State) of
        {ok, PrepStatements} ->
            State#{prepares => PrepStatements};
        Error ->
            ?SLOG(error, maps:merge(
                #{msg => <<"postgresql_init_prepare_statement_failed">>},
                translate_to_log_context(Error)
            )),
            %% mark the prepares failed
            State#{prepares => Error}
    end.

prepare_sql(#{query_templates := Templates, pool_name := PoolName}) ->
    prepare_sql(maps:to_list(Templates), PoolName).

prepare_sql(Templates, PoolName) ->
    case do_prepare_sql(Templates, PoolName) of
        {ok, _Sts} = Ok ->
            %% prepare for reconnect
            ecpool:add_reconnect_callback(PoolName, {?MODULE, prepare_sql_to_conn, [Templates]}),
            Ok;
        Error ->
            Error
    end.

do_prepare_sql(Templates, PoolName) ->
    do_prepare_sql(ecpool:workers(PoolName), Templates, #{}).

do_prepare_sql([{_Name, Worker} | Rest], Templates, _LastSts) ->
    {ok, Conn} = ecpool_worker:client(Worker),
    case prepare_sql_to_conn(Conn, Templates) of
        {ok, Sts} ->
            do_prepare_sql(Rest, Templates, Sts);
        Error ->
            Error
    end;
do_prepare_sql([], _Prepares, LastSts) ->
    {ok, LastSts}.

prepare_sql_to_conn(Conn, Prepares) ->
    prepare_sql_to_conn(Conn, Prepares, #{}).

prepare_sql_to_conn(Conn, [], Statements) when is_pid(Conn) ->
    {ok, Statements};
prepare_sql_to_conn(Conn, [{Key, {SQL, _RowTemplate}} | Rest], Statements) when is_pid(Conn) ->
    LogMeta = #{msg => "PostgreSQL Prepare Statement", name => Key, sql => SQL},
    ?SLOG(info, LogMeta),
    case epgsql:parse2(Conn, Key, SQL, []) of
        {ok, Statement} ->
            prepare_sql_to_conn(Conn, Rest, Statements#{Key => Statement});
        {error, {error, error, _, undefined_table, _, _} = Error} ->
            %% Target table is not created
            ?tp(pgsql_undefined_table, #{}),
            LogMsg =
                maps:merge(
                    LogMeta#{msg => "postgresql_parse_failed"},
                    translate_to_log_context(Error)
                ),
            ?SLOG(error, LogMsg),
            {error, undefined_table};
        {error, Error} = Other ->
            LogMsg =
                maps:merge(
                    LogMeta#{msg => "postgresql_parse_failed"},
                    translate_to_log_context(Error)
                ),
            ?SLOG(error, LogMsg),
            Other
    end.

to_bin(Bin) when is_binary(Bin) ->
    Bin;
to_bin(Atom) when is_atom(Atom) ->
    erlang:atom_to_binary(Atom).

handle_result({error, {recoverable_error, _Error}} = Res) ->
    Res;
handle_result({error, {unrecoverable_error, _Error}} = Res) ->
    Res;
handle_result({error, disconnected}) ->
    {error, {recoverable_error, disconnected}};
handle_result({error, Error}) ->
    {error, {unrecoverable_error, Error}};
handle_result(Res) ->
    Res.

handle_batch_result([{ok, Count} | Rest], Acc) ->
    handle_batch_result(Rest, Acc + Count);
handle_batch_result([{error, Error} | _Rest], _Acc) ->
    {error, {unrecoverable_error, Error}};
handle_batch_result([], Acc) ->
    {ok, Acc}.

translate_to_log_context(#error{} = Reason) ->
    #error{
        severity = Severity,
        code = Code,
        codename = Codename,
        message = Message,
        extra = Extra
    } = Reason,
    #{
        driver_severity => Severity,
        driver_error_codename => Codename,
        driver_error_code => Code,
        driver_error_message => emqx_logger_textfmt:try_format_unicode(Message),
        driver_error_extra => Extra
    };
translate_to_log_context(Reason) ->
    #{reason => Reason}.
