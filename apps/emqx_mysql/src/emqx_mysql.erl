%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mysql).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/4,
    on_get_status/2,
    on_format_query_result/1
]).

%% ecpool connect & reconnect
-export([connect/1, prepare_sql_to_conn/2, get_reconnect_callback_signature/1]).

-export([
    init_prepare/1,
    prepare_sql/2,
    parse_prepare_sql/1,
    parse_prepare_sql/2,
    unprepare_sql/2
]).

-export([roots/0, fields/1, namespace/0]).

-export([do_get_status/1]).

-define(MYSQL_HOST_OPTIONS, #{
    default_port => ?MYSQL_DEFAULT_PORT
}).

%% Modes that would change how string literals are parsed, invalidating the
%% assumptions emqx_mysql_sql makes when rendering templates.
%%
%% The composite modes are listed because the server reports them *alongside*
%% their expansion: `sql_mode = ANSI' reads back as
%% "REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI",
%% so dropping only `ANSI_QUOTES' and setting the rest back re-enables it.
%% Only `ANSI' survives in MySQL 8; the rest are kept for older servers.
-define(UNSAFE_SQL_MODES, [
    <<"ANSI_QUOTES">>,
    <<"NO_BACKSLASH_ESCAPES">>,
    <<"ANSI">>,
    <<"DB2">>,
    <<"MAXDB">>,
    <<"MSSQL">>,
    <<"ORACLE">>,
    <<"POSTGRESQL">>
]).

-define(READ_SQL_MODE_QUERY, <<"SELECT @@SESSION.sql_mode">>).

-type template() :: {unicode:chardata(), emqx_template:str()} | emqx_mysql_sql:plan().
-type state() ::
    #{
        pool_name := binary(),
        prepares := ok | {error, _},
        templates := #{{atom(), batch | prepstmt} => template()},
        query_templates := map()
    }.
-export_type([state/0]).
%%=====================================================================
%% Hocon schema

namespace() -> mysql.

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [{server, server()}] ++
        add_default_username(emqx_connector_schema_lib:relational_db_fields(), []) ++
        emqx_connector_schema_lib:ssl_fields().

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
resource_type() -> mysql.

callback_mode() -> always_sync.

-spec on_start(binary(), hocon:config()) -> {ok, state()} | {error, _}.
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
    ParseServerOpts = maps:get(parse_server_opts, Config, ?MYSQL_HOST_OPTIONS),
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, ParseServerOpts),
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
    Password = maps:get(password, Config, undefined),
    BasicCapabilities = maps:get(basic_capabilities, Config, #{}),
    Options =
        lists:flatten([
            [{password, Password} || Password /= undefined],
            {basic_capabilities, BasicCapabilities},
            {host, Host},
            {port, Port},
            {user, Username},
            {database, DB},
            {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
            {pool_size, PoolSize}
        ]),
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
    #{query_templates := Templates} = State,
    ChannelConfig
) ->
    case maps:get({Key, batch}, Templates, undefined) of
        undefined ->
            {error, {unrecoverable_error, batch_select_not_implemented}};
        Template ->
            on_batch_insert(InstId, BatchReq, Template, State, ChannelConfig)
    end;
on_batch_query(
    InstId,
    BatchReq,
    State,
    _
) ->
    ?SLOG(error, #{
        msg => "invalid request",
        connector => InstId,
        request => BatchReq,
        state => State
    }),
    {error, {unrecoverable_error, invalid_request}}.

on_format_query_result({ok, ColumnNames, Rows}) ->
    #{result => ok, column_names => ColumnNames, rows => Rows};
on_format_query_result({ok, DataList}) ->
    #{result => ok, column_names_rows_list => DataList};
on_format_query_result(Result) ->
    Result.

mysql_function(sql) ->
    query;
mysql_function(prepared_query) ->
    execute;
%% for bridge
mysql_function(_) ->
    mysql_function(prepared_query).

on_get_status(_InstId, #{pool_name := PoolName} = State) ->
    Opts = #{
        check_fn => fun ?MODULE:do_get_status/1,
        is_success_fn => fun
            ({ok, _, _}) -> false;
            (_) -> true
        end,
        on_success_fn => fun() -> do_on_get_status_prepares(State) end
    },
    emqx_resource_pool:common_health_check_workers(PoolName, Opts).

do_get_status(Conn) ->
    mysql:query(Conn, <<"SELECT count(1) AS T">>).

do_on_get_status_prepares(State) ->
    case do_check_prepares(State) of
        ok ->
            ?status_connected;
        {error, undefined_table} ->
            {?status_disconnected, unhealthy_target};
        {error, Reason} ->
            %% do not log error, it is logged in prepare_sql_to_conn
            {?status_connecting, Reason}
    end.

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
do_check_prepares(_NoTemplates) ->
    ok.

%% ===================================================================

connect(Options) ->
    %% TODO: teach `tdengine` to accept 0-arity closures as passwords.
    NOptions = init_connect_opts(Options),
    mysql:start_link(NOptions).

init_connect_opts(Options) ->
    case lists:keytake(password, 1, Options) of
        {value, {password, Secret}, Rest} ->
            [{password, emqx_secret:unwrap(Secret)} | Rest];
        false ->
            Options
    end.

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
    Conns = get_connections_from_pool(PoolName),
    prepare_sql_to_conn_list(Conns, Templates).

get_connections_from_pool(PoolName) ->
    lists:map(
        fun(Worker) ->
            {ok, Conn} = ecpool_worker:client(Worker),
            Conn
        end,
        pool_workers(PoolName)
    ).

pool_workers(PoolName) ->
    lists:map(fun({_Name, Worker}) -> Worker end, ecpool:workers(PoolName)).

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

%% this callback accepts the arg list provided to
%% ecpool:add_reconnect_callback(PoolName, {?MODULE, prepare_sql_to_conn, [Templates]})
%% so ecpool_worker can de-duplicate the callbacks based on the signature.
get_reconnect_callback_signature([Templates]) ->
    [{{ChannelID, _}, _}] = lists:filter(
        fun
            ({{_, prepstmt}, _}) ->
                true;
            (_) ->
                false
        end,
        Templates
    ),
    ChannelID.

prepare_sql_to_conn(Conn, Templates) ->
    case clear_unsafe_sql_modes(Conn) of
        ok -> do_prepare_sql_to_conn(Conn, Templates);
        {error, _} = Error -> Error
    end.

%% Drop the modes listed in ?UNSAFE_SQL_MODES from the session's sql_mode.
%%
%% The filtering is done here rather than in SQL: doing it server-side needs
%% `TRIM(... FROM ...)', which Doris (MySQL wire protocol, narrower dialect)
%% cannot parse, and this connector is shared with the Doris bridge.
clear_unsafe_sql_modes(Conn) ->
    case mysql:query(Conn, ?READ_SQL_MODE_QUERY) of
        {ok, _Columns, [[Modes]]} when is_binary(Modes) ->
            case keep_safe_sql_modes(Modes) of
                Modes -> ok;
                Safe -> mysql:query(Conn, set_sql_mode_query(Safe))
            end;
        {ok, _Columns, _Rows} ->
            %% No sql_mode reported (e.g. NULL): nothing unsafe to clear.
            ok;
        {error, _} = Error ->
            Error
    end.

keep_safe_sql_modes(Modes) ->
    Kept = [
        Mode
     || Mode <- binary:split(Modes, <<",">>, [global, trim_all]),
        not lists:member(Mode, ?UNSAFE_SQL_MODES)
    ],
    iolist_to_binary(lists:join(<<",">>, Kept)).

%% The value is built from mode names the server itself just reported, and any
%% name carrying something other than `[A-Za-z0-9_]' is dropped rather than
%% quoted, so nothing attacker-controlled can reach the statement.
set_sql_mode_query(Modes) ->
    Safe = [Mode || Mode <- binary:split(Modes, <<",">>, [global, trim_all]), is_mode_name(Mode)],
    iolist_to_binary([
        "SET SESSION sql_mode = '", lists:join(<<",">>, Safe), "'"
    ]).

is_mode_name(Mode) ->
    match =:= re:run(Mode, <<"\\A[A-Za-z0-9_]+\\z">>, [{capture, none}]).

do_prepare_sql_to_conn(_Conn, []) ->
    ok;
do_prepare_sql_to_conn(Conn, [{{Key, prepstmt}, {SQL, _RowTemplate}} | Rest]) ->
    LogMeta = #{msg => "MySQL Prepare Statement", name => Key, prepare_sql => SQL},
    ?SLOG(info, LogMeta),
    _ = unprepare_sql_to_conn(Conn, Key),
    case mysql:prepare(Conn, Key, SQL) of
        {ok, _Key} ->
            ?SLOG(info, LogMeta#{result => success}),
            do_prepare_sql_to_conn(Conn, Rest);
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
do_prepare_sql_to_conn(Conn, [{_Key, _Template} | Rest]) ->
    do_prepare_sql_to_conn(Conn, Rest).

unprepare_sql(ChannelID, #{query_templates := Templates, pool_name := PoolName}) ->
    lists:foreach(
        fun(Worker) ->
            ok = ecpool_worker:remove_reconnect_callback_by_signature(Worker, ChannelID),
            case ecpool_worker:client(Worker) of
                {ok, Conn} ->
                    lists:foreach(
                        fun(Template) -> unprepare_sql_to_conn(Conn, Template) end,
                        maps:to_list(Templates)
                    );
                _ ->
                    ok
            end
        end,
        pool_workers(PoolName)
    ).

unprepare_sql_to_conn(Conn, {{Key, prepstmt}, _}) ->
    mysql:unprepare(Conn, Key);
unprepare_sql_to_conn(Conn, Key) when is_atom(Key) ->
    mysql:unprepare(Conn, Key);
unprepare_sql_to_conn(_Conn, _) ->
    ok.

parse_prepare_sql(Config) ->
    parse_prepare_sql(send_message, Config).

parse_prepare_sql(Key, Config) ->
    Queries =
        case Config of
            #{prepare_statement := Qs} ->
                Qs;
            #{sql := Query} ->
                #{Key => Query};
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
            case emqx_mysql_sql:compile(Query) of
                {ok, Plan} ->
                    Acc#{{Key, batch} => Plan};
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
        Type ->
            ?SLOG(error, #{
                msg => "invalid sql statement type",
                sql => Query,
                type => Type
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
            % NOTE
            % Ignoring errors here, missing variables are set to `null`.
            {Row, _Errors} = emqx_template_sql:render_prepstmt(
                RowTemplate,
                {emqx_jsonish, SQLOrData}
            ),
            {TypeOrKey, Row}
    end;
proc_sql_params(_TypeOrKey, SQLOrData, Params, _State) ->
    {SQLOrData, Params}.

on_batch_insert(InstId, BatchReqs, Plan, State, ChannelConfig) ->
    DataList = [Msg || {_, Msg} <- BatchReqs],
    RenderOpts = #{
        undefined_vars_as_null => maps:get(undefined_vars_as_null, ChannelConfig, false)
    },
    case emqx_mysql_sql:render_batch(Plan, DataList, RenderOpts) of
        {ok, Query} ->
            on_sql_query(InstId, query, Query, no_params, default_timeout, State);
        {error, Reason} ->
            {error, {unrecoverable_error, Reason}}
    end.

on_sql_query(InstId, SQLFunc, SQLOrKey, Params, Timeout, #{pool_name := PoolName} = State) ->
    LogMeta = #{connector => InstId, sql => SQLOrKey, state => State},
    ?TRACE("QUERY", "mysql_connector_received", LogMeta),
    ChannelID = maps:get(channel_id, State, no_channel),
    emqx_trace:rendered_action_template(
        ChannelID,
        #{
            sql_or_key => SQLOrKey,
            parameters => Params
        }
    ),
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
