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
-export([
    connect/1,
    prepare_sql_to_conn/2,
    prepare_sql_to_conn/3,
    prepare_sql_to_conn_on_reconnect/3,
    get_reconnect_callback_signature/1
]).

-export([
    init_prepare/1,
    prepare_sql/2,
    parse_prepare_sql/3,
    parse_prepare_sql/4,
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

-type state() ::
    #{
        pool_name := binary(),
        query_templates := map()
    }.

-export_type([state/0]).

-define(BATCH_REQ_KEY(Key), {Key, batch}).
-define(SINGLE_REQ_KEY(Key), {Key, prepstmt}).

%%=====================================================================
%% Hocon schema

namespace() -> mysql.

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [{server, server()}] ++
        emqx_connector_schema_lib:relational_db_fields(#{username => #{default => <<"root">>}}) ++
        emqx_connector_schema_lib:ssl_fields().

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
    case emqx_resource_pool:start(InstId, ?MODULE, Options ++ SslOpts) of
        ok ->
            State = #{pool_name => InstId, query_templates => #{}},
            {ok, State};
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

on_query(InstId, {Key, Bindings}, State) ->
    on_query(InstId, {Key, Bindings, default_timeout}, State);
on_query(
    InstId,
    {Key, Bindings, Timeout} = Request,
    State
) ->
    case render_bindings(Key, Bindings, State) of
        {ok, RenderedRow} ->
            case on_sql_query(InstId, execute, Key, RenderedRow, Timeout, State) of
                {error, not_prepared} ->
                    case maybe_prepare_sql(Key, State) of
                        ok ->
                            ?tp(
                                mysql_connector_on_query_prepared_sql,
                                #{key => Key, bindings => Bindings}
                            ),
                            %% not return result, next loop will try again
                            on_query(InstId, Request, State);
                        {error, Reason} ->
                            ?tp(
                                error,
                                "mysql_connector_do_prepare_failed",
                                #{
                                    connector => InstId,
                                    key => Key,
                                    state => State,
                                    reason => Reason
                                }
                            ),
                            {error, Reason}
                    end;
                Result ->
                    Result
            end;
        {error, Reason} ->
            {error, Reason}
    end.

on_batch_query(
    InstId,
    BatchReq = [{Key, _} | _],
    #{query_templates := Templates} = State,
    ChannelConfig
) ->
    case Templates of
        #{?BATCH_REQ_KEY(Key) := Template} ->
            on_batch_insert(InstId, BatchReq, Template, State, ChannelConfig);
        _ ->
            {error, {unrecoverable_error, batch_select_not_implemented}}
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

on_get_status(_InstId, #{pool_name := PoolName} = _State) ->
    Opts = #{
        check_fn => fun ?MODULE:do_get_status/1,
        is_success_fn => fun
            ({ok, _, _}) -> false;
            (_) -> true
        end
    },
    emqx_resource_pool:common_health_check_workers(PoolName, Opts).

do_get_status(Conn) ->
    mysql:query(Conn, <<"SELECT count(1) AS T">>).

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
                        msg => "mysql_init_prepare_statement_failed",
                        reason => Reason
                    }),
                    %% mark the prepare_statement as failed
                    State#{prepares => {error, Reason}}
            end
    end.

maybe_prepare_sql(Key, State = #{query_templates := Templates}) ->
    case maps:is_key(?SINGLE_REQ_KEY(Key), Templates) of
        true -> prepare_sql(State);
        false -> {error, {unrecoverable_error, prepared_statement_invalid}}
    end.

prepare_sql(#{query_templates := Templates, pool_name := PoolName} = State) ->
    PrepareConnFn = maps:get(prepare_conn_fn, State, undefined),
    prepare_sql(maps:to_list(Templates), PoolName, PrepareConnFn).

prepare_sql(Templates, PoolName) ->
    prepare_sql(Templates, PoolName, undefined).

prepare_sql(Templates, PoolName, PrepareConnFn) ->
    case do_prepare_sql(Templates, PoolName, PrepareConnFn) of
        ok ->
            %% prepare for reconnect
            ecpool:add_reconnect_callback(
                PoolName, {?MODULE, prepare_sql_to_conn_on_reconnect, [Templates, PrepareConnFn]}
            ),
            ok;
        {error, R} ->
            {error, R}
    end.

do_prepare_sql(Templates, PoolName, PrepareConnFn) ->
    Conns = get_connections_from_pool(PoolName),
    prepare_sql_to_conn_list(Conns, Templates, PrepareConnFn).

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

prepare_sql_to_conn_list([], _Templates, _PrepareConnFn) ->
    ok;
prepare_sql_to_conn_list([Conn | ConnList], Templates, PrepareConnFn) ->
    case prepare_sql_to_conn(Conn, Templates, PrepareConnFn) of
        ok ->
            prepare_sql_to_conn_list(ConnList, Templates, PrepareConnFn);
        {error, R} ->
            %% rollback
            _ = [unprepare_sql_to_conn(Conn, Template) || Template <- Templates],
            {error, R}
    end.

%% This callback accepts the argument list passed to ecpool:add_reconnect_callback/2.
%% ecpool_worker uses the result to de-duplicate callbacks.
get_reconnect_callback_signature([Templates | _]) ->
    [{{ChannelID, _}, _}] = lists:filter(
        fun
            ({?SINGLE_REQ_KEY(_), _}) ->
                true;
            (_) ->
                false
        end,
        Templates
    ),
    ChannelID.

prepare_sql_to_conn_on_reconnect(Conn, Templates, PrepareConnFn) ->
    case prepare_sql_to_conn(Conn, Templates, PrepareConnFn) of
        ok ->
            ok;
        {error, Reason} = Error ->
            ?SLOG(error, #{msg => "mysql_reconnect_prepare_failed", reason => Reason}),
            ok = mysql:stop(Conn),
            Error
    end.

prepare_sql_to_conn(Conn, Templates, undefined) ->
    prepare_sql_to_conn(Conn, Templates);
prepare_sql_to_conn(Conn, Templates, PrepareConnFn) ->
    case PrepareConnFn(Conn) of
        ok -> prepare_sql_to_conn(Conn, Templates);
        {error, _} = Error -> Error
    end.

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

is_mode_name(<<>>) ->
    false;
is_mode_name(Mode) ->
    lists:all(
        fun(C) ->
            (C >= $A andalso C =< $Z) orelse
                (C >= $a andalso C =< $z) orelse
                (C >= $0 andalso C =< $9) orelse
                C =:= $_
        end,
        binary_to_list(Mode)
    ).

do_prepare_sql_to_conn(_Conn, []) ->
    ok;
do_prepare_sql_to_conn(Conn, [{?SINGLE_REQ_KEY(Key), {SQL, _RowTemplate}} | Rest]) ->
    LogMeta = #{msg => "mysql_prepare_statement", name => Key, prepare_sql => SQL},
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

unprepare_sql_to_conn(Conn, {?SINGLE_REQ_KEY(Key), _}) ->
    mysql:unprepare(Conn, Key);
unprepare_sql_to_conn(Conn, Key) when is_atom(Key) ->
    mysql:unprepare(Conn, Key);
unprepare_sql_to_conn(_Conn, _) ->
    ok.

parse_prepare_sql(Key, SQL, NeedsBatch) ->
    parse_prepare_sql(Key, SQL, NeedsBatch, #{}).

-doc """
Parse `SQL` into a prepared-statement template. When `NeedsBatch` is `true`, also
compile it into a batch insert plan.

`Opts` may set `sql_compiler`, the module that compiles the batch plan. It defaults
to `emqx_mysql_sql`. The Doris bridge passes `emqx_doris_sql`.
""".
parse_prepare_sql(Key, SQL, NeedsBatch, Opts) ->
    Template = emqx_template_sql:parse_prepstmt(SQL, #{parameters => '?'}),
    Templates0 = #{?SINGLE_REQ_KEY(Key) => Template},
    case NeedsBatch of
        true ->
            Compiler = maps:get(sql_compiler, Opts, emqx_mysql_sql),
            case parse_batch_sql(SQL, Compiler) of
                {ok, BatchPlan} ->
                    Templates = Templates0#{?BATCH_REQ_KEY(Key) => BatchPlan},
                    {ok, #{query_templates => Templates}};
                {error, _} = Error ->
                    Error
            end;
        false ->
            {ok, #{query_templates => Templates0}}
    end.

parse_batch_sql(SQL, Compiler) ->
    case emqx_utils_sql:get_statement_type(SQL) of
        insert ->
            case emqx_sql_plan:compile(Compiler, SQL) of
                {ok, Plan} ->
                    {ok, Plan};
                {error, Reason} ->
                    Error = #{
                        msg => mysql_parse_batch_sql_invalid_insert_statement,
                        sql => SQL,
                        reason => Reason
                    },
                    ?SLOG(error, Error),
                    {error, Error}
            end;
        Type ->
            Error = #{
                msg => mysql_parse_batch_sql_invalid_statement_type,
                sql => SQL,
                type => Type
            },
            ?SLOG(error, Error),
            {error, Error}
    end.

render_bindings(Key, ParamData, #{query_templates := Templates}) ->
    case Templates of
        #{?SINGLE_REQ_KEY(Key) := {_SQL, RowTemplate}} ->
            % NOTE
            % Ignoring errors here, missing variables are set to `null`.
            {Row, _Errors} = emqx_template_sql:render_prepstmt(
                RowTemplate,
                {emqx_jsonish, ParamData}
            ),
            {ok, Row};
        _ ->
            {error, {unrecoverable_error, prepared_statement_invalid}}
    end.

on_batch_insert(InstId, BatchReqs, Plan, State, ChannelConfig) ->
    DataList = [Msg || {_, Msg} <- BatchReqs],
    RenderOpts = #{
        undefined_vars_as_null => maps:get(undefined_vars_as_null, ChannelConfig, false)
    },
    case emqx_sql_plan:render_batch(Plan, DataList, RenderOpts) of
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
            ?tp(
                error,
                "mysql_connector_do_sql_query_failed",
                LogMeta#{reason => disconnected}
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
            ?tp(
                error,
                "mysql_connector_do_sql_query_failed",
                LogMeta#{reason => Reason}
            ),
            {error, {recoverable_error, Reason}};
        {error, Reason} ->
            ?tp(
                error,
                "mysql_connector_do_sql_query_failed",
                LogMeta#{reason => Reason}
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

prepare_sql_to_conn_on_reconnect_error_test() ->
    ok = meck:new(mysql, [passthrough]),
    try
        Conn = self(),
        ok = meck:expect(mysql, stop, fun(Arg) ->
            ?assertEqual(Conn, Arg),
            ok
        end),
        Error = {error, prepare_conn_failed},
        PrepareConnFn = fun(_) -> Error end,
        ?assertEqual(Error, prepare_sql_to_conn_on_reconnect(Conn, [], PrepareConnFn)),
        ?assert(meck:called(mysql, stop, [Conn]))
    after
        meck:unload(mysql)
    end.

-endif.
