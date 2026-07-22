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
    parse_prepare_sql/3,
    unprepare_sql/2
]).

-export([roots/0, fields/1, namespace/0]).

-export([do_get_status/1]).

-define(MYSQL_HOST_OPTIONS, #{
    default_port => ?MYSQL_DEFAULT_PORT
}).

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
            ({?SINGLE_REQ_KEY(_), _}) ->
                true;
            (_) ->
                false
        end,
        Templates
    ),
    ChannelID.

prepare_sql_to_conn(_Conn, []) ->
    ok;
prepare_sql_to_conn(Conn, [{?SINGLE_REQ_KEY(Key), {SQL, _RowTemplate}} | Rest]) ->
    LogMeta = #{msg => "mysql_prepare_statement", name => Key, prepare_sql => SQL},
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
    Template = emqx_template_sql:parse_prepstmt(SQL, #{parameters => '?'}),
    Templates0 = #{?SINGLE_REQ_KEY(Key) => Template},
    case NeedsBatch of
        true ->
            case parse_batch_sql(SQL) of
                {ok, BatchTemplate} ->
                    Templates = Templates0#{?BATCH_REQ_KEY(Key) => BatchTemplate},
                    {ok, #{query_templates => Templates}};
                {error, _} = Error ->
                    Error
            end;
        false ->
            {ok, #{query_templates => Templates0}}
    end.

parse_batch_sql(SQL) ->
    case emqx_utils_sql:get_statement_type(SQL) of
        insert ->
            case emqx_utils_sql:split_insert(SQL) of
                {ok, SplitedInsert} ->
                    {ok, parse_splited_sql(SplitedInsert)};
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

parse_splited_sql({Insert, Values, OnClause}) ->
    RowTemplate = emqx_template_sql:parse(Values),
    {Insert, RowTemplate, OnClause};
parse_splited_sql({Insert, Values}) ->
    RowTemplate = emqx_template_sql:parse(Values),
    {Insert, RowTemplate}.

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

on_batch_insert(InstId, BatchReqs, {InsertPart, RowTemplate, OnClause}, State, ChannelConfig) ->
    Rows = [render_row(RowTemplate, Msg, ChannelConfig) || {_, Msg} <- BatchReqs],
    Query = [InsertPart, <<" values ">> | lists:join($,, Rows)] ++ [<<" on ">>, OnClause],
    on_sql_query(InstId, query, Query, no_params, default_timeout, State);
on_batch_insert(InstId, BatchReqs, {InsertPart, RowTemplate}, State, ChannelConfig) ->
    Rows = [render_row(RowTemplate, Msg, ChannelConfig) || {_, Msg} <- BatchReqs],
    Query = [InsertPart, <<" values ">> | lists:join($,, Rows)],
    on_sql_query(InstId, query, Query, no_params, default_timeout, State).

render_row(RowTemplate, Data, ChannelConfig) ->
    RenderOpts =
        case maps:get(undefined_vars_as_null, ChannelConfig, false) of
            % NOTE:
            %  Ignoring errors here, missing variables are set to "'undefined'" due to backward
            %  compatibility requirements.
            false -> #{escaping => mysql, undefined => <<"undefined">>};
            true -> #{escaping => mysql}
        end,
    {Row, _Errors} = emqx_template_sql:render(RowTemplate, {emqx_jsonish, Data}, RenderOpts),
    Row.

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
