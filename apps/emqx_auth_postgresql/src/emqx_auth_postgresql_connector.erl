%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_postgresql_connector).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("epgsql/include/epgsql.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_get_status/2
]).

%% ecpool callback
-export([connect/1]).

%% hocon schema helpers
-export([config_fields/0]).

-define(PGSQL_DEFAULT_PORT, 5432).
-define(PGSQL_HOST_OPTIONS, #{
    default_port => ?PGSQL_DEFAULT_PORT
}).

-type prepare_statement_key() :: atom() | binary().
-type prepare_statement_sql() :: unicode:chardata().

-type state() ::
    #{
        pool_name := binary(),
        prepare_statements := #{prepare_statement_key() => prepare_statement_sql()},
        disable_prepared_statements := boolean()
    }.

%%------------------------------------------------------------------------------
%% Hocon schema helpers
%%------------------------------------------------------------------------------

config_fields() ->
    [
        {server, emqx_schema:servers_sc(#{desc => ?DESC("server")}, ?PGSQL_HOST_OPTIONS)},
        {connect_timeout, emqx_connector_schema_lib:connect_timeout_field()},
        {disable_prepared_statements, emqx_connector_schema_lib:disable_prepared_statements_field()}
    ] ++
        emqx_connector_schema_lib:relational_db_fields(#{username => #{required => true}}) ++
        emqx_connector_schema_lib:ssl_fields().

%%------------------------------------------------------------------------------
%% emqx_resource callbacks
%%------------------------------------------------------------------------------

resource_type() -> auth_pgsql.

callback_mode() -> always_sync.

-spec on_start(binary(), hocon:config()) -> {ok, state()} | {error, _}.
on_start(
    InstId,
    #{
        server := Server,
        database := DB,
        username := User,
        pool_size := PoolSize,
        connect_timeout := ConnectTimeout,
        ssl := SSL
    } = Config
) ->
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, ?PGSQL_HOST_OPTIONS),
    ?SLOG(info, #{
        msg => "starting_auth_postgresql_connector",
        connector => InstId,
        config => emqx_utils:redact(Config)
    }),
    SslOpts =
        case maps:get(enable, SSL) of
            true ->
                [
                    {ssl, true},
                    {ssl_opts, emqx_tls_lib:to_client_opts(SSL)}
                ];
            false ->
                [{ssl, false}]
        end,
    Password = maps:get(password, Config, emqx_secret:wrap("")),
    PrepareStatements = emqx_utils_maps:binary_key_map(maps:get(prepare_statements, Config, #{})),
    DisablePreparedStatements = maps:get(disable_prepared_statements, Config, false),
    PrepareOpt =
        case DisablePreparedStatements of
            true -> [];
            false -> [{prepare, maps:to_list(PrepareStatements)}]
        end,
    ct:print("PrepareOpt: ~p~n", [PrepareOpt]),
    Options = [
        {host, Host},
        {port, Port},
        {username, User},
        {password, Password},
        {database, DB},
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
        {timeout, ConnectTimeout},
        {pool_size, PoolSize}
    ],
    case emqx_resource_pool:start(InstId, ?MODULE, Options ++ PrepareOpt ++ SslOpts) of
        ok ->
            {ok, #{
                pool_name => InstId,
                prepare_statements => PrepareStatements,
                disable_prepared_statements => DisablePreparedStatements
            }};
        {error, Reason} ->
            ?tp(
                pgsql_auth_connector_start_failed,
                #{error => Reason}
            ),
            {error, Reason}
    end.

on_stop(InstId, State) ->
    ?SLOG(info, #{
        msg => "stopping_auth_postgresql_connector",
        connector => InstId
    }),
    %% Why do we need to close connections explicitly?
    close_connections(State),
    emqx_resource_pool:stop(InstId).

%% query (SQL request)
on_query(InstId, {query, SQL}, State) ->
    on_query(InstId, {query, SQL, []}, State);
on_query(InstId, {query, SQL, Params} = Request, #{pool_name := PoolName} = State) ->
    LogInfo = #{connector => InstId, request => Request, state => State},
    ?TRACE("QUERY", "postgresql_auth_connector_do_query", LogInfo),
    do_on_query(PoolName, fun(Conn) -> query(Conn, SQL, Params) end, LogInfo);
%% execute (prepared request)
on_query(InstId, {prepared_query, Key}, State) ->
    on_query(InstId, {prepared_query, Key, []}, State);
on_query(
    InstId,
    {prepared_query, Key0, Params} = Request,
    #{
        pool_name := PoolName,
        disable_prepared_statements := DisablePreparedStatements,
        prepare_statements := PrepareStatements
    } = State
) ->
    LogInfo = #{connector => InstId, request => Request, state => State},
    ?TRACE("QUERY", "postgresql_auth_connector_do_prepared_query", LogInfo),
    Key = to_bin(Key0),
    case PrepareStatements of
        #{Key := SQL} ->
            QueryFun =
                case DisablePreparedStatements of
                    true ->
                        fun(Conn) -> query(Conn, SQL, Params) end;
                    false ->
                        %% TODO prepare the statement if an unprepared statement error occurs
                        fun(Conn) -> prepared_query(Conn, Key, SQL, Params) end
                end,
            do_on_query(PoolName, QueryFun, LogInfo);
        _ ->
            {error, {unrecoverable_error, prepared_statement_not_found}}
    end.

on_get_status(_InstId, #{pool_name := PoolName} = State) ->
    Opts = #{
        check_fn => fun(Conn) ->
            epgsql:squery(Conn, "SELECT count(1) AS T")
        end,
        is_success_fn => fun
            ({ok, _, _}) -> false;
            (_) -> true
        end,
        on_success_fn => fun() -> do_on_get_status_prepares(_InstId, State) end
    },
    emqx_resource_pool:common_health_check_workers(PoolName, Opts).

%%------------------------------------------------------------------------------
%% ecpool callbacks
%%------------------------------------------------------------------------------

connect(Opts) ->
    Host = proplists:get_value(host, Opts),
    Username = proplists:get_value(username, Opts),
    Password = emqx_secret:unwrap(proplists:get_value(password, Opts)),
    PrepareStatements = proplists:get_value(prepare, Opts, []),
    ConnOpts = conn_opts(Opts),
    ?tp("postgres_auth_epgsql_connect", #{opts => ConnOpts}),
    case epgsql:connect(Host, Username, Password, ConnOpts) of
        {ok, Conn} ->
            case prepare_sql_to_conn(Conn, PrepareStatements) of
                ok ->
                    {ok, Conn};
                {error, Reason} ->
                    _ = epgsql:close(Conn),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% Helper Functions
%%------------------------------------------------------------------------------

close_connections(#{pool_name := PoolName} = _State) ->
    WorkerPids = [Worker || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
    close_connections_with_worker_pids(WorkerPids),
    ok;
close_connections(_) ->
    ok.

close_connections_with_worker_pids([WorkerPid | Rest]) ->
    try ecpool_worker:client(WorkerPid) of
        {ok, Conn} ->
            _ = epgsql:close(Conn),
            close_connections_with_worker_pids(Rest);
        _ ->
            close_connections_with_worker_pids(Rest)
    catch
        _:_ ->
            close_connections_with_worker_pids(Rest)
    end;
close_connections_with_worker_pids([]) ->
    ok.

pool_workers(PoolName) ->
    lists:map(fun({_Name, Worker}) -> Worker end, ecpool:workers(PoolName)).

do_on_get_status_prepares(_InstId, State) ->
    case do_check_prepares(State) of
        ok ->
            ?status_connected;
        {error, Reason} ->
            {?status_disconnected, Reason}
    end.

do_check_prepares(
    #{
        pool_name := PoolName,
        prepare_statements := PrepareStatements
    }
) ->
    ConnsSQLs = [
        {Conn, SQL}
     || Worker <- pool_workers(PoolName),
        {ok, Conn} <- [ecpool_worker:client(Worker)],
        SQL <- maps:values(PrepareStatements)
    ],
    lists:foldl(
        fun
            ({Conn, SQL}, ok) ->
                case parse2(Conn, "", SQL, []) of
                    {ok, _Statement} ->
                        ok;
                    {error, #error{} = Reason} ->
                        {error, emqx_utils:readable_error_msg(format_error(Reason))}
                end;
            (_, Acc) ->
                Acc
        end,
        ok,
        ConnsSQLs
    ).

do_on_query(PoolName, Fun, LogInfo) ->
    Worker = ecpool:get_client(PoolName),
    case ecpool_worker:client(Worker) of
        {ok, Conn} ->
            try Fun(Conn) of
                {error, Reason} ->
                    ?tp(warning, "postgresql_auth_connector_query_failed", LogInfo#{
                        reason => Reason
                    }),
                    {error, {unrecoverable_error, format_error(Reason)}};
                Result ->
                    Result
            catch
                Class:Reason ->
                    ?tp(error, "postgresql_auth_connector_query_exception", LogInfo#{
                        class => Class, reason => Reason
                    }),
                    {error, {unrecoverable_error, Reason}}
            end;
        {error, disconnected} ->
            ?tp(warning, "postgresql_auth_connector_query_failed", LogInfo#{reason => disconnected}),
            {error, {unrecoverable_error, disconnected}}
    end.

query(Conn, SQL, Params) ->
    epgsql:equery(Conn, SQL, Params).

prepared_query(Conn, Name, SQL, Params) ->
    case epgsql:prepared_query2(Conn, Name, Params) of
        {error, #error{codename = invalid_sql_statement_name}} ->
            ?SLOG(warning, #{
                msg => "postgresql_auth_invalid_sql_statement_name",
                name => Name,
                hint =>
                    "If using a PostgreSQL proxy like pgBouncer, "
                    "make sure it is configured to support prepared statements "
                    "or disable the use of prepared statements in EMQX configuration."
            }),
            query(Conn, SQL, Params);
        Result ->
            Result
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

prepare_sql_to_conn(_Conn, []) ->
    ok;
prepare_sql_to_conn(Conn, [{Key, SQL} | Rest]) ->
    LogMeta = #{msg => "postgresql_auth_prepare_statement", name => Key, sql => SQL},
    ?SLOG(info, LogMeta),
    case parse2(Conn, Key, SQL, []) of
        {ok, _Statement} ->
            prepare_sql_to_conn(Conn, Rest);
        {error, #error{codename = duplicate_prepared_statement}} ->
            %% Statement already exists, try to close and re-create
            case epgsql:close(Conn, statement, Key) of
                ok ->
                    prepare_sql_to_conn(Conn, [{Key, SQL} | Rest]);
                {error, CloseError} ->
                    ?SLOG(error, LogMeta#{
                        msg => "postgresql_auth_close_statement_failed", cause => CloseError
                    }),
                    {error, CloseError}
            end;
        {error, #error{} = Error} ->
            ?SLOG(error, LogMeta#{
                msg => "postgresql_auth_parse_failed", error => format_error(Error)
            }),
            {error, format_error(Error)}
    end.

to_bin(Bin) when is_binary(Bin) -> Bin;
to_bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom).

format_error(#error{
    severity = Severity,
    code = Code,
    codename = Codename,
    message = Message
}) ->
    #{
        severity => Severity,
        code => Code,
        codename => Codename,
        message => emqx_logger_textfmt:try_format_unicode(Message)
    };
format_error(Reason) ->
    Reason.

parse2(Conn, Name, SQL, Types) ->
    epgsql:sync_on_error(
        Conn,
        epgsql_sock:sync_command(
            Conn, emqx_auth_postgresql_cmd_parse2, {Name, SQL, Types}
        )
    ).
