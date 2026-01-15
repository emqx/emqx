%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_mysql_connector).

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
    on_get_status/2
]).

%% ecpool connect
-export([connect/1]).

-export([roots/0, fields/1, namespace/0]).

-export([do_get_status/1]).

-define(MYSQL_HOST_OPTIONS, #{
    default_port => ?MYSQL_DEFAULT_PORT
}).

-type prepare_statement_key() :: atom().
-type prepare_statement_sql() :: unicode:chardata().

-type state() ::
    #{
        pool_name := binary(),
        prepare_statements := #{prepare_statement_key() => prepare_statement_sql()}
    }.

%%=====================================================================
%% Hocon schema

namespace() -> mysql.

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [{server, server()}] ++
        emqx_connector_schema_lib:relational_db_fields(#{default_username => <<"root">>}) ++
        emqx_connector_schema_lib:ssl_fields().

server() ->
    Meta = #{desc => ?DESC("server")},
    emqx_schema:servers_sc(Meta, ?MYSQL_HOST_OPTIONS).

%% ===================================================================

resource_type() -> auth_mysql.

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
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, ?MYSQL_HOST_OPTIONS),
    ?SLOG(info, #{
        msg => "starting_auth_mysql_connector",
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
    PrepareStatements = maps:get(prepare_statements, Config, #{}),
    Options =
        lists:flatten([
            [{password, Password} || Password /= undefined],
            {basic_capabilities, BasicCapabilities},
            {host, Host},
            {port, Port},
            {user, Username},
            {database, DB},
            {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
            {pool_size, PoolSize},
            {prepare, maps:to_list(PrepareStatements)}
        ]),
    case emqx_resource_pool:start(InstId, ?MODULE, Options ++ SslOpts) of
        ok ->
            {ok, #{pool_name => InstId, prepare_statements => PrepareStatements}};
        {error, Reason} ->
            ?tp(
                mysql_connector_start_failed,
                #{error => Reason}
            ),
            {error, Reason}
    end.

on_stop(InstId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_auth_mysql_connector",
        connector => InstId
    }),
    emqx_resource_pool:stop(InstId).

%% query (SQL request)
on_query(InstId, {query, SQL}, State) ->
    on_query(InstId, {query, SQL, [], #{}}, State);
on_query(InstId, {query, SQL, Params}, State) ->
    on_query(InstId, {query, SQL, Params, #{}}, State);
on_query(InstId, {query, SQL, Params, Opts} = Request, State) ->
    Timeout = maps:get(timeout, Opts, default_timeout),
    LogInfo = #{connector => InstId, request => Request, state => State},
    Fun = fun(Conn) ->
        mysql:query(Conn, SQL, Params, Timeout)
    end,
    do_on_query(Fun, LogInfo, State);
%% execute (prepared request)
on_query(InstId, {execute, Key}, State) ->
    on_query(InstId, {execute, Key, [], #{}}, State);
on_query(InstId, {execute, Key, Params}, State) ->
    on_query(InstId, {execute, Key, Params, #{}}, State);
on_query(InstId, {execute, Key, Params, Opts} = Request, State) ->
    Timeout = maps:get(timeout, Opts, default_timeout),
    LogInfo = #{connector => InstId, request => Request, state => State},
    Fun = fun(Conn) ->
        mysql:execute(Conn, Key, Params, Timeout)
    end,
    do_on_query(Fun, LogInfo, State).

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
                try mysql:prepare(Conn, get_status, SQL) of
                    {error, Reason} ->
                        {error, emqx_utils:readable_error_msg(Reason)};
                    {ok, _} ->
                        ok
                after
                    _ = mysql:unprepare(Conn, get_status)
                end;
            (_, Acc) ->
                Acc
        end,
        ok,
        ConnsSQLs
    ).

%% ===================================================================

connect(Options) ->
    NOptions = init_connect_opts(Options),
    mysql:start_link(NOptions).

init_connect_opts(Options) ->
    case lists:keytake(password, 1, Options) of
        {value, {password, Secret}, Rest} ->
            [{password, emqx_secret:unwrap(Secret)} | Rest];
        false ->
            Options
    end.

pool_workers(PoolName) ->
    lists:map(fun({_Name, Worker}) -> Worker end, ecpool:workers(PoolName)).

do_on_query(Fun, LogInfo, #{pool_name := PoolName} = _State) ->
    ?TRACE("QUERY", "mysql_auth_connector_do_on_query", LogInfo),
    Worker = ecpool:get_client(PoolName),
    case ecpool_worker:client(Worker) of
        {ok, Conn} ->
            try Fun(Conn) of
                {error, Reason} ->
                    ?tp(warning, "mysql_auth_connector_query_failed", LogInfo#{reason => Reason}),
                    {error, Reason};
                Result ->
                    Result
            catch
                Class:Reason ->
                    ?tp(error, "mysql_auth_connector_query_exception", LogInfo#{
                        class => Class, reason => Reason
                    }),
                    {error, Reason}
            end;
        {error, disconnected} ->
            ?tp(warning, "mysql_auth_connector_query_failed", LogInfo#{reason => disconnected}),
            {error, {recoverable_error, disconnected}}
    end.
