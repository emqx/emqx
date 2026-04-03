%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% PostgreSQL connector used by emqx_agent skills.
%% Adapted from EMQX PostgreSQL connector patterns for agent skill usage.

-module(emqx_agent_skill_postgresql_connector).

-include_lib("emqx/include/logger.hrl").

-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_get_status/2
]).

-export([connect/1]).

-define(PGSQL_DEFAULT_PORT, 5432).
-define(PGSQL_HOST_OPTIONS, #{
    default_port => ?PGSQL_DEFAULT_PORT
}).

resource_type() -> agent_skill_pgsql.

callback_mode() -> always_sync.

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
        msg => "starting_agent_skill_postgresql_connector",
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
    Options = [
        {host, Host},
        {port, Port},
        {username, User},
        {password, Password},
        {database, DB},
        {auto_reconnect, 15},
        {timeout, ConnectTimeout},
        {pool_size, PoolSize}
    ],
    case emqx_resource_pool:start(InstId, ?MODULE, Options ++ SslOpts) of
        ok ->
            {ok, #{pool_name => InstId}};
        {error, Reason} ->
            {error, Reason}
    end.

on_stop(InstId, State) ->
    ?SLOG(info, #{
        msg => "stopping_agent_skill_postgresql_connector",
        connector => InstId
    }),
    close_connections(State),
    emqx_resource_pool:stop(InstId).

on_query(InstId, {query, SQL}, State) ->
    on_query(InstId, {query, SQL, []}, State);
on_query(InstId, {query, SQL, Params} = Request, #{pool_name := PoolName} = State) ->
    LogInfo = #{connector => InstId, request => Request, state => State},
    do_on_query(PoolName, fun(Conn) -> query(Conn, SQL, Params) end, LogInfo).

on_get_status(_InstId, #{pool_name := PoolName}) ->
    Opts = #{
        check_fn => fun(Conn) -> epgsql:squery(Conn, "SELECT 1") end,
        is_success_fn => fun
            ({ok, _, _}) -> false;
            (_) -> true
        end
    },
    emqx_resource_pool:common_health_check_workers(PoolName, Opts).

connect(Opts) ->
    Host = proplists:get_value(host, Opts),
    Username = proplists:get_value(username, Opts),
    Password = emqx_secret:unwrap(proplists:get_value(password, Opts)),
    ConnOpts = conn_opts(Opts),
    epgsql:connect(Host, Username, Password, ConnOpts).

close_connections(#{pool_name := PoolName}) ->
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

do_on_query(PoolName, Fun, _LogInfo) ->
    Worker = ecpool:get_client(PoolName),
    case ecpool_worker:client(Worker) of
        {ok, Conn} ->
            try Fun(Conn) of
                {error, Reason} ->
                    {error, {unrecoverable_error, format_error(Reason)}};
                Result ->
                    Result
            catch
                Class:Reason ->
                    {error, {unrecoverable_error, {Class, Reason}}}
            end;
        {error, disconnected} ->
            {error, {unrecoverable_error, disconnected}}
    end.

query(Conn, SQL, Params) ->
    epgsql:equery(Conn, SQL, Params).

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

format_error(Reason) ->
    Reason.
