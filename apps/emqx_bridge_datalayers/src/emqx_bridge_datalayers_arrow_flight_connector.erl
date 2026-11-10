%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_datalayers_arrow_flight_connector).

-include("emqx_bridge_datalayers.hrl").

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% callbacks of behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_status/2,
    on_get_channel_status/3,
    on_query/3,
    on_batch_query/3,
    on_query_async/4,
    on_batch_query_async/4
]).

%% ecpool callback
-export([
    connect/1,
    call_driver/4,
    prepare_sql_to_conn/4,
    get_reconnect_callback_signature/1,
    ecpool_worker_on_disconnect/2
]).

-ifdef(TEST).
-export([prune_dead_refs/1, ssl_opts/3]).
-endif.

-type state() :: #{
    driver_type := atom(),
    channels => #{channel_id() := channel_state()},
    enable_prepared => boolean()
}.

-type channel_state() ::
    #{
        pool_name := resource_id(),
        client_state_tab := ets:table(),
        enable_prepared => boolean(),
        query_templates := #{
            prepstmt := {emqx_template_sql:statement(), emqx_template_sql:row_template()},
            batch := tuple()
        },
        prepared_refs := ok | {error, any()}
    }.

-type channel_config() ::
    #{
        parameters := #{
            sql := binary()
        }
    }.

-define(sync, sync).
-define(async, async).

-define(prepare, true).
-define(no_prepare, false).

-define(null, null).

%% Allocatable resources
-define(client_state_tab, client_state_tab).

-define(prepared_refs_key(ACTIONRESID, CLIENTPID), {prepared_refs, ACTIONRESID, CLIENTPID}).
-define(prepared_refs_row(ACTIONRESID, CLIENTPID, REF), {
    ?prepared_refs_key(ACTIONRESID, CLIENTPID), REF
}).

resource_type() -> datalayers.

callback_mode() -> async_if_possible.

%% ================================================================================
on_start(
    ConnResId,
    Config = #{
        server := Server,
        parameters := Parameters,
        ssl := SSL
    }
) ->
    Username = maps:get(username, Parameters, <<>>),
    Password = maps:get(password, Parameters, <<>>),
    Database = maps:get(database, Parameters, <<>>),
    #{hostname := Host, port := Port} = emqx_schema:parse_server(
        Server, ?DATALAYERS_HOST_ARROW_OPTIONS
    ),
    ?SLOG(info, #{
        msg => "starting_datalayers_arrow_flight_connector",
        connector => ConnResId,
        config => emqx_utils:redact(Config)
    }),
    case ssl_opts(ConnResId, bin(Host), SSL) of
        SslOpts when is_map(SslOpts) ->
            ?SLOG(debug, #{
                msg => "ssl_options_for_datalayers_arrow_flight_connector",
                ssl_opts => SslOpts
            }),
            ConnConfig = SslOpts#{
                host => bin(Host),
                port => Port,
                username => bin(Username),
                password => Password,
                database => Database
            },
            do_start(ConnResId, ConnConfig, Config);
        {error, _Reason} = Err ->
            Err
    end.

do_start(
    ConnResId,
    ConnConfig,
    Config = #{
        parameters := #{
            enable_prepared := EnablePrepared
        } = _Parameters,
        pool_size := PoolSize
    }
) ->
    PoolOpts = [
        {conn_config, ConnConfig},
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
        {pool_size, PoolSize}
    ],
    Tid = ets:new(?client_state_tab, [public, ordered_set]),
    allocate(ConnResId, ?client_state_tab, Tid),
    InitState = #{
        client_state_tab => Tid,
        health_check_timeout => emqx_resource:get_health_check_timeout(Config),
        channels => #{},
        enable_prepared => EnablePrepared
    },
    case emqx_resource_pool:start(ConnResId, ?MODULE, PoolOpts) of
        ok ->
            lists:foreach(
                fun({_, WorkerPid}) ->
                    ecpool_worker:set_disconnect_callback(
                        WorkerPid,
                        {?MODULE, ecpool_worker_on_disconnect, [Tid]}
                    )
                end,
                ecpool:workers(ConnResId)
            ),
            {ok, InitState};
        {error, {start_pool_failed, _, Reason}} ->
            ?SLOG(error, #{
                msg => "datalayers_arrow_flight_connector_start_failed",
                reason => Reason,
                config => emqx_utils:redact(Config)
            }),
            ets:delete(Tid),
            {error, Reason}
    end.

on_stop(ConnResId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_datalayers_arrow_flight_connector",
        connector => ConnResId
    }),
    maps:foreach(
        fun
            (?client_state_tab = Key, Tid) ->
                try_close_prepared_refs(Tid),
                ets:delete(Tid),
                deallocate(ConnResId, Key);
            (_, _) ->
                ok
        end,
        emqx_resource:get_allocated_resources(ConnResId)
    ),
    try_stop_clients(ConnResId),
    Res = emqx_resource_pool:stop(ConnResId),
    ?tp("arrow_flight_sql_client_stopped", #{instance_id => ConnResId}),
    Res.

-spec on_add_channel(
    ConnResId :: resource_id(),
    State :: state(),
    ChannelId :: channel_id(),
    ChannelConfig :: channel_config()
) -> {ok, state()} | {error, term()}.
on_add_channel(_ConnResId, #{channels := Channels} = _State, ChannelId, _ChannelConfig) when
    is_map_key(ChannelId, Channels)
->
    {error, already_exists};
on_add_channel(
    ConnResId,
    #{channels := Channels} = State,
    ChannelId,
    ChannelConfig
) ->
    {ok, ChannelState} = create_channel_state(ConnResId, ChannelId, State, ChannelConfig),
    {ok, State#{channels := Channels#{ChannelId => ChannelState}}}.

on_remove_channel(_ConnResId, #{channels := Channels} = _State, ChannelId) when
    not is_map_key(ChannelId, Channels)
->
    {error, not_found};
on_remove_channel(
    _ConnResId, #{channels := Channels} = ConnState0, ChannelId
) ->
    ChannelState = maps:get(ChannelId, Channels),
    ok = destroy_channel_state(ChannelState, ChannelId, ConnState0),
    ConnState = ConnState0#{channels := maps:remove(ChannelId, Channels)},
    {ok, ConnState}.

create_channel_state(
    ConnResId,
    ChannelId,
    #{enable_prepared := EnablePrepared} = ConnState,
    #{parameters := ActionParameters} = _ChannelConfig
) ->
    ChannelState0 = parse_sql_template(ChannelId, ActionParameters),
    NChannelState = ChannelState0#{
        pool_name => ConnResId,
        enable_prepared => EnablePrepared
    },
    {ok, init_prepare(ChannelId, NChannelState, ConnState)}.

destroy_channel_state(#{enable_prepared := false} = _ChannelState, _ChanResId, _ConnState) ->
    ok;
destroy_channel_state(#{prepared_refs := ok} = ChannelState, ChanResId, ConnState) ->
    #{client_state_tab := Tid} = ConnState,
    #{pool_name := ConnResId} = ChannelState,
    lists:foreach(
        fun({_, WorkerPid}) ->
            ecpool_worker:remove_reconnect_callback_by_signature(WorkerPid, ChanResId)
        end,
        ecpool:workers(ConnResId)
    ),
    try_close_prepared_refs(Tid, ChanResId),
    ets:match_delete(Tid, ?prepared_refs_row(ChanResId, '_', '_')),
    ok;
destroy_channel_state(_ChannelState, _ChanResId, _ConnState) ->
    ok.

on_get_channels(ConnResId) ->
    emqx_bridge_v2:get_channels_for_connector(ConnResId).

on_get_status(ConnResId, State) ->
    health_check(ConnResId, State).

on_get_channel_status(_ConnResId, _ChannelId, _State) ->
    %% TODO: Check the status for each channel.
    %% e.g.
    %% - check the prepared statements
    %% - check sql database/table existed
    ?status_connected.

health_check(ConnResId, #{health_check_timeout := Timeout} = ConnState) ->
    prune_dead_refs(ConnState),
    case do_health_check(ConnResId, Timeout) of
        ok ->
            ?status_connected;
        {error, Reason} ->
            {?status_disconnected, Reason}
    end.

do_health_check(ConnResId, Timeout) ->
    Workers = [Worker || {_WorkerName, Worker} <- ecpool:workers(ConnResId)],
    Fn = fun(Conn) -> datalayers:execute(Conn, <<"SELECT VERSION()">>) end,
    DoPerWorker =
        fun(Worker) ->
            case ecpool_worker:exec(Worker, Fn, Timeout) of
                {ok, _} ->
                    ok;
                {error, Reason} = Error ->
                    ?SLOG(error, #{
                        msg => "datalayers_arrow_flight__connector_get_status_failed",
                        reason => Reason,
                        worker => Worker
                    }),
                    Error
            end
        end,
    try emqx_utils:pmap(DoPerWorker, Workers, Timeout) of
        Results ->
            case [E || {error, _} = E <- Results] of
                [] ->
                    ok;
                Errors ->
                    hd(Errors)
            end
    catch
        exit:timeout ->
            ?SLOG(error, #{
                msg => "datalayers_arrow_flight_connector_pmap_failed",
                reason => timeout
            }),
            {error, timeout}
    end.

on_query(ConnResId, {_ChannelId, _} = Query, State) ->
    ?TRACE("QUERY", "datalayers_arrow_flight_connector_do_query", #{
        instance_id => ConnResId,
        query => Query,
        state => emqx_utils:redact(State)
    }),
    ?tp(datalayers_arrow_flight_connector_on_query, #{instance_id => ConnResId}),
    do_query(ConnResId, ?sync, [Query], State, []).

on_batch_query(ConnResId, [{_ChannelId, _} | _] = Querys, State) ->
    ?TRACE("QUERY", "datalayers_arrow_flight_connector_do_batch_query", #{
        instance_id => ConnResId,
        querys => Querys,
        state => emqx_utils:redact(State)
    }),
    ?tp(datalayers_arrow_flight_connector_on_batch_query, #{instance_id => ConnResId}),
    do_query(ConnResId, ?sync, Querys, State, []).

on_query_async(ConnResId, {_ChannelId, _Message} = Query, ResCallback, State) ->
    ?TRACE("QUERY", "datalayers_arrow_flight_connector_do_async_query", #{
        instance_id => ConnResId,
        query => Query,
        state => emqx_utils:redact(State)
    }),
    ?tp(datalayers_arrow_flight_connector_on_query_async, #{instance_id => ConnResId}),
    do_query(ConnResId, ?async, [Query], State, [ResCallback]).

on_batch_query_async(ConnResId, Querys, ResCallback, ConnState) ->
    ?TRACE("QUERY", "datalayers_arrow_flight_connector_do_async_batch_query", #{
        instance_id => ConnResId,
        querys => Querys,
        state => emqx_utils:redact(ConnState)
    }),
    ?tp(datalayers_arrow_flight_connector_on_batch_query_async, #{instance_id => ConnResId}),
    do_query(ConnResId, ?async, Querys, ConnState, [ResCallback]).

-spec do_query(
    ConnResId :: resource_id(),
    QueryMode :: ?sync | ?async,
    Querys :: [{channel_id(), term()}],
    ConnState :: state(),
    InitArgs :: [term()]
) ->
    {ok, _Result} | {error, _Reason}.
do_query(
    ConnResId,
    QueryMode,
    [{ChannelId, _Data} | _] = Querys,
    #{channels := Channels} = ConnState,
    InitArgs
) ->
    ChannelState = maps:get(ChannelId, Channels),
    EnablePrepared = enable_prepared(ConnState, ChannelState),
    case get_template(EnablePrepared, ChannelState) of
        undefined ->
            {error, {unrecoverable_error, no_template_found}};
        Template ->
            DriverFunArgs = proc_sql_params(
                {EnablePrepared, Template},
                Querys,
                ChannelState,
                ConnState,
                InitArgs
            ),
            FuncMode = {QueryMode, EnablePrepared},
            pick_and_do(ConnResId, ChannelId, FuncMode, DriverFunArgs)
    end.

pick_and_do(ConnResId, ChanResId, FuncMode, DriverFunArgs) ->
    try
        ecpool:pick_and_do(
            ConnResId, {?MODULE, call_driver, [ChanResId, FuncMode, DriverFunArgs]}, no_handover
        )
    catch
        _:_:_ ->
            {error, {unrecoverable_error, invalid_request}}
    end.

proc_sql_params(
    {?prepare, PrepStmtTemplate},
    [{ChannelId, _} | _] = Querys,
    _ChannelState,
    ConnState,
    InitArgs
) ->
    #{client_state_tab := Tid} = ConnState,
    RenderedRows = [
        render_prepare_sql_row(PrepStmtTemplate, Query)
     || {_, Query} <- Querys
    ],
    emqx_trace:rendered_action_template(ChannelId, #{record_rows => RenderedRows}),
    [
        Tid,
        PrepStmtTemplate,
        RenderedRows
        | InitArgs
    ];
proc_sql_params(
    {?no_prepare, BatchTemplate},
    [{ChannelId, _} | _] = Querys,
    ChannelState,
    _ConnState,
    InitArgs
) ->
    RenderedSql = bin(render_batch_sql_row(BatchTemplate, Querys, ChannelState)),
    emqx_trace:rendered_action_template(ChannelId, #{rendered_sql => RenderedSql}),
    [
        RenderedSql
        | InitArgs
    ].

get_template(?prepare, #{query_templates := QueryTemplates} = _ChannelState) ->
    maps:get(prepstmt, QueryTemplates, undefined);
get_template(?no_prepare, #{query_templates := QueryTemplates} = _ChannelState) ->
    maps:get(batch, QueryTemplates, undefined).

enable_prepared(#{enable_prepared := ?no_prepare} = _State, _ChannelState) ->
    ?no_prepare;
enable_prepared(
    #{enable_prepared := ?prepare},
    #{query_templates := #{prepstmt := _PrepStmt}, prepared_refs := PreparedRefs}
) when
    is_map(PreparedRefs)
->
    ?prepare;
enable_prepared(_, _) ->
    ?no_prepare.

render_prepare_sql_row({_InsertPart, RowTemplate}, Querys) ->
    % NOTE: ignoring errors here, missing variables will be replaced with `null`.
    {Row, _Errors} = emqx_template_sql:render_prepstmt(RowTemplate, {emqx_jsonish, Querys}),
    Row.

render_batch_sql_row({InsertPart, RowTemplate, OnClause}, Querys, ChannelState) ->
    Rows = [render_row(RowTemplate, Msg, ChannelState) || {_, Msg} <- Querys],
    Query = [InsertPart, <<" values ">> | lists:join($,, Rows)] ++ [<<" on ">>, OnClause],
    Query;
render_batch_sql_row({InsertPart, RowTemplate}, Querys, ChannelState) ->
    Rows = [render_row(RowTemplate, Msg, ChannelState) || {_, Msg} <- Querys],
    Query = [InsertPart, <<" values ">> | lists:join($,, Rows)],
    Query.

render_row(RowTemplate, Data, _ChannelState) ->
    %% undefined values are always replaced with `null`
    RenderOpts = #{escaping => mysql, undefined => ?null},
    {Row, _Errors} = emqx_template_sql:render(RowTemplate, {emqx_jsonish, Data}, RenderOpts),
    Row.

%% =================================================================
%% ecpool callback

connect(Opts) ->
    #{password := Password} = Config = proplists:get_value(conn_config, Opts),
    Database = maps:get(database, Config),
    ClientOpts = (maps:remove(database, Config))#{
        password => bin(emqx_secret:unwrap(Password))
    },
    case datalayers:connect(ClientOpts) of
        {ok, Pid} ->
            case datalayers:use_database(Pid, Database) of
                {ok, _} ->
                    {ok, Pid};
                {error, Reason} ->
                    datalayers:stop(Pid),
                    {error, {use_database_failed, Database, Reason}}
            end;
        {error, Reason} = Err ->
            ?SLOG(error, #{
                msg => "datalayers_arrow_flight_connector_connect_failed",
                reason => Reason
            }),
            Err
    end.

call_driver(Client, ChanResId, {_QueryMode, true} = FuncMode, Args0) ->
    [Tid, {SqlStatement0, _RowTemplates} | Args] = Args0,
    PreparedRef =
        case ets:lookup(Tid, ?prepared_refs_key(ChanResId, Client)) of
            [?prepared_refs_row(ChanResId, Client, Ref)] ->
                Ref;
            _ ->
                SqlStatement = bin(SqlStatement0),
                {ok, Ref0} = prepare_sql_to_conn(Client, ChanResId, SqlStatement, Tid),
                Ref0
        end,
    do_call_driver(Client, driver_fun_name(FuncMode), [PreparedRef | Args]);
call_driver(Client, _ChanResId, {_QueryMode, false} = FuncMode, Args) ->
    do_call_driver(Client, driver_fun_name(FuncMode), Args).

do_call_driver(Client, FuncName, Args) ->
    case erlang:apply(datalayers, FuncName, [Client | Args]) of
        ok when FuncName =:= async_execute; FuncName =:= async_execute_prepare ->
            {ok, Client};
        ok ->
            ok;
        {ok, _Res} = Ok ->
            Ok;
        {error, Reason} = Err ->
            ?SLOG(error, #{
                msg => "datalayers_arrow_flight_connector_call_driver_failed",
                reason => Reason,
                driver_function => FuncName,
                args => emqx_utils:redact(Args)
            }),
            Err
    end.

driver_fun_name({?sync, ?no_prepare}) ->
    execute;
driver_fun_name({?sync, ?prepare}) ->
    execute_prepare;
driver_fun_name({?async, ?no_prepare}) ->
    async_execute;
driver_fun_name({?async, ?prepare}) ->
    async_execute_prepare.

%% =================================================================
%% Helper functions

parse_sql_template(
    ChannelId,
    #{sql := SQLTemplate} = _ActionParameters
) ->
    AccIn = #{},
    Templates = maps:fold(
        fun parse_sql_template/3,
        AccIn,
        #{ChannelId => SQLTemplate}
    ),
    #{query_templates => Templates}.

parse_sql_template(ChannelId, SQLTemplate, AccIn) ->
    Template = emqx_template_sql:parse_prepstmt(SQLTemplate, #{parameters => '?'}),
    AccOut = AccIn#{prepstmt => Template},
    parse_batch_sql(ChannelId, SQLTemplate, AccOut).

parse_batch_sql(ChannelId, SQLTemplate, AccIn) ->
    case emqx_utils_sql:get_statement_type(SQLTemplate) of
        insert ->
            case emqx_utils_sql:split_insert(SQLTemplate) of
                {ok, SplitedInsert} ->
                    AccIn#{batch => parse_splited_sql(SplitedInsert)};
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "parse_insert_sql_statement_failed",
                        sql => SQLTemplate,
                        reason => Reason
                    }),
                    AccIn
            end;
        select ->
            AccIn;
        Type ->
            ?SLOG(error, #{
                msg => "invalid_sql_statement_type",
                sql => SQLTemplate,
                type => Type,
                channel => ChannelId
            }),
            AccIn
    end.

parse_splited_sql({Insert, Values, OnClause}) ->
    RowTemplate = emqx_template_sql:parse(Values),
    {Insert, RowTemplate, OnClause};
parse_splited_sql({Insert, Values}) ->
    RowTemplate = emqx_template_sql:parse(Values),
    {Insert, RowTemplate}.

init_prepare(_ChanResId, ChannelState = #{enable_prepared := false}, _ConnState) ->
    ChannelState;
init_prepare(_ChanResId, ChannelState = #{query_templates := Templates}, _ConnState) when
    %% No `batch` or `prepstmt`
    map_size(Templates) == 0
->
    ChannelState;
init_prepare(ChanResId, ChannelState = #{}, ConnState) ->
    case prepare_sql(ChanResId, ChannelState, ConnState) of
        {ok, PrepStatements} when is_map(PrepStatements) ->
            %% each client holds a prepared statements reference
            %% already inserted into the table.
            ChannelState#{prepared_refs => ok};
        {error, Reason} ->
            ChannelState#{prepared_refs => {error, Reason}}
    end.

prepare_sql(ChanResId, ChanState, ConnState) ->
    #{pool_name := PoolName, query_templates := #{prepstmt := PrepStmt}} = ChanState,
    #{client_state_tab := Tid} = ConnState,
    prepare_sql(PoolName, Tid, ChanResId, PrepStmt).

prepare_sql(PoolName, Tid, ChanResId, {SqlStatement0, _RowTemplates}) ->
    SqlStatement = bin(SqlStatement0),
    case do_prepare_sql(PoolName, Tid, ChanResId, SqlStatement) of
        {ok, _PrepStatements} = Ok ->
            %% prepare for reconnect
            ecpool:add_reconnect_callback(
                PoolName, {?MODULE, prepare_sql_to_conn, [ChanResId, SqlStatement, Tid]}
            ),
            Ok;
        Error ->
            Error
    end.

%% for ecpool to deduplicate callbacks.
get_reconnect_callback_signature([ChanResId | _]) ->
    ChanResId.

do_prepare_sql(PoolName, Tid, ChanResId, SqlStatement) ->
    do_prepare_to_conns(
        ecpool:workers(PoolName),
        Tid,
        ChanResId,
        SqlStatement,
        _InitPrepStatements = #{}
    ).

do_prepare_to_conns([{_Name, Worker} | Rest], Tid, ChanResId, SqlStatement, AccIn) ->
    maybe
        {ok, Client} ?= ecpool_worker:client(Worker),
        {ok, PrepareRef} ?= prepare_sql_to_conn(Client, ChanResId, SqlStatement, Tid),
        do_prepare_to_conns(Rest, Tid, ChanResId, SqlStatement, AccIn#{Client => PrepareRef})
    end;
do_prepare_to_conns([], _Tid, _ChanResId, _SqlStatement, AccOut) ->
    {ok, AccOut}.

%% Prepare SQL statement on a specific connection
prepare_sql_to_conn(Client, ChanResId, SqlStatement, Tid) ->
    case datalayers:prepare(Client, SqlStatement, #{auto_rebuild => true}) of
        {ok, Ref} when is_reference(Ref) ->
            ets:insert(Tid, ?prepared_refs_row(ChanResId, Client, Ref)),
            {ok, Ref};
        Error ->
            Error
    end.

close_statement_on_conn(Client, PrepareRef) ->
    datalayers:close_prepared(Client, PrepareRef).

%% Clean up dead pids from the local PreparedRefs copy.
%% Worker pids that are no longer alive still hold prepared statement
%% handles on the Datalayers server (no Close action was sent).
%% They will be cleaned up by the server-side TTL (~1 day).
prune_dead_refs(ConnState) ->
    #{client_state_tab := Tid} = ConnState,
    ets:foldl(
        fun
            (?prepared_refs_row(ChanResId, ClientPid, _), Acc) ->
                case is_process_alive(ClientPid) of
                    true ->
                        ok;
                    false ->
                        ets:delete(Tid, ?prepared_refs_key(ChanResId, ClientPid)),
                        ok
                end,
                Acc;
            (_, Acc) ->
                Acc
        end,
        ok,
        Tid
    ).

try_stop_clients(ConnResId) ->
    lists:foreach(
        fun({_Name, Worker}) ->
            case ecpool_worker:client(Worker) of
                %% Close prepare
                {ok, ConnPid} ->
                    try_stop_client(ConnResId, ConnPid);
                _ ->
                    ok
            end
        end,
        ecpool:workers(ConnResId)
    ).

try_stop_client(ConnResId, ConnPid) ->
    try
        datalayers:stop(ConnPid)
    catch
        Kind:Reason:Stacktrace ->
            ?SLOG(warning, #{
                msg => "datalayers_arrow_flight_error_closing_client",
                reason => {Kind, Reason},
                stacktrace => Stacktrace,
                connector_res_id => ConnResId
            }),
            ok
    end.

ecpool_worker_on_disconnect(ClientPid, Tid) ->
    ets:match_delete(Tid, ?prepared_refs_row('_', ClientPid, '_')),
    ok.

try_close_prepared_refs(Tid) ->
    try_close_prepared_refs(Tid, _ChanResIdPat = '_').

try_close_prepared_refs(Tid, ChanResIdPat) ->
    ets:foldl(
        fun
            (?prepared_refs_row(Id, ClientPid, Ref), Acc) when
                ChanResIdPat == '_' orelse Id == ChanResIdPat
            ->
                try_close_prepared_ref(ClientPid, Ref, Id),
                Acc;
            (_, Acc) ->
                Acc
        end,
        ok,
        Tid
    ).

try_close_prepared_ref(ClientPid, Ref, ChanResId) ->
    try
        maybe
            true ?= is_process_alive(ClientPid),
            close_statement_on_conn(ClientPid, Ref)
        end
    catch
        Kind:Reason:Stacktrace ->
            ?SLOG(warning, #{
                msg => "datalayers_arrow_flight_error_closing_statement",
                reason => {Kind, Reason},
                stacktrace => Stacktrace,
                action_res_id => ChanResId
            }),
            ok
    end.

ssl_opts(ConnResId, Host, #{enable := true} = SSL) ->
    case make_tls_opts(Host, SSL) of
        {ok, TLSOpts} ->
            TLSOpts;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "datalayers_arrow_flight_invalid_ssl_options",
                reason => Reason,
                connector => ConnResId
            }),
            {error, Reason}
    end;
ssl_opts(_ConnResId, _Host, _) ->
    #{}.

make_tls_opts(Host, SSL) ->
    maybe
        ok ?= validate_tls_opts(SSL),
        {ok, CacertFile} ?= ssl_cacertfile(SSL),
        {ok, ServerName} ?= tls_server_name(Host, SSL),
        {ok, #{
            tls_enable => true,
            tls_cacertfile => CacertFile,
            verify => verify_peer,
            tls_server_name => ServerName
        }}
    else
        {error, _} = Error ->
            Error
    end.

validate_tls_opts(SSL) ->
    case maps:get(verify, SSL, verify_none) of
        verify_peer ->
            validate_client_certificates(SSL);
        _ ->
            {error, verify_none_not_supported}
    end.

validate_client_certificates(SSL) ->
    case [Key || Key <- [certfile, keyfile, managed_certs], has_nonempty_value(Key, SSL)] of
        [] -> ok;
        [_ | _] -> {error, tls_client_certificates_not_supported}
    end.

ssl_cacertfile(SSL) ->
    validate_ssl_cacertfile(maps:get(cacertfile, SSL, undefined)).

validate_ssl_cacertfile(undefined) ->
    {error, ssl_cacertfile_is_required};
validate_ssl_cacertfile(Path0) ->
    case to_utf8_binary(Path0) of
        {ok, Path} when byte_size(Path) > 0 ->
            InterpolatedPath = emqx_schema:naive_env_interpolation(Path),
            AbsolutePath = filename:absname(InterpolatedPath),
            validate_cacertfile_file(unicode:characters_to_binary(AbsolutePath));
        _ ->
            {error, ssl_cacertfile_is_required}
    end.

validate_cacertfile_file(Path) ->
    case emqx_tls_lib:ensure_ssl_files(filename:dirname(Path), #{cacertfile => Path}) of
        {ok, _} ->
            {ok, Path};
        {error, Reason} ->
            {error, ssl_cacertfile_error(Reason)}
    end.

ssl_cacertfile_error(#{pem_check := Reason}) when Reason =/= not_pem ->
    ssl_cacertfile_is_not_readable;
ssl_cacertfile_error(_) ->
    ssl_cacertfile_is_not_pem.

tls_server_name(Host, SSL) ->
    case maps:get(server_name_indication, SSL, undefined) of
        undefined ->
            nonempty_binary(Host, ssl_server_name_is_required);
        disable ->
            {error, ssl_server_name_indication_disabled};
        ServerName ->
            case to_utf8_binary(ServerName) of
                {ok, <<"disable">>} ->
                    {error, ssl_server_name_indication_disabled};
                {ok, Name} when byte_size(Name) > 0 ->
                    {ok, Name};
                _ ->
                    {error, ssl_server_name_is_required}
            end
    end.

nonempty_binary(Value, Error) ->
    case to_utf8_binary(Value) of
        {ok, Binary} when byte_size(Binary) > 0 -> {ok, Binary};
        _ -> {error, Error}
    end.

has_nonempty_value(Key, Map) ->
    case maps:get(Key, Map, undefined) of
        undefined -> false;
        null -> false;
        <<>> -> false;
        [] -> false;
        _ -> true
    end.

to_utf8_binary(Value) when is_binary(Value) ->
    case unicode:characters_to_binary(Value) of
        Binary when is_binary(Binary) -> {ok, Binary};
        _ -> error
    end;
to_utf8_binary(Value) when is_list(Value) ->
    case unicode:characters_to_binary(Value) of
        Binary when is_binary(Binary) -> {ok, Binary};
        _ -> error
    end;
to_utf8_binary(_) ->
    error.

bin(Bin) when is_binary(Bin) ->
    Bin;
bin(List) when is_list(List) ->
    iolist_to_binary(List);
bin(Atom) when is_atom(Atom) ->
    erlang:atom_to_binary(Atom).

allocate(ConnResId, Key, Value) ->
    ok = emqx_resource:allocate_resource(ConnResId, ?MODULE, Key, Value).

deallocate(ConnResId, Key) ->
    ok = emqx_resource:deallocate_resource(ConnResId, Key).
