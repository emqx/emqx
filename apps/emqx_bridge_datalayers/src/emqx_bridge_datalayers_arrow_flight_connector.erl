%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    call_driver/3,
    prepare_sql_to_conn/2
]).

-type state() :: #{
    driver_type := atom(),
    channels => #{channel_id() := channel_state()},
    enable_prepared => boolean()
}.

-type channel_state() ::
    #{
        pool_name => resource_id(),
        enable_prepared => boolean(),
        query_templates := #{
            prepstmt := {emqx_template_sql:statement(), emqx_template_sql:row_template()},
            batch := tuple()
        },
        prepared_refs := #{Client :: pid() := datalayers:prepared_statement()}
    }.

-type channel_config() ::
    #{
        parameters := #{
            sql := binary(),
            undefined_vars_as_null := boolean()
        }
    }.

-define(sync, sync).
-define(async, async).

-define(prepare, true).
-define(no_prepare, false).

resource_type() -> datalayers.

callback_mode() -> async_if_possible.

%% ================================================================================
on_start(
    InstId,
    Config = #{
        server := Server,
        parameters := #{
            username := Username,
            password := Password,
            database := _Database,
            enable_prepared := EnablePrepared
        } = _Parameters,
        pool_size := PoolSize,
        ssl := SSL
    }
) ->
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, ?DATALAYERS_HOST_OPTIONS),
    ?SLOG(info, #{
        msg => "starting_datalayers_arrow_flight_connector",
        connector => InstId,
        config => emqx_utils:redact(Config)
    }),
    %% TODO: Handle SSL options properly
    SslOpts =
        case maps:get(enable, SSL) of
            true -> #{};
            false -> #{}
        end,
    ConnConfig =
        maps:merge(
            #{
                host => bin(Host),
                port => Port,
                username => Username,
                password => Password
            },
            SslOpts
        ),
    PoolOpts = [
        {conn_config, ConnConfig},
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
        {pool_size, PoolSize}
    ],

    InitState = #{
        channels => #{},
        enable_prepared => EnablePrepared
    },
    case emqx_resource_pool:start(InstId, ?MODULE, PoolOpts) of
        ok ->
            {ok, InitState};
        {error, {start_pool_failed, _, Reason}} ->
            ?SLOG(error, #{
                msg => "datalayers_arrow_flight_connector_start_failed",
                reason => Reason,
                config => emqx_utils:redact(Config)
            }),
            {error, Reason}
    end.

on_stop(InstId, _State) ->
    ?SLOG(info, #{
        msg => "stoping_datalayers_arrow_flight_connector",
        connector => InstId
    }),
    %% TODO: close all channels prepared statements
    lists:foreach(
        fun({_Name, Worker}) ->
            case ecpool_worker:client(Worker) of
                %% Close prepare
                {ok, Conn} -> datalayers:stop(Conn);
                _ -> ok
            end
        end,
        ecpool:workers(InstId)
    ),
    emqx_resource_pool:stop(InstId).

-spec on_add_channel(
    InstId :: resource_id(),
    State :: state(),
    ChannelId :: channel_id(),
    ChannelConfig :: channel_config()
) -> {ok, state()} | {error, term()}.
on_add_channel(_InstId, #{channels := Channels} = _State, ChannelId, _ChannelConfig) when
    is_map_key(ChannelId, Channels)
->
    {error, already_exists};
on_add_channel(
    InstId,
    #{channels := Channels} = State,
    ChannelId,
    ChannelConfig
) ->
    {ok, ChannelState} = create_channel_state(InstId, ChannelId, State, ChannelConfig),
    {ok, State#{channels => Channels#{ChannelId => ChannelState}}}.

on_remove_channel(_InstId, #{channels := Channels} = _State, ChannelId) when
    not is_map_key(ChannelId, Channels)
->
    {error, not_found};
on_remove_channel(
    _InstId, #{channels := Channels} = State, ChannelId
) ->
    ChannelState = maps:get(ChannelId, Channels),
    ok = destory_channel_state(ChannelState),
    NewState = State#{channels => maps:remove(ChannelId, Channels)},
    {ok, NewState}.

create_channel_state(
    InstId,
    ChannelId,
    #{enable_prepared := EnablePrepared} = _State,
    #{parameters := ActionParameters} = _ChannelConfig
) ->
    ChannelState0 = parse_sql_template(ChannelId, ActionParameters),
    NChannelState = ChannelState0#{
        pool_name => InstId,
        enable_prepared => EnablePrepared
    },
    {ok, init_prepare(NChannelState)}.

destory_channel_state(#{enable_prepared := false} = _ChannelState) ->
    ok;
destory_channel_state(#{prepared_refs := PrepStatements} = _ChannelState) when
    map_size(PrepStatements) == 0
->
    ok;
destory_channel_state(#{prepared_refs := PrepStatements} = _ChannelState) when
    is_map(PrepStatements)
->
    maps:foreach(
        fun(Client, PrepRef) ->
            _ = close_statement_on_conn(Client, PrepRef)
        end,
        PrepStatements
    );
destory_channel_state(_ChannelState) ->
    ok.

on_get_channels(InstId) ->
    emqx_bridge_v2:get_channels_for_connector(InstId).

on_get_status(InstId, _State) ->
    health_check(InstId).

on_get_channel_status(InstId, _ChannelId, _State) ->
    %% TODO: Check the status for each channel.
    %% e.g.
    %% - check the prepared statements
    %% - check sql database/table existed
    health_check(InstId).

health_check(InstId) ->
    case do_health_check(InstId) of
        true -> ?status_connected;
        false -> ?status_disconnected
    end.

do_health_check(InstId) ->
    lists:all(
        fun({_Name, Worker}) ->
            case ecpool_worker:client(Worker) of
                {ok, Conn} ->
                    case datalayers:execute(Conn, <<"SELECT VERSION()">>) of
                        {ok, [[Res]]} when is_binary(Res) -> true;
                        {error, _} -> false
                    end;
                _ ->
                    false
            end
        end,
        ecpool:workers(InstId)
    ).

on_query(InstId, {_ChannelId, _} = Query, State) ->
    ?TRACE("QUERY", "datalayers_arrow_flight_connector_received_sql_query", #{
        instance_id => InstId,
        query => Query,
        state => emqx_utils:redact(State)
    }),
    do_query(InstId, ?sync, [Query], State, []).

on_batch_query(InstId, [{_ChannelId, _} | _] = Querys, State) ->
    ?TRACE("QUERY", "datalayers_arrow_flight_connector_received_batch_sql_query", #{
        instance_id => InstId,
        querys => Querys,
        state => emqx_utils:redact(State)
    }),
    do_query(InstId, ?sync, Querys, State, []).

on_query_async(InstId, {_ChannelId, _Message} = Query, ResCallback, State) ->
    ?TRACE("QUERY", "datalayers_arrow_flight_connector_received_async_sql_query", #{
        instance_id => InstId,
        query => Query,
        state => emqx_utils:redact(State)
    }),
    do_query(InstId, ?async, [Query], State, [ResCallback]).

on_batch_query_async(InstId, Querys, ResCallback, State) ->
    ?TRACE("QUERY", "datalayers_arrow_flight_connector_received_async_batch_sql_query", #{
        instance_id => InstId,
        querys => Querys,
        state => emqx_utils:redact(State)
    }),
    do_query(InstId, ?async, Querys, State, [ResCallback]).

-spec do_query(
    InstId :: resource_id(),
    QueryMode :: ?sync | ?async,
    Querys :: [{channel_id(), term()}],
    State :: state(),
    InitArgs :: [term()]
) ->
    {ok, _Result} | {error, _Reason}.
do_query(
    InstId, QueryMode, [{ChannelId, _Data} | _] = Querys, #{channels := Channels} = State, InitArgs
) ->
    ChannelState = maps:get(ChannelId, Channels),
    EnablePrepared = enable_prepared(State, ChannelState),
    case get_template(EnablePrepared, ChannelState) of
        undefined ->
            {error, no_template_fonud};
        Template ->
            DriverFunArgs = proc_sql_params(
                {EnablePrepared, Template},
                Querys,
                ChannelState,
                InitArgs
            ),
            FuncMode = {QueryMode, EnablePrepared},
            pick_and_do(InstId, FuncMode, DriverFunArgs)
    end.

pick_and_do(InstId, FuncMode, DriverFunArgs) ->
    try
        ecpool:pick_and_do(
            InstId, {?MODULE, call_driver, [FuncMode, DriverFunArgs]}, no_handover
        )
    of
        {error, _Reason} = Err ->
            Err;
        Result ->
            Result
    catch
        _:_:_ ->
            {error, {unrecoverable_error, invalid_request}}
    end.

proc_sql_params({?prepare, PrepStmtTemplate}, Querys, ChannelState, InitArgs) ->
    RenderedRows = [render_prepare_sql_row(PrepStmtTemplate, Query) || {_, Query} <- Querys],
    [maps:get(prepared_refs, ChannelState), RenderedRows | InitArgs];
proc_sql_params({?no_prepare, BatchTemplate}, Querys, ChannelState, InitArgs) ->
    RenderedSql = bin(render_batch_sql_row(BatchTemplate, Querys, ChannelState)),
    [RenderedSql | InitArgs].

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

render_row(RowTemplate, Data, ChannelState) ->
    RenderOpts =
        case maps:get(undefined_vars_as_null, ChannelState, false) of
            % NOTE:
            %  Ignoring errors here, missing variables are set to "'undefined'" due to backward
            %  compatibility requirements.
            false -> #{escaping => mysql, undefined => <<"undefined">>};
            true -> #{escaping => mysql}
        end,
    {Row, _Errors} = emqx_template_sql:render(RowTemplate, {emqx_jsonish, Data}, RenderOpts),
    Row.

%% =================================================================
%% ecpool callback

connect(Opts) ->
    #{password := Password} = Config = proplists:get_value(conn_config, Opts),
    datalayers:connect(Config#{password => emqx_secret:unwrap(Password)}).

call_driver(Client, {_QueryMode, true} = FuncMode, Args0) ->
    [PreparedRefs | Args] = Args0,
    %% Prepared statements
    PreparedRef = maps:get(Client, PreparedRefs),
    do_call_driver(Client, driver_fun_name(FuncMode), [PreparedRef | Args]);
call_driver(Client, {_QueryMode, false} = FuncMode, Args) ->
    do_call_driver(Client, driver_fun_name(FuncMode), Args).

do_call_driver(Client, FuncName, Args) ->
    case erlang:apply(datalayers, FuncName, [Client | Args]) of
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

parse_batch_sql(ChannelId, Query, AccIn) ->
    case emqx_utils_sql:get_statement_type(Query) of
        insert ->
            case emqx_utils_sql:split_insert(Query) of
                {ok, SplitedInsert} ->
                    AccIn#{batch => parse_splited_sql(SplitedInsert)};
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "parse_insert_sql_statement_failed",
                        sql => Query,
                        reason => Reason
                    }),
                    AccIn
            end;
        select ->
            AccIn;
        Type ->
            ?SLOG(error, #{
                msg => "invalid_sql_statement_type",
                sql => Query,
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

init_prepare(ChannelState = #{enable_prepared := false}) ->
    ChannelState;
init_prepare(ChannelState = #{query_templates := Templates}) when
    %% No `batch` or `prepstmt`
    map_size(Templates) == 0
->
    ChannelState;
init_prepare(ChannelState = #{}) ->
    case prepare_sql(ChannelState) of
        {ok, PrepStatements} when is_map(PrepStatements) ->
            %% each client holds a prepared statements reference
            ChannelState#{prepared_refs => PrepStatements};
        _Error ->
            ChannelState#{prepared_refs => {error, 'TODO_ERRORS'}}
    end.

prepare_sql(#{pool_name := PoolName, query_templates := #{prepstmt := PrepStmt}}) ->
    prepare_sql(PoolName, PrepStmt).

prepare_sql(PoolName, {SqlStatement0, _RowTemplates}) ->
    SqlStatement = bin(SqlStatement0),
    case do_prepare_sql(PoolName, SqlStatement) of
        {ok, _PrepStatements} = Ok ->
            %% prepare for reconnect
            ecpool:add_reconnect_callback(PoolName, {?MODULE, prepare_sql_to_conn, [SqlStatement]}),
            Ok;
        Error ->
            Error
    end.

do_prepare_sql(PoolName, SqlStatement) ->
    do_prepare_to_conns(
        ecpool:workers(PoolName),
        SqlStatement,
        _InitPrepStatements = #{}
    ).

do_prepare_to_conns([{_Name, Worker} | Rest], SqlStatement, AccIn) ->
    {ok, Client} = ecpool_worker:client(Worker),
    case prepare_sql_to_conn(Client, SqlStatement) of
        {ok, PrepareRef} when is_reference(PrepareRef) ->
            do_prepare_to_conns(Rest, SqlStatement, AccIn#{Client => PrepareRef});
        Error ->
            Error
    end;
do_prepare_to_conns([], _SqlStatement, AccOut) ->
    {ok, AccOut}.

%% Prepare SQL statement on a specific connection
prepare_sql_to_conn(Client, SqlStatement) ->
    datalayers:prepare(Client, SqlStatement).

close_statement_on_conn(Client, PrepareRef) ->
    datalayers:close_prepared(Client, PrepareRef).

bin(Bin) when is_binary(Bin) ->
    Bin;
bin(List) when is_list(List) ->
    iolist_to_binary(List);
bin(Atom) when is_atom(Atom) ->
    erlang:atom_to_binary(Atom).
