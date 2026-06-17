%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_oracle).

-behaviour(emqx_resource).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_trace.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(UNHEALTHY_TARGET_MSG,
    "Oracle table is invalid. Please check if the table exists in Oracle Database."
).
-define(UNSUPPORTED_SQL_STATEMENT_MSG,
    "unsupported_sql_statement: DDL, DCL, and transaction control statements are not supported "
    "in Oracle Action SQL templates."
).

-define(DEFAULT_ROLE, normal).
-define(SYSDBA_ROLE, sysdba).

%%====================================================================
%% Exports
%%====================================================================

%% callbacks for behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3
]).

%% callbacks for ecpool
-export([connect/1, prepare_sql_to_conn/3, get_reconnect_callback_signature/1]).

%% Internal exports used to execute code with ecpool worker
-export([
    query/3,
    execute_batch/3,
    do_get_status/1
]).

-export([
    oracle_host_options/0
]).

-export_type([state/0]).

-define(ORACLE_DEFAULT_PORT, 1521).
-define(SYNC_QUERY_MODE, no_handover).
-define(DEFAULT_POOL_SIZE, 8).
-define(OPT_TIMEOUT, 30000).
-define(MAX_CURSORS, 10).
-define(ORACLE_HOST_OPTIONS, #{
    default_port => ?ORACLE_DEFAULT_PORT
}).

-type prepares() :: #{atom() => binary()}.
-type prepare_state() ::
    prepares()
    | {error, _Reason :: term(), prepares()}
    | {error, prepares()}.
-type params_tokens() :: #{atom() => list()}.

-type state() ::
    #{
        pool_name := binary(),
        installed_channels := map(),
        health_check_timeout := timeout(),
        prepare_sql := prepare_state(),
        params_tokens := params_tokens(),
        batch_params_tokens := params_tokens()
    }.

resource_type() -> oracle.

% As ecpool is not monitoring the worker's PID when doing a handover_async, the
% request can be lost if worker crashes. Thus, it's better to force requests to
% be sync for now.
callback_mode() -> always_sync.

-spec on_start(binary(), hocon:config()) -> {ok, state()} | {error, _}.
on_start(
    InstId,
    #{
        server := Server,
        username := User
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_oracle_connector",
        connector => InstId,
        config => emqx_utils:redact(Config)
    }),
    ?tp(oracle_bridge_started, #{instance_id => InstId, config => Config}),
    {ok, _} = application:ensure_all_started(ecpool),
    {ok, _} = application:ensure_all_started(jamdb_oracle),
    jamdb_oracle_conn:set_max_cursors_number(?MAX_CURSORS),

    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, oracle_host_options()),
    Sid = maps:get(sid, Config, ""),
    ServiceName =
        case maps:get(service_name, Config, undefined) of
            undefined -> undefined;
            ServiceName0 -> emqx_utils_conv:str(ServiceName0)
        end,
    Role = convert_role_to_integer(maps:get(role, Config, ?DEFAULT_ROLE)),
    Options = [
        {host, Host},
        {port, Port},
        {role, Role},
        {user, emqx_utils_conv:str(User)},
        {password, maps:get(password, Config, "")},
        {sid, emqx_utils_conv:str(Sid)},
        {service_name, ServiceName},
        {pool_size, maps:get(pool_size, Config, ?DEFAULT_POOL_SIZE)},
        {timeout, ?OPT_TIMEOUT},
        {app_name, "EMQX Data To Oracle Database Action"}
    ],
    PoolName = InstId,
    HCTimeout = emqx_resource:get_health_check_timeout(Config),
    State = #{
        pool_name => PoolName,
        installed_channels => #{},
        health_check_timeout => HCTimeout
    },
    case emqx_resource_pool:start(InstId, ?MODULE, Options) of
        ok ->
            {ok, State};
        {error, Reason} ->
            ?tp(
                oracle_connector_start_failed,
                #{error => Reason}
            ),
            {error, Reason}
    end.

on_stop(InstId, #{pool_name := PoolName}) ->
    ?SLOG(info, #{
        msg => "stopping_oracle_connector",
        connector => InstId
    }),
    ?tp(oracle_bridge_stopped, #{instance_id => InstId}),
    emqx_resource_pool:stop(PoolName).

on_add_channel(
    _InstId,
    #{
        installed_channels := InstalledChannels,
        pool_name := PoolName
    } = OldState,
    ChannelId,
    ChannelConfig
) ->
    {ok, ChannelState} = create_channel_state(ChannelId, PoolName, ChannelConfig),
    NewInstalledChannels = maps:put(ChannelId, ChannelState, InstalledChannels),
    %% Update state
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState}.

create_channel_state(
    ChannelId,
    PoolName,
    #{parameters := Conf} = _ChannelConfig
) ->
    State0 = parse_prepare_sql(ChannelId, Conf),
    State1 = init_prepare(PoolName, State0),
    {ok, State1}.

on_remove_channel(
    _InstId,
    #{
        installed_channels := InstalledChannels
    } = OldState,
    ChannelId
) ->
    NewInstalledChannels = maps:remove(ChannelId, InstalledChannels),
    %% Update state
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState}.

on_get_channel_status(
    _ResId,
    ChannelId,
    #{
        pool_name := PoolName,
        installed_channels := Channels
    } = _State
) ->
    State = maps:get(ChannelId, Channels),
    case do_check_prepares(ChannelId, PoolName, State) of
        ok ->
            ?status_connected;
        {error, undefined_table} ->
            %% return new state indicating that we are connected but the target table is not created
            {?status_disconnected, {unhealthy_target, ?UNHEALTHY_TARGET_MSG}};
        {error, unsupported_sql_statement} ->
            {?status_disconnected, {unhealthy_target, ?UNSUPPORTED_SQL_STATEMENT_MSG}};
        {error, _Reason} ->
            %% do not log error, it is logged in prepare_sql_to_conn
            ?status_connecting
    end.

on_get_channels(ResId) ->
    emqx_bridge_v2:get_channels_for_connector(ResId).

on_query(InstId, {TypeOrKey, NameOrSQL}, #{pool_name := _PoolName} = State) ->
    on_query(InstId, {TypeOrKey, NameOrSQL, []}, State);
on_query(
    InstId,
    {TypeOrKey, NameOrSQL, Params},
    #{
        pool_name := PoolName,
        installed_channels := Channels
    } = _ConnectorState
) ->
    State = maps:get(TypeOrKey, Channels, #{}),
    ?SLOG(debug, #{
        msg => "oracle_connector_received_sql_query",
        connector => InstId,
        type => TypeOrKey,
        sql => NameOrSQL,
        state => State
    }),
    Type = query,
    {NameOrSQL2, Data} = proc_sql_params(TypeOrKey, NameOrSQL, Params, State),
    Res = on_sql_query(InstId, TypeOrKey, PoolName, Type, ?SYNC_QUERY_MODE, NameOrSQL2, Data),
    handle_result(Res).

on_batch_query(
    InstId,
    BatchReq,
    #{
        pool_name := PoolName,
        installed_channels := Channels
    } = ConnectorState
) ->
    case BatchReq of
        [{Key, _} = Request | _] ->
            BinKey = to_bin(Key),
            State = maps:get(BinKey, Channels),
            #{
                params_tokens := Tokens,
                prepare_sql := Sts
            } = State,
            case maps:get(BinKey, Tokens, undefined) of
                undefined ->
                    Log = #{
                        connector => InstId,
                        first_request => Request,
                        state => State,
                        msg => "batch_prepare_not_implemented"
                    },
                    ?SLOG(error, Log),
                    {error, {unrecoverable_error, batch_prepare_not_implemented}};
                TokenList ->
                    {_, Datas} = lists:unzip(BatchReq),
                    Datas2 = [emqx_placeholder:proc_sql(TokenList, Data) || Data <- Datas],
                    St = maps:get(BinKey, Sts),
                    case
                        on_sql_query(
                            InstId, BinKey, PoolName, execute_batch, ?SYNC_QUERY_MODE, St, Datas2
                        )
                    of
                        {ok, Results} ->
                            handle_batch_result(Results, 0);
                        Result ->
                            Result
                    end
            end;
        _ ->
            Log = #{
                connector => InstId,
                request => BatchReq,
                state => ConnectorState,
                msg => "invalid_request"
            },
            ?SLOG(error, Log),
            {error, {unrecoverable_error, invalid_request}}
    end.

proc_sql_params(query, SQLOrKey, Params, _State) ->
    {SQLOrKey, Params};
proc_sql_params(TypeOrKey, SQLOrData, Params, #{
    params_tokens := ParamsTokens, prepare_sql := PrepareSQL
}) ->
    Key = to_bin(TypeOrKey),
    case maps:get(Key, ParamsTokens, undefined) of
        undefined ->
            {SQLOrData, Params};
        Tokens ->
            case maps:get(Key, PrepareSQL, undefined) of
                undefined ->
                    {SQLOrData, Params};
                SQL ->
                    {SQL, emqx_placeholder:proc_sql(Tokens, SQLOrData)}
            end
    end.

on_sql_query(InstId, ChannelId, PoolName, Type, ApplyMode, NameOrSQL, Data) ->
    emqx_trace:rendered_action_template(ChannelId, #{
        type => Type,
        apply_mode => ApplyMode,
        name_or_sql => NameOrSQL,
        data => #emqx_trace_format_func_data{function = fun trace_format_data/1, data = Data}
    }),
    case ecpool:pick_and_do(PoolName, {?MODULE, Type, [NameOrSQL, Data]}, ApplyMode) of
        {error, Reason} = Result ->
            ?tp(
                oracle_connector_query_return,
                #{error => Reason}
            ),
            ?SLOG(error, #{
                msg => "oracle_connector_do_sql_query_failed",
                connector => InstId,
                type => Type,
                sql => NameOrSQL,
                reason => Reason
            }),
            case Reason of
                ecpool_empty ->
                    {error, {recoverable_error, Reason}};
                _ ->
                    Result
            end;
        Result ->
            ?tp(
                oracle_connector_query_return,
                #{result => Result}
            ),
            Result
    end.

trace_format_data(Data0) ->
    %% In batch request, we get a two level list
    {'$array$', lists:map(fun insert_array_marker_if_list/1, Data0)}.

insert_array_marker_if_list(List) when is_list(List) ->
    {'$array$', List};
insert_array_marker_if_list(Item) ->
    Item.

on_get_status(_InstId, #{pool_name := PoolName} = ConnState) ->
    #{health_check_timeout := HCTimeout} = ConnState,
    Opts = #{
        check_fn => fun ?MODULE:do_get_status/1,
        timeout => HCTimeout
    },
    emqx_resource_pool:common_health_check_workers(PoolName, Opts).

do_get_status(Conn) ->
    Res = do_sql_query(Conn, "select 1 from dual"),
    case ok == element(1, Res) of
        true ->
            ok;
        false ->
            case Res of
                {error, _Reason} -> Res;
                _ -> {error, Res}
            end
    end.

do_check_prepares(
    _ChannelId,
    _PoolName,
    #{
        prepare_sql := {error, Reason, _Prepares}
    } = _State
) ->
    {error, Reason};
do_check_prepares(
    _ChannelId,
    _PoolName,
    #{
        prepare_sql := {error, _Prepares}
    } = _State
) ->
    {error, undefined_table};
do_check_prepares(
    ChannelId,
    PoolName,
    State
) ->
    #{
        prepare_sql := #{ChannelId := SQL},
        params_tokens := #{ChannelId := Tokens}
    } = State,

    % it's already connected. Verify if target table still exists
    Workers = [Worker || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
    lists:foldl(
        fun
            (WorkerPid, ok) ->
                case ecpool_worker:client(WorkerPid) of
                    {ok, Conn} ->
                        case check_if_table_exists(Conn, SQL, Tokens) of
                            {error, undefined_table} ->
                                {error, undefined_table};
                            {error, unsupported_sql_statement} ->
                                {error, unsupported_sql_statement};
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
    ).

%% ===================================================================

oracle_host_options() ->
    ?ORACLE_HOST_OPTIONS.

connect(Opts) ->
    jamdb_oracle:start_link(Opts).

sql_query_to_str(SqlQuery) ->
    emqx_utils_conv:str(SqlQuery).

sql_params_to_str(Params) when is_list(Params) ->
    lists:map(
        fun
            (false) -> "0";
            (true) -> "1";
            (null) -> null;
            (Value) -> emqx_utils_conv:str(Value)
        end,
        Params
    ).

query(Conn, SQL, Params) ->
    Ret = do_sql_query(Conn, {sql_query_to_str(SQL), sql_params_to_str(Params)}),
    ?tp(oracle_query, #{conn => Conn, sql => SQL, params => Params, result => Ret}),
    handle_result(Ret).

execute_batch(Conn, SQL, ParamsList) ->
    ParamsListStr = lists:map(fun sql_params_to_str/1, ParamsList),
    Ret = do_sql_query(Conn, {batch, sql_query_to_str(SQL), ParamsListStr}),
    ?tp(oracle_batch_query, #{conn => Conn, sql => SQL, params => ParamsList, result => Ret}),
    handle_result(Ret).

do_sql_query(Conn, Req) ->
    try
        jamdb_oracle:sql_query(Conn, Req)
    catch
        exit:{noproc, _} = Reason ->
            %% Note [jamdb oracle race condition]
            %% Race condition?  Maybe the connection process within the ecpool worker is
            %% restarting after an update?
            %% Trying again should not fail consistently.
            {error, Reason}
    end.

parse_prepare_sql(ChannelId, Config) ->
    SQL =
        case maps:get(prepare_statement, Config, undefined) of
            undefined ->
                case maps:get(sql, Config, undefined) of
                    undefined -> #{};
                    Template -> #{ChannelId => Template}
                end;
            Any ->
                Any
        end,
    parse_prepare_sql(maps:to_list(SQL), #{}, #{}).

parse_prepare_sql([{Key, H} | T], Prepares, Tokens) ->
    {PrepareSQL, ParamsTokens} = emqx_placeholder:preproc_sql(H, ':n'),
    parse_prepare_sql(
        T, Prepares#{Key => PrepareSQL}, Tokens#{Key => ParamsTokens}
    );
parse_prepare_sql([], Prepares, Tokens) ->
    #{
        prepare_sql => Prepares,
        params_tokens => Tokens
    }.

init_prepare(PoolName, State = #{prepare_sql := Prepares, params_tokens := TokensMap}) ->
    case prepare_sql(Prepares, PoolName, TokensMap) of
        {ok, Sts} ->
            State#{prepare_sql := Sts};
        Error ->
            LogMeta = #{
                msg => <<"oracle_init_prepare_statement_failed">>, error => Error
            },
            ?SLOG(error, LogMeta),
            %% mark the prepare_sql as failed
            State#{prepare_sql => {error, prepare_error_reason(Error), Prepares}}
    end.

prepare_error_reason({error, Reason}) ->
    Reason.

prepare_sql(Prepares, PoolName, TokensMap) when is_map(Prepares) ->
    prepare_sql(maps:to_list(Prepares), PoolName, TokensMap);
prepare_sql(Prepares, PoolName, TokensMap) ->
    case do_prepare_sql(Prepares, PoolName, TokensMap) of
        {ok, _Sts} = Ok ->
            %% prepare for reconnect
            ecpool:add_reconnect_callback(PoolName, {?MODULE, prepare_sql_to_conn, [Prepares]}),
            Ok;
        Error ->
            Error
    end.

do_prepare_sql(Prepares, PoolName, TokensMap) ->
    do_prepare_sql(ecpool:workers(PoolName), Prepares, PoolName, TokensMap, #{}).

do_prepare_sql([{_Name, Worker} | T], Prepares, PoolName, TokensMap, _LastSts) ->
    {ok, Conn} = ecpool_worker:client(Worker),
    case prepare_sql_to_conn(Conn, Prepares, TokensMap) of
        {ok, Sts} ->
            do_prepare_sql(T, Prepares, PoolName, TokensMap, Sts);
        Error ->
            Error
    end;
do_prepare_sql([], _Prepares, _PoolName, _TokensMap, LastSts) ->
    {ok, LastSts}.

prepare_sql_to_conn(Conn, Prepares, TokensMap) ->
    prepare_sql_to_conn(Conn, Prepares, TokensMap, #{}).

prepare_sql_to_conn(Conn, [], _TokensMap, Statements) when is_pid(Conn) ->
    {ok, Statements};
prepare_sql_to_conn(Conn, [{Key, SQL} | PrepareList], TokensMap, Statements) when is_pid(Conn) ->
    LogMeta = #{msg => "oracle_prepare_statement", name => Key, prepare_sql => SQL},
    Tokens = maps:get(Key, TokensMap, []),
    ?SLOG(info, LogMeta),
    case check_if_table_exists(Conn, SQL, Tokens) of
        ok ->
            ?SLOG(info, LogMeta#{result => success}),
            prepare_sql_to_conn(Conn, PrepareList, TokensMap, Statements#{Key => SQL});
        {error, undefined_table} = Error ->
            %% Target table is not created
            ?SLOG(error, LogMeta#{result => failed, reason => "table does not exist"}),
            ?tp(oracle_undefined_table, #{}),
            Error;
        Error ->
            Error
    end.

%% this callback accepts the arg list provided to
%% ecpool:add_reconnect_callback(PoolName, {?MODULE, prepare_sql_to_conn, [Templates]})
%% so ecpool_worker can de-duplicate the callbacks based on the signature.
get_reconnect_callback_signature([[{ChannelId, _Template}]]) ->
    ChannelId.

check_if_table_exists(Conn, SQL, _Tokens0) ->
    case sql_has_parse_side_effect(SQL) of
        true ->
            {error, unsupported_sql_statement};
        false ->
            check_if_sql_parseable(Conn, SQL, 1)
    end.

check_if_sql_parseable(Conn, SQL, Retries) ->
    ParseSQL =
        "declare "
        "  c integer; "
        "begin "
        "  c := dbms_sql.open_cursor; "
        "  dbms_sql.parse(c, :1, dbms_sql.native); "
        "  dbms_sql.close_cursor(c); "
        "exception "
        "  when others then "
        "    if dbms_sql.is_open(c) then "
        "      dbms_sql.close_cursor(c); "
        "    end if; "
        "    raise; "
        "end;",
    case do_sql_query(Conn, {ParseSQL, [binary_to_list(SQL)]}) of
        {ok, [{proc_result, 0, _Description}]} ->
            ok;
        {ok, [{proc_result, RetCode, _Description}]} when
            RetCode =:= 904; RetCode =:= 942
        ->
            %% ORA-00904: invalid identifier
            %% ORA-00942: table or view does not exist
            {error, undefined_table};
        {ok, [{proc_result, 1013, _Description}]} when Retries > 0 ->
            %% ORA-01013: user requested cancel of current operation
            check_if_sql_parseable(Conn, SQL, Retries - 1);
        {ok, [{proc_result, _RetCode, Description}]} ->
            handle_parse_error_description(Description);
        {error, {noproc, _}} when Retries > 0 ->
            %% See Note [jamdb oracle race condition]
            check_if_sql_parseable(Conn, SQL, Retries - 1);
        {error, socket, closed} when Retries > 0 ->
            check_if_sql_parseable(Conn, SQL, Retries - 1);
        Reason ->
            {error, Reason}
    end.

handle_parse_error_description(Description) ->
    case oracle_error_codes(Description) of
        #{<<"ORA-00904">> := true} ->
            {error, undefined_table};
        #{<<"ORA-00942">> := true} ->
            {error, undefined_table};
        #{<<"ORA-01013">> := true} ->
            ok;
        _ ->
            {error, Description}
    end.

oracle_error_codes(Description) ->
    case
        re:run(iolist_to_binary(Description), <<"(ORA-[0-9]+)">>, [
            global,
            {capture, first, binary}
        ])
    of
        {match, OraCodes} ->
            maps:from_keys([ErrorCode || [ErrorCode] <- OraCodes], true);
        nomatch ->
            #{}
    end.

sql_has_parse_side_effect(SQL) ->
    case first_sql_token(SQL) of
        <<"alter">> -> true;
        <<"analyze">> -> true;
        <<"associate">> -> true;
        <<"audit">> -> true;
        <<"comment">> -> true;
        <<"commit">> -> true;
        <<"create">> -> true;
        <<"disassociate">> -> true;
        <<"drop">> -> true;
        <<"explain">> -> true;
        <<"flashback">> -> true;
        <<"grant">> -> true;
        <<"lock">> -> true;
        <<"noaudit">> -> true;
        <<"purge">> -> true;
        <<"rename">> -> true;
        <<"revoke">> -> true;
        <<"rollback">> -> true;
        <<"savepoint">> -> true;
        <<"set">> -> true;
        <<"truncate">> -> true;
        _ -> false
    end.

first_sql_token(SQL) ->
    take_sql_token(skip_sql_prefix(to_bin(SQL)), <<>>).

skip_sql_prefix(<<C, Rest/binary>>) when
    C =:= $\s; C =:= $\t; C =:= $\n; C =:= $\r; C =:= $\f; C =:= $\v
->
    skip_sql_prefix(Rest);
skip_sql_prefix(<<"--", Rest/binary>>) ->
    skip_sql_prefix(skip_line_comment(Rest));
skip_sql_prefix(<<"/*", Rest/binary>>) ->
    skip_sql_prefix(skip_block_comment(Rest));
skip_sql_prefix(SQL) ->
    SQL.

skip_line_comment(<<"\n", Rest/binary>>) ->
    Rest;
skip_line_comment(<<"\r", Rest/binary>>) ->
    Rest;
skip_line_comment(<<_, Rest/binary>>) ->
    skip_line_comment(Rest);
skip_line_comment(<<>>) ->
    <<>>.

skip_block_comment(<<"*/", Rest/binary>>) ->
    Rest;
skip_block_comment(<<_, Rest/binary>>) ->
    skip_block_comment(Rest);
skip_block_comment(<<>>) ->
    <<>>.

take_sql_token(<<C, Rest/binary>>, Acc) when
    (C >= $A andalso C =< $Z) orelse
        (C >= $a andalso C =< $z) orelse
        (C >= $0 andalso C =< $9) orelse
        C =:= $_
->
    take_sql_token(Rest, <<Acc/binary, (lower_ascii(C))>>);
take_sql_token(_Rest, Acc) ->
    Acc.

lower_ascii(C) when C >= $A, C =< $Z ->
    C + 32;
lower_ascii(C) ->
    C.

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
handle_result({error, {noproc, _}}) ->
    %% See Note [jamdb oracle race condition]
    {error, {recoverable_error, worker_restarting}};
handle_result({error, Error}) ->
    {error, {unrecoverable_error, Error}};
handle_result({error, socket, closed} = Error) ->
    {error, {recoverable_error, Error}};
handle_result({error, Type, Reason}) ->
    {error, {unrecoverable_error, {Type, Reason}}};
handle_result({ok, [{proc_result, RetCode, Reason}]}) when RetCode =/= 0 ->
    {error, {unrecoverable_error, {RetCode, Reason}}};
handle_result(Res) ->
    Res.

handle_batch_result([{affected_rows, RowCount} | Rest], Acc) ->
    handle_batch_result(Rest, Acc + RowCount);
handle_batch_result([{proc_result, RetCode, _Rows} | Rest], Acc) when RetCode =:= 0 ->
    handle_batch_result(Rest, Acc);
handle_batch_result([{proc_result, RetCode, Reason} | _Rest], _Acc) ->
    {error, {unrecoverable_error, {RetCode, Reason}}};
handle_batch_result([], Acc) ->
    {ok, Acc}.

convert_role_to_integer(?SYSDBA_ROLE) ->
    1;
convert_role_to_integer(_) ->
    0.
