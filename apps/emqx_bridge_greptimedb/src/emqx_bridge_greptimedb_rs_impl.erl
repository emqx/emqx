%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_greptimedb_rs_impl).

-behaviour(emqx_resource).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_trace.hrl").

%% `emqx_resource` API
-export([
    resource_type/0,
    callback_mode/0,

    on_start/2,
    on_stop/2,
    on_get_status/2,

    on_get_channels/1,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3,

    on_query/3,
    on_batch_query/3,
    on_query_async/4,
    on_batch_query_async/4
]).

%% API
-export([]).

%% Internal exports
-export([reply_callback/2]).
-ifdef(TEST).
-export([aggregate_message_results/3]).
-endif.

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(AUTO_RECONNECT_S, 2).

%% Allocatable resources
-define(greptimedb_client, greptimedb_client).

-define(client, client).
-define(database, database).
-define(installed_channels, installed_channels).
-define(write_syntax, write_syntax).
-define(precision, precision).
-define(field_types, field_types).

-type connector_config() :: #{
    dbname := database(),
    server := binary(),
    username => binary(),
    password => emqx_secret:t(binary())
}.
-type connector_state() :: #{
    ?client := greptimedb_rs:client(),
    ?database := binary(),
    ?installed_channels := #{channel_id() => channel_state()}
}.

-type database() :: binary().

-type channel_config() :: #{
    parameters := #{
        write_syntax := [map()],
        precision := ns | us | ms | s,
        field_types => #{binary() | atom() => json2}
    }
}.
-type channel_state() :: #{}.

-type query() :: {_Tag :: channel_id(), _Data :: map()}.

%%------------------------------------------------------------------------------
%% `emqx_resource' API
%%------------------------------------------------------------------------------

-spec resource_type() -> atom().
resource_type() ->
    greptimedb.

-spec callback_mode() -> callback_mode().
callback_mode() ->
    async_if_possible.

-spec on_start(connector_resource_id(), connector_config()) ->
    {ok, connector_state()} | {error, _Reason}.
on_start(ConnResId, ConnConfig) ->
    #{dbname := Database} = ConnConfig,
    ensure_consistent_ssl_opts(ConnConfig),
    ClientOpts = client_opts(ConnResId, ConnConfig),
    ok = emqx_resource:allocate_resource(ConnResId, ?MODULE, ?greptimedb_client, ConnResId),
    maybe
        {ok, Client} ?= greptimedb_rs:start_client(ClientOpts),
        ConnState = #{
            ?client => Client,
            ?database => Database,
            ?installed_channels => #{}
        },
        {ok, ConnState}
    end.

-spec on_stop(connector_resource_id(), connector_state()) -> ok.
on_stop(ConnResId, _ConnState) ->
    Res = greptimedb_rs:stop_client(#{pool_name => ConnResId}),
    ?tp("greptimedb_rs_connector_stop", #{instance_id => ConnResId}),
    Res.

-spec on_get_status(connector_resource_id(), connector_state()) ->
    ?status_connected | ?status_disconnected.
on_get_status(_ConnResId, #{?client := Client} = _ConnState) ->
    %% todo: make lib expose a way to health check it.
    try greptimedb_rs:query(Client, <<"select 1">>) of
        {ok, _} ->
            ?status_connected;
        {error, Reason} ->
            {?status_disconnected, Reason}
    catch
        Kind:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "greptimedb_rs_connector_health_check_exception",
                kind => Kind,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {?status_disconnected, {Kind, Reason}}
    end.

-spec on_get_channels(connector_resource_id()) ->
    [{action_resource_id(), channel_config()}].
on_get_channels(ConnResId) ->
    emqx_bridge_v2:get_channels_for_connector(ConnResId).

-spec on_add_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id(),
    channel_config()
) ->
    {ok, connector_state()}.
on_add_channel(_ConnResId, ConnState0, ChanResId, ActionConfig) ->
    #{parameters := Parameters} = ActionConfig,
    #{write_syntax := WriteSyntaxTemplate, precision := Precision} = Parameters,
    ActionState = #{
        ?write_syntax => emqx_bridge_greptimedb_utils:parse_write_syntax(
            WriteSyntaxTemplate, Precision
        ),
        ?precision => Precision,
        ?field_types => maps:get(field_types, Parameters, #{})
    },
    ConnState = maps:update_with(
        ?installed_channels,
        fun(Cs) -> Cs#{ChanResId => ActionState} end,
        ConnState0
    ),
    {ok, ConnState}.

-spec on_remove_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id()
) ->
    {ok, connector_state()}.
on_remove_channel(
    _ConnResId,
    ConnState0 = #{?installed_channels := InstalledChannels0},
    ChanResId
) when
    is_map_key(ChanResId, InstalledChannels0)
->
    InstalledChannels = maps:remove(ChanResId, InstalledChannels0),
    ConnState = ConnState0#{?installed_channels := InstalledChannels},
    {ok, ConnState};
on_remove_channel(_ConnResId, ConnState, _ChanResId) ->
    {ok, ConnState}.

-spec on_get_channel_status(
    connector_resource_id(),
    action_resource_id(),
    connector_state()
) ->
    ?status_connected | ?status_disconnected.
on_get_channel_status(
    _ConnResId,
    ChanResId,
    _ConnState = #{?installed_channels := InstalledChannels}
) when is_map_key(ChanResId, InstalledChannels) ->
    %% there seems to be no particular status for an action of this kind
    ?status_connected;
on_get_channel_status(_ConnResId, _ChanResId, _ConnState) ->
    ?status_disconnected.

-spec on_query(connector_resource_id(), query(), connector_state()) ->
    {ok, _Result} | {error, _Reason}.
on_query(
    ConnResId,
    {ChanResId, #{} = Data},
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    #{?database := Database} = ConnState,
    #{ChanResId := ActionState} = InstalledChannels,
    #{?write_syntax := WriteSyntax} = ActionState,
    FieldTypes = action_field_types(ActionState),
    case parse_batch_data(ConnResId, Database, [{ChanResId, Data}], WriteSyntax, FieldTypes) of
        {ok, TablesToPoints, _Failed = []} ->
            [Result] = do_insert_sync(TablesToPoints, ConnState, [], 1),
            Result;
        {error, ErrorPoints} ->
            {error, ErrorPoints}
    end;
on_query(_ConnResId, Query, _ConnState) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

-spec on_query_async(
    connector_resource_id(),
    {message_tag(), map()},
    {ReplyFun :: function(), Args :: list()},
    connector_state()
) -> {ok, pid()} | {error, term()}.
on_query_async(
    ConnResId,
    {ChanResId, #{} = Data},
    ReplyFnAndArgs,
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    #{?database := Database} = ConnState,
    #{ChanResId := ActionState} = InstalledChannels,
    #{?write_syntax := WriteSyntax} = ActionState,
    FieldTypes = action_field_types(ActionState),
    case parse_batch_data(ConnResId, Database, [{ChanResId, Data}], WriteSyntax, FieldTypes) of
        {ok, TablesToPoints, _Failed = []} ->
            do_insert_async(ChanResId, TablesToPoints, ConnState, ReplyFnAndArgs, [], 1);
        {error, ErrorPoints} ->
            {error, ErrorPoints}
    end;
on_query_async(_ConnResId, Query, _ReplyFnAndArgs, _ConnState) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

-spec on_batch_query(connector_resource_id(), [query()], connector_state()) ->
    ok | {error, term()}.
on_batch_query(
    ConnResId,
    [{ChanResId, _Data} | _] = Batch,
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    #{?database := Database} = ConnState,
    #{ChanResId := ActionState} = InstalledChannels,
    #{?write_syntax := WriteSyntax} = ActionState,
    FieldTypes = action_field_types(ActionState),
    case parse_batch_data(ConnResId, Database, Batch, WriteSyntax, FieldTypes) of
        {ok, TablesToPoints, Failed} ->
            do_insert_sync(TablesToPoints, ConnState, Failed, length(Batch));
        {error, ErrorPoints} ->
            {error, ErrorPoints}
    end;
on_batch_query(_ConnResId, Queries, _ConnectorState) ->
    {error, {unrecoverable_error, {invalid_batch, Queries}}}.

-spec on_batch_query_async(
    connector_resource_id(),
    [query()],
    {ReplyFun :: function(), Args :: list()},
    connector_state()
) ->
    ok | {error, term()}.
on_batch_query_async(
    ConnResId,
    [{ChanResId, _Data} | _] = Batch,
    ReplyFnAndArgs,
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    #{?database := Database} = ConnState,
    #{ChanResId := ActionState} = InstalledChannels,
    #{?write_syntax := WriteSyntax} = ActionState,
    FieldTypes = action_field_types(ActionState),
    case parse_batch_data(ConnResId, Database, Batch, WriteSyntax, FieldTypes) of
        {ok, TablesToPoints, Failed} ->
            do_insert_async(
                ChanResId, TablesToPoints, ConnState, ReplyFnAndArgs, Failed, length(Batch)
            );
        {error, ErrorPoints} ->
            {error, ErrorPoints}
    end;
on_batch_query_async(_ConnResId, Queries, _ReplyFnAndArgs, _ConnectorState) ->
    {error, {unrecoverable_error, {invalid_batch, Queries}}}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

reply_callback(Context0, Result0) ->
    Result = normalize_insert_result(Result0),
    Context = normalize_async_reply_context(Context0),
    #{iter := Iter0} = Context,
    case maps:next(Iter0) of
        none ->
            handle_last_async_result(Context, Result);
        {NextTable, PointsWithIndices, Iter} ->
            Context1 = Context#{iter := Iter},
            handle_async_result_continuation(Context1, Result, NextTable, PointsWithIndices)
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

bin(X) -> emqx_utils_conv:bin(X).

action_field_types(ActionState) ->
    maps:get(?field_types, ActionState, #{}).

normalize_async_reply_context(#{batch_size := _, last_points_by_index := _} = Context) ->
    Context;
normalize_async_reply_context(Context0) ->
    #{
        iter := Iter,
        last_indices := LastIndices,
        final_cont := FinalCont,
        final_res := FinalResults,
        client := Client
    } = Context0,
    #{
        res_id => maps:get(res_id, Context0, undefined),
        iter => Iter,
        last_points_by_index => count_points_by_index([
            {Index, undefined}
         || Index <- LastIndices
        ]),
        final_cont => FinalCont,
        result_acc => [{Index, 1, Result} || {Index, Result} <- FinalResults],
        client => Client,
        batch_size => legacy_batch_size(LastIndices, FinalResults, Iter)
    }.

legacy_batch_size(LastIndices, FinalResults, Iter) ->
    Indices =
        LastIndices ++
            [Index || {Index, _Result} <- FinalResults] ++
            legacy_remaining_indices(Iter),
    lists:max([1 | Indices]).

legacy_remaining_indices(Iter) ->
    legacy_remaining_indices(maps:next(Iter), []).

legacy_remaining_indices(none, Acc) ->
    Acc;
legacy_remaining_indices({_Table, PointsWithIndices, Iter}, Acc) ->
    Indices = [Index || {Index, _Point} <- PointsWithIndices],
    legacy_remaining_indices(maps:next(Iter), Indices ++ Acc).

%% erlnif wraps NIF-side failures as `nif_error'. Normalize them for the
%% resource buffer worker. This does not imply that other `{error, Reason}'
%% results are recoverable.
normalize_insert_result({error, {nif_error, Reason}}) ->
    {error, {unrecoverable_error, Reason}};
normalize_insert_result(Result) ->
    Result.

%% `Failed' carries the messages whose points did not transform, each keyed by
%% its position in the batch. The points keep their own positions too, so the
%% results the caller assembles line up with the queries even when only some of
%% them produced points.
parse_batch_data(ConnResId, Database, Batch, WriteSyntax, FieldTypes) ->
    case
        emqx_bridge_greptimedb_utils:parse_batch_data(
            ConnResId, Database, Batch, WriteSyntax, FieldTypes, native
        )
    of
        {ok, Points} ->
            ValidIndices = lists:seq(1, length(Batch)),
            {ok, group_points_by_table(index_points(ValidIndices, Points, WriteSyntax)), []};
        {ok, Points, BatchResults} ->
            Indexed = lists:enumerate(BatchResults),
            ValidIndices = [I || {I, valid} <- Indexed],
            Failed = [{I, Error} || {I, {error, _} = Error} <- Indexed],
            {ok, group_points_by_table(index_points(ValidIndices, Points, WriteSyntax)), Failed};
        {error, _} = Error ->
            Error
    end.

%% Every message renders against the same syntax lines, so a message whose
%% points transformed contributes one group per line. Repeat its batch position
%% that many times to key each group by the message it came from.
index_points(ValidIndices, Points, WriteSyntax) ->
    GroupsPerMessage = length(WriteSyntax),
    Indices = lists:append([lists:duplicate(GroupsPerMessage, I) || I <- ValidIndices]),
    lists:zip(Indices, Points).

group_points_by_table(IndexedPoints) ->
    TablesToPoints0 = maps:groups_from_list(
        fun({_, {#{table := Table}, _}}) -> Table end,
        fun({I, {_, Points}}) ->
            lists:map(fun(P) -> {I, P} end, Points)
        end,
        IndexedPoints
    ),
    maps:map(
        fun(_, Pss) -> lists:append(Pss) end,
        TablesToPoints0
    ).

do_insert_sync(TablesToPoints, ConnState, Failed, BatchSize) ->
    #{?client := Client} = ConnState,
    TableResults =
        maps:fold(
            fun(Table, PointsWithIndices, Acc) ->
                PointsByIndex = count_points_by_index(PointsWithIndices),
                Points = [Point || {_, Point} <- PointsWithIndices],
                Res0 = normalize_insert_result(greptimedb_rs:insert(Client, Table, Points)),
                [
                    {Index, PointCount, Res0}
                 || {Index, PointCount} <- PointsByIndex
                ] ++ Acc
            end,
            [],
            TablesToPoints
        ),
    Results = coalesce_mixed_batch_results(
        aggregate_message_results(BatchSize, TableResults, Failed)
    ),
    ?tp("greptime_rs_sync_batch_reply", #{results => Results}),
    Results.

do_insert_async(
    _ChanResId, TablesToPoints, _ConnState, ReplyFnAndArgs, Failed, BatchSize
) when
    map_size(TablesToPoints) == 0
->
    Results = aggregate_message_results(BatchSize, [], Failed),
    reply_async_result(ReplyFnAndArgs, BatchSize, Results),
    {ok, self()};
do_insert_async(ChanResId, TablesToPoints, ConnState, ReplyFnAndArgs0, Failed, BatchSize) ->
    #{?client := Client} = ConnState,
    Iter0 = maps:iterator(TablesToPoints),
    %% Already checked that map is non-empty.
    {Table, PointsWithIndices, Iter} = maps:next(Iter0),
    PointsByIndex = count_points_by_index(PointsWithIndices),
    Points = [Point || {_, Point} <- PointsWithIndices],
    Context = #{
        res_id => ChanResId,
        iter => Iter,
        last_points_by_index => PointsByIndex,
        final_cont => ReplyFnAndArgs0,
        result_acc => failed_results(Failed),
        client => Client,
        batch_size => BatchSize
    },
    ReplyFnAndArgs = {fun ?MODULE:reply_callback/2, [Context]},
    submit_async_insert(
        Client,
        Table,
        Points,
        PointsByIndex,
        maps:get(result_acc, Context),
        ReplyFnAndArgs0,
        BatchSize,
        Iter,
        ReplyFnAndArgs
    ).

handle_last_async_result(Context, Result) ->
    #{
        last_points_by_index := LastPointsByIndex,
        final_cont := ReplyFnAndArgs,
        result_acc := ResAcc0,
        batch_size := BatchSize
    } = Context,
    ResAcc = table_results(LastPointsByIndex, Result) ++ ResAcc0,
    FinalResults = aggregate_indexed_results(BatchSize, ResAcc),
    reply_async_result(ReplyFnAndArgs, BatchSize, FinalResults).

handle_async_result_continuation(Context0, Result, NextTable, PointsWithIndices) ->
    #{
        client := Client,
        last_points_by_index := LastPointsByIndex,
        result_acc := ResAcc0
    } = Context0,
    NextPointsByIndex = count_points_by_index(PointsWithIndices),
    Points = [Point || {_, Point} <- PointsWithIndices],
    ResAcc = table_results(LastPointsByIndex, Result) ++ ResAcc0,
    Context = Context0#{
        last_points_by_index := NextPointsByIndex,
        result_acc := ResAcc
    },
    ReplyFnAndArgs = {fun ?MODULE:reply_callback/2, [Context]},
    submit_async_insert(
        Client,
        NextTable,
        Points,
        NextPointsByIndex,
        ResAcc,
        maps:get(final_cont, Context),
        maps:get(batch_size, Context),
        maps:get(iter, Context),
        ReplyFnAndArgs
    ).

submit_async_insert(
    Client,
    Table,
    Points,
    PointsByIndex,
    ResultAcc,
    FinalReplyFnAndArgs,
    BatchSize,
    RemainingTables,
    ReplyFnAndArgs
) ->
    %% greptimedb_rs 0.2.0 declares only `{ok, pid()}' here, but ecpool may
    %% return transient submission errors before the callback is installed.
    AsyncResult = erlang:apply(
        greptimedb_rs, insert_async, [Client, Table, Points, ReplyFnAndArgs]
    ),
    case AsyncResult of
        {ok, _Pid} = Ok ->
            Ok;
        {error, _Reason} = Error ->
            ClassifiedError = classify_insert_result(Error),
            Results = aggregate_indexed_results(
                BatchSize,
                table_results(PointsByIndex, ClassifiedError) ++
                    unsubmitted_table_results(RemainingTables, ClassifiedError) ++
                    ResultAcc
            ),
            reply_async_result(FinalReplyFnAndArgs, BatchSize, Results),
            {ok, self()}
    end.

unsubmitted_table_results(Iter, Result) ->
    unsubmitted_table_results(maps:next(Iter), Result, []).

unsubmitted_table_results(none, _Result, Acc) ->
    Acc;
unsubmitted_table_results({_Table, PointsWithIndices, Iter}, Result, Acc) ->
    Results = table_results(count_points_by_index(PointsWithIndices), Result),
    unsubmitted_table_results(maps:next(Iter), Result, Results ++ Acc).

reply_async_result(ReplyFnAndArgs, BatchSize, Results0) ->
    Results = coalesce_mixed_batch_results(Results0),
    case BatchSize of
        1 ->
            [Result] = Results,
            emqx_resource:apply_reply_fun(ReplyFnAndArgs, Result);
        _ ->
            emqx_resource:apply_reply_fun(ReplyFnAndArgs, Results)
    end.

%% The resource buffer worker currently derives a single retry decision for the
%% whole batch. If any write succeeded, retrying the whole batch may duplicate
%% writes, so retryable failures must become unrecoverable. If no write succeeded,
%% retry the whole batch and retain the original per-message results as details.
coalesce_mixed_batch_results(Results) ->
    Recoverable = [
        Result
     || {error, {recoverable_error, _}} = Result <- Results
    ],
    NonRecoverable = Results -- Recoverable,
    case {Recoverable, NonRecoverable} of
        {[], _} ->
            Results;
        {_, []} ->
            Results;
        {_, _} ->
            case lists:any(fun is_successful_result/1, Results) of
                true ->
                    [
                        coalesce_batch_result(Result, partial_batch_write, Results)
                     || Result <- Results
                    ];
                false ->
                    make_batch_recoverable(Results)
            end
    end.

coalesce_batch_result(
    {error, {recoverable_error, OriginalError}}, Reason, BatchResults
) ->
    {error,
        {unrecoverable_error, #{
            reason => Reason,
            original_error => OriginalError,
            batch_results => BatchResults
        }}};
coalesce_batch_result(Result, _Reason, _BatchResults) ->
    Result.

make_batch_recoverable(BatchResults) ->
    Result =
        {error,
            {recoverable_error, #{
                reason => mixed_batch_failure,
                batch_results => BatchResults
            }}},
    lists:duplicate(length(BatchResults), Result).

is_successful_result({ok, _}) ->
    true;
is_successful_result(
    {error,
        {unrecoverable_error, #{
            reason := multiline_partial_write,
            successful_rows := Rows
        }}}
) when
    is_integer(Rows) andalso Rows > 0
->
    true;
is_successful_result(_) ->
    false.

%% One table insert returns one result, but may cover multiple points from the
%% same original message. Keep the point count for that message, while reporting
%% the table result only once per message.
count_points_by_index(PointsWithIndices) ->
    {OrderRev, Counts} = lists:foldl(
        fun({Index, _Point}, {Order0, Counts0}) ->
            case maps:find(Index, Counts0) of
                {ok, Count} ->
                    {Order0, Counts0#{Index => Count + 1}};
                error ->
                    {[Index | Order0], Counts0#{Index => 1}}
            end
        end,
        {[], #{}},
        PointsWithIndices
    ),
    [{Index, maps:get(Index, Counts)} || Index <- lists:reverse(OrderRev)].

table_results(PointsByIndex, Result) ->
    [{Index, PointCount, Result} || {Index, PointCount} <- PointsByIndex].

failed_results(Failed) ->
    [{Index, 0, Result} || {Index, Result} <- Failed].

aggregate_message_results(BatchSize, TableResults, Failed) ->
    aggregate_indexed_results(BatchSize, TableResults ++ failed_results(Failed)).

aggregate_indexed_results(BatchSize, IndexedResults) ->
    ResultsByIndex = maps:groups_from_list(
        fun({Index, _PointCount, _Result}) -> Index end,
        fun(Result) -> Result end,
        IndexedResults
    ),
    [
        aggregate_message_result(maps:get(Index, ResultsByIndex, []))
     || Index <- lists:seq(1, BatchSize)
    ].

aggregate_message_result([]) ->
    {error, {unrecoverable_error, missing_insert_result}};
aggregate_message_result(Results) ->
    {Successes, Errors} = lists:partition(
        fun({_Index, _PointCount, Result}) -> element(1, Result) =:= ok end,
        Results
    ),
    case {Successes, Errors} of
        {_, []} ->
            {ok, lists:sum([PointCount || {_Index, PointCount, {ok, _}} <- Successes])};
        {[], Errors} ->
            aggregate_errors(Errors);
        {_, Errors} ->
            ErrorDetails = classified_error_details([
                classify_insert_error(Result)
             || {_Index, _PointCount, Result} <- Errors
            ]),
            {error,
                {unrecoverable_error, #{
                    reason => multiline_partial_write,
                    successful_rows =>
                        lists:sum([PointCount || {_Index, PointCount, {ok, _}} <- Successes]),
                    errors => ErrorDetails
                }}}
    end.

aggregate_errors(Errors) ->
    Classified = [
        classify_insert_error(Result)
     || {_Index, _PointCount, Result} <- Errors
    ],
    case lists:any(fun({Kind, _}) -> Kind =:= unrecoverable end, Classified) of
        true ->
            {error, {unrecoverable_error, classified_error_details(Classified)}};
        false ->
            {error, {recoverable_error, classified_error_details(Classified)}}
    end.

classify_insert_error({error, {unrecoverable_error, Reason}}) ->
    {unrecoverable, Reason};
classify_insert_error({error, {recoverable_error, Reason}}) ->
    {recoverable, Reason};
classify_insert_error({error, Reason}) ->
    case is_transient_pool_error(Reason) of
        true -> {recoverable, Reason};
        false -> {unrecoverable, Reason}
    end.

classify_insert_result(Result) ->
    case classify_insert_error(normalize_insert_result(Result)) of
        {recoverable, Reason} -> {error, {recoverable_error, Reason}};
        {unrecoverable, Reason} -> {error, {unrecoverable_error, Reason}}
    end.

is_transient_pool_error(disconnected) ->
    true;
is_transient_pool_error({disconnected, _Reason}) ->
    true;
is_transient_pool_error(ecpool_empty) ->
    true;
is_transient_pool_error(no_such_pool) ->
    true;
is_transient_pool_error(ecpool_disconnected) ->
    true;
is_transient_pool_error(ecpool_client_disconnected) ->
    true;
is_transient_pool_error(not_connected) ->
    true;
is_transient_pool_error(_Reason) ->
    false.

classified_error_details([{_Kind, Reason}]) ->
    Reason;
classified_error_details(Classified) ->
    [Reason || {_Kind, Reason} <- Classified].

%% @doc Build the options map for `greptimedb_rs:start_client/1'.
client_opts(ConnResId, ConnConfig) ->
    #{server := Server0, dbname := Database} = ConnConfig,
    %% Replicate greptime connector behavior: 4001 is the default port.
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server0, #{
        default_port => 4001
    }),
    Server = iolist_to_binary([Host, ":", integer_to_binary(Port)]),
    Opts0 = #{
        pool_name => ConnResId,
        pool_size => erlang:system_info(dirty_io_schedulers),
        pool_type => random,
        auto_reconnect => ?AUTO_RECONNECT_S,
        endpoints => [bin(Server)],
        dbname => Database
    },
    Opts1 = maybe_put_auth(ConnConfig, Opts0),
    Opts2 = maybe_put_tls(ConnConfig, Opts1),
    maybe_put_optional_keys([ts_column, ttl], ConnConfig, Opts2).

maybe_put_auth(#{username := Username, password := Password}, Opts) ->
    Opts#{username => Username, password => Password};
maybe_put_auth(_ConnConfig, Opts) ->
    Opts.

maybe_put_tls(#{ssl := #{enable := true} = SSLOpts}, Opts) ->
    TLSOpts = extract_tls_opts(SSLOpts),
    maps:merge(Opts, TLSOpts#{tls => true});
maybe_put_tls(_ConnConfig, Opts) ->
    Opts.

extract_tls_opts(SSLOpts) ->
    TLSCertOpts = extract_tls_cert_opts(SSLOpts),
    TLSCertOpts1 = maybe_put_tls_verify_opt(SSLOpts, TLSCertOpts),
    maybe_put_cipher_suites(SSLOpts, TLSCertOpts1).

extract_tls_cert_opts(SSLOpts0) ->
    SSLOpts =
        case SSLOpts0 of
            #{cacertfile := CACertFile} ->
                SSLOpts0#{cacertfile := emqx_tls_lib:resolve_cacertfile_path(CACertFile)};
            #{} ->
                SSLOpts0
        end,
    TLSOpts0 = maps:fold(
        fun emqx_utils_maps:rename/3,
        SSLOpts,
        #{
            cacertfile => ca_cert,
            certfile => client_cert,
            keyfile => client_key
        }
    ),
    TLSOpts1 = maps:with([ca_cert, client_cert, client_key], TLSOpts0),
    TLSOpts2 = maps:filter(
        fun(_, V) -> not is_blank_or_undefined(V) end,
        TLSOpts1
    ),
    maps:map(fun(_, V) -> bin(V) end, TLSOpts2).

maybe_put_tls_verify_opt(SSLOpts, TLSOpts) ->
    case maps:find(verify, SSLOpts) of
        {ok, Verify0} ->
            case normalize_tls_verify(Verify0) of
                undefined -> TLSOpts;
                Verify -> TLSOpts#{verify => Verify}
            end;
        error ->
            case maps:find(<<"verify">>, SSLOpts) of
                {ok, Verify0} ->
                    case normalize_tls_verify(Verify0) of
                        undefined -> TLSOpts;
                        Verify -> TLSOpts#{verify => Verify}
                    end;
                error ->
                    TLSOpts
            end
    end.

maybe_put_cipher_suites(SSLOpts, TLSOpts) ->
    Ciphers = find_opt([ciphers, <<"ciphers">>], SSLOpts),
    case Ciphers of
        {ok, List} when is_list(List), List =/= [] ->
            {Converted, Unsupported} = emqx_tls_lib:convert_ciphers_to_rfc(
                List, supported_cipher_suites()
            ),
            case Unsupported of
                [] ->
                    TLSOpts#{cipher_suites => lists:map(fun erlang:list_to_binary/1, Converted)};
                _ ->
                    throw(
                        iolist_to_binary([
                            "unsupported cipher suites: ",
                            lists:join(", ", Unsupported),
                            ". Supported: "
                            "TLS_AES_256_GCM_SHA384, TLS_AES_128_GCM_SHA256, "
                            "TLS_CHACHA20_POLY1305_SHA256, "
                            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, "
                            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, "
                            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384, "
                            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
                        ])
                    )
            end;
        _ ->
            TLSOpts
    end.

find_opt(Keys, Map) ->
    lists:foldl(
        fun
            (_K, {ok, _} = Acc) ->
                Acc;
            (K, Acc) ->
                case maps:find(K, Map) of
                    {ok, _} = Found -> Found;
                    error -> Acc
                end
        end,
        error,
        Keys
    ).

supported_cipher_suites() ->
    [
        <<"TLS_AES_256_GCM_SHA384">>,
        <<"TLS_AES_128_GCM_SHA256">>,
        <<"TLS_CHACHA20_POLY1305_SHA256">>,
        <<"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384">>,
        <<"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256">>,
        <<"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384">>,
        <<"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256">>
    ].

normalize_tls_verify(verify_none) ->
    verify_none;
normalize_tls_verify(<<"verify_none">>) ->
    verify_none;
normalize_tls_verify("verify_none") ->
    verify_none;
normalize_tls_verify(verify_peer) ->
    verify_peer;
normalize_tls_verify(<<"verify_peer">>) ->
    verify_peer;
normalize_tls_verify("verify_peer") ->
    verify_peer;
normalize_tls_verify(_) ->
    undefined.

maybe_put_optional_keys(Keys, Source, Target) ->
    lists:foldl(
        fun(Key, Acc) ->
            case Source of
                #{Key := Val} -> Acc#{Key => Val};
                _ -> Acc
            end
        end,
        Target,
        Keys
    ).

ensure_consistent_ssl_opts(#{ssl := #{enable := true} = SSLOpts} = _ConnConfig) ->
    case normalize_tls_verify(get_ssl_verify_opt(SSLOpts, verify_peer)) of
        verify_none ->
            ok;
        _ ->
            AnyMissing =
                lists:any(
                    fun(Key) -> is_blank_or_missing(Key, SSLOpts) end,
                    [cacertfile, certfile, keyfile]
                ),
            case AnyMissing of
                true ->
                    throw(<<
                        "cacertfile, certfile and keyfile SSL options must be configured"
                        " when SSL is enabled."
                    >>);
                false ->
                    ok
            end
    end;
ensure_consistent_ssl_opts(_ConnConfig) ->
    ok.

get_ssl_verify_opt(SSLOpts, Default) ->
    case maps:find(verify, SSLOpts) of
        {ok, Verify} ->
            Verify;
        error ->
            case maps:find(<<"verify">>, SSLOpts) of
                {ok, Verify} -> Verify;
                error -> Default
            end
    end.

is_blank_or_missing(Key, Cfg) ->
    is_blank_or_undefined(maps:get(Key, Cfg, undefined)).

is_blank_or_undefined(undefined) ->
    true;
is_blank_or_undefined(null) ->
    true;
is_blank_or_undefined(V) when is_binary(V) ->
    byte_size(V) =:= 0;
is_blank_or_undefined(V) when is_list(V) ->
    V =:= [];
is_blank_or_undefined(_) ->
    false.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

client_opts_tls_verify_none_without_cert_paths_test() ->
    ConnConfig = #{
        server => <<"127.0.0.1:5001">>,
        dbname => <<"public">>,
        ssl => #{
            enable => true,
            verify => verify_none,
            cacertfile => <<>>,
            certfile => <<>>,
            keyfile => <<>>
        }
    },
    Opts = client_opts(test_res_id, ConnConfig),
    ?assertEqual(true, maps:get(tls, Opts)),
    ?assertEqual(verify_none, maps:get(verify, Opts)),
    ?assertNot(maps:is_key(ca_cert, Opts)),
    ?assertNot(maps:is_key(client_cert, Opts)),
    ?assertNot(maps:is_key(client_key, Opts)).

client_opts_tls_verify_peer_keeps_certs_test() ->
    ConnConfig = #{
        server => <<"127.0.0.1:5001">>,
        dbname => <<"public">>,
        ssl => #{
            enable => true,
            verify => verify_peer,
            cacertfile => <<"/tmp/ca.crt">>,
            certfile => <<"/tmp/client.crt">>,
            keyfile => <<"/tmp/client.key">>
        }
    },
    Opts = client_opts(test_res_id, ConnConfig),
    ?assertEqual(true, maps:get(tls, Opts)),
    ?assertEqual(verify_peer, maps:get(verify, Opts)),
    ?assertEqual(<<"/tmp/ca.crt">>, maps:get(ca_cert, Opts)),
    ?assertEqual(<<"/tmp/client.crt">>, maps:get(client_cert, Opts)),
    ?assertEqual(<<"/tmp/client.key">>, maps:get(client_key, Opts)).

client_opts_tls_verify_none_without_cacertfile_key_test() ->
    ConnConfig = #{
        server => <<"127.0.0.1:5001">>,
        dbname => <<"public">>,
        ssl => #{enable => true, verify => verify_none}
    },
    Opts = client_opts(test_res_id, ConnConfig),
    ?assertEqual(true, maps:get(tls, Opts)),
    ?assertNot(maps:is_key(ca_cert, Opts)).

client_opts_tls_system_default_cacertfile_test() ->
    ConnConfig = #{
        server => <<"127.0.0.1:5001">>,
        dbname => <<"public">>,
        ssl => #{
            enable => true,
            verify => verify_peer,
            cacertfile => <<"system_default">>,
            certfile => <<"/tmp/client.crt">>,
            keyfile => <<"/tmp/client.key">>
        }
    },
    ok = application:set_env(emqx, trusted_certs_path, "/trusted/ca-bundle.pem"),
    try
        Opts = client_opts(test_res_id, ConnConfig),
        ?assertEqual(<<"/trusted/ca-bundle.pem">>, maps:get(ca_cert, Opts))
    after
        application:unset_env(emqx, trusted_certs_path)
    end.

-endif.
