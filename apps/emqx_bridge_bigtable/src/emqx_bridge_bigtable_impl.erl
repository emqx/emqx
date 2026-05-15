%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_bigtable_impl).

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
-export([mutate_rows_reply_delegator/2]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% Allocatable resources

-define(installed_channels, installed_channels).
-define(client, client).
-define(health_check_timeout, health_check_timeout).
-define(connect_timeout, connect_timeout).
-define(project_id, project_id).
-define(row_key, row_key).
-define(instance_id, instance_id).
-define(table_id, table_id).
-define(table_name, table_name).
-define(instance_name, instance_name).
-define(mutations, mutations).
-define(request_ttl, request_ttl).

-type connector_config() :: #{
    authentication := emqx_bridge_gcp_pubsub_client:authentication_config(),
    connect_timeout := timeout(),
    any() => term()
}.
-type connector_state() :: #{
    ?installed_channels := #{channel_id() => channel_state()},
    ?connect_timeout := timeout(),
    ?health_check_timeout := timeout(),
    ?client := emqx_bridge_bigtable_client:state(),
    ?project_id := binary()
}.

-type channel_config() :: #{
    parameters := #{}
}.
-type channel_state() :: #{}.

-type query() :: {_Tag :: channel_id(), _Data :: map()}.

%%------------------------------------------------------------------------------
%% `emqx_resource' API
%%------------------------------------------------------------------------------

-spec resource_type() -> atom().
resource_type() ->
    bigtable.

-spec callback_mode() -> callback_mode().
callback_mode() ->
    async_if_possible.

-spec on_start(connector_resource_id(), connector_config()) ->
    {ok, connector_state()} | {error, _Reason}.
on_start(ConnResId, ConnConfig) ->
    #{
        connect_timeout := ConnectTimeout,
        resource_opts := #{health_check_timeout := HCTimeout}
    } = ConnConfig,
    maybe
        {ok, Client} ?= emqx_bridge_bigtable_client:start(ConnResId, ConnConfig),
        ConnState = #{
            ?installed_channels => #{},
            ?client => Client,
            ?project_id => maps:get(project_id, Client),
            ?connect_timeout => ConnectTimeout,
            ?health_check_timeout => HCTimeout
        },
        {ok, ConnState}
    end.

-spec on_stop(connector_resource_id(), connector_state()) -> ok.
on_stop(ConnResId, _ConnState) ->
    emqx_bridge_bigtable_client:stop(ConnResId),
    ?tp("bigtable_connector_stop", #{instance_id => ConnResId}),
    ok.

-spec on_get_status(connector_resource_id(), connector_state()) ->
    ?status_connected | {?status_connecting, term()} | {?status_disconnected, term()}.
on_get_status(ConnResId, ConnState) ->
    #{
        ?client := Client,
        ?connect_timeout := ConnectTimeout,
        ?health_check_timeout := HCTimeout
    } = ConnState,
    Opts = #{timeout => HCTimeout, connect_timeout => ConnectTimeout},
    emqx_bridge_bigtable_client:health_check(ConnResId, Opts, Client).

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
    #{?installed_channels := InstalledChannels0} = ConnState0,
    maybe
        {ok, ChanState} ?= create_channel(ActionConfig, ConnState0),
        InstalledChannels = InstalledChannels0#{ChanResId => ChanState},
        ConnState = ConnState0#{?installed_channels := InstalledChannels},
        {ok, ConnState}
    end.

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
    ConnResId,
    ChanResId,
    ConnState = #{?installed_channels := InstalledChannels}
) when is_map_key(ChanResId, InstalledChannels) ->
    #{?client := Client, ?connect_timeout := ConnectTimeout} = ConnState,
    #{ChanResId := ChanState} = InstalledChannels,
    #{
        ?instance_name := InstanceName,
        ?health_check_timeout := HCTimeout
    } = ChanState,
    Opts = #{
        connect_timeout => ConnectTimeout,
        timeout => HCTimeout
    },
    Res = emqx_bridge_bigtable_client:ping_and_warm(ConnResId, InstanceName, Opts, Client),
    case Res of
        {error, {_Code, Msg}} ->
            {?status_disconnected, Msg};
        {error, Reason} ->
            {?status_disconnected, Reason};
        {ok, _, _} ->
            ?status_connected
    end;
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
    #{ChanResId := ChanState} = InstalledChannels,
    Batch = [Data],
    do_mutate_rows(single, sync, Batch, ConnResId, ChanResId, ChanState, ConnState);
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
    #{ChanResId := ChanState} = InstalledChannels,
    Batch = [Data],
    do_mutate_rows(
        single, {async, ReplyFnAndArgs}, Batch, ConnResId, ChanResId, ChanState, ConnState
    );
on_query_async(_ConnResId, Query, _ReplyFnAndArgs, _ConnState) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

-spec on_batch_query(connector_resource_id(), [query()], connector_state()) ->
    ok | {error, term()}.
on_batch_query(
    ConnResId,
    [{ChanResId, _Data} | _] = Queries,
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    #{ChanResId := ChanState} = InstalledChannels,
    Batch = [Data || {_, Data} <- Queries],
    do_mutate_rows(batch, sync, Batch, ConnResId, ChanResId, ChanState, ConnState);
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
    [{ChanResId, _Data} | _] = Queries,
    ReplyFnAndArgs,
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    #{ChanResId := ChanState} = InstalledChannels,
    Batch = [Data || {_, Data} <- Queries],
    do_mutate_rows(
        batch, {async, ReplyFnAndArgs}, Batch, ConnResId, ChanResId, ChanState, ConnState
    );
on_batch_query_async(_ConnResId, Queries, _ReplyFnAndArgs, _ConnectorState) ->
    {error, {unrecoverable_error, {invalid_batch, Queries}}}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

mutate_rows_reply_delegator(ContinuationCtx, Reply) ->
    #{reply_fn_and_args := ReplyFnAndArgs} = ContinuationCtx,
    case Reply of
        {ok, Results0, Trailers} ->
            Results1 = flatten_successful_results(Results0),
            Results2 = maybe_trailers_to_error(Results1, Trailers, ContinuationCtx),
            Results3 = construct_final_results(Results2, ContinuationCtx),
            emqx_resource:apply_reply_fun(ReplyFnAndArgs, Results3);
        {error, Reason0} ->
            %% return same error for all messages
            #{batch_size := BatchSize} = ContinuationCtx,
            Reason = handle_error_reason(Reason0),
            Results1 = lists:duplicate(BatchSize, {error, Reason}),
            Results = construct_final_results(Results1, ContinuationCtx),
            emqx_resource:apply_reply_fun(ReplyFnAndArgs, Results)
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

render_val(KeyName, Key, Data) ->
    try map_get(Key, Data) of
        undefined ->
            {error, {missing_val, KeyName, Key, Data}};
        V ->
            {ok, V}
    catch
        error:{badkey, _} ->
            {error, {missing_val, KeyName, Key, Data}};
        error:badarg ->
            {error, {missing_val, KeyName, Key, Data}}
    end.

instance_name(ProjectId, InstanceId) ->
    <<"projects/", ProjectId/binary, "/instances/", InstanceId/binary>>.

table_name(ProjectId, InstanceId, TableId) ->
    <<"projects/", ProjectId/binary, "/instances/", InstanceId/binary, "/tables/", TableId/binary>>.

deadline(infinity) ->
    infinity;
deadline(RequestTTL) ->
    RequestTTL + now_ms().

now_ms() ->
    erlang:monotonic_time(millisecond).

create_channel(ActionConfig, ConnState) ->
    #{?project_id := ProjectId} = ConnState,
    #{
        parameters := #{
            instance_id := InstanceId,
            table_id := TableId,
            row_key := RowKey,
            mutations := Mutations
        },
        resource_opts := #{
            request_ttl := RequestTTL,
            health_check_timeout := HCTimeout
        }
    } = ActionConfig,
    InstanceName = instance_name(ProjectId, InstanceId),
    TableName = table_name(ProjectId, InstanceId, TableId),
    ChanState = #{
        ?instance_id => InstanceId,
        ?table_id => TableId,
        ?table_name => TableName,
        ?instance_name => InstanceName,
        ?row_key => RowKey,
        %% store reversed list, so we don't need to reverse it again after rendering
        ?mutations => lists:reverse(Mutations),
        ?request_ttl => RequestTTL,
        ?health_check_timeout => HCTimeout
    },
    {ok, ChanState}.

do_mutate_rows(BatchOrSingle, SyncOrAsync, Batch, ConnResId, ChanResId, ChanState, ConnState) ->
    #{
        ?client := Client,
        ?connect_timeout := ConnectTimeout
    } = ConnState,
    #{
        ?instance_name := InstanceName,
        ?table_name := TableName,
        ?request_ttl := RequestTTL
    } = ChanState,
    {RevEntries, ErrorsWithIndices, RevOkIndices, OkBatchSize, BatchSize} =
        lists:foldl(
            fun(Data, {EntriesAcc, ErrorsAcc, OkIndicesAcc, OkN, N}) ->
                case render_entry(Data, ChanState, ConnState) of
                    {ok, Entry} ->
                        {[Entry | EntriesAcc], ErrorsAcc, [N | OkIndicesAcc], OkN + 1, N + 1};
                    {error, Error} ->
                        {EntriesAcc, [{N, {error, {unrecoverable_error, Error}}} | ErrorsAcc],
                            OkIndicesAcc, OkN, N + 1}
                end
            end,
            {[], [], [], 0, 0},
            Batch
        ),
    Req = #{
        table_name => TableName,
        entries => lists:reverse(RevEntries)
    },
    Opts = #{
        connect_timeout => ConnectTimeout,
        timeout => RequestTTL
    },
    IsAsync =
        case SyncOrAsync of
            {async, _} -> true;
            sync -> false
        end,
    emqx_trace:rendered_action_template(ChanResId, #{
        rpc => mutate_rows,
        req => Req,
        is_async => IsAsync,
        batch_size => BatchSize
    }),
    ContinuationCtx0 = #{
        res_id => ChanResId,
        errors => ErrorsWithIndices,
        ok_indices => RevOkIndices,
        batch_or_single => BatchOrSingle,
        batch_size => OkBatchSize
    },
    ReqOk =
        case RevEntries of
            [] ->
                %% would yield a "no mutations provided" error; return render errors
                %% immediately.
                construct_final_results([], ContinuationCtx0);
            [_ | _] ->
                {ok, Req}
        end,
    maybe
        {ok, Req} ?= ReqOk,
        Res = emqx_bridge_bigtable_client:mutate_rows(ConnResId, InstanceName, Req, Opts, Client),
        {ok, Stream} ?= Res,
        case SyncOrAsync of
            sync ->
                PartialResults = [],
                do_recv_sync(Stream, Opts, PartialResults, ContinuationCtx0);
            {async, ReplyFnAndArgs} ->
                ContinuationCtx = ContinuationCtx0#{reply_fn_and_args => ReplyFnAndArgs},
                WrappedReplyFn = {fun ?MODULE:mutate_rows_reply_delegator/2, [ContinuationCtx]},
                Deadline = deadline(RequestTTL),
                emqx_bridge_bigtable_client:async_recv(
                    Stream, Opts, WrappedReplyFn, Deadline, Client
                )
        end
    else
        Error ->
            handle_initial_send_error(Error)
    end.

handle_initial_send_error({error, not_found}) ->
    %% some race condition?  we open the stream and, as we send the initial request, it
    %% has already been closed by the server (possibly due to connections/streams limits,
    %% `stream_refused`, or something else), and we get this "stream not found"
    {error, {recoverable_error, stream_not_found}};
handle_initial_send_error({error, {recoverable_error, Reason}}) ->
    {error, {recoverable_error, Reason}};
handle_initial_send_error({error, {unrecoverable_error, Reason}}) ->
    {error, {unrecoverable_error, Reason}};
handle_initial_send_error({error, Reason}) ->
    {error, {unrecoverable_error, Reason}};
handle_initial_send_error(Error) ->
    {error, {unrecoverable_error, Error}}.

do_recv_sync(Stream, Opts, PartialResults0, ContinuationCtx) ->
    case emqx_bridge_bigtable_async_receiver_worker:do_recv_stream_once(Stream, Opts) of
        {done, Results0, Trailers} ->
            RevBatches = [Results0 | PartialResults0],
            Results1 = lists:flatten(lists:reverse(RevBatches)),
            Results2 = flatten_successful_results(Results1),
            Results3 = maybe_trailers_to_error(Results2, Trailers, ContinuationCtx),
            construct_final_results(Results3, ContinuationCtx);
        {more, Results0} ->
            PartialResults = [Results0 | PartialResults0],
            do_recv_sync(Stream, Opts, PartialResults, ContinuationCtx);
        {error, Reason0} ->
            Reason = handle_error_reason(Reason0),
            %% return same error for all messages
            #{batch_size := BatchSize} = ContinuationCtx,
            Results = lists:duplicate(BatchSize, {error, Reason}),
            construct_final_results(Results, ContinuationCtx)
    end.

%% if the final result array is empty, there was probably an error.
maybe_trailers_to_error([], Trailers, ContinuationCtx) ->
    #{batch_size := BatchSize} = ContinuationCtx,
    Error = {error, grpc_client:trailers_to_error(Trailers)},
    lists:duplicate(BatchSize, Error);
maybe_trailers_to_error(Results, _Trailers, _ContinuationCtx) ->
    Results.

%% For batches, each original message may have independent results and fail at different
%% moments.
construct_final_results(Results0, ContinuationCtx) ->
    #{
        errors := ErrorsWithIndices,
        ok_indices := RevOkIndices,
        batch_or_single := BatchOrSingle,
        batch_size := _BatchSize
    } = ContinuationCtx,
    ResultsWithIndices1 = lists:zip(lists:reverse(RevOkIndices), Results0),
    ResultsWithIndices = lists:keysort(1, ErrorsWithIndices ++ ResultsWithIndices1),
    {_, Results} = lists:unzip(ResultsWithIndices),
    handle_batch_or_single(Results, BatchOrSingle).

handle_error_reason(Reason0) ->
    case Reason0 of
        #{kind := error, reason := not_found} ->
            %% One possible cause is:
            %% `Stream shutdown reason: {stream_error,refused_stream,'Stream
            %% reset by server.'}`
            %% ... maybe there are others?
            {recoverable_error, Reason0};
        #{kind := error, reason := {closed, {error, closed}}} ->
            %% Server is forcefully closing the connection (too many streams?)
            {recoverable_error, Reason0};
        #{kind := error, reason := closed} ->
            %% Server is forcefully closing the connection (too many streams?)
            {recoverable_error, Reason0};
        #{kind := error, reason := {stream_error, refused_stream, 'Stream reset by server.'}} ->
            %% Server is rejecting our stream (too many streams)
            {recoverable_error, Reason0};
        _ ->
            Reason0
    end.

%% each result is a `MutateRowsResponse`.
flatten_successful_results(Results) when is_list(Results) ->
    lists:flatmap(
        fun(#{entries := Entries}) ->
            lists:map(
                fun(#{status := #{code := Code}} = Entry) ->
                    case google_rpc_codename(Code) of
                        ok ->
                            {ok, Entry};
                        Error ->
                            {error, {unrecoverable_error, {Error, Entry}}}
                    end
                end,
                Entries
            )
        end,
        Results
    ).

google_rpc_codename(Code) ->
    CodeBin = integer_to_binary(Code),
    try
        grpc_utils:codename(CodeBin)
    catch
        _:_ ->
            {unknown_error, Code}
    end.

handle_batch_or_single(Results, batch) ->
    ?tp("bigtable_result", #{results => Results}),
    Results;
handle_batch_or_single([Result], single) ->
    ?tp("bigtable_result", #{results => [Result]}),
    Result.

render_entry(Data, ChanState, _ConnState) ->
    #{
        ?row_key := RowKeyKey,
        ?mutations := RevMutationsKeys
    } = ChanState,
    maybe
        {ok, RowKey} ?= render_val(row_key, RowKeyKey, Data),
        {ok, Mutations} ?=
            emqx_utils:foldl_while(
                fun(MutationKeys, {ok, Acc}) ->
                    case render_mutation(Data, MutationKeys) of
                        {ok, Mutation} ->
                            {cont, {ok, [Mutation | Acc]}};
                        {error, Reason} ->
                            {halt, {error, Reason}}
                    end
                end,
                {ok, []},
                RevMutationsKeys
            ),
        Req = #{
            row_key => RowKey,
            mutations => Mutations
        },
        {ok, Req}
    end.

render_mutation(Data, #{type := set_cell} = MutationKeys) ->
    #{
        family_name := FamilyNameKey,
        column_qualifier := ColumnQualifierKey,
        timestamp_micros := TimestampMicrosKey,
        value := ValueKey
    } = MutationKeys,
    maybe
        {ok, FamilyName} ?= render_val(family_name, FamilyNameKey, Data),
        {ok, ColumnQualifier} ?= render_val(column_qualifier, ColumnQualifierKey, Data),
        {ok, TimestampMicros} ?= render_val(timestamp_micros, TimestampMicrosKey, Data),
        {ok, Value} ?= render_val(value, ValueKey, Data),
        Mutation = #{
            family_name => FamilyName,
            column_qualifier => ColumnQualifier,
            timestamp_micros => TimestampMicros,
            value => Value
        },
        {ok, #{mutation => {set_cell, Mutation}}}
    end.
