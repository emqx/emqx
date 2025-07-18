%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_builtin_local).

-behaviour(emqx_ds).
-behaviour(emqx_ds_buffer).
-behaviour(emqx_ds_beamformer).

%% API:
-export([]).

%% behavior callbacks:
-export([
    %% `emqx_ds':
    open_db/2,
    close_db/1,
    add_generation/1,
    update_db_config/2,
    list_shards/1,
    shard_of/2,
    list_generations_with_lifetimes/1,
    drop_generation/2,
    drop_db/1,
    store_batch/3,
    get_streams/4,
    get_delete_streams/3,
    make_iterator/4,
    make_delete_iterator/4,
    delete_next/4,

    update_iterator/3,
    next/3,

    subscribe/3,
    unsubscribe/2,
    suback/3,
    subscription_info/2,

    new_tx/2,
    commit_tx/3,
    tx_commit_outcome/1,

    %% `beamformer':
    unpack_iterator/2,
    scan_stream/5,
    high_watermark/2,
    fast_forward/3,
    message_match_context/4,
    iterator_match_context/2,

    %% `emqx_ds_buffer':
    init_buffer/3,
    flush_buffer/4,
    shard_of_operation/4,

    %% optimistic_tx
    otx_get_tx_serial/2,
    otx_get_leader/2,
    otx_become_leader/2,
    otx_prepare_tx/5,
    otx_commit_tx_batch/5,
    otx_lookup_ttv/4,
    otx_get_runtime_config/1,

    stream_to_binary/2,
    binary_to_stream/2,
    iterator_to_binary/2,
    binary_to_iterator/2
]).

%% Internal exports:
-export([
    do_next/3,
    do_delete_next/4,
    %% Used by batch serializer
    make_batch/3
]).

-ifdef(TEST).
-export([test_applications/1, test_db_config/1]).
-endif.

-export_type([db_opts/0, shard/0, iterator/0, delete_iterator/0]).

-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds_builtin_tx.hrl").
-include("../../emqx_durable_storage/gen_src/DSBuiltinMetadata.hrl").

-elvis([{elvis_style, atom_naming_convention, disable}]).
%% https://github.com/erlang/otp/issues/9841
-dialyzer(
    {nowarn_function, [
        stream_to_binary/2, binary_to_stream/2, iterator_to_binary/2, binary_to_iterator/2
    ]}
).

%%================================================================================
%% Type declarations
%%================================================================================

-define(tag, 1).
-define(shard, 2).
-define(enc, 3).

-define(IT, 61).
-define(DELETE_IT, 62).

-type shard() :: binary().

-opaque iterator() ::
    #{
        ?tag := ?IT,
        ?shard := shard(),
        ?enc := term()
    }.

-opaque delete_iterator() ::
    #{
        ?tag := ?DELETE_IT,
        ?shard := shard(),
        ?enc := term()
    }.

-type db_opts() ::
    #{
        backend := builtin_local,
        store_ttv := boolean(),
        storage := emqx_ds_storage_layer:prototype(),

        n_shards := pos_integer(),
        poll_workers_per_shard => pos_integer(),
        %% Equivalent to `append_only' from `emqx_ds:create_db_opts':
        force_monotonic_timestamps := boolean(),
        atomic_batches := boolean(),
        %% Optimistic transaction:
        transaction => emqx_ds_optimistic_tx:runtime_config()
    }.

-type slab() :: {shard(), emqx_ds_storage_layer:gen_id()}.

-type tx_context() :: emqx_ds_optimistic_tx:ctx().

-define(stream(SHARD, INNER), [2, SHARD | INNER]).
-define(delete_stream(SHARD, INNER), [3, SHARD | INNER]).

%%================================================================================
%% API functions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

-spec open_db(emqx_ds:db(), db_opts()) -> ok | {error, _}.
open_db(DB, CreateOpts0) ->
    %% Rename `append_only' flag to `force_monotonic_timestamps':
    AppendOnly = maps:get(append_only, CreateOpts0),
    CreateOpts1 = maps:put(force_monotonic_timestamps, AppendOnly, CreateOpts0),
    CreateOpts = emqx_utils_maps:deep_merge(
        #{
            transaction => #{
                flush_interval => 1_000,
                idle_flush_interval => 1,
                conflict_window => 5_000
            }
        },
        CreateOpts1
    ),
    case emqx_ds_builtin_local_sup:start_db(DB, CreateOpts) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, Err} ->
            {error, Err}
    end.

-spec close_db(emqx_ds:db()) -> ok.
close_db(DB) ->
    emqx_ds_builtin_local_sup:stop_db(DB).

-spec add_generation(emqx_ds:db()) -> ok | {error, _}.
add_generation(DB) ->
    Shards = emqx_ds_builtin_local_meta:shards(DB),
    Errors = lists:filtermap(
        fun(Shard) ->
            DBShard = {DB, Shard},
            case
                emqx_ds_storage_layer:add_generation(
                    DBShard, emqx_ds_builtin_local_meta:ensure_monotonic_timestamp(DBShard)
                )
            of
                ok ->
                    emqx_ds_beamformer:generation_event(DBShard),
                    false;
                Error ->
                    {true, {Shard, Error}}
            end
        end,
        Shards
    ),
    case Errors of
        [] -> ok;
        _ -> {error, Errors}
    end.

-spec update_db_config(emqx_ds:db(), db_opts()) -> ok | {error, _}.
update_db_config(DB, CreateOpts) ->
    Opts = #{} = emqx_ds_builtin_local_meta:update_db_config(DB, CreateOpts),
    lists:foreach(
        fun(Shard) ->
            ShardId = {DB, Shard},
            emqx_ds_storage_layer:update_config(
                ShardId, emqx_ds_builtin_local_meta:ensure_monotonic_timestamp(ShardId), Opts
            )
        end,
        emqx_ds_builtin_local_meta:shards(DB)
    ).

-spec list_generations_with_lifetimes(emqx_ds:db()) ->
    #{emqx_ds:slab() => emqx_ds:slab_info()}.
list_generations_with_lifetimes(DB) ->
    lists:foldl(
        fun(Shard, Acc) ->
            maps:fold(
                fun(GenId, Data0, Acc1) ->
                    Data = maps:update_with(
                        until,
                        fun timeus_to_timestamp/1,
                        maps:update_with(since, fun timeus_to_timestamp/1, Data0)
                    ),
                    Acc1#{{Shard, GenId} => Data}
                end,
                Acc,
                emqx_ds_storage_layer:list_generations_with_lifetimes({DB, Shard})
            )
        end,
        #{},
        emqx_ds_builtin_local_meta:shards(DB)
    ).

-spec drop_generation(emqx_ds:db(), slab()) -> ok | {error, _}.
drop_generation(DB, {Shard, GenId}) ->
    emqx_ds_storage_layer:drop_generation({DB, Shard}, GenId).

-spec drop_db(emqx_ds:db()) -> ok | {error, _}.
drop_db(DB) ->
    close_db(DB),
    lists:foreach(
        fun(Shard) ->
            emqx_ds_storage_layer:drop_shard({DB, Shard})
        end,
        emqx_ds_builtin_local_meta:shards(DB)
    ),
    emqx_ds_builtin_local_meta:drop_db(DB).

-spec store_batch(emqx_ds:db(), emqx_ds:batch(), emqx_ds:message_store_opts()) ->
    emqx_ds:store_batch_result().
store_batch(DB, Batch, Opts) ->
    case emqx_ds_builtin_local_meta:db_config(DB) of
        #{atomic_batches := true} ->
            store_batch_atomic(DB, Batch, Opts);
        _ ->
            store_batch_buffered(DB, Batch, Opts)
    end.

-spec new_tx(emqx_ds:db(), emqx_ds:transaction_opts()) ->
    {ok, tx_context()} | emqx_ds:error(_).
new_tx(DB, Options = #{shard := ShardOpt, generation := Generation}) ->
    case emqx_ds_builtin_local_meta:db_config(DB) of
        #{atomic_batches := true, store_ttv := true} ->
            case ShardOpt of
                {auto, Owner} ->
                    Shard = shard_of(DB, Owner);
                Shard ->
                    ok
            end,
            emqx_ds_optimistic_tx:new_kv_tx_ctx(?MODULE, DB, Shard, Generation, Options);
        _ ->
            ?err_unrec(database_does_not_support_transactions)
    end.

-spec commit_tx(emqx_ds:db(), tx_context(), emqx_ds:tx_ops()) -> reference().
commit_tx(DB, Ctx, Ops) ->
    emqx_ds_optimistic_tx:commit_kv_tx(DB, Ctx, Ops).

tx_commit_outcome(Reply) ->
    emqx_ds_optimistic_tx:tx_commit_outcome(Reply).

store_batch_buffered(DB, Messages, Opts) ->
    try
        emqx_ds_buffer:store_batch(DB, Messages, Opts)
    catch
        error:{Reason, _Call} when Reason == timeout; Reason == noproc ->
            {error, recoverable, Reason}
    end.

store_batch_atomic(DB, Batch, Opts) ->
    Shards = shards_of_batch(DB, Batch),
    case Shards of
        [Shard] ->
            emqx_ds_builtin_local_batch_serializer:store_batch_atomic(DB, Shard, Batch, Opts);
        [] ->
            ok;
        [_ | _] ->
            ?err_unrec(atomic_batch_spans_multiple_shards)
    end.

shards_of_batch(DB, #dsbatch{operations = Operations, preconditions = Preconditions}) ->
    shards_of_batch(DB, Preconditions, shards_of_batch(DB, Operations, []));
shards_of_batch(DB, Operations) ->
    shards_of_batch(DB, Operations, []).

shards_of_batch(DB, [Operation | Rest], Acc) ->
    case shard_of_operation(DB, Operation, clientid, #{}) of
        Shard when Shard =:= hd(Acc) ->
            shards_of_batch(DB, Rest, Acc);
        Shard when Acc =:= [] ->
            shards_of_batch(DB, Rest, [Shard]);
        ShardAnother ->
            [ShardAnother | Acc]
    end;
shards_of_batch(_DB, [], Acc) ->
    Acc.

-record(bs, {options :: emqx_ds:create_db_opts()}).
-type buffer_state() :: #bs{}.

-spec init_buffer(emqx_ds:db(), shard(), _Options) -> {ok, buffer_state()}.
init_buffer(DB, Shard, Options) ->
    ShardId = {DB, Shard},
    case current_timestamp(ShardId) of
        undefined ->
            Latest = erlang:system_time(microsecond),
            emqx_ds_builtin_local_meta:set_current_timestamp(ShardId, Latest);
        _Latest ->
            ok
    end,
    {ok, #bs{options = Options}}.

-spec flush_buffer(emqx_ds:db(), shard(), [emqx_types:message()], buffer_state()) ->
    {buffer_state(), emqx_ds:store_batch_result()}.
flush_buffer(DB, Shard, Messages, S0 = #bs{options = Options}) ->
    ShardId = {DB, Shard},
    ForceMonotonic = maps:get(force_monotonic_timestamps, Options),
    {Latest, Batch} = make_batch(ForceMonotonic, current_timestamp(ShardId), Messages),
    DispatchF = fun(Events) ->
        emqx_ds_beamformer:shard_event({DB, Shard}, Events)
    end,
    Result = emqx_ds_storage_layer:store_batch(ShardId, Batch, _Options = #{}, DispatchF),
    emqx_ds_builtin_local_meta:set_current_timestamp(ShardId, Latest),
    {S0, Result}.

make_batch(_ForceMonotonic = true, Latest, Messages) ->
    assign_monotonic_timestamps(Latest, Messages, []);
make_batch(false, Latest, Messages) ->
    assign_operation_timestamps(Latest, Messages, []).

assign_monotonic_timestamps(Latest0, [Message = #message{} | Rest], Acc0) ->
    case emqx_message:timestamp(Message, microsecond) of
        TimestampUs when TimestampUs > Latest0 ->
            Latest = TimestampUs;
        _Earlier ->
            Latest = Latest0 + 1
    end,
    Acc = [assign_timestamp(Latest, Message) | Acc0],
    assign_monotonic_timestamps(Latest, Rest, Acc);
assign_monotonic_timestamps(Latest, [Operation | Rest], Acc0) ->
    Acc = [Operation | Acc0],
    assign_monotonic_timestamps(Latest, Rest, Acc);
assign_monotonic_timestamps(Latest, [], Acc) ->
    {Latest, lists:reverse(Acc)}.

assign_operation_timestamps(Latest0, [Message = #message{} | Rest], Acc0) ->
    TimestampUs = emqx_message:timestamp(Message),
    Latest = max(TimestampUs, Latest0),
    Acc = [assign_timestamp(TimestampUs, Message) | Acc0],
    assign_operation_timestamps(Latest, Rest, Acc);
assign_operation_timestamps(Latest, [Operation | Rest], Acc0) ->
    Acc = [Operation | Acc0],
    assign_operation_timestamps(Latest, Rest, Acc);
assign_operation_timestamps(Latest, [], Acc) ->
    {Latest, lists:reverse(Acc)}.

assign_timestamp(TimestampUs, Message) ->
    {TimestampUs, Message}.

-spec shard_of_operation(emqx_ds:db(), emqx_ds:operation(), clientid | topic, _Options) -> shard().
shard_of_operation(DB, #message{from = From, topic = Topic}, SerializeBy, _Options) ->
    case SerializeBy of
        clientid -> Key = From;
        topic -> Key = Topic
    end,
    shard_of(DB, Key);
shard_of_operation(DB, {_, #message_matcher{from = From, topic = Topic}}, SerializeBy, _Options) ->
    case SerializeBy of
        clientid -> Key = From;
        topic -> Key = Topic
    end,
    shard_of(DB, Key);
shard_of_operation(_DB, #ds_tx{ctx = #kv_tx_ctx{shard = Shard}}, _SerializeBy, _Options) ->
    Shard.

list_shards(DB) ->
    emqx_ds_builtin_local_meta:shards(DB).

shard_of(DB, Key) ->
    N = emqx_ds_builtin_local_meta:n_shards(DB),
    Hash = erlang:phash2(Key, N),
    integer_to_binary(Hash).

-spec get_streams(emqx_ds:db(), emqx_ds:topic_filter(), emqx_ds:time(), emqx_ds:get_streams_opts()) ->
    emqx_ds:get_streams_result().
get_streams(DB, TopicFilter, StartTime, Opts) ->
    Shards =
        case Opts of
            #{shard := ReqShard} ->
                [ReqShard];
            _ ->
                emqx_ds_builtin_local_meta:shards(DB)
        end,
    MinGeneration = maps:get(generation_min, Opts, 0),
    #{store_ttv := IsTTV} = emqx_ds_builtin_local_meta:db_config(DB),
    Results =
        case IsTTV of
            false ->
                lists:flatmap(
                    fun(Shard) ->
                        Streams = emqx_ds_storage_layer:get_streams(
                            {DB, Shard}, TopicFilter, timestamp_to_timeus(StartTime), MinGeneration
                        ),
                        lists:map(
                            fun({Generation, InnerStream}) ->
                                Slab = {Shard, Generation},
                                {Slab, ?stream(Shard, InnerStream)}
                            end,
                            Streams
                        )
                    end,
                    Shards
                );
            true ->
                lists:flatmap(
                    fun(Shard) ->
                        Streams = emqx_ds_storage_layer_ttv:get_streams(
                            {DB, Shard}, TopicFilter, timestamp_to_timeus(StartTime), MinGeneration
                        ),
                        [
                            {{Shard, Stream#'Stream'.generation}, Stream}
                         || Stream <- Streams
                        ]
                    end,
                    Shards
                )
        end,
    {Results, []}.

-spec make_iterator(
    emqx_ds:db(), emqx_ds:ds_specific_stream(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    emqx_ds:make_iterator_result(iterator()).
make_iterator(DB, Stream = #'Stream'{}, TopicFilter, StartTime) ->
    emqx_ds_storage_layer_ttv:make_iterator(DB, Stream, TopicFilter, StartTime);
make_iterator(DB, ?stream(Shard, InnerStream), TopicFilter, StartTime) ->
    ShardId = {DB, Shard},
    case
        emqx_ds_storage_layer:make_iterator(
            ShardId, InnerStream, TopicFilter, timestamp_to_timeus(StartTime)
        )
    of
        {ok, Iter} ->
            {ok, #{?tag => ?IT, ?shard => Shard, ?enc => Iter}};
        Error = {error, _, _} ->
            Error
    end.

-spec update_iterator(_Shard, emqx_ds:ds_specific_iterator(), emqx_ds:message_key()) ->
    emqx_ds:make_iterator_result(iterator()).
update_iterator(DBShard, Iter0 = #'Iterator'{}, Key) ->
    emqx_ds_storage_layer_ttv:update_iterator(DBShard, Iter0, Key);
update_iterator(ShardId, Iter0 = #{?tag := ?IT, ?enc := StorageIter0}, Key) ->
    case emqx_ds_storage_layer:update_iterator(ShardId, StorageIter0, Key) of
        {ok, StorageIter} ->
            {ok, Iter0#{?enc => StorageIter}};
        Err = {error, _, _} ->
            Err
    end.

-spec next(emqx_ds:db(), iterator(), pos_integer()) -> emqx_ds:next_result(iterator()).
next(DB, Iter, N) ->
    {ok, _, Ref} = emqx_ds_lib:with_worker(?MODULE, do_next, [DB, Iter, N]),
    receive
        {Ref, Result} ->
            Result
    end.

-spec subscribe(emqx_ds:db(), iterator(), emqx_ds:sub_opts()) ->
    {ok, emqx_ds:subscription_handle(), reference()}.
subscribe(DB, It, SubOpts) ->
    case It of
        #{?tag := ?IT, ?shard := Shard} ->
            ok;
        #'Iterator'{shard = Shard} ->
            ok
    end,
    Server = emqx_ds_beamformer:where({DB, Shard}),
    MRef = monitor(process, Server),
    case emqx_ds_beamformer:subscribe(Server, self(), MRef, It, SubOpts) of
        {ok, MRef} ->
            {ok, {Shard, MRef}, MRef};
        Err = {error, _, _} ->
            Err
    end.

-spec subscription_info(emqx_ds:db(), emqx_ds:subscription_handle()) ->
    emqx_ds:sub_info() | undefined.
subscription_info(DB, {Shard, SubRef}) ->
    emqx_ds_beamformer:subscription_info({DB, Shard}, SubRef).

-spec unsubscribe(emqx_ds:db(), emqx_ds:subscription_handle()) -> boolean().
unsubscribe(DB, {Shard, SubId}) ->
    emqx_ds_beamformer:unsubscribe({DB, Shard}, SubId).

-spec suback(emqx_ds:db(), emqx_ds:subscription_handle(), emqx_ds:sub_seqno()) ->
    ok | {error, subscription_not_found}.
suback(DB, {Shard, SubRef}, SeqNo) ->
    emqx_ds_beamformer:suback({DB, Shard}, SubRef, SeqNo).

unpack_iterator(DBShard, Iterator = #'Iterator'{}) ->
    emqx_ds_storage_layer_ttv:unpack_iterator(DBShard, Iterator);
unpack_iterator(DBShard, #{?tag := ?IT, ?enc := Iterator}) ->
    emqx_ds_storage_layer:unpack_iterator(DBShard, Iterator).

high_watermark(DBShard, Stream) ->
    Now = current_timestamp(DBShard),
    case Stream of
        #'Stream'{} ->
            emqx_ds_storage_layer_ttv:high_watermark(DBShard, Stream, Now);
        _ ->
            emqx_ds_storage_layer:high_watermark(DBShard, Stream, Now)
    end.

fast_forward(DBShard, It = #'Iterator'{}, Key) ->
    emqx_ds_storage_layer_ttv:fast_forward(DBShard, It, Key);
fast_forward(DBShard, It = #{?tag := ?IT, ?enc := Inner0}, Key) ->
    Now = current_timestamp(DBShard),
    case emqx_ds_storage_layer:fast_forward(DBShard, Inner0, Key, Now) of
        {ok, end_of_stream} ->
            {ok, end_of_stream};
        {ok, Inner} ->
            {ok, It#{?enc := Inner}};
        {error, _, _} = Err ->
            Err
    end.

message_match_context(DBShard, Stream = #'Stream'{}, MsgKey, TTV) ->
    emqx_ds_storage_layer_ttv:message_match_context(DBShard, Stream, MsgKey, TTV);
message_match_context(DBShard, Stream, MsgKey, Message) ->
    emqx_ds_storage_layer:message_match_context(DBShard, Stream, MsgKey, Message).

iterator_match_context(DBShard, Iterator = #'Iterator'{}) ->
    emqx_ds_storage_layer_ttv:iterator_match_context(DBShard, Iterator);
iterator_match_context(DBShard, #{?tag := ?IT, ?enc := Iterator}) ->
    emqx_ds_storage_layer:iterator_match_context(DBShard, Iterator).

scan_stream(DBShard, Stream, TopicFilter, StartMsg, BatchSize) ->
    {DB, _} = DBShard,
    Now = current_timestamp(DBShard),
    T0 = erlang:monotonic_time(microsecond),
    Result =
        case Stream of
            #'Stream'{} ->
                emqx_ds_storage_layer_ttv:scan_stream(
                    DBShard, Stream, TopicFilter, infinity, StartMsg, BatchSize
                );
            _ ->
                emqx_ds_storage_layer:scan_stream(
                    DBShard, Stream, TopicFilter, Now, StartMsg, BatchSize
                )
        end,
    T1 = erlang:monotonic_time(microsecond),
    emqx_ds_builtin_metrics:observe_next_time(DB, T1 - T0),
    Result.

-spec get_delete_streams(emqx_ds:db(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    [emqx_ds:ds_specific_delete_stream()].
get_delete_streams(DB, TopicFilter, StartTime) ->
    Shards = emqx_ds_builtin_local_meta:shards(DB),
    lists:flatmap(
        fun(Shard) ->
            Streams = emqx_ds_storage_layer:get_delete_streams(
                {DB, Shard}, TopicFilter, timestamp_to_timeus(StartTime)
            ),
            lists:map(
                fun(InnerStream) ->
                    ?delete_stream(Shard, InnerStream)
                end,
                Streams
            )
        end,
        Shards
    ).

-spec make_delete_iterator(
    emqx_ds:db(), emqx_ds:ds_specific_delete_stream(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    emqx_ds:make_delete_iterator_result(delete_iterator()).
make_delete_iterator(DB, ?delete_stream(Shard, InnerStream), TopicFilter, StartTime) ->
    ShardId = {DB, Shard},
    case
        emqx_ds_storage_layer:make_delete_iterator(
            ShardId, InnerStream, TopicFilter, timestamp_to_timeus(StartTime)
        )
    of
        {ok, Iter} ->
            {ok, #{?tag => ?DELETE_IT, ?shard => Shard, ?enc => Iter}};
        Error = {error, _, _} ->
            Error
    end.

-spec delete_next(emqx_ds:db(), delete_iterator(), emqx_ds:delete_selector(), pos_integer()) ->
    emqx_ds:delete_next_result(emqx_ds:delete_iterator()).
delete_next(DB, Iter, Selector, N) ->
    {ok, _, Ref} = emqx_ds_lib:with_worker(?MODULE, do_delete_next, [DB, Iter, Selector, N]),
    receive
        {Ref, Result} -> Result
    end.

%%================================================================================
%% Internal exports
%%================================================================================

current_timestamp(ShardId) ->
    emqx_ds_builtin_local_meta:current_timestamp(ShardId).

-spec do_next(emqx_ds:db(), iterator(), emqx_ds:next_limit()) -> emqx_ds:next_result(iterator()).
do_next(DB, Iter0 = #'Iterator'{shard = Shard}, NextLimit) ->
    %% New style DB: "TTV"
    T0 = erlang:monotonic_time(microsecond),
    {BatchSize, TimeLimit} = batch_size_and_time_limit(true, DB, Shard, NextLimit),
    Result = emqx_ds_storage_layer_ttv:next(DB, Iter0, BatchSize, TimeLimit),
    T1 = erlang:monotonic_time(microsecond),
    emqx_ds_builtin_metrics:observe_next_time(DB, T1 - T0),
    Result;
do_next(DB, Iter0 = #{?tag := ?IT, ?shard := Shard, ?enc := StorageIter0}, NextLimit) ->
    DBShard = {DB, Shard},
    T0 = erlang:monotonic_time(microsecond),
    {BatchSize, TimeLimit} = batch_size_and_time_limit(false, DB, Shard, NextLimit),
    Result = emqx_ds_storage_layer:next(
        DBShard, StorageIter0, BatchSize, TimeLimit, false
    ),
    T1 = erlang:monotonic_time(microsecond),
    emqx_ds_builtin_metrics:observe_next_time(DB, T1 - T0),
    case Result of
        {ok, StorageIter, Batch} ->
            Iter = Iter0#{?enc := StorageIter},
            {ok, Iter, Batch};
        Other ->
            Other
    end.

-spec do_delete_next(emqx_ds:db(), delete_iterator(), emqx_ds:delete_selector(), pos_integer()) ->
    emqx_ds:delete_next_result(delete_iterator()).
do_delete_next(
    DB, Iter = #{?tag := ?DELETE_IT, ?shard := Shard, ?enc := StorageIter0}, Selector, N
) ->
    ShardId = {DB, Shard},
    case
        emqx_ds_storage_layer:delete_next(
            ShardId, StorageIter0, Selector, N, current_timestamp(ShardId)
        )
    of
        {ok, StorageIter, Ndeleted} ->
            {ok, Iter#{?enc => StorageIter}, Ndeleted};
        {ok, end_of_stream} ->
            {ok, end_of_stream};
        Error ->
            Error
    end.

%%================================================================================
%% Optimistic TX
%%================================================================================

-define(serial_key, <<"emqx_ds_builtin_local_tx_serial">>).

otx_get_tx_serial(DB, Shard) ->
    emqx_ds_storage_layer_ttv:get_read_tx_serial({DB, Shard}).

otx_become_leader(DB, Shard) ->
    {ok, Serial} = get_tx_persistent_serial(DB, Shard),
    case current_timestamp({DB, Shard}) of
        undefined ->
            Timestamp = 0;
        Timestamp when is_integer(Timestamp) ->
            ok
    end,
    {ok, Serial, Timestamp}.

otx_get_leader(DB, Shard) ->
    emqx_ds_optimistic_tx:where(DB, Shard).

otx_prepare_tx(DBShard, Generation, SerialBin, Ops, Opts) ->
    emqx_ds_storage_layer_ttv:prepare_tx(DBShard, Generation, SerialBin, Ops, Opts).

otx_commit_tx_batch(DBShard = {DB, Shard}, SerCtl, Serial, Timestamp, Batches) ->
    case get_tx_persistent_serial(DB, Shard) of
        {ok, SerCtl} ->
            maybe
                %% First, update the leader serial to avoid duplicated
                %% serials, should the following calls fail:
                ok ?=
                    emqx_ds_storage_layer:store_global(
                        DBShard, #{?serial_key => <<Serial:128>>}, #{}
                    ),
                %% Commit data:
                ok ?= emqx_ds_storage_layer_ttv:commit_batch(DBShard, Batches, #{}),
                %% Finally, update read serial:
                emqx_ds_storage_layer_ttv:set_read_tx_serial(DBShard, Serial),
                %% Dispatch events:
                DispatchF = fun(Stream) -> emqx_ds_beamformer:shard_event(DBShard, [Stream]) end,
                emqx_ds_storage_layer_ttv:dispatch_events(DBShard, Batches, DispatchF)
            end;
        Val ->
            ?err_unrec({serial_mismatch, SerCtl, Val})
    end.

otx_lookup_ttv(DBShard, GenId, Topic, Timestamp) ->
    emqx_ds_storage_layer_ttv:lookup(DBShard, GenId, Topic, Timestamp).

otx_get_runtime_config(DB) ->
    #{transaction := Val} = emqx_ds_builtin_local_meta:db_config(DB),
    Val.

stream_to_binary(_DB, Stream = #'Stream'{}) ->
    'DSBuiltinMetadata':encode('Stream', Stream);
stream_to_binary(DB, ?stream(Shard, Inner)) ->
    stream_to_binary(DB, emqx_ds_storage_layer:old_stream_to_new(Shard, Inner)).

binary_to_stream(DB, Bin) ->
    maybe
        {ok, Stream} ?= 'DSBuiltinMetadata':decode('Stream', Bin),
        case emqx_ds_builtin_local_meta:db_config(DB) of
            #{store_ttv := true} ->
                {ok, Stream};
            #{store_ttv := false} ->
                case emqx_ds_storage_layer:new_stream_to_old(Stream) of
                    {ok, Shard, Inner} ->
                        {ok, ?stream(Shard, Inner)};
                    {error, _} = Err ->
                        Err
                end
        end
    end.

iterator_to_binary(_DB, end_of_stream) ->
    'DSBuiltinMetadata':encode('ReplayPosition', {endOfStream, 'NULL'});
iterator_to_binary(_DB, It = #'Iterator'{}) ->
    'DSBuiltinMetadata':encode('ReplayPosition', {value, It});
iterator_to_binary(_DB, #{?tag := ?IT, ?shard := Shard, ?enc := Inner}) ->
    It = emqx_ds_storage_layer:old_iterator_to_new(Shard, Inner),
    'DSBuiltinMetadata':encode('ReplayPosition', {value, It}).

binary_to_iterator(DB, Bin) ->
    case 'DSBuiltinMetadata':decode('ReplayPosition', Bin) of
        {ok, {endOfStream, 'NULL'}} ->
            {ok, end_of_stream};
        {ok, {value, It}} ->
            case emqx_ds_builtin_local_meta:db_config(DB) of
                #{store_ttv := true} ->
                    {ok, It};
                #{store_ttv := false} ->
                    case emqx_ds_storage_layer:new_iterator_to_old(It) of
                        {ok, Shard, Inner} ->
                            {ok, #{?tag => ?IT, ?shard => Shard, ?enc => Inner}};
                        Err ->
                            Err
                    end
            end
    end.

-spec batch_size_and_time_limit(
    _StoreTTV :: boolean(), emqx_ds:db(), shard(), emqx_ds:next_limit()
) ->
    {pos_integer(), emqx_ds:time()}.
batch_size_and_time_limit(false, DB, Shard, BatchSize) when is_integer(BatchSize) ->
    {BatchSize, current_timestamp({DB, Shard})};
batch_size_and_time_limit(false, DB, Shard, {time, MaxTS, BatchSize}) ->
    {BatchSize, min(current_timestamp({DB, Shard}), MaxTS)};
batch_size_and_time_limit(true, _DB, _Shard, BatchSize) when is_integer(BatchSize) ->
    {BatchSize, infinity};
batch_size_and_time_limit(true, _DB, _Shard, {time, MaxTS, BatchSize}) ->
    {BatchSize, MaxTS}.

%%================================================================================
%% Internal functions
%%================================================================================

timestamp_to_timeus(TimestampMs) ->
    TimestampMs * 1000.

timeus_to_timestamp(undefined) ->
    undefined;
timeus_to_timestamp(TimestampUs) ->
    TimestampUs div 1000.

get_tx_persistent_serial(DB, Shard) ->
    case emqx_ds_storage_layer:fetch_global({DB, Shard}, ?serial_key) of
        {ok, <<Val:128>>} ->
            {ok, Val};
        not_found ->
            {ok, 0}
    end.

%%================================================================================
%% Common test options
%%================================================================================

-ifdef(TEST).

test_applications(Config) ->
    [
        {App, maps:get(App, Config, #{})}
     || App <- [emqx_durable_storage, emqx_ds_backends]
    ].

test_db_config(_Config) ->
    #{
        backend => builtin_local,
        storage => {emqx_ds_storage_skipstream_lts, #{with_guid => true}},
        n_shards => 1
    }.

-endif.
