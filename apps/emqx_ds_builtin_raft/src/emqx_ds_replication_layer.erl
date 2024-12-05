%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Replication layer for DS backends that don't support
%% replication on their own.
-module(emqx_ds_replication_layer).

%-behaviour(emqx_ds).
-behaviour(emqx_ds_buffer).

-export([
    list_shards/1,
    open_db/2,
    close_db/1,
    add_generation/1,
    add_generation/2,
    update_db_config/2,
    list_generations_with_lifetimes/1,
    drop_generation/2,
    drop_db/1,
    store_batch/3,
    get_streams/3,
    get_delete_streams/3,
    make_iterator/4,
    make_delete_iterator/4,
    next/3,
    poll/3,
    delete_next/4,

    current_timestamp/2,

    shard_of_operation/4,
    flush_buffer/4,
    init_buffer/3
]).

%% internal exports:
-export([
    %% RPC Targets:
    do_drop_db_v1/1,
    do_get_streams_v1/4,
    do_get_streams_v2/4,
    do_make_iterator_v2/5,
    do_update_iterator_v2/4,
    do_next_v1/4,
    do_list_generations_with_lifetimes_v3/2,
    do_get_delete_streams_v4/4,
    do_make_delete_iterator_v4/5,
    do_delete_next_v4/5,
    do_poll_v1/5,
    %% Obsolete:
    do_store_batch_v1/4,
    do_make_iterator_v1/5,
    do_add_generation_v2/1,
    do_drop_generation_v3/3,

    %% Egress API:
    ra_store_batch/3
]).

-behaviour(ra_machine).
-export([
    init/1,
    apply/3,
    tick/2,

    state_enter/2,

    snapshot_module/0
]).

-behaviour(emqx_ds_beamformer).
-export(
    [
        unpack_iterator/2,
        scan_stream/5,
        update_iterator/3
    ]
).

-ifdef(TEST).
-export([test_applications/1, test_db_config/1]).
-endif.

-export_type([
    shard_id/0,
    builtin_db_opts/0,
    stream_v1/0,
    stream/0,
    delete_stream/0,
    iterator/0,
    delete_iterator/0,
    batch/0
]).

-export_type([
    ra_state/0
]).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include("emqx_ds_replication_layer.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type shard_id() :: binary().

-type builtin_db_opts() ::
    #{
        backend := builtin_raft,
        storage := emqx_ds_storage_layer:prototype(),
        n_shards => pos_integer(),
        n_sites => pos_integer(),
        replication_factor => pos_integer(),
        replication_options => _TODO :: #{},
        %% Equivalent to `append_only' from `emqx_ds:create_db_opts'
        force_monotonic_timestamps => boolean(),
        atomic_batches => boolean()
    }.

%% This enapsulates the stream entity from the replication level.
%%
%% TODO: this type is obsolete and is kept only for compatibility with
%% v3 BPAPI. Remove it when emqx_ds_proto_v4 is gone (EMQX 5.6)
-opaque stream_v1() ::
    #{
        ?tag := ?STREAM,
        ?shard := emqx_ds_replication_layer:shard_id(),
        ?enc := emqx_ds_storage_layer:stream_v1()
    }.

-define(stream_v2(SHARD, INNER), [2, SHARD | INNER]).
-define(delete_stream(SHARD, INNER), [3, SHARD | INNER]).

-opaque stream() :: nonempty_maybe_improper_list().

-opaque delete_stream() :: nonempty_maybe_improper_list().

-opaque iterator() ::
    #{
        ?tag := ?IT,
        ?shard := emqx_ds_replication_layer:shard_id(),
        ?enc := emqx_ds_storage_layer:iterator()
    }.

-opaque delete_iterator() ::
    #{
        ?tag := ?DELETE_IT,
        ?shard := emqx_ds_replication_layer:shard_id(),
        ?enc := emqx_ds_storage_layer:delete_iterator()
    }.

%% Write batch.
%% Instances of this type currently form the majority of the Raft log.
-type batch() :: #{
    ?tag := ?BATCH,
    ?batch_operations := [emqx_ds:operation()],
    ?batch_preconditions => [emqx_ds:precondition()]
}.

-type generation_rank() :: {shard_id(), term()}.

%% Core state of the replication, i.e. the state of ra machine.
-type ra_state() :: #{
    %% Shard ID.
    db_shard := {emqx_ds:db(), shard_id()},

    %% Unique timestamp tracking real time closely.
    %% With microsecond granularity it should be nearly impossible for it to run
    %% too far ahead of the real time clock.
    latest := timestamp_us()
}.

%% Command. Each command is an entry in the replication log.
-type ra_command() :: #{
    ?tag := ?BATCH | add_generation | update_config | drop_generation | storage_event,
    _ => _
}.

-type timestamp_us() :: non_neg_integer().

-define(gv_timestamp(SHARD), {gv_timestamp, SHARD}).

%%================================================================================
%% API functions
%%================================================================================

-spec list_shards(emqx_ds:db()) -> [shard_id()].
list_shards(DB) ->
    emqx_ds_replication_layer_meta:shards(DB).

-spec open_db(emqx_ds:db(), builtin_db_opts()) -> ok | {error, _}.
open_db(DB, CreateOpts0) ->
    %% Rename `append_only' flag to `force_monotonic_timestamps':
    AppendOnly = maps:get(append_only, CreateOpts0),
    CreateOpts = maps:put(force_monotonic_timestamps, AppendOnly, CreateOpts0),
    case emqx_ds_builtin_raft_sup:start_db(DB, CreateOpts) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, Err} ->
            {error, Err}
    end.

-spec close_db(emqx_ds:db()) -> ok.
close_db(DB) ->
    emqx_ds_builtin_raft_sup:stop_db(DB).

-spec add_generation(emqx_ds:db()) -> ok | {error, _}.
add_generation(DB) ->
    add_generation(DB, emqx_ds:timestamp_us()).

-spec add_generation(emqx_ds:db(), emqx_ds:time()) -> ok | {error, _}.
add_generation(DB, Since) ->
    foreach_shard(
        DB,
        fun(Shard) -> ok = ra_add_generation(DB, Shard, Since) end
    ).

-spec update_db_config(emqx_ds:db(), builtin_db_opts()) -> ok | {error, _}.
update_db_config(DB, CreateOpts) ->
    Opts = #{} = emqx_ds_replication_layer_meta:update_db_config(DB, CreateOpts),
    Since = emqx_ds:timestamp_us(),
    foreach_shard(
        DB,
        fun(Shard) -> ok = ra_update_config(DB, Shard, Opts, Since) end
    ).

-spec list_generations_with_lifetimes(emqx_ds:db()) ->
    #{generation_rank() => emqx_ds:generation_info()}.
list_generations_with_lifetimes(DB) ->
    Shards = list_shards(DB),
    lists:foldl(
        fun(Shard, GensAcc) ->
            case ra_list_generations_with_lifetimes(DB, Shard) of
                Gens = #{} ->
                    ok;
                {error, _Class, _Reason} ->
                    %% TODO: log error
                    Gens = #{}
            end,
            maps:fold(
                fun(GenId, Data, AccInner) ->
                    AccInner#{{Shard, GenId} => Data}
                end,
                GensAcc,
                Gens
            )
        end,
        #{},
        Shards
    ).

-spec drop_generation(emqx_ds:db(), generation_rank()) -> ok | {error, _}.
drop_generation(DB, {Shard, GenId}) ->
    ra_drop_generation(DB, Shard, GenId).

-spec drop_db(emqx_ds:db()) -> ok | {error, _}.
drop_db(DB) ->
    foreach_shard(DB, fun(Shard) ->
        {ok, _} = ra_drop_shard(DB, Shard)
    end),
    _ = emqx_ds_proto_v4:drop_db(list_nodes(), DB),
    emqx_ds_replication_layer_meta:drop_db(DB).

-spec store_batch(emqx_ds:db(), emqx_ds:batch(), emqx_ds:message_store_opts()) ->
    emqx_ds:store_batch_result().
store_batch(DB, Batch = #dsbatch{preconditions = [_ | _]}, Opts) ->
    %% NOTE: Atomic batch is implied, will not check with DB config.
    store_batch_atomic(DB, Batch, Opts);
store_batch(DB, Batch, Opts) ->
    case emqx_ds_replication_layer_meta:db_config(DB) of
        #{atomic_batches := true} ->
            store_batch_atomic(DB, Batch, Opts);
        #{} ->
            store_batch_buffered(DB, Batch, Opts)
    end.

store_batch_buffered(DB, #dsbatch{operations = Operations}, Opts) ->
    store_batch_buffered(DB, Operations, Opts);
store_batch_buffered(DB, Batch, Opts) ->
    try
        emqx_ds_buffer:store_batch(DB, Batch, Opts)
    catch
        error:{Reason, _Call} when Reason == timeout; Reason == noproc ->
            {error, recoverable, Reason}
    end.

store_batch_atomic(DB, Batch, _Opts) ->
    Shards = shards_of_batch(DB, Batch),
    case Shards of
        [Shard] ->
            case ra_store_batch(DB, Shard, Batch) of
                {timeout, ServerId} ->
                    {error, recoverable, {timeout, ServerId}};
                Result ->
                    Result
            end;
        [] ->
            ok;
        [_ | _] ->
            {error, unrecoverable, atomic_batch_spans_multiple_shards}
    end.

-spec get_streams(emqx_ds:db(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    [{emqx_ds:stream_rank(), stream()}].
get_streams(DB, TopicFilter, StartTime) ->
    Shards = list_shards(DB),
    lists:flatmap(
        fun(Shard) ->
            case ra_get_streams(DB, Shard, TopicFilter, StartTime) of
                Streams when is_list(Streams) ->
                    ok;
                {error, Class, Reason} ->
                    ?tp(debug, ds_repl_get_streams_failed, #{
                        db => DB,
                        shard => Shard,
                        class => Class,
                        reason => Reason
                    }),
                    Streams = []
            end,
            lists:map(
                fun({RankY, StorageLayerStream}) ->
                    RankX = Shard,
                    Rank = {RankX, RankY},
                    {Rank, ?stream_v2(Shard, StorageLayerStream)}
                end,
                Streams
            )
        end,
        Shards
    ).

-spec get_delete_streams(emqx_ds:db(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    [delete_stream()].
get_delete_streams(DB, TopicFilter, StartTime) ->
    Shards = list_shards(DB),
    lists:flatmap(
        fun(Shard) ->
            Streams = ra_get_delete_streams(DB, Shard, TopicFilter, StartTime),
            lists:map(
                fun(StorageLayerStream) ->
                    ?delete_stream(Shard, StorageLayerStream)
                end,
                Streams
            )
        end,
        Shards
    ).

-spec make_iterator(emqx_ds:db(), stream(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    emqx_ds:make_iterator_result(iterator()).
make_iterator(DB, Stream, TopicFilter, StartTime) ->
    ?stream_v2(Shard, StorageStream) = Stream,
    case ra_make_iterator(DB, Shard, StorageStream, TopicFilter, StartTime) of
        {ok, Iter} ->
            {ok, #{?tag => ?IT, ?shard => Shard, ?enc => Iter}};
        Error = {error, _, _} ->
            Error
    end.

-spec make_delete_iterator(emqx_ds:db(), delete_stream(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    emqx_ds:make_delete_iterator_result(delete_iterator()).
make_delete_iterator(DB, Stream, TopicFilter, StartTime) ->
    ?delete_stream(Shard, StorageStream) = Stream,
    case ra_make_delete_iterator(DB, Shard, StorageStream, TopicFilter, StartTime) of
        {ok, Iter} ->
            {ok, #{?tag => ?DELETE_IT, ?shard => Shard, ?enc => Iter}};
        Error = {error, _, _} ->
            Error
    end.

-spec next(emqx_ds:db(), iterator(), pos_integer()) -> emqx_ds:next_result(iterator()).
next(DB, Iter0, BatchSize) ->
    #{?tag := ?IT, ?shard := Shard, ?enc := StorageIter0} = Iter0,
    %% TODO: iterator can contain information that is useful for
    %% reconstructing messages sent over the network. For example,
    %% when we send messages with the learned topic index, we could
    %% send the static part of topic once, and append it to the
    %% messages on the receiving node, hence saving some network.
    %%
    %% This kind of trickery should be probably done here in the
    %% replication layer. Or, perhaps, in the logic layer.
    T0 = erlang:monotonic_time(microsecond),
    Result = ra_next(DB, Shard, StorageIter0, BatchSize),
    T1 = erlang:monotonic_time(microsecond),
    emqx_ds_builtin_metrics:observe_next_time(DB, T1 - T0),
    case Result of
        {ok, StorageIter, Batch} ->
            Iter = Iter0#{?enc := StorageIter},
            {ok, Iter, Batch};
        Other ->
            Other
    end.

-spec poll(emqx_ds:db(), emqx_ds:poll_iterators(), emqx_ds:poll_opts()) ->
    {ok, reference()}.
poll(DB, Iterators, PollOpts = #{timeout := Timeout}) ->
    %% Create a new alias, if not already provided:
    case PollOpts of
        #{reply_to := ReplyTo} ->
            ok;
        _ ->
            ReplyTo = alias([explicit_unalias])
    end,
    %% Spawn a helper process that will notify the caller when the
    %% poll times out:
    _Completion = emqx_ds_lib:send_poll_timeout(ReplyTo, Timeout),
    %% Submit poll jobs:
    Groups = maps:groups_from_list(
        fun({_Token, #{?tag := ?IT, ?shard := Shard}}) -> Shard end,
        Iterators
    ),
    maps:foreach(
        fun(Shard, ShardIts) ->
            Result = ra_poll(
                DB,
                Shard,
                [{{ReplyTo, Token}, It} || {Token, It} <- ShardIts],
                PollOpts
            ),
            case Result of
                ok ->
                    ok;
                {error, Class, Reason} ->
                    ?tp(debug, ds_repl_poll_shard_failed, #{
                        db => DB,
                        shard => Shard,
                        class => Class,
                        reason => Reason
                    })
            end
        end,
        Groups
    ),
    {ok, ReplyTo}.

-spec delete_next(emqx_ds:db(), delete_iterator(), emqx_ds:delete_selector(), pos_integer()) ->
    emqx_ds:delete_next_result(delete_iterator()).
delete_next(DB, Iter0, Selector, BatchSize) ->
    #{?tag := ?DELETE_IT, ?shard := Shard, ?enc := StorageIter0} = Iter0,
    case ra_delete_next(DB, Shard, StorageIter0, Selector, BatchSize) of
        {ok, StorageIter, NumDeleted} ->
            Iter = Iter0#{?enc := StorageIter},
            {ok, Iter, NumDeleted};
        Other ->
            Other
    end.

-spec foreach_shard(emqx_ds:db(), fun((shard_id()) -> _)) -> ok.
foreach_shard(DB, Fun) ->
    lists:foreach(Fun, list_shards(DB)).

%% @doc Messages have been replicated up to this timestamp on the
%% local server
-spec current_timestamp(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) -> emqx_ds:time().
current_timestamp(DB, Shard) ->
    emqx_ds_builtin_raft_sup:get_gvar(DB, ?gv_timestamp(Shard), 0).

%%================================================================================
%% emqx_ds_buffer callbacks
%%================================================================================

-record(bs, {}).
-type egress_state() :: #bs{}.

-spec init_buffer(emqx_ds:db(), shard_id(), _Options) -> {ok, egress_state()}.
init_buffer(_DB, _Shard, _Options) ->
    {ok, #bs{}}.

-spec flush_buffer(emqx_ds:db(), shard_id(), [emqx_types:message()], egress_state()) ->
    {egress_state(), ok | emqx_ds:error(_)}.
flush_buffer(DB, Shard, Messages, State) ->
    case ra_store_batch(DB, Shard, Messages) of
        {timeout, ServerId} ->
            Result = {error, recoverable, {timeout, ServerId}};
        Result ->
            ok
    end,
    {State, Result}.

-spec shard_of_operation(
    emqx_ds:db(),
    emqx_ds:operation() | emqx_ds:precondition(),
    clientid | topic,
    _Options
) ->
    emqx_ds_replication_layer:shard_id().
shard_of_operation(DB, #message{from = From, topic = Topic}, SerializeBy, _Options) ->
    case SerializeBy of
        clientid -> Key = From;
        topic -> Key = Topic
    end,
    shard_of_key(DB, Key);
shard_of_operation(DB, {_OpName, Matcher}, SerializeBy, _Options) ->
    #message_matcher{from = From, topic = Topic} = Matcher,
    case SerializeBy of
        clientid -> Key = From;
        topic -> Key = Topic
    end,
    shard_of_key(DB, Key).

shard_of_key(DB, Key) ->
    N = emqx_ds_replication_shard_allocator:n_shards(DB),
    Hash = erlang:phash2(Key, N),
    integer_to_binary(Hash).

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

%%================================================================================
%% Internal exports (RPC targets)
%%================================================================================

%% NOTE
%% Target node may still be in the process of starting up when RPCs arrive, it's
%% good to have them handled gracefully.
%% TODO
%% There's a possibility of race condition: storage may shut down right after we
%% ask for its status.
-define(IF_SHARD_READY(SHARDID, EXPR),
    case emqx_ds_builtin_raft_db_sup:shard_info(SHARDID, ready) of
        true -> EXPR;
        _Unready -> {error, recoverable, shard_unavailable}
    end
).

-spec do_drop_db_v1(emqx_ds:db()) -> ok | {error, _}.
do_drop_db_v1(DB) ->
    MyShards = emqx_ds_replication_layer_meta:my_shards(DB),
    emqx_ds_builtin_raft_sup:stop_db(DB),
    lists:foreach(
        fun(Shard) ->
            emqx_ds_storage_layer:drop_shard({DB, Shard})
        end,
        MyShards
    ).

-spec do_store_batch_v1(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    batch(),
    emqx_ds:message_store_opts()
) ->
    no_return().
do_store_batch_v1(_DB, _Shard, _Batch, _Options) ->
    error(obsolete_api).

%% Remove me in EMQX 5.6
-dialyzer({nowarn_function, do_get_streams_v1/4}).
-spec do_get_streams_v1(
    emqx_ds:db(), emqx_ds_replication_layer:shard_id(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    [{integer(), emqx_ds_storage_layer:stream_v1()}].
do_get_streams_v1(_DB, _Shard, _TopicFilter, _StartTime) ->
    error(obsolete_api).

-spec do_get_streams_v2(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    [{integer(), emqx_ds_storage_layer:stream()}] | emqx_ds:error(storage_down).
do_get_streams_v2(DB, Shard, TopicFilter, StartTime) ->
    ShardId = {DB, Shard},
    ?IF_SHARD_READY(
        ShardId,
        emqx_ds_storage_layer:get_streams(ShardId, TopicFilter, StartTime)
    ).

-dialyzer({nowarn_function, do_make_iterator_v1/5}).
-spec do_make_iterator_v1(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer:stream_v1(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    emqx_ds:make_iterator_result(emqx_ds_storage_layer:iterator()).
do_make_iterator_v1(_DB, _Shard, _Stream, _TopicFilter, _StartTime) ->
    error(obsolete_api).

-spec do_make_iterator_v2(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer:stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    emqx_ds:make_iterator_result(emqx_ds_storage_layer:iterator()).
do_make_iterator_v2(DB, Shard, Stream, TopicFilter, StartTime) ->
    ShardId = {DB, Shard},
    ?IF_SHARD_READY(
        ShardId,
        emqx_ds_storage_layer:make_iterator(ShardId, Stream, TopicFilter, StartTime)
    ).

-spec do_make_delete_iterator_v4(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer:delete_stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    {ok, emqx_ds_storage_layer:delete_iterator()} | {error, _}.
do_make_delete_iterator_v4(DB, Shard, Stream, TopicFilter, StartTime) ->
    emqx_ds_storage_layer:make_delete_iterator({DB, Shard}, Stream, TopicFilter, StartTime).

-spec do_update_iterator_v2(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer:iterator(),
    emqx_ds:message_key()
) ->
    emqx_ds:make_iterator_result(emqx_ds_storage_layer:iterator()).
do_update_iterator_v2(DB, Shard, OldIter, DSKey) ->
    emqx_ds_storage_layer:update_iterator({DB, Shard}, OldIter, DSKey).

-spec do_next_v1(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer:iterator(),
    pos_integer()
) ->
    emqx_ds:next_result(emqx_ds_storage_layer:iterator()).
do_next_v1(DB, Shard, Iter, BatchSize) ->
    ShardId = {DB, Shard},
    ?IF_SHARD_READY(
        ShardId,
        emqx_ds_storage_layer:next(
            ShardId, Iter, BatchSize, emqx_ds_replication_layer:current_timestamp(DB, Shard)
        )
    ).

-spec do_delete_next_v4(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer:delete_iterator(),
    emqx_ds:delete_selector(),
    pos_integer()
) ->
    emqx_ds:delete_next_result(emqx_ds_storage_layer:delete_iterator()).
do_delete_next_v4(DB, Shard, Iter, Selector, BatchSize) ->
    emqx_ds_storage_layer:delete_next(
        {DB, Shard},
        Iter,
        Selector,
        BatchSize,
        emqx_ds_replication_layer:current_timestamp(DB, Shard)
    ).

-spec do_add_generation_v2(emqx_ds:db()) -> no_return().
do_add_generation_v2(_DB) ->
    error(obsolete_api).

-spec do_list_generations_with_lifetimes_v3(emqx_ds:db(), shard_id()) ->
    #{emqx_ds:ds_specific_generation_rank() => emqx_ds:generation_info()}
    | emqx_ds:error(storage_down).
do_list_generations_with_lifetimes_v3(DB, Shard) ->
    ShardId = {DB, Shard},
    ?IF_SHARD_READY(
        ShardId,
        emqx_ds_storage_layer:list_generations_with_lifetimes(ShardId)
    ).

-spec do_drop_generation_v3(emqx_ds:db(), shard_id(), emqx_ds_storage_layer:gen_id()) ->
    no_return().
do_drop_generation_v3(_DB, _ShardId, _GenId) ->
    error(obsolete_api).

-spec do_get_delete_streams_v4(
    emqx_ds:db(), emqx_ds_replication_layer:shard_id(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    [emqx_ds_storage_layer:delete_stream()].
do_get_delete_streams_v4(DB, Shard, TopicFilter, StartTime) ->
    emqx_ds_storage_layer:get_delete_streams({DB, Shard}, TopicFilter, StartTime).

-spec do_poll_v1(
    node(),
    emqx_ds:db(),
    shard_id(),
    [{emqx_ds_beamformer:return_addr(_), emqx_ds_storage_layer:iterator()}],
    emqx_ds:poll_opts()
) ->
    ok.
do_poll_v1(SourceNode, DB, Shard, Iterators, PollOpts) ->
    ShardId = {DB, Shard},
    ?tp(ds_raft_do_poll, #{shard => ShardId, iterators => Iterators}),
    ?IF_SHARD_READY(
        ShardId,
        lists:foreach(
            fun({RAddr, It}) ->
                emqx_ds_beamformer:poll(SourceNode, RAddr, ShardId, It, PollOpts)
            end,
            Iterators
        )
    ).

%%================================================================================
%% Internal functions
%%================================================================================

list_nodes() ->
    mria:running_nodes().

%% TODO
%% Too large for normal operation, need better backpressure mechanism.
-define(RA_TIMEOUT, 60 * 1000).

%% How often to release Raft logs?
%% Each time we written approximately this number of bytes.
%% Close to the RocksDB's default of 64 MiB.
-define(RA_RELEASE_LOG_APPROX_SIZE, 50_000_000).
%% ...Or at least each N log entries.
-define(RA_RELEASE_LOG_MIN_FREQ, 64_000).

-ifdef(TEST).
-undef(RA_RELEASE_LOG_APPROX_SIZE).
-undef(RA_RELEASE_LOG_MIN_FREQ).
-define(RA_RELEASE_LOG_APPROX_SIZE, 50_000).
-define(RA_RELEASE_LOG_MIN_FREQ, 1_000).
-endif.

-define(SAFE_ERPC(EXPR),
    try
        EXPR
    catch
        error:RPCError__ = {erpc, _} ->
            {error, recoverable, RPCError__};
        %% Note: remote node never _throws_ unrecoverable errors, so
        %% we can assume that all exceptions are transient.
        EC__:RPCError__:Stack__ ->
            {error, recoverable, #{EC__ => RPCError__, stacktrace => Stack__}}
    end
).

-define(SHARD_RPC(DB, SHARD, NODE, BODY),
    case
        emqx_ds_replication_layer_shard:servers(
            DB, SHARD, application:get_env(emqx_ds_builtin_raft, reads, leader_preferred)
        )
    of
        [{_, NODE} | _] ->
            begin
                BODY
            end;
        [] ->
            {error, recoverable, replica_offline}
    end
).

-spec ra_store_batch(emqx_ds:db(), emqx_ds_replication_layer:shard_id(), emqx_ds:batch()) ->
    ok | {timeout, _} | emqx_ds:error(_).
ra_store_batch(DB, Shard, Batch) ->
    case Batch of
        #dsbatch{operations = Operations, preconditions = Preconditions} ->
            Command = #{
                ?tag => ?BATCH,
                ?batch_operations => Operations,
                ?batch_preconditions => Preconditions
            };
        Operations ->
            Command = #{
                ?tag => ?BATCH,
                ?batch_operations => Operations
            }
    end,
    Servers = emqx_ds_replication_layer_shard:servers(DB, Shard, leader_preferred),
    case emqx_ds_replication_layer_shard:process_command(Servers, Command, ?RA_TIMEOUT) of
        {ok, Result, _Leader} ->
            Result;
        {timeout, _} = Timeout ->
            Timeout;
        {error, Reason = servers_unreachable} ->
            {error, recoverable, Reason}
    end.

ra_add_generation(DB, Shard, Since) ->
    Command = #{
        ?tag => add_generation,
        ?since => Since
    },
    ra_command(DB, Shard, Command, 10).

ra_update_config(DB, Shard, Opts, Since) ->
    Command = #{
        ?tag => update_config,
        ?config => Opts,
        ?since => Since
    },
    ra_command(DB, Shard, Command, 10).

ra_drop_generation(DB, Shard, GenId) ->
    Command = #{?tag => drop_generation, ?generation => GenId},
    ra_command(DB, Shard, Command, 10).

ra_command(DB, Shard, Command, Retries) ->
    Servers = emqx_ds_replication_layer_shard:servers(DB, Shard, leader_preferred),
    case ra:process_command(Servers, Command, ?RA_TIMEOUT) of
        {ok, Result, _Leader} ->
            Result;
        _Error when Retries > 0 ->
            timer:sleep(?RA_TIMEOUT),
            ra_command(DB, Shard, Command, Retries - 1);
        Error ->
            error(Error, [DB, Shard])
    end.

ra_get_streams(DB, Shard, TopicFilter, Time) ->
    TimestampUs = timestamp_to_timeus(Time),
    ?SHARD_RPC(
        DB,
        Shard,
        Node,
        ?SAFE_ERPC(emqx_ds_proto_v4:get_streams(Node, DB, Shard, TopicFilter, TimestampUs))
    ).

ra_get_delete_streams(DB, Shard, TopicFilter, Time) ->
    ?SHARD_RPC(
        DB,
        Shard,
        Node,
        ?SAFE_ERPC(emqx_ds_proto_v4:get_delete_streams(Node, DB, Shard, TopicFilter, Time))
    ).

ra_make_iterator(DB, Shard, Stream, TopicFilter, StartTime) ->
    TimeUs = timestamp_to_timeus(StartTime),
    ?SHARD_RPC(
        DB,
        Shard,
        Node,
        ?SAFE_ERPC(emqx_ds_proto_v4:make_iterator(Node, DB, Shard, Stream, TopicFilter, TimeUs))
    ).

ra_make_delete_iterator(DB, Shard, Stream, TopicFilter, StartTime) ->
    TimeUs = timestamp_to_timeus(StartTime),
    ?SHARD_RPC(
        DB,
        Shard,
        Node,
        ?SAFE_ERPC(
            emqx_ds_proto_v4:make_delete_iterator(Node, DB, Shard, Stream, TopicFilter, TimeUs)
        )
    ).

%% ra_update_iterator(DB, Shard, Iter, DSKey) ->
%%     ?SHARD_RPC(
%%         DB,
%%         Shard,
%%         Node,
%%         ?SAFE_ERPC(emqx_ds_proto_v4:update_iterator(Node, DB, Shard, Iter, DSKey))
%%     ).

ra_next(DB, Shard, Iter, BatchSize) ->
    ?SHARD_RPC(
        DB,
        Shard,
        Node,
        case emqx_ds_proto_v4:next(Node, DB, Shard, Iter, BatchSize) of
            Err = {badrpc, _} ->
                {error, recoverable, Err};
            Ret ->
                Ret
        end
    ).

ra_poll(DB, Shard, Iterators, PollOpts) ->
    ?SHARD_RPC(
        DB,
        Shard,
        DestNode,
        ?SAFE_ERPC(emqx_ds_proto_v5:poll(DestNode, node(), DB, Shard, Iterators, PollOpts))
    ).

ra_delete_next(DB, Shard, Iter, Selector, BatchSize) ->
    ?SHARD_RPC(
        DB,
        Shard,
        Node,
        ?SAFE_ERPC(emqx_ds_proto_v4:delete_next(Node, DB, Shard, Iter, Selector, BatchSize))
    ).

ra_list_generations_with_lifetimes(DB, Shard) ->
    Reply = ?SHARD_RPC(
        DB,
        Shard,
        Node,
        ?SAFE_ERPC(emqx_ds_proto_v4:list_generations_with_lifetimes(Node, DB, Shard))
    ),
    case Reply of
        Gens = #{} ->
            maps:map(
                fun(_GenId, Data = #{since := Since, until := Until}) ->
                    Data#{
                        since := timeus_to_timestamp(Since),
                        until := emqx_maybe:apply(fun timeus_to_timestamp/1, Until)
                    }
                end,
                Gens
            );
        Error ->
            Error
    end.

ra_drop_shard(DB, Shard) ->
    ra:delete_cluster(emqx_ds_replication_layer_shard:shard_servers(DB, Shard), ?RA_TIMEOUT).

%% Ra Machine implementation
%%
%% This code decides how successfully replicated and committed log entries (e.g.
%% commands) are applied to the shard storage state. This state is actually comprised
%% logically of 2 parts:
%% 1. RocksDB database managed through `emqx_ds_storage_layer`.
%% 2. Machine state (`ra_state()`) that holds very minimal state needed to ensure
%%    higher-level semantics, most importantly strictly monotonic quasi-wallclock
%%    timestamp used to assign unique message timestamps to fulfill "append-only"
%%    guarantees.
%%
%% There are few subtleties in how storage state is persisted and recovered.
%% When the shard recovers from a shutdown or crash, this is what usually happens:
%% 1. Shard storage layer starts up the RocksDB database.
%% 2. Ra recovers the Raft log.
%% 3. Ra recovers the latest machine snapshot (`ra_state()`), taken at some point
%%    in time (`RaftIdx`).
%% 4. Ra applies existing Raft log entries starting from `RaftIdx`.
%%
%% While most of the time storage layer state, machine snapshot and log entries are
%% consistent with each other, there are situations when they are not. Namely:
%%  * RocksDB decides to flush memtables to disk by itself, which is unexpected but
%%    possible.
%%  * Lagging replica accepts a storage snapshot sourced from a RocksDB checkpoint,
%%    and RocksDB database is always implicitly flushed before checkpointing.
%% In both of those cases, the Raft log would contain entries that were already
%% applied from the point of view of the storage layer, and we must anticipate that.
%%
%% The process running Ra machine also keeps auxiliary ephemeral state in the process
%% dictionary, see `?pd_ra_*` macrodefs for details.

%% Index of the last yet unreleased Ra log entry.
-define(pd_ra_idx_need_release, '$emqx_ds_raft_idx_need_release').

%% Approximate number of bytes occupied by yet unreleased Ra log entries.
-define(pd_ra_bytes_need_release, '$emqx_ds_raft_bytes_need_release').

%% Cached value of the `append_only` DS DB configuration setting.
-define(pd_ra_force_monotonic, '$emqx_ds_raft_force_monotonic').

-spec init(_Args :: map()) -> ra_state().
init(#{db := DB, shard := Shard}) ->
    #{db_shard => {DB, Shard}, latest => 0}.

-spec apply(ra_machine:command_meta_data(), ra_command(), ra_state()) ->
    {ra_state(), _Reply, _Effects}.
apply(
    RaftMeta = #{index := RaftIdx},
    Command = #{
        ?tag := ?BATCH,
        ?batch_operations := OperationsIn
    },
    #{db_shard := DBShard = {DB, Shard}, latest := Latest0} = State0
) ->
    ?tp(ds_ra_apply_batch, #{db => DB, shard => Shard, batch => OperationsIn, latest => Latest0}),
    {Stats, Latest, Operations} = assign_timestamps(DB, Latest0, OperationsIn),
    Preconditions = maps:get(?batch_preconditions, Command, []),
    Admission =
        case Preconditions of
            [] ->
                %% No preconditions.
                true;
            _ ->
                %% Since preconditions are part of the Ra log, we need to be sure they
                %% are applied perfectly idempotently, even when Ra log entries are
                %% replayed on top of a more recent storage state that already had them
                %% evaluated and applied before.
                case read_storage_raidx(DBShard) of
                    {ok, SRI} when RaftIdx > SRI ->
                        emqx_ds_precondition:verify(emqx_ds_storage_layer, DBShard, Preconditions);
                    {ok, SRI} when SRI >= RaftIdx ->
                        %% This batch looks outdated relative to the storage layer state.
                        false;
                    {error, _, _} = Error ->
                        Error
                end
        end,
    %% Always advance latest timestamp nonetheless, so it won't diverge on replay.
    State = State0#{latest := Latest},
    set_ts(DBShard, Latest),
    case Admission of
        true ->
            %% Plain batch, no preconditions.
            Result = store_batch_nondurable(DBShard, Operations),
            Effects = try_release_log(Stats, RaftMeta, State);
        ok ->
            %% Preconditions succeeded, need to persist `RaftIdx` in the storage layer.
            Result = store_batch_nondurable(DBShard, Operations),
            Result == ok andalso update_storage_raidx(DBShard, RaftIdx),
            Effects = try_release_log(Stats, RaftMeta, State);
        Result = false ->
            %% There are preconditions, but the batch looks outdated. Skip the batch.
            %% This is log replay, reply with `false`, noone expects the reply anyway.
            Effects = [];
        PreconditionFailed = {precondition_failed, _} ->
            %% Preconditions failed. Skip the batch, persist `RaftIdx` in the storage layer.
            Result = {error, unrecoverable, PreconditionFailed},
            update_storage_raidx(DBShard, RaftIdx),
            Effects = [];
        Result = {error, unrecoverable, Reason} ->
            ?tp(error, "emqx_ds_replication_apply_batch_failed", #{
                db => DB,
                shard => Shard,
                reason => Reason,
                details =>
                    "Unrecoverable error storing committed batch. Replicas may diverge. "
                    "Consider rebuilding this shard replica from scratch."
            }),
            Effects = []
    end,
    Effects =/= [] andalso ?tp(ds_ra_effects, #{effects => Effects, meta => RaftMeta}),
    {State, Result, Effects};
apply(
    RaftMeta,
    #{?tag := add_generation, ?since := Since},
    #{db_shard := DBShard, latest := Latest0} = State0
) ->
    ?tp(
        info,
        ds_ra_add_generation,
        #{
            shard => DBShard,
            since => Since
        }
    ),
    {Timestamp, Latest} = ensure_monotonic_timestamp(Since, Latest0),
    Result = emqx_ds_storage_layer:add_generation(DBShard, Timestamp),
    State = State0#{latest := Latest},
    set_ts(DBShard, Latest),
    Effects = release_log(RaftMeta, State),
    Effects =/= [] andalso ?tp(ds_ra_effects, #{effects => Effects, meta => RaftMeta}),
    {State, Result, Effects};
apply(
    RaftMeta,
    #{?tag := update_config, ?since := Since, ?config := Opts},
    #{db_shard := DBShard, latest := Latest0} = State0
) ->
    ?tp(
        notice,
        ds_ra_update_config,
        #{
            shard => DBShard,
            config => Opts,
            since => Since
        }
    ),
    {Timestamp, Latest} = ensure_monotonic_timestamp(Since, Latest0),
    Result = emqx_ds_storage_layer:update_config(DBShard, Timestamp, Opts),
    State = State0#{latest := Latest},
    Effects = release_log(RaftMeta, State),
    Effects =/= [] andalso ?tp(ds_ra_effects, #{effects => Effects, meta => RaftMeta}),
    {State, Result, Effects};
apply(
    _RaftMeta,
    #{?tag := drop_generation, ?generation := GenId},
    #{db_shard := DBShard} = State
) ->
    ?tp(
        info,
        ds_ra_drop_generation,
        #{
            shard => DBShard,
            generation => GenId
        }
    ),
    Result = emqx_ds_storage_layer:drop_generation(DBShard, GenId),
    {State, Result};
apply(
    _RaftMeta,
    #{?tag := storage_event, ?payload := CustomEvent, ?now := Now},
    #{db_shard := DBShard, latest := Latest0} = State
) ->
    Latest = max(Latest0, Now),
    set_ts(DBShard, Latest),
    ?tp(
        debug,
        ds_ra_storage_event,
        #{
            shard => DBShard, payload => CustomEvent, latest => Latest
        }
    ),
    Effects = handle_custom_event(DBShard, Latest, CustomEvent),
    {State#{latest => Latest}, ok, Effects}.

assign_timestamps(DB, Latest, Messages) ->
    ForceMonotonic = force_monotonic_timestamps(DB),
    assign_timestamps(ForceMonotonic, Latest, Messages, [], 0, 0).

force_monotonic_timestamps(DB) ->
    case erlang:get(?pd_ra_force_monotonic) of
        undefined ->
            DBConfig = emqx_ds_replication_layer_meta:db_config(DB),
            Flag = maps:get(force_monotonic_timestamps, DBConfig, _Default = true),
            erlang:put(?pd_ra_force_monotonic, Flag);
        Flag ->
            ok
    end,
    Flag.

store_batch_nondurable(DBShard = {DB, Shard}, Operations) ->
    DispatchF = fun(Events) ->
        emqx_ds_beamformer:shard_event({DB, Shard}, Events)
    end,
    emqx_ds_storage_layer:store_batch(DBShard, Operations, #{durable => false}, DispatchF).

%% Latest Raft index tracking
%%
%% Latest RaIdx is kept in a global, basically a KV pair in the default column family.
%% Each update goes to the same spot in the RocksDB, but this should not be a problem:
%% writes are non-durable and issued only when preconditions are involved.

-define(DSREPL_RAFTIDX, <<"dsrepl/ri">>).

read_storage_raidx(DBShard) ->
    case emqx_ds_storage_layer:fetch_global(DBShard, ?DSREPL_RAFTIDX) of
        {ok, V} when byte_size(V) =< 8 ->
            {ok, binary:decode_unsigned(V)};
        not_found ->
            {ok, 0};
        Error ->
            Error
    end.

update_storage_raidx(DBShard, RaftIdx) ->
    KV = #{?DSREPL_RAFTIDX => binary:encode_unsigned(RaftIdx)},
    ok = emqx_ds_storage_layer:store_global(DBShard, KV, #{durable => false}).

%% Log truncation

try_release_log({_N, BatchSize}, RaftMeta = #{index := CurrentIdx}, State) ->
    %% NOTE
    %% Because cursor release means storage flush (see
    %% `emqx_ds_replication_snapshot:write/3`), we should do that not too often
    %% (so the storage is happy with L0 SST sizes) and not too rarely (so we don't
    %% accumulate huge Raft logs).
    case inc_bytes_need_release(BatchSize) of
        AccSize when AccSize > ?RA_RELEASE_LOG_APPROX_SIZE ->
            release_log(RaftMeta, State);
        _NotYet ->
            case get_log_need_release(RaftMeta) of
                undefined ->
                    [];
                PrevIdx when CurrentIdx - PrevIdx > ?RA_RELEASE_LOG_MIN_FREQ ->
                    %% Release everything up to the last log entry, but only if there were
                    %% more than %% `?RA_RELEASE_LOG_MIN_FREQ` new entries since the last
                    %% release.
                    release_log(RaftMeta, State);
                _ ->
                    []
            end
    end.

release_log(RaftMeta = #{index := CurrentIdx}, State) ->
    %% NOTE
    %% Release everything up to the last log entry. This is important: any log entries
    %% following `CurrentIdx` should not contribute to `State` (that will be recovered
    %% from a snapshot).
    update_log_need_release(RaftMeta),
    reset_bytes_need_release(),
    {release_cursor, CurrentIdx, State}.

get_log_need_release(RaftMeta) ->
    case erlang:get(?pd_ra_idx_need_release) of
        undefined ->
            update_log_need_release(RaftMeta),
            undefined;
        LastIdx ->
            LastIdx
    end.

update_log_need_release(#{index := CurrentIdx}) ->
    erlang:put(?pd_ra_idx_need_release, CurrentIdx).

get_bytes_need_release() ->
    emqx_maybe:define(erlang:get(?pd_ra_bytes_need_release), 0).

inc_bytes_need_release(Size) ->
    Acc = get_bytes_need_release() + Size,
    erlang:put(?pd_ra_bytes_need_release, Acc),
    Acc.

reset_bytes_need_release() ->
    erlang:put(?pd_ra_bytes_need_release, 0).

-spec tick(integer(), ra_state()) -> ra_machine:effects().
tick(TimeMs, #{db_shard := DBShard = {DB, Shard}, latest := Latest}) ->
    %% Leader = emqx_ds_replication_layer_shard:lookup_leader(DB, Shard),
    {Timestamp, _} = ensure_monotonic_timestamp(timestamp_to_timeus(TimeMs), Latest),
    ?tp(emqx_ds_replication_layer_tick, #{db => DB, shard => Shard, timestamp => Timestamp}),
    handle_custom_event(DBShard, Timestamp, ra_tick).

assign_timestamps(true, Latest0, [Message0 = #message{} | Rest], Acc, N, Sz) ->
    case emqx_message:timestamp(Message0, microsecond) of
        TimestampUs when TimestampUs > Latest0 ->
            Latest = TimestampUs,
            Message = assign_timestamp(TimestampUs, Message0);
        _Earlier ->
            Latest = Latest0 + 1,
            Message = assign_timestamp(Latest, Message0)
    end,
    MSize = approx_message_size(Message0),
    assign_timestamps(true, Latest, Rest, [Message | Acc], N + 1, Sz + MSize);
assign_timestamps(false, Latest0, [Message0 = #message{} | Rest], Acc, N, Sz) ->
    TimestampUs = emqx_message:timestamp(Message0),
    Latest = max(Latest0, TimestampUs),
    Message = assign_timestamp(TimestampUs, Message0),
    MSize = approx_message_size(Message0),
    assign_timestamps(false, Latest, Rest, [Message | Acc], N + 1, Sz + MSize);
assign_timestamps(ForceMonotonic, Latest, [Operation | Rest], Acc, N, Sz) ->
    assign_timestamps(ForceMonotonic, Latest, Rest, [Operation | Acc], N + 1, Sz);
assign_timestamps(_ForceMonotonic, Latest, [], Acc, N, Size) ->
    {{N, Size}, Latest, lists:reverse(Acc)}.

assign_timestamp(TimestampUs, Message) ->
    {TimestampUs, Message}.

ensure_monotonic_timestamp(TimestampUs, Latest) when TimestampUs > Latest ->
    {TimestampUs, TimestampUs + 1};
ensure_monotonic_timestamp(_TimestampUs, Latest) ->
    {Latest, Latest + 1}.

timestamp_to_timeus(TimestampMs) ->
    TimestampMs * 1000.

timeus_to_timestamp(TimestampUs) ->
    TimestampUs div 1000.

snapshot_module() ->
    emqx_ds_replication_snapshot.

unpack_iterator(Shard, #{?tag := ?IT, ?enc := Iterator}) ->
    emqx_ds_storage_layer:unpack_iterator(Shard, Iterator).

scan_stream(ShardId = {DB, Shard}, Stream, TopicFilter, StartMsg, BatchSize) ->
    ?IF_SHARD_READY(
        ShardId,
        begin
            Now = current_timestamp(DB, Shard),
            emqx_ds_storage_layer:scan_stream(
                ShardId, Stream, TopicFilter, Now, StartMsg, BatchSize
            )
        end
    ).

-spec update_iterator(emqx_ds_storage_layer:shard_id(), iterator(), emqx_ds:message_key()) ->
    emqx_ds:make_iterator_result(iterator()).
update_iterator(ShardId, OldIter, DSKey) ->
    #{?tag := ?IT, ?enc := Inner0} = OldIter,
    case emqx_ds_storage_layer:update_iterator(ShardId, Inner0, DSKey) of
        {ok, Inner} ->
            {ok, OldIter#{?enc => Inner}};
        Err = {error, _, _} ->
            Err
    end.

handle_custom_event(_DBShard, _Latest, ra_tick) ->
    [];
handle_custom_event(DBShard, Latest, Event) ->
    try
        Events = emqx_ds_storage_layer:handle_event(DBShard, Latest, Event),
        [{append, #{?tag => storage_event, ?payload => I, ?now => Latest}} || I <- Events]
    catch
        EC:Err:Stacktrace ->
            ?tp(error, ds_storage_custom_event_fail, #{
                EC => Err, stacktrace => Stacktrace, event => Event
            }),
            []
    end.

set_ts({DB, Shard}, TS) ->
    emqx_ds_builtin_raft_sup:set_gvar(DB, ?gv_timestamp(Shard), TS).

%%

-spec state_enter(ra_server:ra_state() | eol, ra_state()) -> ra_machine:effects().
state_enter(MemberState, #{db_shard := {DB, Shard}, latest := Latest}) ->
    ?tp(
        ds_ra_state_enter,
        #{db => DB, shard => Shard, latest => Latest, state => MemberState}
    ),
    [].

%%

approx_message_size(#message{from = ClientID, topic = Topic, payload = Payload}) ->
    %% NOTE: Overhead here is basically few empty maps + 8-byte message id.
    %% TODO: Probably need to ask the storage layer about the footprint.
    MinOverhead = 40,
    MinOverhead + clientid_size(ClientID) + byte_size(Topic) + byte_size(Payload).

clientid_size(ClientID) when is_binary(ClientID) ->
    byte_size(ClientID);
clientid_size(ClientID) ->
    erlang:external_size(ClientID).

-ifdef(TEST).

test_db_config(_Config) ->
    #{
        backend => builtin_raft,
        storage => {emqx_ds_storage_reference, #{}},
        n_shards => 1,
        n_sites => 1,
        replication_factor => 3,
        replication_options => #{}
    }.

test_applications(_Config) ->
    [
        emqx_durable_storage,
        emqx_ds_backends
    ].

-endif.
