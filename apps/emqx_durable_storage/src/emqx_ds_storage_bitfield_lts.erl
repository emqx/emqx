%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc A storage layout based on learned topic structure and using
%% bitfield mapping for the varying topic layers.
-module(emqx_ds_storage_bitfield_lts).

-behaviour(emqx_ds_storage_layer).

%% API:
-export([]).

%% behavior callbacks:
-export([
    create/6,
    open/5,
    drop/5,
    prepare_batch/4,
    commit_batch/4,
    get_streams/4,
    get_delete_streams/4,
    make_iterator/5,
    make_delete_iterator/5,
    update_iterator/4,
    next/6,
    delete_next/7,
    lookup_message/3,

    handle_event/4,

    unpack_iterator/3,
    scan_stream/8,
    message_matcher/3,
    batch_events/3
]).

%% internal exports:
-export([format_key/2]).

-export_type([options/0]).

-include("emqx_ds.hrl").
-include("emqx_ds_metrics.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%% # "Record" integer keys.  We use maps with integer keys to avoid persisting and sending
%% records over the wire.

%% tags:
-define(STREAM, 1).
-define(IT, 2).
-define(DELETE_IT, 3).

%% keys:
-define(tag, 1).
-define(topic_filter, 2).
-define(start_time, 3).
-define(storage_key, 4).
-define(last_seen_key, 5).
-define(cooked_msg_ops, 6).
-define(cooked_lts_ops, 7).
-define(cooked_ts, 8).

%% atoms:
-define(delete, 100).

-type options() ::
    #{
        bits_per_wildcard_level => pos_integer(),
        topic_index_bytes => pos_integer(),
        epoch_bits => non_neg_integer(),
        %% Which epochs are considered readable, i.e. visible in `next/6`?
        %% Default: `complete`, unless it's a single neverending epoch, it's always readable.
        epoch_readable => complete | any,
        lts_threshold_spec => emqx_ds_lts:threshold_spec()
    }.

%% Permanent state:
-type schema() ::
    #{
        bits_per_wildcard_level := pos_integer(),
        topic_index_bytes := pos_integer(),
        ts_bits := non_neg_integer(),
        ts_offset_bits := non_neg_integer(),
        epoch_readable => complete | any,
        lts_threshold_spec => emqx_ds_lts:threshold_spec()
    }.

%% Runtime state:
-record(s, {
    db :: rocksdb:db_handle(),
    data :: rocksdb:cf_handle(),
    trie :: emqx_ds_lts:trie(),
    trie_cf :: rocksdb:cf_handle(),
    keymappers :: array:array(emqx_ds_bitmask_keymapper:keymapper()),
    ts_bits :: non_neg_integer(),
    ts_offset :: non_neg_integer(),
    epoch_readable :: complete | any,
    threshold_fun :: emqx_ds_lts:threshold_fun(),
    gvars :: ets:table()
}).

-define(lts_persist_ops, emqx_ds_storage_bitfield_lts_ops).

-type s() :: #s{}.

-type stream() :: emqx_ds_lts:msg_storage_key().

-type delete_stream() :: emqx_ds_lts:msg_storage_key().

-type cooked_batch() ::
    #{
        ?cooked_msg_ops := [{binary(), binary() | ?delete}],
        ?cooked_lts_ops := [{binary(), binary()}],
        ?cooked_ts := integer()
    }.

-type iterator() ::
    #{
        ?tag := ?IT,
        ?topic_filter := emqx_ds:topic_filter(),
        ?start_time := emqx_ds:time(),
        ?storage_key := emqx_ds_lts:msg_storage_key(),
        ?last_seen_key := binary()
    }.

-type delete_iterator() ::
    #{
        ?tag := ?DELETE_IT,
        ?topic_filter := emqx_ds:topic_filter(),
        ?start_time := emqx_ds:time(),
        ?storage_key := emqx_ds_lts:msg_storage_key(),
        ?last_seen_key := binary()
    }.

%% Limit on the number of wildcard levels in the learned topic trie:
-define(WILDCARD_LIMIT, 10).

%% Default LTS thresholds: 0th level = 100 entries max, other levels = 20 entries.
-define(DEFAULT_LTS_THRESHOLD, {simple, {100, 20}}).

%% Persistent (durable) term representing `#message{}' record. Must
%% not change.
-type value_v1() ::
    {
        _Id :: binary(),
        _Qos :: 0..2,
        _From :: atom() | binary(),
        _Flags :: emqx_types:flags(),
        _Headsers :: emqx_types:headers(),
        _Topic :: emqx_types:topic(),
        _Payload :: emqx_types:payload(),
        _Timestamp :: integer(),
        _Extra :: term()
    }.

-include("emqx_ds_bitmask.hrl").

-define(DIM_TOPIC, 1).
-define(DIM_TS, 2).

-define(DS_LTS_COUNTERS, [
    ?DS_BITFIELD_LTS_SEEK_COUNTER, ?DS_BITFIELD_LTS_NEXT_COUNTER, ?DS_BITFIELD_LTS_COLLISION_COUNTER
]).

%% GVar used for idle detection:
-define(IDLE_DETECT, idle_detect).
-define(EPOCH(S, TS), (TS bsr S#s.ts_offset)).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% API functions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

-spec create(
    emqx_ds_storage_layer:shard_id(),
    rocksdb:db_handle(),
    emqx_ds_storage_layer:gen_id(),
    options(),
    _PrevGeneration :: s() | undefined,
    emqx_ds:db_opts()
) ->
    {schema(), emqx_ds_storage_layer:cf_refs()}.
create(_ShardId, DBHandle, GenId, Options, SPrev, _DBOpts) ->
    %% Get options:
    BitsPerTopicLevel = maps:get(bits_per_wildcard_level, Options, 64),
    TopicIndexBytes = maps:get(topic_index_bytes, Options, 4),
    TSBits = 64,
    %% 20 bits -> 1048576 us -> ~1 sec
    TSOffsetBits = maps:get(epoch_bits, Options, 20),
    case TSBits - TSOffsetBits of
        %% NOTE: There's a single, always incomplete epoch, consider it readable.
        0 -> EpochReadableDefault = any;
        _ -> EpochReadableDefault = complete
    end,
    EpochReadable = maps:get(epoch_readable, Options, EpochReadableDefault),
    ThresholdSpec = maps:get(lts_threshold_spec, Options, ?DEFAULT_LTS_THRESHOLD),
    %% Create column families:
    DataCFName = data_cf(GenId),
    TrieCFName = trie_cf(GenId),
    {ok, DataCFHandle} = rocksdb:create_column_family(DBHandle, DataCFName, []),
    {ok, TrieCFHandle} = rocksdb:create_column_family(DBHandle, TrieCFName, []),
    case SPrev of
        #s{trie = TriePrev} ->
            ok = copy_previous_trie(DBHandle, TrieCFHandle, TriePrev),
            ?tp(layout_inherited_lts_trie, #{}),
            ok;
        undefined ->
            ok
    end,
    %% Create schema:
    Schema = #{
        bits_per_wildcard_level => BitsPerTopicLevel,
        topic_index_bytes => TopicIndexBytes,
        ts_bits => TSBits,
        ts_offset_bits => TSOffsetBits,
        epoch_readable => EpochReadable,
        lts_threshold_spec => ThresholdSpec
    },
    {Schema, [{DataCFName, DataCFHandle}, {TrieCFName, TrieCFHandle}]}.

-spec open(
    emqx_ds_storage_layer:shard_id(),
    rocksdb:db_handle(),
    emqx_ds_storage_layer:gen_id(),
    emqx_ds_storage_layer:cf_refs(),
    schema()
) ->
    s().
open(_Shard, DBHandle, GenId, CFRefs, Schema) ->
    #{
        bits_per_wildcard_level := BitsPerTopicLevel,
        topic_index_bytes := TopicIndexBytes,
        ts_bits := TSBits,
        ts_offset_bits := TSOffsetBits
    } = Schema,
    {_, DataCF} = lists:keyfind(data_cf(GenId), 1, CFRefs),
    {_, TrieCF} = lists:keyfind(trie_cf(GenId), 1, CFRefs),
    Trie = restore_trie(TopicIndexBytes, DBHandle, TrieCF),
    %% If user's topics have more than learned 10 wildcard levels
    %% (more than 2, really), then it's total carnage; learned topic
    %% structure won't help.
    MaxWildcardLevels = ?WILDCARD_LIMIT,
    KeymapperCache = array:from_list(
        [
            make_keymapper(TopicIndexBytes, BitsPerTopicLevel, TSBits, TSOffsetBits, N)
         || N <- lists:seq(0, MaxWildcardLevels)
        ]
    ),
    ThresholdSpec = maps:get(lts_threshold_spec, Schema, ?DEFAULT_LTS_THRESHOLD),
    #s{
        db = DBHandle,
        data = DataCF,
        trie = Trie,
        trie_cf = TrieCF,
        keymappers = KeymapperCache,
        ts_offset = TSOffsetBits,
        ts_bits = TSBits,
        epoch_readable = maps:get(epoch_readable, Schema, complete),
        threshold_fun = emqx_ds_lts:threshold_fun(ThresholdSpec),
        gvars = ets:new(?MODULE, [public, set, {read_concurrency, true}])
    }.

-spec drop(
    emqx_ds_storage_layer:shard_id(),
    rocksdb:db_handle(),
    emqx_ds_storage_layer:gen_id(),
    emqx_ds_storage_layer:cf_refs(),
    s()
) ->
    ok.
drop(_Shard, DBHandle, GenId, CFRefs, #s{trie = Trie, gvars = GVars}) ->
    emqx_ds_lts:destroy(Trie),
    catch ets:delete(GVars),
    {_, DataCF} = lists:keyfind(data_cf(GenId), 1, CFRefs),
    {_, TrieCF} = lists:keyfind(trie_cf(GenId), 1, CFRefs),
    ok = rocksdb:drop_column_family(DBHandle, DataCF),
    ok = rocksdb:drop_column_family(DBHandle, TrieCF),
    ok.

-spec prepare_batch(
    emqx_ds_storage_layer:shard_id(),
    s(),
    emqx_ds_storage_layer:batch(),
    emqx_ds_storage_layer:batch_store_opts()
) ->
    {ok, cooked_batch()}.
prepare_batch(_ShardId, S, Batch, _Options) ->
    _ = erase(?lts_persist_ops),
    {Operations, MaxTs} =
        lists:mapfoldl(
            fun
                ({Timestamp, Msg = #message{topic = Topic}}, Acc) ->
                    {Key, _} = make_key(S, Timestamp, Topic),
                    Op = {Key, message_to_value_v1(Msg)},
                    {Op, max(Acc, Timestamp)};
                ({delete, #message_matcher{topic = Topic, timestamp = Timestamp}}, Acc) ->
                    {Key, _} = make_key(S, Timestamp, Topic),
                    {_Op = {Key, ?delete}, Acc}
            end,
            0,
            Batch
        ),
    {ok, #{
        ?cooked_msg_ops => Operations,
        ?cooked_lts_ops => pop_lts_persist_ops(),
        ?cooked_ts => MaxTs
    }}.

-spec commit_batch(
    emqx_ds_storage_layer:shard_id(),
    s(),
    cooked_batch(),
    emqx_ds_storage_layer:batch_store_opts()
) -> ok | emqx_ds:error(_).
commit_batch(
    _ShardId,
    _Data,
    #{?cooked_msg_ops := [], ?cooked_lts_ops := LTS},
    _Options
) ->
    %% Assert:
    [] = LTS,
    ok;
commit_batch(
    _ShardId,
    #s{db = DB, data = DataCF, trie = Trie, trie_cf = TrieCF, gvars = Gvars},
    #{?cooked_lts_ops := LtsOps, ?cooked_msg_ops := Operations, ?cooked_ts := MaxTs},
    Options
) ->
    {ok, Batch} = rocksdb:batch(),
    %% Commit LTS trie to the storage:
    lists:foreach(
        fun({Key, Val}) ->
            ok = rocksdb:batch_put(Batch, TrieCF, term_to_binary(Key), term_to_binary(Val))
        end,
        LtsOps
    ),
    %% Apply LTS ops to the memory cache:
    _ = emqx_ds_lts:trie_update(Trie, LtsOps),
    %% Commit payloads:
    lists:foreach(
        fun
            ({Key, Val}) when is_tuple(Val) ->
                ok = rocksdb:batch_put(Batch, DataCF, Key, term_to_binary(Val));
            ({Key, ?delete}) ->
                ok = rocksdb:batch_delete(Batch, DataCF, Key)
        end,
        Operations
    ),
    Result = rocksdb:write_batch(DB, Batch, write_batch_opts(Options)),
    rocksdb:release_batch(Batch),
    ets:insert(Gvars, {?IDLE_DETECT, false, MaxTs}),
    %% NOTE
    %% Strictly speaking, `{error, incomplete}` is a valid result but should be impossible to
    %% observe until there's `{no_slowdown, true}` in write options.
    case Result of
        ok ->
            ok;
        {error, {error, Reason}} ->
            {error, unrecoverable, {rocksdb, Reason}}
    end.

-spec get_streams(
    emqx_ds_storage_layer:shard_id(),
    s(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) -> [stream()].
get_streams(_Shard, #s{trie = Trie}, TopicFilter, _StartTime) ->
    emqx_ds_lts:match_topics(Trie, TopicFilter).

-spec get_delete_streams(
    emqx_ds_storage_layer:shard_id(),
    s(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) -> [delete_stream()].
get_delete_streams(Shard, State, TopicFilter, StartTime) ->
    get_streams(Shard, State, TopicFilter, StartTime).

-spec make_iterator(
    emqx_ds_storage_layer:shard_id(),
    s(),
    stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) -> {ok, iterator()}.
make_iterator(
    _Shard, _Data, StorageKey, TopicFilter, StartTime
) ->
    %% Note: it's a good idea to keep the iterator structure lean,
    %% since it can be stored on a remote node that could update its
    %% code independently from us.
    {ok, #{
        ?tag => ?IT,
        ?topic_filter => TopicFilter,
        ?start_time => StartTime,
        ?storage_key => StorageKey,
        ?last_seen_key => <<>>
    }}.

-spec make_delete_iterator(
    emqx_ds_storage_layer:shard_id(),
    s(),
    delete_stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) -> {ok, delete_iterator()}.
make_delete_iterator(
    _Shard, _Data, StorageKey, TopicFilter, StartTime
) ->
    %% Note: it's a good idea to keep the iterator structure lean,
    %% since it can be stored on a remote node that could update its
    %% code independently from us.
    {ok, #{
        ?tag => ?DELETE_IT,
        ?topic_filter => TopicFilter,
        ?start_time => StartTime,
        ?storage_key => StorageKey,
        ?last_seen_key => <<>>
    }}.

-spec update_iterator(
    emqx_ds_storage_layer:shard_id(),
    s(),
    iterator(),
    emqx_ds:message_key()
) -> {ok, iterator()}.
update_iterator(
    _Shard,
    _Data,
    #{?tag := ?IT} = OldIter,
    DSKey
) ->
    {ok, OldIter#{?last_seen_key => DSKey}}.

next(
    Shard,
    Schema = #s{ts_offset = TSOffset, ts_bits = TSBits, epoch_readable = EpochReadable},
    It = #{?storage_key := Stream},
    BatchSize,
    Now,
    IsCurrent
) ->
    init_counters(),
    %% Compute safe cutoff time. It's the point in time where the last
    %% complete epoch ends, so we need to know the current time to
    %% compute it. This is needed because new keys can be added before
    %% the iterator.
    %%
    %% This is needed to avoid situations when the iterator advances
    %% to position k1, and then a new message with k2, such that k2 <
    %% k1 is inserted. k2 would be missed.
    HasCutoff =
        case Stream of
            {_StaticKey, []} ->
                %% Iterators scanning streams without varying topic
                %% levels can operate on incomplete epochs, since new
                %% matching keys for the single topic are added in
                %% lexicographic order.
                %%
                %% Note: this DOES NOT apply to non-wildcard topic
                %% filters operating on streams with varying parts:
                %% iterator can jump to the next topic and then it
                %% won't backtrack.
                false;
            _ when EpochReadable == any ->
                %% Incomplete epochs are explicitly marked as readable:
                false;
            _ ->
                %% New batches are only added to the current
                %% generation. We can ignore cutoff time for old
                %% generations:
                IsCurrent
        end,
    SafeCutoffTime =
        case HasCutoff of
            true ->
                ?EPOCH(Schema, Now) bsl TSOffset;
            false ->
                1 bsl TSBits - 1
        end,
    try
        case next_until(Schema, It, SafeCutoffTime, BatchSize) of
            {ok, _, []} when not IsCurrent ->
                {ok, end_of_stream};
            Result ->
                Result
        end
    after
        report_counters(Shard)
    end.

next_until(_Schema, It = #{?tag := ?IT, ?start_time := StartTime}, SafeCutoffTime, _BatchSize) when
    StartTime >= SafeCutoffTime
->
    %% We're in the middle of the current epoch, so we can't yet iterate over it.
    %% It would be unsafe otherwise: messages can be stored in the current epoch
    %% concurrently with iterating over it. They can end up earlier (in the iteration
    %% order) due to the nature of keymapping, potentially causing us to miss them.
    {ok, It, []};
next_until(#s{db = DB, data = CF, keymappers = Keymappers}, It, SafeCutoffTime, BatchSize) ->
    #{
        ?tag := ?IT,
        ?start_time := StartTime,
        ?storage_key := {TopicIndex, Varying}
    } = It,
    #{
        it_handle := ITHandle,
        keymapper := Keymapper,
        filter := Filter
    } = prepare_loop_context(DB, CF, TopicIndex, StartTime, SafeCutoffTime, Varying, Keymappers),
    try
        next_loop(ITHandle, Keymapper, Filter, SafeCutoffTime, It, [], BatchSize)
    after
        rocksdb:iterator_close(ITHandle)
    end.

delete_next(Shard, Schema = #s{ts_offset = TSOffset}, It, Selector, BatchSize, Now, IsCurrent) ->
    %% Compute safe cutoff time.
    %% It's the point in time where the last complete epoch ends, so we need to know
    %% the current time to compute it.
    init_counters(),
    SafeCutoffTime = ?EPOCH(Schema, Now) bsl TSOffset,
    try
        case delete_next_until(Schema, It, SafeCutoffTime, Selector, BatchSize) of
            {ok, _It, 0, 0} when not IsCurrent ->
                {ok, end_of_stream};
            Result ->
                Result
        end
    after
        report_counters(Shard)
    end.

delete_next_until(
    _Schema,
    It = #{?tag := ?DELETE_IT, ?start_time := StartTime},
    SafeCutoffTime,
    _Selector,
    _BatchSize
) when
    StartTime >= SafeCutoffTime
->
    %% We're in the middle of the current epoch, so we can't yet iterate over it.
    %% It would be unsafe otherwise: messages can be stored in the current epoch
    %% concurrently with iterating over it. They can end up earlier (in the iteration
    %% order) due to the nature of keymapping, potentially causing us to miss them.
    {ok, It, 0, 0};
delete_next_until(
    #s{db = DB, data = CF, keymappers = Keymappers}, It, SafeCutoffTime, Selector, BatchSize
) ->
    #{
        ?tag := ?DELETE_IT,
        ?start_time := StartTime,
        ?storage_key := {TopicIndex, Varying}
    } = It,
    #{it_handle := ITHandle} =
        LoopContext0 = prepare_loop_context(
            DB, CF, TopicIndex, StartTime, SafeCutoffTime, Varying, Keymappers
        ),
    try
        LoopContext = LoopContext0#{
            db => DB,
            cf => CF,
            safe_cutoff_time => SafeCutoffTime,
            storage_iter => It,
            deleted => 0,
            iterated_over => 0,
            selector => Selector,
            remaining => BatchSize
        },
        delete_next_loop(LoopContext)
    after
        rocksdb:iterator_close(ITHandle)
    end.

-spec lookup_message(emqx_ds_storage_layer:shard_id(), s(), emqx_ds_precondition:matcher()) ->
    emqx_types:message() | not_found | emqx_ds:error(_).
lookup_message(
    _ShardId,
    S = #s{db = DB, data = CF},
    #message_matcher{topic = Topic, timestamp = Timestamp}
) ->
    Key = lookup_key(S, Timestamp, Topic),
    case is_binary(Key) andalso rocksdb:get(DB, CF, Key, _ReadOpts = []) of
        {ok, Blob} ->
            deserialize(Blob);
        not_found ->
            not_found;
        false ->
            not_found;
        Error ->
            {error, unrecoverable, {rocksdb, Error}}
    end.

handle_event(_ShardId, #s{epoch_readable = any}, _Time, tick) ->
    %% No need for idle tracking.
    [];
handle_event(_ShardId, State = #s{gvars = Gvars}, Time, tick) ->
    %% If the last message was published more than one epoch ago, and
    %% the shard remains idle, we need to advance safety cutoff
    %% interval to make sure the last epoch becomes visible to the
    %% readers.
    %%
    %% We do so by emitting a dummy event that will be persisted by
    %% the replication layer. Processing it will advance the
    %% replication layer's clock.
    %%
    %% This operation is latched to avoid publishing events on every
    %% tick.
    case ets:lookup(Gvars, ?IDLE_DETECT) of
        [{?IDLE_DETECT, Latch, LastWrittenTs}] ->
            ok;
        [] ->
            Latch = false,
            LastWrittenTs = 0
    end,
    case Latch of
        false when ?EPOCH(State, Time) > ?EPOCH(State, LastWrittenTs) + 1 ->
            %% Note: + 1 above delays the event by one epoch to add a
            %% safety margin.
            ets:insert(Gvars, {?IDLE_DETECT, true, LastWrittenTs}),
            [dummy_event];
        _ ->
            []
    end;
handle_event(_ShardId, _Data, _Time, _Event) ->
    %% `dummy_event' goes here and does nothing. But it forces update
    %% of `Time' in the replication layer.
    [].

unpack_iterator(_Shard, _S, #{
    ?tag := ?IT,
    ?storage_key := Stream,
    ?topic_filter := TF,
    ?last_seen_key := <<>>,
    ?start_time := StartTime
}) ->
    {Stream, TF, <<>>, StartTime};
unpack_iterator(_Shard, #s{keymappers = Keymappers}, #{
    ?tag := ?IT, ?storage_key := Stream, ?topic_filter := TF, ?last_seen_key := LSK
}) ->
    {_, Varying} = Stream,
    NVarying = length(Varying),
    Keymapper = array:get(NVarying, Keymappers),
    Timestamp = emqx_ds_bitmask_keymapper:bin_key_to_coord(Keymapper, LSK, ?DIM_TS),
    {Stream, TF, LSK, Timestamp}.

scan_stream(Shard, S, Stream, TopicFilter, LastSeenKey, BatchSize, TMax, IsCurrent) ->
    It = #{
        ?tag => ?IT,
        ?topic_filter => TopicFilter,
        ?start_time => 0,
        ?storage_key => Stream,
        ?last_seen_key => LastSeenKey
    },
    case next(Shard, S, It, BatchSize, TMax, IsCurrent) of
        {ok, #{?last_seen_key := LSK}, Batch} ->
            {ok, LSK, Batch};
        Other ->
            Other
    end.

message_matcher(_Shard, #s{}, #{?tag := ?IT, ?last_seen_key := LSK, ?topic_filter := TF}) ->
    fun(MsgKey, #message{topic = Topic}) ->
        MsgKey > LSK andalso emqx_topic:match(emqx_topic:tokens(Topic), TF)
    end.

batch_events(_Shard, _S, _CookedBatch) ->
    %% FIXME: here we rely on the fact that bitfield_lts layout is
    %% deprecated.
    [].

%%================================================================================
%% Internal functions
%%================================================================================

prepare_loop_context(DB, CF, TopicIndex, StartTime, SafeCutoffTime, Varying, Keymappers) ->
    %% Make filter:
    Inequations = [
        {'=', TopicIndex},
        {StartTime, '..', SafeCutoffTime - 1}
        %% Varying topic levels:
        | lists:map(
            fun
                ('+') ->
                    any;
                (TopicLevel) when is_binary(TopicLevel); TopicLevel =:= '' ->
                    {'=', hash_topic_level(TopicLevel)}
            end,
            Varying
        )
    ],
    %% Obtain a keymapper for the current number of varying levels.
    NVarying = length(Varying),
    %% Assert:
    NVarying =< ?WILDCARD_LIMIT orelse
        error({too_many_varying_topic_levels, NVarying}),
    Keymapper = array:get(NVarying, Keymappers),
    Filter =
        #filter{range_min = LowerBound, range_max = UpperBound} = emqx_ds_bitmask_keymapper:make_filter(
            Keymapper, Inequations
        ),
    {ok, ITHandle} = rocksdb:iterator(DB, CF, [
        {iterate_lower_bound, emqx_ds_bitmask_keymapper:key_to_bitstring(Keymapper, LowerBound)},
        {iterate_upper_bound, emqx_ds_bitmask_keymapper:key_to_bitstring(Keymapper, UpperBound + 1)}
    ]),
    #{
        it_handle => ITHandle,
        keymapper => Keymapper,
        filter => Filter
    }.

next_loop(_ITHandle, _KeyMapper, _Filter, _Cutoff, It, Acc, 0) ->
    {ok, It, lists:reverse(Acc)};
next_loop(ITHandle, KeyMapper, Filter, Cutoff, It0, Acc0, N0) ->
    #{?tag := ?IT, ?last_seen_key := Key0} = It0,
    case emqx_ds_bitmask_keymapper:bin_increment(Filter, Key0) of
        overflow ->
            {ok, It0, lists:reverse(Acc0)};
        Key1 ->
            %% assert
            true = Key1 > Key0,
            inc_counter(?DS_BITFIELD_LTS_SEEK_COUNTER),
            case rocksdb:iterator_move(ITHandle, {seek, Key1}) of
                {ok, Key, Val} ->
                    {N, It, Acc} = traverse_interval(
                        ITHandle, KeyMapper, Filter, Cutoff, Key, Val, It0, Acc0, N0
                    ),
                    next_loop(ITHandle, KeyMapper, Filter, Cutoff, It, Acc, N);
                {error, invalid_iterator} ->
                    {ok, It0, lists:reverse(Acc0)}
            end
    end.

traverse_interval(ITHandle, KeyMapper, Filter, Cutoff, Key, Val, It0, Acc0, N) ->
    It = It0#{?last_seen_key := Key},
    Timestamp = emqx_ds_bitmask_keymapper:bin_key_to_coord(KeyMapper, Key, ?DIM_TS),
    case
        emqx_ds_bitmask_keymapper:bin_checkmask(Filter, Key) andalso
            check_timestamp(Cutoff, It, Timestamp)
    of
        true ->
            Msg = deserialize(Val),
            case check_message(It, Msg) of
                true ->
                    Acc = [{Key, Msg} | Acc0],
                    traverse_interval(ITHandle, KeyMapper, Filter, Cutoff, It, Acc, N - 1);
                false ->
                    inc_counter(?DS_BITFIELD_LTS_COLLISION_COUNTER),
                    traverse_interval(ITHandle, KeyMapper, Filter, Cutoff, It, Acc0, N)
            end;
        overflow ->
            {0, It0, Acc0};
        false ->
            {N, It, Acc0}
    end.

traverse_interval(_ITHandle, _KeyMapper, _Filter, _Cutoff, It, Acc, 0) ->
    {0, It, Acc};
traverse_interval(ITHandle, KeyMapper, Filter, Cutoff, It, Acc, N) ->
    inc_counter(?DS_BITFIELD_LTS_NEXT_COUNTER),
    case rocksdb:iterator_move(ITHandle, next) of
        {ok, Key, Val} ->
            traverse_interval(ITHandle, KeyMapper, Filter, Cutoff, Key, Val, It, Acc, N);
        {error, invalid_iterator} ->
            {0, It, Acc}
    end.

delete_next_loop(
    #{deleted := AccDel, iterated_over := AccIter, storage_iter := It, remaining := 0}
) ->
    {ok, It, AccDel, AccIter};
delete_next_loop(LoopContext0) ->
    #{
        storage_iter := It0,
        filter := Filter,
        deleted := AccDel0,
        iterated_over := AccIter0,
        it_handle := ITHandle
    } = LoopContext0,
    inc_counter(?DS_BITFIELD_LTS_SEEK_COUNTER),
    #{?tag := ?DELETE_IT, ?last_seen_key := Key0} = It0,
    case emqx_ds_bitmask_keymapper:bin_increment(Filter, Key0) of
        overflow ->
            {ok, It0, AccDel0, AccIter0};
        Key1 ->
            %% assert
            true = Key1 > Key0,
            case rocksdb:iterator_move(ITHandle, {seek, Key1}) of
                {ok, Key, Val} ->
                    {N, It, AccDel, AccIter} =
                        delete_traverse_interval(LoopContext0#{
                            iterated_over := AccIter0 + 1,
                            current_key => Key,
                            current_val => Val
                        }),
                    delete_next_loop(LoopContext0#{
                        iterated_over := AccIter,
                        deleted := AccDel,
                        remaining := N,
                        storage_iter := It
                    });
                {error, invalid_iterator} ->
                    {ok, It0, AccDel0, AccIter0}
            end
    end.

delete_traverse_interval(LoopContext0) ->
    #{
        storage_iter := It0,
        current_key := Key,
        current_val := Val,
        keymapper := KeyMapper,
        filter := Filter,
        safe_cutoff_time := Cutoff,
        selector := Selector,
        db := DB,
        cf := CF,
        deleted := AccDel0,
        iterated_over := AccIter0,
        remaining := Remaining0
    } = LoopContext0,
    It = It0#{?last_seen_key := Key},
    Timestamp = emqx_ds_bitmask_keymapper:bin_key_to_coord(KeyMapper, Key, ?DIM_TS),
    case
        emqx_ds_bitmask_keymapper:bin_checkmask(Filter, Key) andalso
            check_timestamp(Cutoff, It, Timestamp)
    of
        true ->
            Msg = deserialize(Val),
            case check_message(It, Msg) of
                true ->
                    case Selector(Msg) of
                        true ->
                            ok = rocksdb:delete(DB, CF, Key, _WriteOpts = []),
                            delete_traverse_interval1(LoopContext0#{
                                deleted := AccDel0 + 1,
                                remaining := Remaining0 - 1
                            });
                        false ->
                            delete_traverse_interval1(LoopContext0#{remaining := Remaining0 - 1})
                    end;
                false ->
                    delete_traverse_interval1(LoopContext0)
            end;
        overflow ->
            {0, It0, AccDel0, AccIter0};
        false ->
            {Remaining0, It, AccDel0, AccIter0}
    end.

delete_traverse_interval1(#{
    storage_iter := It, deleted := AccDel, iterated_over := AccIter, remaining := 0
}) ->
    {0, It, AccDel, AccIter};
delete_traverse_interval1(LoopContext0) ->
    #{
        it_handle := ITHandle,
        deleted := AccDel,
        iterated_over := AccIter,
        storage_iter := It
    } = LoopContext0,
    inc_counter(?DS_BITFIELD_LTS_NEXT_COUNTER),
    case rocksdb:iterator_move(ITHandle, next) of
        {ok, Key, Val} ->
            delete_traverse_interval(LoopContext0#{
                iterated_over := AccIter + 1,
                current_key := Key,
                current_val := Val
            });
        {error, invalid_iterator} ->
            {0, It, AccDel, AccIter}
    end.

-spec check_timestamp(emqx_ds:time(), iterator() | delete_iterator(), emqx_ds:time()) ->
    true | false | overflow.
check_timestamp(Cutoff, _It, Timestamp) when Timestamp >= Cutoff ->
    %% We hit the current epoch, we can't continue iterating over it yet.
    %% It would be unsafe otherwise: messages can be stored in the current epoch
    %% concurrently with iterating over it. They can end up earlier (in the iteration
    %% order) due to the nature of keymapping, potentially causing us to miss them.
    overflow;
check_timestamp(_Cutoff, #{?start_time := StartTime}, Timestamp) ->
    Timestamp >= StartTime.

-spec check_message(iterator() | delete_iterator(), emqx_types:message()) ->
    true | false.
check_message(#{?topic_filter := TopicFilter}, #message{topic = Topic}) ->
    emqx_topic:match(emqx_topic:tokens(Topic), TopicFilter).

format_key(KeyMapper, Key) ->
    Vec = [integer_to_list(I, 16) || I <- emqx_ds_bitmask_keymapper:key_to_vector(KeyMapper, Key)],
    lists:flatten(io_lib:format("~.16B (~s)", [Key, string:join(Vec, ",")])).

-spec make_key(s(), emqx_ds:time(), emqx_types:topic()) -> {binary(), [binary()]}.
make_key(#s{keymappers = KeyMappers, trie = Trie, threshold_fun = TFun}, Timestamp, Topic) ->
    Tokens = emqx_topic:words(Topic),
    {TopicIndex, Varying} = emqx_ds_lts:topic_key(Trie, TFun, Tokens),
    KeyBin = make_key(KeyMappers, Timestamp, TopicIndex, Varying),
    {KeyBin, Varying}.

-spec lookup_key(s(), emqx_ds:time(), emqx_types:topic()) -> binary() | undefined.
lookup_key(#s{keymappers = KeyMappers, trie = Trie}, Timestamp, Topic) ->
    Tokens = emqx_topic:words(Topic),
    case emqx_ds_lts:lookup_topic_key(Trie, Tokens) of
        {ok, {TopicIndex, Varying}} ->
            make_key(KeyMappers, Timestamp, TopicIndex, Varying);
        undefined ->
            undefined
    end.

-spec make_key(
    array:array(emqx_ds_bitmask_keymapper:keymapper()),
    emqx_ds:time(),
    emqx_ds_lts:static_key(),
    [emqx_ds_lts:varying()]
) ->
    binary().
make_key(KeyMappers, Timestamp, TopicIndex, Varying) ->
    VaryingHashes = [hash_topic_level(I) || I <- Varying],
    KeyMapper = array:get(length(Varying), KeyMappers),
    map_key(KeyMapper, TopicIndex, Timestamp, VaryingHashes).

-spec map_key(
    emqx_ds_bitmask_keymapper:keymapper(),
    emqx_ds_lts:static_key(),
    emqx_ds:time(),
    [non_neg_integer()]
) ->
    binary().
map_key(KeyMapper, TopicIndex, Timestamp, Varying) ->
    Vector = [TopicIndex, Timestamp | Varying],
    emqx_ds_bitmask_keymapper:key_to_bitstring(
        KeyMapper,
        emqx_ds_bitmask_keymapper:vector_to_key(KeyMapper, Vector)
    ).

hash_topic_level('') ->
    hash_topic_level(<<>>);
hash_topic_level(TopicLevel) ->
    <<Int:64, _/binary>> = erlang:md5(TopicLevel),
    Int.

-spec message_to_value_v1(emqx_types:message()) -> value_v1().
message_to_value_v1(#message{
    id = Id,
    qos = Qos,
    from = From,
    flags = Flags,
    headers = Headers,
    topic = Topic,
    payload = Payload,
    timestamp = Timestamp,
    extra = Extra
}) ->
    {Id, Qos, From, Flags, Headers, Topic, Payload, Timestamp, Extra}.

-spec value_v1_to_message(value_v1()) -> emqx_types:message().
value_v1_to_message({Id, Qos, From, Flags, Headers, Topic, Payload, Timestamp, Extra}) ->
    #message{
        id = Id,
        qos = Qos,
        from = From,
        flags = Flags,
        headers = Headers,
        topic = Topic,
        payload = Payload,
        timestamp = Timestamp,
        extra = Extra
    }.

deserialize(Blob) ->
    value_v1_to_message(binary_to_term(Blob)).

-define(BYTE_SIZE, 8).

%% erlfmt-ignore
make_keymapper(TopicIndexBytes, BitsPerTopicLevel, TSBits, TSOffsetBits, N) ->
    TsEpochBits = TSBits - TSOffsetBits,
    Bitsources =
    %% Dimension Offset   Bitsize
        [{?DIM_TOPIC,     0,            TopicIndexBytes * ?BYTE_SIZE}] ++   %% Topic index
        [{?DIM_TS,        TSOffsetBits, TsEpochBits} || TsEpochBits > 0] ++ %% Timestamp epoch
        [{?DIM_TS + I,    0,            BitsPerTopicLevel           }       %% Varying topic levels
                                                           || I <- lists:seq(1, N)] ++
        [{?DIM_TS,        0,            TSOffsetBits                }],     %% Timestamp offset
    Keymapper = emqx_ds_bitmask_keymapper:make_keymapper(lists:reverse(Bitsources)),
    %% Assert:
    case emqx_ds_bitmask_keymapper:bitsize(Keymapper) rem 8 of
        0 ->
            ok;
        _ ->
            error(#{'$msg' => "Non-even key size", bitsources => Bitsources})
    end,
    Keymapper.

-spec restore_trie(pos_integer(), rocksdb:db_handle(), rocksdb:cf_handle()) -> emqx_ds_lts:trie().
restore_trie(TopicIndexBytes, DB, CF) ->
    PersistCallback = fun(Key, Val) ->
        push_lts_persist_op(Key, Val),
        ok
    end,
    {ok, IT} = rocksdb:iterator(DB, CF, []),
    try
        Dump = read_persisted_trie(IT, rocksdb:iterator_move(IT, first)),
        TrieOpts = #{persist_callback => PersistCallback, static_key_bits => TopicIndexBytes * 8},
        emqx_ds_lts:trie_restore(TrieOpts, Dump)
    after
        rocksdb:iterator_close(IT)
    end.

-spec copy_previous_trie(rocksdb:db_handle(), rocksdb:cf_handle(), emqx_ds_lts:trie()) ->
    ok.
copy_previous_trie(DB, TrieCF, TriePrev) ->
    {ok, Batch} = rocksdb:batch(),
    lists:foreach(
        fun({Key, Val}) ->
            ok = rocksdb:batch_put(Batch, TrieCF, term_to_binary(Key), term_to_binary(Val))
        end,
        emqx_ds_lts:trie_dump(TriePrev, wildcard)
    ),
    Result = rocksdb:write_batch(DB, Batch, []),
    rocksdb:release_batch(Batch),
    Result.

read_persisted_trie(IT, {ok, KeyB, ValB}) ->
    [
        {binary_to_term(KeyB), binary_to_term(ValB)}
        | read_persisted_trie(IT, rocksdb:iterator_move(IT, next))
    ];
read_persisted_trie(_IT, {error, invalid_iterator}) ->
    [].

inc_counter(Counter) ->
    N = get(Counter),
    put(Counter, N + 1).

init_counters() ->
    _ = [put(I, 0) || I <- ?DS_LTS_COUNTERS],
    ok.

report_counters(Shard) ->
    emqx_ds_builtin_metrics:inc_lts_seek_counter(Shard, get(?DS_BITFIELD_LTS_SEEK_COUNTER)),
    emqx_ds_builtin_metrics:inc_lts_next_counter(Shard, get(?DS_BITFIELD_LTS_NEXT_COUNTER)),
    emqx_ds_builtin_metrics:inc_lts_collision_counter(
        Shard, get(?DS_BITFIELD_LTS_COLLISION_COUNTER)
    ),
    _ = [erase(I) || I <- ?DS_LTS_COUNTERS],
    ok.

%% @doc Generate a column family ID for the MQTT messages
-spec data_cf(emqx_ds_storage_layer:gen_id()) -> [char()].
data_cf(GenId) ->
    "emqx_ds_storage_bitfield_lts_data" ++ integer_to_list(GenId).

%% @doc Generate a column family ID for the trie
-spec trie_cf(emqx_ds_storage_layer:gen_id()) -> [char()].
trie_cf(GenId) ->
    "emqx_ds_storage_bitfield_lts_trie" ++ integer_to_list(GenId).

-spec push_lts_persist_op(_Key, _Val) -> ok.
push_lts_persist_op(Key, Val) ->
    case erlang:get(?lts_persist_ops) of
        undefined ->
            erlang:put(?lts_persist_ops, [{Key, Val}]);
        L when is_list(L) ->
            erlang:put(?lts_persist_ops, [{Key, Val} | L])
    end.

-spec pop_lts_persist_ops() -> [{_Key, _Val}].
pop_lts_persist_ops() ->
    case erlang:erase(?lts_persist_ops) of
        undefined ->
            [];
        L when is_list(L) ->
            L
    end.

-spec write_batch_opts(emqx_ds_storage_layer:batch_store_opts()) ->
    _RocksDBOpts :: [{atom(), _}].
write_batch_opts(#{durable := false}) ->
    [{disable_wal, true}];
write_batch_opts(#{}) ->
    [].

-ifdef(TEST).

serialize(Msg) ->
    term_to_binary(message_to_value_v1(Msg)).

serialize_deserialize_test() ->
    Msg = #message{
        id = <<"message_id_val">>,
        qos = 2,
        from = <<"from_val">>,
        flags = #{sys => true, dup => true},
        headers = #{foo => bar},
        topic = <<"topic/value">>,
        payload = [<<"foo">>, <<"bar">>],
        timestamp = 42424242,
        extra = "extra_val"
    },
    ?assertEqual(Msg, deserialize(serialize(Msg))).

-endif.
