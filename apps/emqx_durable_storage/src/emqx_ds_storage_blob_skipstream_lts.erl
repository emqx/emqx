%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_storage_blob_skipstream_lts).

-behaviour(emqx_ds_storage_layer).

%% API:
-export([]).

%% behavior callbacks:
-export([
    create/6,
    open/5,
    drop/5,
    prepare_batch/4,
    prepare_blob_tx/4,
    commit_batch/4,
    get_streams/4,
    get_delete_streams/4,
    make_iterator/5,
    make_delete_iterator/5,
    update_iterator/4,
    next/6,
    delete_next/7,
    lookup_message/4,

    unpack_iterator/3,
    scan_stream/8,
    message_matcher/3,
    fast_forward/5,
    message_match_context/5,
    iterator_match_context/3,

    batch_events/3
]).

%% internal exports:
-export([]).

%% inline small functions:
-compile(inline).

-export_type([schema/0, s/0]).

-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include("emqx_ds.hrl").
-include("emqx_ds_metrics.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-elvis([{elvis_style, nesting_level, disable}]).
-elvis([{elvis_style, no_if_expression, disable}]).

%%================================================================================
%% Type declarations
%%================================================================================

%% TLOG entry
%%   Keys:
-define(cooked_msg_ops, 6).
-define(cooked_lts_ops, 7).
%%   Payload:
-define(cooked_delete, 100).
-define(cooked_msg_op(STATIC, VARYING, VALUE),
    {STATIC, VARYING, VALUE}
).

-define(lts_persist_ops, emqx_ds_storage_blob_skipstream_lts_ops).

%% Width of the wildcard layer, in bits:
-define(wcb, 16).
-type wildcard_idx() :: 0..16#ffff.

-type wildcard_hash() :: binary().

%% Stream-level key is the hash of all varying topic levels.
-type stream_key() :: binary().

%% Permanent state:
-type schema() ::
    #{
        %% Number of index key bytes allocated for the hash of topic level.
        wildcard_hash_bytes := pos_integer(),
        %% Number of data / index key bytes allocated for the static topic index.
        topic_index_bytes := pos_integer(),
        serialization_schema := emqx_ds_msg_serializer:schema(),
        with_guid := boolean(),
        lts_threshold_spec => emqx_ds_lts:threshold_spec()
    }.

%% Default LTS thresholds: 0th level = 100 entries max, other levels = 10 entries.
-define(DEFAULT_LTS_THRESHOLD, {simple, {100, 10}}).

%% Runtime state:
-record(s, {
    db :: rocksdb:db_handle(),
    data_cf :: rocksdb:cf_handle(),
    trie :: emqx_ds_lts:trie(),
    trie_cf :: rocksdb:cf_handle(),
    serialization_schema :: emqx_ds_msg_serializer:schema(),
    hash_bytes :: pos_integer(),
    threshold_fun :: emqx_ds_lts:threshold_fun()
}).

-type s() :: #s{}.

-record(stream, {
    static_index :: emqx_ds_lts:static_key()
}).

-record(it, {
    static_index :: emqx_ds_lts:static_key(),
    %% Key of the last visited message:
    last_key :: stream_key(),
    %% Compressed topic filter:
    compressed_tf :: binary()
}).

%% Level iterator:
-record(l, {
    n :: non_neg_integer(),
    handle :: rocksdb:itr_handle(),
    hash :: binary()
}).

%% Loop context:
-record(ctx, {
    shard,
    %% Generation runtime state:
    s,
    %% Fold function:
    f,
    %% RocksDB iterators:
    iters,
    %% Cached topic structure for the static index:
    topic_structure,
    %% Effective master hash bitsize:
    master_hash_bits,
    %% Compressed topic filter, split into words:
    filter,
    %% Ignore all keys that are less than this one. Can be `undefined'
    %% in case of a fresh iterator. According to the term order, atom
    %% is less than any binary.
    min_key
}).

%%================================================================================
%% API functions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

create(_ShardId, DBHandle, GenId, Schema0, SPrev, _DBOpts) ->
    Defaults = #{
        wildcard_hash_bytes => 8,
        topic_index_bytes => 8,
        serialization_schema => blob,
        lts_threshold_spec => ?DEFAULT_LTS_THRESHOLD
    },
    Schema = maps:merge(Defaults, Schema0),
    ok = emqx_ds_msg_serializer:check_schema(maps:get(serialization_schema, Schema)),
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
    {Schema, [{DataCFName, DataCFHandle}, {TrieCFName, TrieCFHandle}]}.

open(
    ShardId,
    DBHandle,
    GenId,
    CFRefs,
    Schema = #{
        topic_index_bytes := TIBytes,
        wildcard_hash_bytes := WCBytes,
        serialization_schema := SSchema
    }
) ->
    {_, DataCF} = lists:keyfind(data_cf(GenId), 1, CFRefs),
    {_, TrieCF} = lists:keyfind(trie_cf(GenId), 1, CFRefs),
    Trie = restore_trie(ShardId, TIBytes, DBHandle, TrieCF),
    ThresholdSpec = maps:get(lts_threshold_spec, Schema, ?DEFAULT_LTS_THRESHOLD),
    #s{
        db = DBHandle,
        data_cf = DataCF,
        trie_cf = TrieCF,
        trie = Trie,
        serialization_schema = SSchema,
        hash_bytes = WCBytes,
        threshold_fun = emqx_ds_lts:threshold_fun(ThresholdSpec)
    }.

drop(_ShardId, DBHandle, _GenId, _CFRefs, #s{data_cf = DataCF, trie_cf = TrieCF, trie = Trie}) ->
    emqx_ds_lts:destroy(Trie),
    ok = rocksdb:drop_column_family(DBHandle, DataCF),
    ok = rocksdb:drop_column_family(DBHandle, TrieCF),
    ok.

prepare_batch(_ShardId, S, Operations, _Options) ->
    _ = erase(?lts_persist_ops),
    %% FIXME:
    {ok, #{
        ?cooked_msg_ops => [],
        ?cooked_lts_ops => pop_lts_persist_ops()
    }}.

prepare_blob_tx(DBShard, S, Ops, _Options) ->
    _ = erase(?lts_persist_ops),
    W = maps:get(?ds_tx_write, Ops, []),
    DT = maps:get(?ds_tx_delete_topic, Ops, []),
    try
        OperationsCooked = cook_blob_deletes(DBShard, S, DT, cook_blob_writes(S, W, [])),
        {ok, #{
            ?cooked_msg_ops => OperationsCooked,
            ?cooked_lts_ops => pop_lts_persist_ops()
        }}
    catch
        {Type, Err} ->
            {error, Type, Err}
    end.

cook_blob_writes(_, [], Acc) ->
    lists:reverse(Acc);
cook_blob_writes(S = #s{serialization_schema = SSchema}, [{Topic, Value} | Rest], Acc) ->
    #s{trie = Trie, threshold_fun = TFun} = S,
    {Static, Varying} = emqx_ds_lts:topic_key(Trie, TFun, Topic),
    Bin = emqx_ds_msg_serializer:serialize(SSchema, {Varying, Value}),
    cook_blob_writes(S, Rest, [?cooked_msg_op(Static, Varying, Bin) | Acc]).

cook_blob_deletes(DBShard, S, Topics, Acc0) ->
    lists:foldl(
        fun(TopicFilter, Acc) ->
            cook_blob_deletes1(DBShard, S, TopicFilter, Acc)
        end,
        Acc0,
        Topics
    ).

cook_blob_deletes1(DBShard, S, TopicFilter, Acc0) ->
    lists:foldl(
        fun(Stream, Acc) ->
            {ok, It} = make_iterator(DBShard, S, Stream, TopicFilter, 0),
            TS = get_topic_structure(S#s.trie, Stream#stream.static_index),
            cook_blob_deletes2(DBShard, S, TS, It, Acc)
        end,
        Acc0,
        get_streams(S#s.trie, TopicFilter)
    ).

cook_blob_deletes2(DBShard, S, TS, It0 = #it{static_index = Static}, Acc0) ->
    Fun = fun(_Ctx, _DSKey, Varying, _Val, Acc) ->
        [?cooked_msg_op(Static, Varying, ?cooked_delete) | Acc]
    end,
    case fold(DBShard, S, It0, 100, Acc0, Fun) of
        {ok, It, Acc} ->
            cook_blob_deletes2(DBShard, S, TS, It, Acc)
    end.

commit_batch(
    ShardId,
    #s{db = DB, trie_cf = TrieCF, data_cf = DataCF, trie = Trie, hash_bytes = HashBytes},
    #{?cooked_lts_ops := LtsOps, ?cooked_msg_ops := Operations},
    Options
) ->
    {ok, Batch} = rocksdb:batch(),
    try
        %% Commit LTS trie to the storage:
        lists:foreach(
            fun({Key, Val}) ->
                ok = rocksdb:batch_put(Batch, TrieCF, term_to_binary(Key), term_to_binary(Val))
            end,
            LtsOps
        ),
        %% Apply LTS ops to the memory cache and notify about the new
        %% streams. Note: in case of `builtin_raft' backend
        %% notification is sent _for every replica_ of the database, to
        %% account for possible delays in the replication. Event
        %% deduplication logic of `emqx_ds_new_streams' module should
        %% mitigate the performance impact of repeated events.
        _ = emqx_ds_lts:trie_update(Trie, LtsOps),
        notify_new_streams(ShardId, Trie, LtsOps),
        %% Commit payloads:
        lists:foreach(
            fun(?cooked_msg_op(Static, Varying, Op)) ->
                StreamKey = mk_stream_key(Varying),
                MasterKey = mk_master_key(Static, StreamKey),
                case Op of
                    Payload when is_binary(Payload) ->
                        ok = rocksdb:batch_put(Batch, DataCF, MasterKey, Payload),
                        mk_index(Batch, DataCF, HashBytes, Static, Varying, StreamKey);
                    ?cooked_delete ->
                        delete_index(Batch, DataCF, HashBytes, Static, Varying, StreamKey),
                        ok = rocksdb:batch_delete(Batch, DataCF, MasterKey)
                end
            end,
            Operations
        ),
        Result = rocksdb:write_batch(DB, Batch, [
            {disable_wal, not maps:get(durable, Options, true)}
        ]),
        %% NOTE
        %% Strictly speaking, `{error, incomplete}` is a valid result but should be impossible to
        %% observe until there's `{no_slowdown, true}` in write options.
        case Result of
            ok ->
                ok;
            {error, {error, Reason}} ->
                ?err_unrec({rocksdb, Reason})
        end
    after
        rocksdb:release_batch(Batch)
    end.

batch_events(
    _Shard,
    #s{},
    #{?cooked_msg_ops := Payloads}
) ->
    EventMap = lists:foldl(
        fun
            (?cooked_msg_op(_Static, _Varying, ?cooked_delete), Acc) ->
                Acc;
            (?cooked_msg_op(Static, _Varying, _ValBlob), Acc) ->
                maps:put(#stream{static_index = Static}, 1, Acc)
        end,
        #{},
        Payloads
    ),
    maps:keys(EventMap).

get_streams(_Shard, #s{trie = Trie}, TopicFilter, _StartTime) ->
    get_streams(Trie, TopicFilter).

get_delete_streams(_Shard, #s{trie = Trie}, TopicFilter, _StartTime) ->
    get_streams(Trie, TopicFilter).

make_iterator(
    _Shard,
    #s{trie = Trie},
    #stream{static_index = StaticIdx},
    TopicFilter,
    _TS
) ->
    ?tp_ignore_side_effects_in_prod(emqx_ds_storage_skipstream_lts_make_blob_iterator, #{
        static_index => StaticIdx, topic_filter => TopicFilter
    }),
    {ok, TopicStructure} = emqx_ds_lts:reverse_lookup(Trie, StaticIdx),
    CompressedTF = emqx_ds_lts:compress_topic(StaticIdx, TopicStructure, TopicFilter),
    %% FIXME: this is sketchy
    LastSK = undefined,
    {ok, #it{
        static_index = StaticIdx,
        last_key = LastSK,
        compressed_tf = CompressedTF
    }}.

message_matcher(
    _Shard,
    #s{trie = Trie},
    #it{static_index = StaticIdx, last_key = LastSK, compressed_tf = CTF}
) ->
    {ok, TopicStructure} = emqx_ds_lts:reverse_lookup(Trie, StaticIdx),
    TF = emqx_ds_lts:decompress_topic(TopicStructure, CTF),
    fun(TopicTokens, MsgKey, _Message) ->
        case match_stream_key(StaticIdx, MsgKey) of
            false ->
                ?tp_ignore_side_effects_in_prod(emqx_ds_storage_skipstream_lts_matcher, #{
                    static_index => StaticIdx,
                    last_seen_key => LastSK,
                    topic_filter => TF,
                    its => false
                }),
                false;
            SK ->
                ?tp_ignore_side_effects_in_prod(emqx_ds_storage_skipstream_lts_matcher, #{
                    static_index => StaticIdx,
                    last_seen_key => LastSK,
                    topic_filter => TF,
                    its => SK
                }),
                (SK > LastSK) andalso
                    emqx_topic:match(TopicTokens, TF)
        end
    end.

unpack_iterator(_Shard, S = #s{trie = Trie}, #it{
    static_index = StaticIdx, compressed_tf = CTF, last_key = LSK
}) ->
    DSKey = mk_master_key(StaticIdx, LSK),
    TopicFilter = emqx_ds_lts:decompress_topic(get_topic_structure(Trie, StaticIdx), CTF),
    {#stream{static_index = StaticIdx}, TopicFilter, DSKey, stream_key_ts(LSK, MHB)}.

scan_stream(
    Shard,
    S = #s{trie = Trie},
    #stream{static_index = StaticIdx},
    TopicFilter,
    LastSeenKey,
    BatchSize,
    TMax,
    IsCurrent
) ->
    {ok, TopicStructure} = emqx_ds_lts:reverse_lookup(Trie, StaticIdx),
    Varying = emqx_ds_lts:compress_topic(StaticIdx, TopicStructure, TopicFilter),
    ItSeed = #it{
        static_index = StaticIdx,
        compressed_tf = emqx_topic:join(Varying),
        last_key = match_stream_key(StaticIdx, LastSeenKey)
    },
    case next(Shard, S, ItSeed, BatchSize, TMax, IsCurrent) of
        {ok, #it{last_key = LSK}, Batch} ->
            MHB = master_hash_bits(S, Varying),
            DSKey = mk_master_key(StaticIdx, LSK, MHB),
            {ok, DSKey, Batch};
        Other ->
            Other
    end.

make_delete_iterator(Shard, Data, Stream, TopicFilter, StartTime) ->
    make_iterator(Shard, Data, Stream, TopicFilter, StartTime).

update_iterator(_Shard, _Data, OldIter, DSKey) ->
    case match_stream_key(OldIter#it.static_index, DSKey) of
        false ->
            ?err_unrec("Invalid datastream key");
        StreamKey ->
            {ok, OldIter#it{last_key = StreamKey}}
    end.

fast_forward(
    ShardId,
    S = #s{master_hash_bits = MHB},
    It0 = #it{last_key = LastSeenKey0, static_index = _StaticIdx, compressed_tf = _CompressedTF},
    FFToKey,
    TMax
) ->
    case match_stream_key(It0#it.static_index, FFToKey) of
        false ->
            ?err_unrec(<<"Invalid datastream key">>);
        FFToStreamKey ->
            LastSeenTS = stream_key_ts(LastSeenKey0, MHB),
            case stream_key_ts(FFToStreamKey, MHB) of
                FFToTimestamp when FFToTimestamp > TMax ->
                    ?err_unrec(<<"Key is too far in the future">>);
                FFToTimestamp when FFToTimestamp =< LastSeenTS ->
                    %% The new position is earlier than the current position.
                    %% We keep the original position to prevent duplication of
                    %% messages. De-duplication is performed by
                    %% `message_matcher' callback that filters out messages
                    %% with TS older than the iterator's.
                    {ok, It0};
                _FFToTimestamp ->
                    case next(ShardId, S, It0, 1, TMax, true) of
                        {ok, #it{last_key = LastSeenKey}, [_]} when LastSeenKey =< FFToKey ->
                            ?err_unrec(has_data);
                        {ok, It, _} ->
                            {ok, It};
                        Err ->
                            Err
                    end
            end
    end.

-record(match_ctx, {compressed_topic, stream_key}).

message_match_context(_Shard, GenData, Stream, MsgKey, #message{topic = Topic}) ->
    #s{trie = Trie} = GenData,
    #stream{static_index = StaticIdx} = Stream,
    case match_stream_key(StaticIdx, MsgKey) of
        false ->
            ?err_unrec(mismatched_stream);
        SK ->
            %% Note: it's kind of wasteful to tokenize and compress
            %% the topic that we've just decompressed during scan
            %% stream, but trying to avoid this would complicate the
            %% pipeline even further. Let's live with that for now.
            {ok, TopicStructure} = emqx_ds_lts:reverse_lookup(Trie, StaticIdx),
            CT = emqx_ds_lts:compress_topic(StaticIdx, TopicStructure, Topic),
            {ok, #match_ctx{compressed_topic = CT, stream_key = SK}}
    end.

iterator_match_context(_Shard, _GenData, Iterator) ->
    #it{last_key = LastSK, compressed_tf = CTF} = Iterator,
    fun(#match_ctx{compressed_topic = CT, stream_key = SK}) ->
        (SK > LastSK) andalso
            emqx_topic:match(CT, CTF)
    end.

next(DBShard, S, ItSeed, BatchSize, IsCurrent, _TMax) ->
    Acc0 = [],
    Fun = fun(Ctx, _DSKey, Varying, Val, Acc) ->
        Topic = emqx_ds_lts:decompress_topic(Ctx#ctx.topic_structure, Varying),
        [{Topic, Val} | Acc]
    end,
    Result = fold(DBShard, S, ItSeed, BatchSize, Acc0, Fun),
    case Result of
        {ok, _It, []} when not IsCurrent ->
            {ok, end_of_stream};
        {ok, It, Batch} ->
            %% Note: we don't reverse the batch here.
            {ok, It, Batch};
        {error, EC, Err} ->
            {error, EC, Err}
    end.

fold(DBShard = {_DB, Shard}, S, ItSeed, BatchSize, Acc0, Fun) ->
    init_counters(),
    {Snapshot, PerLevelIterators} = init_iterators(S, ItSeed),
    %% ?tp(notice, skipstream_init_iters, #{it => It, its => Iterators}),
    try
        start_fold_loop(Shard, S, ItSeed, PerLevelIterators, BatchSize, Acc0, Fun)
    after
        free_iterators(PerLevelIterators),
        rocksdb:release_snapshot(Snapshot),
        collect_counters(DBShard)
    end.

delete_next(Shard, S, It0, Selector, BatchSize, Now, IsCurrent) ->
    case next(Shard, S, It0, BatchSize, Now, IsCurrent) of
        {ok, It, KVs} ->
            batch_delete(S, It, Selector, KVs);
        Ret ->
            Ret
    end.

lookup_message(
    _Shard,
    S = #s{db = DB, data_cf = CF, trie = Trie},
    Topic,
    _Timestamp
) ->
    case emqx_ds_lts:lookup_topic_key(Trie, Topic) of
        {ok, {StaticIdx, Varying}} ->
            SK = mk_stream_key(Varying),
            DSKey = mk_master_key(StaticIdx, SK),
            case rocksdb:get(DB, CF, DSKey, []) of
                {ok, Bin} ->
                    {_, Val} = deserialize(S, Bin),
                    {ok, {Topic, Val}};
                not_found ->
                    undefined;
                {error, Reason} ->
                    ?err_unrec({rocksdb, Reason})
            end;
        undefined ->
            undefined
    end.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

get_streams(Trie, TopicFilter) ->
    lists:map(
        fun({Static, _Varying}) ->
            #stream{static_index = Static}
        end,
        emqx_ds_lts:match_topics(Trie, TopicFilter)
    ).

%%%%%%%% Value (de)serialization %%%%%%%%%%

deserialize(
    #s{serialization_schema = SSchema},
    Blob
) ->
    emqx_ds_msg_serializer:deserialize(SSchema, Blob).

%%%%%%%% Deletion %%%%%%%%%%

batch_delete(S = #s{db = DB, data_cf = CF}, It, Selector, KVs) ->
    #it{static_index = Static, compressed_tf = CompressedTF} = It,
    MHB = master_hash_bits(S, CompressedTF),
    {Indices, _} = lists:foldl(
        fun
            ('+', {Acc, WildcardIdx}) ->
                {Acc, WildcardIdx + 1};
            (LevelFilter, {Acc0, WildcardIdx}) ->
                Acc = [{WildcardIdx, hash_topic_level(HashBytes, LevelFilter)} | Acc0],
                {Acc, WildcardIdx + 1}
        end,
        {[], 1},
        CompressedTF
    ),
    KeyFamily = [{0, <<>>} | Indices],
    {ok, Batch} = rocksdb:batch(),
    try
        Ndeleted = lists:foldl(
            fun({MsgKey, Val}, Acc) ->
                case Selector(Val) of
                    true ->
                        do_delete(CF, Batch, MHB, Static, KeyFamily, MsgKey),
                        Acc + 1;
                    false ->
                        Acc
                end
            end,
            0,
            KVs
        ),
        case rocksdb:write_batch(DB, Batch, []) of
            ok ->
                {ok, It, Ndeleted, length(KVs)};
            {error, {error, Reason}} ->
                ?err_unrec({rocksdb, Reason})
        end
    after
        rocksdb:release_batch(Batch)
    end.

do_delete(CF, Batch, MHB, Static, KeyFamily, MsgKey) ->
    SK = match_stream_key(Static, MsgKey),
    lists:foreach(
        fun({WildcardIdx, Hash}) ->
            ok = rocksdb:batch_delete(Batch, CF, mk_key(Static, WildcardIdx, Hash, SK, MHB))
        end,
        KeyFamily
    ).

%%%%%%%% Iteration %%%%%%%%%%

%% @doc Init iterators per level, order is important: e.g. [L1, L2, L3, L0].
init_iterators(S, #it{static_index = Static, compressed_tf = CompressedTF}) ->
    {ok, Snapshot} = rocksdb:snapshot(S#s.db),
    {Snapshot, do_init_iterators(S, Snapshot, Static, CompressedTF, 1)}.

do_init_iterators(S, Snapshot, Static, ['#'], WildcardLevel) ->
    do_init_iterators(S, Snapshot, Static, [], WildcardLevel);
do_init_iterators(S, Snapshot, Static, ['+' | TopicFilter], WildcardLevel) ->
    %% Ignore wildcard levels in the topic filter because it has no value to index '+'
    do_init_iterators(S, Snapshot, Static, TopicFilter, WildcardLevel + 1);
do_init_iterators(S, Snapshot, Static, [Constraint | TopicFilter], WildcardLevel) ->
    %% Create iterator for the index stream:
    #s{db = DB, data_cf = DataCF} = S,
    Hash = hash_topic_level(HashBytes, Constraint),
    {ok, ItHandle} = rocksdb:iterator(
        DB, DataCF, get_key_range(Snapshot, Static, WildcardLevel, Hash)
    ),
    It = #l{
        n = WildcardLevel,
        handle = ItHandle,
        hash = Hash
    },
    [It | do_init_iterators(S, Snapshot, Static, TopicFilter, WildcardLevel + 1)];
do_init_iterators(S, Snapshot, Static, [], _WildcardLevel) ->
    %% Create an iterator for the data stream:
    #s{db = DB, data_cf = DataCF} = S,
    Hash = <<>>,
    {ok, ItHandle} = rocksdb:iterator(DB, DataCF, get_key_range(Snapshot, Static, 0, Hash)),
    [
        #l{
            n = 0,
            handle = ItHandle,
            hash = Hash
        }
    ].

start_fold_loop(
    Shard,
    S = #s{trie = Trie},
    ItSeed = #it{static_index = StaticIdx, last_key = LSK, compressed_tf = CompressedTF},
    PerLevelIterators,
    BatchSize,
    Acc,
    Fun
) ->
    %% cache it?
    TopicStructure = get_topic_structure(Trie, StaticIdx),
    Ctx = #ctx{
        shard = Shard,
        s = S,
        f = Fun,
        iters = PerLevelIterators,
        topic_structure = TopicStructure,
        filter = CompressedTF,
        min_key = LSK
    },
    case LSK of
        undefined ->
            StartPos = <<>>;
        StartPos when is_binary(LSK) ->
            ok
    end,
    fold_loop(Ctx, ItSeed, BatchSize, {seek, StartPos}, Acc).

get_topic_structure(Trie, StaticIdx) ->
    case emqx_ds_lts:reverse_lookup(Trie, StaticIdx) of
        {ok, Rev} ->
            Rev;
        undefined ->
            throw(#{
                msg => "LTS trie missing key",
                key => StaticIdx
            })
    end.

fold_loop(_Ctx, It, 0, _Op, Acc) ->
    {ok, It, Acc};
fold_loop(Ctx, It0, BatchSize, Op, Acc) ->
    %% ?tp(notice, skipstream_loop, #{
    %%     ts => It0#it.ts, tf => It0#it.compressed_tf, bs => BatchSize, tmax => TMax, op => Op
    %% }),
    #ctx{s = S, f = Fun, master_hash_bits = MHB, iters = Iterators} = Ctx,
    #it{static_index = StaticIdx, compressed_tf = CompressedTF} = It0,
    %% Note: `next_step' function destructively updates RocksDB
    %% iterators in `ctx.iters' (they are handles, not values!),
    %% therefore a recursive call with the same arguments is not a
    %% bug.
    case fold_step(S, StaticIdx, MHB, CompressedTF, Iterators, any, Op) of
        none ->
            %% ?tp(notice, skipstream_loop_result, #{r => none}),
            inc_counter(?DS_SKIPSTREAM_LTS_EOS),
            {ok, It0, Acc};
        {seek, SK} ->
            %% ?tp(notice, skipstream_loop_result, #{r => seek, ts => TS}),
            It = It0#it{last_key = SK},
            fold_loop(Ctx, It, BatchSize, {seek, SK}, Acc);
        {ok, SK, DSKey, CompressedTopic, Val} ->
            %% ?tp(notice, skipstream_loop_result, #{r => ok, ts => TS, key => Key}),
            It = It0#it{last_key = SK},
            fold_loop(Ctx, It, BatchSize - 1, next, Fun(Ctx, DSKey, CompressedTopic, Val, Acc))
    end.

fold_step(
    S,
    StaticIdx,
    MHB,
    CompressedTF,
    [#l{hash = Hash, handle = IH, n = N} | Iterators],
    ExpectedSK,
    Op
) ->
    Result =
        case Op of
            next ->
                inc_counter(?DS_SKIPSTREAM_LTS_NEXT),
                rocksdb:iterator_move(IH, next);
            {seek, SK} ->
                inc_counter(?DS_SKIPSTREAM_LTS_SEEK),
                rocksdb:iterator_move(IH, {seek, mk_key(StaticIdx, N, Hash, SK, MHB)})
        end,
    case Result of
        {error, invalid_iterator} ->
            none;
        {ok, Key, Blob} ->
            case match_stream_key(StaticIdx, N, Hash, Key) of
                false ->
                    %% This should not happen, since we set boundaries
                    %% to the iterators, and overflow to a different
                    %% key prefix should be caught by the previous
                    %% clause:
                    none;
                NextSK when ExpectedSK =:= any; NextSK =:= ExpectedSK ->
                    %% We found a key that corresponds to the stream key
                    %% (timestamp) we expect.
                    %% ?tp(notice, ?MODULE_STRING "_step_hit", #{
                    %%     next_ts => NextSK, expected => ExpectedTS, n => N
                    %% }),
                    case Iterators of
                        [] ->
                            %% Last one in PerLevelIterators is the one and the only one data stream.
                            0 = N,
                            %% This is data stream as well. Check
                            %% message for hash collisions and return
                            %% value:
                            {CompressedTopic, Val} = deserialize(S, Blob),
                            case emqx_topic:match(CompressedTopic, CompressedTF) of
                                true ->
                                    inc_counter(?DS_SKIPSTREAM_LTS_HIT),
                                    {ok, NextSK, Key, CompressedTopic, Val};
                                false ->
                                    %% Hash collision. Advance to the
                                    %% next timestamp:
                                    inc_counter(?DS_SKIPSTREAM_LTS_HASH_COLLISION),
                                    {seek, NextSK + 1}
                            end;
                        _ ->
                            %% This is index stream. Keep matching NextSK in other levels.
                            fold_step(
                                S, StaticIdx, MHB, CompressedTF, Iterators, NextSK, {seek, NextSK}
                            )
                    end;
                NextSK when NextSK > ExpectedSK, N > 0 ->
                    %% Next index level is not what we expect. Reset
                    %% search to the first wildcard index, but
                    %% continue from `NextSK'.
                    %%
                    %% Note: if `NextSK > ExpectedSK' and `N =:= 0',
                    %% it means the upper (replication) level is
                    %% broken and supplied us NextSK that advenced
                    %% past the point of time that can be safely read.
                    %% We don't handle it here.
                    inc_counter(?DS_SKIPSTREAM_LTS_MISS),
                    {seek, NextSK};
                NextSK ->
                    error(#{
                        reason => internal_error,
                        move => Op,
                        static => StaticIdx,
                        next_sk => NextSK,
                        expected_sk => ExpectedSK,
                        n => N
                    })
            end
    end.

free_iterators(Its) ->
    lists:foreach(
        fun(#l{handle = IH}) ->
            ok = rocksdb:iterator_close(IH)
        end,
        Its
    ).

%%%%%%%% Indexes %%%%%%%%%%

mk_index(Batch, CF, HashBytes, Static, Varying, StreamKey) ->
    do_mk_index(Batch, CF, HashBytes, Static, 1, Varying, StreamKey).

do_mk_index(Batch, CF, HashBytes, Static, N, [TopicLevel | Varying], StreamKey) ->
    Key = mk_key(Static, N, hash_topic_level(HashBytes, TopicLevel), StreamKey),
    ok = rocksdb:batch_put(Batch, CF, Key, <<>>),
    do_mk_index(Batch, CF, HashBytes, Static, N + 1, Varying, StreamKey);
do_mk_index(_Batch, _CF, _HashBytes, _Static, _N, [], _StreamKey) ->
    ok.

delete_index(Batch, CF, HashBytes, Static, Varying, StreamKey) ->
    do_delete_index(Batch, CF, HashBytes, Static, 1, Varying, StreamKey).

do_delete_index(Batch, CF, HashBytes, Static, N, [TopicLevel | Varying], StreamKey) ->
    Key = mk_key(Static, N, hash_topic_level(HashBytes, TopicLevel), StreamKey),
    ok = rocksdb:batch_delete(Batch, CF, Key),
    do_delete_index(Batch, CF, HashBytes, Static, N + 1, Varying, StreamKey);
do_delete_index(_Batch, _CF, _HashBytes, _Static, _N, [], _StreamKey) ->
    ok.

%%%%%%%% Keys %%%%%%%%%%

get_key_range(Snapshot, StaticIdx, WildcardIdx, Hash) ->
    %% NOTE: Assuming `?max_ts` is not a valid message timestamp.
    [
        {iterate_lower_bound, mk_key(StaticIdx, WildcardIdx, Hash, 0)},
        {iterate_upper_bound, mk_key(StaticIdx, WildcardIdx, Hash, 0)},
        {snapshot, Snapshot}
    ].

-spec match_stream_key(emqx_ds_lts:static_key(), binary()) ->
    stream_key() | false.
match_stream_key(StaticIdx, Key) ->
    case match_key_prefix(StaticIdx, 0, <<>>, Key) of
        false ->
            false;
        StreamKey ->
            decode_stream_key(StreamKey)
    end.

-spec match_stream_key(emqx_ds_lts:static_key(), wildcard_idx(), wildcard_hash(), binary()) ->
    stream_key() | false.
match_stream_key(StaticIdx, WildcardIdx, Hash, Key) ->
    case match_key_prefix(StaticIdx, WildcardIdx, Hash, Key) of
        false ->
            false;
        StreamKey ->
            decode_stream_key(StreamKey)
    end.

-spec match_key_prefix(emqx_ds_lts:static_key(), wildcard_idx(), wildcard_hash(), binary()) ->
    binary() | false.
match_key_prefix(StaticIdx, 0, <<>>, Key) ->
    TSz = byte_size(StaticIdx),
    case Key of
        <<StaticIdx:TSz/binary, 0:?wcb, StreamKey/binary>> ->
            StreamKey;
        _ ->
            false
    end;
match_key_prefix(StaticIdx, Idx, Hash, Key) when Idx > 0 ->
    TSz = byte_size(StaticIdx),
    Hsz = byte_size(Hash),
    case Key of
        <<StaticIdx:TSz/binary, Idx:?wcb, Hash:Hsz/binary, StreamKey/binary>> ->
            StreamKey;
        _ ->
            false
    end.

-spec mk_key(emqx_ds_lts:static_key(), wildcard_idx(), wildcard_hash(), stream_key()) ->
    binary().
mk_key(StaticIdx, 0, <<>>, StreamKey) ->
    mk_master_key(StaticIdx, StreamKey);
mk_key(StaticIdx, N, Hash, StreamKey) when N > 0 ->
    mk_index_key(StaticIdx, N, Hash, StreamKey).

mk_stream_key([]) ->
    <<>>;
mk_stream_key(Varying) when is_list(Varying) ->
    erlang:md5(Varying).

mk_master_key(StaticIdx, StreamKey) ->
    %% Data stream is identified by wildcard level = 0
    <<StaticIdx/binary, 0:?wcb, StreamKey/binary>>.

mk_index_key(StaticIdx, N, Hash, StreamKey) ->
    %% Index stream:
    <<StaticIdx/binary, N:?wcb, Hash/binary, StreamKey/binary>>.

% encode_stream_key(StreamKey, MHB) ->
%     <<StreamKey:(?tsb + MHB)>>.

decode_stream_key(StreamKey) ->
    binary:decode_unsigned(StreamKey).

stream_key_ts(StreamKey, MHB) ->
    StreamKey bsr MHB.

hash_topic_level(HashBytes, TopicLevel) ->
    first_n_bytes(HashBytes, hash_topic_level(TopicLevel)).

hash_topic_level(TopicLevel) ->
    erlang:md5(topic_level_bin(TopicLevel)).

hash_topic_levels(Varying) ->
    erlang:md5_final(hash_topic_levels(Varying, erlang:md5_init())).

hash_topic_levels([TopicLevel | Rest], HashCtx0) ->
    HashCtx = erlang:md5_update(HashCtx0, topic_level_bin(TopicLevel)),
    hash_topic_levels(Rest, HashCtx);
hash_topic_levels([], HashCtx) ->
    HashCtx.

first_n_bytes(NBytes, Hash) ->
    <<Part:NBytes/binary, _/binary>> = Hash,
    Part.

first_n_bits(NBits, Hash) ->
    <<Part:NBits, _/bitstring>> = Hash,
    Part.

topic_level_bin('') -> <<>>;
topic_level_bin(TL) -> TL.

%%%%%%%% LTS %%%%%%%%%%

-spec restore_trie(
    emqx_ds_storage_layer:dbshard(), pos_integer(), rocksdb:db_handle(), rocksdb:cf_handle()
) -> emqx_ds_lts:trie().
restore_trie(Shard, StaticIdxBytes, DB, CF) ->
    PersistCallback = fun(Key, Val) ->
        push_lts_persist_op(Key, Val),
        ok
    end,
    {ok, It} = rocksdb:iterator(DB, CF, []),
    try
        Dump = read_persisted_trie(It, rocksdb:iterator_move(It, first)),
        TrieOpts = #{
            persist_callback => PersistCallback,
            static_key_bytes => StaticIdxBytes,
            reverse_lookups => true
        },
        Trie = emqx_ds_lts:trie_restore(TrieOpts, Dump),
        notify_new_streams(Shard, Trie, Dump),
        Trie
    after
        rocksdb:iterator_close(It)
    end.

%% Send notifications about new streams:
notify_new_streams({DB, _Shard}, Trie, Dump) ->
    [
        emqx_ds_new_streams:notify_new_stream(DB, TopicFilter)
     || TopicFilter <- emqx_ds_lts:updated_topics(Trie, Dump)
    ],
    ok.

-spec copy_previous_trie(rocksdb:db_handle(), rocksdb:cf_handle(), emqx_ds_lts:trie()) ->
    ok.
copy_previous_trie(RocksDB, TrieCF, TriePrev) ->
    {ok, Batch} = rocksdb:batch(),
    lists:foreach(
        fun({Key, Val}) ->
            ok = rocksdb:batch_put(Batch, TrieCF, term_to_binary(Key), term_to_binary(Val))
        end,
        emqx_ds_lts:trie_dump(TriePrev, wildcard)
    ),
    Result = rocksdb:write_batch(RocksDB, Batch, []),
    rocksdb:release_batch(Batch),
    Result.

push_lts_persist_op(Key, Val) ->
    case erlang:get(?lts_persist_ops) of
        undefined ->
            erlang:put(?lts_persist_ops, [{Key, Val}]);
        L when is_list(L) ->
            erlang:put(?lts_persist_ops, [{Key, Val} | L])
    end.

pop_lts_persist_ops() ->
    case erlang:erase(?lts_persist_ops) of
        undefined ->
            [];
        L when is_list(L) ->
            L
    end.

read_persisted_trie(It, {ok, KeyB, ValB}) ->
    [
        {binary_to_term(KeyB), binary_to_term(ValB)}
        | read_persisted_trie(It, rocksdb:iterator_move(It, next))
    ];
read_persisted_trie(_It, {error, invalid_iterator}) ->
    [].

%%%%%%%% Column families %%%%%%%%%%

%% @doc Generate a column family ID for the MQTT messages
-spec data_cf(emqx_ds_storage_layer:gen_id()) -> [char()].
data_cf(GenId) ->
    "emqx_ds_storage_blob_skipstream_lts_data" ++ integer_to_list(GenId).

%% @doc Generate a column family ID for the trie
-spec trie_cf(emqx_ds_storage_layer:gen_id()) -> [char()].
trie_cf(GenId) ->
    "emqx_ds_storage_blob_skipstream_lts_trie" ++ integer_to_list(GenId).

%%%%%%%% Counters %%%%%%%%%%

-define(COUNTERS, [
    ?DS_SKIPSTREAM_LTS_SEEK,
    ?DS_SKIPSTREAM_LTS_NEXT,
    ?DS_SKIPSTREAM_LTS_HASH_COLLISION,
    ?DS_SKIPSTREAM_LTS_HIT,
    ?DS_SKIPSTREAM_LTS_MISS,
    ?DS_SKIPSTREAM_LTS_FUTURE,
    ?DS_SKIPSTREAM_LTS_EOS
]).

inc_counter(Counter) ->
    N = get(Counter),
    put(Counter, N + 1).

init_counters() ->
    _ = [put(I, 0) || I <- ?COUNTERS],
    ok.

collect_counters(Shard) ->
    lists:foreach(
        fun(Key) ->
            emqx_ds_builtin_metrics:collect_shard_counter(Shard, Key, get(Key))
        end,
        ?COUNTERS
    ).

%%================================================================================
%% Tests
%%================================================================================
