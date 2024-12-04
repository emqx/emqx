%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_storage_skipstream_lts).

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

    unpack_iterator/3,
    scan_stream/8,
    message_matcher/3,

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

%%================================================================================
%% Type declarations
%%================================================================================

%% TLOG entry
%%   Keys:
-define(cooked_msg_ops, 6).
-define(cooked_lts_ops, 7).
%%   Payload:
-define(cooked_delete, 100).
-define(cooked_msg_op(TIMESTAMP, STATIC, VARYING, VALUE),
    {TIMESTAMP, STATIC, VARYING, VALUE}
).

-define(lts_persist_ops, emqx_ds_storage_skipstream_lts_ops).

%% Width of the wildcard layer, in bits:
-define(wcb, 16).
-type wildcard_idx() :: 0..16#ffff.

%% Width of the timestamp, in bits:
-define(tsb, 64).
-define(max_ts, 16#ffffffffffffffff).
-type ts() :: 0..?max_ts.

-type wildcard_hash() :: binary().

%% Stream-level key.
%% Uniquely identifies message within a stream, consists of timestamp and (optionally)
%% master hash, which is a hash of all non-static topic levels.
-type stream_key() :: non_neg_integer().

-type n_bits() :: non_neg_integer().

%% Permanent state:
-type schema() ::
    #{
        %% Number of index key bytes allocated for the hash of topic level.
        wildcard_hash_bytes := pos_integer(),
        %% Number of data / index key bytes allocated for the static topic index.
        topic_index_bytes := pos_integer(),
        %% Number of data key bytes allocated for the hash of (non-static) topic.
        %% Default is 0. Setting to non-zero makes sense if you plan to use this layout
        %% in non-append-only DBs.
        master_hash_bytes => pos_integer(),
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
    master_hash_bits :: n_bits(),
    threshold_fun :: emqx_ds_lts:threshold_fun(),
    with_guid :: boolean()
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
    compressed_tf :: binary(),
    misc = []
}).

%% Level iterator:
-record(l, {
    n :: non_neg_integer(),
    handle :: rocksdb:itr_handle(),
    hash :: binary()
}).

%%================================================================================
%% API functions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

create(_ShardId, DBHandle, GenId, Schema0, SPrev, DBOpts) ->
    Defaults = #{
        wildcard_hash_bytes => 8,
        topic_index_bytes => 8,
        master_hash_bytes =>
            case DBOpts of
                #{append_only := true} -> 0;
                #{} = _NonAppendOnly -> 8
            end,
        serialization_schema => asn1,
        with_guid => false,
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
        serialization_schema := SSchema,
        with_guid := WithGuid
    }
) ->
    {_, DataCF} = lists:keyfind(data_cf(GenId), 1, CFRefs),
    {_, TrieCF} = lists:keyfind(trie_cf(GenId), 1, CFRefs),
    Trie = restore_trie(ShardId, TIBytes, DBHandle, TrieCF),
    ThresholdSpec = maps:get(lts_threshold_spec, Schema, ?DEFAULT_LTS_THRESHOLD),
    MHBits = maps:get(master_hash_bytes, Schema, 0) * 8,
    #s{
        db = DBHandle,
        data_cf = DataCF,
        trie_cf = TrieCF,
        trie = Trie,
        hash_bytes = WCBytes,
        master_hash_bits = MHBits,
        serialization_schema = SSchema,
        threshold_fun = emqx_ds_lts:threshold_fun(ThresholdSpec),
        with_guid = WithGuid
    }.

drop(_ShardId, DBHandle, _GenId, _CFRefs, #s{data_cf = DataCF, trie_cf = TrieCF, trie = Trie}) ->
    emqx_ds_lts:destroy(Trie),
    ok = rocksdb:drop_column_family(DBHandle, DataCF),
    ok = rocksdb:drop_column_family(DBHandle, TrieCF),
    ok.

prepare_batch(_ShardId, S, Operations, _Options) ->
    _ = erase(?lts_persist_ops),
    OperationsCooked = cook(S, Operations, []),
    {ok, #{
        ?cooked_msg_ops => OperationsCooked,
        ?cooked_lts_ops => pop_lts_persist_ops()
    }}.

cook(_, [], Acc) ->
    lists:reverse(Acc);
cook(S, [{Timestamp, Msg = #message{topic = Topic}} | Rest], Acc) ->
    #s{trie = Trie, threshold_fun = TFun} = S,
    Tokens = words(Topic),
    {Static, Varying} = emqx_ds_lts:topic_key(Trie, TFun, Tokens),
    cook(S, Rest, [?cooked_msg_op(Timestamp, Static, Varying, serialize(S, Varying, Msg)) | Acc]);
cook(S, [{delete, #message_matcher{topic = Topic, timestamp = Timestamp}} | Rest], Acc) ->
    #s{trie = Trie} = S,
    case emqx_ds_lts:lookup_topic_key(Trie, words(Topic)) of
        {ok, {Static, Varying}} ->
            cook(S, Rest, [?cooked_msg_op(Timestamp, Static, Varying, ?cooked_delete) | Acc]);
        undefined ->
            %% Topic is unknown, nothing to delete.
            cook(S, Rest, Acc)
    end.

commit_batch(
    ShardId,
    S = #s{db = DB, trie_cf = TrieCF, data_cf = DataCF, trie = Trie, hash_bytes = HashBytes},
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
            fun(?cooked_msg_op(Timestamp, Static, Varying, Op)) ->
                MHB = master_hash_bits(S, Varying),
                StreamKey = mk_stream_key(Varying, Timestamp, MHB),
                MasterKey = mk_master_key(Static, StreamKey, MHB),
                case Op of
                    Payload when is_binary(Payload) ->
                        ok = rocksdb:batch_put(Batch, DataCF, MasterKey, Payload),
                        mk_index(Batch, DataCF, HashBytes, MHB, Static, Varying, StreamKey);
                    ?cooked_delete ->
                        ok = rocksdb:batch_delete(Batch, DataCF, MasterKey),
                        delete_index(Batch, DataCF, HashBytes, MHB, Static, Varying, Timestamp)
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
                {error, unrecoverable, {rocksdb, Reason}}
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
            (?cooked_msg_op(_Timestamp, _Static, _Varying, ?cooked_delete), Acc) ->
                Acc;
            (?cooked_msg_op(_Timestamp, Static, Varying, _ValBlob), Acc) ->
                maps:put({Static, Varying}, 1, Acc)
        end,
        #{},
        Payloads
    ),
    maps:keys(EventMap).

get_streams(_Shard, #s{trie = Trie}, TopicFilter, _StartTime) ->
    get_streams(Trie, TopicFilter).

get_delete_streams(_Shard, #s{trie = Trie}, TopicFilter, _StartTime) ->
    get_streams(Trie, TopicFilter).

make_iterator(_Shard, _State, _Stream, _TopicFilter, TS) when TS >= ?max_ts ->
    {error, unrecoverable, "Timestamp is too large"};
make_iterator(
    _Shard,
    S = #s{trie = Trie},
    #stream{static_index = StaticIdx},
    TopicFilter,
    StartTime
) ->
    ?tp_ignore_side_effects_in_prod(emqx_ds_storage_skipstream_lts_make_iterator, #{
        static_index => StaticIdx, topic_filter => TopicFilter, start_time => StartTime
    }),
    {ok, TopicStructure} = emqx_ds_lts:reverse_lookup(Trie, StaticIdx),
    CompressedTF = emqx_ds_lts:compress_topic(StaticIdx, TopicStructure, TopicFilter),
    MHB = master_hash_bits(S, CompressedTF),
    LastSK = dec_stream_key(stream_key(StartTime, 0, MHB), MHB),
    {ok, #it{
        static_index = StaticIdx,
        last_key = LastSK,
        compressed_tf = emqx_topic:join(CompressedTF)
    }}.

message_matcher(
    _Shard,
    #s{trie = Trie} = S,
    #it{static_index = StaticIdx, last_key = LastSK, compressed_tf = CTF0}
) ->
    {ok, TopicStructure} = emqx_ds_lts:reverse_lookup(Trie, StaticIdx),
    CTF = words(CTF0),
    MHB = master_hash_bits(S, CTF),
    TF = emqx_ds_lts:decompress_topic(TopicStructure, CTF),
    fun(MsgKey, #message{topic = Topic}) ->
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
                %% Timestamp stored in the iterator follows modulo
                %% 2^64 arithmetic, so in this context `?max_ts' means
                %% 0.
                (stream_key_ts(LastSK, MHB) =:= ?max_ts orelse SK > LastSK) andalso
                    emqx_topic:match(words(Topic), TF)
        end
    end.

unpack_iterator(_Shard, S, #it{static_index = StaticIdx, compressed_tf = CTF, last_key = LSK}) ->
    MHB = master_hash_bits(S, CTF),
    DSKey = mk_master_key(StaticIdx, LSK, MHB),
    {StaticIdx, words(CTF), DSKey, stream_key_ts(LSK, MHB)}.

scan_stream(Shard, S, StaticIdx, Varying, LastSeenKey, BatchSize, TMax, IsCurrent) ->
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
            {error, unrecoverable, "Invalid datastream key"};
        StreamKey ->
            {ok, OldIter#it{last_key = StreamKey}}
    end.

next(ShardId = {_DB, Shard}, S, ItSeed, BatchSize, TMax, IsCurrent) ->
    init_counters(),
    PerLevelIterators = init_iterators(S, ItSeed),
    %% ?tp(notice, skipstream_init_iters, #{it => It, its => Iterators}),
    try
        case start_next_loop(Shard, S, ItSeed, PerLevelIterators, BatchSize, TMax) of
            {ok, _, []} when not IsCurrent ->
                {ok, end_of_stream};
            Result ->
                Result
        end
    after
        free_iterators(PerLevelIterators),
        collect_counters(ShardId)
    end.

delete_next(Shard, S, It0, Selector, BatchSize, Now, IsCurrent) ->
    case next(Shard, S, It0, BatchSize, Now, IsCurrent) of
        {ok, It, KVs} ->
            batch_delete(S, It, Selector, KVs);
        Ret ->
            Ret
    end.

lookup_message(
    Shard,
    S = #s{db = DB, data_cf = CF, trie = Trie},
    #message_matcher{topic = Topic, timestamp = Timestamp}
) ->
    case emqx_ds_lts:lookup_topic_key(Trie, words(Topic)) of
        {ok, {StaticIdx, Varying}} ->
            MHB = master_hash_bits(S, Varying),
            SK = mk_stream_key(Varying, Timestamp, MHB),
            DSKey = mk_master_key(StaticIdx, SK, MHB),
            case rocksdb:get(DB, CF, DSKey, _ReadOpts = []) of
                {ok, Val} ->
                    {ok, TopicStructure} = emqx_ds_lts:reverse_lookup(Trie, StaticIdx),
                    Msg = deserialize(S, Val),
                    enrich(Shard, S, TopicStructure, DSKey, Msg);
                not_found ->
                    not_found;
                {error, Reason} ->
                    {error, unrecoverable, {rocksdb, Reason}}
            end;
        undefined ->
            not_found
    end.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

%% Loop context:
-record(ctx, {
    shard,
    %% Generation runtime state
    s,
    %% RocksDB iterators:
    iters,
    %% Cached topic structure for the static index:
    topic_structure,
    %% Maximum time:
    max_key,
    %% Effective master hash bitsize:
    master_hash_bits,
    %% Compressed topic filter, split into words:
    filter
}).

get_streams(Trie, TopicFilter) ->
    lists:map(
        fun({Static, _Varying}) ->
            #stream{static_index = Static}
        end,
        emqx_ds_lts:match_topics(Trie, TopicFilter)
    ).

%%%%%%%% Value (de)serialization %%%%%%%%%%

serialize(#s{serialization_schema = SSchema, with_guid = WithGuid}, Varying, Msg0) ->
    %% Replace original topic with the varying parts:
    Msg = Msg0#message{
        id =
            case WithGuid of
                true -> Msg0#message.id;
                false -> <<>>
            end,
        topic = emqx_topic:join(Varying)
    },
    emqx_ds_msg_serializer:serialize(SSchema, Msg).

enrich(#ctx{shard = Shard, s = S, topic_structure = TopicStructure}, DSKey, Msg0) ->
    enrich(Shard, S, TopicStructure, DSKey, Msg0).

enrich(
    Shard,
    #s{with_guid = WithGuid},
    TopicStructure,
    DSKey,
    Msg0
) ->
    Tokens = words(Msg0#message.topic),
    Topic = emqx_topic:join(emqx_ds_lts:decompress_topic(TopicStructure, Tokens)),
    Msg0#message{
        topic = Topic,
        id =
            case WithGuid of
                true -> Msg0#message.id;
                false -> fake_guid(Shard, DSKey)
            end
    }.

deserialize(
    #s{serialization_schema = SSchema},
    Blob
) ->
    emqx_ds_msg_serializer:deserialize(SSchema, Blob).

fake_guid(_Shard, DSKey) ->
    %% Both guid and MD5 are 16 bytes:
    crypto:hash(md5, DSKey).

%%%%%%%% Deletion %%%%%%%%%%

batch_delete(S = #s{db = DB, data_cf = CF, hash_bytes = HashBytes}, It, Selector, KVs) ->
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
        words(CompressedTF)
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
                {error, unrecoverable, {rocksdb, Reason}}
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
    do_init_iterators(S, Static, words(CompressedTF), 1).

do_init_iterators(S, Static, ['+' | TopicFilter], WildcardLevel) ->
    %% Ignore wildcard levels in the topic filter because it has no value to index '+'
    do_init_iterators(S, Static, TopicFilter, WildcardLevel + 1);
do_init_iterators(S, Static, [Constraint | TopicFilter], WildcardLevel) ->
    %% Create iterator for the index stream:
    #s{hash_bytes = HashBytes, db = DB, data_cf = DataCF} = S,
    Hash = hash_topic_level(HashBytes, Constraint),
    {ok, ItHandle} = rocksdb:iterator(DB, DataCF, get_key_range(Static, WildcardLevel, Hash)),
    It = #l{
        n = WildcardLevel,
        handle = ItHandle,
        hash = Hash
    },
    [It | do_init_iterators(S, Static, TopicFilter, WildcardLevel + 1)];
do_init_iterators(S, Static, [], _WildcardLevel) ->
    %% Create an iterator for the data stream:
    #s{db = DB, data_cf = DataCF} = S,
    Hash = <<>>,
    {ok, ItHandle} = rocksdb:iterator(DB, DataCF, get_key_range(Static, 0, Hash)),
    [
        #l{
            n = 0,
            handle = ItHandle,
            hash = Hash
        }
    ].

start_next_loop(
    Shard,
    S = #s{trie = Trie},
    ItSeed = #it{static_index = StaticIdx, last_key = LSK, compressed_tf = CompressedTF},
    PerLevelIterators,
    BatchSize,
    TMax
) ->
    %% cache it?
    TopicStructure = get_topic_structure(Trie, StaticIdx),
    MHB = master_hash_bits(S, CompressedTF),
    Ctx = #ctx{
        shard = Shard,
        s = S,
        iters = PerLevelIterators,
        topic_structure = TopicStructure,
        filter = words(CompressedTF),
        max_key = stream_key(TMax + 1, 0, MHB),
        master_hash_bits = MHB
    },
    next_loop(Ctx, ItSeed, BatchSize, {seek, inc_stream_key(LSK, MHB)}, []).

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

next_loop(_Ctx, It, 0, _Op, Acc) ->
    finalize_loop(It, Acc);
next_loop(Ctx, It0, BatchSize, Op, Acc) ->
    %% ?tp(notice, skipstream_loop, #{
    %%     ts => It0#it.ts, tf => It0#it.compressed_tf, bs => BatchSize, tmax => TMax, op => Op
    %% }),
    #ctx{s = S, max_key = MaxSK, master_hash_bits = MHB, iters = Iterators} = Ctx,
    #it{static_index = StaticIdx, compressed_tf = CompressedTF} = It0,
    %% Note: `next_step' function destructively updates RocksDB
    %% iterators in `ctx.iters' (they are handles, not values!),
    %% therefore a recursive call with the same arguments is not a
    %% bug.
    case next_step(S, StaticIdx, MHB, CompressedTF, Iterators, any, Op) of
        none ->
            %% ?tp(notice, skipstream_loop_result, #{r => none}),
            inc_counter(?DS_SKIPSTREAM_LTS_EOS),
            finalize_loop(It0, Acc);
        {seek, SK} when SK >= MaxSK ->
            %% ?tp(notice, skipstream_loop_result, #{r => seek_future, ts => TS}),
            inc_counter(?DS_SKIPSTREAM_LTS_FUTURE),
            finalize_loop(It0, Acc);
        {ok, SK, _Key, _Msg0} when SK >= MaxSK ->
            %% ?tp(notice, skipstream_loop_result, #{r => ok_future, ts => TS, key => _Key}),
            inc_counter(?DS_SKIPSTREAM_LTS_FUTURE),
            finalize_loop(It0, Acc);
        {seek, SK} ->
            %% ?tp(notice, skipstream_loop_result, #{r => seek, ts => TS}),
            It = It0#it{last_key = SK},
            next_loop(Ctx, It, BatchSize, {seek, SK}, Acc);
        {ok, SK, DSKey, Msg0} ->
            %% ?tp(notice, skipstream_loop_result, #{r => ok, ts => TS, key => Key}),
            Message = enrich(Ctx, DSKey, Msg0),
            It = It0#it{last_key = SK},
            next_loop(Ctx, It, BatchSize - 1, next, [{DSKey, Message} | Acc])
    end.

finalize_loop(It, Acc) ->
    {ok, It, lists:reverse(Acc)}.

next_step(
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
                            Msg0 = deserialize(S, Blob),
                            case emqx_topic:match(Msg0#message.topic, CompressedTF) of
                                true ->
                                    inc_counter(?DS_SKIPSTREAM_LTS_HIT),
                                    {ok, NextSK, Key, Msg0};
                                false ->
                                    %% Hash collision. Advance to the
                                    %% next timestamp:
                                    inc_counter(?DS_SKIPSTREAM_LTS_HASH_COLLISION),
                                    {seek, NextSK + 1}
                            end;
                        _ ->
                            %% This is index stream. Keep matching NextSK in other levels.
                            next_step(
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
                    {seek, NextSK}
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

mk_index(Batch, CF, HashBytes, MHB, Static, Varying, Timestamp) ->
    mk_index(Batch, CF, HashBytes, MHB, Static, Timestamp, 1, Varying).

mk_index(Batch, CF, HashBytes, MHB, Static, Timestamp, N, [TopicLevel | Varying]) ->
    Key = mk_key(Static, N, hash_topic_level(HashBytes, TopicLevel), Timestamp, MHB),
    ok = rocksdb:batch_put(Batch, CF, Key, <<>>),
    mk_index(Batch, CF, HashBytes, MHB, Static, Timestamp, N + 1, Varying);
mk_index(_Batch, _CF, _HashBytes, _MHB, _Static, _Timestamp, _N, []) ->
    ok.

delete_index(Batch, CF, HashBytes, MHB, Static, Varying, Timestamp) ->
    delete_index(Batch, CF, HashBytes, MHB, Static, Timestamp, 1, Varying).

delete_index(Batch, CF, HashBytes, MHB, Static, Timestamp, N, [TopicLevel | Varying]) ->
    Key = mk_key(Static, N, hash_topic_level(HashBytes, TopicLevel), Timestamp, MHB),
    ok = rocksdb:batch_delete(Batch, CF, Key),
    delete_index(Batch, CF, HashBytes, MHB, Static, Timestamp, N + 1, Varying);
delete_index(_Batch, _CF, _HashBytes, _MHB, _Static, _Timestamp, _N, []) ->
    ok.

%%%%%%%% Keys %%%%%%%%%%

get_key_range(StaticIdx, WildcardIdx, Hash) ->
    %% NOTE: Assuming `?max_ts` is not a valid message timestamp.
    [
        {iterate_lower_bound, mk_key(StaticIdx, WildcardIdx, Hash, 0, 0)},
        {iterate_upper_bound, mk_key(StaticIdx, WildcardIdx, Hash, ?max_ts, 0)}
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
    Tsz = byte_size(StaticIdx),
    Hsz = byte_size(Hash),
    case Key of
        <<StaticIdx:Tsz/binary, Idx:?wcb, Hash:Hsz/binary, StreamKey/binary>> ->
            StreamKey;
        _ ->
            false
    end.

-spec master_hash_bits(s(), [emqx_ds_lts:level()] | emqx_types:topic()) -> n_bits().
master_hash_bits(#s{master_hash_bits = MHB}, Varying) ->
    case Varying of
        %% Do not attach / expect master hash if it's a single topic stream.
        [] -> 0;
        <<>> -> 0;
        _NonEmpty -> MHB
    end.

-spec mk_key(emqx_ds_lts:static_key(), wildcard_idx(), wildcard_hash(), stream_key(), n_bits()) ->
    binary().
mk_key(StaticIdx, 0, <<>>, StreamKey, MHB) ->
    mk_master_key(StaticIdx, StreamKey, MHB);
mk_key(StaticIdx, N, Hash, StreamKey, MHB) when N > 0 ->
    mk_index_key(StaticIdx, N, Hash, StreamKey, MHB).

mk_master_key(StaticIdx, StreamKey, MHB) ->
    %% Data stream is identified by wildcard level = 0
    <<StaticIdx/binary, 0:?wcb, StreamKey:(?tsb + MHB)>>.

mk_index_key(StaticIdx, N, Hash, StreamKey, MHB) ->
    %% Index stream:
    <<StaticIdx/binary, N:?wcb, Hash/binary, StreamKey:(?tsb + MHB)>>.

-spec mk_stream_key([emqx_ds_lts:level()], ts(), n_bits()) -> stream_key().
mk_stream_key(_Varying, Timestamp, 0) ->
    Timestamp;
mk_stream_key(Varying, Timestamp, MHB) ->
    Hash = first_n_bits(MHB, hash_topic_levels(Varying)),
    stream_key(Timestamp, Hash, MHB).

stream_key(Timestamp, Hash, MHB) ->
    (Timestamp bsl MHB) bor Hash.

inc_stream_key(StreamKey, MHB) ->
    case stream_key_ts(StreamKey, MHB) of
        ?max_ts -> 0;
        TS when TS < ?max_ts -> StreamKey + 1
    end.

dec_stream_key(StreamKey, MHB) ->
    case stream_key_ts(StreamKey, MHB) of
        0 when StreamKey =:= 0 -> stream_key(?max_ts, 0, MHB);
        TS when TS =< ?max_ts -> StreamKey - 1
    end.

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
    emqx_ds_storage_layer:shard_id(), pos_integer(), rocksdb:db_handle(), rocksdb:cf_handle()
) -> emqx_ds_lts:trie().
restore_trie(Shard, StaticIdxBytes, DB, CF) ->
    PersistCallback = fun(Key, Val) ->
        push_lts_persist_op(Key, Val),
        ok
    end,
    {ok, IT} = rocksdb:iterator(DB, CF, []),
    try
        Dump = read_persisted_trie(IT, rocksdb:iterator_move(IT, first)),
        TrieOpts = #{
            persist_callback => PersistCallback,
            static_key_bytes => StaticIdxBytes,
            reverse_lookups => true
        },
        Trie = emqx_ds_lts:trie_restore(TrieOpts, Dump),
        notify_new_streams(Shard, Trie, Dump),
        Trie
    after
        rocksdb:iterator_close(IT)
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

read_persisted_trie(IT, {ok, KeyB, ValB}) ->
    [
        {binary_to_term(KeyB), binary_to_term(ValB)}
        | read_persisted_trie(IT, rocksdb:iterator_move(IT, next))
    ];
read_persisted_trie(_IT, {error, invalid_iterator}) ->
    [].

%%%%%%%% Column families %%%%%%%%%%

%% @doc Generate a column family ID for the MQTT messages
-spec data_cf(emqx_ds_storage_layer:gen_id()) -> [char()].
data_cf(GenId) ->
    "emqx_ds_storage_skipstream_lts_data" ++ integer_to_list(GenId).

%% @doc Generate a column family ID for the trie
-spec trie_cf(emqx_ds_storage_layer:gen_id()) -> [char()].
trie_cf(GenId) ->
    "emqx_ds_storage_skipstream_lts_trie" ++ integer_to_list(GenId).

%%%%%%%% Topic encoding %%%%%%%%%%

% words(L) when is_list(L) -> L;
words(<<>>) -> [];
words(Bin) -> emqx_topic:words(Bin).

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

-ifdef(TEST).

inc_dec_test_() ->
    Numbers = [0, 1, 100, ?max_ts - 1, ?max_ts],
    [
        ?_assertEqual(
            stream_key(N, _Hash = 0, MHB),
            dec_stream_key(inc_stream_key(stream_key(N, 0, MHB), MHB), MHB)
        )
     || N <- Numbers,
        MHB <- [0, 32]
    ].

dec_inc_test_() ->
    Numbers = [0, 1, 100, ?max_ts - 1, ?max_ts],
    [
        ?_assertEqual(
            stream_key(N, _Hash = 0, MHB),
            inc_stream_key(dec_stream_key(stream_key(N, 0, MHB), MHB), MHB)
        )
     || N <- Numbers,
        MHB <- [0, 32]
    ].

-endif.
