%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    with_guid :: boolean(),
    gen_lts_s :: emqx_ds_gen_skipstream_lts:s()
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
    Schema1 = maps:merge(Defaults, Schema0),
    Schema =
        case DBOpts of
            #{store_kv := true} ->
                Schema1#{serialization_schema => blob, store_kv => true};
            _ ->
                Schema1
        end,
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
    GenSkipstreamS = emqx_ds_gen_skipstream_lts:open(#{
        db_shard => ShardId,
        db_handle => DBHandle,
        data_cf => DataCF,
        trie => Trie,
        serialization_schema => SSchema,
        stream_key_size => (?tsb + MHBits) div 8,
        get_topic => fun topic_of_message/1,
        wildcard_hash_bytes => WCBytes
    }),
    #s{
        db = DBHandle,
        data_cf = DataCF,
        trie_cf = TrieCF,
        trie = Trie,
        hash_bytes = WCBytes,
        master_hash_bits = MHBits,
        serialization_schema = SSchema,
        threshold_fun = emqx_ds_lts:threshold_fun(ThresholdSpec),
        with_guid = WithGuid,
        gen_lts_s = GenSkipstreamS
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
    S = #s{db = DB, trie_cf = TrieCF, trie = Trie, gen_lts_s = GS},
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
                case Op of
                    Payload when is_binary(Payload) ->
                        emqx_ds_gen_skipstream_lts:batch_put(
                            GS, Batch, Static, Varying, StreamKey, Payload
                        );
                    ?cooked_delete ->
                        emqx_ds_gen_skipstream_lts:batch_delete(
                            GS, Batch, Static, Varying, StreamKey
                        )
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
            (?cooked_msg_op(_Timestamp, _Static, _Varying, ?cooked_delete), Acc) ->
                Acc;
            (?cooked_msg_op(_Timestamp, Static, _Varying, _ValBlob), Acc) ->
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

make_iterator(_Shard, _State, _Stream, _TopicFilter, TS) when TS >= ?max_ts ->
    ?err_unrec("Timestamp is too large");
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
                %% Timestamp stored in the iterator follows modulo
                %% 2^64 arithmetic, so in this context `?max_ts' means
                %% 0.
                (stream_key_ts(LastSK, MHB) =:= ?max_ts orelse SK > LastSK) andalso
                    emqx_topic:match(TopicTokens, TF)
        end
    end.

unpack_iterator(_Shard, S = #s{trie = Trie}, #it{
    static_index = StaticIdx, compressed_tf = CTF, last_key = LSK
}) ->
    MHB = master_hash_bits(S, CTF),
    DSKey = mk_master_key(StaticIdx, LSK, MHB),
    TopicFilter = emqx_ds_lts:decompress_topic(get_topic_structure(Trie, StaticIdx), words(CTF)),
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
            Tokens = words(Topic),
            {ok, TopicStructure} = emqx_ds_lts:reverse_lookup(Trie, StaticIdx),
            CT = emqx_ds_lts:compress_topic(StaticIdx, TopicStructure, Tokens),
            {ok, #match_ctx{compressed_topic = CT, stream_key = SK}}
    end.

iterator_match_context(_Shard, GenData, Iterator) ->
    #it{last_key = LastSK, compressed_tf = CTF} = Iterator,
    MHB = master_hash_bits(GenData, CTF),
    CTFTokens = words(CTF),
    fun(#match_ctx{compressed_topic = CT, stream_key = SK}) ->
        %% Timestamp stored in the iterator follows modulo
        %% 2^64 arithmetic, so in this context `?max_ts' means
        %% 0.
        (stream_key_ts(LastSK, MHB) =:= ?max_ts orelse SK > LastSK) andalso
            emqx_topic:match(CT, CTFTokens)
    end.

next(_ShardId, S, It, BatchSize, TMax, IsCurrent) ->
    #s{gen_lts_s = GS, master_hash_bits = MHB} = S,
    #it{static_index = Static, last_key = LSK0, compressed_tf = CompTF} = It,
    Varying = words(CompTF),
    StreamKeySize = ?tsb + master_hash_bits(S, Varying),
    Interval =
        case LSK0 of
            ?max_ts ->
                {'[', <<0:StreamKeySize>>, <<TMax:?tsb/big>>};
            _ ->
                {'(', <<LSK0:StreamKeySize>>, <<TMax:?tsb/big>>}
        end,
    Result = emqx_ds_gen_skipstream_lts:fold(
        GS,
        Static,
        Varying,
        Interval,
        BatchSize,
        [],
        fun(TopicStructure, DSKey, CompressedTopic, Msg0, Acc) ->
            Msg = enrich(S, DSKey, TopicStructure, CompressedTopic, Msg0),
            [{DSKey, Msg} | Acc]
        end
    ),
    %% Note: in this iteration of skipstream layout we store stream
    %% key as integer. We have to extract it from the stream key.
    case Result of
        {ok, _, []} when not IsCurrent ->
            {ok, end_of_stream};
        {ok, LSK, Batch} ->
            case LSK of
                <<LSKInt:StreamKeySize>> ->
                    {ok, It#it{last_key = LSKInt}, lists:reverse(Batch)};
                _ ->
                    error(#{mhb => MHB, s => S, var => Varying, k => LSK})
            end;
        {error, _, _} = Err ->
            Err
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
    S = #s{trie = Trie, gen_lts_s = GS},
    Topic,
    Timestamp
) ->
    maybe
        {ok, {Static, Varying}} ?= emqx_ds_lts:lookup_topic_key(Trie, Topic),
        MHB = master_hash_bits(S, Varying),
        StreamKey = mk_stream_key(Varying, Timestamp, MHB),
        {ok, DSKey, Msg0} ?= emqx_ds_gen_skipstream_lts:lookup_message(GS, Static, StreamKey),
        {ok, TopicStructure} = emqx_ds_lts:reverse_lookup(Trie, Static),
        {ok, enrich(S, DSKey, TopicStructure, words(Msg0#message.topic), Msg0)}
    end.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

enrich(#s{with_guid = WithGuid}, DSKey, TopicStructure, CompressedTopic, Msg0) ->
    Topic = emqx_topic:join(emqx_ds_lts:decompress_topic(TopicStructure, CompressedTopic)),
    Msg0#message{
        topic = Topic,
        id =
            case WithGuid of
                true -> Msg0#message.id;
                false -> fake_guid(DSKey)
            end
    }.

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

fake_guid(DSKey) ->
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

%%%%%%%% Keys %%%%%%%%%%

-spec match_stream_key(emqx_ds_lts:static_key(), binary()) ->
    stream_key() | false.
match_stream_key(StaticIdx, Key) ->
    case match_key_prefix(StaticIdx, 0, <<>>, Key) of
        false ->
            false;
        StreamKey ->
            decode_stream_key(StreamKey)
    end.

%% -spec match_stream_key(emqx_ds_lts:static_key(), wildcard_idx(), wildcard_hash(), binary()) ->
%%     stream_key() | false.
%% match_stream_key(StaticIdx, WildcardIdx, Hash, Key) ->
%%     case match_key_prefix(StaticIdx, WildcardIdx, Hash, Key) of
%%         false ->
%%             false;
%%         StreamKey ->
%%             decode_stream_key(StreamKey)
%%     end.

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
    <<Timestamp:?tsb/big>>;
mk_stream_key(Varying, Timestamp, MHB) ->
    Hash = hash_topic_levels(Varying),
    <<Timestamp:?tsb/big, Hash:(MHB div 8)/binary>>.

stream_key(Timestamp, Hash, MHB) ->
    (Timestamp bsl MHB) bor Hash.

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

hash_topic_levels(Varying) when is_list(Varying) ->
    erlang:md5_final(hash_topic_levels(Varying, erlang:md5_init())).

hash_topic_levels([TopicLevel | Rest], HashCtx0) ->
    HashCtx = erlang:md5_update(HashCtx0, topic_level_bin(TopicLevel)),
    hash_topic_levels(Rest, HashCtx);
hash_topic_levels([], HashCtx) ->
    HashCtx.

first_n_bytes(NBytes, Hash) ->
    <<Part:NBytes/binary, _/binary>> = Hash,
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

words(<<>>) -> [];
words(Bin) when is_binary(Bin) -> emqx_topic:words(Bin).

%%%%%%%% Column families %%%%%%%%%%

%% @doc Generate a column family ID for the MQTT messages
-spec data_cf(emqx_ds_storage_layer:gen_id()) -> [char()].
data_cf(GenId) ->
    "emqx_ds_storage_skipstream_lts_data" ++ integer_to_list(GenId).

%% @doc Generate a column family ID for the trie
-spec trie_cf(emqx_ds_storage_layer:gen_id()) -> [char()].
trie_cf(GenId) ->
    "emqx_ds_storage_skipstream_lts_trie" ++ integer_to_list(GenId).

%%%%%%%% Counters %%%%%%%%%%

topic_of_message(#message{topic = Topic}) ->
    words(Topic).

%%================================================================================
%% Tests
%%================================================================================
