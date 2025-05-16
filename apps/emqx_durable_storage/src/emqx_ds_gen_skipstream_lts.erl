%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Common routines for implementing storage layouts based on
%% "skip-stream" indexing.
-module(emqx_ds_gen_skipstream_lts).

%% API:
-export([
    open/1,
    drop/1,
    %% Trie management:
    restore_trie/4,
    pop_lts_persist_ops/0,
    notify_new_streams/3,
    copy_previous_trie/3,
    %% DB key management:
    mk_key/4,
    stream_key/2,
    match_key/4,
    %% Update the table:
    batch_put/6,
    batch_delete/5,

    %% Reading:
    lookup_message/3,
    fold/7
]).

-export_type([
    fold_fun/1,
    s/0,
    stream_key/0
]).

%% inline small functions:
-compile(inline).

-include("emqx_ds.hrl").
-include("emqx_ds_metrics.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

%% Width of the wildcard layer, in bits:
-define(wcb, 16).

-type wildcard_idx() :: 0..16#ffff.
-type wildcard_hash() :: binary().

%% Deserialized value:
-type deser_value() :: term().

%% Key that identifies a message in the stream. It's the suffix of
%% `emqx_ds:message_key'.
-type stream_key() :: binary().

%% Runtime state:
-record(s, {
    dbshard :: {emqx_ds:db(), emqx_ds:shard()},
    db :: rocksdb:db_handle(),
    data_cf :: rocksdb:cf_handle(),
    trie_cf :: rocksdb:cf_handle(),
    trie :: emqx_ds_lts:trie(),
    serialization_schema :: emqx_ds_msg_serializer:schema(),
    %% A callback that decomposes stream key and value to varying
    %% parts of the topic (used to check for hash collisions),
    %% timestamp and payload:
    decompose :: fun((stream_key(), _) -> emqx_ds:ttv()),
    %% Number of bytes per wildcard topic level:
    hash_bytes :: pos_integer()
}).

-opaque s() :: #s{}.

-define(COUNTERS, [
    ?DS_SKIPSTREAM_LTS_SEEK,
    ?DS_SKIPSTREAM_LTS_NEXT,
    ?DS_SKIPSTREAM_LTS_HASH_COLLISION,
    ?DS_SKIPSTREAM_LTS_HIT,
    ?DS_SKIPSTREAM_LTS_MISS,
    ?DS_SKIPSTREAM_LTS_FUTURE,
    ?DS_SKIPSTREAM_LTS_EOS
]).

%% Interval that constrains stream keys visited by the fold. The
%% interval is always open on the upper endpoint, but it can be open
%% (if the first element of tuple is atom '(') or half-closed '[' on
%% the lower endpoint.
-type fold_interval() :: {
    '[' | '(',
    stream_key() | '-infinity',
    stream_key() | 'infinity'
}.

-type fold_fun(Acc) :: fun(
    (
        emqx_ds_lts:learned_structure(),
        emqx_ds:message_key(),
        emqx_ds:topic(),
        emqx_ds:time(),
        _Value,
        Acc
    ) -> Acc
).

%% Fold loop context:
-record(fold_ctx, {
    %% Generation runtime state:
    s,
    %% Fold function:
    f,
    %% RocksDB iterators:
    iters,
    %% Topic filter:
    %%   Cached topic structure for the static index:
    topic_structure,
    %%   Compressed topic filter, split into words:
    filter,
    %%   Static index:
    static :: emqx_ds_lts:static_key(),
    %% Lower and upper endpoint of the stream key _open_ interval:
    lower_endpoint :: stream_key() | '-infinity',
    upper_endpoint :: stream_key() | infinity
}).

%% Level iterator:
-record(l, {
    n :: wildcard_idx(),
    handle :: rocksdb:itr_handle(),
    hash :: wildcard_hash()
}).

-type l() :: #l{}.

-ifdef(DEBUG2).
-include_lib("snabbkaffe/include/trace.hrl").
-define(dbg(K, A), ?tp(K, A)).
-else.
-define(dbg(K, A), ok).
-endif.

-define(lts_persist_ops, emqx_ds_storage_gen_skipstream_lts_ops).

%%================================================================================
%% API functions
%%================================================================================

%%%% Trie

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

-spec pop_lts_persist_ops() -> emqx_ds_lts:dump().
pop_lts_persist_ops() ->
    case erlang:erase(?lts_persist_ops) of
        undefined ->
            [];
        L when is_list(L) ->
            L
    end.

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

%%%% Data

%% @doc Initialize the module.
-spec open(#{
    db_shard := {emqx_ds:db(), emqx_ds:shard()},
    db_handle := rocksdb:db_handle(),
    data_cf := rocksdb:cf_handle(),
    trie_cf := rocksdb:cf_handle(),
    trie := emqx_ds_lts:trie(),
    serialization_schema := emqx_ds_msg_serializer:schema(),
    decompose := fun((stream_key(), deser_value()) -> emqx_ds:topic()),
    wildcard_hash_bytes := pos_integer()
}) ->
    s().
open(
    #{
        db_shard := DBShard,
        db_handle := DBHandle,
        data_cf := DataCF,
        trie_cf := TrieCF,
        trie := Trie,
        serialization_schema := SerSchema,
        decompose := Decompose,
        wildcard_hash_bytes := HashBytes
    }
) ->
    #s{
        dbshard = DBShard,
        db = DBHandle,
        data_cf = DataCF,
        trie_cf = TrieCF,
        trie = Trie,
        serialization_schema = SerSchema,
        decompose = Decompose,
        hash_bytes = HashBytes
    }.

-spec drop(s()) -> ok.
drop(#s{trie = Trie, db = DBHandle, data_cf = DataCF, trie_cf = TrieCF}) ->
    emqx_ds_lts:destroy(Trie),
    ok = rocksdb:drop_column_family(DBHandle, DataCF),
    ok = rocksdb:drop_column_family(DBHandle, TrieCF),
    ok.

%% @doc Low-level API for writing data and indexes to the column
%% family via RocksDB batch.
%%
%% It's assumed that the caller already made an LTS trie lookup for
%% the topic, and got static and varying parts of the key.
-spec batch_put(
    s(),
    rocksdb:batch_handle(),
    emqx_ds_lts:static_key(),
    emqx_ds_lts:varying(),
    stream_key(),
    binary()
) -> ok.
batch_put(
    #s{data_cf = DataCF, hash_bytes = HashBytes}, Batch, Static, Varying, StreamKey, SerializedValue
) ->
    MasterKey = mk_master_key(Static, StreamKey),
    ok = rocksdb:batch_put(Batch, DataCF, MasterKey, SerializedValue),
    mk_index(Batch, DataCF, HashBytes, Static, Varying, StreamKey).

%% @doc Low-level API for deleting data and indexes. Similar to
%% `batch_put'.
-spec batch_delete(
    s(), rocksdb:batch_handle(), emqx_ds_lts:static_key(), emqx_ds_lts:varying(), stream_key()
) -> ok.
batch_delete(#s{data_cf = DataCF, hash_bytes = HashBytes}, Batch, Static, Varying, StreamKey) ->
    delete_index(Batch, DataCF, HashBytes, Static, Varying, StreamKey),
    MasterKey = mk_master_key(Static, StreamKey),
    ok = rocksdb:batch_delete(Batch, DataCF, MasterKey).

%% @doc Compose a RocksDB key for data or index from the parts.
-spec mk_key(emqx_ds_lts:static_key(), wildcard_idx(), wildcard_hash(), stream_key()) ->
    binary().
mk_key(StaticIdx, 0, <<>>, StreamKey) ->
    mk_master_key(StaticIdx, StreamKey);
mk_key(StaticIdx, N, WildcardHash, StreamKey) when N > 0 ->
    mk_index_key(StaticIdx, N, WildcardHash, StreamKey).

%% @doc Extract stream key from the DSKey. Crashes on mismatch.
-spec stream_key(emqx_ds_lts:static_key(), emqx_ds:message_key()) ->
    stream_key().
stream_key(Static, DSKey) ->
    TSz = byte_size(Static),
    <<Static:TSz/binary, 0:?wcb, StreamKey/binary>> = DSKey,
    StreamKey.

%% @doc Extract a stream key from RocksDB key
-spec match_key(emqx_ds_lts:static_key(), wildcard_idx(), wildcard_hash(), emqx_ds:message_key()) ->
    stream_key() | false.
match_key(StaticIdx, 0, <<>>, Key) ->
    TSz = byte_size(StaticIdx),
    case Key of
        <<StaticIdx:TSz/binary, 0:?wcb, StreamKey/binary>> ->
            StreamKey;
        _ ->
            false
    end;
match_key(StaticIdx, Idx, Hash, Key) when Idx > 0 ->
    TSz = byte_size(StaticIdx),
    Hsz = byte_size(Hash),
    case Key of
        <<StaticIdx:TSz/binary, Idx:?wcb, Hash:Hsz/binary, StreamKey/binary>> ->
            StreamKey;
        _ ->
            false
    end.

%% @doc Lookup a message by LTS static index and stream key.
%%
%% Note: the returned message is *deserialized*, but no further
%% transformation or enrichment is performed.
-spec lookup_message(s(), emqx_ds_lts:static_key(), stream_key()) ->
    {ok, emqx_ds:message_key(), deser_value()}
    | undefined
    | emqx_ds:error(_).
lookup_message(
    S = #s{db = DB, data_cf = CF},
    Static,
    StreamKey
) ->
    DSKey = mk_master_key(Static, StreamKey),
    case rocksdb:get(DB, CF, DSKey, []) of
        {ok, Bin} ->
            {ok, DSKey, deserialize(S, Bin)};
        not_found ->
            undefined;
        {error, Reason} ->
            ?err_unrec({rocksdb, Reason})
    end.

%% @doc Iterate over messages with a given topic filter.
%%
%% @param S State
%%
%% @param Static Static topic index, as returned by the LTS trie
%% lookup for the topic-filter
%%
%% @param Varying Varying topic parts, as returned by the LTS trie
%% lookup for the topic-filter
%%
%% @param Interval Interval that restricts the range of stream keys in
%% the topic filter. It's a 3-tuple
%%
%% - Second element of the tuple is the lower endpoint of the stream
%% key interval
%%
%% - Third element of the tuple is the upper endpoint of the stream
%% key interval. Note: interval is always open on the upper endpoint,
%% i.e. upper endpoint is *excluded* from the iteration.
%%
%% - First element of the tuple is an atom '[' or '('. '[' signifies
%% half-closed interval (i.e. lower endpoint is included in the scan),
%% and '(' signifies open interval.
%%
%% @param BatchSize Stop iteration after visiting so many elements
%%
%% @param Acc0 Initial value of the accumulator
%%
%% @param Fun Fold function
-spec fold(
    s(),
    emqx_ds_lts:static_key(),
    emqx_ds_lts:varying(),
    fold_interval(),
    _BatchSize :: pos_integer(),
    Acc,
    fold_fun(Acc)
) -> {ok, stream_key(), Acc} | emqx_ds:error(_).
fold(
    S = #s{trie = Trie, dbshard = DBShard},
    Static,
    Varying,
    Interval = {LEType, LowerEndpoint, UpperEndpoint},
    BatchSize,
    Acc0,
    Fun
) ->
    init_counters(),
    {Snapshot, PerLevelIterators} = init_iterators(S, Static, Varying, Interval),
    %% ?tp(notice, skipstream_init_iters, #{it => It, its => Iterators}),
    try
        TopicStructure = get_topic_structure(Trie, Static),
        Ctx = #fold_ctx{
            s = S,
            f = Fun,
            iters = PerLevelIterators,
            topic_structure = TopicStructure,
            filter = Varying,
            static = Static,
            %% Note on half-closed intervals: lower_endpoint field is
            %% compared < against visited stream keys. Since every
            %% stream key is a binary, using an atom shunts this
            %% comparison: according to Erlang term order binary is
            %% greater than atom, so every key is accepted.
            lower_endpoint =
                case LEType of
                    '(' ->
                        LowerEndpoint;
                    '[' ->
                        '-infinity'
                end,
            upper_endpoint = UpperEndpoint
        },
        StartPoint =
            case LowerEndpoint of
                '-infinity' ->
                    <<>>;
                _ when is_binary(LowerEndpoint) ->
                    LowerEndpoint
            end,
        fold_loop(Ctx, StartPoint, BatchSize, {seek, StartPoint}, Acc0)
    catch
        EC:Err:Stack ->
            %% FIXME:
            ?err_unrec({EC, Err, Stack})
    after
        free_iterators(PerLevelIterators),
        _ = rocksdb:release_snapshot(Snapshot),
        collect_counters(DBShard)
    end.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%===============================================================================

%%%% Trie

push_lts_persist_op(Key, Val) ->
    case erlang:get(?lts_persist_ops) of
        undefined ->
            erlang:put(?lts_persist_ops, [{Key, Val}]);
        L when is_list(L) ->
            erlang:put(?lts_persist_ops, [{Key, Val} | L])
    end.

read_persisted_trie(It, {ok, KeyB, ValB}) ->
    [
        {binary_to_term(KeyB), binary_to_term(ValB)}
        | read_persisted_trie(It, rocksdb:iterator_move(It, next))
    ];
read_persisted_trie(_It, {error, invalid_iterator}) ->
    [].

%%%% Fold %%%%

%% @doc Init RockDB iterators for each non-wildcard level of indices,
%% order is important: e.g. [L1, L2, L3, L0].
-spec init_iterators(s(), emqx_ds_lts:static_key(), emqx_ds:topic_filter(), fold_interval()) ->
    {rocksdb:snapshot_handle(), [l()]}.
init_iterators(S, Static, CompressedTF, Interval) ->
    {ok, Snapshot} = rocksdb:snapshot(S#s.db),
    {Snapshot, do_init_iterators(S, Snapshot, Static, CompressedTF, Interval, 1)}.

do_init_iterators(S, Snapshot, Static, ['#'], Interval, WildcardLevel) ->
    do_init_iterators(S, Snapshot, Static, [], Interval, WildcardLevel);
do_init_iterators(S, Snapshot, Static, ['+' | TopicFilter], Interval, WildcardLevel) ->
    %% Ignore wildcard levels in the topic filter because it has no value to index '+'
    do_init_iterators(S, Snapshot, Static, TopicFilter, Interval, WildcardLevel + 1);
do_init_iterators(S, Snapshot, Static, [Constraint | TopicFilter], Interval, WildcardLevel) ->
    %% Create iterator for the index stream:
    #s{db = DB, data_cf = DataCF, hash_bytes = HashBytes} = S,
    Hash = hash_topic_level(HashBytes, Constraint),
    {ok, ItHandle} = rocksdb:iterator(
        DB, DataCF, get_key_range(S, Snapshot, Static, Interval, WildcardLevel, Hash)
    ),
    It = #l{
        n = WildcardLevel,
        handle = ItHandle,
        hash = Hash
    },
    [It | do_init_iterators(S, Snapshot, Static, TopicFilter, Interval, WildcardLevel + 1)];
do_init_iterators(S, Snapshot, Static, [], Interval, _WildcardLevel) ->
    %% Create an iterator for the data stream:
    #s{db = DB, data_cf = DataCF} = S,
    {ok, ItHandle} = rocksdb:iterator(
        DB, DataCF, get_key_range(S, Snapshot, Static, Interval, 0, <<>>)
    ),
    [
        #l{
            n = 0,
            handle = ItHandle,
            hash = <<>>
        }
    ].

get_key_range(_S, Snapshot, Static, {_, Lower, Upper}, WildcardLevel, Hash) ->
    %% Note: rocksdb treats lower bound as inclusive
    L =
        case Lower of
            '-infinity' -> <<>>;
            _ -> Lower
        end,
    %% Note: rocksdb treats upper bound as non-inclusive
    U =
        case Upper of
            'infinity' ->
                stream_limit(Static, WildcardLevel, Hash);
            _ ->
                mk_key(Static, WildcardLevel, Hash, Upper)
        end,
    [
        {iterate_lower_bound, mk_key(Static, WildcardLevel, Hash, L)},
        {iterate_upper_bound, U},
        {snapshot, Snapshot}
    ].

%% @doc Calculate `iterate_upper_bound' of the rocksdb iterator that
%% covers _all_ values in the stream.
-spec stream_limit(emqx_ds_lts:static_key(), wildcard_idx(), wildcard_hash()) -> binary().
stream_limit(Static, 0, _) ->
    %% Data stream:
    mk_key(Static, 1, <<>>, <<>>);
stream_limit(_, WcLevel, _) when WcLevel >= 16#ffff ->
    error(too_many_wildcard_levels);
stream_limit(Static, WcLevel, Hash) ->
    Sz = bit_size(Hash),
    <<HashInt:Sz>> = Hash,
    case 1 bsl Sz - 1 of
        HashInt ->
            %% This happens to be the maximum hash of the given size.
            %% We increase the wildcard index instead:
            mk_key(Static, WcLevel + 1, <<>>, <<>>);
        _ ->
            mk_key(Static, WcLevel, <<(HashInt + 1):Sz>>, <<>>)
    end.

free_iterators(Its) ->
    lists:foreach(
        fun(#l{handle = IH}) ->
            ok = rocksdb:iterator_close(IH)
        end,
        Its
    ).

fold_loop(_Ctx, LastKey, 0, _Op, Acc) ->
    {ok, LastKey, Acc};
fold_loop(Ctx, SK0, BatchSize, Op, Acc0) ->
    ?dbg(skipstream_loop, #{
        sk => SK0, tf => Ctx#fold_ctx.filter, bs => BatchSize, op => Op
    }),
    #fold_ctx{
        s = S,
        f = Fun,
        iters = Iterators,
        static = StaticIdx,
        filter = CompressedTF,
        upper_endpoint = UpperEndpoint
    } = Ctx,
    %% Note: `next_step' function destructively updates RocksDB
    %% iterators in `ctx.iters' (they are handles, not values!),
    %% therefore a recursive call with the same arguments is not a
    %% bug.
    case fold_step(S, StaticIdx, CompressedTF, Iterators, any, Op) of
        none ->
            ?dbg(skipstream_loop_result, #{r => none}),
            inc_counter(?DS_SKIPSTREAM_LTS_EOS),
            {ok, SK0, Acc0};
        {seek, SK} when is_binary(UpperEndpoint), SK >= UpperEndpoint ->
            %% Iteration reached the upper endpoint:
            ?dbg(skipstream_loop_seek_upper, #{to => SK, upper => UpperEndpoint}),
            {ok, SK0, Acc0};
        {seek, SK} ->
            ?dbg(skipstream_loop_seek, #{r => seek, sk => SK}),
            fold_loop(Ctx, SK, BatchSize, {seek, SK}, Acc0);
        {ok, SK, CompressedTopic, DSKey, Timestamp, Val} ->
            ?dbg(skipstream_loop_result, #{r => ok, sk => SK, key => DSKey}),
            case Ctx#fold_ctx.lower_endpoint < SK of
                true ->
                    Acc = Fun(
                        Ctx#fold_ctx.topic_structure, DSKey, CompressedTopic, Timestamp, Val, Acc0
                    ),
                    fold_loop(Ctx, SK, BatchSize - 1, next, Acc);
                false ->
                    fold_loop(Ctx, SK, BatchSize, next, Acc0)
            end
    end.

fold_step(
    S = #s{decompose = Decompose},
    StaticIdx,
    CompressedTF,
    [#l{hash = Hash, handle = IH, n = Level} | Iterators],
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
                rocksdb:iterator_move(IH, {seek, mk_key(StaticIdx, Level, Hash, SK)})
        end,
    case Result of
        {error, invalid_iterator} ->
            ?dbg(?MODULE_STRING "_step_limit", #{
                level => Level
            }),
            none;
        {ok, Key, Blob} ->
            case match_key(StaticIdx, Level, Hash, Key) of
                false ->
                    %% This should not happen, since we set boundaries
                    %% to the iterators, and overflow to a different
                    %% key prefix should be caught by the previous
                    %% clause:
                    none;
                NextSK when ExpectedSK =:= any; NextSK =:= ExpectedSK ->
                    %% We found a key that corresponds to the stream key
                    %% (timestamp) we expect.
                    ?dbg(?MODULE_STRING "_step_hit", #{
                        next => NextSK, expected => ExpectedSK, level => Level
                    }),
                    case Iterators of
                        [] ->
                            %% Last one in PerLevelIterators is the one and the only one data stream.
                            0 = Level,
                            %% This is data stream as well. Check
                            %% message for hash collisions and return
                            %% value:
                            Val = deserialize(S, Blob),
                            {CompressedTopic, Timestamp, Payload} = Decompose(NextSK, Val),
                            case emqx_topic:match(CompressedTopic, CompressedTF) of
                                true ->
                                    inc_counter(?DS_SKIPSTREAM_LTS_HIT),
                                    {ok, NextSK, CompressedTopic, Key, Timestamp, Payload};
                                false ->
                                    %% Hash collision. Advance to the
                                    %% next key:
                                    inc_counter(?DS_SKIPSTREAM_LTS_HASH_COLLISION),
                                    %% FIXME: this part is different
                                    %% than in the original file, make
                                    %% sure this path is covered by
                                    %% tests.
                                    next
                            end;
                        _ ->
                            %% This is index stream. Keep matching NextSK in other levels.
                            fold_step(
                                S, StaticIdx, CompressedTF, Iterators, NextSK, {seek, NextSK}
                            )
                    end;
                NextSK when NextSK > ExpectedSK, Level > 0 ->
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
                        n => Level
                    })
            end
    end.

%% Misc.

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

%% Counters

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

%% Key

mk_master_key(StaticIdx, StreamKey) ->
    %% Data stream is identified by wildcard level = 0
    <<StaticIdx/binary, 0:?wcb, StreamKey/binary>>.

mk_index_key(StaticIdx, WildcardLevel, Hash, StreamKey) ->
    <<StaticIdx/binary, WildcardLevel:?wcb/big, Hash/binary, StreamKey/binary>>.

%% Hashing

hash_topic_level(HashBytes, TopicLevel) ->
    <<Ret:HashBytes/binary, _/binary>> = hash_topic_level(TopicLevel),
    Ret.

hash_topic_level(TopicLevel) ->
    erlang:md5(TopicLevel).

%% Serialization

deserialize(
    #s{serialization_schema = SSchema},
    Blob
) ->
    emqx_ds_msg_serializer:deserialize(SSchema, Blob).

-ifdef(TEST).

%% Verify calculation of the upper bound for rocksdb iterators
rocksdb_upper_limit_test() ->
    %% End of the data stream is always calculated as the beginning of
    %% the index stream:
    ?assertMatch(
        <<"static", 1:?wcb>>,
        stream_limit(<<"static">>, 0, <<>>)
    ),
    %% Index streams:
    %% 1. 1st level, 8-bit hash, no overflow:
    ?assertMatch(
        <<"static", 1:?wcb, 2:8>>,
        stream_limit(<<"static">>, 1, <<1:8>>)
    ),
    ?assertMatch(
        <<"static", 1:?wcb, 255:8>>,
        stream_limit(<<"static">>, 1, <<254:8>>)
    ),
    %% 2. 1st level, 8-bit hash, overflow:
    ?assertMatch(
        <<"static", 2:?wcb>>,
        stream_limit(<<"static">>, 1, <<16#ff:8>>)
    ),
    %% 3. 10th level, 128-bit hash, no overflow:
    ?assertMatch(
        <<"hello", 10:?wcb, "0123456789ABCDEG">>,
        stream_limit(<<"hello">>, 10, <<"0123456789ABCDEF">>)
    ),
    %% 3. 10th level, 128-bit hash, overflow:
    ?assertMatch(
        <<"hello", 11:?wcb>>,
        stream_limit(<<"hello">>, 10, <<(1 bsl 128 - 1):128>>)
    ),
    %% 4. Last valid wildcard hash level, 16-bit hash, overflow:
    ?assertMatch(
        <<"hello", 16#ffff:?wcb>>,
        stream_limit(<<"hello">>, 16#ffff - 1, <<16#ffff:16>>)
    ),
    %% 5. Too many wildcard levels:
    ?assertError(
        too_many_wildcard_levels,
        stream_limit(<<"hello">>, 16#ffff, <<"hash">>)
    ),
    ?assertError(
        too_many_wildcard_levels,
        stream_limit(<<"hello">>, 16#100000, <<"hash">>)
    ).

-endif.
