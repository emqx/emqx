%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Common routines for implementing storage layouts based on
%% "skip-stream" indexing.
-module(emqx_ds_gen_skipstream_lts).

%% API:
-export([
    open/1,
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
    s/0
]).

%% inline small functions:
-compile(inline).

-include("emqx_ds.hrl").
-include("emqx_ds_metrics.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%% Width of the wildcard layer, in bits:
-define(wcb, 16).

-type wildcard_idx() :: 0..16#ffff.
-type wildcard_hash() :: binary().

%% Key that identifies a message in the stream. It's the suffix of
%% `emqx_ds:message_key'.
-type stream_key() :: binary().

%% Runtime state:
-record(s, {
    dbshard :: {emqx_ds:db(), emqx_ds:shard()},
    db :: rocksdb:db_handle(),
    data_cf :: rocksdb:cf_handle(),
    trie :: emqx_ds_lts:trie(),
    serialization_schema :: emqx_ds_msg_serializer:schema(),
    %% Maximum possible value of stream key, assuming size of the
    %% stream key is constant or has an upper bound. For example, if
    %% stream key size is 1 byte, then `max_stream_key' is
    %% <<255, 255>> (i.e. 2 bytes):
    max_stream_key :: binary(),
    %% Position of compressed topic in the deserialized record:
    get_topic :: fun((_) -> emqx_ds:topic()),
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
    (emqx_ds_lts:learned_structure(), emqx_ds:message_key(), emqx_ds:topic(), _Value, Acc) -> Acc
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
    static :: emqx_ds_lts:static_index(),
    %% Lower and upper endpoint of the stream key _open_ interval:
    lower_endpoint :: stream_key() | '-infinity',
    upper_endpoint :: stream_key()
}).

%% Level iterator:
-record(l, {
    n :: non_neg_integer(),
    handle :: rocksdb:itr_handle(),
    hash :: binary()
}).

-ifdef(DEBUG2).
-include_lib("snabbkaffe/include/trace.hrl").
-define(dbg(K, A), ?tp(notice, K, A)).
-else.
-define(dbg(K, A), ok).
-endif.

%%================================================================================
%% API functions
%%================================================================================

%% @doc Initialize the module.
-spec open(#{
    db_shard := {emqx_ds:db(), emqx_ds:shard()},
    db_handle := rocksdb:db_handle(),
    data_cf := rocksdb:cf_handle(),
    trie := emqx_ds_lts:trie(),
    serialization_schema := emqx_ds_msg_serializer:schema(),
    stream_key_size := pos_integer(),
    get_topic := fun((_) -> emqx_ds:topic()),
    hash_bytes := pos_integer()
}) ->
    s().
open(
    #{
        db_shard := DBShard,
        db_handle := DBHandle,
        data_cf := DataCF,
        trie := Trie,
        serialization_schema := SerSchema,
        stream_key_size := StreamKeySize,
        get_topic := GetTopic,
        hash_bytes := HashBytes
    }
) ->
    #s{
        dbshard = DBShard,
        db = DBHandle,
        data_cf = DataCF,
        trie = Trie,
        serialization_schema = SerSchema,
        max_stream_key = list_to_binary([255 || _ <- lists:seq(0, StreamKeySize)]),
        get_topic = GetTopic,
        hash_bytes = HashBytes
    }.

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
batch_put(#s{data_cf = DataCF, hash_bytes = HashBytes}, Batch, Static, Varying, StreamKey, Value) ->
    MasterKey = mk_master_key(Static, StreamKey),
    ok = rocksdb:batch_put(Batch, DataCF, MasterKey, Value),
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
-spec mk_key(emqx_ds_lts:static_key(), wildcard_idx(), binary(), stream_key()) ->
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
    <<Static:TSz/binary, 0:?wcb, StreamKey>> = DSKey,
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
%% Note: the returned message is deserialized, but no further
%% transformation or enrichment is performed.
-spec lookup_message(s(), emqx_ds_lts:static_key(), stream_key()) ->
    {ok, emqx_ds:message_key(), _Value} | undefined | emqx_ds:error(_).
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
            upper_endpoint =
                case UpperEndpoint of
                    'infinity' ->
                        S#s.max_stream_key;
                    _ ->
                        UpperEndpoint
                end
        },
        StartPoint =
            case LowerEndpoint of
                '-infinity' ->
                    <<>>;
                _ when is_binary(LowerEndpoint) ->
                    LowerEndpoint
            end,
        fold_loop(Ctx, StartPoint, BatchSize, {seek, StartPoint}, Acc0)
    after
        free_iterators(PerLevelIterators),
        rocksdb:release_snapshot(Snapshot),
        collect_counters(DBShard)
    end.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%===============================================================================

%%%% Fold %%%%

%% @doc Init RockDB iterators for each non-wildcard level of indices,
%% order is important: e.g. [L1, L2, L3, L0].
-spec init_iterators(s(), emqx_ds_lts:static_key(), [binary()], fold_interval()) ->
    {rocksdb:snapshot_handle(), [rocksdb:itr_handle()]}.
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

get_key_range(S, Snapshot, Static, {_, Lower, Upper}, WildcardLevel, Hash) ->
    %% Note: rocksdb treats lower bound as inclusive
    L =
        case Lower of
            '-infinity' -> <<>>;
            _ -> Lower
        end,
    %% Note: rocksdb treats upper bound as non-inclusive
    U =
        case Upper of
            'infinity' -> S#s.max_stream_key;
            _ -> Upper
        end,
    [
        {iterate_lower_bound, mk_key(Static, WildcardLevel, Hash, L)},
        {iterate_upper_bound, mk_key(Static, WildcardLevel, Hash, U)},
        {snapshot, Snapshot}
    ].

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
    #fold_ctx{s = S, f = Fun, iters = Iterators, static = StaticIdx, filter = CompressedTF} = Ctx,
    %% Note: `next_step' function destructively updates RocksDB
    %% iterators in `ctx.iters' (they are handles, not values!),
    %% therefore a recursive call with the same arguments is not a
    %% bug.
    case fold_step(S, StaticIdx, CompressedTF, Iterators, any, Op) of
        none ->
            ?dbg(skipstream_loop_result, #{r => none}),
            inc_counter(?DS_SKIPSTREAM_LTS_EOS),
            {ok, SK0, Acc0};
        {seek, SK} when SK >= Ctx#fold_ctx.upper_endpoint ->
            {ok, SK0, Acc0};
        {seek, SK} ->
            ?dbg(skipstream_loop_result, #{r => seek, sk => SK}),
            fold_loop(Ctx, SK, BatchSize, {seek, SK}, Acc0);
        {ok, SK, CompressedTopic, DSKey, Val} ->
            ?dbg(skipstream_loop_result, #{r => ok, sk => SK, key => DSKey}),
            case Ctx#fold_ctx.lower_endpoint < SK of
                true ->
                    Acc = Fun(Ctx#fold_ctx.topic_structure, DSKey, CompressedTopic, Val, Acc0),
                    fold_loop(Ctx, SK, BatchSize - 1, next, Acc);
                false ->
                    fold_loop(Ctx, SK, BatchSize, next, Acc0)
            end
    end.

fold_step(
    S = #s{get_topic = GetTopic},
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
                            CompressedTopic = GetTopic(Val),
                            case emqx_topic:match(CompressedTopic, CompressedTF) of
                                true ->
                                    inc_counter(?DS_SKIPSTREAM_LTS_HIT),
                                    {ok, NextSK, CompressedTopic, Key, Val};
                                false ->
                                    %% Hash collision. Advance to the
                                    %% next timestamp:
                                    inc_counter(?DS_SKIPSTREAM_LTS_HASH_COLLISION),
                                    {seek, NextSK + 1}
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
