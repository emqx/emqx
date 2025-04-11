%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Generic implementation of 'skip-stream' indexing and
%% iteration.
-module(emqx_ds_gen_skipstream_lts).

%% API:
-export([]).

%% behavior callbacks:
-export([]).

%% internal exports:
-export([]).

-export_type([]).

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

%% Stream-level key is the hash of all varying topic levels.
-type stream_key() :: binary().

%% Last Seen Key placeholder that is used to indicate that the
%% iterator points at the beginning of the topic.
-define(lsk_begin, '').

%% Runtime state:
-record(s, {
    db :: rocksdb:db_handle(),
    data_cf :: rocksdb:cf_handle(),
    trie :: emqx_ds_lts:trie(),
    trie_cf :: rocksdb:cf_handle(),
    serialization_schema :: emqx_ds_msg_serializer:schema(),
    %% Position of compressed topic in the deserialized record:
    topic_pos :: pos_integer(),
    %% Number of bytes per wildcard topic level:
    hash_bytes :: pos_integer(),
    threshold_fun :: emqx_ds_lts:threshold_fun()
}).

-type s() :; #s{}.

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
    %% Compressed topic filter, split into words:
    filter,
    %% Lower and upper endpoints of an _open_ interval for the table scan.
    lower_endpoint,
    upper_endpoint
}).

-record(it, {
    static_index :: emqx_ds_lts:static_key(),
    %% Key of the last visited message:
    last_key :: stream_key() | ?lsk_begin,
    %% Compressed topic filter:
    compressed_tf :: emqx_ds:topic_filter()
}).

-type it() :: #it{}.

-define(COUNTERS, [
    ?DS_SKIPSTREAM_LTS_SEEK,
    ?DS_SKIPSTREAM_LTS_NEXT,
    ?DS_SKIPSTREAM_LTS_HASH_COLLISION,
    ?DS_SKIPSTREAM_LTS_HIT,
    ?DS_SKIPSTREAM_LTS_MISS,
    ?DS_SKIPSTREAM_LTS_FUTURE,
    ?DS_SKIPSTREAM_LTS_EOS
]).

%% Open interval that binds the fold.
-type fold_interval() :: {stream_key() | '-infinity', stream_key() | infinity}.

-type fold_fun(Acc) :: fun((#ctx{}, emqx_ds:message_key(), tuple(), Acc) -> Acc).

%%================================================================================
%% API functions
%%================================================================================

-spec mk_key(emqx_ds_lts:static_key(), wildcard_idx(), wildcard_hash(), stream_key()) ->
    binary().
mk_key(StaticIdx, 0, <<>>, StreamKey) ->
    mk_master_key(StaticIdx, StreamKey);
mk_key(StaticIdx, N, Hash, StreamKey) when N > 0 ->
    mk_index_key(StaticIdx, N, Hash, StreamKey).

-spec fold(
    emqx_ds_storage_layer:dbshard(),
    s(),
    it(),
    _BatchSize :: pos_integer(),
    fold_interval(),
    Acc,
    fold_fun(Acc)
) -> {ok, it(), Acc} | emqx_ds:error(_).
fold(DBShard = {_DB, Shard}, S, ItSeed, BatchSize, OpenInterval, Acc0, Fun) ->
    init_counters(),
    {Snapshot, PerLevelIterators} = init_iterators(S, ItSeed),
    %% ?tp(notice, skipstream_init_iters, #{it => It, its => Iterators}),
    try
        start_fold_loop(Shard, S, ItSeed, PerLevelIterators, BatchSize, OpenInterval, Acc0, Fun)
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

start_fold_loop(
    Shard,
    S = #s{trie = Trie},
    ItSeed = #it{static_index = StaticIdx, last_key = LSK, compressed_tf = CompressedTF},
    PerLevelIterators,
    BatchSize,
    {LowerEndpoint, UpperEndpoint},
    Acc,
    Fun
) ->
    %% cache it?
    TopicStructure = get_topic_structure(Trie, StaticIdx),
    %% Note on deduplication: actual scan goes over a half-open
    %% interval [Infimum, ∞). However, we don't want to return
    %% `last_key' of the iterator multiple times. Hence, inside the
    %% loop we drop elements that are lesser than the `LSK'. So the
    %% user sees elements from the open interval (LSK, ∞)
    Ctx = #ctx{
        shard = Shard,
        s = S,
        f = Fun,
        iters = PerLevelIterators,
        topic_structure = TopicStructure,
        filter = CompressedTF,
        lower_endpoint = LowerEndpoint,
        upper_endpoint = UpperEndpoint
    },
    case LSK of
        ?lsk_begin ->
            Infimum = <<>>;
        Infimum when is_binary(LSK) ->
            ok
    end,
    fold_loop(Ctx, ItSeed, BatchSize, {seek, Infimum}, Acc).

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
    #s{db = DB, data_cf = DataCF, hash_bytes = HashBytes} = S,
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

get_key_range(Snapshot, StaticIdx, WildcardIdx, ) ->
    %% NOTE: Assuming `?max_ts` is not a valid message timestamp.
    [
        %% {iterate_lower_bound, mk_key(StaticIdx, WildcardIdx, Hash, 0)},
        {iterate_upper_bound, mk_key(StaticIdx, WildcardIdx, Hash, 0)},
        {snapshot, Snapshot}
    ].

free_iterators(Its) ->
    lists:foreach(
        fun(#l{handle = IH}) ->
            ok = rocksdb:iterator_close(IH)
        end,
        Its
    ).

fold_loop(_Ctx, It, 0, _Op, Acc) ->
    {ok, It, Acc};
fold_loop(Ctx, It0, BatchSize, Op, Acc0) ->
    %% ?tp(notice, skipstream_loop, #{
    %%     ts => It0#it.ts, tf => It0#it.compressed_tf, bs => BatchSize, tmax => TMax, op => Op
    %% }),
    #ctx{s = S, f = Fun, iters = Iterators} = Ctx,
    #it{static_index = StaticIdx, compressed_tf = CompressedTF} = It0,
    %% Note: `next_step' function destructively updates RocksDB
    %% iterators in `ctx.iters' (they are handles, not values!),
    %% therefore a recursive call with the same arguments is not a
    %% bug.
    case fold_step(S, StaticIdx, CompressedTF, Iterators, any, Op) of
        none ->
            %% ?tp(notice, skipstream_loop_result, #{r => none}),
            inc_counter(?DS_SKIPSTREAM_LTS_EOS),
            {ok, It0, Acc0};
        {seek, SK} when SK >= Ctx#ctx.upper_endpoint ->
            {ok, It, Acc};
        {seek, SK} ->
            %% ?tp(notice, skipstream_loop_result, #{r => seek, ts => TS}),
            It = It0#it{last_key = SK},
            fold_loop(Ctx, It, BatchSize, {seek, SK}, Acc0);
        {ok, SK, DSKey, CompressedTopic, Val} ->
            %% ?tp(notice, skipstream_loop_result, #{r => ok, ts => TS, key => Key}),
            It = It0#it{last_key = SK},
            Acc = Fun(Ctx, DSKey, CompressedTopic, Val, Acc0),
            fold_loop(Ctx, It, BatchSize - 1, next, Acc)
    end.

fold_step(
    S,
    StaticIdx,
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
                rocksdb:iterator_move(IH, {seek, mk_key(StaticIdx, N, Hash, SK)})
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
                                S, StaticIdx, CompressedTF, Iterators, NextSK, {seek, NextSK}
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

%% Key generation:
%%
%%    Master:
mk_master_key(StaticIdx, StreamKey) ->
    %% Data stream is identified by wildcard level = 0
    <<StaticIdx/binary, 0:?wcb, StreamKey/binary>>.

%% Note: RocksDB `iterate_lower_bound' is *inclusive*, therefore we
%% can seek to the lower bound.
master_infimum(StaticIdx, '-infinity') ->
    <<StaticIdx/binary, 0:?wcb/big>>;
master_infimum(StaticIdx, StreamKey) ->
    <<StaticIdx/binary, 0:?wcb/big, StreamKey/binary>>.

%% Note: RocksDB `iterate_upper_bound' is *exclusive*
master_upper_bound(StaticIdx, 'infinity') ->
    <<StaticIdx/binary, 1:?wcb/big>>.

%%   Index:
mk_index_key(StaticIdx, WildcardLevel, Hash, StreamKey) ->
    <<StaticIdx/binary, WildcardLevel:?wcb/big, Hash/binary, StreamKey/binary>>.

index_infimum(StaticIdx, WildcardLevel, Hash, '-infinity') ->
    <<StaticIdx/binary, WildcardLevel:?wcb/big, Hash/binary>>;
index_infimum(StaticIdx, WildcardLevel, Hash, StreamKey) ->
    <<StaticIdx/binary, WildcardLevel:?wcb/big, Hash, StreamKey/binary>>.

%% Note: RocksDB `iterate_upper_bound' is *exclusive*
index_upper_bound(StaticIdx, WildcardLevel, Hash, infinity) ->
    <<N:(size(Hash))/big>> = Hash,
    HashPP = <<(N + 1):(size(Hash))/big>>,
    <<StaticIdx/binary, WildcardLevel:?wcb/big, Hash>>.

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
