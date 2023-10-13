%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Reference implementation of the storage.
%%
%% Trivial, extremely slow and inefficient. It also doesn't handle
%% restart of the Erlang node properly, so obviously it's only to be
%% used for testing.
-module(emqx_ds_storage_bitfield_lts).

-behavior(emqx_ds_storage_layer).

%% API:
-export([]).

%% behavior callbacks:
-export([create/4, open/5, store_batch/4, get_streams/4, make_iterator/5, next/4]).

%% internal exports:
-export([]).

-export_type([options/0]).

-include_lib("emqx/include/emqx.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type options() ::
    #{
        bits_per_wildcard_level => pos_integer(),
        topic_index_bytes => pos_integer(),
        epoch_bits => non_neg_integer()
    }.

%% Permanent state:
-type schema() ::
    #{
        bits_per_wildcard_level := pos_integer(),
        topic_index_bytes := pos_integer(),
        ts_bits := non_neg_integer(),
        ts_offset_bits := non_neg_integer()
    }.

%% Runtime state:
-record(s, {
    db :: rocksdb:db_handle(),
    data :: rocksdb:cf_handle(),
    trie :: emqx_ds_lts:trie(),
    keymappers :: array:array(emqx_ds_bitmask_keymapper:keymapper())
}).

-record(stream, {
    storage_key :: emqx_ds_lts:msg_storage_key()
}).

-record(it, {
    topic_filter :: emqx_ds:topic_filter(),
    start_time :: emqx_ds:time(),
    storage_key :: emqx_ds_lts:msg_storage_key(),
    last_seen_key = 0 :: emqx_ds_bitmask_keymapper:key(),
    key_filter :: [emqx_ds_bitmask_keymapper:scalar_range()]
}).

-define(QUICKCHECK_KEY(KEY, BITMASK, BITFILTER),
    ((KEY band BITMASK) =:= BITFILTER)
).

-define(COUNTER, emqx_ds_storage_bitfield_lts_counter).

%%================================================================================
%% API funcions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

create(_ShardId, DBHandle, GenId, Options) ->
    %% Get options:
    BitsPerTopicLevel = maps:get(bits_per_wildcard_level, Options, 64),
    TopicIndexBytes = maps:get(topic_index_bytes, Options, 4),
    TSOffsetBits = maps:get(epoch_bits, Options, 8), %% TODO: change to 10 to make it around ~1 sec
    %% Create column families:
    DataCFName = data_cf(GenId),
    TrieCFName = trie_cf(GenId),
    {ok, DataCFHandle} = rocksdb:create_column_family(DBHandle, DataCFName, []),
    {ok, TrieCFHandle} = rocksdb:create_column_family(DBHandle, TrieCFName, []),
    %% Create schema:
    Schema = #{
        bits_per_wildcard_level => BitsPerTopicLevel,
        topic_index_bytes => TopicIndexBytes,
        ts_bits => 64,
        ts_offset_bits => TSOffsetBits
    },
    {Schema, [{DataCFName, DataCFHandle}, {TrieCFName, TrieCFHandle}]}.

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
    %% If user's topics have more than learned 10 wildcard levels,
    %% then it's total carnage; learned topic structure won't help
    %% much:
    MaxWildcardLevels = 10,
    Keymappers = array:from_list(
        [
            make_keymapper(TopicIndexBytes, BitsPerTopicLevel, TSBits, TSOffsetBits, N)
         || N <- lists:seq(0, MaxWildcardLevels)
        ]
    ),
    #s{db = DBHandle, data = DataCF, trie = Trie, keymappers = Keymappers}.

store_batch(_ShardId, S = #s{db = DB, data = Data}, Messages, _Options) ->
    lists:foreach(
        fun(Msg) ->
            {Key, _} = make_key(S, Msg),
            Val = serialize(Msg),
            rocksdb:put(DB, Data, Key, Val, [])
        end,
        Messages
    ).

get_streams(_Shard, #s{trie = Trie}, TopicFilter, _StartTime) ->
    Indexes = emqx_ds_lts:match_topics(Trie, TopicFilter),
    [
        #stream{
            storage_key = I
        }
     || I <- Indexes
    ].

make_iterator(_Shard, _Data, #stream{storage_key = StorageKey}, TopicFilter, StartTime) ->
    {TopicIndex, Varying} = StorageKey,
    Filter = [
        {'=', TopicIndex},
        {'>=', StartTime}
        | lists:map(
            fun
                ('+') ->
                    any;
                (TopicLevel) when is_binary(TopicLevel) ->
                    {'=', hash_topic_level(TopicLevel)}
            end,
            Varying
        )
    ],
    {ok, #it{
        topic_filter = TopicFilter,
        start_time = StartTime,
        storage_key = StorageKey,
        key_filter = Filter
    }}.

next(_Shard, #s{db = DB, data = CF, keymappers = Keymappers}, It0, BatchSize) ->
    #it{
        key_filter = KeyFilter
    } = It0,
    % TODO: ugh, so ugly
    NVarying = length(KeyFilter) - 2,
    Keymapper = array:get(NVarying, Keymappers),
    %% Calculate lower and upper bounds for iteration:
    LowerBound = lower_bound(Keymapper, KeyFilter),
    UpperBound = upper_bound(Keymapper, KeyFilter),
    {ok, ITHandle} = rocksdb:iterator(DB, CF, [
        {iterate_lower_bound, LowerBound}, {iterate_upper_bound, UpperBound}
    ]),
    try
        put(?COUNTER, 0),
        next_loop(ITHandle, Keymapper, It0, [], BatchSize)
    after
        rocksdb:iterator_close(ITHandle),
        erase(?COUNTER)
    end.

%%================================================================================
%% Internal functions
%%================================================================================

next_loop(_, _, It, Acc, 0) ->
    {ok, It, lists:reverse(Acc)};
next_loop(ITHandle, KeyMapper, It0 = #it{last_seen_key = Key0, key_filter = KeyFilter}, Acc0, N0) ->
    inc_counter(),
    case next_range(KeyMapper, It0) of
        {Key1, Bitmask, Bitfilter} when Key1 > Key0 ->
            case iterator_move(KeyMapper, ITHandle, {seek, Key1}) of
                {ok, Key, Val} when ?QUICKCHECK_KEY(Key, Bitmask, Bitfilter) ->
                    assert_progress(bitmask_match, KeyMapper, KeyFilter, Key0, Key1),
                    Msg = deserialize(Val),
                    It1 = It0#it{last_seen_key = Key},
                    case check_message(It1, Msg) of
                        true ->
                            N1 = N0 - 1,
                            Acc1 = [Msg | Acc0];
                        false ->
                            N1 = N0,
                            Acc1 = Acc0
                    end,
                    {N, It, Acc} = traverse_interval(
                        ITHandle, KeyMapper, Bitmask, Bitfilter, It1, Acc1, N1
                    ),
                    next_loop(ITHandle, KeyMapper, It, Acc, N);
                {ok, Key, _Val} ->
                    assert_progress(bitmask_miss, KeyMapper, KeyFilter, Key0, Key1),
                    It = It0#it{last_seen_key = Key},
                    next_loop(ITHandle, KeyMapper, It, Acc0, N0);
                {error, invalid_iterator} ->
                    {ok, It0, lists:reverse(Acc0)}
            end;
        _ ->
            {ok, It0, lists:reverse(Acc0)}
    end.

traverse_interval(_, _, _, _, It, Acc, 0) ->
    {0, It, Acc};
traverse_interval(ITHandle, KeyMapper, Bitmask, Bitfilter, It0, Acc, N) ->
    inc_counter(),
    case iterator_move(KeyMapper, ITHandle, next) of
        {ok, Key, Val} when ?QUICKCHECK_KEY(Key, Bitmask, Bitfilter) ->
            Msg = deserialize(Val),
            It = It0#it{last_seen_key = Key},
            case check_message(It, Msg) of
                true ->
                    traverse_interval(
                        ITHandle, KeyMapper, Bitmask, Bitfilter, It, [Msg | Acc], N - 1
                    );
                false ->
                    traverse_interval(ITHandle, KeyMapper, Bitmask, Bitfilter, It, Acc, N)
            end;
        {ok, Key, _Val} ->
            It = It0#it{last_seen_key = Key},
            {N, It, Acc};
        {error, invalid_iterator} ->
            {0, It0, Acc}
    end.

next_range(KeyMapper, #it{key_filter = KeyFilter, last_seen_key = PrevKey}) ->
    emqx_ds_bitmask_keymapper:next_range(KeyMapper, KeyFilter, PrevKey).

check_message(_Iterator, _Msg) ->
    %% TODO.
    true.

iterator_move(KeyMapper, ITHandle, Action0) ->
    Action =
        case Action0 of
            next ->
                next;
            {seek, Int} ->
                {seek, emqx_ds_bitmask_keymapper:key_to_bitstring(KeyMapper, Int)}
        end,
    case rocksdb:iterator_move(ITHandle, Action) of
        {ok, KeyBin, Val} ->
            {ok, emqx_ds_bitmask_keymapper:bitstring_to_key(KeyMapper, KeyBin), Val};
        {ok, KeyBin} ->
            {ok, emqx_ds_bitmask_keymapper:bitstring_to_key(KeyMapper, KeyBin)};
        Other ->
            Other
    end.

assert_progress(_Msg, _KeyMapper, _KeyFilter, Key0, Key1) when Key1 > Key0 ->
    ?tp_ignore_side_effects_in_prod(
       emqx_ds_storage_bitfield_lts_iter_move,
       #{ location => _Msg
        , key0 => format_key(_KeyMapper, Key0)
        , key1 => format_key(_KeyMapper, Key1)
        }),
    ok;
assert_progress(Msg, KeyMapper, KeyFilter, Key0, Key1) ->
    Str0 = format_key(KeyMapper, Key0),
    Str1 = format_key(KeyMapper, Key1),
    error(#{'$msg' => Msg, key0 => Str0, key1 => Str1, step => get(?COUNTER), keyfilter => lists:map(fun format_keyfilter/1, KeyFilter)}).

format_key(KeyMapper, Key) ->
    Vec = [integer_to_list(I, 16) || I <- emqx_ds_bitmask_keymapper:key_to_vector(KeyMapper, Key)],
    lists:flatten(io_lib:format("~.16B (~s)", [Key, string:join(Vec, ",")])).

format_keyfilter(any) ->
    any;
format_keyfilter({Op, Val}) ->
    {Op, integer_to_list(Val, 16)}.

-spec make_key(#s{}, #message{}) -> {binary(), [binary()]}.
make_key(#s{keymappers = KeyMappers, trie = Trie}, #message{timestamp = Timestamp, topic = TopicBin}) ->
    Tokens = emqx_topic:tokens(TopicBin),
    {TopicIndex, Varying} = emqx_ds_lts:topic_key(Trie, fun threshold_fun/1, Tokens),
    VaryingHashes = [hash_topic_level(I) || I <- Varying],
    KeyMapper = array:get(length(Varying), KeyMappers),
    KeyBin = make_key(KeyMapper, TopicIndex, Timestamp, VaryingHashes),
    {KeyBin, Varying}.

-spec make_key(emqx_ds_bitmask_keymapper:keymapper(), emqx_ds_lts:static_key(), emqx_ds:time(), [
    non_neg_integer()
]) ->
    binary().
make_key(KeyMapper, TopicIndex, Timestamp, Varying) ->
    emqx_ds_bitmask_keymapper:key_to_bitstring(
        KeyMapper,
        emqx_ds_bitmask_keymapper:vector_to_key(KeyMapper, [TopicIndex, Timestamp | Varying])
    ).

%% TODO: don't hardcode the thresholds
threshold_fun(0) ->
    100;
threshold_fun(_) ->
    20.

hash_topic_level(TopicLevel) ->
    <<Int:64, _/binary>> = erlang:md5(TopicLevel),
    Int.

serialize(Msg) ->
    term_to_binary(Msg).

deserialize(Blob) ->
    binary_to_term(Blob).

-define(BYTE_SIZE, 8).

%% erlfmt-ignore
make_keymapper(TopicIndexBytes, BitsPerTopicLevel, TSBits, TSOffsetBits, N) ->
    Bitsources =
    %% Dimension Offset        Bitsize
        [{1,     0,            TopicIndexBytes * ?BYTE_SIZE},      %% Topic index
         {2,     TSOffsetBits, TSBits - TSOffsetBits       }] ++   %% Timestamp epoch
        [{2 + I, 0,            BitsPerTopicLevel           }       %% Varying topic levels
                                                           || I <- lists:seq(1, N)] ++
        [{2,     0,            TSOffsetBits                }],     %% Timestamp offset
    Keymapper = emqx_ds_bitmask_keymapper:make_keymapper(lists:reverse(Bitsources)),
    %% Assert:
    case emqx_ds_bitmask_keymapper:bitsize(Keymapper) rem 8 of
        0 ->
            ok;
        _ ->
            error(#{'$msg' => "Non-even key size", bitsources => Bitsources})
    end,
    Keymapper.

upper_bound(Keymapper, [TopicIndex | Rest]) ->
    filter_to_key(Keymapper, [TopicIndex | [{'=', infinity} || _ <- Rest]]).

lower_bound(Keymapper, [TopicIndex | Rest]) ->
    filter_to_key(Keymapper, [TopicIndex | [{'=', 0} || _ <- Rest]]).

filter_to_key(KeyMapper, KeyFilter) ->
    {Key, _, _} = emqx_ds_bitmask_keymapper:next_range(KeyMapper, KeyFilter, 0),
    emqx_ds_bitmask_keymapper:key_to_bitstring(KeyMapper, Key).

-spec restore_trie(pos_integer(), rocksdb:db_handle(), rocksdb:cf_handle()) -> emqx_ds_lts:trie().
restore_trie(TopicIndexBytes, DB, CF) ->
    PersistCallback = fun(Key, Val) ->
        rocksdb:put(DB, CF, term_to_binary(Key), term_to_binary(Val), [])
    end,
    {ok, IT} = rocksdb:iterator(DB, CF, []),
    try
        Dump = read_persisted_trie(IT, rocksdb:iterator_move(IT, first)),
        TrieOpts = #{persist_callback => PersistCallback, static_key_size => TopicIndexBytes},
        emqx_ds_lts:trie_restore(TrieOpts, Dump)
    after
        rocksdb:iterator_close(IT)
    end.

read_persisted_trie(IT, {ok, KeyB, ValB}) ->
    [
        {binary_to_term(KeyB), binary_to_term(ValB)}
        | read_persisted_trie(IT, rocksdb:iterator_move(IT, next))
    ];
read_persisted_trie(IT, {error, invalid_iterator}) ->
    [].

inc_counter() ->
    N = get(?COUNTER),
    put(?COUNTER, N + 1).

%% @doc Generate a column family ID for the MQTT messages
-spec data_cf(emqx_ds_storage_layer:gen_id()) -> [char()].
data_cf(GenId) ->
    "emqx_ds_storage_bitfield_lts_data" ++ integer_to_list(GenId).

%% @doc Generate a column family ID for the trie
-spec trie_cf(emqx_ds_storage_layer:gen_id()) -> [char()].
trie_cf(GenId) ->
    "emqx_ds_storage_bitfield_lts_trie" ++ integer_to_list(GenId).
