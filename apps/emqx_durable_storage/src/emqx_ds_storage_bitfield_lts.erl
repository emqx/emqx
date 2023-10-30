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

%% @doc A storage layout based on learned topic structure and using
%% bitfield mapping for the varying topic layers.
-module(emqx_ds_storage_bitfield_lts).

-behaviour(emqx_ds_storage_layer).

%% API:
-export([]).

%% behavior callbacks:
-export([create/4, open/5, store_batch/4, get_streams/4, make_iterator/5, next/4]).

%% internal exports:
-export([format_key/2, format_keyfilter/1]).

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

-type s() :: #s{}.

-record(stream, {
    storage_key :: emqx_ds_lts:msg_storage_key()
}).

-record(it, {
    topic_filter :: emqx_ds:topic_filter(),
    start_time :: emqx_ds:time(),
    storage_key :: emqx_ds_lts:msg_storage_key(),
    last_seen_key = <<>> :: binary()
}).

-type iterator() :: #it{}.

-define(COUNTER, emqx_ds_storage_bitfield_lts_counter).

%% Limit on the number of wildcard levels in the learned topic trie:
-define(WILDCARD_LIMIT, 10).

-include("emqx_ds_bitmask.hrl").

%%================================================================================
%% API funcions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

-spec create(
    emqx_ds_replication_layer:shard_id(),
    rocksdb:db_handle(),
    emqx_ds_storage_layer:gen_id(),
    options()
) ->
    {schema(), emqx_ds_storage_layer:cf_refs()}.
create(_ShardId, DBHandle, GenId, Options) ->
    %% Get options:
    BitsPerTopicLevel = maps:get(bits_per_wildcard_level, Options, 64),
    TopicIndexBytes = maps:get(topic_index_bytes, Options, 4),
    %% 10 bits -> 1024 ms -> ~1 sec
    TSOffsetBits = maps:get(epoch_bits, Options, 10),
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

-spec open(
    emqx_ds_replication_layer:shard_id(),
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
    #s{db = DBHandle, data = DataCF, trie = Trie, keymappers = KeymapperCache}.

-spec store_batch(
    emqx_ds_replication_layer:shard_id(), s(), [emqx_types:message()], emqx_ds:message_store_opts()
) ->
    emqx_ds:store_batch_result().
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
    [#stream{storage_key = I} || I <- Indexes].

make_iterator(_Shard, _Data, #stream{storage_key = StorageKey}, TopicFilter, StartTime) ->
    %% Note: it's a good idea to keep the iterator structure lean,
    %% since it can be stored on a remote node that could update its
    %% code independently from us.
    {ok, #it{
        topic_filter = TopicFilter,
        start_time = StartTime,
        storage_key = StorageKey
    }}.

next(_Shard, #s{db = DB, data = CF, keymappers = Keymappers}, It0, BatchSize) ->
    #it{
        start_time = StartTime,
        storage_key = StorageKey
    } = It0,
    %% Make filter:
    {TopicIndex, Varying} = StorageKey,
    Inequations = [
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
    %% Obtain a keymapper for the current number of varying
    %% levels. Magic constant 2: we have two extra dimensions of topic
    %% index and time; the rest of dimensions are varying levels.
    NVarying = length(Inequations) - 2,
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
    try
        put(?COUNTER, 0),
        next_loop(ITHandle, Keymapper, Filter, It0, [], BatchSize)
    after
        rocksdb:iterator_close(ITHandle),
        erase(?COUNTER)
    end.

%%================================================================================
%% Internal functions
%%================================================================================

next_loop(_ITHandle, _KeyMapper, _Filter, It, Acc, 0) ->
    {ok, It, lists:reverse(Acc)};
next_loop(ITHandle, KeyMapper, Filter, It0, Acc0, N0) ->
    inc_counter(),
    #it{last_seen_key = Key0} = It0,
    case emqx_ds_bitmask_keymapper:bin_increment(Filter, Key0) of
        overflow ->
            {ok, It0, lists:reverse(Acc0)};
        Key1 ->
            %% assert
            true = Key1 > Key0,
            case rocksdb:iterator_move(ITHandle, {seek, Key1}) of
                {ok, Key, Val} ->
                    It1 = It0#it{last_seen_key = Key},
                    case check_message(Filter, It1, Val) of
                        {true, Msg} ->
                            N1 = N0 - 1,
                            Acc1 = [Msg | Acc0];
                        false ->
                            N1 = N0,
                            Acc1 = Acc0
                    end,
                    {N, It, Acc} = traverse_interval(ITHandle, KeyMapper, Filter, It1, Acc1, N1),
                    next_loop(ITHandle, KeyMapper, Filter, It, Acc, N);
                {error, invalid_iterator} ->
                    {ok, It0, lists:reverse(Acc0)}
            end
    end.

traverse_interval(_ITHandle, _KeyMapper, _Filter, It, Acc, 0) ->
    {0, It, Acc};
traverse_interval(ITHandle, KeyMapper, Filter, It0, Acc, N) ->
    inc_counter(),
    case rocksdb:iterator_move(ITHandle, next) of
        {ok, Key, Val} ->
            It = It0#it{last_seen_key = Key},
            case check_message(Filter, It, Val) of
                {true, Msg} ->
                    traverse_interval(ITHandle, KeyMapper, Filter, It, [Msg | Acc], N - 1);
                false ->
                    traverse_interval(ITHandle, KeyMapper, Filter, It, Acc, N)
            end;
        {error, invalid_iterator} ->
            {0, It0, Acc}
    end.

-spec check_message(emqx_ds_bitmask_keymapper:filter(), iterator(), binary()) ->
    {true, emqx_types:message()} | false.
check_message(Filter, #it{last_seen_key = Key}, Val) ->
    case emqx_ds_bitmask_keymapper:bin_checkmask(Filter, Key) of
        true ->
            Msg = deserialize(Val),
            %% TODO: check strict time and hash collisions
            {true, Msg};
        false ->
            false
    end.

format_key(KeyMapper, Key) ->
    Vec = [integer_to_list(I, 16) || I <- emqx_ds_bitmask_keymapper:key_to_vector(KeyMapper, Key)],
    lists:flatten(io_lib:format("~.16B (~s)", [Key, string:join(Vec, ",")])).

format_keyfilter(any) ->
    any;
format_keyfilter({Op, Val}) ->
    {Op, integer_to_list(Val, 16)}.

-spec make_key(s(), emqx_types:message()) -> {binary(), [binary()]}.
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
read_persisted_trie(_IT, {error, invalid_iterator}) ->
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
