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
-export([
    create/4,
    open/5,
    drop/5,
    store_batch/4,
    get_streams/4,
    make_iterator/5,
    update_iterator/4,
    next/4,
    post_creation_actions/1
]).

%% internal exports:
-export([format_key/2]).

-export_type([options/0]).

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

%% keys:
-define(tag, 1).
-define(topic_filter, 2).
-define(start_time, 3).
-define(storage_key, 4).
-define(last_seen_key, 5).

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
    keymappers :: array:array(emqx_ds_bitmask_keymapper:keymapper()),
    ts_offset :: non_neg_integer()
}).

-type s() :: #s{}.

-type stream() ::
    #{
        ?tag := ?STREAM,
        ?storage_key := emqx_ds_lts:msg_storage_key()
    }.

-type iterator() ::
    #{
        ?tag := ?IT,
        ?topic_filter := emqx_ds:topic_filter(),
        ?start_time := emqx_ds:time(),
        ?storage_key := emqx_ds_lts:msg_storage_key(),
        ?last_seen_key := binary()
    }.

-define(COUNTER, emqx_ds_storage_bitfield_lts_counter).

%% Limit on the number of wildcard levels in the learned topic trie:
-define(WILDCARD_LIMIT, 10).

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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% API funcions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

-spec create(
    emqx_ds_storage_layer:shard_id(),
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
    #s{
        db = DBHandle,
        data = DataCF,
        trie = Trie,
        keymappers = KeymapperCache,
        ts_offset = TSOffsetBits
    }.

-spec post_creation_actions(emqx_ds_storage_layer:post_creation_context()) ->
    s().
post_creation_actions(
    #{
        new_gen_runtime_data := NewGenData,
        old_gen_runtime_data := OldGenData
    }
) ->
    #s{trie = OldTrie} = OldGenData,
    #s{trie = NewTrie0} = NewGenData,
    NewTrie = copy_previous_trie(OldTrie, NewTrie0),
    ?tp(bitfield_lts_inherited_trie, #{}),
    NewGenData#s{trie = NewTrie}.

-spec drop(
    emqx_ds_storage_layer:shard_id(),
    rocksdb:db_handle(),
    emqx_ds_storage_layer:gen_id(),
    emqx_ds_storage_layer:cf_refs(),
    s()
) ->
    ok.
drop(_Shard, DBHandle, GenId, CFRefs, #s{}) ->
    {_, DataCF} = lists:keyfind(data_cf(GenId), 1, CFRefs),
    {_, TrieCF} = lists:keyfind(trie_cf(GenId), 1, CFRefs),
    ok = rocksdb:drop_column_family(DBHandle, DataCF),
    ok = rocksdb:drop_column_family(DBHandle, TrieCF),
    ok.

-spec store_batch(
    emqx_ds_storage_layer:shard_id(), s(), [emqx_types:message()], emqx_ds:message_store_opts()
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

-spec get_streams(
    emqx_ds_storage_layer:shard_id(),
    s(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) -> [stream()].
get_streams(_Shard, #s{trie = Trie}, TopicFilter, _StartTime) ->
    Indexes = emqx_ds_lts:match_topics(Trie, TopicFilter),
    [#{?tag => ?STREAM, ?storage_key => I} || I <- Indexes].

-spec make_iterator(
    emqx_ds_storage_layer:shard_id(),
    s(),
    stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) -> {ok, iterator()}.
make_iterator(
    _Shard, _Data, #{?tag := ?STREAM, ?storage_key := StorageKey}, TopicFilter, StartTime
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

next(_Shard, Schema = #s{ts_offset = TSOffset}, It, BatchSize) ->
    %% Compute safe cutoff time.
    %% It's the point in time where the last complete epoch ends, so we need to know
    %% the current time to compute it.
    Now = emqx_message:timestamp_now(),
    SafeCutoffTime = (Now bsr TSOffset) bsl TSOffset,
    next_until(Schema, It, SafeCutoffTime, BatchSize).

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
    %% Make filter:
    Inequations = [
        {'=', TopicIndex},
        {StartTime, '..', SafeCutoffTime - 1},
        %% Unique integer:
        any
        %% Varying topic levels:
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
    try
        put(?COUNTER, 0),
        next_loop(ITHandle, Keymapper, Filter, SafeCutoffTime, It, [], BatchSize)
    after
        rocksdb:iterator_close(ITHandle),
        erase(?COUNTER)
    end.

%%================================================================================
%% Internal functions
%%================================================================================

next_loop(_ITHandle, _KeyMapper, _Filter, _Cutoff, It, Acc, 0) ->
    {ok, It, lists:reverse(Acc)};
next_loop(ITHandle, KeyMapper, Filter, Cutoff, It0, Acc0, N0) ->
    inc_counter(),
    #{?tag := ?IT, ?last_seen_key := Key0} = It0,
    case emqx_ds_bitmask_keymapper:bin_increment(Filter, Key0) of
        overflow ->
            {ok, It0, lists:reverse(Acc0)};
        Key1 ->
            %% assert
            true = Key1 > Key0,
            case rocksdb:iterator_move(ITHandle, {seek, Key1}) of
                {ok, Key, Val} ->
                    {N, It, Acc} =
                        traverse_interval(ITHandle, Filter, Cutoff, Key, Val, It0, Acc0, N0),
                    next_loop(ITHandle, KeyMapper, Filter, Cutoff, It, Acc, N);
                {error, invalid_iterator} ->
                    {ok, It0, lists:reverse(Acc0)}
            end
    end.

traverse_interval(ITHandle, Filter, Cutoff, Key, Val, It0, Acc0, N) ->
    It = It0#{?last_seen_key := Key},
    case emqx_ds_bitmask_keymapper:bin_checkmask(Filter, Key) of
        true ->
            Msg = deserialize(Val),
            case check_message(Cutoff, It, Msg) of
                true ->
                    Acc = [{Key, Msg} | Acc0],
                    traverse_interval(ITHandle, Filter, Cutoff, It, Acc, N - 1);
                false ->
                    traverse_interval(ITHandle, Filter, Cutoff, It, Acc0, N);
                overflow ->
                    {0, It0, Acc0}
            end;
        false ->
            {N, It, Acc0}
    end.

traverse_interval(_ITHandle, _Filter, _Cutoff, It, Acc, 0) ->
    {0, It, Acc};
traverse_interval(ITHandle, Filter, Cutoff, It, Acc, N) ->
    inc_counter(),
    case rocksdb:iterator_move(ITHandle, next) of
        {ok, Key, Val} ->
            traverse_interval(ITHandle, Filter, Cutoff, Key, Val, It, Acc, N);
        {error, invalid_iterator} ->
            {0, It, Acc}
    end.

-spec check_message(emqx_ds:time(), iterator(), emqx_types:message()) ->
    true | false | overflow.
check_message(
    Cutoff,
    _It,
    #message{timestamp = Timestamp}
) when Timestamp >= Cutoff ->
    %% We hit the current epoch, we can't continue iterating over it yet.
    %% It would be unsafe otherwise: messages can be stored in the current epoch
    %% concurrently with iterating over it. They can end up earlier (in the iteration
    %% order) due to the nature of keymapping, potentially causing us to miss them.
    overflow;
check_message(
    _Cutoff,
    #{?tag := ?IT, ?start_time := StartTime, ?topic_filter := TopicFilter},
    #message{timestamp = Timestamp, topic = Topic}
) when Timestamp >= StartTime ->
    emqx_topic:match(emqx_topic:tokens(Topic), TopicFilter);
check_message(_Cutoff, _It, _Msg) ->
    false.

format_key(KeyMapper, Key) ->
    Vec = [integer_to_list(I, 16) || I <- emqx_ds_bitmask_keymapper:key_to_vector(KeyMapper, Key)],
    lists:flatten(io_lib:format("~.16B (~s)", [Key, string:join(Vec, ",")])).

-spec make_key(s(), emqx_types:message()) -> {binary(), [binary()]}.
make_key(#s{keymappers = KeyMappers, trie = Trie}, #message{timestamp = Timestamp, topic = TopicBin}) ->
    Tokens = emqx_topic:words(TopicBin),
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
    UniqueInteger = erlang:unique_integer([monotonic, positive]),
    emqx_ds_bitmask_keymapper:key_to_bitstring(
        KeyMapper,
        emqx_ds_bitmask_keymapper:vector_to_key(KeyMapper, [
            TopicIndex, Timestamp, UniqueInteger | Varying
        ])
    ).

%% TODO: don't hardcode the thresholds
threshold_fun(0) ->
    100;
threshold_fun(_) ->
    20.

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

serialize(Msg) ->
    term_to_binary(message_to_value_v1(Msg)).

deserialize(Blob) ->
    value_v1_to_message(binary_to_term(Blob)).

-define(BYTE_SIZE, 8).

%% erlfmt-ignore
make_keymapper(TopicIndexBytes, BitsPerTopicLevel, TSBits, TSOffsetBits, N) ->
    Bitsources =
    %% Dimension Offset        Bitsize
        [{1,     0,            TopicIndexBytes * ?BYTE_SIZE},      %% Topic index
         {2,     TSOffsetBits, TSBits - TSOffsetBits       }] ++   %% Timestamp epoch
        [{3 + I, 0,            BitsPerTopicLevel           }       %% Varying topic levels
                                                           || I <- lists:seq(1, N)] ++
        [{2,     0,            TSOffsetBits                },      %% Timestamp offset
         {3,     0,            64                          }],     %% Unique integer
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

-spec copy_previous_trie(emqx_ds_lts:trie(), emqx_ds_lts:trie()) -> emqx_ds_lts:trie().
copy_previous_trie(OldTrie, NewTrie) ->
    emqx_ds_lts:trie_copy_learned_paths(OldTrie, NewTrie).

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

-ifdef(TEST).

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
