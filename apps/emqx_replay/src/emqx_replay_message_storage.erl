%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_replay_message_storage).

%%================================================================================
%% @doc Description of the schema
%%
%% Let us assume that `T' is a topic and `t' is time. These are the two
%% dimensions used to index messages. They can be viewed as
%% "coordinates" of an MQTT message in a 2D space.
%%
%% Oftentimes, when wildcard subscription is used, keys must be
%% scanned in both dimensions simultaneously.
%%
%% Rocksdb allows to iterate over sorted keys very fast. This means we
%% need to map our two-dimentional keys to a single index that is
%% sorted in a way that helps to iterate over both time and topic
%% without having to do a lot of random seeks.
%%
%% == Mapping of 2D keys to rocksdb keys ==
%%
%% We use "zigzag" pattern to store messages, where rocksdb key is
%% composed like like this:
%%
%%              |ttttt|TTTTTTTTT|tttt|
%%                 ^       ^      ^
%%                 |       |      |
%%         +-------+       |      +---------+
%%         |               |                |
%% most significant    topic hash   least significant
%% bits of timestamp                bits of timestamp
%% (a.k.a epoch)                    (a.k.a time offset)
%%
%% Topic hash is level-aware: each topic level is hashed separately
%% and the resulting hashes are bitwise-concatentated. This allows us
%% to map topics to fixed-length bitstrings while keeping some degree
%% of information about the hierarchy.
%%
%% Next important concept is what we call "epoch". It is time
%% interval determined by the number of least significant bits of the
%% timestamp found at the tail of the rocksdb key.
%%
%% The resulting index is a space-filling curve that looks like
%% this in the topic-time 2D space:
%%
%% T ^ ---->------   |---->------   |---->------
%%   |       --/     /      --/     /      --/
%%   |   -<-/       |   -<-/       |   -<-/
%%   | -/           | -/           | -/
%%   | ---->------  | ---->------  | ---->------
%%   |       --/    /       --/    /       --/
%%   |   ---/      |    ---/      |    ---/
%%   | -/          ^  -/          ^  -/
%%   | ---->------ |  ---->------ |  ---->------
%%   |       --/   /        --/   /        --/
%%   |   -<-/     |     -<-/     |     -<-/
%%   | -/         |   -/         |   -/
%%   | ---->------|   ---->------|   ---------->
%%   |
%%  -+------------+-----------------------------> t
%%        epoch
%%
%% This structure allows to quickly seek to a the first message that
%% was recorded in a certain epoch in a certain topic or a
%% group of topics matching filter like `foo/bar/#`.
%%
%% Due to its structure, for each pair of rocksdb keys K1 and K2, such
%% that K1 > K2 and topic(K1) = topic(K2), timestamp(K1) >
%% timestamp(K2).
%% That is, replay doesn't reorder messages published in each
%% individual topic.
%%
%% This property doesn't hold between different topics, but it's not deemed
%% a problem right now.
%%
%%================================================================================

%% API:
-export([create_new/3, open/5]).
-export([make_keymapper/1]).

-export([store/5]).
-export([make_iterator/3]).
-export([make_iterator/4]).
-export([next/1]).

-export([preserve_iterator/1]).
-export([restore_iterator/2]).
-export([refresh_iterator/1]).

%% Debug/troubleshooting:
%% Keymappers
-export([
    keymapper_info/1,
    compute_bitstring/3,
    compute_topic_bitmask/2,
    compute_time_bitmask/1,
    hash/2
]).

%% Keyspace filters
-export([
    make_keyspace_filter/3,
    compute_initial_seek/1,
    compute_next_seek/2,
    compute_time_seek/3,
    compute_topic_seek/4
]).

-export_type([db/0, iterator/0, schema/0]).

-export_type([options/0]).
-export_type([iteration_options/0]).

-compile(
    {inline, [
        bitwise_concat/3,
        ones/1,
        successor/1,
        topic_hash_matches/3,
        time_matches/3
    ]}
).

%%================================================================================
%% Type declarations
%%================================================================================

%% parsed
-type topic() :: list(binary()).

%% TODO granularity?
-type time() :: integer().

%% Number of bits
-type bits() :: non_neg_integer().

%% Key of a RocksDB record.
-type key() :: binary().

%% Distribution of entropy among topic levels.
%% Example: [4, 8, 16] means that level 1 gets 4 bits, level 2 gets 8 bits,
%% and _rest of levels_ (if any) get 16 bits.
-type bits_per_level() :: [bits(), ...].

-type options() :: #{
    %% Number of bits in a message timestamp.
    timestamp_bits := bits(),
    %% Number of bits in a key allocated to each level in a message topic.
    topic_bits_per_level := bits_per_level(),
    %% Maximum granularity of iteration over time.
    epoch := time(),

    iteration => iteration_options(),

    cf_options => emqx_replay_local_store:db_cf_options()
}.

-type iteration_options() :: #{
    %% Request periodic iterator refresh.
    %% This might be helpful during replays taking a lot of time (e.g. tens of seconds).
    %% Note that `{every, 1000}` means 1000 _operations_ with the iterator which is not
    %% the same as 1000 replayed messages.
    iterator_refresh => {every, _NumOperations :: pos_integer()}
}.

%% Persistent configuration of the generation, it is used to create db
%% record when the database is reopened
-record(schema, {keymapper :: keymapper()}).

-opaque schema() :: #schema{}.

-record(db, {
    zone :: emqx_types:zone(),
    handle :: rocksdb:db_handle(),
    cf :: rocksdb:cf_handle(),
    keymapper :: keymapper(),
    write_options = [{sync, true}] :: emqx_replay_local_store:db_write_options(),
    read_options = [] :: emqx_replay_local_store:db_write_options()
}).

-record(it, {
    handle :: rocksdb:itr_handle(),
    filter :: keyspace_filter(),
    cursor :: binary() | undefined,
    next_action :: {seek, binary()} | next,
    refresh_counter :: {non_neg_integer(), pos_integer()} | undefined
}).

-record(filter, {
    keymapper :: keymapper(),
    topic_filter :: emqx_topic:words(),
    start_time :: integer(),
    hash_bitfilter :: integer(),
    hash_bitmask :: integer(),
    time_bitfilter :: integer(),
    time_bitmask :: integer()
}).

% NOTE
% Keymapper decides how to map messages into RocksDB column family keyspace.
-record(keymapper, {
    source :: [bitsource(), ...],
    bitsize :: bits(),
    epoch :: non_neg_integer()
}).

-type bitsource() ::
    %% Consume `_Size` bits from timestamp starting at `_Offset`th bit.
    %% TODO consistency
    {timestamp, _Offset :: bits(), _Size :: bits()}
    %% Consume next topic level (either one or all of them) and compute `_Size` bits-wide hash.
    | {hash, level | levels, _Size :: bits()}.

-opaque db() :: #db{}.
-opaque iterator() :: #it{}.
-type keymapper() :: #keymapper{}.
-type keyspace_filter() :: #filter{}.

%%================================================================================
%% API funcions
%%================================================================================

%% Create a new column family for the generation and a serializable representation of the schema
-spec create_new(rocksdb:db_handle(), emqx_replay_local_store:gen_id(), options()) ->
    {schema(), emqx_replay_local_store:cf_refs()}.
create_new(DBHandle, GenId, Options) ->
    CFName = data_cf(GenId),
    CFOptions = maps:get(cf_options, Options, []),
    {ok, CFHandle} = rocksdb:create_column_family(DBHandle, CFName, CFOptions),
    Schema = #schema{keymapper = make_keymapper(Options)},
    {Schema, [{CFName, CFHandle}]}.

%% Reopen the database
-spec open(
    emqx_types:zone(),
    rocksdb:db_handle(),
    emqx_replay_local_store:gen_id(),
    emqx_replay_local_store:cf_refs(),
    schema()
) ->
    db().
open(Zone, DBHandle, GenId, CFs, #schema{keymapper = Keymapper}) ->
    {value, {_, CFHandle}} = lists:keysearch(data_cf(GenId), 1, CFs),
    #db{
        zone = Zone,
        handle = DBHandle,
        cf = CFHandle,
        keymapper = Keymapper
    }.

-spec make_keymapper(options()) -> keymapper().
make_keymapper(#{
    timestamp_bits := TimestampBits,
    topic_bits_per_level := BitsPerLevel,
    epoch := MaxEpoch
}) ->
    TimestampLSBs = min(TimestampBits, floor(math:log2(MaxEpoch))),
    TimestampMSBs = TimestampBits - TimestampLSBs,
    NLevels = length(BitsPerLevel),
    {LevelBits, [TailLevelsBits]} = lists:split(NLevels - 1, BitsPerLevel),
    Source = lists:flatten([
        [{timestamp, TimestampLSBs, TimestampMSBs} || TimestampMSBs > 0],
        [{hash, level, Bits} || Bits <- LevelBits],
        {hash, levels, TailLevelsBits},
        [{timestamp, 0, TimestampLSBs} || TimestampLSBs > 0]
    ]),
    #keymapper{
        source = Source,
        bitsize = lists:sum([S || {_, _, S} <- Source]),
        epoch = 1 bsl TimestampLSBs
    }.

-spec store(db(), emqx_guid:guid(), time(), topic(), binary()) ->
    ok | {error, _TODO}.
store(DB = #db{handle = DBHandle, cf = CFHandle}, MessageID, PublishedAt, Topic, MessagePayload) ->
    Key = make_message_key(Topic, PublishedAt, MessageID, DB#db.keymapper),
    Value = make_message_value(Topic, MessagePayload),
    rocksdb:put(DBHandle, CFHandle, Key, Value, DB#db.write_options).

-spec make_iterator(db(), emqx_topic:words(), time() | earliest) ->
    {ok, iterator()} | {error, _TODO}.
make_iterator(DB, TopicFilter, StartTime) ->
    Options = emqx_replay_conf:zone_iteration_options(DB#db.zone),
    make_iterator(DB, TopicFilter, StartTime, Options).

-spec make_iterator(db(), emqx_topic:words(), time() | earliest, iteration_options()) ->
    % {error, invalid_start_time}? might just start from the beginning of time
    % and call it a day: client violated the contract anyway.
    {ok, iterator()} | {error, _TODO}.
make_iterator(DB = #db{handle = DBHandle, cf = CFHandle}, TopicFilter, StartTime, Options) ->
    case rocksdb:iterator(DBHandle, CFHandle, DB#db.read_options) of
        {ok, ITHandle} ->
            % TODO earliest
            Filter = make_keyspace_filter(TopicFilter, StartTime, DB#db.keymapper),
            InitialSeek = combine(compute_initial_seek(Filter), <<>>, DB#db.keymapper),
            RefreshCounter = make_refresh_counter(maps:get(iterator_refresh, Options, undefined)),
            {ok, #it{
                handle = ITHandle,
                filter = Filter,
                next_action = {seek, InitialSeek},
                refresh_counter = RefreshCounter
            }};
        Err ->
            Err
    end.

-spec next(iterator()) -> {value, binary(), iterator()} | none | {error, closed}.
next(It0 = #it{filter = #filter{keymapper = Keymapper}}) ->
    It = maybe_refresh_iterator(It0),
    case rocksdb:iterator_move(It#it.handle, It#it.next_action) of
        % spec says `{ok, Key}` is also possible but the implementation says it's not
        {ok, Key, Value} ->
            % Preserve last seen key in the iterator so it could be restored / refreshed later.
            ItNext = It#it{cursor = Key},
            Bitstring = extract(Key, Keymapper),
            case match_next(Bitstring, Value, It#it.filter) of
                {_Topic, Payload} ->
                    {value, Payload, ItNext#it{next_action = next}};
                next ->
                    next(ItNext#it{next_action = next});
                NextBitstring when is_integer(NextBitstring) ->
                    NextSeek = combine(NextBitstring, <<>>, Keymapper),
                    next(ItNext#it{next_action = {seek, NextSeek}});
                none ->
                    stop_iteration(ItNext)
            end;
        {error, invalid_iterator} ->
            stop_iteration(It);
        {error, iterator_closed} ->
            {error, closed}
    end.

-spec preserve_iterator(iterator()) -> binary().
preserve_iterator(#it{cursor = Cursor, filter = Filter}) ->
    State = #{
        v => 1,
        cursor => Cursor,
        filter => Filter#filter.topic_filter,
        stime => Filter#filter.start_time
    },
    term_to_binary(State).

-spec restore_iterator(db(), binary()) -> {ok, iterator()} | {error, _TODO}.
restore_iterator(DB, Serial) when is_binary(Serial) ->
    State = binary_to_term(Serial),
    restore_iterator(DB, State);
restore_iterator(DB, #{
    v := 1,
    cursor := Cursor,
    filter := TopicFilter,
    stime := StartTime
}) ->
    case make_iterator(DB, TopicFilter, StartTime) of
        {ok, It} when Cursor == undefined ->
            % Iterator was preserved right after it has been made.
            {ok, It};
        {ok, It} ->
            % Iterator was preserved mid-replay, seek right past the last seen key.
            {ok, It#it{cursor = Cursor, next_action = {seek, successor(Cursor)}}};
        Err ->
            Err
    end.

-spec refresh_iterator(iterator()) -> iterator().
refresh_iterator(It = #it{handle = Handle, cursor = Cursor, next_action = Action}) ->
    case rocksdb:iterator_refresh(Handle) of
        ok when Action =:= next ->
            % Now the underlying iterator is invalid, need to seek instead.
            It#it{next_action = {seek, successor(Cursor)}};
        ok ->
            % Now the underlying iterator is invalid, but will seek soon anyway.
            It;
        {error, _} ->
            % Implementation could in theory return an {error, ...} tuple.
            % Supposedly our best bet is to ignore it.
            % TODO logging?
            It
    end.

%%================================================================================
%% Internal exports
%%================================================================================

-spec keymapper_info(keymapper()) -> [bitsource()].
keymapper_info(#keymapper{source = Source, bitsize = Bitsize, epoch = Epoch}) ->
    #{source => Source, bitsize => Bitsize, epoch => Epoch}.

make_message_key(Topic, PublishedAt, MessageID, Keymapper) ->
    combine(compute_bitstring(Topic, PublishedAt, Keymapper), MessageID, Keymapper).

make_message_value(Topic, MessagePayload) ->
    term_to_binary({Topic, MessagePayload}).

unwrap_message_value(Binary) ->
    binary_to_term(Binary).

-spec combine(_Bitstring :: integer(), emqx_guid:guid() | <<>>, keymapper()) ->
    key().
combine(Bitstring, MessageID, #keymapper{bitsize = Size}) ->
    <<Bitstring:Size/integer, MessageID/binary>>.

-spec extract(key(), keymapper()) ->
    _Bitstring :: integer().
extract(Key, #keymapper{bitsize = Size}) ->
    <<Bitstring:Size/integer, _MessageID/binary>> = Key,
    Bitstring.

-spec compute_bitstring(topic(), time(), keymapper()) -> integer().
compute_bitstring(Topic, Timestamp, #keymapper{source = Source}) ->
    compute_bitstring(Topic, Timestamp, Source, 0).

-spec compute_topic_bitmask(emqx_topic:words(), keymapper()) -> integer().
compute_topic_bitmask(TopicFilter, #keymapper{source = Source}) ->
    compute_topic_bitmask(TopicFilter, Source, 0).

-spec compute_time_bitmask(keymapper()) -> integer().
compute_time_bitmask(#keymapper{source = Source}) ->
    compute_time_bitmask(Source, 0).

-spec hash(term(), bits()) -> integer().
hash(Input, Bits) ->
    % at most 32 bits
    erlang:phash2(Input, 1 bsl Bits).

-spec make_keyspace_filter(emqx_topic:words(), time(), keymapper()) -> keyspace_filter().
make_keyspace_filter(TopicFilter, StartTime, Keymapper) ->
    Bitstring = compute_bitstring(TopicFilter, StartTime, Keymapper),
    HashBitmask = compute_topic_bitmask(TopicFilter, Keymapper),
    TimeBitmask = compute_time_bitmask(Keymapper),
    HashBitfilter = Bitstring band HashBitmask,
    TimeBitfilter = Bitstring band TimeBitmask,
    #filter{
        keymapper = Keymapper,
        topic_filter = TopicFilter,
        start_time = StartTime,
        hash_bitfilter = HashBitfilter,
        hash_bitmask = HashBitmask,
        time_bitfilter = TimeBitfilter,
        time_bitmask = TimeBitmask
    }.

-spec compute_initial_seek(keyspace_filter()) -> integer().
compute_initial_seek(#filter{hash_bitfilter = HashBitfilter, time_bitfilter = TimeBitfilter}) ->
    % Should be the same as `compute_initial_seek(0, Filter)`.
    HashBitfilter bor TimeBitfilter.

-spec compute_next_seek(integer(), keyspace_filter()) -> integer().
compute_next_seek(
    Bitstring,
    Filter = #filter{
        hash_bitfilter = HashBitfilter,
        hash_bitmask = HashBitmask,
        time_bitfilter = TimeBitfilter,
        time_bitmask = TimeBitmask
    }
) ->
    HashMatches = topic_hash_matches(Bitstring, HashBitfilter, HashBitmask),
    TimeMatches = time_matches(Bitstring, TimeBitfilter, TimeBitmask),
    compute_next_seek(HashMatches, TimeMatches, Bitstring, Filter).

%%================================================================================
%% Internal functions
%%================================================================================

compute_bitstring(Topic, Timestamp, [{timestamp, Offset, Size} | Rest], Acc) ->
    I = (Timestamp bsr Offset) band ones(Size),
    compute_bitstring(Topic, Timestamp, Rest, bitwise_concat(Acc, I, Size));
compute_bitstring([], Timestamp, [{hash, level, Size} | Rest], Acc) ->
    I = hash(<<"/">>, Size),
    compute_bitstring([], Timestamp, Rest, bitwise_concat(Acc, I, Size));
compute_bitstring([Level | Tail], Timestamp, [{hash, level, Size} | Rest], Acc) ->
    I = hash(Level, Size),
    compute_bitstring(Tail, Timestamp, Rest, bitwise_concat(Acc, I, Size));
compute_bitstring(Tail, Timestamp, [{hash, levels, Size} | Rest], Acc) ->
    I = hash(Tail, Size),
    compute_bitstring(Tail, Timestamp, Rest, bitwise_concat(Acc, I, Size));
compute_bitstring(_, _, [], Acc) ->
    Acc.

compute_topic_bitmask(Filter, [{timestamp, _, Size} | Rest], Acc) ->
    compute_topic_bitmask(Filter, Rest, bitwise_concat(Acc, 0, Size));
compute_topic_bitmask(['#'], [{hash, _, Size} | Rest], Acc) ->
    compute_topic_bitmask(['#'], Rest, bitwise_concat(Acc, 0, Size));
compute_topic_bitmask(['+' | Tail], [{hash, _, Size} | Rest], Acc) ->
    compute_topic_bitmask(Tail, Rest, bitwise_concat(Acc, 0, Size));
compute_topic_bitmask([], [{hash, level, Size} | Rest], Acc) ->
    compute_topic_bitmask([], Rest, bitwise_concat(Acc, ones(Size), Size));
compute_topic_bitmask([_ | Tail], [{hash, level, Size} | Rest], Acc) ->
    compute_topic_bitmask(Tail, Rest, bitwise_concat(Acc, ones(Size), Size));
compute_topic_bitmask(Tail, [{hash, levels, Size} | Rest], Acc) ->
    Mask =
        case lists:member('+', Tail) orelse lists:member('#', Tail) of
            true -> 0;
            false -> ones(Size)
        end,
    compute_topic_bitmask([], Rest, bitwise_concat(Acc, Mask, Size));
compute_topic_bitmask(_, [], Acc) ->
    Acc.

compute_time_bitmask([{timestamp, _, Size} | Rest], Acc) ->
    compute_time_bitmask(Rest, bitwise_concat(Acc, ones(Size), Size));
compute_time_bitmask([{hash, _, Size} | Rest], Acc) ->
    compute_time_bitmask(Rest, bitwise_concat(Acc, 0, Size));
compute_time_bitmask([], Acc) ->
    Acc.

bitwise_concat(Acc, Item, ItemSize) ->
    (Acc bsl ItemSize) bor Item.

ones(Bits) ->
    1 bsl Bits - 1.

-spec successor(key()) -> key().
successor(Key) ->
    <<Key/binary, 0:8>>.

%% |123|345|678|
%%  foo bar baz

%% |123|000|678| - |123|fff|678|

%%  foo +   baz

%% |fff|000|fff|

%% |123|000|678|

%% |123|056|678| & |fff|000|fff| = |123|000|678|.

match_next(
    Bitstring,
    Value,
    Filter = #filter{
        topic_filter = TopicFilter,
        hash_bitfilter = HashBitfilter,
        hash_bitmask = HashBitmask,
        time_bitfilter = TimeBitfilter,
        time_bitmask = TimeBitmask
    }
) ->
    HashMatches = topic_hash_matches(Bitstring, HashBitfilter, HashBitmask),
    TimeMatches = time_matches(Bitstring, TimeBitfilter, TimeBitmask),
    case HashMatches and TimeMatches of
        true ->
            Message = {Topic, _Payload} = unwrap_message_value(Value),
            case emqx_topic:match(Topic, TopicFilter) of
                true ->
                    Message;
                false ->
                    next
            end;
        false ->
            compute_next_seek(HashMatches, TimeMatches, Bitstring, Filter)
    end.

%% `Bitstring` is out of the hash space defined by `HashBitfilter`.
compute_next_seek(
    _HashMatches = false,
    _TimeMatches,
    Bitstring,
    Filter = #filter{
        keymapper = Keymapper,
        hash_bitfilter = HashBitfilter,
        hash_bitmask = HashBitmask,
        time_bitfilter = TimeBitfilter,
        time_bitmask = TimeBitmask
    }
) ->
    NextBitstring = compute_topic_seek(Bitstring, HashBitfilter, HashBitmask, Keymapper),
    case NextBitstring of
        none ->
            none;
        _ ->
            TimeMatches = time_matches(NextBitstring, TimeBitfilter, TimeBitmask),
            compute_next_seek(true, TimeMatches, NextBitstring, Filter)
    end;
%% `Bitstring` is out of the time range defined by `TimeBitfilter`.
compute_next_seek(
    _HashMatches = true,
    _TimeMatches = false,
    Bitstring,
    #filter{
        time_bitfilter = TimeBitfilter,
        time_bitmask = TimeBitmask
    }
) ->
    compute_time_seek(Bitstring, TimeBitfilter, TimeBitmask);
compute_next_seek(true, true, Bitstring, _It) ->
    Bitstring.

topic_hash_matches(Bitstring, HashBitfilter, HashBitmask) ->
    (Bitstring band HashBitmask) == HashBitfilter.

time_matches(Bitstring, TimeBitfilter, TimeBitmask) ->
    (Bitstring band TimeBitmask) >= TimeBitfilter.

compute_time_seek(Bitstring, TimeBitfilter, TimeBitmask) ->
    % Replace the bits of the timestamp in `Bistring` with bits from `Timebitfilter`.
    (Bitstring band (bnot TimeBitmask)) bor TimeBitfilter.

%% Find the closest bitstring which is:
%% * greater than `Bitstring`,
%% * and falls into the hash space defined by `HashBitfilter`.
%% Note that the result can end up "back" in time and out of the time range.
compute_topic_seek(Bitstring, HashBitfilter, HashBitmask, Keymapper) ->
    Sources = Keymapper#keymapper.source,
    Size = Keymapper#keymapper.bitsize,
    compute_topic_seek(Bitstring, HashBitfilter, HashBitmask, Sources, Size).

compute_topic_seek(Bitstring, HashBitfilter, HashBitmask, Sources, Size) ->
    % NOTE
    % We're iterating through `Substring` here, in lockstep with `HashBitfilter`
    % and `HashBitmask`, starting from least signigicant bits. Each bitsource in
    % `Sources` has a bitsize `S` and, accordingly, gives us a sub-bitstring `S`
    % bits long which we interpret as a "digit". There are 2 flavors of those
    % "digits":
    %  * regular digit with 2^S possible values,
    %  * degenerate digit with exactly 1 possible value U (represented with 0).
    % Our goal here is to find a successor of `Bistring` and perform a kind of
    % digit-by-digit addition operation with carry propagation.
    NextSeek = zipfoldr3(
        fun(Source, Substring, Filter, LBitmask, Offset, Acc) ->
            case Source of
                {hash, _, S} when LBitmask =:= 0 ->
                    % Regular case
                    bitwise_add_digit(Substring, Acc, S, Offset);
                {hash, _, _} when LBitmask =/= 0, Substring < Filter ->
                    % Degenerate case, I_digit < U, no overflow.
                    % Successor is `U bsl Offset` which is equivalent to 0.
                    0;
                {hash, _, S} when LBitmask =/= 0, Substring > Filter ->
                    % Degenerate case, I_digit > U, overflow.
                    % Successor is `(1 bsl Size + U) bsl Offset`.
                    overflow_digit(S, Offset);
                {hash, _, S} when LBitmask =/= 0 ->
                    % Degenerate case, I_digit = U
                    % Perform digit addition with I_digit = 0, assuming "digit" has
                    % 0 bits of information (but is `S` bits long at the same time).
                    % This will overflow only if the result of previous iteration
                    % was an overflow.
                    bitwise_add_digit(0, Acc, 0, S, Offset);
                {timestamp, _, S} ->
                    % Regular case
                    bitwise_add_digit(Substring, Acc, S, Offset)
            end
        end,
        0,
        Bitstring,
        HashBitfilter,
        HashBitmask,
        Size,
        Sources
    ),
    case NextSeek bsr Size of
        _Carry = 0 ->
            % Found the successor.
            % We need to recover values of those degenerate digits which we
            % represented with 0 during digit-by-digit iteration.
            NextSeek bor (HashBitfilter band HashBitmask);
        _Carry = 1 ->
            % We got "carried away" past the range, time to stop iteration.
            none
    end.

bitwise_add_digit(Digit, Number, Width, Offset) ->
    bitwise_add_digit(Digit, Number, Width, Width, Offset).

%% Add "digit" (represented with integer `Digit`) to the `Number` assuming
%% this digit starts at `Offset` bits in `Number` and is `Width` bits long.
%% Perform an overflow if the result of addition would not fit into `Bits`
%% bits.
bitwise_add_digit(Digit, Number, Bits, Width, Offset) ->
    Sum = (Digit bsl Offset) + Number,
    case (Sum bsr Offset) < (1 bsl Bits) of
        true -> Sum;
        false -> overflow_digit(Width, Offset)
    end.

%% Constuct a number which denotes an overflow of digit that starts at
%% `Offset` bits and is `Width` bits long.
overflow_digit(Width, Offset) ->
    (1 bsl Width) bsl Offset.

%% Iterate through sub-bitstrings of 3 integers in lockstep, starting from least
%% significant bits first.
%%
%% Each integer is assumed to be `Size` bits long. Lengths of sub-bitstring are
%% specified in `Sources` list, in order from most significant bits to least
%% significant. Each iteration calls `FoldFun` with:
%% * bitsource that was used to extract sub-bitstrings,
%% * 3 sub-bitstrings in integer representation,
%% * bit offset into integers,
%% * current accumulator.
-spec zipfoldr3(FoldFun, Acc, integer(), integer(), integer(), _Size :: bits(), [bitsource()]) ->
    Acc
when
    FoldFun :: fun((bitsource(), integer(), integer(), integer(), _Offset :: bits(), Acc) -> Acc).
zipfoldr3(_FoldFun, Acc, _, _, _, 0, []) ->
    Acc;
zipfoldr3(FoldFun, Acc, I1, I2, I3, Offset, [Source = {_, _, S} | Rest]) ->
    OffsetNext = Offset - S,
    AccNext = zipfoldr3(FoldFun, Acc, I1, I2, I3, OffsetNext, Rest),
    FoldFun(
        Source,
        substring(I1, OffsetNext, S),
        substring(I2, OffsetNext, S),
        substring(I3, OffsetNext, S),
        OffsetNext,
        AccNext
    ).

substring(I, Offset, Size) ->
    (I bsr Offset) band ones(Size).

%% @doc Generate a column family ID for the MQTT messages
-spec data_cf(emqx_replay_local_store:gen_id()) -> [char()].
data_cf(GenId) ->
    ?MODULE_STRING ++ integer_to_list(GenId).

make_refresh_counter({every, N}) when is_integer(N), N > 0 ->
    {0, N};
make_refresh_counter(undefined) ->
    undefined.

maybe_refresh_iterator(It = #it{refresh_counter = {N, N}}) ->
    refresh_iterator(It#it{refresh_counter = {0, N}});
maybe_refresh_iterator(It = #it{refresh_counter = {M, N}}) ->
    It#it{refresh_counter = {M + 1, N}};
maybe_refresh_iterator(It = #it{refresh_counter = undefined}) ->
    It.

stop_iteration(It) ->
    ok = rocksdb:iterator_close(It#it.handle),
    none.
