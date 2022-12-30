%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% API:
-export([open/2, close/1]).
-export([make_keymapper/1]).

-export([store/5]).
-export([make_iterator/3]).
-export([next/1]).

%% Debug/troubleshooting:
-export([
    make_message_key/4,
    compute_bitstring/3,
    compute_hash_bitmask/2,
    hash/2
]).

-export_type([db/0, iterator/0]).

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

%% see rocksdb:db_options()
-type db_options() :: proplists:proplist().

%% see rocksdb:cf_options()
-type db_cf_options() :: proplists:proplist().

%% see rocksdb:write_options()
-type db_write_options() :: proplists:proplist().

%% see rocksdb:read_options()
-type db_read_options() :: proplists:proplist().

-type options() :: #{
    %% Keymapper.
    keymapper := keymapper(),
    %% Name and options to use to open specific column family.
    column_family => {_Name :: string(), db_cf_options()},
    %% Options to use when opening the DB.
    open_options => db_options(),
    %% Options to use when writing a message to the DB.
    write_options => db_write_options(),
    %% Options to use when iterating over messages in the DB.
    read_options => db_read_options()
}.

-define(DEFAULT_COLUMN_FAMILY, {"default", []}).

-define(DEFAULT_OPEN_OPTIONS, [
    {create_if_missing, true},
    {create_missing_column_families, true}
]).

-define(DEFAULT_WRITE_OPTIONS, [{sync, true}]).
-define(DEFAULT_READ_OPTIONS, []).

-record(db, {
    handle :: rocksdb:db_handle(),
    cf :: rocksdb:cf_handle(),
    keymapper :: keymapper(),
    write_options = [{sync, true}] :: db_write_options(),
    read_options = [] :: db_write_options()
}).

-record(it, {
    handle :: rocksdb:itr_handle(),
    keymapper :: keymapper(),
    next_action :: {seek, binary()} | next,
    topic_filter :: emqx_topic:words(),
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
    tau :: non_neg_integer()
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

%%================================================================================
%% API funcions
%%================================================================================

-spec open(file:filename_all(), options()) ->
    {ok, db()} | {error, _TODO}.
open(Filename, Options) ->
    CFDescriptors =
        case maps:get(column_family, Options, undefined) of
            CF = {_Name, _} ->
                % TODO
                % > When opening a DB in a read-write mode, you need to specify all
                % > Column Families that currently exist in a DB. If that's not the case,
                % > DB::Open call will return Status::InvalidArgument().
                % This probably means that we need the _manager_ (the thing which knows
                % about all the column families there is) to hold the responsibility to
                % open the database and hold all the handles.
                [CF, ?DEFAULT_COLUMN_FAMILY];
            undefined ->
                [?DEFAULT_COLUMN_FAMILY]
        end,
    DBOptions = maps:get(open_options, Options, ?DEFAULT_OPEN_OPTIONS),
    case rocksdb:open(Filename, DBOptions, CFDescriptors) of
        {ok, Handle, [CFHandle | _]} ->
            {ok, #db{
                handle = Handle,
                cf = CFHandle,
                keymapper = maps:get(keymapper, Options),
                write_options = maps:get(write_options, Options, ?DEFAULT_WRITE_OPTIONS),
                read_options = maps:get(read_options, Options, ?DEFAULT_READ_OPTIONS)
            }};
        Error ->
            Error
    end.

-spec close(db()) -> ok | {error, _}.
close(#db{handle = DB}) ->
    rocksdb:close(DB).

-spec make_keymapper(Options) -> keymapper() when
    Options :: #{
        %% Number of bits in a message timestamp.
        timestamp_bits := bits(),
        %% Number of bits in a key allocated to each level in a message topic.
        topic_bits_per_level := bits_per_level(),
        %% Maximum granularity of iteration over time.
        max_tau := time()
    }.
make_keymapper(#{
    timestamp_bits := TimestampBits,
    topic_bits_per_level := BitsPerLevel,
    max_tau := MaxTau
}) ->
    TimestampLSBs = floor(math:log2(MaxTau)),
    TimestampMSBs = TimestampBits - TimestampLSBs,
    NLevels = length(BitsPerLevel),
    {LevelBits, [TailLevelsBits]} = lists:split(NLevels - 1, BitsPerLevel),
    Source = lists:flatten([
        {timestamp, TimestampLSBs, TimestampMSBs},
        [{hash, level, Bits} || Bits <- LevelBits],
        {hash, levels, TailLevelsBits},
        [{timestamp, 0, TimestampLSBs} || TimestampLSBs > 0]
    ]),
    #keymapper{
        source = Source,
        bitsize = lists:sum([S || {_, _, S} <- Source]),
        tau = 1 bsl TimestampLSBs
    }.

-spec store(db(), emqx_guid:guid(), time(), topic(), binary()) ->
    ok | {error, _TODO}.
store(DB = #db{handle = DBHandle, cf = CFHandle}, MessageID, PublishedAt, Topic, MessagePayload) ->
    Key = make_message_key(Topic, PublishedAt, MessageID, DB#db.keymapper),
    Value = make_message_value(Topic, MessagePayload),
    rocksdb:put(DBHandle, CFHandle, Key, Value, DB#db.write_options).

-spec make_iterator(db(), emqx_topic:words(), time() | earliest) ->
    % {error, invalid_start_time}? might just start from the beginning of time
    % and call it a day: client violated the contract anyway.
    {ok, iterator()} | {error, _TODO}.
make_iterator(DB = #db{handle = DBHandle, cf = CFHandle}, TopicFilter, StartTime) ->
    case rocksdb:iterator(DBHandle, CFHandle, DB#db.read_options) of
        {ok, ITHandle} ->
            Bitstring = compute_bitstring(TopicFilter, StartTime, DB#db.keymapper),
            HashBitmask = compute_hash_bitmask(TopicFilter, DB#db.keymapper),
            TimeBitmask = compute_time_bitmask(DB#db.keymapper),
            HashBitfilter = Bitstring band HashBitmask,
            TimeBitfilter = Bitstring band TimeBitmask,
            InitialSeek = combine(HashBitfilter bor TimeBitfilter, <<>>, DB#db.keymapper),
            {ok, #it{
                handle = ITHandle,
                keymapper = DB#db.keymapper,
                next_action = {seek, InitialSeek},
                topic_filter = TopicFilter,
                hash_bitfilter = HashBitfilter,
                hash_bitmask = HashBitmask,
                time_bitfilter = TimeBitfilter,
                time_bitmask = TimeBitmask
            }};
        Err ->
            Err
    end.

-spec next(iterator()) -> {value, binary(), iterator()} | none | {error, closed}.
next(It = #it{next_action = Action}) ->
    case rocksdb:iterator_move(It#it.handle, Action) of
        % spec says `{ok, Key}` is also possible but the implementation says it's not
        {ok, Key, Value} ->
            Bitstring = extract(Key, It#it.keymapper),
            match_next(It, Bitstring, Value);
        {error, invalid_iterator} ->
            stop_iteration(It);
        {error, iterator_closed} ->
            {error, closed}
    end.

%%================================================================================
%% Internal exports
%%================================================================================

make_message_key(Topic, PublishedAt, MessageID, Keymapper) ->
    combine(compute_bitstring(Topic, PublishedAt, Keymapper), MessageID, Keymapper).

make_message_value(Topic, MessagePayload) ->
    term_to_binary({Topic, MessagePayload}).

unwrap_message_value(Binary) ->
    binary_to_term(Binary).

-spec combine(_Bitstring :: integer(), emqx_guid:guid(), keymapper()) ->
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

-spec compute_hash_bitmask(emqx_topic:words(), keymapper()) -> integer().
compute_hash_bitmask(TopicFilter, #keymapper{source = Source}) ->
    compute_hash_bitmask(TopicFilter, Source, 0).

-spec compute_time_bitmask(keymapper()) -> integer().
compute_time_bitmask(#keymapper{source = Source}) ->
    compute_time_bitmask(Source, 0).

hash(Input, Bits) ->
    % at most 32 bits
    erlang:phash2(Input, 1 bsl Bits).

%%================================================================================
%% Internal functions
%%================================================================================

compute_bitstring(Topic, Timestamp, [{timestamp, Offset, Size} | Rest], Acc) ->
    I = (Timestamp bsr Offset) band ones(Size),
    compute_bitstring(Topic, Timestamp, Rest, (Acc bsl Size) + I);
compute_bitstring([], Timestamp, [{hash, level, Size} | Rest], Acc) ->
    I = hash(<<"/">>, Size),
    compute_bitstring([], Timestamp, Rest, (Acc bsl Size) + I);
compute_bitstring([Level | Tail], Timestamp, [{hash, level, Size} | Rest], Acc) ->
    I = hash(Level, Size),
    compute_bitstring(Tail, Timestamp, Rest, (Acc bsl Size) + I);
compute_bitstring(Tail, Timestamp, [{hash, levels, Size} | Rest], Acc) ->
    I = hash(Tail, Size),
    compute_bitstring(Tail, Timestamp, Rest, (Acc bsl Size) + I);
compute_bitstring(_, _, [], Acc) ->
    Acc.

compute_hash_bitmask(Filter, [{timestamp, _, Size} | Rest], Acc) ->
    compute_hash_bitmask(Filter, Rest, (Acc bsl Size) + 0);
compute_hash_bitmask(['#'], [{hash, _, Size} | Rest], Acc) ->
    compute_hash_bitmask(['#'], Rest, (Acc bsl Size) + 0);
compute_hash_bitmask(['+' | Tail], [{hash, _, Size} | Rest], Acc) ->
    compute_hash_bitmask(Tail, Rest, (Acc bsl Size) + 0);
compute_hash_bitmask([], [{hash, level, Size} | Rest], Acc) ->
    compute_hash_bitmask([], Rest, (Acc bsl Size) + ones(Size));
compute_hash_bitmask([_ | Tail], [{hash, level, Size} | Rest], Acc) ->
    compute_hash_bitmask(Tail, Rest, (Acc bsl Size) + ones(Size));
compute_hash_bitmask(_, [{hash, levels, Size} | Rest], Acc) ->
    compute_hash_bitmask([], Rest, (Acc bsl Size) + ones(Size));
compute_hash_bitmask(_, [], Acc) ->
    Acc.

compute_time_bitmask([{timestamp, _, Size} | Rest], Acc) ->
    compute_time_bitmask(Rest, (Acc bsl Size) + ones(Size));
compute_time_bitmask([{hash, _, Size} | Rest], Acc) ->
    compute_time_bitmask(Rest, (Acc bsl Size) + 0);
compute_time_bitmask([], Acc) ->
    Acc.

ones(Bits) ->
    1 bsl Bits - 1.

%% |123|345|678|
%%  foo bar baz

%% |123|000|678| - |123|fff|678|

%%  foo +   baz

%% |fff|000|fff|

%% |123|000|678|

%% |123|056|678| & |fff|000|fff| = |123|000|678|.

match_next(
    It = #it{
        keymapper = Keymapper,
        topic_filter = TopicFilter,
        hash_bitfilter = HashBitfilter,
        hash_bitmask = HashBitmask,
        time_bitfilter = TimeBitfilter,
        time_bitmask = TimeBitmask
    },
    Bitstring,
    Value
) ->
    HashMatches = (Bitstring band HashBitmask) == HashBitfilter,
    TimeMatches = (Bitstring band TimeBitmask) >= TimeBitfilter,
    case HashMatches of
        true when TimeMatches ->
            {Topic, MessagePayload} = unwrap_message_value(Value),
            case emqx_topic:match(Topic, TopicFilter) of
                true ->
                    {value, MessagePayload, It#it{next_action = next}};
                false ->
                    next(It#it{next_action = next})
            end;
        true when not TimeMatches ->
            NextBitstring = (Bitstring band (bnot TimeBitmask)) bor TimeBitfilter,
            NextSeek = combine(NextBitstring, <<>>, Keymapper),
            next(It#it{next_action = {seek, NextSeek}});
        false ->
            % _ ->
            case compute_next_seek(Bitstring, HashBitfilter, HashBitmask, Keymapper) of
                NextBitstring when is_integer(NextBitstring) ->
                    % ct:pal("Bitstring     = ~32.16.0B", [Bitstring]),
                    % ct:pal("Bitfilter     = ~32.16.0B", [Bitfilter]),
                    % ct:pal("HBitmask      = ~32.16.0B", [HashBitmask]),
                    % ct:pal("TBitmask      = ~32.16.0B", [TimeBitmask]),
                    % ct:pal("NextBitstring = ~32.16.0B", [NextBitstring]),
                    NextSeek = combine(NextBitstring, <<>>, Keymapper),
                    next(It#it{next_action = {seek, NextSeek}});
                none ->
                    stop_iteration(It)
            end
    end.

stop_iteration(It) ->
    ok = rocksdb:iterator_close(It#it.handle),
    none.

compute_next_seek(Bitstring, HashBitfilter, HashBitmask, Keymapper) ->
    Sources = Keymapper#keymapper.source,
    Size = Keymapper#keymapper.bitsize,
    compute_next_seek(Bitstring, HashBitfilter, HashBitmask, Sources, Size).

compute_next_seek(Bitstring, HashBitfilter, HashBitmask, Sources, Size) ->
    % NOTE
    % Ok, this convoluted mess implements a sort of _increment operation_ for some
    % strange number in variable bit-width base. There are `Levels` "digits", those
    % with `0` level bitmask have `BitsPerLevel` bit-width and those with `111...`
    % level bitmask have in some sense 0 bits (because they are fixed "digits"
    % with exacly one possible value).
    % TODO make at least remotely readable / optimize later
    Result = zipfoldr3(
        fun(Source, Substring, Filter, LBitmask, Offset, {Carry, Acc}) ->
            case Source of
                {hash, _, _} when LBitmask =:= 0, Carry =:= 0 ->
                    {0, Acc + (Substring bsl Offset)};
                {hash, _, S} when LBitmask =:= 0 ->
                    Substring1 = Substring + Carry,
                    Carry1 = Substring1 bsr S,
                    Acc1 = (Substring1 band ones(S)) bsl Offset,
                    {Carry1, Acc1};
                {hash, _, _} when LBitmask =/= 0, (Substring + Carry) =:= Filter ->
                    {0, Acc + (Filter bsl Offset)};
                {hash, _, _} when LBitmask =/= 0, (Substring + Carry) > Filter ->
                    {1, Filter bsl Offset};
                {hash, _, _} when LBitmask =/= 0 ->
                    {0, Filter bsl Offset};
                {timestamp, _, _} when Carry =:= 0 ->
                    {0, Acc + (Substring bsl Offset)};
                {timestamp, _, S} ->
                    Substring1 = Substring + Carry,
                    Carry1 = Substring1 bsr S,
                    Acc1 = (Substring1 band ones(S)) bsl Offset,
                    {Carry1, Acc1}
            end
        end,
        % TODO
        % We can put carry bit into the `Acc`'s MSB instead of wrapping it into a tuple.
        % This could save us a heap alloc which might be imporatant in a hot path.
        {1, 0},
        Bitstring,
        HashBitfilter,
        HashBitmask,
        Size,
        Sources
    ),
    case Result of
        {_Carry = 0, Next} ->
            Next bor (HashBitfilter band HashBitmask);
        {_Carry = 1, _} ->
            % we got "carried away" past the range, time to stop iteration
            none
    end.

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

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

make_keymapper_test_() ->
    [
        ?_assertEqual(
            #keymapper{
                source = [
                    {timestamp, 9, 23},
                    {hash, level, 2},
                    {hash, level, 4},
                    {hash, levels, 8},
                    {timestamp, 0, 9}
                ],
                bitsize = 46,
                tau = 512
            },
            make_keymapper(#{
                timestamp_bits => 32,
                topic_bits_per_level => [2, 4, 8],
                max_tau => 1000
            })
        ),
        ?_assertEqual(
            #keymapper{
                source = [
                    {timestamp, 0, 32},
                    {hash, levels, 16}
                ],
                bitsize = 48,
                tau = 1
            },
            make_keymapper(#{
                timestamp_bits => 32,
                topic_bits_per_level => [16],
                max_tau => 1
            })
        )
    ].

compute_test_bitmask(TopicFilter) ->
    compute_hash_bitmask(
        TopicFilter,
        [
            {hash, level, 3},
            {hash, level, 4},
            {hash, level, 5},
            {hash, levels, 2}
        ],
        0
    ).

bitmask_test_() ->
    [
        ?_assertEqual(
            2#111_1111_11111_11,
            compute_test_bitmask([<<"foo">>, <<"bar">>])
        ),
        ?_assertEqual(
            2#111_0000_11111_11,
            compute_test_bitmask([<<"foo">>, '+'])
        ),
        ?_assertEqual(
            2#111_0000_00000_11,
            compute_test_bitmask([<<"foo">>, '+', '+'])
        ),
        ?_assertEqual(
            2#111_0000_11111_00,
            compute_test_bitmask([<<"foo">>, '+', <<"bar">>, '+'])
        )
    ].

wildcard_bitmask_test_() ->
    [
        ?_assertEqual(
            2#000_0000_00000_00,
            compute_test_bitmask(['#'])
        ),
        ?_assertEqual(
            2#111_0000_00000_00,
            compute_test_bitmask([<<"foo">>, '#'])
        ),
        ?_assertEqual(
            2#111_1111_11111_00,
            compute_test_bitmask([<<"foo">>, <<"bar">>, <<"baz">>, '#'])
        ),
        ?_assertEqual(
            2#111_1111_11111_11,
            compute_test_bitmask([<<"foo">>, <<"bar">>, <<"baz">>, <<>>, '#'])
        )
    ].

%% Filter = |123|***|678|***|
%% Mask   = |123|***|678|***|
%% Key1   = |123|011|108|121| → Seek = 0 |123|011|678|000|
%% Key2   = |123|011|679|919| → Seek = 0 |123|012|678|000|
%% Key3   = |123|999|679|001| → Seek = 1 |123|000|678|000| → eos
%% Key4   = |125|011|179|017| → Seek = 1 |123|000|678|000| → eos

compute_test_next_seek(Bitstring, Bitfilter, HBitmask) ->
    compute_next_seek(
        Bitstring,
        Bitfilter,
        HBitmask,
        [
            {hash, level, 8},
            {hash, level, 8},
            {hash, level, 16},
            {hash, levels, 12}
        ],
        8 + 8 + 16 + 12
    ).

next_seek_test_() ->
    [
        ?_assertMatch(
            none,
            compute_test_next_seek(
                16#FD_42_4242_043,
                16#FD_42_4242_042,
                16#FF_FF_FFFF_FFF
            )
        ),
        ?_assertMatch(
            16#FD_11_0678_000,
            compute_test_next_seek(
                16#FD_11_0108_121,
                16#FD_00_0678_000,
                16#FF_00_FFFF_000
            )
        ),
        ?_assertMatch(
            16#FD_12_0678_000,
            compute_test_next_seek(
                16#FD_11_0679_919,
                16#FD_00_0678_000,
                16#FF_00_FFFF_000
            )
        ),
        ?_assertMatch(
            none,
            compute_test_next_seek(
                16#FD_FF_0679_001,
                16#FD_00_0678_000,
                16#FF_00_FFFF_000
            )
        ),
        ?_assertMatch(
            none,
            compute_test_next_seek(
                16#FE_11_0179_017,
                16#FD_00_0678_000,
                16#FF_00_FFFF_000
            )
        )
    ].

-endif.
