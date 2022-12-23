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

-export([store/5]).
-export([make_iterator/3]).
-export([next/1]).

%% Debug/troubleshooting:
-export([make_message_key/3, compute_topic_hash/1, hash/2, combine/3]).

-export_type([db/0, iterator/0]).

%%================================================================================
%% Type declarations
%%================================================================================

%% see rocksdb:db_options()
-type options() :: proplists:proplist().

%% parsed
-type topic() :: list(binary()).

%% TODO granularity?
-type time() :: integer().

-record(db, {
    handle :: rocksdb:db_handle()
}).

-record(it, {
    handle :: rocksdb:itr_handle(),
    next_action :: {seek, binary()} | next,
    topic_filter :: emqx_topic:words(),
    hash_filter :: integer(),
    hash_bitmask :: integer(),
    start_time :: time()
}).

-opaque db() :: #db{}.

-opaque iterator() :: #it{}.

%%================================================================================
%% API funcions
%%================================================================================

-spec open(file:filename_all(), options()) ->
    {ok, db()} | {error, _TODO}.
open(Filename, Options) ->
    case rocksdb:open(Filename, [{create_if_missing, true}, Options]) of
        {ok, Handle} ->
            {ok, #db{handle = Handle}};
        Error ->
            Error
    end.

-spec close(db()) -> ok | {error, _}.
close(#db{handle = DB}) ->
    rocksdb:close(DB).

-spec store(db(), emqx_guid:guid(), time(), topic(), binary()) ->
    ok | {error, _TODO}.
store(#db{handle = DB}, MessageID, PublishedAt, Topic, MessagePayload) ->
    Key = make_message_key(MessageID, Topic, PublishedAt),
    Value = make_message_value(Topic, MessagePayload),
    rocksdb:put(DB, Key, Value, [{sync, true}]).

-spec make_iterator(db(), emqx_topic:words(), time() | earliest) ->
    % {error, invalid_start_time}? might just start from the beginning of time
    % and call it a day: client violated the contract anyway.
    {ok, iterator()} | {error, _TODO}.
make_iterator(#db{handle = DBHandle}, TopicFilter, StartTime) ->
    case rocksdb:iterator(DBHandle, []) of
        {ok, ITHandle} ->
            Hash = compute_topic_hash(TopicFilter),
            HashBitmask = make_bitmask(TopicFilter),
            HashFilter = Hash band HashBitmask,
            #it{
                handle = ITHandle,
                next_action = {seek, combine(HashFilter, StartTime, <<>>)},
                topic_filter = TopicFilter,
                start_time = StartTime,
                hash_filter = HashFilter,
                hash_bitmask = HashBitmask
            };
        Err ->
            Err
    end.

-spec next(iterator()) -> {value, binary(), iterator()} | none | {error, closed}.
next(It = #it{next_action = Action}) ->
    case rocksdb:iterator_move(It#it.handle, Action) of
        % spec says `{ok, Key}` is also possible but the implementation says it's not
        {ok, Key, Value} ->
            {TopicHash, PublishedAt} = extract(Key),
            match_next(It, TopicHash, PublishedAt, Value);
        {error, invalid_iterator} ->
            stop_iteration(It);
        {error, iterator_closed} ->
            {error, closed}
    end.

%%================================================================================
%% Internal exports
%%================================================================================

-define(TOPIC_LEVELS_ENTROPY_BITS, [8, 8, 32, 16]).

make_message_key(MessageID, Topic, PublishedAt) ->
    combine(compute_topic_hash(Topic), PublishedAt, MessageID).

make_message_value(Topic, MessagePayload) ->
    term_to_binary({Topic, MessagePayload}).

unwrap_message_value(Binary) ->
    binary_to_term(Binary).

combine(TopicHash, PublishedAt, MessageID) ->
    <<TopicHash:64/integer, PublishedAt:64/integer, MessageID/binary>>.

extract(<<TopicHash:64/integer, PublishedAt:64/integer, _MessageID/binary>>) ->
    {TopicHash, PublishedAt}.

compute_topic_hash(Topic) ->
    compute_topic_hash(Topic, ?TOPIC_LEVELS_ENTROPY_BITS, 0).

hash(Input, Bits) ->
    % at most 32 bits
    erlang:phash2(Input, 1 bsl Bits).

-spec make_bitmask(emqx_topic:words()) -> integer().
make_bitmask(TopicFilter) ->
    make_bitmask(TopicFilter, ?TOPIC_LEVELS_ENTROPY_BITS, 0).

%%================================================================================
%% Internal functions
%%================================================================================

compute_topic_hash(LevelsRest, [Bits], Acc) ->
    Hash = hash(LevelsRest, Bits),
    Acc bsl Bits + Hash;
compute_topic_hash([], [Bits | BitsRest], Acc) ->
    Hash = hash(<<"/">>, Bits),
    compute_topic_hash([], BitsRest, Acc bsl Bits + Hash);
compute_topic_hash([Level | LevelsRest], [Bits | BitsRest], Acc) ->
    Hash = hash(Level, Bits),
    compute_topic_hash(LevelsRest, BitsRest, Acc bsl Bits + Hash).

make_bitmask(['#'], BitsPerLevel, Acc) ->
    Acc bsl lists:sum(BitsPerLevel) + 0;
make_bitmask(['+' | LevelsRest], [Bits | BitsRest], Acc) ->
    make_bitmask(LevelsRest, BitsRest, Acc bsl Bits + 0);
make_bitmask(_, [Bits], Acc) ->
    Acc bsl Bits + ones(Bits);
make_bitmask([], [Bits | BitsRest], Acc) ->
    make_bitmask([], BitsRest, Acc bsl Bits + ones(Bits));
make_bitmask([_ | LevelsRest], [Bits | BitsRest], Acc) ->
    make_bitmask(LevelsRest, BitsRest, Acc bsl Bits + ones(Bits));
make_bitmask(_, [], Acc) ->
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

%% Filter = |123|***|678|
%% Key1   = |123|011|108| → Seek = |123|011|678|
%% Key1   = |123|011|679| → Seek = |123|012|678|
%% Key1   = |123|999|679| → Seek = 1|123|000|678| → eos

%% Filter = |123|***|678|***|
%% Key1   = |123|011|108|121| → Seek = |123|011|678|000|
%% Key1   = |123|011|679|919| → Seek = |123|012|678|000|
%% Key1   = |123|999|679|001| → Seek = 1|123|000|678|000| → eos
%% Key1   = |125|999|179|017| → Seek = 1|123|000|678|000| → eos

match_next(
    It = #it{
        topic_filter = TopicFilter,
        hash_filter = HashFilter,
        hash_bitmask = HashBitmask,
        start_time = StartTime
    },
    TopicHash,
    PublishedAt,
    Value
) ->
    HashMatches = (TopicHash band It#it.hash_bitmask) == It#it.hash_filter,
    TimeMatches = PublishedAt >= It#it.start_time,
    case HashMatches of
        true when TimeMatches ->
            {Topic, MessagePayload} = unwrap_message_value(Value),
            case emqx_topic:match(Topic, TopicFilter) of
                true ->
                    {value, MessagePayload, It#it{next_action = next}};
                false ->
                    next(It#it{next_action = next})
            end;
        true ->
            NextAction = {seek, combine(TopicHash, StartTime, <<>>)},
            next(It#it{next_action = NextAction});
        false ->
            case compute_next_seek(TopicHash, HashFilter, HashBitmask) of
                NextHash when is_integer(NextHash) ->
                    NextAction = {seek, combine(NextHash, StartTime, <<>>)},
                    next(It#it{next_action = NextAction});
                none ->
                    stop_iteration(It)
            end
    end.

stop_iteration(It) ->
    ok = rocksdb:iterator_close(It#it.handle),
    none.

compute_next_seek(TopicHash, HashFilter, HashBitmask) ->
    compute_next_seek(TopicHash, HashFilter, HashBitmask, ?TOPIC_LEVELS_ENTROPY_BITS).

compute_next_seek(TopicHash, HashFilter, HashBitmask, BitsPerLevel) ->
    % NOTE
    % Ok, this convoluted mess implements a sort of _increment operation_ for some
    % strange number in variable bit-width base. There are `Levels` "digits", those
    % with `0` level bitmask have `BitsPerLevel` bit-width and those with `111...`
    % level bitmask have in some sense 0 bits (because they are fixed "digits"
    % with exacly one possible value).
    % TODO make at least remotely readable / optimize later
    Result = zipfoldr3(
        fun(LevelHash, Filter, LevelMask, Bits, Shift, {Carry, Acc}) ->
            %   io:format(user, "~n *** LH: ~.16B / F: ~.16B / M: ~.16B / Bs: ~B / Sh: ~B~n", [LevelHash, Filter, LevelMask, Bits, Shift]),
            %   io:format(user, "~n *** Carry: ~B / Acc: ~.16B~n", [Carry, Acc]),
            case LevelMask of
                0 when Carry == 0 ->
                    {0, Acc + (LevelHash bsl Shift)};
                0 ->
                    LevelHash1 = LevelHash + Carry,
                    NextCarry = LevelHash1 bsr Bits,
                    NextAcc = (LevelHash1 band ones(Bits)) bsl Shift,
                    {NextCarry, NextAcc};
                _ when (LevelHash + Carry) == Filter ->
                    {0, Acc + (Filter bsl Shift)};
                _ when (LevelHash + Carry) > Filter ->
                    {1, Filter bsl Shift};
                _ ->
                    {0, Filter bsl Shift}
            end
        end,
        {1, 0},
        TopicHash,
        HashFilter,
        HashBitmask,
        BitsPerLevel
    ),
    case Result of
        {_, {_Carry = 0, Next}} ->
            Next bor HashFilter;
        {_, {_Carry = 1, _}} ->
            % we got "carried away" past the range, time to stop iteration
            none
    end.

% zipfoldr3(FoldFun, Acc, I1, I2, I3, Shift, [Bits]) ->
%     { Shift + Bits
%     , FoldFun( I1 band ones(Bits)
%              , I2 band ones(Bits)
%              , I3 band ones(Bits)
%              , Bits, Acc ) };
zipfoldr3(_FoldFun, Acc, _, _, _, []) ->
    {0, Acc};
zipfoldr3(FoldFun, Acc, I1, I2, I3, [Bits | Rest]) ->
    {Shift, AccNext} = zipfoldr3(
        FoldFun,
        Acc,
        I1,
        I2,
        I3,
        Rest
    ),
    % { FoldFun(I1 band ones(Bits), I2 band ones(Bits), I3 band ones(Bits), Bits, AccNext).
    {
        Shift + Bits,
        FoldFun(
            (I1 bsr Shift) band ones(Bits),
            (I2 bsr Shift) band ones(Bits),
            (I3 bsr Shift) band ones(Bits),
            Bits,
            Shift,
            AccNext
        )
    }.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

make_test_bitmask(TopicFilter) ->
    make_bitmask(TopicFilter, [3, 4, 5, 2], 0).

bitmask_test_() ->
    [
        ?_assertEqual(
            2#111_1111_11111_11,
            make_test_bitmask([<<"foo">>, <<"bar">>])
        ),
        ?_assertEqual(
            2#111_0000_11111_11,
            make_test_bitmask([<<"foo">>, '+'])
        ),
        ?_assertEqual(
            2#111_0000_00000_11,
            make_test_bitmask([<<"foo">>, '+', '+'])
        ),
        ?_assertEqual(
            2#111_0000_11111_00,
            make_test_bitmask([<<"foo">>, '+', <<"bar">>, '+'])
        )
    ].

wildcard_bitmask_test_() ->
    [
        ?_assertEqual(
            2#000_0000_00000_00,
            make_test_bitmask(['#'])
        ),
        ?_assertEqual(
            2#111_0000_00000_00,
            make_test_bitmask([<<"foo">>, '#'])
        ),
        ?_assertEqual(
            2#111_1111_11111_00,
            make_test_bitmask([<<"foo">>, <<"bar">>, <<"baz">>, '#'])
        ),
        ?_assertEqual(
            2#111_1111_11111_11,
            make_test_bitmask([<<"foo">>, <<"bar">>, <<"baz">>, <<>>, '#'])
        )
    ].

%% Filter = |123|***|678|***|
%% Mask   = |123|***|678|***|
%% Key1   = |123|011|108|121| → Seek = 0 |123|011|678|000|
%% Key2   = |123|011|679|919| → Seek = 0 |123|012|678|000|
%% Key3   = |123|999|679|001| → Seek = 1 |123|000|678|000| → eos
%% Key4   = |125|011|179|017| → Seek = 1 |123|000|678|000| → eos

compute_test_next_seek(TopicHash, HashFilter, HashBitmask) ->
    compute_next_seek(TopicHash, HashFilter, HashBitmask, [8, 8, 16, 12]).

next_seek_test_() ->
    [
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
