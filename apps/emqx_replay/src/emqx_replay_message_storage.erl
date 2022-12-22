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
    db :: rocksdb:db_handle()
}).

-record(it, {
    handle :: rocksdb:itr_handle(),
    topic_filter :: emqx_topic:words(),
    bitmask :: integer(),
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
            {ok, #db{db = Handle}};
        Error ->
            Error
    end.

-spec close(db()) -> ok | {error, _}.
close(#db{db = DB}) ->
    rocksdb:close(DB).

-spec store(db(), emqx_guid:guid(), time(), topic(), binary()) ->
    ok.
store(#db{db = DB}, MessageID, PublishedAt, Topic, MessagePayload) ->
    Key = make_message_key(MessageID, Topic, PublishedAt),
    Value = make_message_value(Topic, MessagePayload),
    rocksdb:put(DB, Key, Value, [{sync, true}]).

-spec make_iterator(db(), emqx_topic:words(), time() | earliest) ->
    {ok, iterator()} | {error, invalid_start_time}.
make_iterator(#db{db = DBHandle}, TopicFilter, StartTime) ->
    case rocksdb:iterator(DBHandle, []) of
        {ok, ITHandle} ->
            #it{
                handle = ITHandle,
                topic_filter = TopicFilter,
                start_time = StartTime,
                bitmask = make_bitmask(TopicFilter)
            };
        Err ->
            Err
    end.

-spec next(iterator()) -> {value, binary()} | none.
next(It) ->
    error(noimpl).

%%================================================================================
%% Internal exports
%%================================================================================

-define(TOPIC_LEVELS_ENTROPY_BITS, [8, 8, 32, 16]).

make_message_key(MessageID, Topic, PublishedAt) ->
    combine(compute_topic_hash(Topic), PublishedAt, MessageID).

make_message_value(Topic, MessagePayload) ->
    term_to_binary({Topic, MessagePayload}).

combine(TopicHash, PublishedAt, MessageID) ->
    <<TopicHash:64/integer, PublishedAt:64/integer, MessageID/binary>>.

compute_topic_hash(Topic) ->
    compute_topic_hash(Topic, ?TOPIC_LEVELS_ENTROPY_BITS, 0).

hash(Input, Bits) ->
    % at most 32 bits
    erlang:phash2(Input, 1 bsl Bits).

-spec make_bitmask(emqx_topic:words()) -> integer().
make_bitmask(TopicFilter) ->
    make_bitmask(TopicFilter, ?TOPIC_LEVELS_ENTROPY_BITS).

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

make_bitmask(LevelsRest, [Bits], Acc) ->
    Hash = hash(LevelsRest, Bits),
    Acc bsl Bits + Hash;
make_bitmask([], [Bits | BitsRest], Acc) ->
    Hash = hash(<<"/">>, Bits),
    make_bitmask([], BitsRest, Acc bsl Bits + Hash);
make_bitmask([Level | LevelsRest], [Bits | BitsRest], Acc) ->
    Hash = case Level of
               '+' ->
                   0;
               Bin when is_binary(Bin) ->
                   1 bsl Bits - 1;

    Hash = hash(Level, Bits),
    make_bitmask(LevelsRest, BitsRest, Acc bsl Bits + Hash).

%% |123|345|678|
%%  foo bar baz

%% |123|000|678| - |123|fff|678|

%%  foo +   baz

%% |fff|000|fff|

%% |123|000|678|

%% |123|056|678| & |fff|000|fff| = |123|000|678|.
