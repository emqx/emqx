%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([open/2]).

-export([store/5]).
-export([make_iterator/3]).

%% see rocksdb:db_options()
-type options() :: proplists:proplist().

%% parsed
-type topic() :: list(binary()).

%% TODO granularity?
-type time() :: integer().

-record(db, {
    db :: rocksdb:db_handle()
}).

-opaque db() :: #db{}.
-opaque iterator() :: _TODO.

-spec open(file:filename_all(), options()) ->
    {ok, db()} | {error, _TODO}.
open(Filename, Options) ->
    #db{
        db = rocksdb:open(Filename, Options)
    }.

-spec store(db(), emqx_guid:guid(), time(), topic(), binary()) ->
    ok.
store(DB, MessageID, PublishedAt, Topic, MessagePayload) ->
    Key = make_message_key(MessageID, Topic, PublishedAt),
    Value = make_message_value(Topic, MessagePayload),
    rocksdb:put(DB, Key, Value, [{sync, true}]).

-spec make_iterator(db(), emqx_topic:words(), time()) ->
    {ok, iterator()} | {error, invalid_start_time}.
make_iterator(DB, TopicFilter, StartTime) ->
    error(noimpl).

%%

-define(TOPIC_LEVELS_ENTROPY_BITS, [8, 8, 32, 16]).

make_message_key(MessageID, Topic, PublishedAt) ->
    combine(compute_topic_hash(Topic), PublishedAt, MessageID).

make_message_value(Topic, MessagePayload) ->
    term_to_binary({Topic, MessagePayload}).

combine(TopicHash, PublishedAt, MessageID) ->
    <<TopicHash:64/integer, PublishedAt:64/integer, MessageID/binary>>.

compute_topic_hash(Topic) ->
    compute_topic_hash(Topic, ?TOPIC_LEVELS_ENTROPY_BITS, 0).

compute_topic_hash([], [Bits | BitsRest], Acc) ->
    Hash = hash(<<"/">>, Bits),
    compute_topic_hash([], BitsRest, Acc bsl Bits + Hash);
compute_topic_hash(LevelsRest, [Bits], Acc) ->
    Hash = hash(LevelsRest, Bits),
    Acc bsl Bits + Hash;
compute_topic_hash([Level | LevelsRest], [Bits | BitsRest], Acc) ->
    Hash = hash(Level, Bits),
    compute_topic_hash(LevelsRest, BitsRest, Acc bsl Bits + Hash).

hash(Input, Bits) ->
    % at most 32 bits
    erlang:phash2(Input, 1 bsl Bits).
