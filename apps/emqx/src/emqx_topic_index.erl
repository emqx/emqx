%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Topic index implemetation with ETS table as ordered-set storage.

-module(emqx_topic_index).

-export([new/0, new/1]).
-export([insert/4]).
-export([delete/3]).
-export([match/2]).
-export([matches/3]).
-export([matches_filter/3]).

-export([make_key/2]).

-export([get_id/1]).
-export([get_topic/1]).
-export([get_record/2]).

-export_type([key/1]).

-type key(ID) :: emqx_trie_search:key(ID).
-type match(ID) :: key(ID).
-type words() :: emqx_trie_search:words().

-spec new() -> ets:table().
new() ->
    new([public, {read_concurrency, true}]).

%% @doc Create a new ETS table suitable for topic index.
%% Usable mostly for testing purposes.
-spec new(list()) -> ets:table().
new(Options) ->
    ets:new(?MODULE, [ordered_set | Options]).

%% @doc Insert a new entry into the index that associates given topic filter to given
%% record ID, and attaches arbitrary record to the entry. This allows users to choose
%% between regular and "materialized" indexes, for example.
-spec insert(emqx_types:topic() | words(), _ID, _Record, ets:table()) -> true.
insert(Filter, ID, Record, Tab) ->
    Key = make_key(Filter, ID),
    true = ets:insert(Tab, {Key, Record}).

%% @doc Delete an entry from the index that associates given topic filter to given
%% record ID. Deleting non-existing entry is not an error.
-spec delete(emqx_types:topic() | words(), _ID, ets:table()) -> true.
delete(Filter, ID, Tab) ->
    ets:delete(Tab, make_key(Filter, ID)).

-spec make_key(emqx_types:topic() | words(), ID) -> key(ID).
make_key(TopicOrFilter, ID) ->
    emqx_trie_search:make_key(TopicOrFilter, ID).

%% @doc Match given topic against the index and return the first match, or `false` if
%% no match is found.
-spec match(emqx_types:topic(), ets:table()) -> match(_ID) | false.
match(Topic, Tab) ->
    emqx_trie_search:match(Topic, make_nextf(Tab)).

%% @doc Match given topic against the index and return _all_ matches.
%% If `unique` option is given, return only unique matches by record ID.
-spec matches(emqx_types:topic(), ets:table(), emqx_trie_search:opts()) -> [match(_ID)].
matches(Topic, Tab, Opts) ->
    emqx_trie_search:matches(Topic, make_nextf(Tab), Opts).

%% @doc Match given topic filter against the index and return _all_ matches.
%% If `unique` option is given, return only unique matches by record ID.
-spec matches_filter(emqx_types:topic(), ets:table(), emqx_trie_search:opts()) -> [match(_ID)].
matches_filter(TopicFilter, Tab, Opts) ->
    emqx_trie_search:matches_filter(TopicFilter, make_nextf(Tab), Opts).

%% @doc Extract record ID from the match.
-spec get_id(match(ID)) -> ID.
get_id(Key) ->
    emqx_trie_search:get_id(Key).

%% @doc Extract topic (or topic filter) from the match.
-spec get_topic(match(_ID)) -> emqx_types:topic().
get_topic(Key) ->
    emqx_trie_search:get_topic(Key).

%% @doc Fetch the record associated with the match.
%% May return empty list if the index entry was deleted in the meantime.
%% NOTE: Only really useful for ETS tables where the record data is the last element.
-spec get_record(match(_ID), ets:table()) -> [_Record].
get_record(K, Tab) ->
    case ets:lookup(Tab, K) of
        [Entry] ->
            [erlang:element(tuple_size(Entry), Entry)];
        [] ->
            []
    end.

make_nextf(Tab) ->
    fun(Key) -> ets:next(Tab, Key) end.
