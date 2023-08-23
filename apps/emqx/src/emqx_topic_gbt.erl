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

%% @doc Topic index implemetation with gb_trees stored in persistent_term.
%% This is only suitable for a static set of topic or topic-filters.

-module(emqx_topic_gbt).

-export([new/0, new/1]).
-export([insert/4]).
-export([delete/3]).
-export([match/2]).
-export([matches/3]).

-export([get_id/1]).
-export([get_topic/1]).
-export([get_record/2]).

-type word() :: binary() | '+' | '#'.
-type key(ID) :: {[word()], {ID}}.
-type match(ID) :: key(ID).
-type name() :: any().

%% @private Only for testing.
-spec new() -> name().
new() ->
    new(test).

%% @doc Create a new gb_tree and store it in the persitent_term with the
%% given name.
-spec new(name()) -> name().
new(Name) ->
    T = gb_trees:from_orddict([]),
    true = gbt_update(Name, T),
    Name.

%% @doc Insert a new entry into the index that associates given topic filter to given
%% record ID, and attaches arbitrary record to the entry. This allows users to choose
%% between regular and "materialized" indexes, for example.
-spec insert(emqx_types:topic(), _ID, _Record, name()) -> true.
insert(Filter, ID, Record, Name) ->
    Tree = gbt(Name),
    Key = key(Filter, ID),
    NewTree = gb_trees:enter(Key, Record, Tree),
    true = gbt_update(Name, NewTree).

%% @doc Delete an entry from the index that associates given topic filter to given
%% record ID. Deleting non-existing entry is not an error.
-spec delete(emqx_types:topic(), _ID, name()) -> true.
delete(Filter, ID, Name) ->
    Tree = gbt(Name),
    Key = key(Filter, ID),
    NewTree = gb_trees:delete_any(Key, Tree),
    true = gbt_update(Name, NewTree).

%% @doc Match given topic against the index and return the first match, or `false` if
%% no match is found.
-spec match(emqx_types:topic(), name()) -> match(_ID) | false.
match(Topic, Name) ->
    emqx_trie_search:match(Topic, make_nextf(Name)).

%% @doc Match given topic against the index and return _all_ matches.
%% If `unique` option is given, return only unique matches by record ID.
matches(Topic, Name, Opts) ->
    emqx_trie_search:matches(Topic, make_nextf(Name), Opts).

%% @doc Extract record ID from the match.
-spec get_id(match(ID)) -> ID.
get_id(Key) ->
    emqx_trie_search:get_id(Key).

%% @doc Extract topic (or topic filter) from the match.
-spec get_topic(match(_ID)) -> emqx_types:topic().
get_topic(Key) ->
    emqx_trie_search:get_topic(Key).

%% @doc Fetch the record associated with the match.
-spec get_record(match(_ID), name()) -> _Record.
get_record(Key, Name) ->
    Gbt = gbt(Name),
    gb_trees:get(Key, Gbt).

key(TopicOrFilter, ID) ->
    emqx_trie_search:make_key(TopicOrFilter, ID).

gbt(Name) ->
    persistent_term:get({?MODULE, Name}).

gbt_update(Name, Tree) ->
    persistent_term:put({?MODULE, Name}, Tree),
    true.

gbt_next(nil, _Input) ->
    '$end_of_table';
gbt_next({P, _V, _Smaller, Bigger}, K) when K >= P ->
    gbt_next(Bigger, K);
gbt_next({P, _V, Smaller, _Bigger}, K) ->
    case gbt_next(Smaller, K) of
        '$end_of_table' ->
            P;
        NextKey ->
            NextKey
    end.

make_nextf(Name) ->
    {_SizeWeDontCare, TheTree} = gbt(Name),
    fun(Key) -> gbt_next(TheTree, Key) end.
