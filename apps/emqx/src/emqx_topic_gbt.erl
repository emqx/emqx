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

%% @doc Topic index implemetation with gb_trees as the underlying data
%% structure.

-module(emqx_topic_gbt).

-export([new/0]).
-export([size/1]).
-export([insert/4]).
-export([delete/3]).
-export([lookup/4]).
-export([fold/3]).
-export([match/2]).
-export([matches/3]).

-export([get_id/1]).
-export([get_topic/1]).
-export([get_record/2]).

-export_type([t/0, t/2, match/1]).

-type key(ID) :: emqx_trie_search:key(ID).
-type words() :: emqx_trie_search:words().
-type match(ID) :: key(ID).

-opaque t(ID, Value) :: gb_trees:tree(key(ID), Value).
-type t() :: t(_ID, _Value).

%% @doc Create a new gb_tree and store it in the persitent_term with the
%% given name.
-spec new() -> t(_ID, _Value).
new() ->
    gb_trees:empty().

-spec size(t()) -> non_neg_integer().
size(Gbt) ->
    gb_trees:size(Gbt).

%% @doc Insert a new entry into the index that associates given topic filter to given
%% record ID, and attaches arbitrary record to the entry. This allows users to choose
%% between regular and "materialized" indexes, for example.
-spec insert(emqx_types:topic() | words(), ID, Record, t(ID, Record)) -> t(ID, Record).
insert(Filter, ID, Record, Gbt) ->
    Key = key(Filter, ID),
    gb_trees:enter(Key, Record, Gbt).

%% @doc Delete an entry from the index that associates given topic filter to given
%% record ID. Deleting non-existing entry is not an error.
-spec delete(emqx_types:topic() | words(), ID, t(ID, Record)) -> t(ID, Record).
delete(Filter, ID, Gbt) ->
    Key = key(Filter, ID),
    gb_trees:delete_any(Key, Gbt).

-spec lookup(emqx_types:topic() | words(), ID, t(ID, Record), Default) -> Record | Default.
lookup(Filter, ID, Gbt, Default) ->
    Key = key(Filter, ID),
    case gb_trees:lookup(Key, Gbt) of
        {value, Record} ->
            Record;
        none ->
            Default
    end.

-spec fold(fun((key(ID), Record, Acc) -> Acc), Acc, t(ID, Record)) -> Acc.
fold(Fun, Acc, Gbt) ->
    Iter = gb_trees:iterator(Gbt),
    fold_iter(Fun, Acc, Iter).

fold_iter(Fun, Acc, Iter) ->
    case gb_trees:next(Iter) of
        {Key, Record, NIter} ->
            fold_iter(Fun, Fun(Key, Record, Acc), NIter);
        none ->
            Acc
    end.

%% @doc Match given topic against the index and return the first match, or `false` if
%% no match is found.
-spec match(emqx_types:topic(), t(ID, _Record)) -> match(ID) | false.
match(Topic, Gbt) ->
    emqx_trie_search:match(Topic, make_nextf(Gbt)).

%% @doc Match given topic against the index and return _all_ matches.
%% If `unique` option is given, return only unique matches by record ID.
-spec matches(emqx_types:topic(), t(ID, _Record), emqx_trie_search:opts()) -> [match(ID)].
matches(Topic, Gbt, Opts) ->
    emqx_trie_search:matches(Topic, make_nextf(Gbt), Opts).

%% @doc Extract record ID from the match.
-spec get_id(match(ID)) -> ID.
get_id(Key) ->
    emqx_trie_search:get_id(Key).

%% @doc Extract topic (or topic filter) from the match.
-spec get_topic(match(_ID)) -> emqx_types:topic().
get_topic(Key) ->
    emqx_trie_search:get_topic(Key).

%% @doc Fetch the record associated with the match.
-spec get_record(match(ID), t(ID, Record)) -> Record.
get_record(Key, Gbt) ->
    gb_trees:get(Key, Gbt).

key(TopicOrFilter, ID) ->
    emqx_trie_search:make_key(TopicOrFilter, ID).

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

make_nextf({_Size, Tree}) ->
    fun(Key) -> gbt_next(Tree, Key) end.
