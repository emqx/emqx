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

%% @doc Topic index implemetation with gb_tree as a persistent term.
%% This is only suitable for a static set of topic or topic-filters.

-module(emqx_topic_gbt_pterm).

-export([new/0, new/1]).
-export([insert/4]).
-export([delete/3]).
-export([match/2]).
-export([matches/3]).

-export([get_record/2]).

-type name() :: any().
-type match(ID) :: emqx_topic_gbt:match(ID).

%% @private Only for testing.
-spec new() -> name().
new() ->
    new(test).

-spec new(name()) -> name().
new(Name) ->
    true = pterm_update(Name, emqx_topic_gbt:new()),
    Name.

-spec insert(emqx_types:topic() | emqx_trie_search:words(), _ID, _Record, name()) -> true.
insert(Filter, ID, Record, Name) ->
    pterm_update(Name, emqx_topic_gbt:insert(Filter, ID, Record, pterm(Name))).

-spec delete(emqx_types:topic() | emqx_trie_search:words(), _ID, name()) -> name().
delete(Filter, ID, Name) ->
    pterm_update(Name, emqx_topic_gbt:delete(Filter, ID, pterm(Name))).

-spec match(emqx_types:topic(), name()) -> match(_ID) | false.
match(Topic, Name) ->
    emqx_topic_gbt:match(Topic, pterm(Name)).

-spec matches(emqx_types:topic(), name(), emqx_trie_search:opts()) -> [match(_ID)].
matches(Topic, Name, Opts) ->
    emqx_topic_gbt:matches(Topic, pterm(Name), Opts).

%% @doc Fetch the record associated with the match.
-spec get_record(match(_ID), name()) -> _Record.
get_record(Key, Name) ->
    emqx_topic_gbt:get_record(Key, pterm(Name)).

%%

pterm(Name) ->
    persistent_term:get({?MODULE, Name}).

pterm_update(Name, Tree) ->
    persistent_term:put({?MODULE, Name}, Tree),
    true.
