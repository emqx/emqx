%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_inflight).

-compile(inline).

%% APIs
-export([ new/0
        , new/1
        , contain/2
        , lookup/2
        , insert/3
        , update/3
        , resize/2
        , delete/2
        , values/1
        , to_list/1
        , to_list/2
        , size/1
        , max_size/1
        , is_full/1
        , is_empty/1
        , window/1
        ]).

-export_type([inflight/0]).

-type(key() :: term()).

-type(max_size() :: pos_integer()).

-opaque(inflight() :: {inflight, max_size(), gb_trees:tree()}).

-define(Inflight(Tree), {inflight, _MaxSize, Tree}).

-define(Inflight(MaxSize, Tree), {inflight, MaxSize, (Tree)}).

-spec(new() -> inflight()).
new() -> new(0).

-spec(new(non_neg_integer()) -> inflight()).
new(MaxSize) when MaxSize >= 0 ->
    ?Inflight(MaxSize, gb_trees:empty()).

-spec(contain(key(), inflight()) -> boolean()).
contain(Key, ?Inflight(Tree)) ->
    gb_trees:is_defined(Key, Tree).

-spec(lookup(key(), inflight()) -> {value, term()} | none).
lookup(Key, ?Inflight(Tree)) ->
    gb_trees:lookup(Key, Tree).

-spec(insert(key(), Val :: term(), inflight()) -> inflight()).
insert(Key, Val, ?Inflight(MaxSize, Tree)) ->
    ?Inflight(MaxSize, gb_trees:insert(Key, Val, Tree)).

-spec(delete(key(), inflight()) -> inflight()).
delete(Key, ?Inflight(MaxSize, Tree)) ->
    ?Inflight(MaxSize, gb_trees:delete(Key, Tree)).

-spec(update(key(), Val :: term(), inflight()) -> inflight()).
update(Key, Val, ?Inflight(MaxSize, Tree)) ->
    ?Inflight(MaxSize, gb_trees:update(Key, Val, Tree)).

-spec(resize(integer(), inflight()) -> inflight()).
resize(MaxSize, ?Inflight(Tree)) ->
    ?Inflight(MaxSize, Tree).

-spec(is_full(inflight()) -> boolean()).
is_full(?Inflight(0, _Tree)) ->
    false;
is_full(?Inflight(MaxSize, Tree)) ->
    MaxSize =< gb_trees:size(Tree).

-spec(is_empty(inflight()) -> boolean()).
is_empty(?Inflight(Tree)) ->
    gb_trees:is_empty(Tree).

-spec(smallest(inflight()) -> {key(), term()}).
smallest(?Inflight(Tree)) ->
    gb_trees:smallest(Tree).

-spec(largest(inflight()) -> {key(), term()}).
largest(?Inflight(Tree)) ->
    gb_trees:largest(Tree).

-spec(values(inflight()) -> list()).
values(?Inflight(Tree)) ->
    gb_trees:values(Tree).

-spec(to_list(inflight()) -> list({key(), term()})).
to_list(?Inflight(Tree)) ->
    gb_trees:to_list(Tree).

-spec(to_list(fun(), inflight()) -> list({key(), term()})).
to_list(SortFun, ?Inflight(Tree)) ->
    lists:sort(SortFun, gb_trees:to_list(Tree)).

-spec(window(inflight()) -> list()).
window(Inflight = ?Inflight(Tree)) ->
    case gb_trees:is_empty(Tree) of
        true -> [];
        false -> [Key || {Key, _Val} <- [smallest(Inflight), largest(Inflight)]]
    end.

-spec(size(inflight()) -> non_neg_integer()).
size(?Inflight(Tree)) ->
    gb_trees:size(Tree).

-spec(max_size(inflight()) -> non_neg_integer()).
max_size(?Inflight(MaxSize, _Tree)) ->
    MaxSize.

