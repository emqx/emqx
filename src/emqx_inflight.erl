%%%===================================================================
%%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%===================================================================

-module(emqx_inflight).

-export([new/1, contain/2, lookup/2, insert/3, update/3, delete/2, values/1,
         to_list/1, size/1, max_size/1, is_full/1, is_empty/1, window/1]).

-type(inflight() :: {max_size, gb_trees:tree()}).

-export_type([inflight/0]).

-spec(new(non_neg_integer()) -> inflight()).
new(MaxSize) when MaxSize >= 0 ->
    {MaxSize, gb_trees:empty()}.

-spec(contain(Key :: term(), inflight()) -> boolean()).
contain(Key, {_MaxSize, Tree}) ->
    gb_trees:is_defined(Key, Tree).

-spec(lookup(Key :: term(), inflight()) -> {value, term()} | none).
lookup(Key, {_MaxSize, Tree}) ->
    gb_trees:lookup(Key, Tree).

-spec(insert(Key :: term(), Value :: term(), inflight()) -> inflight()).
insert(Key, Value, {MaxSize, Tree}) ->
    {MaxSize, gb_trees:insert(Key, Value, Tree)}.

-spec(delete(Key :: term(), inflight()) -> inflight()).
delete(Key, {MaxSize, Tree}) ->
    {MaxSize, gb_trees:delete(Key, Tree)}.

-spec(update(Key :: term(), Val :: term(), inflight()) -> inflight()).
update(Key, Val, {MaxSize, Tree}) ->
    {MaxSize, gb_trees:update(Key, Val, Tree)}.

-spec(is_full(inflight()) -> boolean()).
is_full({0, _Tree}) ->
    false;
is_full({MaxSize, Tree}) ->
    MaxSize =< gb_trees:size(Tree).

-spec(is_empty(inflight()) -> boolean()).
is_empty({_MaxSize, Tree}) ->
    gb_trees:is_empty(Tree).

-spec(smallest(inflight()) -> {K :: term(), V :: term()}).
smallest({_MaxSize, Tree}) ->
    gb_trees:smallest(Tree).

-spec(largest(inflight()) -> {K :: term(), V :: term()}).
largest({_MaxSize, Tree}) ->
    gb_trees:largest(Tree).

-spec(values(inflight()) -> list()).
values({_MaxSize, Tree}) ->
    gb_trees:values(Tree).

-spec(to_list(inflight()) -> list({K :: term(), V :: term()})).
to_list({_MaxSize, Tree}) ->
    gb_trees:to_list(Tree).

-spec(window(inflight()) -> list()).
window(Inflight = {_MaxSize, Tree}) ->
    case gb_trees:is_empty(Tree) of
        true  -> [];
        false -> [Key || {Key, _Val} <- [smallest(Inflight), largest(Inflight)]]
    end.

-spec(size(inflight()) -> non_neg_integer()).
size({_MaxSize, Tree}) ->
    gb_trees:size(Tree).

-spec(max_size(inflight()) -> non_neg_integer()).
max_size({MaxSize, _Tree}) ->
    MaxSize.

