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

-module(emqx_inflight_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-import(emqx_inflight, [new/1, contain/2, insert/3, lookup/2, update/3,
                        delete/2, is_empty/1, is_full/1]).

all() ->
    [t_contain, t_lookup, t_insert, t_update, t_delete, t_window,
     t_is_full, t_is_empty].

t_contain(_) ->
    ?assertNot(contain(k, new(0))),
    ?assert(contain(k, insert(k, v, new(0)))).

t_lookup(_) ->
    Inflight = insert(k, v, new(0)),
    ?assertEqual({value, v}, lookup(k, Inflight)),
    ?assertEqual(none, lookup(x, Inflight)).

t_insert(_) ->
    Inflight = insert(k2, v2, insert(k1, v1, new(0))),
    ?assertEqual({value, v1}, lookup(k1, Inflight)),
    ?assertEqual({value, v2}, lookup(k2, Inflight)).

t_update(_) ->
    Inflight = update(k, v2, insert(k, v1, new(0))),
    ?assertEqual({value, v2}, lookup(k, Inflight)).

t_delete(_) ->
    ?assert(is_empty(delete(k, insert(k, v1, new(0))))).

t_window(_) ->
    ?assertEqual([], emqx_inflight:window(new(10))),
    Inflight = insert(2, 2, insert(1, 1, new(0))),
    ?assertEqual([1, 2], emqx_inflight:window(Inflight)).

t_is_full(_) ->
    ?assert(is_full(insert(k, v1, new(1)))).

t_is_empty(_) ->
    ?assertNot(is_empty(insert(k, v1, new(1)))).

