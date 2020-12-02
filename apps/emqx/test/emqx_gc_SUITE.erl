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

-module(emqx_gc_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

t_init(_) ->
    GC1 = emqx_gc:init(#{count => 10, bytes => 0}),
    ?assertEqual(#{cnt => {10, 10}}, emqx_gc:info(GC1)),
    GC2 = emqx_gc:init(#{count => 0, bytes => 10}),
    ?assertEqual(#{oct => {10, 10}}, emqx_gc:info(GC2)),
    GC3 = emqx_gc:init(#{count => 10, bytes => 10}),
    ?assertEqual(#{cnt => {10, 10}, oct => {10, 10}}, emqx_gc:info(GC3)).

t_run(_) ->
    GC = emqx_gc:init(#{count => 10, bytes => 10}),
    ?assertEqual({true, GC}, emqx_gc:run(1, 1000, GC)),
    ?assertEqual({true, GC}, emqx_gc:run(1000, 1, GC)),
    {false, GC1} = emqx_gc:run(1, 1, GC),
    ?assertEqual(#{cnt => {10, 9}, oct => {10, 9}}, emqx_gc:info(GC1)),
    {false, GC2} = emqx_gc:run(2, 2, GC1),
    ?assertEqual(#{cnt => {10, 7}, oct => {10, 7}}, emqx_gc:info(GC2)),
    {false, GC3} = emqx_gc:run(3, 3, GC2),
    ?assertEqual(#{cnt => {10, 4}, oct => {10, 4}}, emqx_gc:info(GC3)),
    ?assertEqual({true, GC}, emqx_gc:run(4, 4, GC3)),
    %% Disabled?
    DisabledGC = emqx_gc:init(#{count => 0, bytes => 0}),
    ?assertEqual({false, DisabledGC}, emqx_gc:run(1, 1, DisabledGC)).

t_info(_) ->
    GC = emqx_gc:init(#{count => 10, bytes => 0}),
    ?assertEqual(#{cnt => {10, 10}}, emqx_gc:info(GC)).

t_reset(_) ->
    GC = emqx_gc:init(#{count => 10, bytes => 10}),
    {false, GC1} = emqx_gc:run(5, 5, GC),
    ?assertEqual(#{cnt => {10, 5}, oct => {10, 5}}, emqx_gc:info(GC1)),
    ?assertEqual(GC, emqx_gc:reset(GC1)),
    DisabledGC = emqx_gc:init(#{count => 0, bytes => 0}),
    ?assertEqual(DisabledGC, emqx_gc:reset(DisabledGC)).

