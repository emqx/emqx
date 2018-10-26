%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_router_SUITE).

-include("emqx.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(R, emqx_router).
-define(TABS, [emqx_route, emqx_trie, emqx_trie_node]).

all() ->
    [{group, route}].

groups() ->
    [{route, [sequence],
      [add_del_route,
       match_routes,
       has_routes,
       router_add_del]}].

init_per_suite(Config) ->
    emqx_ct_broker_helpers:run_setup_steps(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_broker_helpers:run_teardown_steps().

init_per_testcase(_TestCase, Config) ->
    clear_tables(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    clear_tables().

add_del_route(_) ->
    From = {self(), make_ref()},
    ?R:add_route(From, <<"a/b/c">>, node()),
    timer:sleep(1),

    ?R:add_route(From, <<"a/b/c">>, node()),
    timer:sleep(1),

    ?R:add_route(From, <<"a/+/b">>, node()),
    ct:log("Topics: ~p ~n", [emqx_topic:wildcard(<<"a/+/b">>)]),
    timer:sleep(1),

    ?assertEqual([<<"a/+/b">>, <<"a/b/c">>], lists:sort(?R:topics())),

    ?R:del_route(From, <<"a/b/c">>, node()),

    ?R:del_route(From, <<"a/+/b">>, node()),
    timer:sleep(1),
    ?assertEqual([], lists:sort(?R:topics())).

match_routes(_) ->
    From = {self(), make_ref()},
    ?R:add_route(From, <<"a/b/c">>, node()),
    ?R:add_route(From, <<"a/+/c">>, node()),
    ?R:add_route(From, <<"a/b/#">>, node()),
    ?R:add_route(From, <<"#">>, node()),
    timer:sleep(1000),
    ?assertEqual([#route{topic = <<"#">>, dest = node()},
                  #route{topic = <<"a/+/c">>, dest = node()},
                  #route{topic = <<"a/b/#">>, dest = node()},
                  #route{topic = <<"a/b/c">>, dest = node()}],
                 lists:sort(?R:match_routes(<<"a/b/c">>))).

has_routes(_) ->
    From = {self(), make_ref()},
    ?R:add_route(From, <<"devices/+/messages">>, node()),
    timer:sleep(200),
    ?assert(?R:has_routes(<<"devices/+/messages">>)).

clear_tables() ->
    lists:foreach(fun mnesia:clear_table/1, ?TABS).

router_add_del(_) ->
    ?R:add_route(<<"#">>),
    ?R:add_route(<<"a/b/c">>, node()),
    ?R:add_route(<<"+/#">>),
    Routes = [R1, R2 | _] = [
            #route{topic = <<"#">>,     dest = node()},
            #route{topic = <<"+/#">>,   dest = node()},
            #route{topic = <<"a/b/c">>, dest = node()}],
    timer:sleep(500),
    ?assertEqual(Routes, lists:sort(?R:match_routes(<<"a/b/c">>))),

    ?R:print_routes(<<"a/b/c">>),

    %% Batch Add
    lists:foreach(fun(R) -> ?R:add_route(R) end, Routes),
    ?assertEqual(Routes, lists:sort(?R:match_routes(<<"a/b/c">>))),

    %% Del
    ?R:del_route(<<"a/b/c">>, node()),
    timer:sleep(500),
    [R1, R2] = lists:sort(?R:match_routes(<<"a/b/c">>)),
    {atomic, []} = mnesia:transaction(fun emqx_trie:lookup/1, [<<"a/b/c">>]),

    %% Batch Del
    R3 = #route{topic = <<"#">>, dest = 'a@127.0.0.1'},
    ?R:add_route(R3),
    ?R:del_route(<<"#">>),
    ?R:del_route(R2),
    ?R:del_route(R3),
    timer:sleep(500),
    [] = lists:sort(?R:match_routes(<<"a/b/c">>)).

