%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_access_rule_tests).

-import(emqttd_access_rule, [compile/1, match/3]).

-include("emqttd.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

compile_test() ->

    ?assertMatch({allow, {'and', [{ipaddr, {"127.0.0.1", _I, _I}},
                                      {user, <<"user">>}]}, subscribe, [ [<<"$SYS">>, '#'], ['#'] ]},
                 compile({allow, {'and', [{ipaddr, "127.0.0.1"}, {user, <<"user">>}]}, subscribe, ["$SYS/#", "#"]})),
    ?assertMatch({allow, {'or', [{ipaddr, {"127.0.0.1", _I, _I}},
                                 {user, <<"user">>}]}, subscribe, [ [<<"$SYS">>, '#'], ['#'] ]},
                 compile({allow, {'or', [{ipaddr, "127.0.0.1"}, {user, <<"user">>}]}, subscribe, ["$SYS/#", "#"]})),

    ?assertMatch({allow, {ipaddr, {"127.0.0.1", _I, _I}}, subscribe, [ [<<"$SYS">>, '#'], ['#'] ]},
                 compile({allow, {ipaddr, "127.0.0.1"}, subscribe, ["$SYS/#", "#"]})),
    ?assertMatch({allow, {user, <<"testuser">>}, subscribe, [ [<<"a">>, <<"b">>, <<"c">>], [<<"d">>, <<"e">>, <<"f">>, '#'] ]},
                 compile({allow, {user, "testuser"}, subscribe, ["a/b/c", "d/e/f/#"]})),
    ?assertEqual({allow, {user, <<"admin">>}, pubsub, [ [<<"d">>, <<"e">>, <<"f">>, '#'] ]},
                 compile({allow, {user, "admin"}, pubsub, ["d/e/f/#"]})),
    ?assertEqual({allow, {client, <<"testClient">>}, publish, [ [<<"testTopics">>, <<"testClient">>] ]},
                 compile({allow, {client, "testClient"}, publish, ["testTopics/testClient"]})),
    ?assertEqual({allow, all, pubsub, [{pattern, [<<"clients">>, <<"$c">>]}]},
                 compile({allow, all, pubsub, ["clients/$c"]})),
    ?assertEqual({allow, all, subscribe, [{pattern, [<<"users">>, <<"$u">>, '#']}]},
                 compile({allow, all, subscribe, ["users/$u/#"]})),
    ?assertEqual({deny, all, subscribe, [ [<<"$SYS">>, '#'], ['#'] ]},
                 compile({deny, all, subscribe, ["$SYS/#", "#"]})),
    ?assertEqual({allow, all}, compile({allow, all})),
    ?assertEqual({deny, all},  compile({deny, all})).

match_test() ->
    User = #mqtt_client{peername = {{127,0,0,1}, 2948}, client_id = <<"testClient">>, username = <<"TestUser">>},
    User2 = #mqtt_client{peername = {{192,168,0,10}, 3028}, client_id = <<"testClient">>, username = <<"TestUser">>},
    
    ?assertEqual({matched, allow}, match(User, <<"Test/Topic">>, {allow, all})),
    ?assertEqual({matched, deny},  match(User, <<"Test/Topic">>, {deny, all})),
    ?assertMatch({matched, allow}, match(User, <<"Test/Topic">>,
                 compile({allow, {ipaddr, "127.0.0.1"}, subscribe, ["$SYS/#", "#"]}))),
    ?assertMatch({matched, allow}, match(User2, <<"Test/Topic">>,
                 compile({allow, {ipaddr, "192.168.0.1/24"}, subscribe, ["$SYS/#", "#"]}))),
    ?assertMatch({matched, allow}, match(User, <<"d/e/f/x">>, compile({allow, {user, "TestUser"}, subscribe, ["a/b/c", "d/e/f/#"]}))),
    ?assertEqual(nomatch, match(User, <<"d/e/f/x">>, compile({allow, {user, "admin"}, pubsub, ["d/e/f/#"]}))),
    ?assertMatch({matched, allow}, match(User, <<"testTopics/testClient">>,
                 compile({allow, {client, "testClient"}, publish, ["testTopics/testClient"]}))),
    ?assertMatch({matched, allow}, match(User, <<"clients/testClient">>,
                                                       compile({allow, all, pubsub, ["clients/$c"]}))),
    ?assertMatch({matched, allow}, match(#mqtt_client{username = <<"user2">>}, <<"users/user2/abc/def">>,
                                         compile({allow, all, subscribe, ["users/$u/#"]}))),
    ?assertMatch({matched, deny}, match(User, <<"d/e/f">>,
                                        compile({deny, all, subscribe, ["$SYS/#", "#"]}))),
    Rule = compile({allow, {'and', [{ipaddr, "127.0.0.1"}, {user, <<"WrongUser">>}]}, publish, <<"Topic">>}),
    ?assertMatch(nomatch, match(User, <<"Topic">>, Rule)),
    AndRule = compile({allow, {'and', [{ipaddr, "127.0.0.1"}, {user, <<"TestUser">>}]}, publish, <<"Topic">>}),
    ?assertMatch({matched, allow}, match(User, <<"Topic">>, AndRule)),
    OrRule = compile({allow, {'or', [{ipaddr, "127.0.0.1"}, {user, <<"WrongUser">>}]}, publish, ["Topic"]}),
    ?assertMatch({matched, allow}, match(User, <<"Topic">>, OrRule)).

-endif.


