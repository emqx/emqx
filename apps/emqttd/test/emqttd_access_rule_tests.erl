%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd_access_rule tests.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_access_rule_tests).

-import(emqttd_access_rule, [compile/1, match/3]).

-include("emqttd.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

compile_test() ->
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
    User = #mqtt_user{ipaddr = {127,0,0,1}, clientid = <<"testClient">>, username = <<"TestUser">>},
    User2 = #mqtt_user{ipaddr = {192,168,0,10}, clientid = <<"testClient">>, username = <<"TestUser">>},
    
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
    ?assertMatch({matched, allow}, match(#mqtt_user{username = <<"user2">>}, <<"users/user2/abc/def">>,
                                                                  compile({allow, all, subscribe, ["users/$u/#"]}))),
    ?assertMatch({matched, deny}, 
                 match(User, <<"d/e/f">>,
                                     compile({deny, all, subscribe, ["$SYS/#", "#"]}))).

-endif.


