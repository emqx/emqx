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
%%% emqttd_access rules tests.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_access_tests).

-include("emqttd.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

compile_test() ->
    ?assertMatch({allow, {ipaddr, {"127.0.0.1", _I, _I}}, subscribe, [ [<<"$SYS">>, '#'], ['#'] ]},
                 emqttd_access:compile({allow, {ipaddr, "127.0.0.1"}, subscribe, ["$SYS/#", "#"]})),
    ?assertMatch({allow, {user, <<"testuser">>}, subscribe, [ [<<"a">>, <<"b">>, <<"c">>], [<<"d">>, <<"e">>, <<"f">>, '#'] ]},
                 emqttd_access:compile({allow, {user, "testuser"}, subscribe, ["a/b/c", "d/e/f/#"]})),
    ?assertEqual({allow, {user, <<"admin">>}, pubsub, [ [<<"d">>, <<"e">>, <<"f">>, '#'] ]},
                 emqttd_access:compile({allow, {user, "admin"}, pubsub, ["d/e/f/#"]})),
    ?assertEqual({allow, {client, <<"testClient">>}, publish, [ [<<"testTopics">>, <<"testClient">>] ]},
                 emqttd_access:compile({allow, {client, "testClient"}, publish, ["testTopics/testClient"]})),
    ?assertEqual({allow, all, pubsub, [{pattern, [<<"clients">>, <<"$c">>]}]},
                 emqttd_access:compile({allow, all, pubsub, ["clients/$c"]})),
    ?assertEqual({allow, all, subscribe, [{pattern, [<<"users">>, <<"$u">>, '#']}]},
                 emqttd_access:compile({allow, all, subscribe, ["users/$u/#"]})),
    ?assertEqual({deny, all, subscribe, [ [<<"$SYS">>, '#'], ['#'] ]},
                 emqttd_access:compile({deny, all, subscribe, ["$SYS/#", "#"]})),
    ?assertEqual({allow, all}, emqttd_access:compile({allow, all})),
    ?assertEqual({deny, all},  emqttd_access:compile({deny, all})).

match_test() ->
    User = #mqtt_user{ipaddr = {127,0,0,1}, clientid = <<"testClient">>, username = <<"TestUser">>},
    ?assertEqual({matched, allow}, emqttd_access:match(User, <<"Test/Topic">>, {allow, all})),
    ?assertEqual({matched, deny},  emqttd_access:match(User, <<"Test/Topic">>, {deny, all})).

-endif.


