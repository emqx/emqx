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

-module(emqttd_access_SUITE).

-compile(export_all).

-include("emqttd.hrl").

-define(AC, emqttd_access_control).

-import(emqttd_access_rule, [compile/1, match/3]).

all() ->
    [{group, access_control},
     {group, access_rule}].

groups() ->
    [{access_control, [sequence],
      [reload_acl,
       register_mod,
       unregister_mod,
       check_acl]},
     {access_rule, [],
      [compile_rule,
       match_rule]}].

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(TestCase, Config) when TestCase =:= reload_acl;
                                         TestCase =:= register_mod;
                                         TestCase =:= unregister_mod;
                                         TestCase =:= check_acl ->
    DataDir = proplists:get_value(data_dir, Config),
    AclOpts = [
        {auth, [{anonymous, []}]},
        {acl,  [{internal, [{file, filename:join([DataDir, "test_acl.config"])},
                            {nomatch, allow}]}]}
    ],
    {ok, _Pid} = ?AC:start_link(AclOpts),
    Config;

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(TestCase, _Config) when TestCase =:= reload_acl;
                                         TestCase =:= register_mod;
                                         TestCase =:= unregister_mod;
                                         TestCase =:= check_acl ->
    ?AC:stop();

end_per_testcase(_TestCase, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% emqttd_access_control
%%--------------------------------------------------------------------

reload_acl(_) ->
    [ok] = ?AC:reload_acl().

register_mod(_) ->
    ok = ?AC:register_mod(acl, emqttd_acl_test_mod, []),
    {error, already_existed} = ?AC:register_mod(acl, emqttd_acl_test_mod, []),
    [{emqttd_acl_test_mod, _, 0},
     {emqttd_acl_internal, _, 0}] = ?AC:lookup_mods(acl),
    ok = ?AC:register_mod(auth, emqttd_auth_anonymous_test_mod,[]),
    ok = ?AC:register_mod(auth, emqttd_auth_dashboard, [], 99),
    [{emqttd_auth_dashboard, _, 99},
     {emqttd_auth_anonymous_test_mod, _, 0},
     {emqttd_auth_anonymous, _, 0}] = ?AC:lookup_mods(auth).

unregister_mod(_) ->
    ok = ?AC:register_mod(acl, emqttd_acl_test_mod, []),
    [{emqttd_acl_test_mod, _, 0},
     {emqttd_acl_internal, _, 0}] = ?AC:lookup_mods(acl),
    ok = ?AC:unregister_mod(acl, emqttd_acl_test_mod),
    timer:sleep(5),
    [{emqttd_acl_internal, _, 0}] = ?AC:lookup_mods(acl),
    ok = ?AC:register_mod(auth, emqttd_auth_anonymous_test_mod,[]),
    [{emqttd_auth_anonymous_test_mod, _, 0},
     {emqttd_auth_anonymous, _, 0}] = ?AC:lookup_mods(auth),

    ok = ?AC:unregister_mod(auth, emqttd_auth_anonymous_test_mod),
    timer:sleep(5),
    [{emqttd_auth_anonymous, _, 0}] = ?AC:lookup_mods(auth).

check_acl(_) ->
    User1 = #mqtt_client{client_id = <<"client1">>, username = <<"testuser">>},
    User2 = #mqtt_client{client_id = <<"client2">>, username = <<"xyz">>},
    allow = ?AC:check_acl(User1, subscribe, <<"users/testuser/1">>),
    allow = ?AC:check_acl(User1, subscribe, <<"clients/client1">>),
    deny  = ?AC:check_acl(User1, subscribe, <<"clients/client1/x/y">>),
    allow = ?AC:check_acl(User1, publish, <<"users/testuser/1">>),
    allow = ?AC:check_acl(User1, subscribe, <<"a/b/c">>),
    deny  = ?AC:check_acl(User2, subscribe, <<"a/b/c">>).

%%--------------------------------------------------------------------
%% emqttd_access_rule
%%--------------------------------------------------------------------

compile_rule(_) ->

    {allow, {'and', [{ipaddr, {{127,0,0,1}, {127,0,0,1}, 32}},
                     {user, <<"user">>}]}, subscribe, [ [<<"$SYS">>, '#'], ['#'] ]} =
        compile({allow, {'and', [{ipaddr, "127.0.0.1"}, {user, <<"user">>}]}, subscribe, ["$SYS/#", "#"]}),
    {allow, {'or', [{ipaddr, {{127,0,0,1}, {127,0,0,1}, 32}},
                    {user, <<"user">>}]}, subscribe, [ [<<"$SYS">>, '#'], ['#'] ]} =
        compile({allow, {'or', [{ipaddr, "127.0.0.1"}, {user, <<"user">>}]}, subscribe, ["$SYS/#", "#"]}),

    {allow, {ipaddr, {{127,0,0,1}, {127,0,0,1}, 32}}, subscribe, [ [<<"$SYS">>, '#'], ['#'] ]} =
        compile({allow, {ipaddr, "127.0.0.1"}, subscribe, ["$SYS/#", "#"]}),
    {allow, {user, <<"testuser">>}, subscribe, [ [<<"a">>, <<"b">>, <<"c">>], [<<"d">>, <<"e">>, <<"f">>, '#'] ]} =
        compile({allow, {user, "testuser"}, subscribe, ["a/b/c", "d/e/f/#"]}),
    {allow, {user, <<"admin">>}, pubsub, [ [<<"d">>, <<"e">>, <<"f">>, '#'] ]} =
        compile({allow, {user, "admin"}, pubsub, ["d/e/f/#"]}),
    {allow, {client, <<"testClient">>}, publish, [ [<<"testTopics">>, <<"testClient">>] ]} =
        compile({allow, {client, "testClient"}, publish, ["testTopics/testClient"]}),
    {allow, all, pubsub, [{pattern, [<<"clients">>, <<"$c">>]}]} =
        compile({allow, all, pubsub, ["clients/$c"]}),
    {allow, all, subscribe, [{pattern, [<<"users">>, <<"$u">>, '#']}]} =
        compile({allow, all, subscribe, ["users/$u/#"]}),
    {deny, all, subscribe, [ [<<"$SYS">>, '#'], ['#'] ]} =
        compile({deny, all, subscribe, ["$SYS/#", "#"]}),
    {allow, all} = compile({allow, all}),
    {deny, all} = compile({deny, all}).

match_rule(_) ->
    User = #mqtt_client{peername = {{127,0,0,1}, 2948}, client_id = <<"testClient">>, username = <<"TestUser">>},
    User2 = #mqtt_client{peername = {{192,168,0,10}, 3028}, client_id = <<"testClient">>, username = <<"TestUser">>},
    
    {matched, allow} = match(User, <<"Test/Topic">>, {allow, all}),
    {matched, deny} = match(User, <<"Test/Topic">>, {deny, all}),
    {matched, allow} = match(User, <<"Test/Topic">>, compile({allow, {ipaddr, "127.0.0.1"}, subscribe, ["$SYS/#", "#"]})),
    {matched, allow} = match(User2, <<"Test/Topic">>, compile({allow, {ipaddr, "192.168.0.1/24"}, subscribe, ["$SYS/#", "#"]})),
    {matched, allow} = match(User, <<"d/e/f/x">>, compile({allow, {user, "TestUser"}, subscribe, ["a/b/c", "d/e/f/#"]})),
    nomatch = match(User, <<"d/e/f/x">>, compile({allow, {user, "admin"}, pubsub, ["d/e/f/#"]})),
    {matched, allow} = match(User, <<"testTopics/testClient">>, compile({allow, {client, "testClient"}, publish, ["testTopics/testClient"]})),
    {matched, allow} = match(User, <<"clients/testClient">>, compile({allow, all, pubsub, ["clients/$c"]})),
    {matched, allow} = match(#mqtt_client{username = <<"user2">>}, <<"users/user2/abc/def">>,
                             compile({allow, all, subscribe, ["users/$u/#"]})),
    {matched, deny} = match(User, <<"d/e/f">>, compile({deny, all, subscribe, ["$SYS/#", "#"]})),
    Rule = compile({allow, {'and', [{ipaddr, "127.0.0.1"}, {user, <<"WrongUser">>}]}, publish, <<"Topic">>}),
    nomatch = match(User, <<"Topic">>, Rule),
    AndRule = compile({allow, {'and', [{ipaddr, "127.0.0.1"}, {user, <<"TestUser">>}]}, publish, <<"Topic">>}),
    {matched, allow} = match(User, <<"Topic">>, AndRule),
    OrRule = compile({allow, {'or', [{ipaddr, "127.0.0.1"}, {user, <<"WrongUser">>}]}, publish, ["Topic"]}),
    {matched, allow} = match(User, <<"Topic">>, OrRule).

