%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_access_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(AC, emqx_access_control).
-define(CACHE, emqx_acl_cache).

-import(emqx_access_rule, [compile/1, match/3]).

all() ->
    [{group, access_control},
     {group, acl_cache},
     {group, access_control_cache_mode},
     {group, access_rule}].

groups() ->
    [{access_control, [sequence],
      [reload_acl,
       register_mod,
       unregister_mod,
       check_acl_1,
       check_acl_2]},
     {access_control_cache_mode, [],
      [acl_cache_basic,
       acl_cache_expiry,
       acl_cache_cleanup,
       acl_cache_full]},
     {acl_cache, [],
      [put_get_del_cache,
       cache_update,
       cache_expiry,
       cache_replacement,
       cache_cleanup,
       cache_auto_emtpy,
       cache_auto_cleanup]},
     {access_rule, [],
      [compile_rule,
       match_rule]}].

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

init_per_group(Group, Config) when Group =:= access_control;
                                   Group =:= access_control_cache_mode ->
    prepare_config(Group),
    application:load(emqx),
    Config;
init_per_group(_Group, Config) ->
    Config.

prepare_config(Group = access_control) ->
    set_acl_config_file(Group),
    application:set_env(emqx, enable_acl_cache, false);
prepare_config(Group = access_control_cache_mode) ->
    set_acl_config_file(Group),
    application:set_env(emqx, enable_acl_cache, true),
    application:set_env(emqx, acl_cache_max_size, 100).

set_acl_config_file(_Group) ->
    Rules = [{allow, {ipaddr, "127.0.0.1"}, subscribe, ["$SYS/#", "#"]},
             {allow, {user, "testuser"}, subscribe, ["a/b/c", "d/e/f/#"]},
             {allow, {user, "admin"}, pubsub, ["a/b/c", "d/e/f/#"]},
             {allow, {client, "testClient"}, subscribe, ["testTopics/testClient"]},
             {allow, all, subscribe, ["clients/%c"]},
             {allow, all, pubsub, ["users/%u/#"]},
             {deny, all, subscribe, ["$SYS/#", "#"]},
             {deny, all}],
    write_config("access_SUITE_acl.conf", Rules),
    application:set_env(emqx, acl_file, "access_SUITE_acl.conf").

write_config(Filename, Terms) ->
    file:write_file(Filename, [io_lib:format("~tp.~n", [Term]) || Term <- Terms]).

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(_TestCase, Config) ->
    ?AC:start_link(),
    Config.
end_per_testcase(_TestCase, _Config) ->
    ok.

per_testcase_config(acl_cache_full, Config) ->
    Config;
per_testcase_config(_TestCase, Config) ->
    Config.

%%--------------------------------------------------------------------
%% emqx_access_control
%%--------------------------------------------------------------------

reload_acl(_) ->
    [ok] = ?AC:reload_acl().

register_mod(_) ->
    ok = ?AC:register_mod(acl, emqx_acl_test_mod, []),
    {emqx_acl_test_mod, _, 0} = hd(?AC:lookup_mods(acl)),
    ok = ?AC:register_mod(auth, emqx_auth_anonymous_test_mod,[]),
    ok = ?AC:register_mod(auth, emqx_auth_dashboard, [], 99),
    [{emqx_auth_dashboard, _, 99},
     {emqx_auth_anonymous_test_mod, _, 0}] = ?AC:lookup_mods(auth).

unregister_mod(_) ->
    ok = ?AC:register_mod(acl, emqx_acl_test_mod, []),
    {emqx_acl_test_mod, _, 0} = hd(?AC:lookup_mods(acl)),
    ok = ?AC:unregister_mod(acl, emqx_acl_test_mod),
    timer:sleep(5),
    {emqx_acl_internal, _, 0}= hd(?AC:lookup_mods(acl)),
    ok = ?AC:register_mod(auth, emqx_auth_anonymous_test_mod,[]),
    [{emqx_auth_anonymous_test_mod, _, 0}] = ?AC:lookup_mods(auth),

    ok = ?AC:unregister_mod(auth, emqx_auth_anonymous_test_mod),
    timer:sleep(5),
    [] = ?AC:lookup_mods(auth).

check_acl_1(_) ->
    SelfUser = #{client_id => <<"client1">>, username => <<"testuser">>},
    allow = ?AC:check_acl(SelfUser, subscribe, <<"users/testuser/1">>),
    allow = ?AC:check_acl(SelfUser, subscribe, <<"clients/client1">>),
    deny = ?AC:check_acl(SelfUser, subscribe, <<"clients/client1/x/y">>),
    allow = ?AC:check_acl(SelfUser, publish, <<"users/testuser/1">>),
    allow = ?AC:check_acl(SelfUser, subscribe, <<"a/b/c">>).
check_acl_2(_) ->
    SelfUser = #{client_id => <<"client2">>, username => <<"xyz">>},
    deny = ?AC:check_acl(SelfUser, subscribe, <<"a/b/c">>).

acl_cache_basic(_) ->
    SelfUser = #{client_id => <<"client1">>, username => <<"testuser">>},
    not_found = ?CACHE:get_acl_cache(subscribe, <<"users/testuser/1">>),
    not_found = ?CACHE:get_acl_cache(subscribe, <<"clients/client1">>),

    allow = ?AC:check_acl(SelfUser, subscribe, <<"users/testuser/1">>),
    allow = ?AC:check_acl(SelfUser, subscribe, <<"clients/client1">>),

    allow = ?CACHE:get_acl_cache(subscribe, <<"users/testuser/1">>),
    allow = ?CACHE:get_acl_cache(subscribe, <<"clients/client1">>),
    ok.

acl_cache_expiry(_) ->
    application:set_env(emqx, acl_cache_ttl, 100),
    SelfUser = #{client_id => <<"client1">>, username => <<"testuser">>},
    allow = ?AC:check_acl(SelfUser, subscribe, <<"clients/client1">>),
    allow = ?CACHE:get_acl_cache(subscribe, <<"clients/client1">>),
    ct:sleep(150),
    not_found = ?CACHE:get_acl_cache(subscribe, <<"clients/client1">>),
    ok.

acl_cache_full(_) ->
    application:set_env(emqx, acl_cache_max_size, 1),

    SelfUser = #{client_id => <<"client1">>, username => <<"testuser">>},
    allow = ?AC:check_acl(SelfUser, subscribe, <<"users/testuser/1">>),
    allow = ?AC:check_acl(SelfUser, subscribe, <<"clients/client1">>),

    %% the older ones (the <<"users/testuser/1">>) will be evicted first
    not_found = ?CACHE:get_acl_cache(subscribe, <<"users/testuser/1">>),
    allow = ?CACHE:get_acl_cache(subscribe, <<"clients/client1">>),
    ok.

acl_cache_cleanup(_) ->
    %% The acl cache will try to evict memory, if the size is full and the newest
    %%   cache entry is expired
    application:set_env(emqx, acl_cache_ttl, 100),
    application:set_env(emqx, acl_cache_max_size, 2),

    SelfUser = #{client_id => <<"client1">>, username => <<"testuser">>},
    allow = ?AC:check_acl(SelfUser, subscribe, <<"users/testuser/1">>),
    allow = ?AC:check_acl(SelfUser, subscribe, <<"clients/client1">>),

    allow = ?CACHE:get_acl_cache(subscribe, <<"users/testuser/1">>),
    allow = ?CACHE:get_acl_cache(subscribe, <<"clients/client1">>),

    ct:sleep(150),
    %% now the cache is full and the newest one - "clients/client1"
    %%  should be expired, so we'll empty the cache before putting
    %%  the next cache entry
    deny = ?AC:check_acl(SelfUser, subscribe, <<"#">>),

    not_found = ?CACHE:get_acl_cache(subscribe, <<"users/testuser/1">>),
    not_found = ?CACHE:get_acl_cache(subscribe, <<"clients/client1">>),
    deny = ?CACHE:get_acl_cache(subscribe, <<"#">>),
    ok.

put_get_del_cache(_) ->
    application:set_env(emqx, acl_cache_ttl, 300000),
    application:set_env(emqx, acl_cache_max_size, 30),

    not_found = ?CACHE:get_acl_cache(publish, <<"a">>),
    ok = ?CACHE:put_acl_cache(publish, <<"a">>, allow),
    allow = ?CACHE:get_acl_cache(publish, <<"a">>),

    not_found = ?CACHE:get_acl_cache(subscribe, <<"b">>),
    ok = ?CACHE:put_acl_cache(subscribe, <<"b">>, deny),
    deny = ?CACHE:get_acl_cache(subscribe, <<"b">>),

    2 = ?CACHE:get_cache_size(),
    ?assertEqual(?CACHE:cache_k(subscribe, <<"b">>), ?CACHE:get_newest_key()).

cache_expiry(_) ->
    application:set_env(emqx, acl_cache_ttl, 100),
    application:set_env(emqx, acl_cache_max_size, 30),
    ok = ?CACHE:put_acl_cache(subscribe, <<"a">>, allow),
    allow = ?CACHE:get_acl_cache(subscribe, <<"a">>),

    ct:sleep(150),
    not_found = ?CACHE:get_acl_cache(subscribe, <<"a">>),

    ok = ?CACHE:put_acl_cache(subscribe, <<"a">>, deny),
    deny = ?CACHE:get_acl_cache(subscribe, <<"a">>),

    ct:sleep(150),
    not_found = ?CACHE:get_acl_cache(subscribe, <<"a">>).

cache_update(_) ->
    application:set_env(emqx, acl_cache_ttl, 300000),
    application:set_env(emqx, acl_cache_max_size, 30),
    [] = ?CACHE:dump_acl_cache(),

    ok = ?CACHE:put_acl_cache(subscribe, <<"a">>, allow),
    ok = ?CACHE:put_acl_cache(publish, <<"b">>, allow),
    ok = ?CACHE:put_acl_cache(publish, <<"c">>, allow),
    3 = ?CACHE:get_cache_size(),
    ?assertEqual(?CACHE:cache_k(publish, <<"c">>), ?CACHE:get_newest_key()),

    %% update the 2nd one
    ok = ?CACHE:put_acl_cache(publish, <<"b">>, allow),
    ct:pal("dump acl cache: ~p~n", [?CACHE:dump_acl_cache()]),

    3 = ?CACHE:get_cache_size(),
    ?assertEqual(?CACHE:cache_k(publish, <<"b">>), ?CACHE:get_newest_key()),
    ?assertEqual(?CACHE:cache_k(subscribe, <<"a">>), ?CACHE:get_oldest_key()).

cache_replacement(_) ->
    application:set_env(emqx, acl_cache_ttl, 300000),
    application:set_env(emqx, acl_cache_max_size, 3),
    ok = ?CACHE:put_acl_cache(subscribe, <<"a">>, allow),
    ok = ?CACHE:put_acl_cache(publish, <<"b">>, allow),
    ok = ?CACHE:put_acl_cache(publish, <<"c">>, allow),
    allow = ?CACHE:get_acl_cache(subscribe, <<"a">>),
    allow = ?CACHE:get_acl_cache(publish, <<"b">>),
    allow = ?CACHE:get_acl_cache(publish, <<"c">>),
    3 = ?CACHE:get_cache_size(),
    ?assertEqual(?CACHE:cache_k(publish, <<"c">>), ?CACHE:get_newest_key()),

    ok = ?CACHE:put_acl_cache(publish, <<"d">>, deny),
    3 = ?CACHE:get_cache_size(),
    ?assertEqual(?CACHE:cache_k(publish, <<"d">>), ?CACHE:get_newest_key()),
    ?assertEqual(?CACHE:cache_k(publish, <<"b">>), ?CACHE:get_oldest_key()),

    ok = ?CACHE:put_acl_cache(publish, <<"e">>, deny),
    3 = ?CACHE:get_cache_size(),
    ?assertEqual(?CACHE:cache_k(publish, <<"e">>), ?CACHE:get_newest_key()),
    ?assertEqual(?CACHE:cache_k(publish, <<"c">>), ?CACHE:get_oldest_key()),

    not_found = ?CACHE:get_acl_cache(subscribe, <<"a">>),
    not_found = ?CACHE:get_acl_cache(publish, <<"b">>),
    allow = ?CACHE:get_acl_cache(publish, <<"c">>).

cache_cleanup(_) ->
    application:set_env(emqx, acl_cache_ttl, 100),
    application:set_env(emqx, acl_cache_max_size, 30),
    ok = ?CACHE:put_acl_cache(subscribe, <<"a">>, allow),
    ok = ?CACHE:put_acl_cache(publish, <<"b">>, allow),
    ct:sleep(150),
    ok = ?CACHE:put_acl_cache(publish, <<"c">>, allow),
    3 = ?CACHE:get_cache_size(),

    ?CACHE:cleanup_acl_cache(),
    ?assertEqual(?CACHE:cache_k(publish, <<"c">>), ?CACHE:get_oldest_key()),
    1 = ?CACHE:get_cache_size().

cache_auto_emtpy(_) ->
    %% verify cache is emptied when cache full and even the newest
    %%   one is expired.
    application:set_env(emqx, acl_cache_ttl, 100),
    application:set_env(emqx, acl_cache_max_size, 3),
    ok = ?CACHE:put_acl_cache(subscribe, <<"a">>, allow),
    ok = ?CACHE:put_acl_cache(publish, <<"b">>, allow),
    ok = ?CACHE:put_acl_cache(publish, <<"c">>, allow),
    3 = ?CACHE:get_cache_size(),

    ct:sleep(150),
    ok = ?CACHE:put_acl_cache(subscribe, <<"d">>, deny),
    1 = ?CACHE:get_cache_size().

cache_auto_cleanup(_) ->
    %% verify we'll cleanup expired entries when we got a exipired acl
    %%   from cache.
    application:set_env(emqx, acl_cache_ttl, 100),
    application:set_env(emqx, acl_cache_max_size, 30),
    ok = ?CACHE:put_acl_cache(subscribe, <<"a">>, allow),
    ok = ?CACHE:put_acl_cache(publish, <<"b">>, allow),
    ct:sleep(150),
    ok = ?CACHE:put_acl_cache(publish, <<"c">>, allow),
    ok = ?CACHE:put_acl_cache(publish, <<"d">>, deny),
    4 = ?CACHE:get_cache_size(),

    %% "a" and "b" expires, while "c" and "d" not
    not_found = ?CACHE:get_acl_cache(publish, <<"b">>),
    2 = ?CACHE:get_cache_size(),

    ct:sleep(150), %% now "c" and "d" expires
    not_found = ?CACHE:get_acl_cache(publish, <<"c">>),
    0 = ?CACHE:get_cache_size().

%%--------------------------------------------------------------------
%% emqx_access_rule
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
    {allow, all, pubsub, [{pattern, [<<"clients">>, <<"%c">>]}]} =
        compile({allow, all, pubsub, ["clients/%c"]}),
    {allow, all, subscribe, [{pattern, [<<"users">>, <<"%u">>, '#']}]} =
        compile({allow, all, subscribe, ["users/%u/#"]}),
    {deny, all, subscribe, [ [<<"$SYS">>, '#'], ['#'] ]} =
        compile({deny, all, subscribe, ["$SYS/#", "#"]}),
    {allow, all} = compile({allow, all}),
    {deny, all} = compile({deny, all}).

match_rule(_) ->
    User = #{client_id => <<"testClient">>, username => <<"TestUser">>, peername => {{127,0,0,1}, 2948}},
    User2 = #{client_id => <<"testClient">>, username => <<"TestUser">>, peername => {{192,168,0,10}, 3028}},

    {matched, allow} = match(User, <<"Test/Topic">>, {allow, all}),
    {matched, deny} = match(User, <<"Test/Topic">>, {deny, all}),
    {matched, allow} = match(User, <<"Test/Topic">>, compile({allow, {ipaddr, "127.0.0.1"}, subscribe, ["$SYS/#", "#"]})),
    {matched, allow} = match(User2, <<"Test/Topic">>, compile({allow, {ipaddr, "192.168.0.1/24"}, subscribe, ["$SYS/#", "#"]})),
    {matched, allow} = match(User, <<"d/e/f/x">>, compile({allow, {user, "TestUser"}, subscribe, ["a/b/c", "d/e/f/#"]})),
    nomatch = match(User, <<"d/e/f/x">>, compile({allow, {user, "admin"}, pubsub, ["d/e/f/#"]})),
    {matched, allow} = match(User, <<"testTopics/testClient">>, compile({allow, {client, "testClient"}, publish, ["testTopics/testClient"]})),
    {matched, allow} = match(User, <<"clients/testClient">>, compile({allow, all, pubsub, ["clients/%c"]})),
    {matched, allow} = match(#{username => <<"user2">>}, <<"users/user2/abc/def">>, compile({allow, all, subscribe, ["users/%u/#"]})),
    {matched, deny} = match(User, <<"d/e/f">>, compile({deny, all, subscribe, ["$SYS/#", "#"]})),
    Rule = compile({allow, {'and', [{ipaddr, "127.0.0.1"}, {user, <<"WrongUser">>}]}, publish, <<"Topic">>}),
    nomatch = match(User, <<"Topic">>, Rule),
    AndRule = compile({allow, {'and', [{ipaddr, "127.0.0.1"}, {user, <<"TestUser">>}]}, publish, <<"Topic">>}),
    {matched, allow} = match(User, <<"Topic">>, AndRule),
    OrRule = compile({allow, {'or', [{ipaddr, "127.0.0.1"}, {user, <<"WrongUser">>}]}, publish, ["Topic"]}),
    {matched, allow} = match(User, <<"Topic">>, OrRule).
