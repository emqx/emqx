%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_mnesia_migration_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_auth_mnesia/include/emqx_auth_mnesia.hrl").

matrix() ->
    [{ImportAs, Version} || ImportAs <- [clientid, username]
                          , Version <- ["v4.2.10", "v4.1.5"]].

all() ->
    [t_import_4_0, t_import_4_1, t_import_4_2].

groups() ->
    [{username, [], cases()}, {clientid, [], cases()}].

cases() ->
    [t_import].

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_management, emqx_dashboard, emqx_auth_mnesia]),
    ekka_mnesia:start(),
    emqx_mgmt_auth:mnesia(boot),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_modules, emqx_management, emqx_dashboard, emqx_auth_mnesia]),
    ekka_mnesia:ensure_stopped().

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    {atomic,ok} = mnesia:clear_table(emqx_acl),
    {atomic,ok} = mnesia:clear_table(emqx_user),
    ok.
-ifdef(EMQX_ENTERPRISE).
t_import_4_0(Config) ->
    Overrides = emqx_json:encode(#{<<"auth.mnesia.as">> => atom_to_binary(clientid)}),
    ?assertMatch(ok, do_import("e4.0.10.json", Config, Overrides)),
    timer:sleep(100),
    ct:pal("---~p~n", [ets:tab2list(emqx_user)]),
    test_import(username, {<<"emqx_username">>, <<"public">>}),
    test_import(clientid, {<<"emqx_c">>, <<"public">>}),

    Overrides1 = emqx_json:encode(#{<<"auth.mnesia.as">> => atom_to_binary(username)}),
    ?assertMatch(ok, do_import("e4.0.10.json", Config, Overrides1)),
    timer:sleep(100),
    test_import(username, {<<"emqx_c">>, <<"public">>}),
    test_import(username, {<<"emqx_username">>, <<"public">>}).
t_import_4_1(Config) ->
    Overrides = emqx_json:encode(#{<<"auth.mnesia.as">> => atom_to_binary(clientid)}),
    ?assertMatch(ok, do_import("e4.1.1.json", Config, Overrides)),
    timer:sleep(100),
    test_import(clientid, {<<"emqx_c">>, <<"public">>}),
    test_import(clientid, {<<"emqx_c">>, <<"public">>}),

    Overrides1 = emqx_json:encode(#{<<"auth.mnesia.as">> => atom_to_binary(username)}),
    ?assertMatch(ok, do_import("e4.1.1.json", Config, Overrides1)),
    timer:sleep(100),
    test_import(username, {<<"emqx_c">>, <<"public">>}),
    test_import(clientid, {<<"emqx_clientid">>, <<"public">>}).

t_import_4_2(Config) ->
    ?assertMatch(ok, do_import("e4.2.9.json", Config, "{}")),
    timer:sleep(100),
    test_import(username, {<<"emqx_c">>, <<"public">>}),
    test_import(clientid, {<<"emqx_clientid">>, <<"public">>}).

-else.
t_import_4_0(Config) ->
    ?assertMatch(ok, do_import("v4.0.11-no-auth.json", Config)),
    timer:sleep(100),
    ?assertMatch(0, ets:info(emqx_user, size)),

    ?assertMatch({error, unsupported_version, "4.0"}, do_import("v4.0.11.json", Config)),

    ?assertMatch(ok, do_import("v4.0.13.json", Config)),
    timer:sleep(100),
    test_import(clientid, {<<"client_for_test">>, <<"public">>}),
    test_import(username, {<<"user_for_test">>, <<"public">>}).

t_import_4_1(Config) ->
    Overrides = emqx_json:encode(#{<<"auth.mnesia.as">> => atom_to_binary(clientid)}),
    ?assertMatch(ok, do_import("v4.1.5.json", Config, Overrides)),
    timer:sleep(100),
    test_import(clientid, {<<"user_mnesia">>, <<"public">>}),
    test_import(clientid, {<<"client_for_test">>, <<"public">>}),
    test_import(username, {<<"user_for_test">>, <<"public">>}),

    Overrides1 = emqx_json:encode(#{<<"auth.mnesia.as">> => atom_to_binary(username)}),
    ?assertMatch(ok, do_import("v4.1.5.json", Config, Overrides1)),
    timer:sleep(100),
    test_import(username, {<<"user_mnesia">>, <<"public">>}),
    test_import(clientid, {<<"client_for_test">>, <<"public">>}),
    test_import(username, {<<"user_for_test">>, <<"public">>}).

t_import_4_2(Config) ->
    ?assertMatch(ok, do_import("v4.2.10-no-auth.json", Config)),
    timer:sleep(100),
    ?assertMatch(0, ets:info(emqx_user, size)),

    Overrides = emqx_json:encode(#{<<"auth.mnesia.as">> => atom_to_binary(clientid)}),
    ?assertMatch({error, unsupported_version, "4.2"}, do_import("v4.2.10.json", Config, Overrides)),

    Overrides1 = emqx_json:encode(#{<<"auth.mnesia.as">> => atom_to_binary(clientid)}),
    ?assertMatch(ok, do_import("v4.2.11.json", Config, Overrides1)),
    timer:sleep(100),
    test_import(clientid, {<<"user_mnesia">>, <<"public">>}),
    test_import(clientid, {<<"client_for_test">>, <<"public">>}),
    test_import(username, {<<"user_for_test">>, <<"public">>}),

    Overrides2 = emqx_json:encode(#{<<"auth.mnesia.as">> => atom_to_binary(username)}),
    ?assertMatch(ok, do_import("v4.2.11.json", Config, Overrides2)),
    timer:sleep(100),
    test_import(username, {<<"user_mnesia">>, <<"public">>}),
    test_import(clientid, {<<"client_for_test">>, <<"public">>}),
    test_import(username, {<<"user_for_test">>, <<"public">>}),

    ?assertMatch([#emqx_acl{
                     filter = {{Type,<<"emqx_c">>}, <<"Topic/A">>},
                     action = pub,
                     access = allow
                    },
                  #emqx_acl{
                     filter = {{Type,<<"emqx_c">>}, <<"Topic/A">>},
                     action = sub,
                     access = allow
                    }],
                 lists:sort(ets:tab2list(emqx_acl))).
-endif.

do_import(File, Config) ->
    do_import(File, Config, "{}").

do_import(File, Config, Overrides) ->
    mnesia:clear_table(emqx_acl),
    mnesia:clear_table(emqx_user),
    Filename = filename:join(proplists:get_value(data_dir, Config), File),
    emqx_mgmt_data_backup:import(Filename, Overrides).

test_import(username, {Username, Password}) ->
    [#emqx_user{password = _}] = ets:lookup(emqx_user, {username, Username}),
    Req = #{clientid => <<"anyname">>,
            username => Username,
            password => Password},
    ?assertMatch({stop, #{auth_result := success}},
                 emqx_auth_mnesia:check(Req, #{}, #{hash_type => sha256}));
test_import(clientid, {ClientID, Password}) ->
    [#emqx_user{password = _}] = ets:lookup(emqx_user, {clientid, ClientID}),
    Req = #{clientid => ClientID,
            password => Password},
    ?assertMatch({stop, #{auth_result := success}},
                 emqx_auth_mnesia:check(Req, #{}, #{hash_type => sha256})).