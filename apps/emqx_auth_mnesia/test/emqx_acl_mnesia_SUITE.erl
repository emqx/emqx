%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_acl_mnesia_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_auth_mnesia.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_ct_http, [ request_api/3
                      , request_api/4
                      , request_api/5
                      , get_http_data/1
                      , create_default_app/0
                      , delete_default_app/0
                      , default_auth_header/0
                      ]).

-define(HOST, "http://127.0.0.1:8081/").
-define(API_VERSION, "v4").
-define(BASE_PATH, "api").

all() ->
    emqx_ct:all(?MODULE).

groups() ->
    [{async_migration_tests, [sequence], [
        t_old_and_new_acl_migration_by_migrator,
        t_old_and_new_acl_migration_repeated_by_migrator,
        t_migration_concurrency
    ]}].

init_per_suite(Config) ->
    application:load(emqx_plugin_libs),
    emqx_ct_helpers:start_apps( [emqx_modules, emqx_management, emqx_auth_mnesia]
                              , fun set_special_configs/1
                              ),
    supervisor:terminate_child(emqx_auth_mnesia_sup, emqx_acl_mnesia_migrator),
    create_default_app(),
    Config.

end_per_suite(_Config) ->
    delete_default_app(),
    emqx_ct_helpers:stop_apps([emqx_modules, emqx_management, emqx_auth_mnesia]).

init_per_testcase_clean(_, Config) ->
    mnesia:clear_table(?ACL_TABLE),
    mnesia:clear_table(?ACL_TABLE2),
    Config.

init_per_testcase_emqx_hook(t_check_acl_as_clientid, Config) ->
    emqx:hook('client.check_acl', fun emqx_acl_mnesia:check_acl/5, [#{key_as => clientid}]),
    Config;
init_per_testcase_emqx_hook(_, Config) ->
    emqx:hook('client.check_acl', fun emqx_acl_mnesia:check_acl/5, [#{key_as => username}]),
    Config.

init_per_testcase_migration(t_management_before_migration, Config) ->
    Config;
init_per_testcase_migration(_, Config) ->
    emqx_acl_mnesia_migrator:migrate_records(),
    Config.

init_per_testcase_other(t_last_will_testament_message_check_acl, Config) ->
    OriginalACLNoMatch = application:get_env(emqx, acl_nomatch),
    application:set_env(emqx, acl_nomatch, deny),
    emqx_mod_acl_internal:unload([]),
    %% deny all for this client
    ClientID = <<"lwt_client">>,
    ok = emqx_acl_mnesia_db:add_acl({clientid, ClientID}, <<"#">>, pubsub, deny),
    [ {original_acl_nomatch, OriginalACLNoMatch}
    , {clientid, ClientID}
    | Config];
init_per_testcase_other(_TestCase, Config) ->
    Config.

init_per_testcase(Case, Config) ->
    PerTestInitializers = [
        fun init_per_testcase_clean/2,
        fun init_per_testcase_migration/2,
        fun init_per_testcase_emqx_hook/2,
        fun init_per_testcase_other/2
    ],
    lists:foldl(fun(Init, Conf) -> Init(Case, Conf) end, Config, PerTestInitializers).

end_per_testcase(t_last_will_testament_message_check_acl, Config) ->
    emqx:unhook('client.check_acl', fun emqx_acl_mnesia:check_acl/5),
    case ?config(original_acl_nomatch, Config) of
        {ok, Original} -> application:set_env(emqx, acl_nomatch, Original);
        _ -> ok
    end,
    emqx_mod_acl_internal:load([]),
    ok;
end_per_testcase(_TestCase, Config) ->
    emqx:unhook('client.check_acl', fun emqx_acl_mnesia:check_acl/5),
    Config.

set_special_configs(emqx) ->
    application:set_env(emqx, allow_anonymous, true),
    application:set_env(emqx, enable_acl_cache, false),
    LoadedPluginPath = filename:join(["test", "emqx_SUITE_data", "loaded_plugins"]),
    application:set_env(emqx, plugins_loaded_file,
                        emqx_ct_helpers:deps_path(emqx, LoadedPluginPath));

set_special_configs(_App) ->
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_management_before_migration(_Config) ->
    {atomic, IsStarted} = mnesia:transaction(fun emqx_acl_mnesia_db:is_migration_started/0),
    ?assertNot(IsStarted),
    run_acl_tests().

t_management_after_migration(_Config) ->
    {atomic, IsStarted} = mnesia:transaction(fun emqx_acl_mnesia_db:is_migration_started/0),
    ?assert(IsStarted),
    run_acl_tests().

run_acl_tests() ->
    ?assertEqual("Acl with Mnesia", emqx_acl_mnesia:description()),
    ?assertEqual([], emqx_acl_mnesia_db:all_acls()),

    ok = emqx_acl_mnesia_db:add_acl({clientid, <<"test_clientid">>}, <<"topic/%c">>, sub, allow),
    ok = emqx_acl_mnesia_db:add_acl({clientid, <<"test_clientid">>}, <<"topic/+">>, pub, deny),
    ok = emqx_acl_mnesia_db:add_acl({username, <<"test_username">>}, <<"topic/%u">>, sub, deny),
    ok = emqx_acl_mnesia_db:add_acl({username, <<"test_username">>}, <<"topic/+">>, pub, allow),
    ok = emqx_acl_mnesia_db:add_acl(all, <<"#">>, pubsub, deny),
    %% Sleeps below are needed to hide the race condition between
    %% mnesia and ets dirty select in check_acl, that make this test
    %% flaky
    timer:sleep(100),

    ?assertEqual(2, length(emqx_acl_mnesia_db:lookup_acl({clientid, <<"test_clientid">>}))),
    ?assertEqual(2, length(emqx_acl_mnesia_db:lookup_acl({username, <<"test_username">>}))),
    ?assertEqual(2, length(emqx_acl_mnesia_db:lookup_acl(all))),
    ?assertEqual(6, length(emqx_acl_mnesia_db:all_acls())),

    User1 = #{zone => external, clientid => <<"test_clientid">>},
    User2 = #{zone => external, clientid => <<"no_exist">>, username => <<"test_username">>},
    User3 = #{zone => external, clientid => <<"test_clientid">>, username => <<"test_username">>},
    allow = emqx_access_control:check_acl(User1, subscribe, <<"topic/test_clientid">>),
    deny  = emqx_access_control:check_acl(User1, publish,   <<"topic/A">>),
    deny  = emqx_access_control:check_acl(User2, subscribe, <<"topic/test_username">>),
    allow = emqx_access_control:check_acl(User2, publish,   <<"topic/A">>),
    allow = emqx_access_control:check_acl(User3, subscribe, <<"topic/test_clientid">>),
    deny  = emqx_access_control:check_acl(User3, subscribe, <<"topic/test_username">>),
    deny  = emqx_access_control:check_acl(User3, publish,   <<"topic/A">>),
    deny  = emqx_access_control:check_acl(User3, subscribe, <<"topic/A/B">>),
    deny  = emqx_access_control:check_acl(User3, publish,   <<"topic/A/B">>),

    %% Test merging of pubsub capability:
    ok = emqx_acl_mnesia_db:add_acl({clientid, <<"test_clientid">>}, <<"topic/mix">>, pubsub, deny),
    timer:sleep(100),
    deny  = emqx_access_control:check_acl(User1, subscribe,   <<"topic/mix">>),
    deny  = emqx_access_control:check_acl(User1, publish,     <<"topic/mix">>),
    ok = emqx_acl_mnesia_db:add_acl({clientid, <<"test_clientid">>}, <<"topic/mix">>, pub, allow),
    timer:sleep(100),
    deny  = emqx_access_control:check_acl(User1, subscribe,   <<"topic/mix">>),
    allow = emqx_access_control:check_acl(User1, publish,     <<"topic/mix">>),
    ok = emqx_acl_mnesia_db:add_acl( {clientid, <<"test_clientid">>}
                                   , <<"topic/mix">>, pubsub, allow
                                   ),
    timer:sleep(100),
    allow = emqx_access_control:check_acl(User1, subscribe,   <<"topic/mix">>),
    allow = emqx_access_control:check_acl(User1, publish,     <<"topic/mix">>),
    ok = emqx_acl_mnesia_db:add_acl({clientid, <<"test_clientid">>}, <<"topic/mix">>, sub, deny),
    timer:sleep(100),
    deny  = emqx_access_control:check_acl(User1, subscribe,   <<"topic/mix">>),
    allow = emqx_access_control:check_acl(User1, publish,     <<"topic/mix">>),
    ok = emqx_acl_mnesia_db:add_acl({clientid, <<"test_clientid">>}, <<"topic/mix">>, pub, deny),
    timer:sleep(100),
    deny  = emqx_access_control:check_acl(User1, subscribe,   <<"topic/mix">>),
    deny  = emqx_access_control:check_acl(User1, publish,     <<"topic/mix">>),

    %% Test implicit migration of pubsub to pub and sub:
    ok = emqx_acl_mnesia_db:remove_acl({clientid, <<"test_clientid">>}, <<"topic/mix">>),
    ok = mnesia:dirty_write(#?ACL_TABLE{
                               filter = {{clientid, <<"test_clientid">>}, <<"topic/mix">>},
                               action = pubsub,
                               access = allow,
                               created_at = erlang:system_time(millisecond)
                              }),
    timer:sleep(100),
    allow = emqx_access_control:check_acl(User1, subscribe,   <<"topic/mix">>),
    allow = emqx_access_control:check_acl(User1, publish,     <<"topic/mix">>),
    ok = emqx_acl_mnesia_db:add_acl({clientid, <<"test_clientid">>}, <<"topic/mix">>, pub, deny),
    timer:sleep(100),
    allow = emqx_access_control:check_acl(User1, subscribe,   <<"topic/mix">>),
    deny  = emqx_access_control:check_acl(User1, publish,     <<"topic/mix">>),
    ok = emqx_acl_mnesia_db:add_acl({clientid, <<"test_clientid">>}, <<"topic/mix">>, sub, deny),
    timer:sleep(100),
    deny  = emqx_access_control:check_acl(User1, subscribe,   <<"topic/mix">>),
    deny  = emqx_access_control:check_acl(User1, publish,     <<"topic/mix">>),

    ok = emqx_acl_mnesia_db:remove_acl({clientid, <<"test_clientid">>}, <<"topic/%c">>),
    ok = emqx_acl_mnesia_db:remove_acl({clientid, <<"test_clientid">>}, <<"topic/+">>),
    ok = emqx_acl_mnesia_db:remove_acl({clientid, <<"test_clientid">>}, <<"topic/mix">>),
    ok = emqx_acl_mnesia_db:remove_acl({username, <<"test_username">>}, <<"topic/%u">>),
    ok = emqx_acl_mnesia_db:remove_acl({username, <<"test_username">>}, <<"topic/+">>),
    ok = emqx_acl_mnesia_db:remove_acl(all, <<"#">>),
    timer:sleep(100),

    ?assertEqual([], emqx_acl_mnesia_db:all_acls()).

t_old_and_new_acl_combination(_Config) ->
    create_conflicting_records(),

    ?assertEqual(combined_conflicting_records(), emqx_acl_mnesia_db:all_acls()),
    ?assertEqual(
        lists:usort(combined_conflicting_records()),
        lists:usort(emqx_acl_mnesia_db:all_acls_export())).

t_old_and_new_acl_migration(_Config) ->
    create_conflicting_records(),
    emqx_acl_mnesia_migrator:migrate_records(),

    ?assertEqual(combined_conflicting_records(), emqx_acl_mnesia_db:all_acls()),
    ?assertEqual(
        lists:usort(combined_conflicting_records()),
        lists:usort(emqx_acl_mnesia_db:all_acls_export())),

    % check that old table is not popoulated anymore
    ok = emqx_acl_mnesia_db:add_acl({clientid, <<"test_clientid">>}, <<"topic/%c">>, sub, allow),
    ?assert(emqx_acl_mnesia_migrator:is_old_table_migrated()).


t_migration_concurrency(_Config) ->
    Key = {{clientid,<<"client6">>}, <<"t">>},
    Record = #?ACL_TABLE{filter = Key, action = pubsub, access = deny, created_at = 0},
    {atomic, ok} = mnesia:transaction(fun mnesia:write/1, [Record]),

    LockWaitAndDelete =
        fun() ->
           [_Rec] = mnesia:wread({?ACL_TABLE, Key}),
           {{Pid, Ref}, _} =
               ?wait_async_action(spawn_monitor(fun emqx_acl_mnesia_migrator:migrate_records/0),
                                  #{?snk_kind := emqx_acl_mnesia_migrator_record_selected},
                                  1000),
           mnesia:delete({?ACL_TABLE, Key}),
           {Pid, Ref}
        end,

    ?check_trace(
        begin
            {atomic, {Pid, Ref}} = mnesia:transaction(LockWaitAndDelete),
            receive {'DOWN', Ref, process, Pid, _} -> ok end
        end,
        fun(_, Trace) ->
            ?assertMatch([_], ?of_kind(emqx_acl_mnesia_migrator_record_missed, Trace))
        end),

    ?assert(emqx_acl_mnesia_migrator:is_old_table_migrated()),
    ?assertEqual([], emqx_acl_mnesia_db:all_acls()).


t_old_and_new_acl_migration_by_migrator(_Config) ->
    create_conflicting_records(),

    meck:new(fake_nodes, [non_strict]),
    meck:expect(fake_nodes, all, fun() -> [node(), 'somebadnode@127.0.0.1'] end),

    ?check_trace(
        begin
            % check all nodes every 30 ms
            {ok, _} = emqx_acl_mnesia_migrator:start_link(#{
                name => ct_migrator,
                check_nodes_interval => 30,
                get_nodes => fun fake_nodes:all/0
            }),
            timer:sleep(100)
        end,
        fun(_, Trace) ->
            ?assertEqual([], ?of_kind(emqx_acl_mnesia_migrator_start_migration, Trace))
        end),

    ?check_trace(
        begin
            meck:expect(fake_nodes, all, fun() -> [node()] end),
            timer:sleep(100)
        end,
        fun(_, Trace) ->
            ?assertMatch([_], ?of_kind(emqx_acl_mnesia_migrator_finish, Trace))
        end),

    meck:unload(fake_nodes),

    ?assertEqual(combined_conflicting_records(), emqx_acl_mnesia_db:all_acls()),
    ?assert(emqx_acl_mnesia_migrator:is_old_table_migrated()).

t_old_and_new_acl_migration_repeated_by_migrator(_Config) ->
    create_conflicting_records(),
    emqx_acl_mnesia_migrator:migrate_records(),

    ?check_trace(
        begin
            {ok, _} = emqx_acl_mnesia_migrator:start_link(ct_migrator),
            timer:sleep(100)
        end,
        fun(_, Trace) ->
            ?assertEqual([], ?of_kind(emqx_acl_mnesia_migrator_start_migration, Trace)),
            ?assertMatch([_], ?of_kind(emqx_acl_mnesia_migrator_finish, Trace))
        end).

t_start_stop_supervised(_Config) ->
    ?assertEqual(undefined, whereis(emqx_acl_mnesia_migrator)),
    ok = emqx_acl_mnesia_migrator:start_supervised(),
    ?assert(is_pid(whereis(emqx_acl_mnesia_migrator))),
    ok = emqx_acl_mnesia_migrator:stop_supervised(),
    ?assertEqual(undefined, whereis(emqx_acl_mnesia_migrator)).

t_acl_cli(_Config) ->
    meck:new(emqx_ctl, [non_strict, passthrough]),
    meck:expect(emqx_ctl, print, fun(Arg) -> emqx_ctl:format(Arg, []) end),
    meck:expect(emqx_ctl, print, fun(Msg, Arg) -> emqx_ctl:format(Msg, Arg) end),
    meck:expect(emqx_ctl, usage, fun(Usages) -> emqx_ctl:format_usage(Usages) end),
    meck:expect(emqx_ctl, usage, fun(Cmd, Descr) -> emqx_ctl:format_usage(Cmd, Descr) end),

    ?assertEqual(0, length(emqx_acl_mnesia_cli:cli(["list"]))),

    emqx_acl_mnesia_cli:cli(["add", "clientid", "test_clientid", "topic/A", "pub", "deny"]),
    emqx_acl_mnesia_cli:cli(["add", "clientid", "test_clientid", "topic/A", "pub", "allow"]),
    R1 = emqx_ctl:format("Acl(clientid = ~p topic = ~p action = ~p access = ~p)~n",
                         [<<"test_clientid">>, <<"topic/A">>, pub, allow]),
    ?assertEqual([R1], emqx_acl_mnesia_cli:cli(["show", "clientid", "test_clientid"])),
    ?assertEqual([R1], emqx_acl_mnesia_cli:cli(["list", "clientid"])),

    emqx_acl_mnesia_cli:cli(["add", "username", "test_username", "topic/B", "sub", "deny"]),
    R2 = emqx_ctl:format("Acl(username = ~p topic = ~p action = ~p access = ~p)~n",
                         [<<"test_username">>, <<"topic/B">>, sub, deny]),
    ?assertEqual([R2], emqx_acl_mnesia_cli:cli(["show", "username", "test_username"])),
    ?assertEqual([R2], emqx_acl_mnesia_cli:cli(["list", "username"])),

    emqx_acl_mnesia_cli:cli(["add", "_all", "#", "pub", "allow"]),
    emqx_acl_mnesia_cli:cli(["add", "_all", "#", "pubsub", "deny"]),
    ?assertMatch(["",
                  "Acl($all topic = <<\"#\">> action = pub access = deny)",
                  "Acl($all topic = <<\"#\">> action = sub access = deny)"],
                 lists:sort(string:split(emqx_acl_mnesia_cli:cli(["list", "_all"]), "\n", all))
                ),
    ?assertEqual(4, length(emqx_acl_mnesia_cli:cli(["list"]))),

    emqx_acl_mnesia_cli:cli(["del", "clientid", "test_clientid", "topic/A"]),
    emqx_acl_mnesia_cli:cli(["del", "username", "test_username", "topic/B"]),
    emqx_acl_mnesia_cli:cli(["del", "_all", "#"]),
    ?assertEqual(0, length(emqx_acl_mnesia_cli:cli(["list"]))),

    meck:unload(emqx_ctl).

t_rest_api(_Config) ->
    Params1 = [#{<<"clientid">> => <<"test_clientid">>,
                 <<"topic">> => <<"topic/A">>,
                 <<"action">> => <<"pub">>,
                 <<"access">> => <<"allow">>
                },
               #{<<"clientid">> => <<"test_clientid">>,
                 <<"topic">> => <<"topic/B">>,
                 <<"action">> => <<"sub">>,
                 <<"access">> => <<"allow">>
                },
               #{<<"clientid">> => <<"test_clientid">>,
                 <<"topic">> => <<"topic/C">>,
                 <<"action">> => <<"pubsub">>,
                 <<"access">> => <<"deny">>
                },
               #{<<"clientid">> => <<"good_clientid1">>,
                 <<"topic">> => <<"topic/D">>,
                 <<"action">> => <<"pubsub">>,
                 <<"access">> => <<"deny">>
                }],
    {ok, _} = request_http_rest_add([], Params1),

    {ok, Re1} = request_http_rest_list(["clientid", "test_clientid"]),
    ?assertMatch(4, length(get_http_data(Re1))),
    {ok, Re11} = request_http_rest_list(["clientid"], "_like_clientid=good"),
    ?assertMatch(2, length(get_http_data(Re11))),
    {ok, Re12} = request_http_rest_list(["clientid"], "_like_clientid=clientid"),
    ?assertMatch(6, length(get_http_data(Re12))),
    {ok, Re13} = request_http_rest_list(["clientid"], "_like_clientid=clientid&action=pub"),
    ?assertMatch(3, length(get_http_data(Re13))),
    {ok, Re14} = request_http_rest_list(["clientid"], "_like_clientid=clientid&access=deny"),
    ?assertMatch(4, length(get_http_data(Re14))),
    {ok, Re15} = request_http_rest_list(["clientid"], "_like_clientid=clientid&topic=topic/A"),
    ?assertMatch(1, length(get_http_data(Re15))),

    {ok, _} = request_http_rest_delete(["clientid", "test_clientid", "topic", "topic/A"]),
    {ok, _} = request_http_rest_delete(["clientid", "test_clientid", "topic", "topic/B"]),
    {ok, _} = request_http_rest_delete(["clientid", "test_clientid", "topic", "topic/C"]),
    {ok, _} = request_http_rest_delete(["clientid", "good_clientid1", "topic", "topic/D"]),
    {ok, Res1} = request_http_rest_list(["clientid"]),
    ?assertMatch([], get_http_data(Res1)),

    Params2 = [#{<<"username">> => <<"test_username">>,
                 <<"topic">> => <<"topic/A">>,
                 <<"action">> => <<"pub">>,
                 <<"access">> => <<"allow">>
                },
               #{<<"username">> => <<"test_username">>,
                 <<"topic">> => <<"topic/B">>,
                 <<"action">> => <<"sub">>,
                 <<"access">> => <<"allow">>
                },
               #{<<"username">> => <<"test_username">>,
                 <<"topic">> => <<"topic/C">>,
                 <<"action">> => <<"pubsub">>,
                 <<"access">> => <<"deny">>
                },
               #{<<"username">> => <<"good_username">>,
                 <<"topic">> => <<"topic/D">>,
                 <<"action">> => <<"pubsub">>,
                 <<"access">> => <<"deny">>
                }],
    {ok, _} = request_http_rest_add([], Params2),
    {ok, Re2} = request_http_rest_list(["username", "test_username"]),
    ?assertMatch(4, length(get_http_data(Re2))),
    {ok, Re21} = request_http_rest_list(["username"], "_like_username=good"),
    ?assertMatch(2, length(get_http_data(Re21))),
    {ok, Re22} = request_http_rest_list(["username"], "_like_username=username"),
    ?assertMatch(6, length(get_http_data(Re22))),
    {ok, Re23} = request_http_rest_list(["username"], "_like_username=username&action=pub"),
    ?assertMatch(3, length(get_http_data(Re23))),
    {ok, Re24} = request_http_rest_list(["username"], "_like_username=username&access=deny"),
    ?assertMatch(4, length(get_http_data(Re24))),
    {ok, Re25} = request_http_rest_list(["username"], "_like_username=username&topic=topic/A"),
    ?assertMatch(1, length(get_http_data(Re25))),

    {ok, _} = request_http_rest_delete(["username", "test_username", "topic", "topic/A"]),
    {ok, _} = request_http_rest_delete(["username", "test_username", "topic", "topic/B"]),
    {ok, _} = request_http_rest_delete(["username", "test_username", "topic", "topic/C"]),
    {ok, _} = request_http_rest_delete(["username", "good_username", "topic", "topic/D"]),
    {ok, Res2} = request_http_rest_list(["username"]),
    ?assertMatch([], get_http_data(Res2)),

    Params3 = [#{<<"topic">> => <<"topic/A">>,
                 <<"action">> => <<"pub">>,
                 <<"access">> => <<"allow">>
                },
               #{<<"topic">> => <<"topic/B">>,
                 <<"action">> => <<"sub">>,
                 <<"access">> => <<"allow">>
                },
               #{<<"topic">> => <<"topic/C">>,
                 <<"action">> => <<"pubsub">>,
                 <<"access">> => <<"deny">>
                },
               #{<<"topic">> => <<"topic/D">>,
                 <<"action">> => <<"pubsub">>,
                 <<"access">> => <<"deny">>
                }
        ],
    {ok, _} = request_http_rest_add([], Params3),

    {ok, Re3} = request_http_rest_list(["$all"]),
    ?assertMatch(6, length(get_http_data(Re3))),
    {ok, Re31} = request_http_rest_list(["$all"], "topic=topic/A"),
    ?assertMatch(1, length(get_http_data(Re31))),
    {ok, Re32} = request_http_rest_list(["$all"], "action=sub"),
    ?assertMatch(3, length(get_http_data(Re32))),
    {ok, Re33} = request_http_rest_list(["$all"], "access=deny"),
    ?assertMatch(4, length(get_http_data(Re33))),
    {ok, Re34} = request_http_rest_list(["$all"], "action=sub&access=deny"),
    ?assertMatch(2, length(get_http_data(Re34))),

    {ok, _} = request_http_rest_delete(["$all", "topic", "topic/A"]),
    {ok, _} = request_http_rest_delete(["$all", "topic", "topic/B"]),
    {ok, _} = request_http_rest_delete(["$all", "topic", "topic/C"]),
    {ok, _} = request_http_rest_delete(["$all", "topic", "topic/D"]),
    {ok, Res3} = request_http_rest_list(["$all"]),
    ?assertMatch([], get_http_data(Res3)).

%% asserts that we check ACL for the LWT topic before publishing the
%% LWT.
t_last_will_testament_message_check_acl(Config) ->
    ClientID = ?config(clientid, Config),
    {ok, C} = emqtt:start_link([
        {clientid, ClientID},
        {will_topic, <<"$SYS/lwt">>},
        {will_payload, <<"should not be published">>}
    ]),
    {ok, _} = emqtt:connect(C),
    ok = emqx:subscribe(<<"$SYS/lwt">>),
    unlink(C),
    ok = snabbkaffe:start_trace(),
    {true, {ok, _}} =
        ?wait_async_action(
        exit(C, kill),
        #{?snk_kind := last_will_testament_publish_denied},
        1_000
    ),
    ok = snabbkaffe:stop(),

    receive
        {deliver, <<"$SYS/lwt">>, #message{payload = <<"should not be published">>}} ->
            error(lwt_should_not_be_published_to_forbidden_topic)
    after 1_000 ->
        ok
    end,

    ok.

create_conflicting_records() ->
    Records = [
        #?ACL_TABLE{ filter = {{clientid,<<"client6">>}, <<"t">>}
                   , action = pubsub, access = deny, created_at = 0
                   },
        #?ACL_TABLE{ filter = {{clientid,<<"client5">>}, <<"t">>}
                   , action = pubsub, access = deny, created_at = 1
                   },
        #?ACL_TABLE2{who = {clientid,<<"client5">>}, rules = [{allow, sub, <<"t">>, 2}]}
    ],
    mnesia:transaction(fun() -> lists:foreach(fun mnesia:write/1, Records) end).


combined_conflicting_records() ->
    % pubsub's are split, ACL_TABLE2 rules shadow ACL_TABLE rules
    [
        {{clientid,<<"client5">>},<<"t">>,sub,allow,2},
        {{clientid,<<"client5">>},<<"t">>,pub,deny,1},
        {{clientid,<<"client6">>},<<"t">>,sub,deny,0},
        {{clientid,<<"client6">>},<<"t">>,pub,deny,0}
    ].

%%--------------------------------------------------------------------
%% HTTP Request
%%--------------------------------------------------------------------

request_http_rest_list(Path) ->
    request_api(get, uri(Path), default_auth_header()).

request_http_rest_list(Path, Qs) ->
    request_api(get, uri(Path), Qs, default_auth_header()).

request_http_rest_lookup(Path) ->
    request_api(get, uri(Path), default_auth_header()).

request_http_rest_add(Path, Params) ->
    request_api(post, uri(Path), [], default_auth_header(), Params).

request_http_rest_delete(Path) ->
    request_api(delete, uri(Path), default_auth_header()).

uri() -> uri([]).
uri(Parts) when is_list(Parts) ->
    NParts = [b2l(E) || E <- Parts],
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION, "acl"| NParts]).

b2l(B) -> binary_to_list(emqx_http_lib:uri_encode(iolist_to_binary(B))).
