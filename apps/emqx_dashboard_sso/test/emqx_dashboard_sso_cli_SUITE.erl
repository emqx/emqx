%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_cli_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("../../emqx_dashboard/include/emqx_dashboard.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_dashboard_sso_cli, [admins/1]).

-define(RETRY(Action),
    ?retry(
        _Interval = 200,
        _NAttempts = 20,
        Action
    )
).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_dashboard,
            emqx_dashboard_sso
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(Case, Config) ->
    ?MODULE:Case({init, Config}).

end_per_testcase(Case, Config) ->
    ?MODULE:Case({'end', Config}).

t_add({init, Config}) ->
    Config;
t_add({'end', _Config}) ->
    ok;
t_add(_Config) ->
    admins(["add", "user1", "password1"]),
    admins(["add", "user2", "password2", "user2"]),
    admins(["add", "user3", "password3", "user3", ?ROLE_VIEWER]),
    admins(["add", "user1", "password3", "user3"]),

    ?RETRY(
        ?assertMatch(
            [
                #?ADMIN{
                    username = <<"user1">>,
                    role = ?ROLE_SUPERUSER,
                    description = <<>>
                }
            ],
            emqx_dashboard_admin:lookup_user(<<"user1">>)
        )
    ),

    ?assertMatch(
        [
            #?ADMIN{
                username = <<"user2">>,
                role = ?ROLE_SUPERUSER,
                description = <<"user2">>
            }
        ],
        emqx_dashboard_admin:lookup_user(<<"user2">>)
    ),

    ?assertMatch(
        [
            #?ADMIN{
                username = <<"user3">>,
                role = ?ROLE_VIEWER,
                description = <<"user3">>
            }
        ],
        emqx_dashboard_admin:lookup_user(<<"user3">>)
    ),
    ok.

t_passwd({init, Config}) ->
    admins(["add", "user1", "password1"]),
    Config;
t_passwd({'end', _Config}) ->
    ok;
t_passwd(_) ->
    [#?ADMIN{pwdhash = Old}] = emqx_dashboard_admin:lookup_user(<<"user1">>),
    admins(["passwd", "user1", "newpassword1"]),
    [#?ADMIN{pwdhash = New}] = emqx_dashboard_admin:lookup_user(<<"user1">>),
    ?assertNotEqual(Old, New),
    ok.

t_del({init, Config}) ->
    Config;
t_del({'end', _Config}) ->
    ok;
t_del(_) ->
    admins(["del", "user1"]),
    ?assertEqual([], emqx_dashboard_admin:lookup_user(<<"user1">>)),

    admins(["del", "user2", ?BACKEND_LOCAL]),
    ?assertEqual([], emqx_dashboard_admin:lookup_user(<<"user2">>)),

    admins(["del", "user3", ldap]),
    ?assertNotEqual([], emqx_dashboard_admin:lookup_user(<<"user3">>)),

    emqx_dashboard_admin:add_sso_user(ldap, <<"user4">>, ?ROLE_VIEWER, ""),

    admins(["del", "user4"]),
    ?RETRY(?assertNotEqual([], emqx_dashboard_admin:lookup_user(?SSO_USERNAME(ldap, <<"user4">>)))),

    admins(["del", "user4", ldap]),
    ?assertEqual([], emqx_dashboard_admin:lookup_user(?SSO_USERNAME(ldap, <<"user4">>))),
    ok.

t_mfa({init, Config}) ->
    meck:new(emqx_dashboard_cli, [passthrough, no_history, no_link]),
    meck:expect(emqx_dashboard_cli, print_error, fun(X) -> {print_error, X} end),
    meck:expect(emqx_ctl, usage, fun(_X) -> print_usage end),
    Config;
t_mfa({'end', _Config}) ->
    meck:unload(emqx_dashboard_cli);
t_mfa(_) ->
    Username = "usermfa1",
    ?assertEqual(print_usage, admins(["mfa"])),
    ?assertEqual(print_usage, admins(["mfa", Username])),
    ?assertMatch(
        {print_error, <<"username_not_found">>}, admins(["mfa", "unknownuser1", "enable"])
    ),
    ?assertMatch(
        {print_error, <<"username_not_found">>}, admins(["mfa", "unknownuser1", "disable"])
    ),
    ok = admins(["add", Username, "password1"]),
    [#?ADMIN{}] = emqx_dashboard_admin:lookup_user(bin(Username)),
    ?assertEqual({error, no_mfa_state}, emqx_dashboard_admin:get_mfa_state(bin(Username))),
    ok = admins(["mfa", Username, "disable"]),
    ?assertEqual({ok, disabled}, emqx_dashboard_admin:get_mfa_state(bin(Username))),
    ok = admins(["mfa", Username, "enable"]),
    ?assertMatch(
        {print_error, <<"MFA is already enabled using 'totp'">>},
        admins(["mfa", Username, "enable", "totp"])
    ),
    ok = admins(["mfa", Username, "disable"]),
    ?assertEqual({ok, disabled}, emqx_dashboard_admin:get_mfa_state(bin(Username))),
    %% disable again should be ok
    ?assertEqual({ok, disabled}, emqx_dashboard_admin:get_mfa_state(bin(Username))),
    ?assertMatch(
        {print_error, <<"Unsupported MFA mechanism">>},
        admins(["mfa", Username, "enable", "no-such-mechanism"])
    ),
    ok.

bin(X) -> iolist_to_binary(X).
