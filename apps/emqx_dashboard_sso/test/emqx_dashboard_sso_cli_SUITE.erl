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

all() -> [t_add, t_passwd, t_del].

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

t_add(_) ->
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

t_passwd(_) ->
    [#?ADMIN{pwdhash = Old}] = emqx_dashboard_admin:lookup_user(<<"user1">>),
    admins(["passwd", "user1", "newpassword1"]),
    [#?ADMIN{pwdhash = New}] = emqx_dashboard_admin:lookup_user(<<"user1">>),
    ?assertNotEqual(Old, New),
    ok.

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
