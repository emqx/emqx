%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_rbac_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("../../emqx_dashboard/include/emqx_dashboard.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_dashboard_api_test_helpers, [request/4, uri/1]).

-define(DEFAULT_SUPERUSER, <<"admin_user">>).
-define(DEFAULT_SUPERUSER_PASS, <<"admin_password">>).
-define(ADD_DESCRIPTION, <<>>).

-define(global_superuser, global_superuser).
-define(global_viewer, global_viewer).
-define(namespaced_superuser, namespaced_superuser).
-define(namespaced_viewer, namespaced_viewer).

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard(),
            emqx_dashboard_rbac
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

end_per_testcase(_, _Config) ->
    All = emqx_dashboard_admin:all_users(),
    [emqx_dashboard_admin:remove_user(Name) || #{username := Name} <- All].

role_of(TCConfig) ->
    Alternatives = [
        ?global_superuser,
        ?global_viewer,
        ?namespaced_superuser,
        ?namespaced_viewer
    ],
    emqx_common_test_helpers:get_matrix_prop(TCConfig, Alternatives, ?global_superuser).

t_create_bad_role(_) ->
    ?assertEqual(
        {error, <<"Role does not exist">>},
        emqx_dashboard_admin:add_user(
            ?DEFAULT_SUPERUSER,
            ?DEFAULT_SUPERUSER_PASS,
            <<"bad_role">>,
            ?ADD_DESCRIPTION
        )
    ).

t_permission(_) ->
    add_default_superuser(),

    ViewerUser = <<"viewer_user">>,
    ViewerPassword = <<"add_password">>,

    %% add by superuser
    {ok, 200, Payload} = emqx_dashboard_api_test_helpers:request(
        ?DEFAULT_SUPERUSER,
        ?DEFAULT_SUPERUSER_PASS,
        post,
        uri([users]),
        #{
            username => ViewerUser,
            password => ViewerPassword,
            role => ?ROLE_VIEWER,
            description => ?ADD_DESCRIPTION
        }
    ),

    ?assertMatch(
        #{
            <<"username">> := ViewerUser,
            <<"role">> := ?ROLE_VIEWER,
            <<"description">> := ?ADD_DESCRIPTION
        },
        emqx_utils_json:decode(Payload)
    ),

    %% add by viewer
    ?assertMatch(
        {ok, 403, _},
        emqx_dashboard_api_test_helpers:request(
            ViewerUser,
            ViewerPassword,
            post,
            uri([users]),
            #{
                username => ViewerUser,
                password => ViewerPassword,
                role => ?ROLE_VIEWER,
                description => ?ADD_DESCRIPTION
            }
        )
    ),

    ok.

t_update_role(_) ->
    add_default_superuser(),

    %% update role by superuser
    {ok, 200, Payload} = emqx_dashboard_api_test_helpers:request(
        ?DEFAULT_SUPERUSER,
        ?DEFAULT_SUPERUSER_PASS,
        put,
        uri([users, ?DEFAULT_SUPERUSER]),
        #{
            role => ?ROLE_VIEWER,
            description => ?ADD_DESCRIPTION
        }
    ),

    ?assertMatch(
        #{
            <<"username">> := ?DEFAULT_SUPERUSER,
            <<"role">> := ?ROLE_VIEWER,
            <<"description">> := ?ADD_DESCRIPTION
        },
        emqx_utils_json:decode(Payload)
    ),

    %% update role by viewer
    ?assertMatch(
        {ok, 403, _},
        emqx_dashboard_api_test_helpers:request(
            ?DEFAULT_SUPERUSER,
            ?DEFAULT_SUPERUSER_PASS,
            put,
            uri([users, ?DEFAULT_SUPERUSER]),
            #{
                role => ?ROLE_SUPERUSER,
                description => ?ADD_DESCRIPTION
            }
        )
    ),
    ok.

t_clean_token(_) ->
    Username = <<"admin_token">>,
    Password = <<"public_www1">>,
    Desc = <<"desc">>,
    NewDesc = <<"new desc">>,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, ?ROLE_SUPERUSER, Desc),
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(Username, Password),
    FakeReq = #{},
    FakeHandlerInfo = #{method => get, module => any, function => any},
    {ok, #{actor := Username}} = emqx_dashboard_admin:verify_token(FakeReq, FakeHandlerInfo, Token),
    %% change description
    {ok, _} = emqx_dashboard_admin:update_user(Username, ?ROLE_SUPERUSER, NewDesc),
    timer:sleep(5),
    {ok, #{actor := Username}} = emqx_dashboard_admin:verify_token(FakeReq, FakeHandlerInfo, Token),
    %% change role
    {ok, _} = emqx_dashboard_admin:update_user(Username, ?ROLE_VIEWER, NewDesc),
    timer:sleep(5),
    {error, not_found} = emqx_dashboard_admin:verify_token(FakeReq, FakeHandlerInfo, Token),
    ok.

t_login_out() ->
    [{matrix, true}].
t_login_out(matrix) ->
    [
        [?global_superuser],
        [?global_viewer],
        [?namespaced_superuser],
        [?namespaced_viewer]
    ];
t_login_out(TCConfig) when is_list(TCConfig) ->
    Username = <<"admin_token">>,
    Password = <<"public_www1">>,
    Desc = <<"desc">>,
    Role =
        case role_of(TCConfig) of
            ?global_superuser -> ?ROLE_SUPERUSER;
            ?global_viewer -> ?ROLE_VIEWER;
            ?namespaced_superuser -> <<"ns:ns1::", ?ROLE_SUPERUSER/binary>>;
            ?namespaced_viewer -> <<"ns:ns1::", ?ROLE_VIEWER/binary>>
        end,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, Role, Desc),
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(Username, Password),
    FakeReq = #{},
    FakeHandlerInfo = #{method => post, function => logout, module => emqx_dashboard_api},
    {ok, #{actor := Username}} = emqx_dashboard_admin:verify_token(FakeReq, FakeHandlerInfo, Token),
    ok.

t_change_pwd(_) ->
    Viewer1 = <<"viewer1">>,
    Viewer2 = <<"viewer2">>,
    SuperUser = <<"super_user">>,
    Password = <<"public_www1">>,
    Desc = <<"desc">>,
    {ok, _} = emqx_dashboard_admin:add_user(Viewer1, Password, ?ROLE_VIEWER, Desc),
    {ok, _} = emqx_dashboard_admin:add_user(Viewer2, Password, ?ROLE_VIEWER, Desc),
    {ok, _} = emqx_dashboard_admin:add_user(SuperUser, Password, ?ROLE_SUPERUSER, Desc),
    {ok, #{role := ?ROLE_VIEWER, token := Viewer1Token}} = emqx_dashboard_admin:sign_token(
        Viewer1, Password
    ),
    {ok, #{role := ?ROLE_SUPERUSER, token := SuperToken}} = emqx_dashboard_admin:sign_token(
        SuperUser, Password
    ),
    %% viewer can change own password
    ?assertMatch({ok, #{actor := Viewer1}}, change_pwd(Viewer1Token, Viewer1)),
    %% viewer can't change other's password
    ?assertEqual({error, unauthorized_role}, change_pwd(Viewer1Token, Viewer2)),
    ?assertEqual({error, unauthorized_role}, change_pwd(Viewer1Token, SuperUser)),
    %% superuser can change other's password
    ?assertMatch({ok, #{actor := SuperUser}}, change_pwd(SuperToken, Viewer1)),
    ?assertMatch({ok, #{actor := SuperUser}}, change_pwd(SuperToken, Viewer2)),
    ?assertMatch({ok, #{actor := SuperUser}}, change_pwd(SuperToken, SuperUser)),
    ok.

change_pwd(Token, Username) ->
    Req = #{bindings => #{username => Username}},
    HandlerInfo = #{method => post, function => change_pwd, module => emqx_dashboard_api},
    emqx_dashboard_admin:verify_token(Req, HandlerInfo, Token).

t_setup_mfa(_) ->
    test_mfa(fun setup_mfa/2).

t_delete_mfa(_) ->
    test_mfa(fun delete_mfa/2).

test_mfa(VerifyFn) ->
    Viewer1 = <<"viewermfa1">>,
    Viewer2 = <<"viewermfa2">>,
    SuperUser = <<"adminmfa">>,
    Password = <<"xyz124abc">>,
    Desc = <<"desc">>,
    {ok, _} = emqx_dashboard_admin:add_user(Viewer1, Password, ?ROLE_VIEWER, Desc),
    {ok, _} = emqx_dashboard_admin:add_user(Viewer2, Password, ?ROLE_VIEWER, Desc),
    {ok, _} = emqx_dashboard_admin:add_user(SuperUser, Password, ?ROLE_SUPERUSER, Desc),
    {ok, #{role := ?ROLE_VIEWER, token := Viewer1Token}} = emqx_dashboard_admin:sign_token(
        Viewer1, Password
    ),
    {ok, #{role := ?ROLE_SUPERUSER, token := SuperToken}} = emqx_dashboard_admin:sign_token(
        SuperUser, Password
    ),
    %% viewer can change own password
    ?assertMatch({ok, #{actor := Viewer1}}, VerifyFn(Viewer1Token, Viewer1)),
    %% viewer can't change other's password
    ?assertEqual({error, unauthorized_role}, VerifyFn(Viewer1Token, Viewer2)),
    ?assertEqual({error, unauthorized_role}, VerifyFn(Viewer1Token, SuperUser)),
    %% superuser can change other's password
    ?assertMatch({ok, #{actor := SuperUser}}, VerifyFn(SuperToken, Viewer1)),
    ?assertMatch({ok, #{actor := SuperUser}}, VerifyFn(SuperToken, Viewer2)),
    ?assertMatch({ok, #{actor := SuperUser}}, VerifyFn(SuperToken, SuperUser)),
    ok.

delete_mfa(Token, Username) ->
    Req = #{bindings => #{username => Username}},
    HandlerInfo = #{method => delete, module => emqx_dashboard_api, function => change_mfa},
    emqx_dashboard_admin:verify_token(Req, HandlerInfo, Token).

setup_mfa(Token, Username) ->
    Req = #{bindings => #{username => Username}},
    HandlerInfo = #{method => post, module => emqx_dashboard_api, function => change_mfa},
    emqx_dashboard_admin:verify_token(Req, HandlerInfo, Token).

add_default_superuser() ->
    {ok, _NewUser} = emqx_dashboard_admin:add_user(
        ?DEFAULT_SUPERUSER,
        ?DEFAULT_SUPERUSER_PASS,
        ?ROLE_SUPERUSER,
        ?ADD_DESCRIPTION
    ).
