%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-define(REDACTED, <<"******">>).
-define(SENTINEL, <<"sec431-sentinel">>).
-define(VIEWER_USER, <<"viewer_user_for_configs">>).
-define(VIEWER_PASS, <<"viewer_pass_for_configs">>).
%% Raw-socket requests must bypass `httpc' URL normalization (see below).
-define(DASHBOARD_HOST, "127.0.0.1").
-define(DASHBOARD_PORT, 18083).
-define(TIMEOUT, 5000).

all() ->
    emqx_common_test_helpers:all(?MODULE).

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
    FakePath = erlang:list_to_binary(emqx_dashboard_swagger:relative_uri("/fake")),
    FakeReq = #{method => <<"GET">>, path => FakePath},
    {ok, Username} = emqx_dashboard_admin:verify_token(FakeReq, Token),
    %% change description
    {ok, _} = emqx_dashboard_admin:update_user(Username, ?ROLE_SUPERUSER, NewDesc),
    timer:sleep(5),
    {ok, Username} = emqx_dashboard_admin:verify_token(FakeReq, Token),
    %% change role
    {ok, _} = emqx_dashboard_admin:update_user(Username, ?ROLE_VIEWER, NewDesc),
    timer:sleep(5),
    {error, not_found} = emqx_dashboard_admin:verify_token(FakeReq, Token),
    ok.

t_login_out(_) ->
    Username = <<"admin_token">>,
    Password = <<"public_www1">>,
    Desc = <<"desc">>,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, ?ROLE_SUPERUSER, Desc),
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(Username, Password),
    FakePath = erlang:list_to_binary(emqx_dashboard_swagger:relative_uri("/logout")),
    FakeReq = #{method => <<"POST">>, path => FakePath},
    {ok, Username} = emqx_dashboard_admin:verify_token(FakeReq, Token),
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
    ?assertEqual({ok, Viewer1}, change_pwd(Viewer1Token, Viewer1)),
    %% viewer can't change other's password
    ?assertEqual({error, {unauthorized_role, Viewer1}}, change_pwd(Viewer1Token, Viewer2)),
    ?assertEqual({error, {unauthorized_role, Viewer1}}, change_pwd(Viewer1Token, SuperUser)),
    %% superuser can change other's password
    ?assertEqual({ok, SuperUser}, change_pwd(SuperToken, Viewer1)),
    ?assertEqual({ok, SuperUser}, change_pwd(SuperToken, Viewer2)),
    ?assertEqual({ok, SuperUser}, change_pwd(SuperToken, SuperUser)),
    ok.

change_pwd(Token, Username) ->
    Path = "/users/" ++ binary_to_list(Username) ++ "/change_pwd",
    Path1 = erlang:list_to_binary(emqx_dashboard_swagger:relative_uri(Path)),
    Req = #{method => <<"POST">>, path => Path1},
    emqx_dashboard_admin:verify_token(Req, Token).

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
    ?assertEqual({ok, Viewer1}, VerifyFn(Viewer1Token, Viewer1)),
    %% viewer can't change other's password
    ?assertEqual({error, {unauthorized_role, Viewer1}}, VerifyFn(Viewer1Token, Viewer2)),
    ?assertEqual({error, {unauthorized_role, Viewer1}}, VerifyFn(Viewer1Token, SuperUser)),
    %% superuser can change other's password
    ?assertEqual({ok, SuperUser}, VerifyFn(SuperToken, Viewer1)),
    ?assertEqual({ok, SuperUser}, VerifyFn(SuperToken, Viewer2)),
    ?assertEqual({ok, SuperUser}, VerifyFn(SuperToken, SuperUser)),
    ok.

delete_mfa(Token, Username) ->
    Path = "/users/" ++ binary_to_list(Username) ++ "/mfa",
    Path1 = erlang:list_to_binary(emqx_dashboard_swagger:relative_uri(Path)),
    Req = #{method => <<"DELETE">>, path => Path1},
    emqx_dashboard_admin:verify_token(Req, Token).

setup_mfa(Token, Username) ->
    Path = "/users/" ++ binary_to_list(Username) ++ "/mfa",
    Path1 = erlang:list_to_binary(emqx_dashboard_swagger:relative_uri(Path)),
    Req = #{method => <<"POST">>, path => Path1},
    emqx_dashboard_admin:verify_token(Req, Token).

%% `GET /api/v5/configs' with `Accept: text/plain' dumps the whole configuration as
%% cleartext HOCON, so it is restricted to administrators. Viewers keep the redacted
%% JSON variant of the same endpoint. See PLANS.md.
t_configs_plaintext_permission(_) ->
    add_default_superuser(),
    {ok, _} = put_sentinel(),
    Admin = admin_auth_header(),
    Viewer = viewer_auth_header(),
    ViewerKey = create_api_key(<<"sec431-viewer-key">>, ?ROLE_API_VIEWER),
    AdminKey = create_api_key(<<"sec431-admin-key">>, ?ROLE_API_SUPERUSER),
    try
        %% A viewer (dashboard token) must not be able to read the cleartext dump,
        %% in any of the request shapes that negotiate to `text/plain'. Every denial
        %% must use the existing RBAC `UNAUTHORIZED_ROLE' code.
        assert_configs_denied(Viewer, <<"text/plain">>, no_key),
        assert_configs_denied(Viewer, no_accept, no_key),
        assert_configs_denied(Viewer, <<"*/*">>, no_key),
        assert_configs_denied(Viewer, <<"application/json, */*;q=0.8">>, no_key),
        %% `?key=' is served by the same cleartext dump, so it is denied as well.
        assert_configs_denied(Viewer, <<"text/plain">>, {key, <<"sysmon">>}),
        %% The rule is evaluated on the node that receives the request, before any
        %% RPC, so `?node=' is covered by the same path check.
        assert_configs_denied(Viewer, <<"text/plain">>, {node, atom_to_list(node())}),
        %% Path aliases must not bypass the rule: `cowboy_router' routes on the
        %% percent-decoded, dot-segment-collapsed path, so these all reach the
        %% `configs' handler. `httpc' normalizes URLs before sending them
        %% (`uri_string:normalize/2'), which would hide the aliases, so the raw
        %% request bytes are sent instead.
        ?assertEqual(<<"403">>, raw_configs_status(Viewer, "/api/v5/%63onfigs")),
        ?assertEqual(<<"403">>, raw_configs_status(Viewer, "/api/v5/conf%69gs")),
        ?assertEqual(<<"403">>, raw_configs_status(Viewer, "/api/v5/./configs")),
        ?assertEqual(<<"403">>, raw_configs_status(Viewer, "/api/v5/x/../configs")),
        %% `cowboy_router' also drops the trailing empty segment, so these reach
        %% the same handler too.
        ?assertEqual(<<"403">>, raw_configs_status(Viewer, "/api/v5/configs/")),
        ?assertEqual(<<"403">>, raw_configs_status(Viewer, "/api/v5/configs/.")),
        ?assertEqual(<<"403">>, raw_configs_status(Viewer, "/api/v5/configs/%2E")),
        %% Non-vacuity: those aliases really do reach the `configs' handler, so an
        %% administrator gets the cleartext dump through them.
        {<<"200">>, AdminAliasBody} = raw_configs_response(Admin, "/api/v5/%63onfigs"),
        ?assertNotEqual(nomatch, binary:match(AdminAliasBody, ?SENTINEL)),
        {<<"200">>, AdminTrailingBody} = raw_configs_response(Admin, "/api/v5/configs/"),
        ?assertNotEqual(nomatch, binary:match(AdminTrailingBody, ?SENTINEL)),
        %% The redacted JSON path is not affected.
        {{200, JsonBody}, "application/json"} = configs_get(
            Viewer, <<"application/json">>, no_key
        ),
        #{<<"sysmon">> := #{<<"top">> := #{<<"db_password">> := ?REDACTED}}} =
            emqx_utils_json:decode(JsonBody, [return_maps]),
        ?assertEqual(nomatch, binary:match(JsonBody, ?SENTINEL)),

        %% A viewer API key goes through the same RBAC rules.
        assert_configs_denied(ViewerKey, <<"text/plain">>, no_key),
        assert_configs_denied(ViewerKey, no_accept, no_key),
        assert_configs_denied(ViewerKey, <<"*/*">>, no_key),
        ?assertEqual(<<"403">>, raw_configs_status(ViewerKey, "/api/v5/conf%69gs")),
        ?assertEqual(<<"403">>, raw_configs_status(ViewerKey, "/api/v5/configs/")),
        {{200, ViewerKeyJson}, "application/json"} = configs_get(
            ViewerKey, <<"application/json">>, no_key
        ),
        ?assertEqual(nomatch, binary:match(ViewerKeyJson, ?SENTINEL)),

        %% Administrators keep the cleartext dump: it is the export half of the
        %% `GET /configs' -> `PUT /configs' (export/reload) workflow.
        {{200, PlainBody}, "text/plain"} = configs_get(Admin, <<"text/plain">>, no_key),
        ?assertNotEqual(nomatch, binary:match(PlainBody, ?SENTINEL)),
        {{200, NoAcceptBody}, "text/plain"} = configs_get(Admin, no_accept, no_key),
        ?assertNotEqual(nomatch, binary:match(NoAcceptBody, ?SENTINEL)),
        {{200, StarBody}, "text/plain"} = configs_get(Admin, <<"*/*">>, no_key),
        ?assertNotEqual(nomatch, binary:match(StarBody, ?SENTINEL)),
        {{200, MixedBody}, "text/plain"} = configs_get(
            Admin, <<"application/json, */*;q=0.8">>, no_key
        ),
        ?assertNotEqual(nomatch, binary:match(MixedBody, ?SENTINEL)),
        {{200, KeyBody}, "text/plain"} = configs_get(
            Admin, <<"text/plain">>, {key, <<"sysmon">>}
        ),
        ?assertNotEqual(nomatch, binary:match(KeyBody, ?SENTINEL)),
        %% ... and the JSON variant stays redacted for them too.
        {{200, AdminJson}, "application/json"} = configs_get(
            Admin, <<"application/json">>, no_key
        ),
        #{<<"sysmon">> := #{<<"top">> := #{<<"db_password">> := ?REDACTED}}} =
            emqx_utils_json:decode(AdminJson, [return_maps]),
        ?assertEqual(nomatch, binary:match(AdminJson, ?SENTINEL)),

        %% An administrator API key can still export the cleartext HOCON.
        {{200, AdminKeyBody}, "text/plain"} = configs_get(AdminKey, <<"text/plain">>, no_key),
        ?assertNotEqual(nomatch, binary:match(AdminKeyBody, ?SENTINEL)),
        ok
    after
        emqx_mgmt_auth:delete(<<"sec431-viewer-key">>),
        emqx_mgmt_auth:delete(<<"sec431-admin-key">>),
        emqx_conf:remove([sysmon, top, db_password], #{override_to => cluster})
    end.

configs_get(Auth, Accept, Query) ->
    configs_get_uri(
        emqx_mgmt_api_test_util:api_path(["configs" ++ query_string(Query)]), Auth, Accept
    ).

configs_get_uri(URI, Auth, Accept) ->
    Headers = accept_header(Accept) ++ [Auth],
    Opts = #{return_all => true, httpc_req_opts => [{body_format, binary}]},
    case emqx_mgmt_api_test_util:request_api(get, URI, [], Headers, [], Opts) of
        {ok, {{_, Code, _}, RespHeaders, Body}} ->
            {{Code, Body}, proplists:get_value("content-type", RespHeaders)};
        {error, {{_, Code, _}, RespHeaders, Body}} ->
            {{Code, Body}, proplists:get_value("content-type", RespHeaders)}
    end.

query_string(no_key) -> "";
query_string({key, Key}) -> "?key=" ++ binary_to_list(Key);
query_string({node, Node}) -> "?node=" ++ Node.

%% Asserts the denial status *and* the error code promised by the RBAC layer.
assert_configs_denied(Auth, Accept, Query) ->
    {{403, Body}, ContentType} = configs_get(Auth, Accept, Query),
    #{<<"code">> := <<"UNAUTHORIZED_ROLE">>} = emqx_utils_json:decode(Body, [return_maps]),
    {{403, Body}, ContentType}.

%% Sends a raw request line, bypassing the client-side URL normalization that
%% `httpc' would apply, and returns the HTTP status code.
raw_configs_status(Auth, RawPath) ->
    {Code, _Resp} = raw_configs_response(Auth, RawPath),
    Code.

raw_configs_response({_AuthName, AuthValue}, RawPath) ->
    {ok, Socket} = gen_tcp:connect(
        ?DASHBOARD_HOST, ?DASHBOARD_PORT, [binary, {active, false}, {packet, raw}], ?TIMEOUT
    ),
    try
        ok = gen_tcp:send(Socket, [
            "GET ",
            RawPath,
            " HTTP/1.1\r\n",
            "Host: ",
            ?DASHBOARD_HOST,
            ":",
            integer_to_list(?DASHBOARD_PORT),
            "\r\n",
            "Authorization: ",
            AuthValue,
            "\r\n",
            "Accept: text/plain\r\n",
            "Connection: close\r\n\r\n"
        ]),
        Resp = recv_raw(Socket, <<>>),
        [StatusLine | _] = binary:split(Resp, <<"\r\n">>),
        <<"HTTP/1.1 ", Code:3/binary, _/binary>> = StatusLine,
        {Code, Resp}
    after
        gen_tcp:close(Socket)
    end.

recv_raw(Socket, Acc) ->
    case gen_tcp:recv(Socket, 0, ?TIMEOUT) of
        {ok, Data} -> recv_raw(Socket, <<Acc/binary, Data/binary>>);
        {error, closed} -> Acc;
        {error, Reason} -> ct:fail({recv_failed, Reason, Acc})
    end.

accept_header(no_accept) -> [];
accept_header(Accept) -> [{"accept", Accept}].

admin_auth_header() ->
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(
        ?DEFAULT_SUPERUSER, ?DEFAULT_SUPERUSER_PASS
    ),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

viewer_auth_header() ->
    {ok, _} = emqx_dashboard_admin:add_user(
        ?VIEWER_USER, ?VIEWER_PASS, ?ROLE_VIEWER, ?ADD_DESCRIPTION
    ),
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(?VIEWER_USER, ?VIEWER_PASS),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

create_api_key(Name, Role) ->
    ApiKey = <<Name/binary, "-key">>,
    ApiSecret = <<Name/binary, "-secret">>,
    ExpiredAt = erlang:system_time(second) + 3600,
    {ok, _} = emqx_mgmt_auth:create(
        Name, ApiKey, ApiSecret, true, ExpiredAt, <<"configs rbac test">>, Role
    ),
    emqx_common_test_http:auth_header(binary_to_list(ApiKey), binary_to_list(ApiSecret)).

%% A sentinel in a `sensitive => true' field, used to tell cleartext from redacted.
put_sentinel() ->
    emqx_conf:update([sysmon, top, db_password], ?SENTINEL, #{
        rawconf_with_defaults => true, override_to => cluster
    }).

add_default_superuser() ->
    {ok, _NewUser} = emqx_dashboard_admin:add_user(
        ?DEFAULT_SUPERUSER,
        ?DEFAULT_SUPERUSER_PASS,
        ?ROLE_SUPERUSER,
        ?ADD_DESCRIPTION
    ).
