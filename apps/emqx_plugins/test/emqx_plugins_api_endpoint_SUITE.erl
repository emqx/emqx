%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_api_endpoint_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").

-define(SERVER, "http://127.0.0.1:18083/api/v5").
%% `emqx_plugins` does not depend on `emqx_dashboard` at compile time,
%% so the role constant is spelled out rather than included.
-define(ADMIN_ROLE, <<"administrator">>).
-define(GATEWAY_TEMPLATE, <<"/plugin_api/:plugin/[...]">>).
-define(GATEWAY_PATH, <<"/plugin_api/fake/ping">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard(),
            %% Needed by the login-user scope tests, which call
            %% `emqx_dashboard_rbac:check_login_user_scopes/2' directly.
            emqx_dashboard_rbac
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TC, Config) ->
    ok = meck:new(emqx_plugins, [non_strict, passthrough, no_link]),
    Config.

end_per_testcase(_TC, _Config) ->
    ok = meck:unload(emqx_plugins).

t_plugin_api_ok(_Config) ->
    ok = meck:expect(
        emqx_plugins,
        handle_api_call,
        fun(<<"fake">>, _Request, _Timeout) -> {200, #{ok => true}} end
    ),
    {200, Body} = request(get, ?SERVER ++ "/plugin_api/fake/ping"),
    ?assertEqual(#{<<"ok">> => true}, Body).

t_plugin_api_not_found(_Config) ->
    ok = meck:expect(
        emqx_plugins,
        handle_api_call,
        fun(_, _Request, _Timeout) ->
            {404, #{code => <<"NOT_FOUND">>, message => <<"Plugin API Not Found">>}}
        end
    ),
    {404, Body} = request(get, ?SERVER ++ "/plugin_api/nope/ping"),
    ?assertMatch(
        #{<<"code">> := <<"NOT_FOUND">>},
        Body
    ).

t_plugin_api_unauthorized(_Config) ->
    ok = meck:expect(
        emqx_plugins,
        handle_api_call,
        fun(_, _Request, _Timeout) -> {200, #{ok => true}} end
    ),
    {401, _Body} = request(get, ?SERVER ++ "/plugin_api/fake/ping", [{"x-test", "1"}]).

t_plugin_api_callback_crash(_Config) ->
    ok = meck:expect(
        emqx_plugins,
        handle_api_call,
        fun(_, _Request, _Timeout) -> {500, #{code => <<"INTERNAL_ERROR">>}} end
    ),
    {500, Body} = request(get, ?SERVER ++ "/plugin_api/fake/ping"),
    ?assertMatch(
        #{<<"code">> := <<"INTERNAL_ERROR">>},
        Body
    ).

t_plugin_api_headers_passthrough(_Config) ->
    ok = meck:expect(
        emqx_plugins,
        handle_api_call,
        fun(<<"fake">>, #{request := ReqInfo}, _Timeout) ->
            Headers = maps:get(headers, ReqInfo, #{}),
            %% cowboy lowercases all header names
            HasContentType = maps:is_key(<<"content-type">>, Headers),
            {200, #{has_content_type => HasContentType, header_count => map_size(Headers)}}
        end
    ),
    {200, Body} = request(get, ?SERVER ++ "/plugin_api/fake/ping"),
    %% Headers should be populated from the cowboy request, not empty
    ?assert(maps:get(<<"header_count">>, Body) > 0).

t_plugin_api_sensitive_headers_redacted(_Config) ->
    ok = meck:expect(
        emqx_plugins,
        handle_api_call,
        fun(<<"fake">>, #{request := ReqInfo}, _Timeout) ->
            Headers = maps:get(headers, ReqInfo, #{}),
            {200, #{
                has_authorization => maps:is_key(<<"authorization">>, Headers),
                has_cookie => maps:is_key(<<"cookie">>, Headers),
                has_x_test => maps:is_key(<<"x-test">>, Headers)
            }}
        end
    ),
    AuthHeaders = [emqx_mgmt_api_test_util:auth_header_()],
    ExtraHeaders = [{"cookie", "emqx_auth=secret; other=value"}, {"x-test", "1"}],
    {200, Body} = request(get, ?SERVER ++ "/plugin_api/fake/ping", AuthHeaders ++ ExtraHeaders),
    ?assertEqual(false, maps:get(<<"has_authorization">>, Body)),
    ?assertEqual(false, maps:get(<<"has_cookie">>, Body)),
    ?assertEqual(true, maps:get(<<"has_x_test">>, Body)).

t_plugin_api_query_string_passthrough(_Config) ->
    ok = meck:expect(
        emqx_plugins,
        handle_api_call,
        fun(<<"fake">>, #{request := ReqInfo}, _Timeout) ->
            Qs = maps:get(query_string, ReqInfo, #{}),
            {200, Qs}
        end
    ),
    {200, Body} = request(get, ?SERVER ++ "/plugin_api/fake/ping?foo=bar&used_gte=1"),
    ?assertEqual(<<"bar">>, maps:get(<<"foo">>, Body)),
    ?assertEqual(<<"1">>, maps:get(<<"used_gte">>, Body)).

t_plugin_api_path_remainder_is_percent_decoded(_Config) ->
    ok = meck:expect(
        emqx_plugins,
        handle_api_call,
        fun(<<"fake">>, #{path := [<<"user/name">>]}, _Timeout) -> {200, #{ok => true}} end
    ),
    {200, Body} = request(get, ?SERVER ++ "/plugin_api/fake/user%2Fname"),
    ?assertEqual(#{<<"ok">> => true}, Body).

%%--------------------------------------------------------------------
%% Scope tests
%%--------------------------------------------------------------------

-doc """
The gateway route declares both `plugin_api' and `system', and the
route template and a concrete request path must resolve to the same
pair. The API-key check looks the template up; the login-user check
looks a concrete path up. If the two disagree the scope gate is
enforced on only one of them.
""".
t_gateway_declares_both_scopes(_Config) ->
    ?assertEqual([?SCOPE_PLUGIN_API, ?SCOPE_SYSTEM], emqx_plugins_api_endpoint:scopes()),
    Expected = [?SCOPE_PLUGIN_API, ?SCOPE_SYSTEM],
    ?assertEqual(Expected, emqx_mgmt_api_key_scopes:path_to_scopes(?GATEWAY_TEMPLATE)),
    ?assertEqual(Expected, emqx_mgmt_api_key_scopes:path_to_scopes(?GATEWAY_PATH)),
    %% The catch-all covers an empty and a multi-segment remainder too.
    ?assertEqual(Expected, emqx_mgmt_api_key_scopes:path_to_scopes(<<"/plugin_api/fake">>)),
    ?assertEqual(
        Expected, emqx_mgmt_api_key_scopes:path_to_scopes(<<"/plugin_api/fake/a/b/c">>)
    ).

-doc """
Backward-compatibility regression: an API key holding only `system'
predates the `plugin_api' scope and must keep reaching the gateway.
This case must fail if the `system' entry is ever dropped from
`emqx_plugins_api_endpoint:scopes/0'.
""".
t_gateway_reachable_with_system_scope_key(_Config) ->
    ok = mock_plugin_ok(),
    with_api_key(<<"PLUGIN-API-SYSTEM">>, [?SCOPE_SYSTEM], fun(Auth) ->
        ?assertMatch({200, #{<<"ok">> := true}}, request(get, gateway_url(), Auth))
    end).

-doc """
An API key holding only `plugin_api' reaches the plugin gateway and
nothing else: `/configs' (the `system' scope) is rejected.
""".
t_gateway_reachable_with_plugin_api_scope_key(_Config) ->
    ok = mock_plugin_ok(),
    with_api_key(<<"PLUGIN-API-ONLY">>, [?SCOPE_PLUGIN_API], fun(Auth) ->
        ?assertMatch({200, #{<<"ok">> := true}}, request(get, gateway_url(), Auth)),
        ?assertMatch({403, _}, request(get, ?SERVER ++ "/configs", Auth))
    end).

-doc "An API key holding neither scope is rejected on the plugin gateway.".
t_gateway_denied_without_either_scope(_Config) ->
    ok = mock_plugin_ok(),
    with_api_key(<<"PLUGIN-API-NEITHER">>, [?SCOPE_MONITORING], fun(Auth) ->
        ?assertMatch({403, _}, request(get, gateway_url(), Auth))
    end).

-doc """
The login-user scope check runs on concrete request paths through
`emqx_dashboard_rbac', separately from the API-key check. Both the new
scope and the legacy `system' scope must grant the gateway there, and
neither may grant it to a user holding some other scope.
""".
t_gateway_login_user_scopes(_Config) ->
    Cases = [
        {<<"plugin_api_user">>, [?SCOPE_PLUGIN_API], true},
        {<<"system_user">>, [?SCOPE_SYSTEM], true},
        {<<"monitoring_user">>, [?SCOPE_MONITORING], false}
    ],
    lists:foreach(
        fun({Username, Scopes, Expected}) ->
            with_login_user(Username, Scopes, fun() ->
                ?assertEqual(
                    Expected,
                    emqx_dashboard_rbac:check_login_user_scopes(Username, ?GATEWAY_PATH),
                    #{username => Username, scopes => Scopes}
                )
            end)
        end,
        Cases
    ),
    %% The plugin_api scope grants the gateway only. `/configs' stays
    %% on `system'.
    with_login_user(<<"plugin_api_user2">>, [?SCOPE_PLUGIN_API], fun() ->
        ?assertNot(
            emqx_dashboard_rbac:check_login_user_scopes(
                <<"plugin_api_user2">>, <<"/configs">>
            )
        )
    end).

%%--------------------------------------------------------------------
%% Scope test helpers
%%--------------------------------------------------------------------

gateway_url() ->
    ?SERVER ++ "/plugin_api/fake/ping".

mock_plugin_ok() ->
    meck:expect(
        emqx_plugins,
        handle_api_call,
        fun(<<"fake">>, _Request, _Timeout) -> {200, #{ok => true}} end
    ).

%% Create an API key holding exactly `Scopes', run `Fun' with its
%% basic-auth header, then delete the key.
with_api_key(Name, Scopes, Fun) ->
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(<<"admin">>, <<"public">>),
    AdminAuth = {"Authorization", "Bearer " ++ binary_to_list(Token)},
    Path = emqx_mgmt_api_test_util:api_path(["api_key"]),
    Body = #{
        name => Name,
        expired_at => <<"2099-01-01T00:00:00.000Z">>,
        desc => <<"plugin api scope test">>,
        enable => true,
        scopes => Scopes
    },
    {ok, Res} = emqx_mgmt_api_test_util:request_api(post, Path, "", AdminAuth, Body),
    #{<<"api_key">> := Key, <<"api_secret">> := Secret} = emqx_utils_json:decode(Res),
    Auth = emqx_common_test_http:auth_header(binary_to_list(Key), binary_to_list(Secret)),
    try
        Fun(Auth)
    after
        DeletePath = emqx_mgmt_api_test_util:api_path(["api_key", Name]),
        {ok, _} = emqx_mgmt_api_test_util:request_api(delete, DeletePath, AdminAuth)
    end.

%% Create a dashboard login user holding exactly `Scopes', run `Fun',
%% then remove the user.
with_login_user(Username, Scopes, Fun) ->
    {ok, _} = emqx_dashboard_admin:add_user(
        Username, <<"public_Pass1!">>, ?ADMIN_ROLE, <<"plugin api scope test">>
    ),
    {ok, _} = emqx_dashboard_admin:set_user_scopes(Username, Scopes),
    try
        Fun()
    after
        _ = emqx_dashboard_admin:remove_user(Username)
    end.

request(Method, Url) ->
    request(Method, Url, emqx_mgmt_api_test_util:auth_header_()).

request(Method, Url, AuthOrHeaders) ->
    Res = emqx_mgmt_api_test_util:request_api(
        Method,
        Url,
        [],
        AuthOrHeaders,
        [],
        #{return_all => true, httpc_req_opts => [{body_format, binary}]}
    ),
    case Res of
        {ok, {{"HTTP/1.1", Code, _}, _Headers, Body}} ->
            {Code, maybe_decode(Body)};
        {error, {{"HTTP/1.1", Code, _}, _Headers, Body}} ->
            {Code, maybe_decode(Body)}
    end.

maybe_decode(Body) when is_binary(Body) ->
    case emqx_utils_json:safe_decode(Body) of
        {ok, Decoded} -> Decoded;
        {error, _} -> Body
    end;
maybe_decode(Body) ->
    Body.
