%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(
    emqx_common_test_http,
    [
        request_api/3,
        request_api/5,
        get_http_data/1
    ]
).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_dashboard.hrl").

-define(HOST, "http://127.0.0.1:18083").

-define(BASE_PATH, "/api/v5").
-define(CAPTURE(Expr), emqx_common_test_helpers:capture_io_format(fun() -> Expr end)).

-define(OVERVIEWS, [
    "alarms",
    "banned",
    "stats",
    "metrics",
    "listeners",
    "clients",
    "subscriptions"
]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    %% Load all applications to ensure swagger.json is fully generated.
    Apps = emqx_machine_boot:reboot_apps(),
    ct:pal("load apps:~p~n", [Apps]),
    lists:foreach(fun(App) -> application:load(App) end, Apps),
    SuiteApps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    _ = emqx_conf_schema:roots(),
    ok = emqx_dashboard_desc_cache:init(),
    [{suite_apps, SuiteApps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_test_case(_, Config) ->
    Config.

end_per_test_case(t_default_public_password_login_security_profile, _Config) ->
    emqx_common_test_helpers:clear_security_profile(),
    mnesia:clear_table(?ADMIN);
end_per_test_case(_Case, _Config) ->
    ok.

t_overview(_) ->
    mnesia:clear_table(?ADMIN),
    emqx_dashboard_admin:add_user(
        <<"admin">>, <<"public_www1">>, ?ROLE_SUPERUSER, <<"simple_description">>
    ),
    Headers = auth_header_(<<"admin">>, <<"public_www1">>),
    [
        {ok, _} = request_dashboard(get, api_path([Overview]), Headers)
     || Overview <- ?OVERVIEWS
    ].

t_dashboard_restart(Config) ->
    emqx_config:put([dashboard], #{
        i18n_lang => en,
        swagger_support => true,
        listeners =>
            #{
                http =>
                    #{
                        inet6 => false,
                        bind => 18083,
                        ipv6_v6only => false,
                        send_timeout => 10000,
                        num_acceptors => 8,
                        max_connections => 512,
                        backlog => 1024,
                        proxy_header => false
                    }
            }
    }),
    application:stop(emqx_dashboard),
    application:start(emqx_dashboard),
    Name = 'http:dashboard',
    t_overview(Config),
    [{'_', [], Rules}] = BaseDispatch = persistent_term:get(Name),

    %% complete dispatch has more than 150 rules.
    ?assertNotMatch([{[], [], cowboy_static, _} | _], Rules),
    ?assert(erlang:length(Rules) > 150),

    %% After we restart the dashboard, the dispatch rules should be the same.
    ok = application:stop(emqx_dashboard),
    assert_same_dispatch(BaseDispatch, Name, step_0),
    ok = application:start(emqx_dashboard),
    assert_same_dispatch(BaseDispatch, Name, step_1),
    t_overview(Config),

    %% erase to mock the initial dashboard startup.
    persistent_term:erase(Name),
    ok = application:stop(emqx_dashboard),
    ok = application:start(emqx_dashboard),
    assert_same_dispatch(BaseDispatch, Name, step_2),
    t_overview(Config),
    ok.

t_admins_add_delete(_) ->
    mnesia:clear_table(?ADMIN),
    Desc = <<"simple description">>,
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"username">>, <<"password_0">>, ?ROLE_SUPERUSER, Desc
    ),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"username1">>, <<"password1">>, ?ROLE_SUPERUSER, Desc
    ),
    Admins = emqx_dashboard_admin:all_users(),
    ?assertEqual(2, length(Admins)),
    {ok, _} = emqx_dashboard_admin:remove_user(<<"username1">>),
    Users = emqx_dashboard_admin:all_users(),
    ?assertEqual(1, length(Users)),
    {ok, _} = emqx_dashboard_admin:change_password(
        <<"username">>,
        <<"password_0">>,
        <<"new_pwd_1234">>
    ),
    timer:sleep(10),
    {ok, _} = emqx_dashboard_admin:remove_user(<<"username">>).

t_admin_delete_self_failed(_) ->
    mnesia:clear_table(?ADMIN),
    Desc = <<"simple description">>,
    _ = emqx_dashboard_admin:add_user(<<"username1">>, <<"password_1">>, ?ROLE_SUPERUSER, Desc),
    Admins = emqx_dashboard_admin:all_users(),
    ?assertEqual(1, length(Admins)),
    Header = auth_header_(<<"username1">>, <<"password_1">>),
    {error, {_, 400, _}} = request_dashboard(delete, api_path(["users", "username1"]), Header),
    Token = ["Basic ", base64:encode("username1:password_1")],
    Header2 = {"Authorization", Token},
    {error, {_, 401, _}} = request_dashboard(delete, api_path(["users", "username1"]), Header2),
    mnesia:clear_table(?ADMIN).

%% The default admin user (configured via `dashboard.default_username') is
%% a break-glass account and must never be deleted via the REST API,
%% regardless of how many other admins exist. Restarting the application
%% re-creates the default user if it is missing only when the table is
%% empty; once any admin exists, no recreation happens.
t_admin_delete_default_username(_TCConfig) ->
    mnesia:clear_table(?ADMIN),
    DefaultUsername = emqx_dashboard_admin:default_username(),
    DefaultPassword = emqx_dashboard_admin:default_password(),
    %% Sanity checks
    ?assertNotEqual(<<"">>, DefaultUsername),
    ?assertNotEqual(<<"">>, DefaultPassword),
    {ok, #{}} = emqx_dashboard_admin:add_default_user(),
    HeaderDefault = auth_header_(DefaultUsername, DefaultPassword),
    %% The default admin cannot delete itself (both the default-user
    %% protection and the self-delete guard apply).
    ?assertMatch(
        {error, {_, 400, _}},
        request_dashboard(delete, api_path(["users", DefaultUsername]), HeaderDefault)
    ),
    NewAdmin = <<"newadmin">>,
    NewPassword = <<"newadminpassword_123">>,
    {ok, #{}} = emqx_dashboard_admin:add_user(
        NewAdmin, NewPassword, ?ROLE_SUPERUSER, <<"description">>
    ),
    NewHeader = auth_header_(NewAdmin, NewPassword),
    %% Even with another admin present, the default admin remains
    %% protected from deletion.
    ?assertMatch(
        {error, {_, 400, _}},
        request_dashboard(delete, api_path(["users", DefaultUsername]), NewHeader)
    ),
    %% The new admin still cannot delete itself.
    ?assertMatch(
        {error, {_, 400, _}},
        request_dashboard(delete, api_path(["users", NewAdmin]), NewHeader)
    ),
    %% The default admin record is still present.
    ?assertMatch([_ | _], emqx_dashboard_admin:lookup_user(DefaultUsername)),
    ok.

t_default_public_password_login_security_profile(_TCConfig) ->
    mnesia:clear_table(?ADMIN),
    Username = <<"someuser">>,
    PublicPassword = <<"public">>,
    ok = emqx_dashboard_admin:force_add_user(
        Username, PublicPassword, ?ROLE_SUPERUSER, <<"public password test">>, #{}
    ),

    emqx_common_test_helpers:with_security_profile("legacy", fun() ->
        ?assertMatch(
            {ok, {{_, 200, _}, _, _}},
            login_api(Username, PublicPassword)
        )
    end),

    emqx_common_test_helpers:with_security_profile("hardened", fun() ->
        {ok, {{_, 401, _}, _, Body}} = login_api(Username, PublicPassword),
        #{
            <<"code">> := <<"BAD_USERNAME_OR_PWD">>,
            <<"message">> := Message
        } = json(Body),
        ?assertNotEqual(
            nomatch,
            binary:match(
                Message,
                <<"Default admin password must be changed before login is allowed.">>
            )
        )
    end).

t_rest_api(_Config) ->
    mnesia:clear_table(?ADMIN),
    Desc = <<"administrator">>,
    Password = <<"public_www1">>,
    emqx_dashboard_admin:add_user(<<"admin">>, Password, ?ROLE_SUPERUSER, Desc),
    {ok, 200, Res0} = http_get(["users"]),
    %% to_external_user/1 also surfaces the effective scopes list
    %% (admin -> common + login-only scopes by role-default). This test doesn't care
    %% about the exact list, so drop the field before asserting the
    %% rest of the user record.
    [User0] = get_http_data(Res0),
    ?assertEqual(
        filter_req(#{
            <<"backend">> => <<"local">>,
            <<"username">> => <<"admin">>,
            <<"description">> => <<"administrator">>,
            <<"role">> => ?ROLE_SUPERUSER,
            <<"namespace">> => null,
            <<"mfa">> => <<"none">>
        }),
        maps:remove(<<"scopes">>, User0)
    ),
    {ok, 200, _} = http_put(
        ["users", "admin"],
        filter_req(#{
            <<"role">> => ?ROLE_SUPERUSER,
            <<"description">> => <<"a_new_description">>
        })
    ),
    {ok, 200, _} = http_post(
        ["users"],
        filter_req(#{
            <<"username">> => <<"usera">>,
            <<"password">> => <<"passwd_01234">>,
            <<"role">> => ?ROLE_SUPERUSER,
            <<"mfa">> => <<"none">>,
            <<"description">> => Desc
        })
    ),
    {ok, 204, _} = http_delete(["users", "usera"]),
    {ok, 404, _} = http_delete(["users", "usera"]),
    {ok, 204, _} = http_post(
        ["users", "admin", "change_pwd"],
        #{
            <<"old_pwd">> => Password,
            <<"new_pwd">> => <<"newpwd_lkdfki1">>
        }
    ),
    mnesia:clear_table(?ADMIN),
    emqx_dashboard_admin:add_user(<<"admin">>, Password, ?ROLE_SUPERUSER, <<"administrator">>),
    ok.

t_swagger_json(_Config) ->
    %% `/api-docs/swagger.json` keeps serving the full OpenAPI 3 JSON spec
    %% so external Swagger UI deployments that point at it keep working
    %% after the bundled in-tree UI was removed.
    Url = ?HOST ++ "/api-docs/swagger.json",
    mnesia:clear_table(?ADMIN),
    emqx_dashboard_admin:add_user(
        <<"admin">>, <<"public_www1">>, ?ROLE_SUPERUSER, <<"administrator">>
    ),
    AuthHeader = auth_header_(<<"admin">>, <<"public_www1">>),
    {ok, {{"HTTP/1.1", 200, "OK"}, _Headers, Body}} =
        httpc:request(get, {Url, [AuthHeader]}, [], [{body_format, binary}]),
    ?assert(emqx_utils_json:is_json(Body)),
    Spec = emqx_utils_json:decode(Body),
    ?assertMatch(
        #{
            <<"openapi">> := <<"3.", _/binary>>,
            <<"info">> := #{<<"title">> := _, <<"version">> := _},
            <<"paths">> := _
        },
        Spec
    ),
    %% Anonymous callers get a 401 with a minimal OpenAPI stub.
    {ok, {{"HTTP/1.1", 401, _}, AnonHeaders, AnonBody}} =
        httpc:request(get, {Url, []}, [], [{body_format, binary}]),
    ?assertMatch(
        #{<<"openapi">> := <<"3.", _/binary>>},
        emqx_utils_json:decode(AnonBody)
    ),
    ?assertMatch(
        {_, "Basic realm" ++ _},
        lists:keyfind("www-authenticate", 1, AnonHeaders)
    ),
    %% Every operation's `tags' field must be a flat JSON array of
    %% strings. A regression in `trans_tags' (e.g. forgetting to
    %% flatten the chardata returned by `string:titlecase/1', or a
    %% callsite mistakenly wrapping a list tag in another list) would
    %% leak an iolist into the JSON and emit a tag such as
    %% `[68, "ashboard SSO"]', which crashes downstream OpenAPI
    %% tooling like Redocly's `slugify(tagName)'.
    BadTags = collect_non_string_tags(Spec),
    ?assertEqual([], BadTags, {non_string_tags_in_openapi_spec, BadTags}),
    ok.

-doc """
Every tagged operation in every production API module must carry a
non-empty binary `summary'.  Without it, Redoc falls back to the first
~50 chars of the description for the sidebar title and operation header,
which silently truncates a complete sentence (e.g. `GET /banned' once
surfaced "List all currently banned client IDs, usernames an").

This guard is paired with the boot-time enforcement in
`emqx_dashboard_swagger:enforce_method_desc_policy/1' so a future
regression cannot ship even if the boot check is loosened.

We walk production API modules directly via
`emqx_dashboard_swagger:spec/2' instead of the live `/api-docs/swagger.json'
spec, because the test build profile bundles test-SUITE modules into the
dashboard app's module list and they pollute the live spec with
intentionally permissive synthetic operations.
""".
t_swagger_summary_required(_Config) ->
    Modules = production_api_modules(),
    ?assert(length(Modules) > 30, {too_few_api_modules, length(Modules)}),
    Missing = lists:flatmap(fun module_missing_summaries/1, Modules),
    ?assertEqual([], Missing, {ops_missing_summary, Missing}).

production_api_modules() ->
    %% Use `emqx_machine_boot:reboot_apps/0' rather than
    %% `application:loaded_applications/0': the latter only sees apps
    %% the SUITE happens to have loaded, so a future init_per_suite
    %% trimming could silently shrink coverage to a handful of apps.
    %% `reboot_apps/0' is the canonical EMQX umbrella business-app list
    %% and lets us load each one explicitly here.
    Apps = emqx_machine_boot:reboot_apps(),
    AlreadyLoaded = [App || {App, _, _} <- application:loaded_applications()],
    NewlyLoaded = [App || App <- Apps, ensure_loaded(App, AlreadyLoaded)],
    %% `cth_suite' unloads the apps it started; we only need to
    %% unload the ones we just loaded ourselves, so other code that
    %% relies on the original load state isn't surprised.
    emqx_common_test_helpers:on_exit(
        fun() -> lists:foreach(fun application:unload/1, NewlyLoaded) end
    ),
    AllModules = lists:flatten([app_modules(App) || App <- Apps]),
    [
        M
     || M <- AllModules,
        not is_suite_module(M),
        implements_minirest_api(M)
    ].

%% Returns true when this call actually loaded the app (so the caller
%% knows to undo it later).
ensure_loaded(App, AlreadyLoaded) ->
    case lists:member(App, AlreadyLoaded) of
        true ->
            false;
        false ->
            case application:load(App) of
                ok ->
                    true;
                {error, {already_loaded, _}} ->
                    false;
                {error, Other} ->
                    ct:pal("Skip ~p: ~p", [App, Other]),
                    false
            end
    end.

app_modules(App) ->
    case application:get_key(App, modules) of
        {ok, Modules} -> Modules;
        undefined -> []
    end.

is_suite_module(Module) ->
    case lists:reverse(atom_to_list(Module)) of
        "ETIUS_" ++ _ -> true;
        _ -> false
    end.

implements_minirest_api(Module) ->
    try
        Attrs = Module:module_info(attributes),
        Behaviours =
            proplists:get_all_values(behaviour, Attrs) ++
                proplists:get_all_values(behavior, Attrs),
        lists:member(minirest_api, lists:flatten(Behaviours))
    catch
        _:_ -> false
    end.

module_missing_summaries(Module) ->
    try
        {ApiSpec, _Components} = emqx_dashboard_swagger:spec(Module, #{check_schema => false}),
        lists:flatmap(
            fun({Path, MethodSpecs, _Refs, _Extras}) ->
                missing_in_path(Module, Path, MethodSpecs)
            end,
            ApiSpec
        )
    catch
        Class:Reason:Stack ->
            ct:pal(
                "Failed to introspect spec for ~p:~n~p:~p~n~p",
                [Module, Class, Reason, Stack]
            ),
            [{Module, spec_introspection_failed, {Class, Reason}}]
    end.

missing_in_path(Module, Path, MethodSpecs) when is_map(MethodSpecs) ->
    maps:fold(
        fun(Method, OpSpec, Acc) ->
            case is_op_spec(OpSpec) andalso has_tags(OpSpec) of
                true ->
                    case maps:get(summary, OpSpec, maps:get(<<"summary">>, OpSpec, undefined)) of
                        S when is_binary(S), byte_size(S) > 0 ->
                            Acc;
                        Other ->
                            [{Module, Path, Method, Other} | Acc]
                    end;
                false ->
                    Acc
            end
        end,
        [],
        MethodSpecs
    );
missing_in_path(_Module, _Path, _Other) ->
    [].

is_op_spec(Spec) when is_map(Spec) -> true;
is_op_spec(_) -> false.

has_tags(#{tags := Tags}) -> is_list(Tags) andalso Tags =/= [];
has_tags(#{<<"tags">> := Tags}) -> is_list(Tags) andalso Tags =/= [];
has_tags(_) -> false.

collect_non_string_tags(#{<<"paths">> := Paths}) ->
    maps:fold(
        fun(Path, Methods, Acc) ->
            maps:fold(
                fun
                    (Method, #{<<"tags">> := Tags}, InnerAcc) when is_list(Tags) ->
                        case [T || T <- Tags, not is_binary(T)] of
                            [] -> InnerAcc;
                            Bad -> [{Path, Method, Bad} | InnerAcc]
                        end;
                    (_Method, _Op, InnerAcc) ->
                        InnerAcc
                end,
                Acc,
                Methods
            )
        end,
        [],
        Paths
    ).

t_disable_swagger_json(_Config) ->
    %% All `/api-docs*` and `/api-spec*` paths share gating with
    %% `dashboard.swagger_support`: flipping it to false makes them
    %% all 404, and flipping it back restores them.
    RedirectUrl = ?HOST ++ "/api-docs",
    OkUrls = [
        ?HOST ++ "/api-docs/swagger.json",
        ?HOST ++ "/api-spec.html",
        ?HOST ++ "/api-spec.md",
        ?HOST ++ "/api-spec.json"
    ],
    mnesia:clear_table(?ADMIN),
    emqx_dashboard_admin:add_user(
        <<"admin">>, <<"public_www1">>, ?ROLE_SUPERUSER, <<"administrator">>
    ),
    AuthHeader = auth_header_(<<"admin">>, <<"public_www1">>),
    AssertStatus =
        fun(Status, Url, Headers) ->
            ?assertMatch(
                {ok, {{"HTTP/1.1", Status, _}, _, _}},
                httpc:request(
                    get, {Url, Headers}, [{autoredirect, false}], [{body_format, binary}]
                ),
                #{url => Url, expected_status => Status}
            )
        end,
    %% Initial state: redirect returns 308 (no auth needed); the rest 200
    %% with auth and 401 without.
    AssertStatus(308, RedirectUrl, []),
    lists:foreach(fun(U) -> AssertStatus(200, U, [AuthHeader]) end, OkUrls),
    lists:foreach(fun(U) -> AssertStatus(401, U, []) end, OkUrls),
    DashboardCfg = emqx:get_raw_config([dashboard]),
    ?check_trace(
        {_, {ok, _}} = ?wait_async_action(
            begin
                DashboardCfg2 = DashboardCfg#{<<"swagger_support">> => false},
                emqx:update_config([dashboard], DashboardCfg2)
            end,
            #{?snk_kind := regenerate_dispatch, i18n_lang := en},
            3_000
        ),
        []
    ),
    lists:foreach(fun(U) -> AssertStatus(404, U, [AuthHeader]) end, [RedirectUrl | OkUrls]),
    ?check_trace(
        {_, {ok, _}} = ?wait_async_action(
            begin
                DashboardCfg3 = DashboardCfg#{<<"swagger_support">> => true},
                emqx:update_config([dashboard], DashboardCfg3)
            end,
            #{?snk_kind := regenerate_dispatch, i18n_lang := en},
            3_000
        ),
        []
    ),
    AssertStatus(308, RedirectUrl, []),
    lists:foreach(fun(U) -> AssertStatus(200, U, [AuthHeader]) end, OkUrls).

t_cli(_Config) ->
    [mria:dirty_delete(?ADMIN, Admin) || Admin <- mnesia:dirty_all_keys(?ADMIN)],
    emqx_dashboard_cli:admins(["add", "username", "password_ww2"]),
    [#?ADMIN{username = <<"username">>, pwdhash = PwdHash}] =
        emqx_dashboard_admin:lookup_user(<<"username">>),
    ?assertEqual(ok, emqx_dashboard_admin:verify_hash(<<"password_ww2">>, PwdHash)),
    emqx_dashboard_cli:admins(["passwd", "username", "new_password"]),
    [#?ADMIN{username = <<"username">>, pwdhash = PwdHash1}] =
        emqx_dashboard_admin:lookup_user(<<"username">>),
    ?assertEqual(ok, emqx_dashboard_admin:verify_hash(<<"new_password">>, PwdHash1)),
    emqx_dashboard_cli:admins(["del", "username"]),
    [] = emqx_dashboard_admin:lookup_user(<<"username">>),
    emqx_dashboard_cli:admins(["add", "admin1", "pass_lkdfkd1"]),
    emqx_dashboard_cli:admins(["add", "admin2", "w_pass_lkdfkd2"]),
    AdminList = emqx_dashboard_admin:all_users(),
    ?assertEqual(2, length(AdminList)),
    lists:foreach(
        fun(#{name := Name}) -> ok = mria:dirty_delete(emqx_app, Name) end,
        emqx_mgmt_auth:list()
    ),
    {ok, [CreateOutput]} = ?CAPTURE(
        emqx_dashboard_cli:api_keys([
            "add",
            "--name",
            "test-key",
            "--desc",
            "test description",
            "--role",
            "viewer"
        ])
    ),
    CreateJSON = emqx_utils_json:decode(iolist_to_binary(CreateOutput), [return_maps]),
    ?assertMatch(#{<<"name">> := <<"test-key">>, <<"api_secret">> := _}, CreateJSON),
    ?assertMatch(
        {ok, #{
            name := <<"test-key">>,
            enable := true,
            desc := <<"test description">>,
            role := <<"viewer">>
        }},
        emqx_mgmt_auth:read(<<"test-key">>)
    ),
    ?assertMatch({ok, _}, ?CAPTURE(emqx_dashboard_cli:api_keys(["show", "--name", "test-key"]))),
    emqx_dashboard_cli:api_keys(["list"]),
    emqx_dashboard_cli:api_keys(["disable", "--name", "test-key"]),
    ?assertMatch({ok, #{enable := false}}, emqx_mgmt_auth:read(<<"test-key">>)),
    emqx_dashboard_cli:api_keys(["enable", "--name", "test-key"]),
    ?assertMatch({ok, #{enable := true}}, emqx_mgmt_auth:read(<<"test-key">>)),
    emqx_dashboard_cli:api_keys(["del", "--name", "test-key"]),
    ?assertMatch({error, not_found}, emqx_mgmt_auth:read(<<"test-key">>)),
    BeforeCreate = erlang:system_time(second),
    emqx_dashboard_cli:api_keys([
        "add",
        "--name",
        "test-key-expiring",
        "--valid-days",
        "3"
    ]),
    AfterCreate = erlang:system_time(second),
    ?assertMatch(
        {ok, #{expired_at := ExpiredAt}} when
            is_integer(ExpiredAt) andalso
                ExpiredAt >= BeforeCreate + 3 * 24 * 60 * 60 andalso
                ExpiredAt =< AfterCreate + 3 * 24 * 60 * 60,
        emqx_mgmt_auth:read(<<"test-key-expiring">>)
    ),
    Secret = <<"12345678901234567890123456789012">>,
    {ok, [CreateWithSecretOutput]} = ?CAPTURE(
        emqx_dashboard_cli:api_keys([
            "add",
            "--name",
            "test-key-2",
            "--api-secret",
            binary_to_list(Secret),
            "--valid-days",
            "infinity"
        ])
    ),
    CreateWithSecretJSON =
        emqx_utils_json:decode(iolist_to_binary(CreateWithSecretOutput), [return_maps]),
    ?assertEqual(false, maps:is_key(<<"api_secret">>, CreateWithSecretJSON)),
    [{emqx_app, <<"test-key-2">>, _APIKey, SecretHash, _Enable, _Extra, _ExpiredAt, _CreatedAt}] =
        mnesia:dirty_read(emqx_app, <<"test-key-2">>),
    ?assertEqual(ok, emqx_dashboard_admin:verify_hash(Secret, SecretHash)),
    {ok, [ShortSecretError]} = ?CAPTURE(
        emqx_dashboard_cli:api_keys([
            "add",
            "--name",
            "test-key-3",
            "--api-secret",
            "short-secret"
        ])
    ),
    ?assertMatch(#{<<"error">> := _}, json(iolist_to_binary(ShortSecretError))),
    ?assertMatch({error, not_found}, emqx_mgmt_auth:read(<<"test-key-3">>)),
    {ok, [BadValidDaysError]} = ?CAPTURE(
        emqx_dashboard_cli:api_keys([
            "add",
            "--name",
            "test-key-4",
            "--valid-days",
            "0"
        ])
    ),
    ?assertMatch(#{<<"error">> := _}, json(iolist_to_binary(BadValidDaysError))),
    ?assertMatch({error, not_found}, emqx_mgmt_auth:read(<<"test-key-4">>)),
    %% --scopes: valid list is stored on the app record, both with and without --api-secret.
    ?CAPTURE(
        emqx_dashboard_cli:api_keys([
            "add",
            "--name",
            "test-key-scoped",
            "--scopes",
            "connections,publish"
        ])
    ),
    ?assertMatch(
        {ok, #{scopes := [<<"connections">>, <<"publish">>]}},
        emqx_mgmt_auth:read(<<"test-key-scoped">>)
    ),
    ?CAPTURE(
        emqx_dashboard_cli:api_keys([
            "add",
            "--name",
            "test-key-scoped-with-secret",
            "--api-secret",
            binary_to_list(Secret),
            "--scopes",
            "monitoring"
        ])
    ),
    ?assertMatch(
        {ok, #{scopes := [<<"monitoring">>]}},
        emqx_mgmt_auth:read(<<"test-key-scoped-with-secret">>)
    ),
    %% --scopes: unknown scope is rejected and no key is created.
    {ok, [BadScopeError]} = ?CAPTURE(
        emqx_dashboard_cli:api_keys([
            "add",
            "--name",
            "test-key-bad-scope",
            "--scopes",
            "connections,nonexistent"
        ])
    ),
    ?assertMatch(#{<<"error">> := _}, json(iolist_to_binary(BadScopeError))),
    ?assertMatch({error, not_found}, emqx_mgmt_auth:read(<<"test-key-bad-scope">>)).

t_lookup_by_username_jwt(_Config) ->
    User = bin(["user-", integer_to_list(random_num())]),
    emqx_dashboard_token:sign(#?ADMIN{username = User}),
    ?assertMatch(
        [#?ADMIN_JWT{username = User}],
        emqx_dashboard_token:lookup_by_username(User)
    ),
    ok = emqx_dashboard_token:destroy_by_username(User),
    %% issue a gen_server call to sync the async destroy gen_server cast
    ok = gen_server:call(emqx_dashboard_token, dummy, infinity),
    ?assertMatch([], emqx_dashboard_token:lookup_by_username(User)),
    ok.

t_clean_expired_jwt(_Config) ->
    User = bin(["user-", integer_to_list(random_num())]),
    emqx_dashboard_token:sign(#?ADMIN{username = User}),
    [#?ADMIN_JWT{username = User, exptime = ExpTime}] =
        emqx_dashboard_token:lookup_by_username(User),
    ok = emqx_dashboard_token:clean_expired_jwt(_Now1 = ExpTime),
    ?assertMatch(
        [#?ADMIN_JWT{username = User}],
        emqx_dashboard_token:lookup_by_username(User)
    ),
    ok = emqx_dashboard_token:clean_expired_jwt(_Now2 = ExpTime + 1),
    ?assertMatch([], emqx_dashboard_token:lookup_by_username(User)),
    ok.

t_default_password_file(Config) ->
    Password = <<"passwordfromfile">>,
    Passfile = filename:join(?config(priv_dir, Config), "passfile"),
    FileURI = iolist_to_binary([<<"file://">>, Passfile]),
    ok = file:write_file(Passfile, Password),
    Port = 18089,
    AppSpecs = [
        emqx_conf,
        {emqx_dashboard, #{
            config =>
                #{
                    <<"dashboard">> =>
                        #{
                            <<"listeners">> => #{
                                <<"http">> => #{
                                    <<"enable">> => true,
                                    %% to avoid clash with master test node
                                    <<"bind">> => Port
                                }
                            },
                            <<"default_password">> => FileURI
                        }
                }
        }}
    ],
    Nodes = emqx_cth_cluster:start(
        [{dash_default_pass1, #{apps => AppSpecs}}],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    Username = <<"admin">>,
    URL = "http://127.0.0.1:" ++ integer_to_list(Port) ++ filename:join([?BASE_PATH, "login"]),
    Body = emqx_utils_json:encode(#{username => Username, password => Password}),
    ?assertMatch(
        {ok, {{_, 200, _}, _, _}},
        httpc:request(post, {URL, [], "application/json", Body}, [], [{body_format, binary}])
    ),
    emqx_cth_cluster:stop(Nodes),
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

bin(X) -> iolist_to_binary(X).

random_num() ->
    erlang:system_time(nanosecond).

http_get(Parts) ->
    request_api(get, api_path(Parts), auth_header_(<<"admin">>, <<"public_www1">>)).

http_delete(Parts) ->
    request_api(delete, api_path(Parts), auth_header_(<<"admin">>, <<"public_www1">>)).

http_post(Parts, Body) ->
    request_api(post, api_path(Parts), [], auth_header_(<<"admin">>, <<"public_www1">>), Body).

http_put(Parts, Body) ->
    request_api(put, api_path(Parts), [], auth_header_(<<"admin">>, <<"public_www1">>), Body).

request_dashboard(Method, Url, Auth) ->
    Request = {Url, [Auth]},
    do_request_dashboard(Method, Request).
request_dashboard(Method, Url, QueryParams, Auth) ->
    Request = {Url ++ "?" ++ QueryParams, [Auth]},
    do_request_dashboard(Method, Request).

do_request_dashboard(Method, {Url, _} = Request) ->
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, maybe_ssl(Url), []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _Headers, Return}} when
            Code >= 200 andalso Code =< 299
        ->
            {ok, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.

maybe_ssl("http://" ++ _) -> [];
maybe_ssl("https://" ++ _) -> [{ssl, [{verify, verify_none}]}].

auth_header_() ->
    auth_header_(<<"admin">>, <<"public">>).

auth_header_(Username, Password) ->
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(Username, Password),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

login_api(Username, Password) ->
    Body = emqx_utils_json:encode(#{username => Username, password => Password}),
    httpc:request(
        post,
        {?HOST ++ filename:join([?BASE_PATH, "login"]), [], "application/json", Body},
        [],
        [{body_format, binary}]
    ).

api_path(Parts) ->
    ?HOST ++ filename:join([?BASE_PATH | Parts]).

json(Data) ->
    emqx_utils_json:decode(Data).

assert_same_dispatch([{'_', [], BaseRoutes}], Name, Tag) ->
    [{'_', [], NewRoutes}] = persistent_term:get(Name, Tag),
    snabbkaffe_diff:assert_lists_eq(BaseRoutes, NewRoutes, #{comment => Tag}).

filter_req(Req) ->
    Req.
