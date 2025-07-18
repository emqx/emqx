%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_dashboard_admin_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_dashboard.hrl").
-include("emqx_dashboard_rbac.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
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

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

bearer_auth_header(Token) ->
    {"Authorization", iolist_to_binary(["Bearer ", Token])}.

create_superuser() ->
    emqx_common_test_http:create_default_app(),
    Username = <<"superuser">>,
    Password = <<"secretP@ss1">>,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, ?ROLE_SUPERUSER, <<"desc">>),
    {200, #{<<"token">> := Token}} = login(#{<<"username">> => Username, <<"password">> => Password}),
    {"Authorization", iolist_to_binary(["Bearer ", Token])}.

login(Params) ->
    URL = emqx_mgmt_api_test_util:api_path(["login"]),
    emqx_mgmt_api_test_util:simple_request(#{
        auth_header => [{"no", "auth"}],
        method => post,
        url => URL,
        body => Params
    }).

create_user_api(Params, AuthHeader) ->
    URL = emqx_mgmt_api_test_util:api_path(["users"]),
    emqx_mgmt_api_test_util:simple_request(#{
        auth_header => AuthHeader,
        method => post,
        url => URL,
        body => Params
    }).

update_user_api(Username, Params, AuthHeader) ->
    URL = emqx_mgmt_api_test_util:api_path(["users", Username]),
    emqx_mgmt_api_test_util:simple_request(#{
        auth_header => AuthHeader,
        method => put,
        url => URL,
        body => Params
    }).

create_api_key_api(Params, AuthHeader) ->
    URL = emqx_mgmt_api_test_util:api_path(["api_key"]),
    emqx_mgmt_api_test_util:simple_request(#{
        auth_header => AuthHeader,
        method => post,
        url => URL,
        body => Params
    }).

to_rfc3339(Sec) ->
    list_to_binary(calendar:system_time_to_rfc3339(Sec)).

bin(X) -> emqx_utils_conv:bin(X).

umbrella_apps() ->
    [
        App
     || {App, _, _} <- application:loaded_applications(),
        case re:run(atom_to_list(App), "^emqx", [{capture, none}]) of
            match -> true;
            nomatch -> false
        end
    ].

all_handlers() ->
    AllMethods = [get, post, put, delete],
    Mods = minirest_api:find_api_modules(umbrella_apps()),
    lists:flatmap(
        fun(Mod) ->
            Paths = Mod:paths(),
            lists:flatmap(
                fun(Path) ->
                    #{'operationId' := Fn} = Sc = Mod:schema(Path),
                    [
                        #{method => M, module => Mod, function => Fn}
                     || M <- maps:keys(Sc), lists:member(M, AllMethods)
                    ]
                end,
                Paths
            )
        end,
        Mods
    ).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_check_user(_) ->
    Username = <<"admin1">>,
    Password = <<"public_1">>,
    BadUsername = <<"admin_bad">>,
    BadPassword = <<"public_bad">>,
    EmptyUsername = <<>>,
    EmptyPassword = <<>>,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, ?ROLE_SUPERUSER, <<"desc">>),
    {ok, _} = emqx_dashboard_admin:check(Username, Password),
    {error, <<"password_error">>} = emqx_dashboard_admin:check(Username, BadPassword),
    {error, <<"username_not_found">>} = emqx_dashboard_admin:check(BadUsername, Password),
    {error, <<"username_not_found">>} = emqx_dashboard_admin:check(BadUsername, BadPassword),
    {error, <<"username_not_found">>} = emqx_dashboard_admin:check(EmptyUsername, Password),
    {error, <<"password_error">>} = emqx_dashboard_admin:check(Username, EmptyPassword),
    {error, <<"username_not_provided">>} = emqx_dashboard_admin:check(undefined, Password),
    {error, <<"password_not_provided">>} = emqx_dashboard_admin:check(Username, undefined),
    ok.

t_add_user(_) ->
    AddUser = <<"add_user">>,
    AddPassword = <<"add_password">>,
    AddDescription = <<"add_description">>,

    BadAddUser = <<"***add_user_bad">>,

    %% add success. not return password
    {ok, NewUser} = emqx_dashboard_admin:add_user(
        AddUser, AddPassword, ?ROLE_SUPERUSER, AddDescription
    ),
    AddUser = maps:get(username, NewUser),
    AddDescription = maps:get(description, NewUser),
    false = maps:is_key(password, NewUser),

    %% add again
    {error, <<"username_already_exists">>} =
        emqx_dashboard_admin:add_user(AddUser, AddPassword, ?ROLE_SUPERUSER, AddDescription),

    %% add bad username
    BadNameError =
        <<"Bad Username. Only upper and lower case letters, numbers and underscores are supported">>,
    {error, BadNameError} = emqx_dashboard_admin:add_user(
        BadAddUser, AddPassword, ?ROLE_SUPERUSER, AddDescription
    ),
    ok.

t_lookup_user(_) ->
    LookupUser = <<"lookup_user">>,
    LookupPassword = <<"lookup_password">>,
    LookupDescription = <<"lookup_description">>,

    BadLookupUser = <<"***lookup_user_bad">>,

    {ok, _} =
        emqx_dashboard_admin:add_user(
            LookupUser, LookupPassword, ?ROLE_SUPERUSER, LookupDescription
        ),
    %% lookup success. not return password
    [#emqx_admin{username = LookupUser, description = LookupDescription}] =
        emqx_dashboard_admin:lookup_user(LookupUser),

    [] = emqx_dashboard_admin:lookup_user(BadLookupUser),
    ok.

t_all_users(_) ->
    Username = <<"admin_all">>,
    Password = <<"public_2">>,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, ?ROLE_SUPERUSER, <<"desc">>),
    All = emqx_dashboard_admin:all_users(),
    ?assert(erlang:length(All) >= 1),
    ok.

t_delete_user(_) ->
    DeleteUser = <<"delete_user">>,
    DeletePassword = <<"delete_password">>,
    DeleteDescription = <<"delete_description">>,

    DeleteBadUser = <<"delete_user_bad">>,

    {ok, _NewUser} =
        emqx_dashboard_admin:add_user(
            DeleteUser, DeletePassword, ?ROLE_SUPERUSER, DeleteDescription
        ),
    {ok, ok} = emqx_dashboard_admin:remove_user(DeleteUser),
    %% remove again
    {error, <<"username_not_found">>} = emqx_dashboard_admin:remove_user(DeleteUser),
    {error, <<"username_not_found">>} = emqx_dashboard_admin:remove_user(DeleteBadUser),
    ok.

t_update_user(_) ->
    UpdateUser = <<"update_user">>,
    UpdatePassword = <<"update_password">>,
    UpdateDescription = <<"update_description">>,

    NewDesc = <<"new_description">>,

    BadUpdateUser = <<"update_user_bad">>,

    {ok, _} = emqx_dashboard_admin:add_user(
        UpdateUser, UpdatePassword, ?ROLE_SUPERUSER, UpdateDescription
    ),
    {ok, NewUserInfo} =
        emqx_dashboard_admin:update_user(UpdateUser, ?ROLE_SUPERUSER, NewDesc),
    UpdateUser = maps:get(username, NewUserInfo),
    NewDesc = maps:get(description, NewUserInfo),

    {error, <<"username_not_found">>} = emqx_dashboard_admin:update_user(
        BadUpdateUser, ?ROLE_SUPERUSER, NewDesc
    ),
    ok.

t_change_password(_) ->
    User = <<"change_user">>,
    OldPassword = <<"change_password">>,
    Description = <<"change_description">>,

    NewPassword = <<"new_password">>,
    NewBadPassword = <<"public">>,

    BadChangeUser = <<"change_user_bad">>,

    {ok, _} = emqx_dashboard_admin:add_user(User, OldPassword, ?ROLE_SUPERUSER, Description),

    {ok, ok} = emqx_dashboard_admin:change_password(User, OldPassword, NewPassword),
    %% change pwd again
    {error, <<"password_error">>} =
        emqx_dashboard_admin:change_password(User, OldPassword, NewPassword),

    {error, <<"The range of password length is 8~64">>} =
        emqx_dashboard_admin:change_password(User, NewPassword, NewBadPassword),

    {error, <<"username_not_found">>} =
        emqx_dashboard_admin:change_password(BadChangeUser, OldPassword, NewPassword),
    ok.

t_clean_token(_) ->
    Username = <<"admin_token">>,
    Password = <<"public_www1">>,
    NewPassword = <<"public_www2">>,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, ?ROLE_SUPERUSER, <<"desc">>),
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(Username, Password),
    FakeReq = #{},
    FakeHandlerInfo = #{method => get, function => any, module => any},
    {ok, #{actor := Username}} = emqx_dashboard_admin:verify_token(FakeReq, FakeHandlerInfo, Token),
    %% change password
    {ok, _} = emqx_dashboard_admin:change_password(Username, Password, NewPassword),
    timer:sleep(5),
    {error, not_found} = emqx_dashboard_admin:verify_token(FakeReq, FakeHandlerInfo, Token),
    %% remove user
    {ok, #{token := Token2}} = emqx_dashboard_admin:sign_token(Username, NewPassword),
    {ok, #{actor := Username}} = emqx_dashboard_admin:verify_token(
        FakeReq, FakeHandlerInfo, Token2
    ),
    {ok, _} = emqx_dashboard_admin:remove_user(Username),
    timer:sleep(5),
    {error, not_found} = emqx_dashboard_admin:verify_token(FakeReq, FakeHandlerInfo, Token2),
    ok.

t_password_expired(_) ->
    Username = <<"t_password_expired">>,
    Password = <<"public_www1">>,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, ?ROLE_SUPERUSER, <<"desc">>),
    {ok, #{token := _Token}} = emqx_dashboard_admin:sign_token(Username, Password),
    [#?ADMIN{extra = #{password_ts := PwdTS}} = User] = emqx_dashboard_admin:lookup_user(Username),
    PwdTS2 = PwdTS - 86400 * 2,
    emqx_dashboard_admin:unsafe_update_user(User#?ADMIN{extra = #{password_ts => PwdTS2}}),
    SignResult = emqx_dashboard_admin:sign_token(Username, Password),
    ?assertMatch({ok, #{password_expire_in_seconds := X}} when X =< -86400, SignResult),
    Now = erlang:system_time(second),
    timer:sleep(1000),
    emqx_dashboard_admin:change_password_trusted(Username, Password),
    [#?ADMIN{extra = #{password_ts := PwdTS3}}] = emqx_dashboard_admin:lookup_user(Username),
    ?assert(PwdTS3 > PwdTS),
    ?assert(PwdTS3 > Now),
    ok.

-doc """
Checks that we can create and use an user that is scoped to a namespace.
""".
t_namespaced_user(_TCConfig) ->
    GlobalAdminHeader = create_superuser(),
    Username1 = <<"iminans">>,
    Password1 = <<"superSecureP@ss">>,
    Namespace1 = <<"ns1">>,
    AdminRole1 = <<"ns:ns1::administrator">>,
    %% Unknown role
    BadRole1 = <<"ns:ns1::blobber">>,
    ?assertMatch(
        {400, #{
            <<"message">> := <<"Role does not exist">>
        }},
        create_user_api(
            #{
                <<"username">> => Username1,
                <<"password">> => Password1,
                <<"role">> => BadRole1,
                <<"description">> => <<"???">>
            },
            GlobalAdminHeader
        )
    ),
    %% Ok
    ?assertMatch(
        {200, #{
            <<"namespace">> := Namespace1,
            <<"role">> := <<"administrator">>
        }},
        create_user_api(
            #{
                <<"username">> => Username1,
                <<"password">> => Password1,
                <<"role">> => AdminRole1,
                <<"description">> => <<"namespaced person">>
            },
            GlobalAdminHeader
        )
    ),
    %% Unknown role
    ?assertMatch(
        {400, #{
            <<"message">> := <<"Role does not exist">>
        }},
        update_user_api(
            Username1,
            #{
                <<"role">> => BadRole1,
                <<"description">> => <<"???">>
            },
            GlobalAdminHeader
        )
    ),
    %% Login
    {200, #{<<"token">> := Token1, <<"namespace">> := Namespace1}} =
        login(#{<<"username">> => Username1, <<"password">> => Password1}),
    %% User updating self to viewer: at the time of writing, this is not yet supported.
    ViewerRole1 = <<"ns:ns1::viewer">>,
    ?assertMatch(
        {403, _},
        update_user_api(
            Username1,
            #{
                <<"role">> => ViewerRole1,
                <<"description">> => <<"namespaced viewer person">>
            },
            bearer_auth_header(Token1)
        )
    ),
    %% User should not be able to create other users in own namespace (unsupported right
    %% now)
    {200, #{<<"token">> := Token2, <<"namespace">> := Namespace1}} =
        login(#{<<"username">> => Username1, <<"password">> => Password1}),
    Username1B = <<"iminthesamens">>,
    Password1B = <<"superSecureP@ss!">>,
    ?assertMatch(
        {403, _},
        create_user_api(
            #{
                <<"username">> => Username1B,
                <<"password">> => Password1B,
                <<"role">> => ViewerRole1,
                <<"description">> => <<"just another viewer">>
            },
            bearer_auth_header(Token2)
        )
    ),
    %% User should not be able to create other users outside its namespace
    Username2 = <<"iminanotherns">>,
    Password2 = <<"superSecureP@ss!!">>,
    ViewerRole2 = <<"ns:ns2::viewer">>,
    ?assertMatch(
        {403, _},
        create_user_api(
            #{
                <<"username">> => Username2,
                <<"password">> => Password2,
                <<"role">> => ViewerRole2,
                <<"description">> => <<"???">>
            },
            bearer_auth_header(Token2)
        )
    ),
    %% Global user (without namespace)
    GlobalUser = <<"imnotinans">>,
    GlobalViewerRole = <<"viewer">>,
    ?assertMatch(
        {200, #{
            <<"namespace">> := null,
            <<"role">> := <<"viewer">>
        }},
        create_user_api(
            #{
                <<"username">> => GlobalUser,
                <<"password">> => Password1,
                <<"role">> => GlobalViewerRole,
                <<"description">> => <<"non-namespaced person">>
            },
            GlobalAdminHeader
        )
    ),
    ?assertMatch(
        {200, #{<<"token">> := _, <<"namespace">> := null}},
        login(#{<<"username">> => GlobalUser, <<"password">> => Password1})
    ),
    ok.

-doc """
Checks that we can create and use an API key that is scoped to a namespace.
""".
t_namespaced_api_key(_TCConfig) ->
    GlobalAdminHeader = create_superuser(),
    APIKeyName1 = <<"api1">>,
    ExpiresAt = to_rfc3339(erlang:system_time(second) + 1_000),
    Namespace1 = <<"ns1">>,
    AdminRole1 = <<"ns:ns1::administrator">>,
    Res1 = create_api_key_api(
        #{
            <<"name">> => APIKeyName1,
            <<"expired_at">> => ExpiresAt,
            <<"desc">> => <<"namespaced api 1">>,
            <<"enable">> => true,
            <<"role">> => AdminRole1
        },
        GlobalAdminHeader
    ),
    ?assertMatch(
        {200, #{
            <<"role">> := <<"administrator">>,
            <<"namespace">> := Namespace1
        }},
        Res1
    ),
    {200, #{<<"api_key">> := Key1, <<"api_secret">> := Secret1}} = Res1,
    %% At this moment, cannot perform any mutations (authorization will be added in the future)
    ?assertMatch(
        {403, _},
        emqx_mgmt_api_test_util:simple_request(#{
            auth_header => emqx_common_test_http:auth_header(Key1, Secret1),
            method => post,
            url => emqx_mgmt_api_test_util:api_path(["banned"]),
            body => #{<<"as">> => <<"peerhost">>, <<"who">> => <<"127.0.0.1">>}
        })
    ),
    ok.

-doc """
Simple assertions about namespaced user permissions.

   - Both viewers and admins can `GET` anything, even outside their namespace.  Namespaces
     are mostly to avoid accidentally mutating the wrong resources rather than hiding
     information.
""".
t_namespaced_user_permissions(_TCConfig) ->
    GlobalAdminHeader = create_superuser(),
    Username = <<"iminans">>,
    Password = <<"superSecureP@ss">>,
    AdminRole = <<"ns:ns1::", ?ROLE_SUPERUSER/binary>>,
    {200, _} = create_user_api(
        #{
            <<"username">> => Username,
            <<"password">> => Password,
            <<"role">> => AdminRole,
            <<"description">> => <<"namespaced person">>
        },
        GlobalAdminHeader
    ),
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(Username, Password),
    AllHandlers = [_ | _] = all_handlers(),
    GetHandlers = [_ | _] = [FHI || #{method := get} = FHI <- AllHandlers],
    FakeReq = #{},
    Failures =
        lists:filtermap(
            fun(FakeHandlerInfo) ->
                case emqx_dashboard_admin:verify_token(FakeReq, FakeHandlerInfo, Token) of
                    {ok, _} ->
                        false;
                    {error, _} ->
                        true
                end
            end,
            GetHandlers
        ),
    maybe
        [_ | _] ?= Failures,
        ct:fail({should_have_been_allowed, Failures})
    end,
    ok.

-doc """
Checks the authorization behavior of namespaced publisher API keys.

Currently, they are not authorized even to use the publish HTTP APIs that their
non-namespaced counterparts are.
""".
t_namespaced_api_publisher(TCConfig) when is_list(TCConfig) ->
    GlobalAdminHeader = create_superuser(),
    APIKeyName = atom_to_binary(?FUNCTION_NAME),
    Namespace = <<"ns1">>,
    APIPublisherRole = <<"ns:ns1::", ?ROLE_API_PUBLISHER/binary>>,
    ExpiresAt = to_rfc3339(erlang:system_time(second) + 1_000),
    Res1 = create_api_key_api(
        #{
            <<"name">> => APIKeyName,
            <<"expired_at">> => ExpiresAt,
            <<"desc">> => <<"namespaced api publisher">>,
            <<"enable">> => true,
            <<"role">> => APIPublisherRole
        },
        GlobalAdminHeader
    ),
    ?assertMatch(
        {200, #{
            <<"role">> := ?ROLE_API_PUBLISHER,
            <<"namespace">> := Namespace
        }},
        Res1
    ),
    {200, #{<<"api_key">> := Key, <<"api_secret">> := Secret}} = Res1,
    {_, BasicAuth} = emqx_common_test_http:auth_header(Key, Secret),

    AllHandlers = [_ | _] = all_handlers(),
    FakeReq = #{headers => #{<<"authorization">> => bin(BasicAuth)}},
    Failures =
        lists:filtermap(
            fun(FakeHandlerInfo) ->
                case emqx_dashboard:authorize(FakeReq, FakeHandlerInfo) of
                    {403, _, _} ->
                        %% Currently, namespaced API publishers can't do anything.
                        false;
                    {401, _, _} ->
                        %% Although a weird status code, it's current state of affairs...
                        %% Some endpoints can't be called by API keys and return 401.
                        %% See `emqx_mgmt_auth:authorize/2`.
                        false;
                    Unexpected ->
                        {true, {FakeHandlerInfo, Unexpected}}
                end
            end,
            AllHandlers
        ),
    maybe
        [_ | _] ?= Failures,
        ct:fail({should_have_been_forbidden, Failures})
    end,
    ok.
