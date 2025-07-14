%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_api_keys_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-define(EE_CASES, [
    t_ee_create,
    t_ee_update,
    t_ee_authorize_viewer,
    t_ee_authorize_admin,
    t_ee_authorize_publisher
]).

-define(APP, emqx_app).

-record(?APP, {
    name = <<>> :: binary() | '_',
    api_key = <<>> :: binary() | '_',
    api_secret_hash = <<>> :: binary() | '_',
    enable = true :: boolean() | '_',
    desc = <<>> :: binary() | '_',
    expired_at = 0 :: integer() | undefined | infinity | '_',
    created_at = 0 :: integer() | '_'
}).

all() -> [{group, parallel}, {group, sequence}].
suite() -> [{timetrap, {minutes, 1}}].
groups() ->
    [
        {parallel, [parallel], [t_create, t_update, t_delete, t_authorize, t_create_unexpired_app]},
        {parallel, [parallel], ?EE_CASES},
        {sequence, [], [t_bootstrap_file, t_bootstrap_file_with_role, t_create_failed]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(hackney),
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)),
    application:stop(hackney).

t_bootstrap_file(_) ->
    TestPath = <<"/api/v5/status">>,
    Bin = <<"test-1:secret-1\ntest-2:secret-2">>,
    File = "./bootstrap_api_keys.txt",
    ok = file:write_file(File, Bin),
    update_file(File),
    ?assertMatch({ok, _}, auth_authorize(TestPath, <<"test-1">>, <<"secret-1">>)),
    ?assertMatch({ok, _}, auth_authorize(TestPath, <<"test-2">>, <<"secret-2">>)),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-2">>, <<"secret-1">>)),

    %% relaunch to check if the table is changed.
    Bin1 = <<"test-1:new-secret-1\ntest-2:new-secret-2">>,
    ok = file:write_file(File, Bin1),
    update_file(File),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-1">>, <<"secret-1">>)),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-2">>, <<"secret-2">>)),
    ?assertMatch({ok, _}, auth_authorize(TestPath, <<"test-1">>, <<"new-secret-1">>)),
    ?assertMatch({ok, _}, auth_authorize(TestPath, <<"test-2">>, <<"new-secret-2">>)),

    %% not error when bootstrap_file is empty
    update_file(<<>>),
    update_file("./bootstrap_apps_not_exist.txt"),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-1">>, <<"secret-1">>)),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-2">>, <<"secret-2">>)),
    ?assertMatch({ok, _}, auth_authorize(TestPath, <<"test-1">>, <<"new-secret-1">>)),
    ?assertMatch({ok, _}, auth_authorize(TestPath, <<"test-2">>, <<"new-secret-2">>)),

    %% bad format
    BadBin = <<"test-1:secret-11\ntest-2 secret-12">>,
    ok = file:write_file(File, BadBin),
    update_file(File),
    ?assertMatch({error, #{reason := "invalid_format"}}, emqx_mgmt_auth:init_bootstrap_file(File)),
    ?assertMatch({ok, _}, auth_authorize(TestPath, <<"test-1">>, <<"secret-11">>)),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-2">>, <<"secret-12">>)),
    update_file(<<>>),

    %% skip the empty line
    Bin2 = <<"test-3:new-secret-1\n\n\n   \ntest-4:new-secret-2">>,
    ok = file:write_file(File, Bin2),
    update_file(File),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-3">>, <<"secret-1">>)),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-4">>, <<"secret-2">>)),
    ?assertMatch({ok, _}, auth_authorize(TestPath, <<"test-3">>, <<"new-secret-1">>)),
    ?assertMatch({ok, _}, auth_authorize(TestPath, <<"test-4">>, <<"new-secret-2">>)),
    ok.

t_bootstrap_file_override(_) ->
    TestPath = <<"/api/v5/status">>,
    Bin =
        <<"test-1:secret-1\ntest-1:duplicated-secret-1\ntest-2:secret-2\ntest-2:duplicated-secret-2">>,
    File = "./bootstrap_api_keys.txt",
    ok = file:write_file(File, Bin),
    update_file(File),

    ?assertEqual(ok, emqx_mgmt_auth:init_bootstrap_file(File)),

    MatchFun = fun(ApiKey) -> mnesia:match_object(#?APP{api_key = ApiKey, _ = '_'}) end,
    ?assertMatch(
        {ok, [
            #?APP{
                name = <<"from_bootstrap_file_18926f94712af04e">>,
                api_key = <<"test-1">>
            }
        ]},
        emqx_mgmt_auth:trans(MatchFun, [<<"test-1">>])
    ),
    ?assertEqual(ok, emqx_mgmt_auth:authorize(TestPath, <<"test-1">>, <<"duplicated-secret-1">>)),

    ?assertMatch(
        {ok, [
            #?APP{
                name = <<"from_bootstrap_file_de1c28a2e610e734">>,
                api_key = <<"test-2">>
            }
        ]},
        emqx_mgmt_auth:trans(MatchFun, [<<"test-2">>])
    ),
    ?assertEqual(ok, emqx_mgmt_auth:authorize(TestPath, <<"test-2">>, <<"duplicated-secret-2">>)),
    ok.

t_bootstrap_file_dup_override(_) ->
    TestPath = <<"/api/v5/status">>,
    TestApiKey = <<"test-1">>,
    Bin = <<"test-1:secret-1">>,
    File = "./bootstrap_api_keys.txt",
    ok = file:write_file(File, Bin),
    update_file(File),
    ?assertEqual(ok, emqx_mgmt_auth:init_bootstrap_file(File)),

    SameAppWithDiffName = #?APP{
        name = <<"name-1">>,
        api_key = <<"test-1">>,
        api_secret_hash = emqx_dashboard_admin:hash(<<"duplicated-secret-1">>),
        enable = true,
        desc = <<"dup api key">>,
        created_at = erlang:system_time(second),
        expired_at = infinity
    },
    WriteFun = fun(App) -> mnesia:write(App) end,
    MatchFun = fun(ApiKey) -> mnesia:match_object(#?APP{api_key = ApiKey, _ = '_'}) end,

    ?assertEqual({ok, ok}, emqx_mgmt_auth:trans(WriteFun, [SameAppWithDiffName])),
    %% as erlang term order
    ?assertMatch(
        {ok, [
            #?APP{
                name = <<"name-1">>,
                api_key = <<"test-1">>
            },
            #?APP{
                name = <<"from_bootstrap_file_18926f94712af04e">>,
                api_key = <<"test-1">>
            }
        ]},
        emqx_mgmt_auth:trans(MatchFun, [TestApiKey])
    ),

    update_file(File),

    %% Similar to loading bootstrap file at node startup
    %% the duplicated apikey in mnesia will be cleaned up
    ?assertEqual(ok, emqx_mgmt_auth:init_bootstrap_file(File)),
    ?assertMatch(
        {ok, [
            #?APP{
                name = <<"from_bootstrap_file_18926f94712af04e">>,
                api_key = <<"test-1">>
            }
        ]},
        emqx_mgmt_auth:trans(MatchFun, [<<"test-1">>])
    ),

    %% the last apikey in bootstrap file will override the all in mnesia and the previous one(s) in bootstrap file
    ?assertEqual(ok, emqx_mgmt_auth:authorize(TestPath, <<"test-1">>, <<"secret-1">>)),

    ok.

t_bootstrap_file_with_role(_) ->
    Search = fun(Name) ->
        lists:search(
            fun(#{api_key := AppName}) ->
                AppName =:= Name
            end,
            emqx_mgmt_auth:list()
        )
    end,

    Bin = iolist_to_binary(
        lists:join($\n, [
            <<"role-1:role-1:viewer">>,
            <<"role-2:role-2:administrator">>,
            <<"role-3:role-3">>,
            <<"role-4:role-4:ns:ns1::administrator">>,
            <<"role-5:role-5:ns:ns1::viewer">>,
            <<"role-6:role-6:ns:ns1::blobber">>
        ])
    ),
    File = "./bootstrap_api_keys.txt",
    ok = file:write_file(File, Bin),
    update_file(File),

    ?assertMatch(
        {value, #{api_key := <<"role-1">>, role := <<"viewer">>, namespace := ?global_ns}},
        Search(<<"role-1">>)
    ),
    ?assertMatch(
        {value, #{api_key := <<"role-2">>, role := <<"administrator">>, namespace := ?global_ns}},
        Search(<<"role-2">>)
    ),
    ?assertMatch(
        {value, #{api_key := <<"role-3">>, role := <<"administrator">>, namespace := ?global_ns}},
        Search(<<"role-3">>)
    ),
    ?assertMatch(
        {value, #{api_key := <<"role-4">>, role := <<"administrator">>, namespace := <<"ns1">>}},
        Search(<<"role-4">>)
    ),
    ?assertMatch(
        {value, #{api_key := <<"role-5">>, role := <<"viewer">>, namespace := <<"ns1">>}},
        Search(<<"role-5">>)
    ),
    ?assertMatch(false, Search(<<"role-6">>)),

    %% bad role
    BadBin = <<"role-7:secret-11:bad\n">>,
    ok = file:write_file(File, BadBin),
    update_file(File),
    ?assertEqual(
        false,
        Search(<<"role-7">>)
    ),
    ok.

auth_authorize(Path, Key, Secret) ->
    FakePath = erlang:list_to_binary(emqx_dashboard_swagger:relative_uri("/fake")),
    FakeReq = #{method => <<"GET">>, path => FakePath},
    emqx_mgmt_auth:authorize(Path, FakeReq, Key, Secret).

update_file(File) ->
    ?assertMatch({ok, _}, emqx:update_config([<<"api_key">>], #{<<"bootstrap_file">> => File})).

t_create(_Config) ->
    Name = <<"EMQX-API-KEY-1">>,
    {ok, Create} = create_app(Name),
    ?assertMatch(
        #{
            <<"api_key">> := _,
            <<"api_secret">> := _,
            <<"created_at">> := _,
            <<"desc">> := _,
            <<"enable">> := true,
            <<"expired_at">> := _,
            <<"name">> := Name
        },
        Create
    ),
    {ok, List} = list_app(),
    [App] = lists:filter(fun(#{<<"name">> := NameA}) -> NameA =:= Name end, List),
    ?assertEqual(false, maps:is_key(<<"api_secret">>, App)),
    {ok, App1} = read_app(Name),
    ?assertEqual(Name, maps:get(<<"name">>, App1)),
    ?assertEqual(true, maps:get(<<"enable">>, App1)),
    ?assertEqual(false, maps:is_key(<<"api_secret">>, App1)),
    ?assertEqual({error, {"HTTP/1.1", 404, "Not Found"}}, read_app(<<"EMQX-API-KEY-NO-EXIST">>)),
    ok.

t_create_failed(_Config) ->
    BadRequest = {error, {"HTTP/1.1", 400, "Bad Request"}},

    ?assertEqual(BadRequest, create_app(<<" error format name">>)),
    LongName = iolist_to_binary(lists:duplicate(257, "A")),
    ?assertEqual(BadRequest, create_app(<<" error format name">>)),
    ?assertEqual(BadRequest, create_app(LongName)),

    {ok, List} = list_app(),
    CreateNum = 100 - erlang:length(List),
    Names = lists:map(
        fun(Seq) ->
            <<"EMQX-API-FAILED-KEY-", (integer_to_binary(Seq))/binary>>
        end,
        lists:seq(1, CreateNum)
    ),
    lists:foreach(fun(N) -> {ok, _} = create_app(N) end, Names),
    ?assertEqual(BadRequest, create_app(<<"EMQX-API-KEY-MAXIMUM">>)),

    lists:foreach(fun(N) -> {ok, _} = delete_app(N) end, Names),
    Name = <<"EMQX-API-FAILED-KEY-1">>,
    ?assertMatch({ok, _}, create_app(Name)),
    ?assertEqual(BadRequest, create_app(Name)),
    {ok, _} = delete_app(Name),
    ?assertMatch({ok, #{<<"name">> := Name}}, create_app(Name)),
    {ok, _} = delete_app(Name),
    ok.

t_update(_Config) ->
    Name = <<"EMQX-API-UPDATE-KEY">>,
    {ok, _} = create_app(Name),

    ExpiredAt = to_rfc3339(erlang:system_time(second) + 10000),
    Change = #{
        expired_at => ExpiredAt,
        desc => <<"NoteVersion1"/utf8>>,
        enable => false
    },
    {ok, Update1} = update_app(Name, Change),
    ?assertEqual(Name, maps:get(<<"name">>, Update1)),
    ?assertEqual(false, maps:get(<<"enable">>, Update1)),
    ?assertEqual(<<"NoteVersion1"/utf8>>, maps:get(<<"desc">>, Update1)),
    ?assertEqual(
        calendar:rfc3339_to_system_time(binary_to_list(ExpiredAt)),
        calendar:rfc3339_to_system_time(binary_to_list(maps:get(<<"expired_at">>, Update1)))
    ),
    Unexpired1 = maps:without([expired_at], Change),
    {ok, Update2} = update_app(Name, Unexpired1),
    ?assertEqual(<<"infinity">>, maps:get(<<"expired_at">>, Update2)),
    Unexpired2 = Change#{expired_at => <<"infinity">>},
    {ok, Update3} = update_app(Name, Unexpired2),
    ?assertEqual(<<"infinity">>, maps:get(<<"expired_at">>, Update3)),

    ?assertEqual({error, {"HTTP/1.1", 404, "Not Found"}}, update_app(<<"Not-Exist">>, Change)),
    ok.

t_delete(_Config) ->
    Name = <<"EMQX-API-DELETE-KEY">>,
    {ok, _Create} = create_app(Name),
    {ok, Delete} = delete_app(Name),
    ?assertEqual([], Delete),
    ?assertEqual({error, {"HTTP/1.1", 404, "Not Found"}}, delete_app(Name)),
    ok.

t_authorize(_Config) ->
    Name = <<"EMQX-API-AUTHORIZE-KEY">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} = create_app(Name),
    BasicHeader = emqx_common_test_http:auth_header(
        binary_to_list(ApiKey),
        binary_to_list(ApiSecret)
    ),
    SecretError = emqx_common_test_http:auth_header(
        binary_to_list(ApiKey),
        binary_to_list(ApiKey)
    ),
    KeyError = emqx_common_test_http:auth_header("not_found_key", binary_to_list(ApiSecret)),
    Unauthorized = {error, {"HTTP/1.1", 401, "Unauthorized"}},

    BanPath = emqx_mgmt_api_test_util:api_path(["banned"]),
    ApiKeyPath = emqx_mgmt_api_test_util:api_path(["api_key"]),
    UserPath = emqx_mgmt_api_test_util:api_path(["users"]),
    DeleteUserPath1 = emqx_mgmt_api_test_util:api_path(["users", "some_user"]),
    DeleteUserPath2 = [emqx_mgmt_api_test_util:api_path([""]), "./users/some_user"],

    {ok, _Status} = emqx_mgmt_api_test_util:request_api(get, BanPath, BasicHeader),
    ?assertEqual(Unauthorized, emqx_mgmt_api_test_util:request_api(get, BanPath, KeyError)),
    ?assertEqual(Unauthorized, emqx_mgmt_api_test_util:request_api(get, BanPath, SecretError)),
    ?assertEqual(Unauthorized, emqx_mgmt_api_test_util:request_api(get, UserPath, BasicHeader)),

    ?assertEqual(
        Unauthorized, emqx_mgmt_api_test_util:request_api(delete, DeleteUserPath1, BasicHeader)
    ),
    %% We make request with hackney to avoid path normalization made by httpc.
    {ok, Code, _Headers0, _Body0} = hackney:request(delete, DeleteUserPath2, [BasicHeader], <<>>),
    ?assertEqual(401, Code),

    {error, {{"HTTP/1.1", 401, "Unauthorized"}, _Headers1, Body1}} =
        emqx_mgmt_api_test_util:request_api(
            get,
            ApiKeyPath,
            [],
            BasicHeader,
            [],
            #{return_all => true}
        ),
    ?assertMatch(
        #{
            <<"code">> := <<"API_KEY_NOT_ALLOW">>,
            <<"message">> := _
        },
        emqx_utils_json:decode(Body1, [return_maps])
    ),

    ?assertMatch(
        {ok, #{<<"api_key">> := _, <<"enable">> := false}},
        update_app(Name, #{enable => false})
    ),
    ?assertEqual(Unauthorized, emqx_mgmt_api_test_util:request_api(get, BanPath, BasicHeader)),

    Expired = #{
        expired_at => to_rfc3339(erlang:system_time(second) - 1),
        enable => true
    },
    ?assertMatch({ok, #{<<"api_key">> := _, <<"enable">> := true}}, update_app(Name, Expired)),
    ?assertEqual(Unauthorized, emqx_mgmt_api_test_util:request_api(get, BanPath, BasicHeader)),
    UnExpired = #{expired_at => infinity},
    ?assertMatch(
        {ok, #{<<"api_key">> := _, <<"expired_at">> := <<"infinity">>}},
        update_app(Name, UnExpired)
    ),
    {ok, _Status1} = emqx_mgmt_api_test_util:request_api(get, BanPath, BasicHeader),
    ok.

t_create_unexpired_app(_Config) ->
    Name1 = <<"EMQX-UNEXPIRED-API-KEY-1">>,
    Name2 = <<"EMQX-UNEXPIRED-API-KEY-2">>,
    {ok, Create1} = create_unexpired_app(Name1, #{}),
    ?assertMatch(#{<<"expired_at">> := <<"infinity">>}, Create1),
    {ok, Create2} = create_unexpired_app(Name2, #{expired_at => <<"infinity">>}),
    ?assertMatch(#{<<"expired_at">> := <<"infinity">>}, Create2),
    ok.

t_ee_create(_Config) ->
    Name = <<"EMQX-EE-API-KEY-1">>,
    {ok, Create} = create_app(Name, #{role => ?ROLE_API_VIEWER}),
    ?assertMatch(
        #{
            <<"api_key">> := _,
            <<"api_secret">> := _,
            <<"created_at">> := _,
            <<"desc">> := _,
            <<"enable">> := true,
            <<"expired_at">> := _,
            <<"name">> := Name,
            <<"role">> := ?ROLE_API_VIEWER
        },
        Create
    ),

    {ok, App} = read_app(Name),
    ?assertMatch(#{<<"name">> := Name, <<"role">> := ?ROLE_API_VIEWER}, App).

t_ee_update(_Config) ->
    Name = <<"EMQX-EE-API-UPDATE-KEY">>,
    {ok, _} = create_app(Name, #{role => ?ROLE_API_VIEWER}),

    Change = #{
        desc => <<"NoteVersion1"/utf8>>,
        enable => false,
        role => ?ROLE_API_SUPERUSER
    },
    {ok, Update1} = update_app(Name, Change),
    ?assertEqual(?ROLE_API_SUPERUSER, maps:get(<<"role">>, Update1)),

    {ok, App} = read_app(Name),
    ?assertMatch(#{<<"name">> := Name, <<"role">> := ?ROLE_API_SUPERUSER}, App).

t_ee_authorize_viewer(_Config) ->
    Name = <<"EMQX-EE-API-AUTHORIZE-KEY-VIEWER">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} = create_app(Name, #{
        role => ?ROLE_API_VIEWER
    }),
    BasicHeader = emqx_common_test_http:auth_header(
        binary_to_list(ApiKey),
        binary_to_list(ApiSecret)
    ),

    BanPath = emqx_mgmt_api_test_util:api_path(["banned"]),
    ?assertMatch({ok, _}, emqx_mgmt_api_test_util:request_api(get, BanPath, BasicHeader)),
    ?assertMatch(
        {error, {_, 403, _}}, emqx_mgmt_api_test_util:request_api(delete, BanPath, BasicHeader)
    ).

t_ee_authorize_admin(_Config) ->
    Name = <<"EMQX-EE-API-AUTHORIZE-KEY-ADMIN">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} = create_app(Name, #{
        role => ?ROLE_API_SUPERUSER
    }),
    BasicHeader = emqx_common_test_http:auth_header(
        binary_to_list(ApiKey),
        binary_to_list(ApiSecret)
    ),

    BanPath = emqx_mgmt_api_test_util:api_path(["banned"]),
    ?assertMatch({ok, _}, emqx_mgmt_api_test_util:request_api(get, BanPath, BasicHeader)),
    ?assertMatch(
        {ok, _}, emqx_mgmt_api_test_util:request_api(delete, BanPath, BasicHeader)
    ).

t_ee_authorize_publisher(_Config) ->
    Name = <<"EMQX-EE-API-AUTHORIZE-KEY-PUBLISHER">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} = create_app(Name, #{
        role => ?ROLE_API_PUBLISHER
    }),
    BasicHeader = emqx_common_test_http:auth_header(
        binary_to_list(ApiKey),
        binary_to_list(ApiSecret)
    ),

    BanPath = emqx_mgmt_api_test_util:api_path(["banned"]),
    Publish = emqx_mgmt_api_test_util:api_path(["publish"]),
    ?assertMatch(
        {error, {_, 403, _}}, emqx_mgmt_api_test_util:request_api(get, BanPath, BasicHeader)
    ),
    ?assertMatch(
        {error, {_, 403, _}}, emqx_mgmt_api_test_util:request_api(delete, BanPath, BasicHeader)
    ),
    ?_assertMatch(
        {ok, _},
        emqx_mgmt_api_test_util:request_api(
            post,
            Publish,
            [],
            BasicHeader,
            #{topic => <<"t/t_ee_authorize_publisher">>, payload => <<"hello">>}
        )
    ).

list_app() ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["api_key"]),
    case emqx_mgmt_api_test_util:request_api(get, Path, AuthHeader) of
        {ok, Apps} -> {ok, emqx_utils_json:decode(Apps)};
        Error -> Error
    end.

read_app(Name) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["api_key", Name]),
    case emqx_mgmt_api_test_util:request_api(get, Path, AuthHeader) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res)};
        Error -> Error
    end.

create_app(Name) ->
    create_app(Name, #{}).

create_app(Name, Extra) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["api_key"]),
    ExpiredAt = to_rfc3339(erlang:system_time(second) + 1000),
    App = Extra#{
        name => Name,
        expired_at => ExpiredAt,
        desc => <<"Note"/utf8>>,
        enable => true
    },
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, App) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res)};
        Error -> Error
    end.

create_unexpired_app(Name, Params) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["api_key"]),
    App = maps:merge(#{name => Name, desc => <<"Note"/utf8>>, enable => true}, Params),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, App) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res)};
        Error -> Error
    end.

delete_app(Name) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    DeletePath = emqx_mgmt_api_test_util:api_path(["api_key", Name]),
    emqx_mgmt_api_test_util:request_api(delete, DeletePath, AuthHeader).

update_app(Name, Change) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    UpdatePath = emqx_mgmt_api_test_util:api_path(["api_key", Name]),
    case emqx_mgmt_api_test_util:request_api(put, UpdatePath, "", AuthHeader, Change) of
        {ok, Update} -> {ok, emqx_utils_json:decode(Update)};
        Error -> Error
    end.

to_rfc3339(Sec) ->
    list_to_binary(calendar:system_time_to_rfc3339(Sec)).
