%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_api_keys_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").

-if(?EMQX_RELEASE_EDITION == ee).
%% The `role' request field only exists in the enterprise edition, so cases that
%% exercise an explicitly requested role are kept in this edition only.
-define(EE_CASES, [
    t_ee_create,
    t_ee_update,
    t_update_partial_without_role,
    t_ee_authorize_viewer,
    t_ee_authorize_admin,
    t_ee_authorize_publisher
]).
-else.
%% In this edition `role' is neither accepted nor reported; note that `PUT' still
%% reaches the update path here, covered by `t_update' and `t_authorize'.
-define(EE_CASES, []).
-endif.

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
        {parallel, [parallel], [
            t_create,
            t_update,
            t_update_legacy_undefined_expiry,
            t_delete,
            t_authorize,
            t_create_unexpired_app
        ]},
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
    ?assertEqual(ok, auth_authorize(TestPath, <<"test-1">>, <<"secret-1">>)),
    ?assertEqual(ok, auth_authorize(TestPath, <<"test-2">>, <<"secret-2">>)),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-2">>, <<"secret-1">>)),

    %% relaunch to check if the table is changed.
    Bin1 = <<"test-1:new-secret-1\ntest-2:new-secret-2">>,
    ok = file:write_file(File, Bin1),
    update_file(File),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-1">>, <<"secret-1">>)),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-2">>, <<"secret-2">>)),
    ?assertEqual(ok, auth_authorize(TestPath, <<"test-1">>, <<"new-secret-1">>)),
    ?assertEqual(ok, auth_authorize(TestPath, <<"test-2">>, <<"new-secret-2">>)),

    %% not error when bootstrap_file is empty
    update_file(<<>>),
    update_file("./bootstrap_apps_not_exist.txt"),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-1">>, <<"secret-1">>)),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-2">>, <<"secret-2">>)),
    ?assertEqual(ok, auth_authorize(TestPath, <<"test-1">>, <<"new-secret-1">>)),
    ?assertEqual(ok, auth_authorize(TestPath, <<"test-2">>, <<"new-secret-2">>)),

    %% bad format
    BadBin = <<"test-1:secret-11\ntest-2 secret-12">>,
    ok = file:write_file(File, BadBin),
    update_file(File),
    ?assertMatch({error, #{reason := "invalid_format"}}, emqx_mgmt_auth:init_bootstrap_file(File)),
    ?assertEqual(ok, auth_authorize(TestPath, <<"test-1">>, <<"secret-11">>)),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-2">>, <<"secret-12">>)),
    update_file(<<>>),

    %% skip the empty line
    Bin2 = <<"test-3:new-secret-1\n\n\n   \ntest-4:new-secret-2">>,
    ok = file:write_file(File, Bin2),
    update_file(File),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-3">>, <<"secret-1">>)),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-4">>, <<"secret-2">>)),
    ?assertEqual(ok, auth_authorize(TestPath, <<"test-3">>, <<"new-secret-1">>)),
    ?assertEqual(ok, auth_authorize(TestPath, <<"test-4">>, <<"new-secret-2">>)),
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

-if(?EMQX_RELEASE_EDITION == ee).
t_bootstrap_file_with_role(_) ->
    Search = fun(Name) ->
        lists:search(
            fun(#{api_key := AppName}) ->
                AppName =:= Name
            end,
            emqx_mgmt_auth:list()
        )
    end,

    Bin = <<"role-1:role-1:viewer\nrole-2:role-2:administrator\nrole-3:role-3">>,
    File = "./bootstrap_api_keys.txt",
    ok = file:write_file(File, Bin),
    update_file(File),

    ?assertMatch(
        {value, #{api_key := <<"role-1">>, role := <<"viewer">>}},
        Search(<<"role-1">>)
    ),

    ?assertMatch(
        {value, #{api_key := <<"role-2">>, role := <<"administrator">>}},
        Search(<<"role-2">>)
    ),

    ?assertMatch(
        {value, #{api_key := <<"role-3">>, role := <<"administrator">>}},
        Search(<<"role-3">>)
    ),

    %% bad role
    BadBin = <<"role-4:secret-11:bad\n">>,
    ok = file:write_file(File, BadBin),
    update_file(File),
    ?assertEqual(
        false,
        Search(<<"role-4">>)
    ),
    ok.
-else.
t_bootstrap_file_with_role(_) ->
    Search = fun(Name) ->
        lists:search(
            fun(#{api_key := AppName}) ->
                AppName =:= Name
            end,
            emqx_mgmt_auth:list()
        )
    end,

    Bin = <<"role-1:role-1:administrator\nrole-2:role-2">>,
    File = "./bootstrap_api_keys.txt",
    ok = file:write_file(File, Bin),
    update_file(File),

    ?assertMatch(
        {value, #{api_key := <<"role-1">>, role := <<"administrator">>}},
        Search(<<"role-1">>)
    ),

    ?assertMatch(
        {value, #{api_key := <<"role-2">>, role := <<"administrator">>}},
        Search(<<"role-2">>)
    ),

    %% only administrator
    OtherRoleBin = <<"role-3:role-3:viewer\n">>,
    ok = file:write_file(File, OtherRoleBin),
    update_file(File),
    ?assertEqual(
        false,
        Search(<<"role-3">>)
    ),

    %% bad role
    BadBin = <<"role-4:secret-11:bad\n">>,
    ok = file:write_file(File, BadBin),
    update_file(File),
    ?assertEqual(
        false,
        Search(<<"role-4">>)
    ),
    ok.
-endif.

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
    %% omitting `expired_at' keeps the stored expiry, it must not make the key immortal
    ?assertEqual(
        calendar:rfc3339_to_system_time(binary_to_list(ExpiredAt)),
        calendar:rfc3339_to_system_time(binary_to_list(maps:get(<<"expired_at">>, Update2)))
    ),
    Unexpired2 = Change#{expired_at => <<"infinity">>},
    {ok, Update3} = update_app(Name, Unexpired2),
    ?assertEqual(<<"infinity">>, maps:get(<<"expired_at">>, Update3)),

    ?assertEqual({error, {"HTTP/1.1", 404, "Not Found"}}, update_app(<<"Not-Exist">>, Change)),
    ok.

%% Releases before 5.0.0 stored `expired_at = undefined' to mean "never expires", and
%% such records survive an upgrade (and a backup restore) unchanged. They must not take
%% the API down: both reading and a partial update have to keep working.
t_update_legacy_undefined_expiry(_Config) ->
    Name = <<"EMQX-API-LEGACY-EXPIRY-KEY">>,
    {ok, _} = create_app(Name),
    ok = set_stored_expired_at(Name, undefined),

    {ok, #{<<"expired_at">> := <<"infinity">>}} = read_app(Name),
    {ok, #{<<"expired_at">> := <<"infinity">>}} = update_app(Name, #{enable => false}),
    {ok, #{<<"expired_at">> := <<"infinity">>}} = read_app(Name),
    ok.

%% Overwrite the stored expiry directly to emulate a record written by an old release.
%% Only the `expired_at' slot is touched, so the stored `extra' payload survives as-is.
set_stored_expired_at(Name, ExpiredAt) ->
    {ok, ok} = emqx_mgmt_auth:trans(
        fun() ->
            [App0] = mnesia:read(?APP, Name, write),
            mnesia:write(App0#?APP{expired_at = ExpiredAt})
        end,
        []
    ),
    ok.

-if(?EMQX_RELEASE_EDITION == ee).

%% A partial update must only touch the fields it carries: omitting `role' keeps the
%% stored role instead of escalating the key to the default (administrator) role, while
%% an explicitly supplied role must still be able to change it.
t_update_partial_without_role(_Config) ->
    Name = <<"EMQX-API-PARTIAL-UPDATE-KEY">>,
    %% an expiry given at creation time must be honoured, not silently dropped.
    %% `create_app/2' overwrites `expired_at', so use the pass-through helper here.
    SentExpiry = to_rfc3339(erlang:system_time(second) + 30000),
    {ok, Created} = create_unexpired_app(Name, #{
        role => ?ROLE_API_VIEWER, expired_at => SentExpiry
    }),
    {ok, #{<<"role">> := Role0, <<"expired_at">> := ExpiredAt0}} = read_app(Name),
    ?assertEqual(?ROLE_API_VIEWER, Role0),
    ?assertEqual(
        calendar:rfc3339_to_system_time(binary_to_list(SentExpiry)),
        calendar:rfc3339_to_system_time(binary_to_list(ExpiredAt0))
    ),
    ?assertEqual(maps:get(<<"expired_at">>, Created), ExpiredAt0),

    Partial = #{enable => false},
    {ok, Update1} = update_app(Name, Partial),
    ?assertEqual(Role0, maps:get(<<"role">>, Update1)),
    ?assertEqual(false, maps:get(<<"enable">>, Update1)),
    %% the response must not merely echo the old values, the stored ones must be intact
    ?assertEqual(ExpiredAt0, maps:get(<<"expired_at">>, Update1)),
    {ok, Update1Read} = read_app(Name),
    ?assertEqual(Role0, maps:get(<<"role">>, Update1Read)),
    ?assertEqual(ExpiredAt0, maps:get(<<"expired_at">>, Update1Read)),

    {ok, Update2} = update_app(Name, Partial#{desc => <<"NoteVersion2"/utf8>>}),
    ?assertEqual(Role0, maps:get(<<"role">>, Update2)),
    ?assertEqual(<<"NoteVersion2"/utf8>>, maps:get(<<"desc">>, Update2)),
    ?assertEqual(ExpiredAt0, maps:get(<<"expired_at">>, Update2)),

    %% `expired_at' is absent from `Partial', so the key must not be made immortal by
    %% this update; asking for a new expiry explicitly must still work.
    {ok, Update3} = update_app(Name, Partial#{expired_at => <<"infinity">>}),
    ?assertEqual(<<"infinity">>, maps:get(<<"expired_at">>, Update3)),
    NewExpiry = to_rfc3339(erlang:system_time(second) + 20000),
    {ok, Update4} = update_app(Name, #{expired_at => NewExpiry}),
    ?assertEqual(
        calendar:rfc3339_to_system_time(binary_to_list(NewExpiry)),
        calendar:rfc3339_to_system_time(binary_to_list(maps:get(<<"expired_at">>, Update4)))
    ),

    %% omitting `role' must not escalate, but asking for one explicitly must still work
    {ok, Update5} = update_app(Name, #{role => ?ROLE_API_SUPERUSER}),
    ?assertEqual(?ROLE_API_SUPERUSER, maps:get(<<"role">>, Update5)),
    {ok, #{<<"role">> := ?ROLE_API_SUPERUSER}} = read_app(Name),

    %% an unknown role is still rejected by the schema validator ...
    ?assertMatch({error, {"HTTP/1.1", 400, _}}, update_app(Name, #{role => <<"bad_role">>})),
    {ok, #{<<"role">> := ?ROLE_API_SUPERUSER}} = read_app(Name),

    %% ... and by `emqx_mgmt_auth:update/5' itself, which is the clause this change
    %% touches; also check the "no role asked for" case at the storage layer.
    ?assertEqual(
        {error, <<"Role does not exist">>},
        emqx_mgmt_auth:update(Name, true, infinity, undefined, <<"bad_role">>)
    ),
    {ok, ViaApi} = emqx_mgmt_auth:update(Name, undefined, undefined, undefined, undefined),
    ?assertEqual(?ROLE_API_SUPERUSER, maps:get(role, ViaApi)),
    ?assertEqual(<<"NoteVersion2"/utf8>>, maps:get(desc, ViaApi)),

    %% a key that does not exist is still reported as missing
    ?assertEqual({error, {"HTTP/1.1", 404, "Not Found"}}, update_app(<<"Not-Exist">>, Partial)),
    ok.

-endif.

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
        {ok, Apps} -> {ok, emqx_utils_json:decode(Apps, [return_maps])};
        Error -> Error
    end.

read_app(Name) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["api_key", Name]),
    case emqx_mgmt_api_test_util:request_api(get, Path, AuthHeader) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res, [return_maps])};
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
        {ok, Res} -> {ok, emqx_utils_json:decode(Res, [return_maps])};
        Error -> Error
    end.

create_unexpired_app(Name, Params) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["api_key"]),
    App = maps:merge(#{name => Name, desc => <<"Note"/utf8>>, enable => true}, Params),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, App) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res, [return_maps])};
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
        {ok, Update} -> {ok, emqx_utils_json:decode(Update, [return_maps])};
        Error -> Error
    end.

to_rfc3339(Sec) ->
    list_to_binary(calendar:system_time_to_rfc3339(Sec)).
