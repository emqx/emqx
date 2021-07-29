%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_apps_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    ekka_mnesia:start(),
    emqx_mgmt_auth:mnesia(boot),
    emqx_ct_helpers:start_apps([emqx_management], fun set_special_configs/1),
    Config.

end_per_suite(_) ->
    emqx_ct_helpers:stop_apps([emqx_management]).

set_special_configs(emqx_management) ->
    emqx_config:put([emqx_management], #{listeners => [#{protocol => http, port => 8081}],
        applications =>[#{id => "admin", secret => "public"}]}),
    ok;
set_special_configs(_App) ->
    ok.

t_list_app(_) ->
    Path = emqx_mgmt_api_test_util:api_path(["apps"]),
    {ok, Body} = emqx_mgmt_api_test_util:request_api(get, Path),
    Data = emqx_json:decode(Body, [return_maps]),
    AdminApp = hd(Data),
    Admin = maps:get(<<"app_id">>, AdminApp),
    ?assertEqual(<<"admin">>, Admin).

t_get_app(_) ->
    Path = emqx_mgmt_api_test_util:api_path(["apps/admin"]),
    {ok, Body} = emqx_mgmt_api_test_util:request_api(get, Path),
    AdminApp = emqx_json:decode(Body, [return_maps]),
    ?assertEqual(<<"admin">>, maps:get(<<"app_id">>, AdminApp)),
    ?assertEqual(<<"public">>, maps:get(<<"secret">>, AdminApp)).

t_add_app(_) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    AppId = <<"test_app_id">>,
    TestAppPath = emqx_mgmt_api_test_util:api_path(["apps", AppId]),
    AppSecret = <<"test_app_secret">>,

    %% new test app
    Path = emqx_mgmt_api_test_util:api_path(["apps"]),
    RequestBody = #{
        app_id => AppId,
        secret => AppSecret,
        desc => <<"test desc">>,
        name => <<"test_app_name">>,
        expired => erlang:system_time(second) + 3000,
        status => true
    },
    {ok, Body} = emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, RequestBody),
    TestAppSecret = emqx_json:decode(Body, [return_maps]),
    ?assertEqual(AppSecret, maps:get(<<"secret">>, TestAppSecret)),

    %% get new test app
    {ok, GetApp} = emqx_mgmt_api_test_util:request_api(get, TestAppPath),
    TestApp = emqx_json:decode(GetApp, [return_maps]),
    ?assertEqual(AppId, maps:get(<<"app_id">>, TestApp)),
    ?assertEqual(AppSecret, maps:get(<<"secret">>, TestApp)),

    %% update app
    Desc2 = <<"test desc 2">>,
    Name2 = <<"test_app_name_2">>,
    PutBody = #{
        desc => Desc2,
        name => Name2,
        expired => erlang:system_time(second) + 3000,
        status => false
    },
    {ok, PutApp} = emqx_mgmt_api_test_util:request_api(put, TestAppPath, "", AuthHeader, PutBody),
    TestApp1 = emqx_json:decode(PutApp, [return_maps]),
    ?assertEqual(Desc2, maps:get(<<"desc">>, TestApp1)),
    ?assertEqual(Name2, maps:get(<<"name">>, TestApp1)),
    ?assertEqual(false, maps:get(<<"status">>, TestApp1)),

    %% after update
    {ok, GetApp2} = emqx_mgmt_api_test_util:request_api(get, TestAppPath),
    TestApp2 = emqx_json:decode(GetApp2, [return_maps]),
    ?assertEqual(Desc2, maps:get(<<"desc">>, TestApp2)),
    ?assertEqual(Name2, maps:get(<<"name">>, TestApp2)),
    ?assertEqual(false, maps:get(<<"status">>, TestApp2)),

    %% delete new app
    {ok, _} = emqx_mgmt_api_test_util:request_api(delete, TestAppPath),

    %% after delete
    ?assertEqual({error,{"HTTP/1.1",404,"Not Found"}},
        emqx_mgmt_api_test_util:request_api(get, TestAppPath)).
