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
-module(emqx_mgmt_clients_api_SUITE).
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(APP, emqx_management).

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

t_clients(_) ->
    process_flag(trap_exit, true),

    Username1 = <<"user1">>,
    ClientId1 = <<"client1">>,

    Username2 = <<"user2">>,
    ClientId2 = <<"client2">>,

    Topic = <<"topic_1">>,
    Qos = 0,

    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),

    {ok, C1} = emqtt:start_link(#{username => Username1, clientid => ClientId1}),
    {ok, _} = emqtt:connect(C1),
    {ok, C2} = emqtt:start_link(#{username => Username2, clientid => ClientId2}),
    {ok, _} = emqtt:connect(C2),

    timer:sleep(300),

    %% get /clients
    ClientsPath = emqx_mgmt_api_test_util:api_path(["clients"]),
    {ok, Clients} = emqx_mgmt_api_test_util:request_api(get, ClientsPath),
    ClientsResponse = emqx_json:decode(Clients, [return_maps]),
    ClientsMeta = maps:get(<<"meta">>, ClientsResponse),
    ClientsPage = maps:get(<<"page">>, ClientsMeta),
    ClientsLimit = maps:get(<<"limit">>, ClientsMeta),
    ClientsCount = maps:get(<<"count">>, ClientsMeta),
    ?assertEqual(ClientsPage, 1),
    ?assertEqual(ClientsLimit, emqx_mgmt:max_row_limit()),
    ?assertEqual(ClientsCount, 2),

    %% get /clients/:clientid
    Client1Path = emqx_mgmt_api_test_util:api_path(["clients", binary_to_list(ClientId1)]),
    {ok, Client1} = emqx_mgmt_api_test_util:request_api(get, Client1Path),
    Client1Response = emqx_json:decode(Client1, [return_maps]),
    ?assertEqual(Username1, maps:get(<<"username">>, Client1Response)),
    ?assertEqual(ClientId1, maps:get(<<"clientid">>, Client1Response)),

    %% delete /clients/:clientid kickout
    Client2Path = emqx_mgmt_api_test_util:api_path(["clients", binary_to_list(ClientId2)]),
    {ok, _} = emqx_mgmt_api_test_util:request_api(delete, Client2Path),
    AfterKickoutResponse = emqx_mgmt_api_test_util:request_api(get, Client2Path),
    ?assertEqual({error, {"HTTP/1.1", 404, "Not Found"}}, AfterKickoutResponse),

    %% get /clients/:clientid/acl_cache should has no acl cache
    Client1AclCachePath = emqx_mgmt_api_test_util:api_path(["clients", binary_to_list(ClientId1), "acl_cache"]),
    {ok, Client1AclCache} = emqx_mgmt_api_test_util:request_api(get, Client1AclCachePath),
    ?assertEqual("[]", Client1AclCache),

    %% post /clients/:clientid/subscribe
    SubscribeBody = #{topic => Topic, qos => Qos},
    SubscribePath =  emqx_mgmt_api_test_util:api_path(["clients", binary_to_list(ClientId1), "subscribe"]),
    {ok, _} =  emqx_mgmt_api_test_util:request_api(post, SubscribePath, "", AuthHeader, SubscribeBody),
    [{{_, AfterSubTopic}, #{qos := AfterSubQos}}] = emqx_mgmt:lookup_subscriptions(ClientId1),
    ?assertEqual(AfterSubTopic, Topic),
    ?assertEqual(AfterSubQos, Qos),

    %% delete /clients/:clientid/subscribe
    UnSubscribeQuery = "topic=" ++ binary_to_list(Topic),
    {ok, _} =  emqx_mgmt_api_test_util:request_api(delete, SubscribePath, UnSubscribeQuery, AuthHeader),
    ?assertEqual([], emqx_mgmt:lookup_subscriptions(Client1)).
