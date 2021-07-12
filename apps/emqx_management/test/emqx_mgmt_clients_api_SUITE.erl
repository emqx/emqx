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

-define(SERVER, "http://127.0.0.1:8081").
-define(BASE_PATH, "/api/v5").

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

    {ok, C1} = emqtt:start_link(#{username => Username1, clientid => ClientId1}),
    {ok, _} = emqtt:connect(C1),
    {ok, C2} = emqtt:start_link(#{username => Username2, clientid => ClientId2}),
    {ok, _} = emqtt:connect(C2),

    timer:sleep(300),

    %% get /clients
    {ok, Clients} = request_api(get, api_path(["clients"])),
    ClientsResponse = emqx_json:decode(Clients, [return_maps]),
    ClientsMeta = maps:get(<<"meta">>, ClientsResponse),
    ClientsPage = maps:get(<<"page">>, ClientsMeta),
    ClientsLimit = maps:get(<<"limit">>, ClientsMeta),
    ClientsCount = maps:get(<<"count">>, ClientsMeta),
    ?assertEqual(ClientsPage, 1),
    ?assertEqual(ClientsLimit, emqx_mgmt:max_row_limit()),
    ?assertEqual(ClientsCount, 2),

    %% get /clients/:clientid
    {ok, Client1} = request_api(get, api_path(["clients", binary_to_list(ClientId1)])),
    Client1Response = emqx_json:decode(Client1, [return_maps]),
    ?assertEqual(Username1, maps:get(<<"username">>, Client1Response)),
    ?assertEqual(ClientId1, maps:get(<<"clientid">>, Client1Response)),

    %% delete /clients/:clientid kickout
    {ok, _} = request_api(delete, api_path(["clients", binary_to_list(ClientId2)])),
    AfterKickoutResponse = request_api(get, api_path(["clients", binary_to_list(ClientId2)])),
    ?assertEqual({error, {"HTTP/1.1", 404, "Not Found"}}, AfterKickoutResponse),

    %% get /clients/:clientid/acl_cache should has no acl cache
    {ok, Client1AclCache} = request_api(get,
        api_path(["clients", binary_to_list(ClientId1), "acl_cache"])),
    ?assertEqual("[]", Client1AclCache),

    %% get /clients/:clientid/acl_cache should has no acl cache
    {ok, Client1AclCache} = request_api(get,
        api_path(["clients", binary_to_list(ClientId1), "acl_cache"])),
    ?assertEqual("[]", Client1AclCache),

    %% post /clients/:clientid/subscribe
    SubscribeBody = #{topic => Topic, qos => Qos},
    SubscribePath = api_path(["clients", binary_to_list(ClientId1), "subscribe"]),
    {ok, _} = request_api(post, SubscribePath, "", auth_header_(), SubscribeBody),
    [{{_, AfterSubTopic}, #{qos := AfterSubQos}}] = emqx_mgmt:lookup_subscriptions(ClientId1),
    ?assertEqual(AfterSubTopic, Topic),
    ?assertEqual(AfterSubQos, Qos),

    %% delete /clients/:clientid/subscribe
    UnSubscribeQuery = "topic=" ++ binary_to_list(Topic),
    {ok, _} = request_api(delete, SubscribePath, UnSubscribeQuery, auth_header_()),
    ?assertEqual([], emqx_mgmt:lookup_subscriptions(Client1)).

%%%==============================================================================================
%% test util function
request_api(Method, Url) ->
    request_api(Method, Url, [], auth_header_(), []).

request_api(Method, Url, Auth) ->
    request_api(Method, Url, [], Auth, []).

request_api(Method, Url, QueryParams, Auth) ->
    request_api(Method, Url, QueryParams, Auth, []).

request_api(Method, Url, QueryParams, Auth, []) ->
    NewUrl = case QueryParams of
                 "" -> Url;
                 _ -> Url ++ "?" ++ QueryParams
             end,
    do_request_api(Method, {NewUrl, [Auth]});
request_api(Method, Url, QueryParams, Auth, Body) ->
    NewUrl = case QueryParams of
                 "" -> Url;
                 _ -> Url ++ "?" ++ QueryParams
             end,
    do_request_api(Method, {NewUrl, [Auth], "application/json", emqx_json:encode(Body)}).

do_request_api(Method, Request)->
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _, Return} }
            when Code =:= 200 orelse Code =:= 201 ->
            {ok, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

auth_header_() ->
    AppId = <<"admin">>,
    AppSecret = <<"public">>,
    auth_header_(binary_to_list(AppId), binary_to_list(AppSecret)).

auth_header_(User, Pass) ->
    Encoded = base64:encode_to_string(lists:append([User,":",Pass])),
    {"Authorization","Basic " ++ Encoded}.

api_path(Parts)->
    ?SERVER ++ filename:join([?BASE_PATH | Parts]).
