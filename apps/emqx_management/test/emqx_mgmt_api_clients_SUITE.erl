%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_clients_SUITE).
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite(),
    Config.

end_per_suite(_) ->
    emqx_mgmt_api_test_util:end_suite().

t_clients(_) ->
    process_flag(trap_exit, true),

    Username1 = <<"user1">>,
    ClientId1 = <<"client1">>,

    Username2 = <<"user2">>,
    ClientId2 = <<"client2">>,

    Topic = <<"topic_1">>,
    Qos = 0,

    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),

    {ok, C1} = emqtt:start_link(#{
        username => Username1,
        clientid => ClientId1,
        proto_ver => v5,
        properties => #{'Session-Expiry-Interval' => 120}
    }),
    {ok, _} = emqtt:connect(C1),
    {ok, C2} = emqtt:start_link(#{username => Username2, clientid => ClientId2}),
    {ok, _} = emqtt:connect(C2),

    timer:sleep(300),

    %% get /clients
    ClientsPath = emqx_mgmt_api_test_util:api_path(["clients"]),
    {ok, Clients} = emqx_mgmt_api_test_util:request_api(get, ClientsPath),
    ClientsResponse = emqx_utils_json:decode(Clients, [return_maps]),
    ClientsMeta = maps:get(<<"meta">>, ClientsResponse),
    ClientsPage = maps:get(<<"page">>, ClientsMeta),
    ClientsLimit = maps:get(<<"limit">>, ClientsMeta),
    ClientsCount = maps:get(<<"count">>, ClientsMeta),
    ?assertEqual(ClientsPage, 1),
    ?assertEqual(ClientsLimit, emqx_mgmt:default_row_limit()),
    ?assertEqual(ClientsCount, 2),

    %% get /clients/:clientid
    Client1Path = emqx_mgmt_api_test_util:api_path(["clients", binary_to_list(ClientId1)]),
    {ok, Client1} = emqx_mgmt_api_test_util:request_api(get, Client1Path),
    Client1Response = emqx_utils_json:decode(Client1, [return_maps]),
    ?assertEqual(Username1, maps:get(<<"username">>, Client1Response)),
    ?assertEqual(ClientId1, maps:get(<<"clientid">>, Client1Response)),
    ?assertEqual(120, maps:get(<<"expiry_interval">>, Client1Response)),

    %% delete /clients/:clientid kickout
    Client2Path = emqx_mgmt_api_test_util:api_path(["clients", binary_to_list(ClientId2)]),
    {ok, _} = emqx_mgmt_api_test_util:request_api(delete, Client2Path),
    Kick =
        receive
            {'EXIT', C2, _} ->
                ok
        after 300 ->
            timeout
        end,
    ?assertEqual(ok, Kick),
    %% Client info is cleared after DOWN event
    ?retry(_Interval = 100, _Attempts = 5, begin
        AfterKickoutResponse2 = emqx_mgmt_api_test_util:request_api(get, Client2Path),
        ?assertEqual(AfterKickoutResponse2, {error, {"HTTP/1.1", 404, "Not Found"}})
    end),

    %% get /clients/:clientid/authorization/cache should have no authz cache
    Client1AuthzCachePath = emqx_mgmt_api_test_util:api_path([
        "clients",
        binary_to_list(ClientId1),
        "authorization",
        "cache"
    ]),
    {ok, Client1AuthzCache0} = emqx_mgmt_api_test_util:request_api(get, Client1AuthzCachePath),
    ?assertEqual("[]", Client1AuthzCache0),

    %% post /clients/:clientid/subscribe
    SubscribeBody = #{topic => Topic, qos => Qos, nl => 1, rh => 1},
    SubscribePath = emqx_mgmt_api_test_util:api_path([
        "clients",
        binary_to_list(ClientId1),
        "subscribe"
    ]),
    {ok, _} = emqx_mgmt_api_test_util:request_api(
        post,
        SubscribePath,
        "",
        AuthHeader,
        SubscribeBody
    ),
    timer:sleep(100),
    {_, [{AfterSubTopic, #{qos := AfterSubQos}}]} = emqx_mgmt:list_client_subscriptions(ClientId1),
    ?assertEqual(AfterSubTopic, Topic),
    ?assertEqual(AfterSubQos, Qos),

    %% get /clients/:clientid/subscriptions
    SubscriptionsPath = emqx_mgmt_api_test_util:api_path([
        "clients",
        binary_to_list(ClientId1),
        "subscriptions"
    ]),
    {ok, SubscriptionsRes} = emqx_mgmt_api_test_util:request_api(
        get,
        SubscriptionsPath,
        "",
        AuthHeader
    ),
    [SubscriptionsData] = emqx_utils_json:decode(SubscriptionsRes, [return_maps]),
    ?assertMatch(
        #{
            <<"clientid">> := ClientId1,
            <<"nl">> := 1,
            <<"rap">> := 0,
            <<"rh">> := 1,
            <<"node">> := _,
            <<"qos">> := Qos,
            <<"topic">> := Topic
        },
        SubscriptionsData
    ),

    %% post /clients/:clientid/unsubscribe
    UnSubscribePath = emqx_mgmt_api_test_util:api_path([
        "clients",
        binary_to_list(ClientId1),
        "unsubscribe"
    ]),
    UnSubscribeBody = #{topic => Topic},
    {ok, _} = emqx_mgmt_api_test_util:request_api(
        post,
        UnSubscribePath,
        "",
        AuthHeader,
        UnSubscribeBody
    ),
    timer:sleep(100),
    ?assertEqual([], emqx_mgmt:list_client_subscriptions(ClientId1)),

    %% testcase cleanup, kickout client1
    {ok, _} = emqx_mgmt_api_test_util:request_api(delete, Client1Path),
    timer:sleep(300),
    AfterKickoutResponse1 = emqx_mgmt_api_test_util:request_api(get, Client1Path),
    ?assertEqual({error, {"HTTP/1.1", 404, "Not Found"}}, AfterKickoutResponse1).

t_clients_bad_value_type(_) ->
    %% get /clients
    AuthHeader = [emqx_common_test_http:default_auth_header()],
    ClientsPath = emqx_mgmt_api_test_util:api_path(["clients"]),
    QsString = cow_qs:qs([{<<"ip_address">>, <<"127.0.0.1:8080">>}]),
    {ok, 400, Resp} = emqx_mgmt_api_test_util:request_api(
        get, ClientsPath, QsString, AuthHeader, [], #{compatible_mode => true}
    ),
    ?assertMatch(
        #{
            <<"code">> := <<"INVALID_PARAMETER">>,
            <<"message">> :=
                <<"the ip_address parameter expected type is ip, but the value is 127.0.0.1:8080">>
        },
        emqx_utils_json:decode(Resp, [return_maps])
    ).

t_authz_cache(_) ->
    ClientId = <<"client_authz">>,

    {ok, C} = emqtt:start_link(#{clientid => ClientId}),
    {ok, _} = emqtt:connect(C),
    {ok, _, _} = emqtt:subscribe(C, <<"topic/1">>, 1),

    ClientAuthzCachePath = emqx_mgmt_api_test_util:api_path([
        "clients",
        binary_to_list(ClientId),
        "authorization",
        "cache"
    ]),
    {ok, ClientAuthzCache} = emqx_mgmt_api_test_util:request_api(get, ClientAuthzCachePath),
    ?assertMatch(
        [
            #{
                <<"access">> :=
                    #{<<"action_type">> := <<"subscribe">>, <<"qos">> := 1},
                <<"result">> := <<"allow">>,
                <<"topic">> := <<"topic/1">>,
                <<"updated_time">> := _
            }
        ],
        emqx_utils_json:decode(ClientAuthzCache, [return_maps])
    ),

    ok = emqtt:stop(C).

t_kickout_clients(_) ->
    process_flag(trap_exit, true),

    ClientId1 = <<"client1">>,
    ClientId2 = <<"client2">>,
    ClientId3 = <<"client3">>,

    {ok, C1} = emqtt:start_link(#{
        clientid => ClientId1,
        proto_ver => v5,
        properties => #{'Session-Expiry-Interval' => 120}
    }),
    {ok, _} = emqtt:connect(C1),
    {ok, C2} = emqtt:start_link(#{clientid => ClientId2}),
    {ok, _} = emqtt:connect(C2),
    {ok, C3} = emqtt:start_link(#{clientid => ClientId3}),
    {ok, _} = emqtt:connect(C3),

    emqx_common_test_helpers:wait_for(
        ?FUNCTION_NAME,
        ?LINE,
        fun() ->
            try
                [_] = emqx_cm:lookup_channels(ClientId1),
                [_] = emqx_cm:lookup_channels(ClientId2),
                [_] = emqx_cm:lookup_channels(ClientId3),
                true
            catch
                error:badmatch ->
                    false
            end
        end,
        2000
    ),

    %% get /clients
    ClientsPath = emqx_mgmt_api_test_util:api_path(["clients"]),
    {ok, Clients} = emqx_mgmt_api_test_util:request_api(get, ClientsPath),
    ClientsResponse = emqx_utils_json:decode(Clients, [return_maps]),
    ClientsMeta = maps:get(<<"meta">>, ClientsResponse),
    ClientsPage = maps:get(<<"page">>, ClientsMeta),
    ClientsLimit = maps:get(<<"limit">>, ClientsMeta),
    ClientsCount = maps:get(<<"count">>, ClientsMeta),
    ?assertEqual(ClientsPage, 1),
    ?assertEqual(ClientsLimit, emqx_mgmt:default_row_limit()),
    ?assertEqual(ClientsCount, 3),

    %% kickout clients
    KickoutPath = emqx_mgmt_api_test_util:api_path(["clients", "kickout", "bulk"]),
    KickoutBody = [ClientId1, ClientId2, ClientId3],
    {ok, 204, _} = emqx_mgmt_api_test_util:request_api_with_body(post, KickoutPath, KickoutBody),

    ReceiveExit = fun({ClientPid, ClientId}) ->
        receive
            {'EXIT', Pid, _} when Pid =:= ClientPid ->
                ok
        after 1000 ->
            error({timeout, ClientId})
        end
    end,
    lists:foreach(ReceiveExit, [{C1, ClientId1}, {C2, ClientId2}, {C3, ClientId3}]),
    {ok, Clients2} = emqx_mgmt_api_test_util:request_api(get, ClientsPath),
    ClientsResponse2 = emqx_utils_json:decode(Clients2, [return_maps]),
    ?assertMatch(#{<<"meta">> := #{<<"count">> := 0}}, ClientsResponse2).

t_query_clients_with_time(_) ->
    process_flag(trap_exit, true),

    Username1 = <<"user1">>,
    ClientId1 = <<"client1">>,

    Username2 = <<"user2">>,
    ClientId2 = <<"client2">>,

    {ok, C1} = emqtt:start_link(#{username => Username1, clientid => ClientId1}),
    {ok, _} = emqtt:connect(C1),
    {ok, C2} = emqtt:start_link(#{username => Username2, clientid => ClientId2}),
    {ok, _} = emqtt:connect(C2),

    timer:sleep(100),

    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    ClientsPath = emqx_mgmt_api_test_util:api_path(["clients"]),
    %% get /clients with time(rfc3339)
    NowTimeStampInt = erlang:system_time(millisecond),
    %% Do not uri_encode `=` to `%3D`
    Rfc3339String = emqx_http_lib:uri_encode(
        binary:bin_to_list(
            emqx_utils_calendar:epoch_to_rfc3339(NowTimeStampInt)
        )
    ),
    TimeStampString = emqx_http_lib:uri_encode(integer_to_list(NowTimeStampInt)),

    LteKeys = ["lte_created_at=", "lte_connected_at="],
    GteKeys = ["gte_created_at=", "gte_connected_at="],
    LteParamRfc3339 = [Param ++ Rfc3339String || Param <- LteKeys],
    LteParamStamp = [Param ++ TimeStampString || Param <- LteKeys],
    GteParamRfc3339 = [Param ++ Rfc3339String || Param <- GteKeys],
    GteParamStamp = [Param ++ TimeStampString || Param <- GteKeys],

    RequestResults =
        [
            emqx_mgmt_api_test_util:request_api(get, ClientsPath, Param, AuthHeader)
         || Param <-
                LteParamRfc3339 ++ LteParamStamp ++
                    GteParamRfc3339 ++ GteParamStamp
        ],
    DecodedResults = [
        emqx_utils_json:decode(Response, [return_maps])
     || {ok, Response} <- RequestResults
    ],
    {LteResponseDecodeds, GteResponseDecodeds} = lists:split(4, DecodedResults),
    %% EachData :: list()
    [
        ?assert(time_string_to_epoch_millisecond(CreatedAt) < NowTimeStampInt)
     || #{<<"data">> := EachData} <- LteResponseDecodeds,
        #{<<"created_at">> := CreatedAt} <- EachData
    ],
    [
        ?assert(time_string_to_epoch_millisecond(ConnectedAt) < NowTimeStampInt)
     || #{<<"data">> := EachData} <- LteResponseDecodeds,
        #{<<"connected_at">> := ConnectedAt} <- EachData
    ],
    [
        ?assertEqual(EachData, [])
     || #{<<"data">> := EachData} <- GteResponseDecodeds
    ],

    %% testcase cleanup, kickout client1 and client2
    Client1Path = emqx_mgmt_api_test_util:api_path(["clients", binary_to_list(ClientId1)]),
    Client2Path = emqx_mgmt_api_test_util:api_path(["clients", binary_to_list(ClientId2)]),
    {ok, _} = emqx_mgmt_api_test_util:request_api(delete, Client1Path),
    {ok, _} = emqx_mgmt_api_test_util:request_api(delete, Client2Path).

t_keepalive(_Config) ->
    Username = "user_keepalive",
    ClientId = "client_keepalive",
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["clients", ClientId, "keepalive"]),
    Body = #{interval => 11},
    {error, {"HTTP/1.1", 404, "Not Found"}} =
        emqx_mgmt_api_test_util:request_api(put, Path, <<"">>, AuthHeader, Body),
    %% 65535 is the max value of keepalive
    MaxKeepalive = 65535,
    InitKeepalive = round(MaxKeepalive / 1.5 + 1),
    {ok, C1} = emqtt:start_link(#{
        username => Username, clientid => ClientId, keepalive => InitKeepalive
    }),
    {ok, _} = emqtt:connect(C1),
    [Pid] = emqx_cm:lookup_channels(list_to_binary(ClientId)),
    %% will reset to max keepalive if keepalive > max keepalive
    #{conninfo := #{keepalive := InitKeepalive}} = emqx_connection:info(Pid),
    ?assertMatch({keepalive, 65535000, _}, element(5, element(9, sys:get_state(Pid)))),

    {ok, NewClient} = emqx_mgmt_api_test_util:request_api(put, Path, <<"">>, AuthHeader, Body),
    #{<<"keepalive">> := 11} = emqx_utils_json:decode(NewClient, [return_maps]),
    #{conninfo := #{keepalive := Keepalive}} = emqx_connection:info(Pid),
    ?assertEqual(11, Keepalive),
    %% Disable keepalive
    Body1 = #{interval => 0},
    {ok, NewClient1} = emqx_mgmt_api_test_util:request_api(put, Path, <<"">>, AuthHeader, Body1),
    #{<<"keepalive">> := 0} = emqx_utils_json:decode(NewClient1, [return_maps]),
    ?assertMatch(#{conninfo := #{keepalive := 0}}, emqx_connection:info(Pid)),
    %% Maximal keepalive
    Body2 = #{interval => 65536},
    {error, {"HTTP/1.1", 400, _}} =
        emqx_mgmt_api_test_util:request_api(put, Path, <<"">>, AuthHeader, Body2),
    emqtt:disconnect(C1),
    ok.

t_client_id_not_found(_Config) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Http = {"HTTP/1.1", 404, "Not Found"},
    Body = "{\"code\":\"CLIENTID_NOT_FOUND\",\"message\":\"Client ID not found\"}",

    PathFun = fun(Suffix) ->
        emqx_mgmt_api_test_util:api_path(["clients", "no_existed_clientid"] ++ Suffix)
    end,
    ReqFun = fun(Method, Path) ->
        emqx_mgmt_api_test_util:request_api(
            Method, Path, "", AuthHeader, [], #{return_all => true}
        )
    end,

    PostFun = fun(Method, Path, Data) ->
        emqx_mgmt_api_test_util:request_api(
            Method, Path, "", AuthHeader, Data, #{return_all => true}
        )
    end,

    %% Client lookup
    ?assertMatch({error, {Http, _, Body}}, ReqFun(get, PathFun([]))),
    %% Client kickout
    ?assertMatch({error, {Http, _, Body}}, ReqFun(delete, PathFun([]))),
    %% Client Subscription list
    ?assertMatch({error, {Http, _, Body}}, ReqFun(get, PathFun(["subscriptions"]))),
    %% AuthZ Cache lookup
    ?assertMatch({error, {Http, _, Body}}, ReqFun(get, PathFun(["authorization", "cache"]))),
    %% AuthZ Cache clean
    ?assertMatch({error, {Http, _, Body}}, ReqFun(delete, PathFun(["authorization", "cache"]))),
    %% Client Subscribe
    SubBody = #{topic => <<"testtopic">>, qos => 1, nl => 1, rh => 1},
    ?assertMatch({error, {Http, _, Body}}, PostFun(post, PathFun(["subscribe"]), SubBody)),
    ?assertMatch(
        {error, {Http, _, Body}}, PostFun(post, PathFun(["subscribe", "bulk"]), [SubBody])
    ),
    %% Client Unsubscribe
    UnsubBody = #{topic => <<"testtopic">>},
    ?assertMatch({error, {Http, _, Body}}, PostFun(post, PathFun(["unsubscribe"]), UnsubBody)),
    ?assertMatch(
        {error, {Http, _, Body}}, PostFun(post, PathFun(["unsubscribe", "bulk"]), [UnsubBody])
    ).

time_string_to_epoch_millisecond(DateTime) ->
    time_string_to_epoch(DateTime, millisecond).

time_string_to_epoch(DateTime, Unit) when is_binary(DateTime) ->
    try binary_to_integer(DateTime) of
        TimeStamp when is_integer(TimeStamp) -> TimeStamp
    catch
        error:badarg ->
            calendar:rfc3339_to_system_time(
                binary_to_list(DateTime), [{unit, Unit}]
            )
    end.
