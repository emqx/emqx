%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    [
        {group, persistent_sessions}
        | AllTCs -- persistent_session_testcases()
    ].

groups() ->
    [{persistent_sessions, persistent_session_testcases()}].

persistent_session_testcases() ->
    [
        t_persistent_sessions1,
        t_persistent_sessions2,
        t_persistent_sessions3,
        t_persistent_sessions4,
        t_persistent_sessions5
    ].

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite(),
    Config.

end_per_suite(_) ->
    emqx_mgmt_api_test_util:end_suite().

init_per_group(persistent_sessions, Config) ->
    AppSpecs = [
        {emqx, "session_persistence.enable = true"},
        emqx_management
    ],
    Dashboard = emqx_mgmt_api_test_util:emqx_dashboard(
        "dashboard.listeners.http { enable = true, bind = 18084 }"
    ),
    Cluster = [
        {emqx_mgmt_api_clients_SUITE1, #{role => core, apps => AppSpecs ++ [Dashboard]}},
        {emqx_mgmt_api_clients_SUITE2, #{role => core, apps => AppSpecs}}
    ],
    Nodes = emqx_cth_cluster:start(
        Cluster,
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{nodes, Nodes} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(persistent_sessions, Config) ->
    Nodes = ?config(nodes, Config),
    emqx_cth_cluster:stop(Nodes),
    ok;
end_per_group(_Group, _Config) ->
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

t_persistent_sessions1(Config) ->
    [N1, _N2] = ?config(nodes, Config),
    APIPort = 18084,
    Port1 = get_mqtt_port(N1, tcp),

    ?assertMatch({ok, {{_, 200, _}, _, #{<<"data">> := []}}}, list_request(APIPort)),

    ?check_trace(
        begin
            %% Scenario 1
            %% 1) Client connects and is listed as connected.
            ?tp(notice, "scenario 1", #{}),
            O = #{api_port => APIPort},
            ClientId = <<"c1">>,
            C1 = connect_client(#{port => Port1, clientid => ClientId}),
            assert_single_client(O#{node => N1, clientid => ClientId, status => connected}),
            %% 2) Client disconnects and is listed as disconnected.
            ok = emqtt:disconnect(C1),
            assert_single_client(O#{node => N1, clientid => ClientId, status => disconnected}),
            %% 3) Client reconnects and is listed as connected.
            C2 = connect_client(#{port => Port1, clientid => ClientId}),
            assert_single_client(O#{node => N1, clientid => ClientId, status => connected}),
            %% 4) Client disconnects.
            ok = emqtt:stop(C2),
            %% 5) Session is GC'ed, client is removed from list.
            ?tp(notice, "gc", #{}),
            %% simulate GC
            ok = erpc:call(N1, emqx_persistent_session_ds, destroy_session, [ClientId]),
            ?retry(
                100,
                20,
                ?assertMatch(
                    {ok, {{_, 200, _}, _, #{<<"data">> := []}}},
                    list_request(APIPort)
                )
            ),
            ok
        end,
        []
    ),
    ok.

t_persistent_sessions2(Config) ->
    [N1, _N2] = ?config(nodes, Config),
    APIPort = 18084,
    Port1 = get_mqtt_port(N1, tcp),

    ?assertMatch({ok, {{_, 200, _}, _, #{<<"data">> := []}}}, list_request(APIPort)),

    ?check_trace(
        begin
            %% Scenario 2
            %% 1) Client connects and is listed as connected.
            ?tp(notice, "scenario 2", #{}),
            O = #{api_port => APIPort},
            ClientId = <<"c2">>,
            C1 = connect_client(#{port => Port1, clientid => ClientId}),
            assert_single_client(O#{node => N1, clientid => ClientId, status => connected}),
            unlink(C1),
            %% 2) Client connects to the same node and takes over, listed only once.
            C2 = connect_client(#{port => Port1, clientid => ClientId}),
            assert_single_client(O#{node => N1, clientid => ClientId, status => connected}),
            ok = emqtt:stop(C2),
            ok = erpc:call(N1, emqx_persistent_session_ds, destroy_session, [ClientId]),
            ?retry(
                100,
                20,
                ?assertMatch(
                    {ok, {{_, 200, _}, _, #{<<"data">> := []}}},
                    list_request(APIPort)
                )
            ),

            ok
        end,
        []
    ),
    ok.

t_persistent_sessions3(Config) ->
    [N1, N2] = ?config(nodes, Config),
    APIPort = 18084,
    Port1 = get_mqtt_port(N1, tcp),
    Port2 = get_mqtt_port(N2, tcp),

    ?assertMatch({ok, {{_, 200, _}, _, #{<<"data">> := []}}}, list_request(APIPort)),

    ?check_trace(
        begin
            %% Scenario 3
            %% 1) Client connects and is listed as connected.
            ?tp(notice, "scenario 3", #{}),
            O = #{api_port => APIPort},
            ClientId = <<"c3">>,
            C1 = connect_client(#{port => Port1, clientid => ClientId}),
            assert_single_client(O#{node => N1, clientid => ClientId, status => connected}),
            unlink(C1),
            %% 2) Client connects to *another node* and takes over, listed only once.
            C2 = connect_client(#{port => Port2, clientid => ClientId}),
            assert_single_client(O#{node => N2, clientid => ClientId, status => connected}),
            %% Doesn't show up in the other node while alive
            ?retry(
                100,
                20,
                ?assertMatch(
                    {ok, {{_, 200, _}, _, #{<<"data">> := []}}},
                    list_request(APIPort, "node=" ++ atom_to_list(N1))
                )
            ),
            ok = emqtt:stop(C2),
            ok = erpc:call(N1, emqx_persistent_session_ds, destroy_session, [ClientId]),

            ok
        end,
        []
    ),
    ok.

t_persistent_sessions4(Config) ->
    [N1, N2] = ?config(nodes, Config),
    APIPort = 18084,
    Port1 = get_mqtt_port(N1, tcp),
    Port2 = get_mqtt_port(N2, tcp),

    ?assertMatch({ok, {{_, 200, _}, _, #{<<"data">> := []}}}, list_request(APIPort)),

    ?check_trace(
        begin
            %% Scenario 4
            %% 1) Client connects and is listed as connected.
            ?tp(notice, "scenario 4", #{}),
            O = #{api_port => APIPort},
            ClientId = <<"c4">>,
            C1 = connect_client(#{port => Port1, clientid => ClientId}),
            assert_single_client(O#{node => N1, clientid => ClientId, status => connected}),
            %% 2) Client disconnects and is listed as disconnected.
            ok = emqtt:stop(C1),
            %% While disconnected, shows up in both nodes.
            assert_single_client(O#{node => N1, clientid => ClientId, status => disconnected}),
            assert_single_client(O#{node => N2, clientid => ClientId, status => disconnected}),
            %% 3) Client reconnects to *another node* and is listed as connected once.
            C2 = connect_client(#{port => Port2, clientid => ClientId}),
            assert_single_client(O#{node => N2, clientid => ClientId, status => connected}),
            %% Doesn't show up in the other node while alive
            ?retry(
                100,
                20,
                ?assertMatch(
                    {ok, {{_, 200, _}, _, #{<<"data">> := []}}},
                    list_request(APIPort, "node=" ++ atom_to_list(N1))
                )
            ),
            ok = emqtt:stop(C2),
            ok = erpc:call(N1, emqx_persistent_session_ds, destroy_session, [ClientId]),

            ok
        end,
        []
    ),
    ok.

t_persistent_sessions5(Config) ->
    [N1, N2] = ?config(nodes, Config),
    APIPort = 18084,
    Port1 = get_mqtt_port(N1, tcp),
    Port2 = get_mqtt_port(N2, tcp),

    ?assertMatch({ok, {{_, 200, _}, _, #{<<"data">> := []}}}, list_request(APIPort)),

    ?check_trace(
        begin
            %% Pagination with mixed clients
            ClientId1 = <<"c5">>,
            ClientId2 = <<"c6">>,
            ClientId3 = <<"c7">>,
            ClientId4 = <<"c8">>,
            %% persistent
            C1 = connect_client(#{port => Port1, clientid => ClientId1}),
            C2 = connect_client(#{port => Port2, clientid => ClientId2}),
            %% in-memory
            C3 = connect_client(#{
                port => Port1, clientid => ClientId3, expiry => 0, clean_start => true
            }),
            C4 = connect_client(#{
                port => Port2, clientid => ClientId4, expiry => 0, clean_start => true
            }),

            P1 = list_request(APIPort, "limit=3&page=1"),
            P2 = list_request(APIPort, "limit=3&page=2"),
            ?assertMatch(
                {ok,
                    {{_, 200, _}, _, #{
                        <<"data">> := [_, _, _],
                        <<"meta">> := #{
                            %% TODO: if/when we fix the persistent session count, this
                            %% should be 4.
                            <<"count">> := 6,
                            <<"hasnext">> := true
                        }
                    }}},
                P1
            ),
            ?assertMatch(
                {ok,
                    {{_, 200, _}, _, #{
                        <<"data">> := [_],
                        <<"meta">> := #{
                            %% TODO: if/when we fix the persistent session count, this
                            %% should be 4.
                            <<"count">> := 6,
                            <<"hasnext">> := false
                        }
                    }}},
                P2
            ),
            {ok, {_, _, #{<<"data">> := R1}}} = P1,
            {ok, {_, _, #{<<"data">> := R2}}} = P2,
            ?assertEqual(
                lists:sort([ClientId1, ClientId2, ClientId3, ClientId4]),
                lists:sort(lists:map(fun(#{<<"clientid">> := CId}) -> CId end, R1 ++ R2))
            ),
            ?assertMatch(
                {ok,
                    {{_, 200, _}, _, #{
                        <<"data">> := [_, _],
                        <<"meta">> := #{
                            %% TODO: if/when we fix the persistent session count, this
                            %% should be 4.
                            <<"count">> := 6,
                            <<"hasnext">> := true
                        }
                    }}},
                list_request(APIPort, "limit=2&page=1")
            ),
            %% Disconnect persistent sessions
            lists:foreach(fun emqtt:stop/1, [C1, C2]),

            P3 =
                ?retry(200, 10, begin
                    P3_ = list_request(APIPort, "limit=3&page=1"),
                    ?assertMatch(
                        {ok,
                            {{_, 200, _}, _, #{
                                <<"data">> := [_, _, _],
                                <<"meta">> := #{
                                    <<"count">> := 4,
                                    <<"hasnext">> := true
                                }
                            }}},
                        P3_
                    ),
                    P3_
                end),
            P4 =
                ?retry(200, 10, begin
                    P4_ = list_request(APIPort, "limit=3&page=2"),
                    ?assertMatch(
                        {ok,
                            {{_, 200, _}, _, #{
                                <<"data">> := [_],
                                <<"meta">> := #{
                                    <<"count">> := 4,
                                    <<"hasnext">> := false
                                }
                            }}},
                        P4_
                    ),
                    P4_
                end),
            {ok, {_, _, #{<<"data">> := R3}}} = P3,
            {ok, {_, _, #{<<"data">> := R4}}} = P4,
            ?assertEqual(
                lists:sort([ClientId1, ClientId2, ClientId3, ClientId4]),
                lists:sort(lists:map(fun(#{<<"clientid">> := CId}) -> CId end, R3 ++ R4))
            ),

            lists:foreach(fun emqtt:stop/1, [C3, C4]),

            ok
        end,
        []
    ),
    ok.

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

get_mqtt_port(Node, Type) ->
    {_IP, Port} = erpc:call(Node, emqx_config, get, [[listeners, Type, default, bind]]),
    Port.

request(Method, Path, Params) ->
    request(Method, Path, Params, _QueryParams = "").

request(Method, Path, Params, QueryParams) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    case emqx_mgmt_api_test_util:request_api(Method, Path, QueryParams, AuthHeader, Params, Opts) of
        {ok, {Status, Headers, Body0}} ->
            Body = maybe_json_decode(Body0),
            {ok, {Status, Headers, Body}};
        {error, {Status, Headers, Body0}} ->
            Body =
                case emqx_utils_json:safe_decode(Body0, [return_maps]) of
                    {ok, Decoded0 = #{<<"message">> := Msg0}} ->
                        Msg = maybe_json_decode(Msg0),
                        Decoded0#{<<"message">> := Msg};
                    {ok, Decoded0} ->
                        Decoded0;
                    {error, _} ->
                        Body0
                end,
            {error, {Status, Headers, Body}};
        Error ->
            Error
    end.

maybe_json_decode(X) ->
    case emqx_utils_json:safe_decode(X, [return_maps]) of
        {ok, Decoded} -> Decoded;
        {error, _} -> X
    end.

list_request(Port) ->
    list_request(Port, _QueryParams = "").

list_request(Port, QueryParams) ->
    Host = "http://127.0.0.1:" ++ integer_to_list(Port),
    Path = emqx_mgmt_api_test_util:api_path(Host, ["clients"]),
    request(get, Path, [], QueryParams).

lookup_request(ClientId) ->
    lookup_request(ClientId, 18083).

lookup_request(ClientId, Port) ->
    Host = "http://127.0.0.1:" ++ integer_to_list(Port),
    Path = emqx_mgmt_api_test_util:api_path(Host, ["clients", ClientId]),
    request(get, Path, []).

assert_single_client(Opts) ->
    #{
        api_port := APIPort,
        clientid := ClientId,
        node := Node,
        status := Connected
    } = Opts,
    IsConnected =
        case Connected of
            connected -> true;
            disconnected -> false
        end,
    ?retry(
        100,
        20,
        ?assertMatch(
            {ok, {{_, 200, _}, _, #{<<"data">> := [#{<<"connected">> := IsConnected}]}}},
            list_request(APIPort)
        )
    ),
    ?retry(
        100,
        20,
        ?assertMatch(
            {ok, {{_, 200, _}, _, #{<<"data">> := [#{<<"connected">> := IsConnected}]}}},
            list_request(APIPort, "node=" ++ atom_to_list(Node)),
            #{node => Node}
        )
    ),
    ?assertMatch(
        {ok, {{_, 200, _}, _, #{<<"connected">> := IsConnected}}},
        lookup_request(ClientId, APIPort)
    ),
    ok.

connect_client(Opts) ->
    Defaults = #{
        expiry => 30,
        clean_start => false
    },
    #{
        port := Port,
        clientid := ClientId,
        clean_start := CleanStart,
        expiry := EI
    } = maps:merge(Defaults, Opts),
    {ok, C} = emqtt:start_link([
        {port, Port},
        {proto_ver, v5},
        {clientid, ClientId},
        {clean_start, CleanStart},
        {properties, #{'Session-Expiry-Interval' => EI}}
    ]),
    {ok, _} = emqtt:connect(C),
    C.
