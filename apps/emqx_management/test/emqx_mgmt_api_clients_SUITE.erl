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

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_router.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

all() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    [
        {group, persistent_sessions},
        {group, msgs_base64_encoding},
        {group, msgs_plain_encoding}
        | AllTCs -- (persistent_session_testcases() ++ client_msgs_testcases())
    ].

groups() ->
    [
        {persistent_sessions, persistent_session_testcases()},
        {msgs_base64_encoding, client_msgs_testcases()},
        {msgs_plain_encoding, client_msgs_testcases()}
    ].

persistent_session_testcases() ->
    [
        t_persistent_sessions1,
        t_persistent_sessions2,
        t_persistent_sessions3,
        t_persistent_sessions4,
        t_persistent_sessions5,
        t_persistent_sessions6,
        t_persistent_sessions_subscriptions1,
        t_list_clients_v2
    ].
client_msgs_testcases() ->
    [
        t_inflight_messages,
        t_mqueue_messages
    ].

init_per_suite(Config) ->
    ok = snabbkaffe:start_trace(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    {ok, _} = emqx_common_test_http:create_default_app(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_group(persistent_sessions, Config) ->
    AppSpecs = [
        {emqx, "durable_sessions.enable = true"},
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
init_per_group(msgs_base64_encoding, Config) ->
    [{payload_encoding, base64} | Config];
init_per_group(msgs_plain_encoding, Config) ->
    [{payload_encoding, plain} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(persistent_sessions, Config) ->
    Nodes = ?config(nodes, Config),
    emqx_cth_cluster:stop(Nodes),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(TC, _Config) when
    TC =:= t_inflight_messages;
    TC =:= t_mqueue_messages
->
    ok = snabbkaffe:stop(),
    ClientId = atom_to_binary(TC),
    lists:foreach(fun(P) -> exit(P, kill) end, emqx_cm:lookup_channels(local, ClientId)),
    ok = emqx_common_test_helpers:wait_for(
        ?FUNCTION_NAME,
        ?LINE,
        fun() -> [] =:= emqx_cm:lookup_channels(local, ClientId) end,
        5000
    ),
    ok = snabbkaffe:stop(),
    ok;
end_per_testcase(_TC, _Config) ->
    ok = snabbkaffe:stop(),
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
            disconnect_and_destroy_session(C2),
            ?retry(
                100,
                20,
                ?assertMatch(
                    {ok, {{_, 200, _}, _, #{<<"data">> := []}}},
                    list_request(APIPort)
                )
            )
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
            disconnect_and_destroy_session(C2)
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
            disconnect_and_destroy_session(C2)
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
            lists:foreach(
                fun(ClientId) ->
                    ok = erpc:call(N1, emqx_persistent_session_ds, destroy_session, [ClientId])
                end,
                [ClientId1, ClientId2, ClientId3, ClientId4]
            ),

            ok
        end,
        []
    ),
    ok.

%% Checks that expired durable sessions are returned with `is_expired => true'.
t_persistent_sessions6(Config) ->
    [N1, _N2] = ?config(nodes, Config),
    APIPort = 18084,
    Port1 = get_mqtt_port(N1, tcp),

    ?assertMatch({ok, {{_, 200, _}, _, #{<<"data">> := []}}}, list_request(APIPort)),

    ?check_trace(
        begin
            O = #{api_port => APIPort},
            ClientId = <<"c1">>,
            C1 = connect_client(#{port => Port1, clientid => ClientId, expiry => 1}),
            assert_single_client(O#{node => N1, clientid => ClientId, status => connected}),
            ?retry(
                100,
                20,
                ?assertMatch(
                    {ok, {{_, 200, _}, _, #{<<"data">> := [#{<<"is_expired">> := false}]}}},
                    list_request(APIPort)
                )
            ),

            ok = emqtt:disconnect(C1),
            %% Wait for session to be considered expired but not GC'ed
            ct:sleep(2_000),
            assert_single_client(O#{node => N1, clientid => ClientId, status => disconnected}),
            N1Bin = atom_to_binary(N1),
            ?retry(
                100,
                20,
                ?assertMatch(
                    {ok,
                        {{_, 200, _}, _, #{
                            <<"data">> := [
                                #{
                                    <<"is_expired">> := true,
                                    <<"node">> := N1Bin,
                                    <<"disconnected_at">> := <<_/binary>>
                                }
                            ]
                        }}},
                    list_request(APIPort)
                )
            ),
            ?assertMatch(
                {ok,
                    {{_, 200, _}, _, #{
                        <<"is_expired">> := true,
                        <<"node">> := N1Bin,
                        <<"disconnected_at">> := <<_/binary>>
                    }}},
                get_client_request(APIPort, ClientId)
            ),

            C2 = connect_client(#{port => Port1, clientid => ClientId}),
            disconnect_and_destroy_session(C2),

            ok
        end,
        []
    ),
    ok.

%% Check that the output of `/clients/:clientid/subscriptions' has the expected keys.
t_persistent_sessions_subscriptions1(Config) ->
    [N1, _N2] = ?config(nodes, Config),
    APIPort = 18084,
    Port1 = get_mqtt_port(N1, tcp),

    ?assertMatch({ok, {{_, 200, _}, _, #{<<"data">> := []}}}, list_request(APIPort)),

    ?check_trace(
        begin
            ClientId = <<"c1">>,
            C1 = connect_client(#{port => Port1, clientid => ClientId}),
            {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(C1, <<"topic/1">>, 1),
            ?assertMatch(
                {ok,
                    {{_, 200, _}, _, [
                        #{
                            <<"durable">> := true,
                            <<"node">> := <<_/binary>>,
                            <<"clientid">> := ClientId,
                            <<"qos">> := 1,
                            <<"rap">> := 0,
                            <<"rh">> := 0,
                            <<"nl">> := 0,
                            <<"topic">> := <<"topic/1">>
                        }
                    ]}},
                get_subscriptions_request(APIPort, ClientId)
            ),

            %% Just disconnect
            ok = emqtt:disconnect(C1),
            ?assertMatch(
                {ok,
                    {{_, 200, _}, _, [
                        #{
                            <<"durable">> := true,
                            <<"node">> := null,
                            <<"clientid">> := ClientId,
                            <<"qos">> := 1,
                            <<"rap">> := 0,
                            <<"rh">> := 0,
                            <<"nl">> := 0,
                            <<"topic">> := <<"topic/1">>
                        }
                    ]}},
                get_subscriptions_request(APIPort, ClientId)
            ),

            C2 = connect_client(#{port => Port1, clientid => ClientId}),
            disconnect_and_destroy_session(C2),
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

t_query_multiple_clients(_) ->
    process_flag(trap_exit, true),
    ClientIdsUsers = [
        {<<"multi_client1">>, <<"multi_user1">>},
        {<<"multi_client1-1">>, <<"multi_user1">>},
        {<<"multi_client2">>, <<"multi_user2">>},
        {<<"multi_client2-1">>, <<"multi_user2">>},
        {<<"multi_client3">>, <<"multi_user3">>},
        {<<"multi_client3-1">>, <<"multi_user3">>},
        {<<"multi_client4">>, <<"multi_user4">>},
        {<<"multi_client4-1">>, <<"multi_user4">>}
    ],
    _Clients = lists:map(
        fun({ClientId, Username}) ->
            {ok, C} = emqtt:start_link(#{clientid => ClientId, username => Username}),
            {ok, _} = emqtt:connect(C),
            C
        end,
        ClientIdsUsers
    ),
    timer:sleep(100),

    Auth = emqx_mgmt_api_test_util:auth_header_(),

    %% Not found clients/users
    ?assertEqual([], get_clients(Auth, "clientid=no_such_client")),
    ?assertEqual([], get_clients(Auth, "clientid=no_such_client&clientid=no_such_client1")),
    %% Duplicates must cause no issues
    ?assertEqual([], get_clients(Auth, "clientid=no_such_client&clientid=no_such_client")),
    ?assertEqual([], get_clients(Auth, "username=no_such_user&clientid=no_such_user1")),
    ?assertEqual([], get_clients(Auth, "username=no_such_user&clientid=no_such_user")),
    ?assertEqual(
        [],
        get_clients(
            Auth,
            "clientid=no_such_client&clientid=no_such_client"
            "username=no_such_user&clientid=no_such_user1"
        )
    ),

    %% Requested ClientId / username values relate to different clients
    ?assertEqual([], get_clients(Auth, "clientid=multi_client1&username=multi_user2")),
    ?assertEqual(
        [],
        get_clients(
            Auth,
            "clientid=multi_client1&clientid=multi_client1-1"
            "&username=multi_user2&username=multi_user3"
        )
    ),
    ?assertEqual([<<"multi_client1">>], get_clients(Auth, "clientid=multi_client1")),
    %% Duplicates must cause no issues
    ?assertEqual(
        [<<"multi_client1">>], get_clients(Auth, "clientid=multi_client1&clientid=multi_client1")
    ),
    ?assertEqual(
        [<<"multi_client1">>], get_clients(Auth, "clientid=multi_client1&username=multi_user1")
    ),
    ?assertEqual(
        lists:sort([<<"multi_client1">>, <<"multi_client1-1">>]),
        lists:sort(get_clients(Auth, "username=multi_user1"))
    ),
    ?assertEqual(
        lists:sort([<<"multi_client1">>, <<"multi_client1-1">>]),
        lists:sort(get_clients(Auth, "clientid=multi_client1&clientid=multi_client1-1"))
    ),
    ?assertEqual(
        lists:sort([<<"multi_client1">>, <<"multi_client1-1">>]),
        lists:sort(
            get_clients(
                Auth,
                "clientid=multi_client1&clientid=multi_client1-1"
                "&username=multi_user1"
            )
        )
    ),
    ?assertEqual(
        lists:sort([<<"multi_client1">>, <<"multi_client1-1">>]),
        lists:sort(
            get_clients(
                Auth,
                "clientid=no-such-client&clientid=multi_client1&clientid=multi_client1-1"
                "&username=multi_user1"
            )
        )
    ),
    ?assertEqual(
        lists:sort([<<"multi_client1">>, <<"multi_client1-1">>]),
        lists:sort(
            get_clients(
                Auth,
                "clientid=no-such-client&clientid=multi_client1&clientid=multi_client1-1"
                "&username=multi_user1&username=no-such-user"
            )
        )
    ),

    AllQsFun = fun(QsKey, Pos) ->
        QsParts = [
            QsKey ++ "=" ++ binary_to_list(element(Pos, ClientUser))
         || ClientUser <- ClientIdsUsers
        ],
        lists:flatten(lists:join("&", QsParts))
    end,
    AllClientsQs = AllQsFun("clientid", 1),
    AllUsersQs = AllQsFun("username", 2),
    AllClientIds = lists:sort([C || {C, _U} <- ClientIdsUsers]),

    ?assertEqual(AllClientIds, lists:sort(get_clients(Auth, AllClientsQs))),
    ?assertEqual(AllClientIds, lists:sort(get_clients(Auth, AllUsersQs))),
    ?assertEqual(AllClientIds, lists:sort(get_clients(Auth, AllClientsQs ++ "&" ++ AllUsersQs))),

    %% Test with other filter params
    NodeQs = "&node=" ++ atom_to_list(node()),
    NoNodeQs = "&node=nonode@nohost",
    ?assertEqual(
        AllClientIds, lists:sort(get_clients(Auth, AllClientsQs ++ "&" ++ AllUsersQs ++ NodeQs))
    ),
    ?assertMatch(
        {error, _}, get_clients_expect_error(Auth, AllClientsQs ++ "&" ++ AllUsersQs ++ NoNodeQs)
    ),

    %% fuzzy search (like_{key}) must be ignored if accurate filter ({key}) is present
    ?assertEqual(
        AllClientIds,
        lists:sort(get_clients(Auth, AllClientsQs ++ "&" ++ AllUsersQs ++ "&like_clientid=multi"))
    ),
    ?assertEqual(
        AllClientIds,
        lists:sort(get_clients(Auth, AllClientsQs ++ "&" ++ AllUsersQs ++ "&like_username=multi"))
    ),
    ?assertEqual(
        AllClientIds,
        lists:sort(
            get_clients(Auth, AllClientsQs ++ "&" ++ AllUsersQs ++ "&like_clientid=does-not-matter")
        )
    ),
    ?assertEqual(
        AllClientIds,
        lists:sort(
            get_clients(Auth, AllClientsQs ++ "&" ++ AllUsersQs ++ "&like_username=does-not-matter")
        )
    ),

    %% Combining multiple clientids with like_username and vice versa must narrow down search results
    ?assertEqual(
        lists:sort([<<"multi_client1">>, <<"multi_client1-1">>]),
        lists:sort(get_clients(Auth, AllClientsQs ++ "&like_username=user1"))
    ),
    ?assertEqual(
        lists:sort([<<"multi_client1">>, <<"multi_client1-1">>]),
        lists:sort(get_clients(Auth, AllUsersQs ++ "&like_clientid=client1"))
    ),
    ?assertEqual([], get_clients(Auth, AllClientsQs ++ "&like_username=nouser")),
    ?assertEqual([], get_clients(Auth, AllUsersQs ++ "&like_clientid=nouser")).

t_query_multiple_clients_urlencode(_) ->
    process_flag(trap_exit, true),
    ClientIdsUsers = [
        {<<"multi_client=a?">>, <<"multi_user=a?">>},
        {<<"mutli_client=b?">>, <<"multi_user=b?">>}
    ],
    _Clients = lists:map(
        fun({ClientId, Username}) ->
            {ok, C} = emqtt:start_link(#{clientid => ClientId, username => Username}),
            {ok, _} = emqtt:connect(C),
            C
        end,
        ClientIdsUsers
    ),
    timer:sleep(100),

    Auth = emqx_mgmt_api_test_util:auth_header_(),
    ClientsQs = uri_string:compose_query([{<<"clientid">>, C} || {C, _} <- ClientIdsUsers]),
    UsersQs = uri_string:compose_query([{<<"username">>, U} || {_, U} <- ClientIdsUsers]),
    ExpectedClients = lists:sort([C || {C, _} <- ClientIdsUsers]),
    ?assertEqual(ExpectedClients, lists:sort(get_clients(Auth, ClientsQs))),
    ?assertEqual(ExpectedClients, lists:sort(get_clients(Auth, UsersQs))).

t_query_clients_with_fields(_) ->
    process_flag(trap_exit, true),
    TCBin = atom_to_binary(?FUNCTION_NAME),
    ClientId = <<TCBin/binary, "_client">>,
    Username = <<TCBin/binary, "_user">>,
    {ok, C} = emqtt:start_link(#{clientid => ClientId, username => Username}),
    {ok, _} = emqtt:connect(C),
    timer:sleep(100),

    Auth = emqx_mgmt_api_test_util:auth_header_(),
    ?assertEqual([#{<<"clientid">> => ClientId}], get_clients_all_fields(Auth, "fields=clientid")),
    ?assertEqual(
        [#{<<"clientid">> => ClientId, <<"username">> => Username}],
        get_clients_all_fields(Auth, "fields=clientid,username")
    ),

    AllFields = get_clients_all_fields(Auth, "fields=all"),
    DefaultFields = get_clients_all_fields(Auth, ""),

    ?assertEqual(AllFields, DefaultFields),
    ?assertMatch(
        [#{<<"clientid">> := ClientId, <<"username">> := Username}],
        AllFields
    ),
    ?assert(map_size(hd(AllFields)) > 2),
    ?assertMatch({error, _}, get_clients_expect_error(Auth, "fields=bad_field_name")),
    ?assertMatch({error, _}, get_clients_expect_error(Auth, "fields=all,bad_field_name")),
    ?assertMatch({error, _}, get_clients_expect_error(Auth, "fields=all,username,clientid")).

get_clients_all_fields(Auth, Qs) ->
    get_clients(Auth, Qs, false, false).

get_clients_expect_error(Auth, Qs) ->
    get_clients(Auth, Qs, true, true).

get_clients(Auth, Qs) ->
    get_clients(Auth, Qs, false, true).

get_clients(Auth, Qs, ExpectError, ClientIdOnly) ->
    ClientsPath = emqx_mgmt_api_test_util:api_path(["clients"]),
    Resp = emqx_mgmt_api_test_util:request_api(get, ClientsPath, Qs, Auth),
    case ExpectError of
        false ->
            {ok, Body} = Resp,
            #{<<"data">> := Clients} = emqx_utils_json:decode(Body),
            case ClientIdOnly of
                true -> [ClientId || #{<<"clientid">> := ClientId} <- Clients];
                false -> Clients
            end;
        true ->
            Resp
    end.

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
    ),
    %% Mqueue messages
    ?assertMatch({error, {Http, _, Body}}, ReqFun(get, PathFun(["mqueue_messages"]))),
    %% Inflight messages
    ?assertMatch({error, {Http, _, Body}}, ReqFun(get, PathFun(["inflight_messages"]))).

t_sessions_count(_Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Topic = <<"t/test_sessions_count">>,
    Conf0 = emqx_config:get([broker]),
    Conf1 = hocon_maps:deep_merge(Conf0, #{session_history_retain => 5}),
    %% from 1 seconds ago, which is for sure less than histry retain duration
    %% hence force a call to the gen_server emqx_cm_registry_keeper
    Since = erlang:system_time(seconds) - 1,
    ok = emqx_config:put(#{broker => Conf1}),
    {ok, Client} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {clean_start, true}
    ]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, Topic, 1),
    Path = emqx_mgmt_api_test_util:api_path(["sessions_count"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    ?assertMatch(
        {ok, "1"},
        emqx_mgmt_api_test_util:request_api(
            get, Path, "since=" ++ integer_to_list(Since), AuthHeader
        )
    ),
    ok = emqtt:disconnect(Client),
    %% simulate the situation in which the process is not running
    ok = supervisor:terminate_child(emqx_cm_sup, emqx_cm_registry_keeper),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(
            get, Path, "since=" ++ integer_to_list(Since), AuthHeader
        )
    ),
    %% restore default value
    ok = emqx_config:put(#{broker => Conf0}),
    ok = emqx_cm_registry_keeper:purge(),
    ok.

t_mqueue_messages(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Topic = <<"t/test_mqueue_msgs">>,
    Count = emqx_mgmt:default_row_limit(),
    ok = client_with_mqueue(ClientId, Topic, Count),
    Path = emqx_mgmt_api_test_util:api_path(["clients", ClientId, "mqueue_messages"]),
    ?assert(Count =< emqx:get_config([mqtt, max_mqueue_len])),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    IsMqueue = true,
    test_messages(Path, Topic, Count, AuthHeader, ?config(payload_encoding, Config), IsMqueue),

    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(
            get, Path, "limit=10&position=not-valid", AuthHeader
        )
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(
            get, Path, "limit=-5&position=not-valid", AuthHeader
        )
    ).

t_inflight_messages(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Topic = <<"t/test_inflight_msgs">>,
    PubCount = emqx_mgmt:default_row_limit(),
    {ok, Client} = client_with_inflight(ClientId, Topic, PubCount),
    Path = emqx_mgmt_api_test_util:api_path(["clients", ClientId, "inflight_messages"]),
    InflightLimit = emqx:get_config([mqtt, max_inflight]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    IsMqueue = false,
    test_messages(
        Path, Topic, InflightLimit, AuthHeader, ?config(payload_encoding, Config), IsMqueue
    ),

    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(
            get, Path, "limit=10&position=not-int", AuthHeader
        )
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(
            get, Path, "limit=-5&position=invalid-int", AuthHeader
        )
    ),
    emqtt:stop(Client).

client_with_mqueue(ClientId, Topic, Count) ->
    {ok, Client} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {clean_start, true},
        {properties, #{'Session-Expiry-Interval' => 120}}
    ]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, Topic, 1),
    ct:sleep(300),
    ok = emqtt:disconnect(Client),
    ct:sleep(100),
    publish_msgs(Topic, Count),
    ok.

client_with_inflight(ClientId, Topic, Count) ->
    {ok, Client} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {clean_start, true},
        {auto_ack, never}
    ]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, Topic, 1),
    publish_msgs(Topic, Count),
    {ok, Client}.

publish_msgs(Topic, Count) ->
    lists:foreach(
        fun(Seq) ->
            emqx_broker:publish(emqx_message:make(undefined, ?QOS_1, Topic, integer_to_binary(Seq)))
        end,
        lists:seq(1, Count)
    ).

test_messages(Path, Topic, Count, AuthHeader, PayloadEncoding, IsMqueue) ->
    Qs0 = io_lib:format("payload=~s", [PayloadEncoding]),

    {Msgs, StartPos, Pos} = ?retry(500, 10, begin
        {ok, MsgsResp} = emqx_mgmt_api_test_util:request_api(get, Path, Qs0, AuthHeader),
        #{<<"meta">> := Meta, <<"data">> := Msgs} = emqx_utils_json:decode(MsgsResp),
        #{<<"start">> := StartPos, <<"position">> := Pos} = Meta,

        ?assertEqual(StartPos, msg_pos(hd(Msgs), IsMqueue)),
        ?assertEqual(Pos, msg_pos(lists:last(Msgs), IsMqueue)),
        ?assertEqual(length(Msgs), Count),

        {Msgs, StartPos, Pos}
    end),

    lists:foreach(
        fun({Seq, #{<<"payload">> := P} = M}) ->
            ?assertEqual(Seq, binary_to_integer(decode_payload(P, PayloadEncoding))),
            ?assertMatch(
                #{
                    <<"msgid">> := _,
                    <<"topic">> := Topic,
                    <<"qos">> := _,
                    <<"publish_at">> := _,
                    <<"from_clientid">> := _,
                    <<"from_username">> := _,
                    <<"inserted_at">> := _
                },
                M
            ),
            IsMqueue andalso ?assertMatch(#{<<"mqueue_priority">> := _}, M)
        end,
        lists:zip(lists:seq(1, Count), Msgs)
    ),

    %% The first message payload is <<"1">>,
    %% and when it is urlsafe base64 encoded (with no padding), it's <<"MQ">>,
    %% so we cover both cases:
    %% - when total payload size exceeds the limit,
    %% - when the first message payload already exceeds the limit but is still returned in the response.
    QsPayloadLimit = io_lib:format("payload=~s&max_payload_bytes=1", [PayloadEncoding]),
    {ok, LimitedMsgsResp} = emqx_mgmt_api_test_util:request_api(
        get, Path, QsPayloadLimit, AuthHeader
    ),
    #{<<"meta">> := _, <<"data">> := FirstMsgOnly} = emqx_utils_json:decode(LimitedMsgsResp),
    ?assertEqual(1, length(FirstMsgOnly)),
    ?assertEqual(
        <<"1">>, decode_payload(maps:get(<<"payload">>, hd(FirstMsgOnly)), PayloadEncoding)
    ),

    Limit = 19,
    LastPos = lists:foldl(
        fun(PageSeq, ThisPos) ->
            Qs = io_lib:format("payload=~s&position=~s&limit=~p", [PayloadEncoding, ThisPos, Limit]),
            {ok, MsgsRespPage} = emqx_mgmt_api_test_util:request_api(get, Path, Qs, AuthHeader),
            #{
                <<"meta">> := #{<<"position">> := NextPos, <<"start">> := ThisStart},
                <<"data">> := MsgsPage
            } = emqx_utils_json:decode(MsgsRespPage),

            ?assertEqual(NextPos, msg_pos(lists:last(MsgsPage), IsMqueue)),
            %% Start position is the same in every response and points to the first msg
            ?assertEqual(StartPos, ThisStart),

            ?assertEqual(length(MsgsPage), Limit),
            ExpFirstPayload = integer_to_binary(PageSeq * Limit - Limit + 1),
            ExpLastPayload = integer_to_binary(PageSeq * Limit),
            ?assertEqual(
                ExpFirstPayload,
                decode_payload(maps:get(<<"payload">>, hd(MsgsPage)), PayloadEncoding)
            ),
            ?assertEqual(
                ExpLastPayload,
                decode_payload(maps:get(<<"payload">>, lists:last(MsgsPage)), PayloadEncoding)
            ),
            NextPos
        end,
        none,
        lists:seq(1, Count div 19)
    ),
    LastPartialPage = Count div 19 + 1,
    LastQs = io_lib:format("payload=~s&position=~s&limit=~p", [PayloadEncoding, LastPos, Limit]),
    {ok, MsgsRespLastP} = emqx_mgmt_api_test_util:request_api(get, Path, LastQs, AuthHeader),
    #{<<"meta">> := #{<<"position">> := LastPartialPos}, <<"data">> := MsgsLastPage} = emqx_utils_json:decode(
        MsgsRespLastP
    ),
    %% The same as the position of all messages returned in one request
    ?assertEqual(Pos, LastPartialPos),

    ?assertEqual(
        integer_to_binary(LastPartialPage * Limit - Limit + 1),
        decode_payload(maps:get(<<"payload">>, hd(MsgsLastPage)), PayloadEncoding)
    ),
    ?assertEqual(
        integer_to_binary(Count),
        decode_payload(maps:get(<<"payload">>, lists:last(MsgsLastPage)), PayloadEncoding)
    ),

    ExceedQs = io_lib:format("payload=~s&position=~s&limit=~p", [
        PayloadEncoding, LastPartialPos, Limit
    ]),
    {ok, MsgsEmptyResp} = emqx_mgmt_api_test_util:request_api(get, Path, ExceedQs, AuthHeader),
    ?assertMatch(
        #{
            <<"data">> := [],
            <<"meta">> := #{<<"position">> := LastPartialPos, <<"start">> := StartPos}
        },
        emqx_utils_json:decode(MsgsEmptyResp)
    ),

    %% Invalid common page params
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(get, Path, "limit=0", AuthHeader)
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(get, Path, "limit=limit", AuthHeader)
    ),

    %% Invalid max_paylod_bytes param
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(get, Path, "max_payload_bytes=0", AuthHeader)
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(get, Path, "max_payload_bytes=-1", AuthHeader)
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(get, Path, "max_payload_bytes=-1MB", AuthHeader)
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(get, Path, "max_payload_bytes=0MB", AuthHeader)
    ).

msg_pos(#{<<"inserted_at">> := TsBin, <<"mqueue_priority">> := Prio} = _Msg, true = _IsMqueue) ->
    <<TsBin/binary, "_", (emqx_utils_conv:bin(Prio))/binary>>;
msg_pos(#{<<"inserted_at">> := TsBin} = _Msg, _IsMqueue) ->
    TsBin.

decode_payload(Payload, base64) ->
    base64:decode(Payload);
decode_payload(Payload, _) ->
    Payload.

t_subscribe_shared_topic(_Config) ->
    ClientId = <<"client_subscribe_shared">>,

    {ok, C} = emqtt:start_link(#{clientid => ClientId}),
    {ok, _} = emqtt:connect(C),

    ClientPuber = <<"publish_client">>,
    {ok, PC} = emqtt:start_link(#{clientid => ClientPuber}),
    {ok, _} = emqtt:connect(PC),

    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),

    Http200 = {"HTTP/1.1", 200, "OK"},
    Http204 = {"HTTP/1.1", 204, "No Content"},

    PathFun = fun(Suffix) ->
        emqx_mgmt_api_test_util:api_path(["clients", "client_subscribe_shared"] ++ Suffix)
    end,

    PostFun = fun(Method, Path, Data) ->
        emqx_mgmt_api_test_util:request_api(
            Method, Path, "", AuthHeader, Data, #{return_all => true}
        )
    end,

    SharedT = <<"$share/group/testtopic">>,
    NonSharedT = <<"t/#">>,

    SubBodyFun = fun(T) -> #{topic => T, qos => 1, nl => 0, rh => 1} end,
    UnSubBodyFun = fun(T) -> #{topic => T} end,

    %% ====================
    %% Client Subscribe
    ?assertMatch(
        {ok, {Http200, _, _}},
        PostFun(post, PathFun(["subscribe"]), SubBodyFun(SharedT))
    ),
    ?assertMatch(
        {ok, {Http200, _, _}},
        PostFun(
            post,
            PathFun(["subscribe", "bulk"]),
            [SubBodyFun(T) || T <- [SharedT, NonSharedT]]
        )
    ),

    %% assert subscription
    ?assertMatch(
        [
            {_, #share{group = <<"group">>, topic = <<"testtopic">>}},
            {_, <<"t/#">>}
        ],
        ets:tab2list(?SUBSCRIPTION)
    ),

    ?assertMatch(
        [
            {{#share{group = <<"group">>, topic = <<"testtopic">>}, _}, #{
                nl := 0, qos := 1, rh := 1, rap := 0
            }},
            {{<<"t/#">>, _}, #{nl := 0, qos := 1, rh := 1, rap := 0}}
        ],
        ets:tab2list(?SUBOPTION)
    ),
    ?assertMatch(
        [{emqx_shared_subscription, <<"group">>, <<"testtopic">>, _}],
        ets:tab2list(emqx_shared_subscription)
    ),

    %% assert subscription virtual
    _ = emqtt:publish(PC, <<"testtopic">>, <<"msg1">>, [{qos, 0}]),
    ?assertReceive({publish, #{topic := <<"testtopic">>, payload := <<"msg1">>}}),
    _ = emqtt:publish(PC, <<"t/1">>, <<"msg2">>, [{qos, 0}]),
    ?assertReceive({publish, #{topic := <<"t/1">>, payload := <<"msg2">>}}),

    %% ====================
    %% Client Unsubscribe
    ?assertMatch(
        {ok, {Http204, _, _}},
        PostFun(post, PathFun(["unsubscribe"]), UnSubBodyFun(SharedT))
    ),
    ?assertMatch(
        {ok, {Http204, _, _}},
        PostFun(
            post,
            PathFun(["unsubscribe", "bulk"]),
            [UnSubBodyFun(T) || T <- [SharedT, NonSharedT]]
        )
    ),

    %% assert subscription
    ?assertEqual([], ets:tab2list(?SUBSCRIPTION)),
    ?assertEqual([], ets:tab2list(?SUBOPTION)),
    ?assertEqual([], ets:tab2list(emqx_shared_subscription)),

    %% assert subscription virtual
    _ = emqtt:publish(PC, <<"testtopic">>, <<"msg3">>, [{qos, 0}]),
    _ = emqtt:publish(PC, <<"t/1">>, <<"msg4">>, [{qos, 0}]),
    ?assertNotReceive({publish, #{topic := <<"testtopic">>, payload := <<"msg3">>}}),
    ?assertNotReceive({publish, #{topic := <<"t/1">>, payload := <<"msg4">>}}).

t_subscribe_shared_topic_nl(_Config) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Http400 = {"HTTP/1.1", 400, "Bad Request"},
    Body =
        "{\"code\":\"INVALID_PARAMETER\","
        "\"message\":\"Invalid Subscribe options: `no_local` not allowed for shared-sub. See [MQTT-3.8.3-4]\"}",
    ClientId = <<"client_subscribe_shared">>,

    {ok, C} = emqtt:start_link(#{clientid => ClientId}),
    {ok, _} = emqtt:connect(C),

    PathFun = fun(Suffix) ->
        emqx_mgmt_api_test_util:api_path(["clients", "client_subscribe_shared"] ++ Suffix)
    end,
    PostFun = fun(Method, Path, Data) ->
        emqx_mgmt_api_test_util:request_api(
            Method, Path, "", AuthHeader, Data, #{return_all => true}
        )
    end,
    T = <<"$share/group/testtopic">>,
    ?assertMatch(
        {error, {Http400, _, Body}},
        PostFun(post, PathFun(["subscribe"]), #{topic => T, qos => 1, nl => 1, rh => 1})
    ).

t_list_clients_v2(Config) ->
    [N1, N2] = ?config(nodes, Config),
    APIPort = 18084,
    Port1 = get_mqtt_port(N1, tcp),
    Port2 = get_mqtt_port(N2, tcp),

    ?check_trace(
        begin
            ClientId1 = <<"ca1">>,
            ClientId2 = <<"c2">>,
            ClientId3 = <<"c3">>,
            ClientId4 = <<"ca4">>,
            ClientId5 = <<"ca5">>,
            ClientId6 = <<"c6">>,
            AllClientIds = [
                ClientId1,
                ClientId2,
                ClientId3,
                ClientId4,
                ClientId5,
                ClientId6
            ],
            C1 = connect_client(#{port => Port1, clientid => ClientId1, clean_start => true}),
            C2 = connect_client(#{port => Port2, clientid => ClientId2, clean_start => true}),
            C3 = connect_client(#{port => Port1, clientid => ClientId3, clean_start => true}),
            C4 = connect_client(#{port => Port2, clientid => ClientId4, clean_start => true}),
            %% in-memory clients
            C5 = connect_client(#{
                port => Port1, clientid => ClientId5, expiry => 0, clean_start => true
            }),
            C6 = connect_client(#{
                port => Port2, clientid => ClientId6, expiry => 0, clean_start => true
            }),
            %% offline persistent clients
            ok = emqtt:stop(C3),
            ok = emqtt:stop(C4),

            %% one by one
            QueryParams1 = #{limit => "1"},
            Res1 = list_all_v2(APIPort, QueryParams1),
            ?assertMatch(
                [
                    #{
                        <<"data">> := [_],
                        <<"meta">> :=
                            #{
                                <<"hasnext">> := true,
                                <<"count">> := 1,
                                <<"cursor">> := _
                            }
                    },
                    #{
                        <<"data">> := [_],
                        <<"meta">> :=
                            #{
                                <<"hasnext">> := true,
                                <<"count">> := 1,
                                <<"cursor">> := _
                            }
                    },
                    #{
                        <<"data">> := [_],
                        <<"meta">> :=
                            #{
                                <<"hasnext">> := true,
                                <<"count">> := 1,
                                <<"cursor">> := _
                            }
                    },
                    #{
                        <<"data">> := [_],
                        <<"meta">> :=
                            #{
                                <<"hasnext">> := true,
                                <<"count">> := 1,
                                <<"cursor">> := _
                            }
                    },
                    #{
                        <<"data">> := [_],
                        <<"meta">> :=
                            #{
                                <<"hasnext">> := true,
                                <<"count">> := 1,
                                <<"cursor">> := _
                            }
                    },
                    #{
                        <<"data">> := [_],
                        <<"meta">> :=
                            #{
                                <<"hasnext">> := false,
                                <<"count">> := 1
                            }
                    }
                ],
                Res1
            ),
            assert_contains_clientids(Res1, AllClientIds),

            %% Reusing the same cursors yield the same pages
            traverse_in_reverse_v2(APIPort, QueryParams1, Res1),

            %% paging
            QueryParams2 = #{limit => "4"},
            Res2 = list_all_v2(APIPort, QueryParams2),
            ?assertMatch(
                [
                    #{
                        <<"data">> := [_, _, _, _],
                        <<"meta">> :=
                            #{
                                <<"hasnext">> := true,
                                <<"count">> := 4,
                                <<"cursor">> := _
                            }
                    },
                    #{
                        <<"data">> := [_, _],
                        <<"meta">> :=
                            #{
                                <<"hasnext">> := false,
                                <<"count">> := 2
                            }
                    }
                ],
                Res2
            ),
            assert_contains_clientids(Res2, AllClientIds),
            traverse_in_reverse_v2(APIPort, QueryParams2, Res2),

            QueryParams3 = #{limit => "2"},
            Res3 = list_all_v2(APIPort, QueryParams3),
            ?assertMatch(
                [
                    #{
                        <<"data">> := [_, _],
                        <<"meta">> :=
                            #{
                                <<"hasnext">> := true,
                                <<"count">> := 2,
                                <<"cursor">> := _
                            }
                    },
                    #{
                        <<"data">> := [_, _],
                        <<"meta">> :=
                            #{
                                <<"hasnext">> := true,
                                <<"count">> := 2,
                                <<"cursor">> := _
                            }
                    },
                    #{
                        <<"data">> := [_, _],
                        <<"meta">> :=
                            #{
                                <<"hasnext">> := false,
                                <<"count">> := 2
                            }
                    }
                ],
                Res3
            ),
            assert_contains_clientids(Res3, AllClientIds),
            traverse_in_reverse_v2(APIPort, QueryParams3, Res3),

            %% fuzzy filters
            QueryParams4 = #{limit => "100", like_clientid => "ca"},
            Res4 = list_all_v2(APIPort, QueryParams4),
            ?assertMatch(
                [
                    #{
                        <<"data">> := [_, _, _],
                        <<"meta">> :=
                            #{
                                <<"hasnext">> := false,
                                <<"count">> := 3
                            }
                    }
                ],
                Res4
            ),
            assert_contains_clientids(Res4, [ClientId1, ClientId4, ClientId5]),
            traverse_in_reverse_v2(APIPort, QueryParams4, Res4),
            QueryParams5 = #{limit => "1", like_clientid => "ca"},
            Res5 = list_all_v2(APIPort, QueryParams5),
            ?assertMatch(
                [
                    #{
                        <<"data">> := [_],
                        <<"meta">> :=
                            #{
                                <<"hasnext">> := true,
                                <<"count">> := 1,
                                <<"cursor">> := _
                            }
                    },
                    #{
                        <<"data">> := [_],
                        <<"meta">> :=
                            #{
                                <<"hasnext">> := true,
                                <<"count">> := 1,
                                <<"cursor">> := _
                            }
                    },
                    #{
                        <<"data">> := [_],
                        <<"meta">> :=
                            #{
                                <<"hasnext">> := false,
                                <<"count">> := 1
                            }
                    }
                ],
                Res5
            ),
            assert_contains_clientids(Res5, [ClientId1, ClientId4, ClientId5]),
            traverse_in_reverse_v2(APIPort, QueryParams5, Res5),

            lists:foreach(
                fun(C) ->
                    {_, {ok, _}} =
                        ?wait_async_action(
                            emqtt:stop(C),
                            #{?snk_kind := emqx_cm_clean_down}
                        )
                end,
                [C1, C2, C5, C6]
            ),

            %% Verify that a malicious cursor that could generate an atom on the node is
            %% rejected
            EvilAtomBin0 = <<131, 100, 0, 5, "some_atom_that_doesnt_exist_on_the_remote_node">>,
            EvilAtomBin = emqx_base62:encode(EvilAtomBin0),

            ?assertMatch(
                {error, {{_, 400, _}, _, #{<<"message">> := <<"bad cursor">>}}},
                list_v2_request(APIPort, #{limit => "1", cursor => EvilAtomBin})
            ),
            %% Verify that the atom was not created
            erpc:call(N1, fun() ->
                ?assertError(badarg, binary_to_term(EvilAtomBin0, [safe]))
            end),
            ?assert(is_atom(binary_to_term(EvilAtomBin0))),

            lists:foreach(
                fun(ClientId) ->
                    ok = erpc:call(N1, emqx_persistent_session_ds, destroy_session, [ClientId])
                end,
                AllClientIds
            ),

            ok
        end,
        []
    ),
    ok.

t_cursor_serde_prop(_Config) ->
    ?assert(proper:quickcheck(cursor_serde_prop(), [{numtests, 100}, {to_file, user}])).

cursor_serde_prop() ->
    ?FORALL(
        NumNodes,
        range(1, 10),
        ?FORALL(
            Cursor,
            list_clients_cursor_gen(NumNodes),
            begin
                Nodes = lists:seq(1, NumNodes),
                Bin = emqx_mgmt_api_clients:serialize_cursor(Cursor),
                Res = emqx_mgmt_api_clients:parse_cursor(Bin, Nodes),
                ?WHENFAIL(
                    ct:pal("original:\n  ~p\nroundtrip:\n  ~p", [Cursor, Res]),
                    {ok, Cursor} =:= Res
                )
            end
        )
    ).

list_clients_cursor_gen(NumNodes) ->
    oneof([
        lists_clients_ets_cursor_gen(NumNodes),
        lists_clients_ds_cursor_gen()
    ]).

-define(CURSOR_TYPE_ETS, 1).
-define(CURSOR_TYPE_DS, 2).

lists_clients_ets_cursor_gen(NumNodes) ->
    ?LET(
        {NodeIdx, Cont},
        {range(1, NumNodes), oneof([undefined, tuple()])},
        #{
            type => ?CURSOR_TYPE_ETS,
            node => NodeIdx,
            node_idx => NodeIdx,
            cont => Cont
        }
    ).

lists_clients_ds_cursor_gen() ->
    ?LET(
        Iter,
        oneof(['$end_of_table', list(term())]),
        #{
            type => ?CURSOR_TYPE_DS,
            iterator => Iter
        }
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

get_subscriptions_request(APIPort, ClientId) ->
    Host = "http://127.0.0.1:" ++ integer_to_list(APIPort),
    Path = emqx_mgmt_api_test_util:api_path(Host, ["clients", ClientId, "subscriptions"]),
    request(get, Path, []).

get_client_request(Port, ClientId) ->
    Host = "http://127.0.0.1:" ++ integer_to_list(Port),
    Path = emqx_mgmt_api_test_util:api_path(Host, ["clients", ClientId]),
    request(get, Path, []).

list_request(Port) ->
    list_request(Port, _QueryParams = "").

list_request(Port, QueryParams) ->
    Host = "http://127.0.0.1:" ++ integer_to_list(Port),
    Path = emqx_mgmt_api_test_util:api_path(Host, ["clients"]),
    request(get, Path, [], QueryParams).

list_v2_request(Port, QueryParams = #{}) ->
    Host = "http://127.0.0.1:" ++ integer_to_list(Port),
    Path = emqx_mgmt_api_test_util:api_path(Host, ["clients_v2"]),
    QS = uri_string:compose_query(maps:to_list(emqx_utils_maps:binary_key_map(QueryParams))),
    request(get, Path, [], QS).

list_all_v2(Port, QueryParams = #{}) ->
    do_list_all_v2(Port, QueryParams, _Acc = []).

do_list_all_v2(Port, QueryParams, Acc) ->
    case list_v2_request(Port, QueryParams) of
        {ok, {{_, 200, _}, _, Resp = #{<<"meta">> := #{<<"cursor">> := Cursor}}}} ->
            do_list_all_v2(Port, QueryParams#{cursor => Cursor}, [Resp | Acc]);
        {ok, {{_, 200, _}, _, Resp = #{<<"meta">> := #{<<"hasnext">> := false}}}} ->
            lists:reverse([Resp | Acc]);
        Other ->
            error(
                {unexpected_response, #{
                    acc_so_far => Acc,
                    response => Other,
                    query_params => QueryParams
                }}
            )
    end.

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
    ?assertMatch(
        {ok,
            {{_, 200, _}, _, #{
                <<"connected">> := IsConnected,
                <<"is_persistent">> := true,
                %% contains statistics from disconnect time
                <<"recv_pkt">> := _,
                %% contains channel info from disconnect time
                <<"listener">> := _,
                <<"clean_start">> := _
            }}},
        get_client_request(APIPort, ClientId)
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

assert_contains_clientids(Results, ExpectedClientIds) ->
    ContainedClientIds = [
        ClientId
     || #{<<"data">> := Rows} <- Results,
        #{<<"clientid">> := ClientId} <- Rows
    ],
    ?assertEqual(
        lists:sort(ExpectedClientIds),
        lists:sort(ContainedClientIds),
        #{results => Results}
    ).

traverse_in_reverse_v2(APIPort, QueryParams0, Results) ->
    Cursors0 =
        lists:map(
            fun(#{<<"meta">> := Meta}) ->
                maps:get(<<"cursor">>, Meta, <<"wontbeused">>)
            end,
            Results
        ),
    Cursors1 = [<<"none">> | lists:droplast(Cursors0)],
    DirectOrderClientIds = [
        ClientId
     || #{<<"data">> := Rows} <- Results,
        #{<<"clientid">> := ClientId} <- Rows
    ],
    ReverseCursors = lists:reverse(Cursors1),
    do_traverse_in_reverse_v2(
        APIPort, QueryParams0, ReverseCursors, DirectOrderClientIds, _Acc = []
    ).

do_traverse_in_reverse_v2(_APIPort, _QueryParams0, _Cursors = [], DirectOrderClientIds, Acc) ->
    ?assertEqual(DirectOrderClientIds, Acc);
do_traverse_in_reverse_v2(APIPort, QueryParams0, [Cursor | Rest], DirectOrderClientIds, Acc) ->
    QueryParams = QueryParams0#{cursor => Cursor},
    Res0 = list_v2_request(APIPort, QueryParams),
    ?assertMatch({ok, {{_, 200, _}, _, #{<<"data">> := _}}}, Res0),
    {ok, {{_, 200, _}, _, #{<<"data">> := Rows}}} = Res0,
    ClientIds = [ClientId || #{<<"clientid">> := ClientId} <- Rows],
    do_traverse_in_reverse_v2(APIPort, QueryParams0, Rest, DirectOrderClientIds, ClientIds ++ Acc).

disconnect_and_destroy_session(Client) ->
    ok = emqtt:disconnect(Client, ?RC_SUCCESS, #{'Session-Expiry-Interval' => 0}).
