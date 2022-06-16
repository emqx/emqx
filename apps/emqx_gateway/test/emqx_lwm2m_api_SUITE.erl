%%--------------------------------------------------------------------
%% Copyright (C) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_lwm2m_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(PORT, 5783).

-define(LOGT(Format, Args), ct:pal("TEST_SUITE: " ++ Format, Args)).

-include("src/lwm2m/include/emqx_lwm2m.hrl").
-include("src/coap/include/emqx_coap.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CONF_DEFAULT, <<
    "\n"
    "gateway.lwm2m {\n"
    "  xml_dir = \"../../lib/emqx_gateway/src/lwm2m/lwm2m_xml\"\n"
    "  lifetime_min = 100s\n"
    "  lifetime_max = 86400s\n"
    "  qmode_time_window = 200\n"
    "  auto_observe = false\n"
    "  mountpoint = \"lwm2m/${username}\"\n"
    "  update_msg_publish_condition = contains_object_list\n"
    "  translators {\n"
    "    command = {topic = \"/dn/#\", qos = 0}\n"
    "    response = {topic = \"/up/resp\", qos = 0}\n"
    "    notify = {topic = \"/up/notify\", qos = 0}\n"
    "    register = {topic = \"/up/resp\", qos = 0}\n"
    "    update = {topic = \"/up/resp\", qos = 0}\n"
    "  }\n"
    "  listeners.udp.default {\n"
    "    bind = 5783\n"
    "  }\n"
    "}\n"
>>).

-define(assertExists(Map, Key),
    ?assertNotEqual(maps:get(Key, Map, undefined), undefined)
).

-record(coap_content, {content_format, payload = <<>>}).

-import(emqx_lwm2m_SUITE, [
    request/4,
    response/3,
    test_send_coap_response/7,
    test_recv_coap_request/1,
    test_recv_coap_response/1,
    test_send_coap_request/6,
    test_recv_mqtt_response/1,
    std_register/5,
    reslove_uri/1,
    split_path/1,
    split_query/1,
    join_path/2,
    sprintf/2
]).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:load_config(emqx_gateway_schema, ?CONF_DEFAULT),
    application:load(emqx_gateway),
    emqx_mgmt_api_test_util:init_suite([emqx_conf, emqx_authn]),
    Config.

end_per_suite(Config) ->
    timer:sleep(300),
    {ok, _} = emqx_conf:remove([<<"gateway">>, <<"lwm2m">>], #{}),
    emqx_mgmt_api_test_util:end_suite([emqx_authn, emqx_conf]),
    Config.

init_per_testcase(_AllTestCase, Config) ->
    ok = emqx_common_test_helpers:load_config(emqx_gateway_schema, ?CONF_DEFAULT),
    {ok, _} = application:ensure_all_started(emqx_gateway),
    {ok, ClientUdpSock} = gen_udp:open(0, [binary, {active, false}]),

    {ok, C} = emqtt:start_link([{host, "localhost"}, {port, 1883}, {clientid, <<"c1">>}]),
    {ok, _} = emqtt:connect(C),
    timer:sleep(100),

    [{sock, ClientUdpSock}, {emqx_c, C} | Config].

end_per_testcase(_AllTestCase, Config) ->
    gen_udp:close(?config(sock, Config)),
    emqtt:disconnect(?config(emqx_c, Config)),
    ok = application:stop(emqx_gateway),
    timer:sleep(300).

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------
t_lookup_read(Config) ->
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:1",
    MsgId1 = 15,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),
    %% step 1, device register ...
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=600&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<
                "</lwm2m>;rt=\"oma.lwm2m\";ct=11543,"
                "</lwm2m/1/0>,</lwm2m/2/0>,</lwm2m/3/0>"
            >>
        },
        [],
        MsgId1
    ),
    #coap_message{method = Method1} = test_recv_coap_response(UdpSock),
    ?assertEqual({ok, created}, Method1),

    timer:sleep(100),
    test_recv_mqtt_response(RespTopic),

    %% step2,  send a READ command to device
    CmdId = 206,
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{
            <<"path">> => <<"/3/0/0">>
        }
    },
    CommandJson = emqx_json:encode(Command),
    ?LOGT("CommandJson=~p", [CommandJson]),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),

    timer:sleep(200),
    no_received_request(Epn, <<"/3/0/0">>, <<"read">>),

    Request2 = test_recv_coap_request(UdpSock),
    ?LOGT("LwM2M client got ~p", [Request2]),
    timer:sleep(50),

    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{content_format = <<"text/plain">>, payload = <<"EMQ">>},
        Request2,
        true
    ),

    timer:sleep(200),
    normal_received_request(Epn, <<"/3/0/0">>, <<"read">>).

t_lookup_discover(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:2",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2,  send a WRITE command to device
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 307,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"discover">>,
        <<"data">> => #{
            <<"path">> => <<"/3/0/7">>
        }
    },
    CommandJson = emqx_json:encode(Command),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),

    timer:sleep(200),
    no_received_request(Epn, <<"/3/0/7">>, <<"discover">>),

    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    timer:sleep(50),

    PayloadDiscover = <<"</3/0/7>;dim=8;pmin=10;pmax=60;gt=50;lt=42.2,</3/0/8>">>,
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"application/link-format">>,
            payload = PayloadDiscover
        },
        Request2,
        true
    ),
    timer:sleep(200),
    discover_received_request(Epn, <<"/3/0/7">>, <<"discover">>).

t_read(Config) ->
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),
    %% step 1, device register ...
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=600&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<
                "</lwm2m>;rt=\"oma.lwm2m\";ct=11543,"
                "</lwm2m/1/0>,</lwm2m/2/0>,</lwm2m/3/0>"
            >>
        },
        [],
        MsgId1
    ),
    #coap_message{method = Method1} = test_recv_coap_response(UdpSock),
    ?assertEqual({ok, created}, Method1),

    timer:sleep(100),
    test_recv_mqtt_response(RespTopic),

    %% step2, call Read API
    call_send_api(Epn, "read", "path=/3/0/0"),
    timer:sleep(100),
    #coap_message{type = Type, method = Method, options = Opts} = test_recv_coap_request(UdpSock),
    ?assertEqual(con, Type),
    ?assertEqual(get, Method),
    ?assertEqual([<<"lwm2m">>, <<"3">>, <<"0">>, <<"0">>], maps:get(uri_path, Opts)).

t_write(Config) ->
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:4",
    MsgId1 = 15,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),
    %% step 1, device register ...
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=600&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<
                "</lwm2m>;rt=\"oma.lwm2m\";ct=11543,"
                "</lwm2m/1/0>,</lwm2m/2/0>,</lwm2m/3/0>"
            >>
        },
        [],
        MsgId1
    ),
    #coap_message{method = Method1} = test_recv_coap_response(UdpSock),
    ?assertEqual({ok, created}, Method1),

    timer:sleep(100),
    test_recv_mqtt_response(RespTopic),

    %% step2, call write API
    call_send_api(Epn, "write", "path=/3/0/13&type=Integer&value=123"),
    timer:sleep(100),
    #coap_message{type = Type, method = Method, options = Opts} = test_recv_coap_request(UdpSock),
    ?assertEqual(con, Type),
    ?assertEqual(put, Method),
    ?assertEqual([<<"lwm2m">>, <<"3">>, <<"0">>, <<"13">>], maps:get(uri_path, Opts)),
    ?assertEqual(<<"application/vnd.oma.lwm2m+tlv">>, maps:get(content_format, Opts)).

t_observe(Config) ->
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:5",
    MsgId1 = 15,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),
    %% step 1, device register ...
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=600&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<
                "</lwm2m>;rt=\"oma.lwm2m\";ct=11543,"
                "</lwm2m/1/0>,</lwm2m/2/0>,</lwm2m/3/0>"
            >>
        },
        [],
        MsgId1
    ),
    #coap_message{method = Method1} = test_recv_coap_response(UdpSock),
    ?assertEqual({ok, created}, Method1),

    timer:sleep(100),
    test_recv_mqtt_response(RespTopic),

    %% step2, call observe API
    call_send_api(Epn, "observe", "path=/3/0/1&enable=false"),
    timer:sleep(100),
    #coap_message{type = Type, method = Method, options = Opts} = test_recv_coap_request(UdpSock),
    ?assertEqual(con, Type),
    ?assertEqual(get, Method),
    ?assertEqual([<<"lwm2m">>, <<"3">>, <<"0">>, <<"1">>], maps:get(uri_path, Opts)),
    ?assertEqual(1, maps:get(observe, Opts)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
call_lookup_api(ClientId, Path, Action) ->
    ApiPath = emqx_mgmt_api_test_util:api_path(["gateway/lwm2m/clients", ClientId, "lookup"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Query = io_lib:format("path=~ts&action=~ts", [Path, Action]),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(get, ApiPath, Query, Auth),
    ?LOGT("rest api response:~ts~n", [Response]),
    Response.

call_send_api(ClientId, Cmd, Query) ->
    ApiPath = emqx_mgmt_api_test_util:api_path(["gateway/lwm2m/clients", ClientId, Cmd]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(post, ApiPath, Query, Auth),
    ?LOGT("rest api response:~ts~n", [Response]),
    Response.

no_received_request(ClientId, Path, Action) ->
    Response = call_lookup_api(ClientId, Path, Action),
    NotReceived = #{
        <<"clientid">> => list_to_binary(ClientId),
        <<"action">> => Action,
        <<"code">> => <<"6.01">>,
        <<"codeMsg">> => <<"reply_not_received">>,
        <<"path">> => Path
    },
    ?assertEqual(NotReceived, emqx_json:decode(Response, [return_maps])).
normal_received_request(ClientId, Path, Action) ->
    Response = call_lookup_api(ClientId, Path, Action),
    RCont = emqx_json:decode(Response, [return_maps]),
    ?assertEqual(list_to_binary(ClientId), maps:get(<<"clientid">>, RCont, undefined)),
    ?assertEqual(Path, maps:get(<<"path">>, RCont, undefined)),
    ?assertEqual(Action, maps:get(<<"action">>, RCont, undefined)),
    ?assertExists(RCont, <<"code">>),
    ?assertExists(RCont, <<"codeMsg">>),
    ?assertExists(RCont, <<"content">>),
    RCont.

discover_received_request(ClientId, Path, Action) ->
    RCont = normal_received_request(ClientId, Path, Action),
    [Res | _] = maps:get(<<"content">>, RCont),
    ?assertExists(Res, <<"path">>),
    ?assertExists(Res, <<"name">>),
    ?assertExists(Res, <<"operations">>).
