%%--------------------------------------------------------------------
%% Copyright (C) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx_gateway/src/lwm2m/include/emqx_lwm2m.hrl").
-include_lib("lwm2m_coap/include/coap.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CONF_DEFAULT, <<"
gateway.lwm2m {
  xml_dir = \"../../lib/emqx_gateway/src/lwm2m/lwm2m_xml\"
  lifetime_min = 1s
  lifetime_max = 86400s
  qmode_time_windonw = 22
  auto_observe = false
  mountpoint = \"lwm2m/%u\"
  update_msg_publish_condition = contains_object_list
  translators {
    command = {topic = \"/dn/#\", qos = 0}
    response = {topic = \"/up/resp\", qos = 0}
    notify = {topic = \"/up/notify\", qos = 0}
    register = {topic = \"/up/resp\", qos = 0}
    update = {topic = \"/up/resp\", qos = 0}
  }
  listeners.udp.default {
    bind = 5783
  }
}
">>).

-define(assertExists(Map, Key),
        ?assertNotEqual(maps:get(Key, Map, undefined), undefined)).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_config:init_load(emqx_gateway_schema, ?CONF_DEFAULT),
    emqx_mgmt_api_test_util:init_suite([emqx_gateway]),
    Config.

end_per_suite(Config) ->
    timer:sleep(300),
    emqx_mgmt_api_test_util:end_suite([emqx_gateway]),
    Config.

init_per_testcase(_AllTestCase, Config) ->
    ok = emqx_config:init_load(emqx_gateway_schema, ?CONF_DEFAULT),
    {ok, _} = application:ensure_all_started(emqx_gateway),
    {ok, ClientUdpSock} = gen_udp:open(0, [binary, {active, false}]),

    {ok, C} = emqtt:start_link([{host, "localhost"},{port, 1883},{clientid, <<"c1">>}]),
    {ok, _} = emqtt:connect(C),
    timer:sleep(100),

    [{sock, ClientUdpSock}, {emqx_c, C} | Config].

end_per_testcase(_AllTestCase, Config) ->
    timer:sleep(300),
    gen_udp:close(?config(sock, Config)),
    emqtt:disconnect(?config(emqx_c, Config)),
    ok = application:stop(emqx_gateway).

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------
t_lookup_cmd_read(Config) ->
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    RespTopic = list_to_binary("lwm2m/"++Epn++"/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),
    %% step 1, device register ...
    test_send_coap_request( UdpSock,
                            post,
                            sprintf("coap://127.0.0.1:~b/rd?ep=~s&lt=345&lwm2m=1", [?PORT, Epn]),
                            #coap_content{content_format = <<"text/plain">>,
                                          payload = <<"</lwm2m>;rt=\"oma.lwm2m\";ct=11543,</lwm2m/1/0>,</lwm2m/2/0>,</lwm2m/3/0>">>},
                            [],
                            MsgId1),
    #coap_message{method = Method1} = test_recv_coap_response(UdpSock),
    ?assertEqual({ok,created}, Method1),
    test_recv_mqtt_response(RespTopic),

    %% step2,  send a READ command to device
    CmdId = 206,
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    Command =   #{
                  <<"requestID">> => CmdId, <<"cacheID">> => CmdId,
                  <<"msgType">> => <<"read">>,
                  <<"data">> => #{
                                  <<"path">> => <<"/3/0/0">>
                                 }
                 },
    CommandJson = emqx_json:encode(Command),
    ?LOGT("CommandJson=~p", [CommandJson]),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),

    no_received_request(Epn, <<"/3/0/0">>, <<"read">>),

    Request2 = test_recv_coap_request(UdpSock),
    ?LOGT("LwM2M client got ~p", [Request2]),
    timer:sleep(50),

    test_send_coap_response(UdpSock, "127.0.0.1", ?PORT, {ok, content}, #coap_content{content_format = <<"text/plain">>, payload = <<"EMQ">>}, Request2, true),
    timer:sleep(100),

    normal_received_request(Epn, <<"/3/0/0">>, <<"read">>).

t_lookup_cmd_discover(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/"++Epn++"/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2,  send a WRITE command to device
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 307,
    Command = #{<<"requestID">> => CmdId, <<"cacheID">> => CmdId,
                <<"msgType">> => <<"discover">>,
                <<"data">> => #{
                                <<"path">> => <<"/3/0/7">>
                               } },
    CommandJson = emqx_json:encode(Command),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),

    no_received_request(Epn, <<"/3/0/7">>, <<"discover">>),

    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    timer:sleep(50),

    PayloadDiscover = <<"</3/0/7>;dim=8;pmin=10;pmax=60;gt=50;lt=42.2,</3/0/8>">>,
    test_send_coap_response(UdpSock,
                            "127.0.0.1",
                            ?PORT,
                            {ok, content},
                            #coap_content{content_format = <<"application/link-format">>, payload = PayloadDiscover},
                            Request2,
                            true),
    timer:sleep(100),
    discover_received_request(Epn, <<"/3/0/7">>, <<"discover">>).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
send_request(ClientId, Path, Action) ->
    ApiPath = emqx_mgmt_api_test_util:api_path(["gateway/lwm2m", ClientId, "lookup_cmd"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Query = io_lib:format("path=~s&action=~s", [Path, Action]),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(get, ApiPath, Query, Auth),
    ?LOGT("rest api response:~s~n", [Response]),
    Response.

no_received_request(ClientId, Path, Action) ->
    Response = send_request(ClientId, Path, Action),
    NotReceived = #{<<"clientid">> => list_to_binary(ClientId),
                    <<"action">> => Action,
                    <<"code">> => <<"6.01">>,
                    <<"codeMsg">> => <<"reply_not_received">>,
                    <<"path">> => Path},
    ?assertEqual(NotReceived, emqx_json:decode(Response, [return_maps])).
normal_received_request(ClientId, Path, Action) ->
    Response = send_request(ClientId, Path, Action),
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

test_recv_mqtt_response(RespTopic) ->
    receive
        {publish, #{topic := RespTopic, payload := RM}} ->
            ?LOGT("test_recv_mqtt_response Response=~p", [RM]),
            RM
    after 1000 -> timeout_test_recv_mqtt_response
    end.

test_send_coap_request(UdpSock, Method, Uri, Content, Options, MsgId) ->
    is_record(Content, coap_content) orelse error("Content must be a #coap_content!"),
    is_list(Options) orelse error("Options must be a list"),
    case resolve_uri(Uri) of
        {coap, {IpAddr, Port}, Path, Query} ->
            Request0 = lwm2m_coap_message:request(con, Method, Content, [{uri_path, Path}, {uri_query, Query} | Options]),
            Request = Request0#coap_message{id = MsgId},
            ?LOGT("send_coap_request Request=~p", [Request]),
            RequestBinary = lwm2m_coap_message_parser:encode(Request),
            ?LOGT("test udp socket send to ~p:~p, data=~p", [IpAddr, Port, RequestBinary]),
            ok = gen_udp:send(UdpSock, IpAddr, Port, RequestBinary);
        {SchemeDiff, ChIdDiff, _, _} ->
            error(lists:flatten(io_lib:format("scheme ~s or ChId ~s does not match with socket", [SchemeDiff, ChIdDiff])))
    end.

test_recv_coap_response(UdpSock) ->
    {ok, {Address, Port, Packet}} = gen_udp:recv(UdpSock, 0, 2000),
    Response = lwm2m_coap_message_parser:decode(Packet),
    ?LOGT("test udp receive from ~p:~p, data1=~p, Response=~p", [Address, Port, Packet, Response]),
    #coap_message{type = ack, method = Method, id=Id, token = Token, options = Options, payload = Payload} = Response,
    ?LOGT("receive coap response Method=~p, Id=~p, Token=~p, Options=~p, Payload=~p", [Method, Id, Token, Options, Payload]),
    Response.

test_recv_coap_request(UdpSock) ->
    case gen_udp:recv(UdpSock, 0, 2000) of
        {ok, {_Address, _Port, Packet}} ->
            Request = lwm2m_coap_message_parser:decode(Packet),
            #coap_message{type = con, method = Method, id=Id, token = Token, payload = Payload, options = Options} = Request,
            ?LOGT("receive coap request Method=~p, Id=~p, Token=~p, Options=~p, Payload=~p", [Method, Id, Token, Options, Payload]),
            Request;
        {error, Reason} ->
            ?LOGT("test_recv_coap_request failed, Reason=~p", [Reason]),
            timeout_test_recv_coap_request
    end.

test_send_coap_response(UdpSock, Host, Port, Code, Content, Request, Ack) ->
    is_record(Content, coap_content) orelse error("Content must be a #coap_content!"),
    is_list(Host) orelse error("Host is not a string"),

    {ok, IpAddr} = inet:getaddr(Host, inet),
    Response = lwm2m_coap_message:response(Code, Content, Request),
    Response2 = case Ack of
                    true -> Response#coap_message{type = ack};
                    false -> Response
                end,
    ?LOGT("test_send_coap_response Response=~p", [Response2]),
    ok = gen_udp:send(UdpSock, IpAddr, Port, lwm2m_coap_message_parser:encode(Response2)).

std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic) ->
    test_send_coap_request( UdpSock,
                            post,
                            sprintf("coap://127.0.0.1:~b/rd?ep=~s&lt=345&lwm2m=1", [?PORT, Epn]),
                            #coap_content{content_format = <<"text/plain">>, payload = ObjectList},
                            [],
                            MsgId1),
    #coap_message{method = {ok,created}} = test_recv_coap_response(UdpSock),
    test_recv_mqtt_response(RespTopic),
    timer:sleep(100).

resolve_uri(Uri) ->
    {ok, #{scheme := Scheme,
           host := Host,
           port := PortNo,
           path := Path} = URIMap} = emqx_http_lib:uri_parse(Uri),
    Query = maps:get(query, URIMap, ""),
    {ok, PeerIP} = inet:getaddr(Host, inet),
    {Scheme, {PeerIP, PortNo}, split_path(Path), split_query(Query)}.

split_path([]) -> [];
split_path([$/]) -> [];
split_path([$/ | Path]) -> split_segments(Path, $/, []).

split_query([]) -> [];
split_query(Path) -> split_segments(Path, $&, []).

split_segments(Path, Char, Acc) ->
    case string:rchr(Path, Char) of
        0 ->
            [make_segment(Path) | Acc];
        N when N > 0 ->
            split_segments(string:substr(Path, 1, N-1), Char,
                [make_segment(string:substr(Path, N+1)) | Acc])
    end.

make_segment(Seg) ->
    list_to_binary(emqx_http_lib:uri_decode(Seg)).

join_path([], Acc) -> Acc;
join_path([<<"/">>|T], Acc) ->
    join_path(T, Acc);
join_path([H|T], Acc) ->
    join_path(T, <<Acc/binary, $/, H/binary>>).

sprintf(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).
