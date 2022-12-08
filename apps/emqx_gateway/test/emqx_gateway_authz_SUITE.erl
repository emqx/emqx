%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_authz_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_gateway_auth_ct, [init_gateway_conf/0, with_resource/3]).

-define(checkMatch(Guard),
    (fun(Expr) ->
        case (Expr) of
            Guard ->
                ok;
            X__V ->
                erlang:error(
                    {assertMatch, [
                        {module, ?MODULE},
                        {line, ?LINE},
                        {expression, (??Expr)},
                        {pattern, (??Guard)},
                        {value, X__V}
                    ]}
                )
        end
    end)
).
-define(FUNCTOR(Expr), fun() -> Expr end).
-define(FUNCTOR(Arg, Expr), fun(Arg) -> Expr end).

-define(AUTHNS, [authz_http]).

all() ->
    emqx_gateway_auth_ct:group_names(?AUTHNS).

groups() ->
    emqx_gateway_auth_ct:init_groups(?MODULE, ?AUTHNS).

init_per_group(AuthName, Conf) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    ok = emqx_authz_test_lib:reset_authorizers(),
    emqx_gateway_auth_ct:start_auth(AuthName),
    timer:sleep(500),
    Conf.

end_per_group(AuthName, Conf) ->
    emqx_gateway_auth_ct:stop_auth(AuthName),
    Conf.

init_per_suite(Config) ->
    emqx_config:erase(gateway),
    init_gateway_conf(),
    meck:new(emqx_authz_file, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_authz_file, create, fun(S) -> S end),
    emqx_mgmt_api_test_util:init_suite([emqx_conf, emqx_authz, emqx_authn, emqx_gateway]),
    application:ensure_all_started(cowboy),
    emqx_gateway_auth_ct:start(),
    Config.

end_per_suite(Config) ->
    meck:unload(emqx_authz_file),
    emqx_gateway_auth_ct:stop(),
    ok = emqx_authz_test_lib:restore_authorizers(),
    emqx_config:erase(gateway),
    emqx_mgmt_api_test_util:end_suite([emqx_gateway, emqx_authn, emqx_authz, emqx_conf]),
    Config.

init_per_testcase(_Case, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    Config.

end_per_testcase(_Case, Config) ->
    Config.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_case_coap_publish(_) ->
    Mod = emqx_coap_SUITE,
    Prefix = Mod:ps_prefix(),
    Fun = fun(Channel, Token, Topic, Checker) ->
        TopicStr = binary_to_list(Topic),
        URI = Prefix ++ "/" ++ TopicStr ++ "?clientid=client1&token=" ++ Token,

        Req = Mod:make_req(post, <<>>),
        Checker(Mod:do_request(Channel, URI, Req))
    end,
    Case = fun(Channel, Token) ->
        Fun(Channel, Token, <<"/publish">>, ?checkMatch({ok, changed, _})),
        Fun(Channel, Token, <<"/badpublish">>, ?checkMatch({error, uauthorized}))
    end,
    Mod:with_connection(Case).

t_case_coap_subscribe(_) ->
    Mod = emqx_coap_SUITE,
    Prefix = Mod:ps_prefix(),
    Fun = fun(Channel, Token, Topic, Checker) ->
        TopicStr = binary_to_list(Topic),
        URI = Prefix ++ "/" ++ TopicStr ++ "?clientid=client1&token=" ++ Token,

        Req = Mod:make_req(get, <<>>, [{observe, 0}]),
        Checker(Mod:do_request(Channel, URI, Req))
    end,
    Case = fun(Channel, Token) ->
        Fun(Channel, Token, <<"/subscribe">>, ?checkMatch({ok, content, _})),
        Fun(Channel, Token, <<"/badsubscribe">>, ?checkMatch({error, uauthorized}))
    end,
    Mod:with_connection(Case).

-record(coap_content, {content_format, payload = <<>>}).

t_case_lwm2m(_) ->
    MsgId = 12,
    Mod = emqx_lwm2m_SUITE,
    Epn = "urn:oma:lwm2m:oma:3",
    Port = emqx_lwm2m_SUITE:default_port(),
    URI = "coap://127.0.0.1:~b/rd?ep=~ts&imei=~ts&lt=345&lwm2m=1&password=public",
    Test = fun(Username, Checker) ->
        SubTopic = list_to_binary("lwm2m/" ++ Username ++ "/dn/#"),
        ReportTopic = list_to_binary("lwm2m/" ++ Username ++ "/up/resp"),
        with_resource(
            ?FUNCTOR(gen_udp:open(0, [binary, {active, false}])),
            ?FUNCTOR(Socket, gen_udp:close(Socket)),
            fun(Socket) ->
                Send = fun() ->
                    Mod:test_send_coap_request(
                        Socket,
                        post,
                        Mod:sprintf(URI, [Port, Epn, Username]),
                        #coap_content{
                            content_format = <<"text/plain">>,
                            payload = <<"</1>, </2>, </3>, </4>, </5>">>
                        },
                        [],
                        MsgId
                    ),

                    LoginResult = Mod:test_recv_coap_response(Socket),
                    ?assertEqual(ack, emqx_coap_SUITE:get_field(type, LoginResult)),
                    ?assertEqual({ok, created}, emqx_coap_SUITE:get_field(method, LoginResult))
                end,
                try_publish_recv(ReportTopic, Send, fun(Data) -> Checker(SubTopic, Data) end)
            end
        )
    end,
    Test("lwm2m", fun(SubTopic, Msg) ->
        ?assertEqual(true, lists:member(SubTopic, test_mqtt_broker:get_subscrbied_topics())),
        Payload = emqx_message:payload(Msg),
        Cmd = emqx_json:decode(Payload, [return_maps]),
        ?assertMatch(#{<<"msgType">> := <<"register">>, <<"data">> := _}, Cmd)
    end),

    Test("/baduser", fun(SubTopic, Data) ->
        ?assertEqual(false, lists:member(SubTopic, test_mqtt_broker:get_subscrbied_topics())),
        ?assertEqual(timeout, Data)
    end),
    ok.

-define(SN_CONNACK, 16#05).
t_case_sn_publish(_) ->
    set_sn_predefined_topic(),
    Mod = emqx_sn_protocol_SUITE,
    Payload = <<"publish with authz">>,
    Publish = fun(TopicId, Topic, Checker) ->
        with_resource(
            ?FUNCTOR(gen_udp:open(0, [binary])),
            ?FUNCTOR(Socket, gen_udp:close(Socket)),
            fun(Socket) ->
                Mod:send_connect_msg(Socket, <<"client_id_test1">>),
                ?assertEqual(<<3, ?SN_CONNACK, 0>>, Mod:receive_response(Socket)),

                Send = fun() ->
                    Mod:send_publish_msg_normal_topic(Socket, 0, 1, TopicId, Payload)
                end,
                try_publish_recv(Topic, Send, Checker)
            end
        )
    end,
    Publish(1, <<"/publish">>, fun(Msg) -> ?assertMatch(Payload, emqx_message:payload(Msg)) end),
    Publish(2, <<"/badpublish">>, fun(Msg) -> ?assertEqual(timeout, Msg) end),
    ok.

t_case_sn_subscribe(_) ->
    set_sn_predefined_topic(),
    Mod = emqx_sn_protocol_SUITE,
    Payload = <<"subscribe with authz">>,
    Sub = fun(Topic, Checker) ->
        with_resource(
            ?FUNCTOR(gen_udp:open(0, [binary])),
            ?FUNCTOR(Socket, gen_udp:close(Socket)),
            fun(Socket) ->
                Mod:send_connect_msg(Socket, <<"client_id_test1">>),
                ?assertEqual(<<3, ?SN_CONNACK, 0>>, Mod:receive_response(Socket)),

                Mod:send_subscribe_msg_normal_topic(Socket, 0, Topic, 1),
                _ = Mod:receive_response(Socket),

                timer:sleep(100),
                Msg = emqx_message:make(Topic, Payload),
                emqx:publish(Msg),

                timer:sleep(100),

                Recv = Mod:receive_response(Socket),
                Checker(Recv)
            end
        )
    end,
    Sub(<<"/subscribe">>, fun(Data) ->
        {ok, Msg, _, _} = emqx_sn_frame:parse(Data, undefined),
        ?assertMatch({mqtt_sn_message, _, {_, 3, 0, Payload}}, Msg)
    end),
    Sub(<<"/badsubscribe">>, fun(Data) ->
        ?assertEqual(udp_receive_timeout, Data)
    end),
    ok.

%% t_case_stomp_publish(_) ->
%%     Mod = emqx_stomp_SUITE,
%%     Payload = <<"publish with authz">>,
%%     Publish = fun(Topic, Checker) ->
%%         Fun = fun(Sock) ->
%%             gen_tcp:send(
%%                 Sock,
%%                 Mod:serialize(
%%                     <<"CONNECT">>,
%%                     [
%%                         {<<"accept-version">>, Mod:stomp_ver()},
%%                         {<<"host">>, <<"127.0.0.1:61613">>},
%%                         {<<"login">>, <<"guest">>},
%%                         {<<"passcode">>, <<"guest">>},
%%                         {<<"heart-beat">>, <<"0,0">>}
%%                     ]
%%                 )
%%             ),
%%             {ok, Data} = gen_tcp:recv(Sock, 0),
%%             {ok, Frame, _, _} = Mod:parse(Data),
%%             ?assertEqual(<<"CONNECTED">>, Mod:get_field(command, Frame)),
%%             Send = fun() ->
%%                 gen_tcp:send(
%%                     Sock,
%%                     Mod:serialize(
%%                         <<"SEND">>,
%%                         [{<<"destination">>, Topic}],
%%                         Payload
%%                     )
%%                 )
%%             end,
%%             try_publish_recv(Topic, Send, Checker)
%%         end,
%%         Mod:with_connection(Fun)
%%     end,
%%     Publish(<<"/publish">>, fun(Msg) -> ?assertMatch(Payload, emqx_message:payload(Msg)) end),
%%     Publish(<<"/badpublish">>, fun(Msg) -> ?assertEqual(timeout, Msg) end),
%%     ok.

%% t_case_stomp_subscribe(_) ->
%%     Mod = emqx_stomp_SUITE,
%%     Payload = <<"subscribe with authz">>,
%%     Sub = fun(Topic, Checker) ->
%%         Fun = fun(Sock) ->
%%             gen_tcp:send(
%%                 Sock,
%%                 Mod:serialize(
%%                     <<"CONNECT">>,
%%                     [
%%                         {<<"accept-version">>, Mod:stomp_ver()},
%%                         {<<"host">>, <<"127.0.0.1:61613">>},
%%                         {<<"login">>, <<"guest">>},
%%                         {<<"passcode">>, <<"guest">>},
%%                         {<<"heart-beat">>, <<"0,0">>}
%%                     ]
%%                 )
%%             ),
%%             {ok, Data} = gen_tcp:recv(Sock, 0),
%%             {ok, Frame, _, _} = Mod:parse(Data),
%%             ?assertEqual(<<"CONNECTED">>, Mod:get_field(command, Frame)),

%%             %% Subscribe
%%             gen_tcp:send(
%%                 Sock,
%%                 Mod:serialize(
%%                     <<"SUBSCRIBE">>,
%%                     [
%%                         {<<"id">>, 0},
%%                         {<<"destination">>, Topic},
%%                         {<<"ack">>, <<"auto">>}
%%                     ]
%%                 )
%%             ),

%%             timer:sleep(200),
%%             Msg = emqx_message:make(Topic, Payload),
%%             emqx:publish(Msg),

%%             timer:sleep(200),
%%             {ok, Data1} = gen_tcp:recv(Sock, 0, 10000),
%%             {ok, Frame1, _, _} = Mod:parse(Data1),
%%             Checker(Frame1)
%%         end,
%%         Mod:with_connection(Fun)
%%     end,
%%     Sub(<<"/subscribe">>, fun(Frame) ->
%%         ?assertMatch(<<"MESSAGE">>, Mod:get_field(command, Frame)),
%%         ?assertMatch(Payload, Mod:get_field(body, Frame))
%%     end),
%%     Sub(<<"/badsubscribe">>, fun(Frame) ->
%%         ?assertMatch(<<"ERROR">>, Mod:get_field(command, Frame)),
%%         ?assertMatch(<<"ACL Deny">>, Mod:get_field(body, Frame))
%%     end),
%%     ok.

t_case_exproto_publish(_) ->
    Mod = emqx_exproto_SUITE,
    SvrMod = emqx_exproto_echo_svr,
    Svrs = SvrMod:start(),
    Payload = <<"publish with authz">>,
    Publish = fun(Topic, Checker) ->
        with_resource(
            ?FUNCTOR(Mod:open(tcp)),
            ?FUNCTOR(Sock, Mod:close(Sock)),
            fun(Sock) ->
                Client = #{
                    proto_name => <<"demo">>,
                    proto_ver => <<"v0.1">>,
                    clientid => <<"test_client_1">>,
                    username => <<"admin">>
                },

                ConnBin = SvrMod:frame_connect(Client, <<"public">>),

                Mod:send(Sock, ConnBin),
                {ok, Recv} = Mod:recv(Sock, 5000),
                C = ?FUNCTOR(Bin, emqx_json:decode(Bin, [return_maps])),
                ?assertEqual(C(SvrMod:frame_connack(0)), C(Recv)),

                Send = fun() ->
                    PubBin = SvrMod:frame_publish(Topic, 0, Payload),
                    Mod:send(Sock, PubBin)
                end,
                try_publish_recv(Topic, Send, Checker)
            end
        )
    end,
    Publish(<<"/publish">>, fun(Msg) -> ?assertMatch(Payload, emqx_message:payload(Msg)) end),
    Publish(<<"/badpublish">>, fun(Msg) -> ?assertEqual(timeout, Msg) end),
    SvrMod:stop(Svrs),
    ok.

t_case_exproto_subscribe(_) ->
    Mod = emqx_exproto_SUITE,
    SvrMod = emqx_exproto_echo_svr,
    Svrs = SvrMod:start(),
    WaitTime = 5000,
    Sub = fun(Topic, ErrorCode) ->
        with_resource(
            ?FUNCTOR(Mod:open(tcp)),
            ?FUNCTOR(Sock, Mod:close(Sock)),
            fun(Sock) ->
                Client = #{
                    proto_name => <<"demo">>,
                    proto_ver => <<"v0.1">>,
                    clientid => <<"test_client_1">>,
                    username => <<"admin">>
                },

                ConnBin = SvrMod:frame_connect(Client, <<"public">>),

                Mod:send(Sock, ConnBin),
                {ok, Recv} = Mod:recv(Sock, WaitTime),
                C = ?FUNCTOR(Bin, emqx_json:decode(Bin, [return_maps])),
                ?assertEqual(C(SvrMod:frame_connack(0)), C(Recv)),

                SubBin = SvrMod:frame_subscribe(Topic, 0),
                Mod:send(Sock, SubBin),
                {ok, SubAckBin} = Mod:recv(Sock, WaitTime),
                ?assertEqual(SvrMod:frame_suback(ErrorCode), SubAckBin)
            end
        )
    end,

    Sub(<<"/subscribe">>, 0),
    Sub(<<"/badsubscribe">>, 1),
    SvrMod:stop(Svrs),
    ok.

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------
try_publish_recv(Topic, Publish, Checker) ->
    try_publish_recv(Topic, Publish, Checker, 500).

try_publish_recv(Topic, Publish, Checker, Timeout) ->
    emqx:subscribe(Topic),
    timer:sleep(200),
    Clear = fun(Msg) ->
        emqx:unsubscribe(Topic),
        Checker(Msg)
    end,
    Publish(),
    timer:sleep(200),
    receive
        {deliver, Topic, Msg} ->
            Clear(Msg)
    after Timeout ->
        Clear(timeout)
    end.

set_sn_predefined_topic() ->
    RawCfg = emqx_conf:get_raw([gateway, mqttsn], #{}),
    NewCfg = RawCfg#{
        <<"predefined">> => [
            #{
                id => 1,
                topic => "/publish"
            },
            #{
                id => 2,
                topic => "/badpublish"
            },
            #{
                id => 3,
                topic => "/subscribe"
            },
            #{
                id => 4,
                topic => "/badsubscribe"
            }
        ]
    },
    emqx_gateway_conf:update_gateway(mqttsn, NewCfg),
    ok.
