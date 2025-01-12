%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_stomp_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").
-include("emqx_stomp.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-import(
    emqx_gateway_test_utils,
    [
        assert_fields_exist/2,
        request/2,
        request/3
    ]
).

-define(HEARTBEAT, <<$\n>>).

-define(CONF_DEFAULT, <<
    "\n"
    "gateway.stomp {\n"
    "  clientinfo_override {\n"
    "    username = \"${Packet.headers.login}\"\n"
    "    password = \"${Packet.headers.passcode}\"\n"
    "  }\n"
    " frame {\n"
    "    max_headers = 10\n"
    "    max_headers_length = 100\n"
    "    max_body_length = 1024\n"
    "  }\n"
    " listeners.tcp.default {\n"
    "    bind = 61613\n"
    "  }\n"
    "}\n"
>>).

all() -> emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    application:load(emqx_gateway_stomp),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, ?CONF_DEFAULT},
            emqx_gateway,
            emqx_auth,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    emqx_common_test_http:create_default_app(),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_common_test_http:delete_default_app(),
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    ok.

init_per_testcase(_TestCase, Config) ->
    snabbkaffe:start_trace(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    snabbkaffe:stop(),
    ok.

default_config() ->
    ?CONF_DEFAULT.

stomp_ver() ->
    ?STOMP_VER.

restart_stomp_with_mountpoint(Mountpoint) ->
    Conf = emqx:get_raw_config([gateway, stomp]),
    emqx_gateway_conf:update_gateway(
        stomp,
        Conf#{<<"mountpoint">> => Mountpoint}
    ).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_connect(_) ->
    %% Successful connect
    ConnectSucced = fun(Sock) ->
        emqx_gateway_test_utils:meck_emqx_hook_calls(),

        ok = send_connection_frame(Sock, <<"guest">>, <<"guest">>, <<"1000,2000">>),
        {ok, Frame} = recv_a_frame(Sock),
        ?assertMatch(<<"CONNECTED">>, Frame#stomp_frame.command),
        ?assertEqual(
            <<"2000,1000">>, proplists:get_value(<<"heart-beat">>, Frame#stomp_frame.headers)
        ),

        ?assertMatch(
            ['client.connect' | _],
            emqx_gateway_test_utils:collect_emqx_hooks_calls()
        ),

        ok = send_disconnect_frame(Sock, <<"12345">>),
        ?assertMatch(
            {ok, #stomp_frame{
                command = <<"RECEIPT">>,
                headers = [{<<"receipt-id">>, <<"12345">>}]
            }},
            recv_a_frame(Sock)
        )
    end,
    with_connection(ConnectSucced),

    %% Connect will be failed, because of bad version
    ProtocolError = fun(Sock) ->
        gen_tcp:send(
            Sock,
            serialize(
                <<"CONNECT">>,
                [
                    {<<"accept-version">>, <<"2.0,2.1">>},
                    {<<"host">>, <<"127.0.0.1:61613">>},
                    {<<"login">>, <<"guest">>},
                    {<<"passcode">>, <<"guest">>},
                    {<<"heart-beat">>, <<"1000,2000">>}
                ]
            )
        ),
        {ok, Data} = gen_tcp:recv(Sock, 0),
        {ok, Frame, _, _} = parse(Data),
        #stomp_frame{
            command = <<"ERROR">>,
            headers = _,
            body = <<"Login Failed: Supported protocol versions < 1.2">>
        } = Frame
    end,
    with_connection(ProtocolError).

t_auth_expire(_) ->
    ok = meck:new(emqx_access_control, [passthrough, no_history]),
    ok = meck:expect(
        emqx_access_control,
        authenticate,
        fun(_) ->
            {ok, #{is_superuser => false, expire_at => erlang:system_time(millisecond) + 500}}
        end
    ),

    ConnectWithExpire = fun(Sock) ->
        ?assertWaitEvent(
            begin
                ok = send_connection_frame(Sock, <<"guest">>, <<"guest">>, <<"1000,2000">>),
                {ok, Frame} = recv_a_frame(Sock),
                ?assertMatch(<<"CONNECTED">>, Frame#stomp_frame.command)
            end,
            #{
                ?snk_kind := conn_process_terminated,
                clientid := _,
                reason := {shutdown, expired}
            },
            5000
        )
    end,
    with_connection(ConnectWithExpire),
    meck:unload(emqx_access_control).

t_heartbeat(_) ->
    %% Test heart beat
    with_connection(fun(Sock) ->
        gen_tcp:send(
            Sock,
            serialize(
                <<"CONNECT">>,
                [
                    {<<"accept-version">>, ?STOMP_VER},
                    {<<"host">>, <<"127.0.0.1:61613">>},
                    {<<"login">>, <<"guest">>},
                    {<<"passcode">>, <<"guest">>},
                    {<<"heart-beat">>, <<"500,800">>}
                ]
            )
        ),
        {ok, Data} = gen_tcp:recv(Sock, 0),
        {ok,
            #stomp_frame{
                command = <<"CONNECTED">>,
                headers = _,
                body = _
            },
            _, _} = parse(Data),

        {ok, ?HEARTBEAT} = gen_tcp:recv(Sock, 0),
        %% Server will close the connection because never receive the heart beat from client
        {error, closed} = gen_tcp:recv(Sock, 0)
    end).

t_subscribe(_) ->
    with_connection(fun(Sock) ->
        ok = send_connection_frame(Sock, <<"guest">>, <<"guest">>),
        ?assertMatch({ok, #stomp_frame{command = <<"CONNECTED">>}}, recv_a_frame(Sock)),

        ok = send_subscribe_frame(Sock, 0, <<"/queue/foo">>),
        ?assertMatch({ok, #stomp_frame{command = <<"RECEIPT">>}}, recv_a_frame(Sock)),

        %% 'user-defined' header will be retain
        ok = send_message_frame(Sock, <<"/queue/foo">>, <<"hello">>, [
            {<<"user-defined">>, <<"emq">>},
            {<<"content-type">>, <<"text/html">>}
        ]),
        ?assertMatch({ok, #stomp_frame{command = <<"RECEIPT">>}}, recv_a_frame(Sock)),

        {ok, Frame} = recv_a_frame(Sock),
        ?assertEqual(
            <<"text/html">>, proplists:get_value(<<"content-type">>, Frame#stomp_frame.headers)
        ),

        ?assertMatch(
            #stomp_frame{
                command = <<"MESSAGE">>,
                headers = _,
                body = <<"hello">>
            },
            Frame
        ),
        lists:foreach(
            fun({Key, Val}) ->
                Val = proplists:get_value(Key, Frame#stomp_frame.headers)
            end,
            [
                {<<"destination">>, <<"/queue/foo">>},
                {<<"subscription">>, <<"0">>},
                {<<"user-defined">>, <<"emq">>}
            ]
        ),

        %% assert subscription stats
        [ClientInfo1] = clients(),
        ?assertMatch(#{subscriptions_cnt := 1}, ClientInfo1),

        %% Unsubscribe
        ok = send_unsubscribe_frame(Sock, 0),
        ?assertMatch({ok, #stomp_frame{command = <<"RECEIPT">>}}, recv_a_frame(Sock)),

        %% assert subscription stats
        [ClientInfo2] = clients(),
        ?assertMatch(#{subscriptions_cnt := 0}, ClientInfo2),

        ok = send_message_frame(Sock, <<"/queue/foo">>, <<"You will not receive this msg">>),
        ?assertMatch({ok, #stomp_frame{command = <<"RECEIPT">>}}, recv_a_frame(Sock)),

        {error, timeout} = gen_tcp:recv(Sock, 0, 500)
    end).

t_subscribe_inuse(_) ->
    UsedTopic = <<"/queue/foo">>,
    UsedSubId = <<"0">>,
    Setup =
        fun(Sock) ->
            ok = send_connection_frame(Sock, <<"guest">>, <<"guest">>),
            ?assertMatch({ok, #stomp_frame{command = <<"CONNECTED">>}}, recv_a_frame(Sock)),
            ok = send_subscribe_frame(Sock, UsedSubId, UsedTopic),
            ?assertMatch({ok, #stomp_frame{command = <<"RECEIPT">>}}, recv_a_frame(Sock))
        end,
    TopicIdInuse =
        fun(Sock) ->
            Setup(Sock),
            %% topic-id is in use
            ok = send_subscribe_frame(Sock, UsedSubId, <<"/queue/bar">>),

            {ok, ErrorFrame} = recv_a_frame(Sock),
            ?assertMatch(#stomp_frame{command = <<"ERROR">>}, ErrorFrame),
            ?assertEqual(<<"Subscription id 0 is in used">>, ErrorFrame#stomp_frame.body),
            ?assertMatch({error, closed}, gen_tcp:recv(Sock, 0))
        end,

    SubscriptionInuse =
        fun(Sock) ->
            Setup(Sock),
            %% topic is in use
            ok = send_subscribe_frame(Sock, 1, UsedTopic),

            {ok, ErrorFrame} = recv_a_frame(Sock),
            ?assertMatch(#stomp_frame{command = <<"ERROR">>}, ErrorFrame),
            ?assertEqual(<<"Topic /queue/foo already in subscribed">>, ErrorFrame#stomp_frame.body),
            ?assertMatch({error, closed}, gen_tcp:recv(Sock, 0))
        end,

    TopicIdInuseViaHttp =
        fun(Sock) ->
            Setup(Sock),
            %% assert subscription stats
            [#{clientid := ClientId}] = clients(),
            {error, ErrMsg} = create_subscription(ClientId, <<"/queue/bar">>, UsedSubId),
            ?assertEqual(<<"Subscription id 0 is in used">>, ErrMsg),

            ok = send_disconnect_frame(Sock)
        end,

    SubscriptionInuseViaHttp =
        fun(Sock) ->
            Setup(Sock),
            %% assert subscription stats
            [#{clientid := ClientId}] = clients(),
            {error, ErrMsg} = create_subscription(ClientId, UsedTopic, <<"1">>),
            ?assertEqual(<<"Topic /queue/foo already in subscribed">>, ErrMsg),

            ok = send_disconnect_frame(Sock)
        end,

    with_connection(TopicIdInuse),
    with_connection(SubscriptionInuse),
    with_connection(TopicIdInuseViaHttp),
    with_connection(SubscriptionInuseViaHttp).

t_receive_from_mqtt_publish(_) ->
    with_connection(fun(Sock) ->
        ok = send_connection_frame(Sock, <<"guest">>, <<"guest">>),
        ?assertMatch({ok, #stomp_frame{command = <<"CONNECTED">>}}, recv_a_frame(Sock)),

        ok = send_subscribe_frame(Sock, 0, <<"/queue/foo">>),
        ?assertMatch({ok, #stomp_frame{command = <<"RECEIPT">>}}, recv_a_frame(Sock)),

        %% send mqtt publish with content-type
        Msg = emqx_message:make(
            _From = from_testsuite,
            _QoS = 1,
            _Topic = <<"/queue/foo">>,
            _Payload = <<"hello">>,
            _Flags = #{},
            _Headers = #{properties => #{'Content-Type' => <<"application/json">>}}
        ),
        emqx:publish(Msg),

        {ok, Frame} = recv_a_frame(Sock),
        ?assertEqual(
            <<"application/json">>,
            proplists:get_value(<<"content-type">>, Frame#stomp_frame.headers)
        ),

        ?assertMatch(
            #stomp_frame{
                command = <<"MESSAGE">>,
                headers = _,
                body = <<"hello">>
            },
            Frame
        ),
        lists:foreach(
            fun({Key, Val}) ->
                Val = proplists:get_value(Key, Frame#stomp_frame.headers)
            end,
            [
                {<<"destination">>, <<"/queue/foo">>},
                {<<"subscription">>, <<"0">>}
            ]
        ),

        %% assert subscription stats
        [ClientInfo1] = clients(),
        ?assertMatch(#{subscriptions_cnt := 1}, ClientInfo1),

        %% Unsubscribe
        ok = send_unsubscribe_frame(Sock, 0),
        ?assertMatch({ok, #stomp_frame{command = <<"RECEIPT">>}}, recv_a_frame(Sock)),

        %% assert subscription stats
        [ClientInfo2] = clients(),
        ?assertMatch(#{subscriptions_cnt := 0}, ClientInfo2),

        ok = send_message_frame(Sock, <<"/queue/foo">>, <<"You will not receive this msg">>),
        ?assertMatch({ok, #stomp_frame{command = <<"RECEIPT">>}}, recv_a_frame(Sock)),

        {error, timeout} = gen_tcp:recv(Sock, 0, 500)
    end).

t_transaction(_) ->
    with_connection(fun(Sock) ->
        gen_tcp:send(
            Sock,
            serialize(
                <<"CONNECT">>,
                [
                    {<<"accept-version">>, ?STOMP_VER},
                    {<<"host">>, <<"127.0.0.1:61613">>},
                    {<<"login">>, <<"guest">>},
                    {<<"passcode">>, <<"guest">>},
                    {<<"heart-beat">>, <<"0,0">>}
                ]
            )
        ),
        {ok, Data} = gen_tcp:recv(Sock, 0),
        {ok,
            #stomp_frame{
                command = <<"CONNECTED">>,
                headers = _,
                body = _
            },
            _, _} = parse(Data),

        %% Subscribe
        gen_tcp:send(
            Sock,
            serialize(
                <<"SUBSCRIBE">>,
                [
                    {<<"id">>, 0},
                    {<<"destination">>, <<"/queue/foo">>},
                    {<<"ack">>, <<"auto">>}
                ]
            )
        ),

        %% Transaction: tx1
        gen_tcp:send(
            Sock,
            serialize(
                <<"BEGIN">>,
                [{<<"transaction">>, <<"tx1">>}]
            )
        ),

        gen_tcp:send(
            Sock,
            serialize(
                <<"SEND">>,
                [
                    {<<"destination">>, <<"/queue/foo">>},
                    {<<"transaction">>, <<"tx1">>}
                ],
                <<"hello">>
            )
        ),

        %% You will not receive any messages
        {error, timeout} = gen_tcp:recv(Sock, 0, 1000),

        gen_tcp:send(
            Sock,
            serialize(
                <<"SEND">>,
                [
                    {<<"destination">>, <<"/queue/foo">>},
                    {<<"transaction">>, <<"tx1">>}
                ],
                <<"hello again">>
            )
        ),

        gen_tcp:send(
            Sock,
            serialize(
                <<"COMMIT">>,
                [{<<"transaction">>, <<"tx1">>}]
            )
        ),

        ct:sleep(1000),
        {ok, Data1} = gen_tcp:recv(Sock, 0, 500),

        {ok,
            #stomp_frame{
                command = <<"MESSAGE">>,
                headers = _,
                body = <<"hello">>
            },
            Rest1, _} = parse(Data1),

        %{ok, Data2} = gen_tcp:recv(Sock, 0, 500),
        {ok,
            #stomp_frame{
                command = <<"MESSAGE">>,
                headers = _,
                body = <<"hello again">>
            },
            _Rest2, _} = parse(Rest1),

        %% Transaction: tx2
        gen_tcp:send(
            Sock,
            serialize(
                <<"BEGIN">>,
                [{<<"transaction">>, <<"tx2">>}]
            )
        ),

        gen_tcp:send(
            Sock,
            serialize(
                <<"SEND">>,
                [
                    {<<"destination">>, <<"/queue/foo">>},
                    {<<"transaction">>, <<"tx2">>}
                ],
                <<"hello">>
            )
        ),

        gen_tcp:send(
            Sock,
            serialize(
                <<"ABORT">>,
                [{<<"transaction">>, <<"tx2">>}]
            )
        ),

        %% You will not receive any messages
        {error, timeout} = gen_tcp:recv(Sock, 0, 1000),

        gen_tcp:send(
            Sock,
            serialize(
                <<"DISCONNECT">>,
                [{<<"receipt">>, <<"12345">>}]
            )
        ),

        {ok, Data3} = gen_tcp:recv(Sock, 0),
        {ok,
            #stomp_frame{
                command = <<"RECEIPT">>,
                headers = [{<<"receipt-id">>, <<"12345">>}],
                body = _
            },
            _, _} = parse(Data3)
    end).

t_receipt_in_error(_) ->
    with_connection(fun(Sock) ->
        gen_tcp:send(
            Sock,
            serialize(
                <<"CONNECT">>,
                [
                    {<<"accept-version">>, ?STOMP_VER},
                    {<<"host">>, <<"127.0.0.1:61613">>},
                    {<<"login">>, <<"guest">>},
                    {<<"passcode">>, <<"guest">>},
                    {<<"heart-beat">>, <<"0,0">>}
                ]
            )
        ),
        {ok, Data} = gen_tcp:recv(Sock, 0),
        {ok,
            #stomp_frame{
                command = <<"CONNECTED">>,
                headers = _,
                body = _
            },
            _, _} = parse(Data),

        gen_tcp:send(
            Sock,
            serialize(
                <<"ABORT">>,
                [
                    {<<"transaction">>, <<"tx1">>},
                    {<<"receipt">>, <<"12345">>}
                ]
            )
        ),

        {ok, Data1} = gen_tcp:recv(Sock, 0),
        {ok,
            Frame = #stomp_frame{
                command = <<"ERROR">>,
                headers = _,
                body = <<"Transaction tx1 not found">>
            },
            _, _} = parse(Data1),

        <<"12345">> = proplists:get_value(<<"receipt-id">>, Frame#stomp_frame.headers)
    end).

t_ack(_) ->
    with_connection(fun(Sock) ->
        gen_tcp:send(
            Sock,
            serialize(
                <<"CONNECT">>,
                [
                    {<<"accept-version">>, ?STOMP_VER},
                    {<<"host">>, <<"127.0.0.1:61613">>},
                    {<<"login">>, <<"guest">>},
                    {<<"passcode">>, <<"guest">>},
                    {<<"heart-beat">>, <<"0,0">>}
                ]
            )
        ),
        {ok, Data} = gen_tcp:recv(Sock, 0),
        {ok,
            #stomp_frame{
                command = <<"CONNECTED">>,
                headers = _,
                body = _
            },
            _, _} = parse(Data),

        %% Subscribe
        gen_tcp:send(
            Sock,
            serialize(
                <<"SUBSCRIBE">>,
                [
                    {<<"id">>, 0},
                    {<<"destination">>, <<"/queue/foo">>},
                    {<<"ack">>, <<"client">>}
                ]
            )
        ),

        gen_tcp:send(
            Sock,
            serialize(
                <<"SEND">>,
                [{<<"destination">>, <<"/queue/foo">>}],
                <<"ack test">>
            )
        ),

        {ok, Data1} = gen_tcp:recv(Sock, 0),
        {ok,
            Frame = #stomp_frame{
                command = <<"MESSAGE">>,
                headers = _,
                body = <<"ack test">>
            },
            _, _} = parse(Data1),

        AckId = proplists:get_value(<<"ack">>, Frame#stomp_frame.headers),

        gen_tcp:send(
            Sock,
            serialize(
                <<"ACK">>,
                [
                    {<<"id">>, AckId},
                    {<<"receipt">>, <<"12345">>}
                ]
            )
        ),

        {ok, Data2} = gen_tcp:recv(Sock, 0),
        {ok,
            #stomp_frame{
                command = <<"RECEIPT">>,
                headers = [{<<"receipt-id">>, <<"12345">>}],
                body = _
            },
            _, _} = parse(Data2),

        gen_tcp:send(
            Sock,
            serialize(
                <<"SEND">>,
                [{<<"destination">>, <<"/queue/foo">>}],
                <<"nack test">>
            )
        ),

        {ok, Data3} = gen_tcp:recv(Sock, 0),
        {ok,
            Frame1 = #stomp_frame{
                command = <<"MESSAGE">>,
                headers = _,
                body = <<"nack test">>
            },
            _, _} = parse(Data3),

        AckId1 = proplists:get_value(<<"ack">>, Frame1#stomp_frame.headers),

        gen_tcp:send(
            Sock,
            serialize(
                <<"NACK">>,
                [
                    {<<"id">>, AckId1},
                    {<<"receipt">>, <<"12345">>}
                ]
            )
        ),

        {ok, Data4} = gen_tcp:recv(Sock, 0),
        {ok,
            #stomp_frame{
                command = <<"RECEIPT">>,
                headers = [{<<"receipt-id">>, <<"12345">>}],
                body = _
            },
            _, _} = parse(Data4)
    end).

t_1000_msg_send(_) ->
    with_connection(fun(Sock) ->
        gen_tcp:send(
            Sock,
            serialize(
                <<"CONNECT">>,
                [
                    {<<"accept-version">>, ?STOMP_VER},
                    {<<"host">>, <<"127.0.0.1:61613">>},
                    {<<"login">>, <<"guest">>},
                    {<<"passcode">>, <<"guest">>},
                    {<<"heart-beat">>, <<"0,0">>}
                ]
            )
        ),
        {ok, Data} = gen_tcp:recv(Sock, 0),
        {ok,
            #stomp_frame{
                command = <<"CONNECTED">>,
                headers = _,
                body = _
            },
            _, _} = parse(Data),

        Topic = <<"/queue/foo">>,
        SendFun = fun() ->
            gen_tcp:send(
                Sock,
                serialize(
                    <<"SEND">>,
                    [{<<"destination">>, Topic}],
                    <<"msgtest">>
                )
            )
        end,

        RecvFun = fun() ->
            receive
                {deliver, Topic, _Msg} ->
                    ok
            after 100 ->
                ?assert(false, "waiting message timeout")
            end
        end,

        emqx:subscribe(Topic),
        lists:foreach(fun(_) -> SendFun() end, lists:seq(1, 1000)),
        lists:foreach(fun(_) -> RecvFun() end, lists:seq(1, 1000))
    end).

t_sticky_packets_truncate_after_headers(_) ->
    with_connection(fun(Sock) ->
        gen_tcp:send(
            Sock,
            serialize(
                <<"CONNECT">>,
                [
                    {<<"accept-version">>, ?STOMP_VER},
                    {<<"host">>, <<"127.0.0.1:61613">>},
                    {<<"login">>, <<"guest">>},
                    {<<"passcode">>, <<"guest">>},
                    {<<"heart-beat">>, <<"0,0">>}
                ]
            )
        ),
        {ok, Data} = gen_tcp:recv(Sock, 0),
        {ok,
            #stomp_frame{
                command = <<"CONNECTED">>,
                headers = _,
                body = _
            },
            _, _} = parse(Data),

        Topic = <<"/queue/foo">>,

        emqx:subscribe(Topic),
        gen_tcp:send(Sock, [
            "SEND\n",
            "content-length:3\n",
            "destination:/queue/foo\n"
        ]),
        timer:sleep(300),
        gen_tcp:send(Sock, ["\nfoo", 0]),
        receive
            {deliver, Topic, _Msg} ->
                ok
        after 100 ->
            ?assert(false, "waiting message timeout")
        end
    end).

t_frame_error_in_connect(_) ->
    with_connection(fun(Sock) ->
        gen_tcp:send(
            Sock,
            serialize(
                <<"CONNECT">>,
                [
                    {<<"accept-version">>, ?STOMP_VER},
                    {<<"host">>, <<"127.0.0.1:61613">>},
                    {<<"login">>, <<"guest">>},
                    {<<"passcode">>, <<"guest">>},
                    {<<"heart-beat">>, <<"0,0">>},
                    {<<"custome_header1">>, <<"val">>},
                    {<<"custome_header2">>, <<"val">>},
                    {<<"custome_header3">>, <<"val">>},
                    {<<"custome_header4">>, <<"val">>},
                    {<<"custome_header5">>, <<"val">>},
                    {<<"custome_header6">>, <<"val">>}
                ]
            )
        ),
        ?assertMatch({error, closed}, gen_tcp:recv(Sock, 0))
    end).

t_frame_error_too_many_headers(_) ->
    Frame = serialize(
        <<"SEND">>,
        [
            {<<"destination">>, <<"/queue/foo">>},
            {<<"custome_header1">>, <<"val">>},
            {<<"custome_header2">>, <<"val">>},
            {<<"custome_header3">>, <<"val">>},
            {<<"custome_header4">>, <<"val">>},
            {<<"custome_header5">>, <<"val">>},
            {<<"custome_header6">>, <<"val">>},
            {<<"custome_header7">>, <<"val">>},
            {<<"custome_header8">>, <<"val">>},
            {<<"custome_header9">>, <<"val">>},
            {<<"custome_header10">>, <<"val">>}
        ],
        <<"test">>
    ),
    Assert =
        fun(Sock) ->
            {ok, ErrorFrame} = recv_a_frame(Sock),
            ?assertMatch(#stomp_frame{command = <<"ERROR">>}, ErrorFrame),
            ?assertMatch(
                match, re:run(ErrorFrame#stomp_frame.body, "too_many_headers", [{capture, none}])
            ),
            ?assertMatch({error, closed}, gen_tcp:recv(Sock, 0))
        end,
    test_frame_error(Frame, Assert).

t_frame_error_too_long_header(_) ->
    LongHeaderVal = emqx_utils:bin_to_hexstr(crypto:strong_rand_bytes(50), upper),
    Frame = serialize(
        <<"SEND">>,
        [
            {<<"destination">>, <<"/queue/foo">>},
            {<<"custome_header10">>, LongHeaderVal}
        ],
        <<"test">>
    ),
    Assert =
        fun(Sock) ->
            {ok, ErrorFrame} = recv_a_frame(Sock),
            ?assertMatch(#stomp_frame{command = <<"ERROR">>}, ErrorFrame),
            ?assertMatch(
                match, re:run(ErrorFrame#stomp_frame.body, "too_long_header", [{capture, none}])
            ),
            ?assertMatch({error, closed}, gen_tcp:recv(Sock, 0))
        end,
    test_frame_error(Frame, Assert).

t_frame_error_too_long_body(_) ->
    LongBody = emqx_utils:bin_to_hexstr(crypto:strong_rand_bytes(513), upper),
    Frame = serialize(
        <<"SEND">>,
        [{<<"destination">>, <<"/queue/foo">>}],
        LongBody
    ),
    Assert =
        fun(Sock) ->
            {ok, ErrorFrame} = recv_a_frame(Sock),
            ?assertMatch(#stomp_frame{command = <<"ERROR">>}, ErrorFrame),
            ?assertMatch(
                match, re:run(ErrorFrame#stomp_frame.body, "too_long_body", [{capture, none}])
            ),
            ?assertMatch({error, closed}, gen_tcp:recv(Sock, 0))
        end,
    test_frame_error(Frame, Assert).

test_frame_error(Frame, AssertFun) ->
    with_connection(fun(Sock) ->
        send_connection_frame(Sock, <<"guest">>, <<"guest">>),
        ?assertMatch({ok, #stomp_frame{command = <<"CONNECTED">>}}, recv_a_frame(Sock)),
        gen_tcp:send(Sock, Frame),
        AssertFun(Sock)
    end).

t_rest_clientid_info(_) ->
    with_connection(fun(Sock) ->
        send_connection_frame(Sock, <<"guest">>, <<"guest">>),
        ?assertMatch({ok, #stomp_frame{command = <<"CONNECTED">>}}, recv_a_frame(Sock)),

        %% client lists
        {200, Clients} = request(get, "/gateways/stomp/clients"),
        ?assertEqual(1, length(maps:get(data, Clients))),
        StompClient = lists:nth(1, maps:get(data, Clients)),
        ClientId = maps:get(clientid, StompClient),
        ClientPath =
            "/gateways/stomp/clients/" ++
                binary_to_list(ClientId),
        {200, StompClient1} = request(get, ClientPath),
        ?assertEqual(StompClient, StompClient1),
        assert_fields_exist(
            [
                proto_name,
                awaiting_rel_max,
                inflight_cnt,
                disconnected_at,
                send_msg,
                heap_size,
                connected,
                recv_cnt,
                send_pkt,
                mailbox_len,
                username,
                recv_pkt,
                expiry_interval,
                clientid,
                mqueue_max,
                send_oct,
                ip_address,
                is_bridge,
                awaiting_rel_cnt,
                mqueue_dropped,
                mqueue_len,
                node,
                inflight_max,
                reductions,
                subscriptions_max,
                connected_at,
                keepalive,
                created_at,
                clean_start,
                subscriptions_cnt,
                recv_msg,
                send_cnt,
                proto_ver,
                recv_oct
            ],
            StompClient
        ),

        %% assert keepalive
        ?assertEqual(10, maps:get(keepalive, StompClient)),

        %% sub & unsub
        {200, []} = request(get, ClientPath ++ "/subscriptions"),
        ok = send_subscribe_frame(Sock, 0, <<"/queue/foo">>),
        ?assertMatch({ok, #stomp_frame{command = <<"RECEIPT">>}}, recv_a_frame(Sock)),

        {200, Subs} = request(get, ClientPath ++ "/subscriptions"),
        ?assertEqual(1, length(Subs)),
        assert_fields_exist([topic, qos], lists:nth(1, Subs)),

        {201, _} = request(
            post,
            ClientPath ++ "/subscriptions",
            #{
                topic => <<"t/a">>,
                qos => 1,
                sub_props => #{subid => <<"1001">>}
            }
        ),

        {200, Subs1} = request(get, ClientPath ++ "/subscriptions"),
        ?assertEqual(2, length(Subs1)),
        {200, StompClient2} = request(get, ClientPath),
        ?assertMatch(#{subscriptions_cnt := 2}, StompClient2),

        {204, _} = request(delete, ClientPath ++ "/subscriptions/t%2Fa"),
        {200, Subs2} = request(get, ClientPath ++ "/subscriptions"),
        ?assertEqual(1, length(Subs2)),
        {200, StompClient3} = request(get, ClientPath),
        ?assertMatch(#{subscriptions_cnt := 1}, StompClient3),

        %% kickout
        {204, _} = request(delete, ClientPath),
        % sync
        ignored = gen_server:call(emqx_cm, ignore, infinity),
        ok = emqx_pool:flush_async_tasks(),
        {200, Clients2} = request(get, "/gateways/stomp/clients"),
        ?assertEqual(0, length(maps:get(data, Clients2)))
    end).

t_authn_superuser(_) ->
    %% mock authn
    meck:new(emqx_access_control, [passthrough]),
    meck:expect(
        emqx_access_control,
        authenticate,
        fun
            (#{username := <<"admin">>}) ->
                {ok, #{is_superuser => true}};
            (#{username := <<"bad_user">>}) ->
                {error, not_authorized};
            (_) ->
                {ok, #{is_superuser => false}}
        end
    ),
    %% mock authz
    meck:expect(
        emqx_access_control,
        authorize,
        fun
            (_ClientInfo = #{is_superuser := true}, _PubSub, _Topic) ->
                allow;
            (_ClientInfo, _PubSub, _Topic) ->
                deny
        end
    ),

    LoginFailure = fun(Sock) ->
        ok = send_connection_frame(Sock, <<"bad_user">>, <<"public">>),
        ?assertMatch({ok, #stomp_frame{command = <<"ERROR">>}}, recv_a_frame(Sock)),
        ?assertMatch({error, closed}, recv_a_frame(Sock))
    end,

    PublishFailure = fun(Sock) ->
        ok = send_connection_frame(Sock, <<"user1">>, <<"public">>),
        ?assertMatch({ok, #stomp_frame{command = <<"CONNECTED">>}}, recv_a_frame(Sock)),
        ok = send_message_frame(Sock, <<"t/a">>, <<"hello">>),
        ?assertMatch({ok, #stomp_frame{command = <<"ERROR">>}}, recv_a_frame(Sock)),
        ?assertMatch({error, closed}, recv_a_frame(Sock))
    end,

    SubscribeFailed = fun(Sock) ->
        ok = send_connection_frame(Sock, <<"user1">>, <<"public">>),
        ?assertMatch({ok, #stomp_frame{command = <<"CONNECTED">>}}, recv_a_frame(Sock)),
        ok = send_subscribe_frame(Sock, 0, <<"t/a">>),
        ?assertMatch({ok, #stomp_frame{command = <<"ERROR">>}}, recv_a_frame(Sock)),
        ?assertMatch({error, closed}, recv_a_frame(Sock))
    end,

    LoginAsSuperUser = fun(Sock) ->
        ok = send_connection_frame(Sock, <<"admin">>, <<"public">>),
        ?assertMatch({ok, #stomp_frame{command = <<"CONNECTED">>}}, recv_a_frame(Sock)),
        ok = send_subscribe_frame(Sock, 0, <<"t/a">>),
        ?assertMatch({ok, #stomp_frame{command = <<"RECEIPT">>}}, recv_a_frame(Sock)),
        ok = send_message_frame(Sock, <<"t/a">>, <<"hello">>),
        ?assertMatch({ok, #stomp_frame{command = <<"RECEIPT">>}}, recv_a_frame(Sock)),
        ?assertMatch(
            {ok, #stomp_frame{
                command = <<"MESSAGE">>,
                body = <<"hello">>
            }},
            recv_a_frame(Sock)
        ),
        ok = send_disconnect_frame(Sock)
    end,

    with_connection(LoginFailure),
    with_connection(PublishFailure),
    with_connection(SubscribeFailed),
    with_connection(LoginAsSuperUser),
    meck:unload(emqx_access_control).

t_mountpoint(_) ->
    restart_stomp_with_mountpoint(<<"stomp/">>),

    PubSub = fun(Sock) ->
        ok = send_connection_frame(Sock, <<"user1">>, <<"public">>),
        ?assertMatch({ok, #stomp_frame{command = <<"CONNECTED">>}}, recv_a_frame(Sock)),
        ok = send_subscribe_frame(Sock, 0, <<"t/a">>),
        ?assertMatch({ok, #stomp_frame{command = <<"RECEIPT">>}}, recv_a_frame(Sock)),
        ok = send_message_frame(Sock, <<"t/a">>, <<"hello">>),
        ?assertMatch({ok, #stomp_frame{command = <<"RECEIPT">>}}, recv_a_frame(Sock)),

        {ok, #stomp_frame{
            command = <<"MESSAGE">>,
            headers = Headers,
            body = <<"hello">>
        }} = recv_a_frame(Sock),
        ?assertEqual(<<"t/a">>, proplists:get_value(<<"destination">>, Headers)),

        ?assertEqual(
            <<"text/plain">>, proplists:get_value(<<"content-type">>, Headers)
        ),

        ok = send_disconnect_frame(Sock)
    end,

    PubToMqtt = fun(Sock) ->
        ok = send_connection_frame(Sock, <<"user1">>, <<"public">>),
        ?assertMatch({ok, #stomp_frame{command = <<"CONNECTED">>}}, recv_a_frame(Sock)),

        ok = emqx:subscribe(<<"stomp/t/a">>),
        ok = send_message_frame(Sock, <<"t/a">>, <<"hello">>),
        ?assertMatch({ok, #stomp_frame{command = <<"RECEIPT">>}}, recv_a_frame(Sock)),

        receive
            {deliver, Topic, Msg} ->
                ?assertEqual(<<"stomp/t/a">>, Topic),
                ?assertEqual(<<"hello">>, emqx_message:payload(Msg))
        after 100 ->
            ?assert(false, "waiting message timeout")
        end,
        ok = send_disconnect_frame(Sock)
    end,

    ReceiveMsgFromMqtt = fun(Sock) ->
        ok = send_connection_frame(Sock, <<"user1">>, <<"public">>),
        ?assertMatch({ok, #stomp_frame{command = <<"CONNECTED">>}}, recv_a_frame(Sock)),
        ok = send_subscribe_frame(Sock, 0, <<"t/a">>),
        ?assertMatch({ok, #stomp_frame{command = <<"RECEIPT">>}}, recv_a_frame(Sock)),

        Msg = emqx_message:make(<<"stomp/t/a">>, <<"hello">>),
        emqx:publish(Msg),

        {ok, #stomp_frame{
            command = <<"MESSAGE">>,
            headers = Headers,
            body = <<"hello">>
        }} = recv_a_frame(Sock),
        ?assertEqual(<<"t/a">>, proplists:get_value(<<"destination">>, Headers)),

        ok = send_disconnect_frame(Sock)
    end,

    with_connection(PubSub),
    with_connection(PubToMqtt),
    with_connection(ReceiveMsgFromMqtt),
    restart_stomp_with_mountpoint(<<>>).

%% TODO: Mountpoint, AuthChain, Authorization + Mountpoint, ClientInfoOverride,
%%       Listeners, Metrics, Stats, ClientInfo
%%
%% TODO: Start/Stop, List Instance
%%
%% TODO: RateLimit, OOM,

%%--------------------------------------------------------------------
%% helpers

with_connection(DoFun) ->
    {ok, Sock} = gen_tcp:connect(
        {127, 0, 0, 1},
        61613,
        [binary, {packet, raw}, {active, false}],
        3000
    ),
    try
        DoFun(Sock)
    after
        erase(parser),
        erase(rest),
        gen_tcp:close(Sock)
    end.

serialize(Command, Headers) ->
    emqx_stomp_frame:serialize_pkt(emqx_stomp_frame:make(Command, Headers), #{}).

serialize(Command, Headers, Body) ->
    emqx_stomp_frame:serialize_pkt(emqx_stomp_frame:make(Command, Headers, Body), #{}).

recv_a_frame(Sock) ->
    Parser =
        case get(parser) of
            undefined ->
                ProtoEnv = #{
                    max_headers => 1024,
                    max_header_length => 10240,
                    max_body_length => 81920
                },
                emqx_stomp_frame:initial_parse_state(ProtoEnv);
            P ->
                P
        end,
    LastRest =
        case get(rest) of
            undefined -> <<>>;
            R -> R
        end,
    case emqx_stomp_frame:parse(LastRest, Parser) of
        {more, NParser} ->
            case gen_tcp:recv(Sock, 0, 5000) of
                {ok, Data} ->
                    put(parser, NParser),
                    put(rest, <<LastRest/binary, Data/binary>>),
                    recv_a_frame(Sock);
                {error, _} = Err1 ->
                    erase(parser),
                    erase(rest),
                    Err1
            end;
        {ok, Frame, Rest, NParser} ->
            put(parser, NParser),
            put(rest, Rest),
            ct:pal("recv_a_frame: ~p~n", [Frame]),
            {ok, Frame};
        {error, _} = Err ->
            erase(parser),
            erase(rest),
            Err
    end.

parse(Data) ->
    ProtoEnv = #{
        max_headers => 1024,
        max_header_length => 10240,
        max_body_length => 81920
    },
    Parser = emqx_stomp_frame:initial_parse_state(ProtoEnv),
    emqx_stomp_frame:parse(Data, Parser).

get_field(command, #stomp_frame{command = Command}) ->
    Command;
get_field(body, #stomp_frame{body = Body}) ->
    Body.

send_connection_frame(Sock, Username, Password) ->
    send_connection_frame(Sock, Username, Password, <<"10000,10000">>).

send_connection_frame(Sock, Username, Password, Heartbeat) ->
    Headers =
        case Username == undefined of
            true -> [];
            false -> [{<<"login">>, Username}]
        end ++
            case Password == undefined of
                true -> [];
                false -> [{<<"passcode">>, Password}]
            end,
    Headers1 = [
        {<<"accept-version">>, ?STOMP_VER},
        {<<"host">>, <<"127.0.0.1:61613">>},
        {<<"heart-beat">>, Heartbeat}
        | Headers
    ],
    ok = gen_tcp:send(Sock, serialize(<<"CONNECT">>, Headers1)).

send_subscribe_frame(Sock, Id, Topic) ->
    Headers =
        [
            {<<"id">>, Id},
            {<<"receipt">>, Id},
            {<<"destination">>, Topic},
            {<<"ack">>, <<"auto">>}
        ],
    ok = gen_tcp:send(Sock, serialize(<<"SUBSCRIBE">>, Headers)).

send_unsubscribe_frame(Sock, Id) when is_integer(Id) ->
    Headers =
        [
            {<<"id">>, Id},
            {<<"receipt">>, <<"rp-", (integer_to_binary(Id))/binary>>}
        ],
    gen_tcp:send(Sock, serialize(<<"UNSUBSCRIBE">>, Headers)).

send_message_frame(Sock, Topic, Payload) ->
    send_message_frame(Sock, Topic, Payload, []).

send_message_frame(Sock, Topic, Payload, Headers0) ->
    Headers =
        [
            {<<"destination">>, Topic},
            {<<"receipt">>, <<"rp-", Topic/binary>>}
            | Headers0
        ],
    ok = gen_tcp:send(Sock, serialize(<<"SEND">>, Headers, Payload)).

send_disconnect_frame(Sock) ->
    ok = gen_tcp:send(Sock, serialize(<<"DISCONNECT">>, [])).

send_disconnect_frame(Sock, ReceiptId) ->
    Headers = [{<<"receipt">>, ReceiptId}],
    ok = gen_tcp:send(Sock, serialize(<<"DISCONNECT">>, Headers)).

clients() ->
    {200, Clients} = request(get, "/gateways/stomp/clients"),
    maps:get(data, Clients).

create_subscription(ClientId, Topic, SubId) ->
    Path = io_lib:format("/gateways/stomp/clients/~s/subscriptions", [ClientId]),
    Body = #{
        topic => Topic,
        qos => 1,
        sub_props => #{subid => SubId}
    },
    case request(post, Path, Body) of
        {201, _} ->
            ok;
        {400, #{message := Message}} ->
            {error, Message}
    end.
