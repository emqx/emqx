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

-module(emqx_stomp_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include("src/stomp/include/emqx_stomp.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-import(
    emqx_gateway_test_utils,
    [
        assert_feilds_apperence/2,
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
    "  listeners.tcp.default {\n"
    "    bind = 61613\n"
    "  }\n"
    "}\n"
>>).

all() -> emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

init_per_suite(Cfg) ->
    ok = emqx_common_test_helpers:load_config(emqx_gateway_schema, ?CONF_DEFAULT),
    emqx_mgmt_api_test_util:init_suite([emqx_authn, emqx_gateway]),
    Cfg.

end_per_suite(_Cfg) ->
    emqx_mgmt_api_test_util:end_suite([emqx_gateway, emqx_authn]),
    ok.

default_config() ->
    ?CONF_DEFAULT.

stomp_ver() ->
    ?STOMP_VER.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_connect(_) ->
    %% Connect should be succeed
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
                    {<<"heart-beat">>, <<"1000,2000">>}
                ]
            )
        ),
        {ok, Data} = gen_tcp:recv(Sock, 0),
        {ok,
            Frame = #stomp_frame{
                command = <<"CONNECTED">>,
                headers = _,
                body = _
            },
            _, _} = parse(Data),
        <<"2000,1000">> = proplists:get_value(<<"heart-beat">>, Frame#stomp_frame.headers),

        gen_tcp:send(
            Sock,
            serialize(
                <<"DISCONNECT">>,
                [{<<"receipt">>, <<"12345">>}]
            )
        ),

        {ok, Data1} = gen_tcp:recv(Sock, 0),
        {ok,
            #stomp_frame{
                command = <<"RECEIPT">>,
                headers = [{<<"receipt-id">>, <<"12345">>}],
                body = _
            },
            _, _} = parse(Data1)
    end),

    %% Connect will be failed, because of bad login or passcode
    %% FIXME: Waiting for authentication works
    %with_connection(
    %    fun(Sock) ->
    %        gen_tcp:send(Sock, serialize(<<"CONNECT">>,
    %                                     [{<<"accept-version">>, ?STOMP_VER},
    %                                      {<<"host">>, <<"127.0.0.1:61613">>},
    %                                      {<<"login">>, <<"admin">>},
    %                                      {<<"passcode">>, <<"admin">>},
    %                                      {<<"heart-beat">>, <<"1000,2000">>}])),
    %          {ok, Data} = gen_tcp:recv(Sock, 0),
    %          {ok, Frame, _, _} = parse(Data),
    %          #stomp_frame{command = <<"ERROR">>,
    %                       headers = _,
    %                       body    = <<"Login or passcode error!">>} = Frame
    %      end),

    %% Connect will be failed, because of bad version
    with_connection(fun(Sock) ->
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
    end).

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
                    {<<"heart-beat">>, <<"1000,800">>}
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

        %% 'user-defined' header will be retain
        gen_tcp:send(
            Sock,
            serialize(
                <<"SEND">>,
                [
                    {<<"destination">>, <<"/queue/foo">>},
                    {<<"user-defined">>, <<"emq">>}
                ],
                <<"hello">>
            )
        ),

        {ok, Data1} = gen_tcp:recv(Sock, 0, 1000),
        {ok,
            Frame = #stomp_frame{
                command = <<"MESSAGE">>,
                headers = _,
                body = <<"hello">>
            },
            _, _} = parse(Data1),
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

        %% Unsubscribe
        gen_tcp:send(
            Sock,
            serialize(
                <<"UNSUBSCRIBE">>,
                [
                    {<<"id">>, 0},
                    {<<"receipt">>, <<"12345">>}
                ]
            )
        ),

        {ok, Data2} = gen_tcp:recv(Sock, 0, 1000),

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
                <<"You will not receive this msg">>
            )
        ),

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
t_rest_clienit_info(_) ->
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

        %% client lists
        {200, Clients} = request(get, "/gateway/stomp/clients"),
        ?assertEqual(1, length(maps:get(data, Clients))),
        StompClient = lists:nth(1, maps:get(data, Clients)),
        ClientId = maps:get(clientid, StompClient),
        ClientPath =
            "/gateway/stomp/clients/" ++
                binary_to_list(ClientId),
        {200, StompClient1} = request(get, ClientPath),
        ?assertEqual(StompClient, StompClient1),
        assert_feilds_apperence(
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

        %% sub & unsub
        {200, []} = request(get, ClientPath ++ "/subscriptions"),
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
        timer:sleep(100),

        {200, Subs} = request(get, ClientPath ++ "/subscriptions"),
        ?assertEqual(1, length(Subs)),
        assert_feilds_apperence([topic, qos], lists:nth(1, Subs)),

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

        {204, _} = request(delete, ClientPath ++ "/subscriptions/t%2Fa"),
        {200, Subs2} = request(get, ClientPath ++ "/subscriptions"),
        ?assertEqual(1, length(Subs2)),

        %% kickout
        {204, _} = request(delete, ClientPath),
        % sync
        ignored = gen_server:call(emqx_cm, ignore, infinity),
        ok = emqx_pool:flush_async_tasks(),
        {200, Clients2} = request(get, "/gateway/stomp/clients"),
        ?assertEqual(0, length(maps:get(data, Clients2)))
    end).

%% TODO: Mountpoint, AuthChain, Authorization + Mountpoint, ClientInfoOverride,
%%       Listeners, Metrics, Stats, ClientInfo
%%
%% TODO: Start/Stop, List Instance
%%
%% TODO: RateLimit, OOM,

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
        gen_tcp:close(Sock)
    end.

serialize(Command, Headers) ->
    emqx_stomp_frame:serialize_pkt(emqx_stomp_frame:make(Command, Headers), #{}).

serialize(Command, Headers, Body) ->
    emqx_stomp_frame:serialize_pkt(emqx_stomp_frame:make(Command, Headers, Body), #{}).

parse(Data) ->
    ProtoEnv = #{
        max_headers => 10,
        max_header_length => 1024,
        max_body_length => 8192
    },
    Parser = emqx_stomp_frame:initial_parse_state(ProtoEnv),
    emqx_stomp_frame:parse(Data, Parser).

get_field(command, #stomp_frame{command = Command}) ->
    Command;
get_field(body, #stomp_frame{body = Body}) ->
    Body.
