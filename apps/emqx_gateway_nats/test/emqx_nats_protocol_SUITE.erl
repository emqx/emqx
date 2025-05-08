%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_protocol_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_nats.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(DEFAULT_CLIENT_OPTS, #{
    host => "tcp://localhost",
    port => 4222,
    verbose => false
}).

-define(DEFAULT_WS_CLIENT_OPTS, #{
    host => "ws://localhost",
    port => 4223,
    verbose => false
}).

-define(CONF_DEFAULT, <<
    "\n"
    "gateway.nats {\n"
    "  protocol {\n"
    "    max_payload_size = 1024\n"
    "  }\n"
    "  listeners.tcp.default {\n"
    "    bind = 4222\n"
    "  }\n"
    "  listeners.ws.default {\n"
    "    bind = 4223\n"
    "  }\n"
    "}\n"
>>).

%%--------------------------------------------------------------------
%% CT Callbacks
%%--------------------------------------------------------------------

all() -> [{group, tcp}, {group, ws}].

groups() ->
    CTs = emqx_common_test_helpers:all(?MODULE),
    [
        {tcp, [], CTs},
        {ws, [], CTs}
    ].

init_per_suite(Config) ->
    application:load(emqx_gateway_nats),
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

init_per_group(tcp, Config) ->
    [{client_opts, ?DEFAULT_CLIENT_OPTS} | Config];
init_per_group(ws, Config) ->
    [{client_opts, ?DEFAULT_WS_CLIENT_OPTS} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    snabbkaffe:start_trace(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    snabbkaffe:stop(),
    ok.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_connect(Config) ->
    ClientOpts = ?config(client_opts, Config),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:ping(Client),
    {ok, Msgs2} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_PONG}], Msgs2),
    emqx_nats_client:stop(Client).

t_verbose_mode(Config) ->
    ClientOpts = maps:merge(?config(client_opts, Config), #{verbose => true}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),

    %% Test INFO message
    {ok, [InfoMsg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_INFO,
            message = #{
                <<"auth_required">> := false,
                <<"version">> := _,
                <<"max_payload">> := _
            }
        },
        InfoMsg
    ),

    %% Test CONNECT with verbose mode
    ok = emqx_nats_client:connect(Client),
    {ok, [ConnectAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        ConnectAck
    ),

    %% Test PING/PONG
    ok = emqx_nats_client:ping(Client),
    {ok, [PongMsg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_PONG
        },
        PongMsg
    ),

    %% Test SUBSCRIBE
    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-1">>),
    {ok, [SubAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        SubAck
    ),

    %% Test PUBLISH and message delivery
    ok = emqx_nats_client:publish(Client, <<"foo">>, <<"hello">>),
    {ok, [PubAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        PubAck
    ),

    {ok, [Msg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_MSG,
            message = #{
                subject := <<"foo">>,
                sid := <<"sid-1">>,
                payload := <<"hello">>
            }
        },
        Msg
    ),

    %% Test PUBLISH with reply-to
    ok = emqx_nats_client:publish(Client, <<"foo">>, <<"reply-to">>, <<"with reply">>),
    {ok, [PubAck2]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        PubAck2
    ),

    {ok, [Msg2]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_MSG,
            message = #{
                subject := <<"foo">>,
                sid := <<"sid-1">>,
                payload := <<"with reply">>
            }
        },
        Msg2
    ),

    %% Test UNSUBSCRIBE
    ok = emqx_nats_client:unsubscribe(Client, <<"sid-1">>),
    {ok, [UnsubAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        UnsubAck
    ),

    emqx_nats_client:stop(Client).

t_ping_pong(Config) ->
    ClientOpts = ?config(client_opts, Config),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:ping(Client),
    {ok, Msgs2} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_PONG}], Msgs2),
    emqx_nats_client:stop(Client).

t_subscribe(Config) ->
    ClientOpts = ?config(client_opts, Config),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-1">>),
    emqx_nats_client:stop(Client).

t_publish(Config) ->
    ClientOpts = ?config(client_opts, Config),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:publish(Client, <<"foo">>, <<"hello">>),
    emqx_nats_client:stop(Client).

t_receive_message(Config) ->
    ClientOpts = ?config(client_opts, Config),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-1">>),
    ok = emqx_nats_client:publish(Client, <<"foo">>, <<"hello">>),
    {ok, [Msg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_MSG,
            message = #{
                subject := <<"foo">>,
                sid := <<"sid-1">>,
                payload := <<"hello">>
            }
        },
        Msg
    ),
    emqx_nats_client:stop(Client).

t_unsubscribe(Config) ->
    ClientOpts = ?config(client_opts, Config),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-1">>),
    ok = emqx_nats_client:unsubscribe(Client, <<"sid-1">>),
    emqx_nats_client:stop(Client).

t_queue_group(Config) ->
    ClientOpts = maps:merge(?config(client_opts, Config), #{verbose => true}),
    {ok, Client1} = emqx_nats_client:start_link(ClientOpts),
    {ok, Client2} = emqx_nats_client:start_link(ClientOpts),

    %% Connect both clients
    {ok, [_]} = emqx_nats_client:receive_message(Client1),
    {ok, [_]} = emqx_nats_client:receive_message(Client2),
    ok = emqx_nats_client:connect(Client1),
    ok = emqx_nats_client:connect(Client2),
    {ok, [_]} = emqx_nats_client:receive_message(Client1),
    {ok, [_]} = emqx_nats_client:receive_message(Client2),

    %% Subscribe to the same queue group
    ok = emqx_nats_client:subscribe(Client1, <<"foo">>, <<"sid-1">>, <<"group-1">>),
    ok = emqx_nats_client:subscribe(Client2, <<"foo">>, <<"sid-2">>, <<"group-1">>),
    {ok, [_]} = emqx_nats_client:receive_message(Client1),
    {ok, [_]} = emqx_nats_client:receive_message(Client2),

    %% Publish messages with mqtt
    _ = emqx_broker:publish(emqx_message:make(<<"foo">>, <<"msgs1">>)),
    _ = emqx_broker:publish(emqx_message:make(<<"foo">>, <<"msgs2">>)),

    %% Receive messages - only one client should receive each message
    {ok, Msgs1} = emqx_nats_client:receive_message(Client1),
    {ok, Msgs2} = emqx_nats_client:receive_message(Client2),

    ?assertEqual(2, length(Msgs1 ++ Msgs2)),

    emqx_nats_client:stop(Client1),
    emqx_nats_client:stop(Client2).
