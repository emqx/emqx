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
    Config.

end_per_testcase(_TestCase, _Config) ->
    try
        allow_pubsub_all(),
        delete_test_user(),
        disable_auth()
    catch
        _:_ ->
            ok
    end,
    ok.

%%--------------------------------------------------------------------
%% Helper Functions
%%--------------------------------------------------------------------

enable_auth() ->
    emqx_gateway_test_utils:enable_gateway_auth(<<"nats">>).

disable_auth() ->
    emqx_gateway_test_utils:disable_gateway_auth(<<"nats">>).

create_test_user() ->
    User = #{
        user_id => <<"test_user">>,
        password => <<"password">>,
        is_superuser => false
    },
    emqx_gateway_test_utils:add_gateway_auth_user(<<"nats">>, User).

delete_test_user() ->
    emqx_gateway_test_utils:delete_gateway_auth_user(<<"nats">>, <<"test_user">>).

allow_pubsub_all() ->
    emqx_gateway_test_utils:update_authz_file_rule(
        <<
            "\n"
            "        {allow,all}.\n"
            "    "
        >>
    ).

deny_pubsub_all() ->
    emqx_gateway_test_utils:update_authz_file_rule(
        <<
            "\n"
            "        {deny,all}.\n"
            "    "
        >>
    ).

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

't_receive_message_with_wildcard_*'(Config) ->
    ClientOpts = ?config(client_opts, Config),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:subscribe(Client, <<"foo.*">>, <<"sid-1">>),
    ok = emqx_nats_client:publish(Client, <<"foo.bar">>, <<"hello">>),
    {ok, [Msg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_MSG,
            message = #{
                subject := <<"foo.bar">>,
                sid := <<"sid-1">>,
                payload := <<"hello">>
            }
        },
        Msg
    ),
    emqx_nats_client:stop(Client).

't_receive_message_with_wildcard_>'(Config) ->
    ClientOpts = ?config(client_opts, Config),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:subscribe(Client, <<"foo.>">>, <<"sid-1">>),
    ok = emqx_nats_client:publish(Client, <<"foo.bar.baz">>, <<"hello">>),
    {ok, [Msg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_MSG,
            message = #{
                subject := <<"foo.bar.baz">>,
                sid := <<"sid-1">>,
                payload := <<"hello">>
            }
        },
        Msg
    ),
    emqx_nats_client:stop(Client).

't_receive_message_with_wildcard_combined_*_>'(Config) ->
    ClientOpts = ?config(client_opts, Config),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:subscribe(Client, <<"*.bar.>">>, <<"sid-1">>),
    ok = emqx_nats_client:publish(Client, <<"foo.bar.cato.delta">>, <<"hello">>),
    {ok, [Msg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_MSG,
            message = #{
                subject := <<"foo.bar.cato.delta">>,
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

t_unsubscribe_with_max_msgs(Config) ->
    Username = <<"unsub_user">>,
    ClientOpts = maps:merge(?config(client_opts, Config), #{
        verbose => true,
        user => Username
    }),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),

    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),

    [ClientInfo] = find_client_by_username(Username),
    ClientId = maps:get(clientid, ClientInfo),

    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-1">>),
    recv_ok_frame(Client),

    ?assertMatch(
        [#{topic := <<"foo">>}],
        emqx_gateway_test_utils:get_gateway_client_subscriptions(<<"nats">>, ClientId)
    ),

    ok = emqx_nats_client:unsubscribe(Client, <<"sid-1">>, 1),
    recv_ok_frame(Client),

    ok = emqx_nats_client:publish(Client, <<"foo">>, <<"hello1">>),
    recv_ok_frame(Client),

    {ok, [Msg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_MSG,
            message = #{
                subject := <<"foo">>,
                sid := <<"sid-1">>,
                payload := <<"hello1">>
            }
        },
        Msg
    ),

    ok = emqx_nats_client:publish(Client, <<"foo">>, <<"hello2">>),
    recv_ok_frame(Client),

    {ok, []} = emqx_nats_client:receive_message(Client, 1000),

    ?assertEqual(
        [], emqx_gateway_test_utils:get_gateway_client_subscriptions(<<"nats">>, ClientId)
    ),
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

t_queue_group_with_empty_string(Config) ->
    ClientOpts = maps:merge(?config(client_opts, Config), #{verbose => true}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, [_]} = emqx_nats_client:receive_message(Client),

    ok = emqx_nats_client:connect(Client),
    {ok, [_]} = emqx_nats_client:receive_message(Client),

    %% Subscribe to the empty string queue group treated as no queue group
    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-1">>, <<>>),
    {ok, [SubAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        SubAck
    ),

    [ClientInfo0] = emqx_gateway_test_utils:list_gateway_clients(<<"nats">>),
    ClientId = maps:get(clientid, ClientInfo0),

    Subscriptions = emqx_gateway_test_utils:get_gateway_client_subscriptions(<<"nats">>, ClientId),
    ?assert(lists:any(fun(S) -> maps:get(topic, S) =:= <<"foo">> end, Subscriptions)),

    emqx_nats_client:stop(Client).

t_reply_to(Config) ->
    ClientOpts = maps:merge(?config(client_opts, Config), #{verbose => true}),
    {ok, Publisher} = emqx_nats_client:start_link(ClientOpts),
    {ok, Subscriber} = emqx_nats_client:start_link(ClientOpts),

    %% Connect both clients
    {ok, [_]} = emqx_nats_client:receive_message(Publisher),
    {ok, [_]} = emqx_nats_client:receive_message(Subscriber),
    ok = emqx_nats_client:connect(Publisher),
    ok = emqx_nats_client:connect(Subscriber),
    {ok, [_]} = emqx_nats_client:receive_message(Publisher),
    {ok, [_]} = emqx_nats_client:receive_message(Subscriber),

    %% Subscribe to the subject
    ok = emqx_nats_client:subscribe(Subscriber, <<"test.subject">>, <<"sid-1">>),
    {ok, [_]} = emqx_nats_client:receive_message(Subscriber),

    %% Publish message with reply-to
    ReplyTo = <<"reply.subject">>,
    Payload = <<"test payload">>,
    ok = emqx_nats_client:publish(Publisher, <<"test.subject">>, ReplyTo, Payload),
    {ok, [_]} = emqx_nats_client:receive_message(Publisher),

    %% Receive message and verify reply-to
    {ok, [Msg]} = emqx_nats_client:receive_message(Subscriber),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_MSG,
            message = #{
                subject := <<"test.subject">>,
                sid := <<"sid-1">>,
                reply_to := ReplyTo,
                payload := Payload
            }
        },
        Msg
    ),

    emqx_nats_client:stop(Publisher),
    emqx_nats_client:stop(Subscriber).

t_reply_to_with_no_responders(Config) ->
    ClientOpts = maps:merge(
        ?config(client_opts, Config),
        #{
            verbose => true,
            no_responders => true,
            headers => true
        }
    ),

    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),

    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),

    ok = emqx_nats_client:subscribe(Client, <<"reply.*">>, <<"sid-1">>),
    recv_ok_frame(Client),

    ok = emqx_nats_client:publish(
        Client, <<"test.subject">>, <<"reply.subject">>, <<"test payload">>
    ),
    recv_ok_frame(Client),

    {ok, [Msg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_HMSG,
            message = #{
                subject := <<"reply.subject">>,
                sid := <<"sid-1">>,
                headers := #{<<"code">> := 503}
            }
        },
        Msg
    ),
    emqx_nats_client:stop(Client).

t_no_responders_must_work_with_headers(Config) ->
    ClientOpts = maps:merge(
        ?config(client_opts, Config),
        #{
            verbose => true,
            no_responders => true,
            headers => false
        }
    ),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),
    ok = emqx_nats_client:connect(Client),
    {ok, [ConnectAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_ERR
        },
        ConnectAck
    ),
    emqx_nats_client:stop(Client).

t_auth_success(Config) ->
    ok = enable_auth(),
    ok = create_test_user(),
    ClientOpts = maps:merge(
        ?config(client_opts, Config),
        #{
            user => <<"test_user">>,
            pass => <<"password">>,
            verbose => true
        }
    ),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, [InfoMsg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_INFO,
            message = _Message
        },
        InfoMsg
    ),
    Message = InfoMsg#nats_frame.message,
    ?assert(maps:get(<<"auth_required">>, Message) =:= true),

    %% Connect with credentials
    ok = emqx_nats_client:connect(Client),
    {ok, [ConnectAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        ConnectAck
    ),

    %% Test basic operations after successful authentication
    ok = emqx_nats_client:ping(Client),
    {ok, [PongMsg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_PONG
        },
        PongMsg
    ),

    emqx_nats_client:stop(Client),
    ok = delete_test_user(),
    ok = disable_auth().

t_auth_failure(Config) ->
    ok = enable_auth(),
    ok = create_test_user(),
    ClientOpts = maps:merge(
        ?config(client_opts, Config),
        #{
            user => <<"test_user">>,
            pass => <<"wrong_password">>,
            verbose => true
        }
    ),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, [InfoMsg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_INFO,
            message = _Message
        },
        InfoMsg
    ),
    Message = InfoMsg#nats_frame.message,
    ?assert(maps:get(<<"auth_required">>, Message) =:= true),

    %% Connect with wrong credentials
    ok = emqx_nats_client:connect(Client),
    {ok, [ErrorMsg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_ERR,
            message = <<"Login Failed: bad_username_or_password">>
        },
        ErrorMsg
    ),

    emqx_nats_client:stop(Client),
    ok = delete_test_user(),
    ok = disable_auth().

t_auth_dynamic_enable_disable(Config) ->
    %% Start with auth disabled
    ok = disable_auth(),
    ClientOpts = maps:merge(
        ?config(client_opts, Config),
        #{verbose => true}
    ),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, [InfoMsg1]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_INFO,
            message = _Message1
        },
        InfoMsg1
    ),
    Message1 = InfoMsg1#nats_frame.message,
    ?assert(maps:get(<<"auth_required">>, Message1) =:= false),

    %% Connect without credentials (should succeed)
    ok = emqx_nats_client:connect(Client),
    {ok, [ConnectAck1]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        ConnectAck1
    ),

    %% Enable auth and create test user
    ok = enable_auth(),
    ok = create_test_user(),
    {ok, Client2} = emqx_nats_client:start_link(ClientOpts),
    {ok, [InfoMsg2]} = emqx_nats_client:receive_message(Client2),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_INFO,
            message = _Message2
        },
        InfoMsg2
    ),
    Message2 = InfoMsg2#nats_frame.message,
    ?assert(maps:get(<<"auth_required">>, Message2) =:= true),

    %% Try to connect without credentials (should fail)
    ok = emqx_nats_client:connect(Client2),
    {ok, [ErrorMsg]} = emqx_nats_client:receive_message(Client2),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_ERR,
            message = <<"Login Failed: not_authorized">>
        },
        ErrorMsg
    ),

    %% Disable auth again
    ok = delete_test_user(),
    ok = disable_auth(),
    {ok, Client3} = emqx_nats_client:start_link(ClientOpts),
    {ok, [InfoMsg3]} = emqx_nats_client:receive_message(Client3),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_INFO,
            message = _Message3
        },
        InfoMsg3
    ),
    Message3 = InfoMsg3#nats_frame.message,
    ?assert(maps:get(<<"auth_required">>, Message3) =:= false),

    %% Connect without credentials (should succeed again)
    ok = emqx_nats_client:connect(Client3),
    {ok, [ConnectAck2]} = emqx_nats_client:receive_message(Client3),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        ConnectAck2
    ),

    emqx_nats_client:stop(Client),
    emqx_nats_client:stop(Client2),
    emqx_nats_client:stop(Client3).

t_publish_authz(Config) ->
    %% Enable authorization with deny all first
    ok = deny_pubsub_all(),

    %% Create test user and enable auth
    ok = enable_auth(),
    ok = create_test_user(),

    ClientOpts = maps:merge(
        ?config(client_opts, Config),
        #{
            user => <<"test_user">>,
            pass => <<"password">>,
            verbose => true
        }
    ),

    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, [_]} = emqx_nats_client:receive_message(Client),
    ok = emqx_nats_client:connect(Client),
    {ok, [_]} = emqx_nats_client:receive_message(Client),

    %% Test denied topic (should fail)
    ok = emqx_nats_client:publish(Client, <<"test.topic">>, <<"test message">>),
    {ok, [ErrorMsg1]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_ERR,
            message = <<"Permissions Violation for Publish to test.topic">>
        },
        ErrorMsg1
    ),

    %% Allow all publish operations
    ok = allow_pubsub_all(),

    %% Test allowed topic (should succeed)
    ok = emqx_nats_client:publish(Client, <<"test.topic">>, <<"test message">>),
    {ok, [PubAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        PubAck
    ),

    emqx_nats_client:stop(Client),
    ok = delete_test_user(),
    ok = disable_auth().

t_subscribe_authz(Config) ->
    %% Enable authorization with deny all first
    ok = deny_pubsub_all(),

    %% Create test user and enable auth
    ok = enable_auth(),
    ok = create_test_user(),

    ClientOpts = maps:merge(
        ?config(client_opts, Config),
        #{
            user => <<"test_user">>,
            pass => <<"password">>,
            verbose => true
        }
    ),

    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, [_]} = emqx_nats_client:receive_message(Client),
    ok = emqx_nats_client:connect(Client),
    {ok, [_]} = emqx_nats_client:receive_message(Client),

    %% Test denied subscription (should fail)
    ok = emqx_nats_client:subscribe(Client, <<"test.topic">>, <<"sid-1">>),
    {ok, [ErrorMsg1]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_ERR,
            message = <<"Permissions Violation for Subscription to test.topic">>
        },
        ErrorMsg1
    ),

    %% Allow all subscribe operations
    ok = allow_pubsub_all(),

    %% Test allowed subscription (should succeed)
    ok = emqx_nats_client:subscribe(Client, <<"test.topic">>, <<"sid-1">>),
    {ok, [SubAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        SubAck
    ),

    emqx_nats_client:stop(Client),
    ok = delete_test_user(),
    ok = disable_auth().

t_gateway_client_management(Config) ->
    ClientOpts = maps:merge(
        ?config(client_opts, Config),
        #{
            user => <<"test_user">>,
            verbose => true
        }
    ),

    %% Start a client
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, [_]} = emqx_nats_client:receive_message(Client),
    ok = emqx_nats_client:connect(Client),
    {ok, [_]} = emqx_nats_client:receive_message(Client),

    %% Test list clients
    [ClientInfo0] = emqx_gateway_test_utils:list_gateway_clients(<<"nats">>),
    ?assertEqual(<<"test_user">>, maps:get(username, ClientInfo0)),
    %% ClientId assigned by emqx_gateway_nats, it's a random string.
    %% We can get it from the client info.
    ClientId = maps:get(clientid, ClientInfo0),

    %% Test get client info
    ClientInfo = emqx_gateway_test_utils:get_gateway_client(<<"nats">>, ClientId),
    ?assertEqual(ClientId, maps:get(clientid, ClientInfo)),
    ?assertEqual(true, maps:get(connected, ClientInfo)),

    %% Test kick client
    ok = emqx_gateway_test_utils:kick_gateway_client(<<"nats">>, ClientId),
    {ok, [ErrorMsg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_ERR,
            message = <<"Kicked out">>
        },
        ErrorMsg
    ),

    %% Verify client is disconnected
    ?assertEqual([], emqx_gateway_test_utils:list_gateway_clients(<<"nats">>)),

    emqx_nats_client:stop(Client).

t_gateway_client_subscription_management(Config) ->
    ClientOpts = maps:merge(
        ?config(client_opts, Config),
        #{
            user => <<"test_user">>,
            verbose => true
        }
    ),
    Topic = <<"test/subject">>,
    Subject = <<"test.subject">>,
    QueueSubject = <<"test.subject.queue">>,
    Queue = <<"queue-1">>,
    %% Start a client
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, [_]} = emqx_nats_client:receive_message(Client),
    ok = emqx_nats_client:connect(Client),
    {ok, [_]} = emqx_nats_client:receive_message(Client),

    [ClientInfo0] = emqx_gateway_test_utils:list_gateway_clients(<<"nats">>),
    ?assertEqual(<<"test_user">>, maps:get(username, ClientInfo0)),
    ClientId = maps:get(clientid, ClientInfo0),

    %% Create subscription by client
    ok = emqx_nats_client:subscribe(Client, Subject, <<"sid-1">>),
    {ok, [SubAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        SubAck
    ),

    %% Create queue subscription by client
    ok = emqx_nats_client:subscribe(Client, QueueSubject, <<"sid-2">>, Queue),
    {ok, [SubAck2]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        SubAck2
    ),

    %% Verify subscription list
    Subscriptions = emqx_gateway_test_utils:get_gateway_client_subscriptions(<<"nats">>, ClientId),
    ?assertEqual(2, length(Subscriptions)),

    %% XXX: Not implemented yet
    ?assertMatch(
        {400, _},
        emqx_gateway_test_utils:create_gateway_client_subscription(<<"nats">>, ClientId, Topic)
    ),

    %% XXX: Not implemented yet
    ?assertMatch(
        {400, _},
        emqx_gateway_test_utils:delete_gateway_client_subscription(<<"nats">>, ClientId, Topic)
    ),

    %% Delete subscription by client
    ok = emqx_nats_client:unsubscribe(Client, <<"sid-1">>),
    {ok, [UnsubAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        UnsubAck
    ),
    %% Delete queue subscription by client
    ok = emqx_nats_client:unsubscribe(Client, <<"sid-2">>),
    {ok, [UnsubAck2]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        UnsubAck2
    ),

    ?assertEqual(
        [], emqx_gateway_test_utils:get_gateway_client_subscriptions(<<"nats">>, ClientId)
    ),

    emqx_nats_client:stop(Client).

%%--------------------------------------------------------------------
%% Utils

recv_ok_frame(Client) ->
    {ok, [Frame]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        Frame
    ).

recv_info_frame(Client) ->
    {ok, [Frame]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_INFO
        },
        Frame
    ).

find_client_by_username(Username) ->
    ClientInfos = emqx_gateway_test_utils:list_gateway_clients(<<"nats">>),
    lists:filter(
        fun(ClientInfo) ->
            maps:get(username, ClientInfo) =:= Username
        end,
        ClientInfos
    ).
