%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sn_protocol_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-import(
    emqx_gateway_test_utils,
    [
        request/2,
        request/3
    ]
).

-include("emqx_mqttsn.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(HOST, {127, 0, 0, 1}).
-define(PORT, 1884).

-define(LOG(Format, Args), ct:log("TEST: " ++ Format, Args)).

-define(MAX_PRED_TOPIC_ID, ?SN_MAX_PREDEF_TOPIC_ID).
-define(PREDEF_TOPIC_ID1, 1).
-define(PREDEF_TOPIC_ID2, 2).
-define(PREDEF_TOPIC_NAME1, <<"/predefined/topic/name/hello">>).
-define(PREDEF_TOPIC_NAME2, <<"/predefined/topic/name/nice">>).
-define(DEFAULT_PREDEFINED_TOPICS, [
    #{<<"id">> => ?PREDEF_TOPIC_ID1, <<"topic">> => ?PREDEF_TOPIC_NAME1},
    #{<<"id">> => ?PREDEF_TOPIC_ID2, <<"topic">> => ?PREDEF_TOPIC_NAME2}
]).
% FLAG NOT USED
-define(FNU, 0).

%% erlang:system_time should be unique and random enough
-define(CLIENTID,
    iolist_to_binary([
        atom_to_list(?FUNCTION_NAME),
        "-",
        integer_to_list(erlang:system_time())
    ])
).

-elvis([{elvis_style, dont_repeat_yourself, disable}]).

-define(CONF_DEFAULT, <<
    "gateway.mqttsn {\n"
    "  gateway_id = 1\n"
    "  broadcast = true\n"
    "  enable_qos3 = true\n"
    "  predefined = [\n"
    "    { id = 1,\n"
    "      topic = \"/predefined/topic/name/hello\"\n"
    "    },\n"
    "    { id = 2,\n"
    "      topic = \"/predefined/topic/name/nice\"\n"
    "    }\n"
    "  ]\n"
    "  clientinfo_override {\n"
    "    username = \"user1\"\n"
    "    password = \"pw123\"\n"
    "  }\n"
    "  listeners.udp.default {\n"
    "    bind = 1884\n"
    "  }\n"
    "}\n"
>>).

-define(CONF_DTLS, <<
    "\n"
    "gateway.mqttsn {"
    "  listeners.dtls.default {\n"
    "    bind = 1885\n"
    "    dtls_options {\n"
    "      cacertfile = \"${cacertfile}\"\n"
    "      certfile = \"${certfile}\"\n"
    "      keyfile = \"${keyfile}\"\n"
    "    }\n"
    "  }\n"
    "}\n"
>>).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Root = emqx_cth_tls:gen_cert(#{key => ec, issuer => root}),
    Server = emqx_cth_tls:gen_cert(#{key => ec, issuer => Root}),
    {CACertfile, _} = emqx_cth_tls:write_cert(PrivDir, Root),
    {Certfile, Keyfile} = emqx_cth_tls:write_cert(PrivDir, Server),
    Conf = emqx_template:render_strict(
        emqx_template:parse([?CONF_DEFAULT, ?CONF_DTLS]),
        #{
            cacertfile => CACertfile,
            certfile => Certfile,
            keyfile => Keyfile
        }
    ),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, Conf},
            emqx_gateway,
            emqx_auth,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    emqx_common_test_http:create_default_app(),
    [{suite_apps, Apps}, {cacertfile, CACertfile} | Config].

end_per_suite(Config) ->
    {ok, _} = emqx:remove_config([gateway, mqttsn]),
    emqx_common_test_http:delete_default_app(),
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TestCase, Config) ->
    snabbkaffe:start_trace(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    snabbkaffe:stop(),
    ok.

update_mqttsn_with_subs_resume_on() ->
    Conf = emqx:get_raw_config([gateway, mqttsn]),
    emqx_gateway_conf:update_gateway(
        mqttsn,
        Conf#{<<"subs_resume">> => <<"true">>}
    ).

update_mqttsn_with_subs_resume_off() ->
    Conf = emqx:get_raw_config([gateway, mqttsn]),
    emqx_gateway_conf:update_gateway(
        mqttsn,
        Conf#{<<"subs_resume">> => <<"false">>}
    ).

update_mqttsn_with_neg_qos_on() ->
    Conf = emqx:get_raw_config([gateway, mqttsn]),
    emqx_gateway_conf:update_gateway(
        mqttsn,
        Conf#{<<"enable_qos3">> => <<"true">>}
    ).

update_mqttsn_with_neg_qos_off() ->
    Conf = emqx:get_raw_config([gateway, mqttsn]),
    emqx_gateway_conf:update_gateway(
        mqttsn,
        Conf#{<<"enable_qos3">> => <<"false">>}
    ).

update_mqttsn_with_mountpoint(Mp) ->
    Conf = emqx:get_raw_config([gateway, mqttsn]),
    emqx_gateway_conf:update_gateway(
        mqttsn,
        Conf#{<<"mountpoint">> => Mp}
    ).

update_mqttsn_with_predefined_topics(Topics) ->
    Conf = emqx:get_raw_config([gateway, mqttsn]),
    emqx_gateway_conf:update_gateway(
        mqttsn,
        Conf#{<<"predefined">> => Topics}
    ).

default_config() ->
    ?CONF_DEFAULT.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Connect

t_connect(_) ->
    SockName = {'mqttsn:udp:default', 1884},
    ?assertEqual(true, lists:keymember(SockName, 1, esockd:listeners())),

    emqx_gateway_test_utils:meck_emqx_hook_calls(),

    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"client_id_test1">>),

    %% assert: client.connect hook is called
    ?assertMatch(
        ['client.connect' | _],
        emqx_gateway_test_utils:collect_emqx_hooks_calls()
    ),

    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    send_disconnect_msg(Socket, undefined),
    %% assert: mqttsn gateway will ack disconnect msg with DISCONNECT packet
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_update_not_restart_listener(_) ->
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    update_mqttsn_with_mountpoint(<<"mp/">>),

    MsgId = 1,
    TopicId = ?MAX_PRED_TOPIC_ID + 1,
    TopicName1 = <<"abcD">>,
    send_register_msg(Socket, TopicName1, MsgId),
    %% send successfully means the listener is not restarted
    ?assertEqual(<<7, ?SN_REGACK, TopicId:16, MsgId:16, 0:8>>, receive_response(Socket)),

    update_mqttsn_with_mountpoint(<<>>),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_auth_expire(_) ->
    SockName = {'mqttsn:udp:default', 1884},
    ?assertEqual(true, lists:keymember(SockName, 1, esockd:listeners())),

    ok = meck:new(emqx_access_control, [passthrough, no_history]),
    ok = meck:expect(
        emqx_access_control,
        authenticate,
        fun(_) ->
            {ok, #{is_superuser => false, expire_at => erlang:system_time(millisecond) + 100}}
        end
    ),
    ClientId = atom_to_binary(?FUNCTION_NAME),

    {ok, {ok, Event}} =
        ?wait_async_action(
            begin
                {ok, Socket} = gen_udp:open(0, [binary]),
                send_connect_msg(Socket, ClientId),
                ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
                ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
                gen_udp:close(Socket)
            end,
            #{?snk_kind := conn_process_terminated},
            15_000
        ),
    ?assertMatch(
        #{
            ?snk_kind := conn_process_terminated,
            clientid := ClientId,
            reason := {shutdown, expired}
        },
        Event
    ),
    meck:unload().

t_first_disconnect(_) ->
    SockName = {'mqttsn:udp:default', 1884},
    ?assertEqual(true, lists:keymember(SockName, 1, esockd:listeners())),

    {ok, Socket} = gen_udp:open(0, [binary]),
    send_disconnect_msg(Socket, undefined),

    %% assert: mqttsn gateway will ack disconnect msg with DISCONNECT packet
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_connect_dtls(Config) ->
    SockName = {'mqttsn:dtls:default', 1885},
    ?assertEqual(true, lists:keymember(SockName, 1, esockd:listeners())),

    ClientOpts = [
        binary,
        {active, false},
        {protocol, dtls},
        {cacertfile, ?config(cacertfile, Config)}
        | emqx_common_test_helpers:ssl_verify_fun_allow_any_host()
    ],
    {ok, Socket} = ssl:connect(?HOST, 1885, ClientOpts, 1000),
    ok = ssl:send(Socket, make_connect_msg(<<"client_id_test1">>, 1)),
    ?assertEqual({ok, <<3, ?SN_CONNACK, 0>>}, ssl:recv(Socket, 0, 1000)),

    ok = ssl:send(Socket, make_disconnect_msg(undefined)),
    ?assertEqual({ok, <<2, ?SN_DISCONNECT>>}, ssl:recv(Socket, 0, 1000)),
    ssl:close(Socket).

t_subscribe(_) ->
    Dup = 0,
    QoS = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId = ?MAX_PRED_TOPIC_ID + 1,
    ReturnCode = 0,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    TopicName1 = <<"abcD">>,
    send_register_msg(Socket, TopicName1, MsgId),
    ?assertEqual(<<7, ?SN_REGACK, TopicId:16, MsgId:16, 0:8>>, receive_response(Socket)),
    send_subscribe_msg_normal_topic(Socket, QoS, TopicName1, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId:16, MsgId:16, ReturnCode>>,
        receive_response(Socket)
    ),
    ?assert(lists:member(TopicName1, emqx_broker:topics())),

    send_unsubscribe_msg_normal_topic(Socket, TopicName1, MsgId),
    ?assertEqual(<<4, ?SN_UNSUBACK, MsgId:16>>, receive_response(Socket)),
    timer:sleep(100),
    ?assertNot(lists:member(TopicName1, emqx_broker:topics())),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_subscribe_case01(_) ->
    Dup = 0,
    QoS = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId = ?MAX_PRED_TOPIC_ID + 1,
    ReturnCode = 0,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    TopicName1 = <<"abcD">>,
    send_register_msg(Socket, TopicName1, MsgId),
    ?assertEqual(<<7, ?SN_REGACK, TopicId:16, MsgId:16, 0:8>>, receive_response(Socket)),
    send_subscribe_msg_normal_topic(Socket, QoS, TopicName1, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId:16, MsgId:16, ReturnCode>>,
        receive_response(Socket)
    ),

    send_unsubscribe_msg_normal_topic(Socket, TopicName1, MsgId),
    ?assertEqual(<<4, ?SN_UNSUBACK, MsgId:16>>, receive_response(Socket)),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    gen_udp:close(Socket).

t_subscribe_case02(_) ->
    Dup = 0,
    QoS = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId = ?PREDEF_TOPIC_ID1,
    ReturnCode = 0,
    {ok, Socket} = gen_udp:open(0, [binary]),

    send_connect_msg(Socket, ?CLIENTID),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    Topic1 = ?PREDEF_TOPIC_NAME1,
    send_register_msg(Socket, Topic1, MsgId),
    ?assertEqual(<<7, ?SN_REGACK, TopicId:16, MsgId:16, 0:8>>, receive_response(Socket)),
    send_subscribe_msg_predefined_topic(Socket, QoS, TopicId, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId:16, MsgId:16, ReturnCode>>,
        receive_response(Socket)
    ),

    send_unsubscribe_msg_predefined_topic(Socket, TopicId, MsgId),
    ?assertEqual(<<4, ?SN_UNSUBACK, MsgId:16>>, receive_response(Socket)),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    gen_udp:close(Socket).

t_subscribe_case03(_) ->
    Dup = 0,
    QoS = 2,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId = 0,
    ReturnCode = 0,
    {ok, Socket} = gen_udp:open(0, [binary]),

    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    send_subscribe_msg_short_topic(Socket, QoS, <<"te">>, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId:16, MsgId:16, ReturnCode>>,
        receive_response(Socket)
    ),

    send_unsubscribe_msg_short_topic(Socket, <<"te">>, MsgId),
    ?assertEqual(<<4, ?SN_UNSUBACK, MsgId:16>>, receive_response(Socket)),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

%% In this case We use predefined topic name to register and subscribe,
%% and expect to receive the corresponding predefined topic id but not a new
%% generated topic id from broker. We design this case to illustrate
%% MQTT-SN Gateway's compatibility of dealing with predefined and normal
%% topics.
%%
%% Once we give more restrictions to different topic id type, this case
%% would be deleted or modified.
t_subscribe_case04(_) ->
    Dup = 0,
    QoS = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId = ?PREDEF_TOPIC_ID1,
    ReturnCode = 0,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    Topic1 = ?PREDEF_TOPIC_NAME1,
    send_register_msg(Socket, Topic1, MsgId),
    ?assertEqual(<<7, ?SN_REGACK, TopicId:16, MsgId:16, 0:8>>, receive_response(Socket)),
    send_subscribe_msg_normal_topic(Socket, QoS, Topic1, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId:16, MsgId:16, ReturnCode>>,
        receive_response(Socket)
    ),

    send_unsubscribe_msg_normal_topic(Socket, Topic1, MsgId),
    ?assertEqual(<<4, ?SN_UNSUBACK, MsgId:16>>, receive_response(Socket)),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    gen_udp:close(Socket).

t_subscribe_case05(_) ->
    Dup = 0,
    QoS = 1,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 25,
    TopicId0 = 0,
    TopicId1 = ?MAX_PRED_TOPIC_ID + 1,
    TopicId2 = ?MAX_PRED_TOPIC_ID + 2,
    ReturnCode = 0,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    send_register_msg(Socket, <<"abcD">>, MsgId),
    ?assertEqual(<<7, ?SN_REGACK, TopicId1:16, MsgId:16, 0:8>>, receive_response(Socket)),

    send_subscribe_msg_normal_topic(Socket, QoS, <<"abcD">>, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId1:16, MsgId:16, ReturnCode>>,
        receive_response(Socket)
    ),

    send_subscribe_msg_normal_topic(Socket, QoS, <<"/sport/#">>, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId0:16, MsgId:16, ReturnCode>>,
        receive_response(Socket)
    ),

    send_subscribe_msg_normal_topic(Socket, QoS, <<"/a/+/water">>, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId0:16, MsgId:16, ReturnCode>>,
        receive_response(Socket)
    ),

    send_subscribe_msg_normal_topic(Socket, QoS, <<"/Tom/Home">>, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId2:16, MsgId:16, ReturnCode>>,
        receive_response(Socket)
    ),
    send_unsubscribe_msg_normal_topic(Socket, <<"abcD">>, MsgId),
    ?assertEqual(<<4, ?SN_UNSUBACK, MsgId:16>>, receive_response(Socket)),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_subscribe_case06(_) ->
    Dup = 0,
    QoS = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId0 = 0,
    TopicId1 = ?MAX_PRED_TOPIC_ID + 1,
    TopicId2 = ?MAX_PRED_TOPIC_ID + 2,
    ReturnCode = 0,
    ClientId = ?CLIENTID,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, ClientId),

    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_register_msg(Socket, <<"abc">>, MsgId),
    ?assertEqual(
        <<7, ?SN_REGACK, TopicId1:16, MsgId:16, 0:8>>,
        receive_response(Socket)
    ),

    send_register_msg(Socket, <<"/blue/#">>, MsgId),
    ?assertEqual(
        <<7, ?SN_REGACK, TopicId0:16, MsgId:16, ?SN_RC_NOT_SUPPORTED:8>>,
        receive_response(Socket)
    ),

    send_register_msg(Socket, <<"/blue/+/white">>, MsgId),
    ?assertEqual(
        <<7, ?SN_REGACK, TopicId0:16, MsgId:16, ?SN_RC_NOT_SUPPORTED:8>>,
        receive_response(Socket)
    ),
    send_register_msg(Socket, <<"/$sys/rain">>, MsgId),
    ?assertEqual(
        <<7, ?SN_REGACK, TopicId2:16, MsgId:16, 0:8>>,
        receive_response(Socket)
    ),

    send_subscribe_msg_short_topic(Socket, QoS, <<"Q2">>, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId0:16, MsgId:16, ReturnCode>>,
        receive_response(Socket)
    ),

    send_unsubscribe_msg_normal_topic(Socket, <<"Q2">>, MsgId),
    ?assertEqual(<<4, ?SN_UNSUBACK, MsgId:16>>, receive_response(Socket)),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_subscribe_case07(_) ->
    Dup = 0,
    QoS = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId1 = ?MAX_PRED_TOPIC_ID + 2,
    TopicId2 = ?MAX_PRED_TOPIC_ID + 3,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    send_subscribe_msg_predefined_topic(Socket, QoS, TopicId1, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            ?SN_INVALID_TOPIC_ID:16, MsgId:16, ?SN_RC_INVALID_TOPIC_ID>>,
        receive_response(Socket)
    ),

    send_unsubscribe_msg_predefined_topic(Socket, TopicId2, MsgId),
    ?assertEqual(<<4, ?SN_UNSUBACK, MsgId:16>>, receive_response(Socket)),
    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_subscribe_case08(_) ->
    Dup = 0,
    QoS = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId2 = 2,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    send_subscribe_msg_reserved_topic(Socket, QoS, TopicId2, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            ?SN_INVALID_TOPIC_ID:16, MsgId:16, ?SN_RC_NOT_SUPPORTED>>,
        receive_response(Socket)
    ),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_subscribe_predefined_topic(_) ->
    Dup = 0,
    QoS = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    Socket = ensure_connected_client(?CLIENTID),
    send_subscribe_msg_predefined_topic(Socket, 0, ?PREDEF_TOPIC_ID1, 1),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            ?PREDEF_TOPIC_ID1:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),
    send_disconnect_msg(Socket, undefined),
    gen_udp:close(Socket),

    update_mqttsn_with_predefined_topics([]),
    Socket1 = ensure_connected_client(?CLIENTID),
    send_subscribe_msg_predefined_topic(Socket1, 0, ?PREDEF_TOPIC_ID1, 1),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2, 0:16,
            MsgId:16, ?SN_RC_INVALID_TOPIC_ID>>,
        receive_response(Socket1)
    ),
    send_disconnect_msg(Socket1, undefined),
    update_mqttsn_with_predefined_topics(?DEFAULT_PREDEFINED_TOPICS),
    gen_udp:close(Socket1).

t_publish_negqos_enabled(_) ->
    Dup = 0,
    QoS = 0,
    NegQoS = 3,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId1 = ?MAX_PRED_TOPIC_ID + 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    Topic = <<"abc">>,

    send_subscribe_msg_normal_topic(Socket, QoS, Topic, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId1:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),
    MsgId1 = 3,
    Payload1 = <<20, 21, 22, 23>>,
    send_publish_msg_normal_topic(Socket, NegQoS, MsgId1, TopicId1, Payload1),
    timer:sleep(100),
    Eexp =
        <<11, ?SN_PUBLISH, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId1:16, (mid(0)):16, <<20, 21, 22, 23>>/binary>>,
    What = receive_response(Socket),
    ?assertEqual(Eexp, What),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_publish_negqos_disabled(_) ->
    update_mqttsn_with_neg_qos_off(),
    NegQoS = 3,
    MsgId = 1,
    Payload = <<"abc">>,
    TopicId = ?MAX_PRED_TOPIC_ID,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ?check_trace(
        begin
            send_publish_msg_predefined_topic(Socket, NegQoS, MsgId, TopicId, Payload),
            ?assertEqual(
                <<7, ?SN_PUBACK, TopicId:16, MsgId:16, ?SN_RC_NOT_SUPPORTED>>,
                receive_response(Socket)
            ),
            receive_response(Socket)
        end,
        fun(Trace0) ->
            Trace = ?of_kind(ignore_negative_qos, Trace0),
            ?assertMatch([#{return_code := ?SN_RC_NOT_SUPPORTED}], Trace)
        end
    ),
    update_mqttsn_with_neg_qos_on(),
    gen_udp:close(Socket).

t_publish_qos0_case01(_) ->
    Dup = 0,
    QoS = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId1 = ?MAX_PRED_TOPIC_ID + 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    Topic = <<"abc">>,
    send_subscribe_msg_normal_topic(Socket, QoS, Topic, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId1:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),
    MsgId1 = 3,
    Payload1 = <<20, 21, 22, 23>>,
    send_publish_msg_normal_topic(Socket, QoS, MsgId1, TopicId1, Payload1),
    timer:sleep(100),

    Eexp =
        <<11, ?SN_PUBLISH, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId1:16, (mid(0)):16, <<20, 21, 22, 23>>/binary>>,
    What = receive_response(Socket),
    ?assertEqual(Eexp, What),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_publish_qos0_case02(_) ->
    Dup = 0,
    QoS = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    PredefTopicId = ?PREDEF_TOPIC_ID1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    send_subscribe_msg_predefined_topic(Socket, QoS, PredefTopicId, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            PredefTopicId:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),

    MsgId1 = 3,
    Payload1 = <<20, 21, 22, 23>>,
    send_publish_msg_predefined_topic(Socket, QoS, MsgId1, PredefTopicId, Payload1),
    timer:sleep(100),

    Eexp =
        <<11, ?SN_PUBLISH, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_PREDEFINED_TOPIC:2,
            PredefTopicId:16, (mid(0)):16, <<20, 21, 22, 23>>/binary>>,
    What = receive_response(Socket),
    ?assertEqual(Eexp, What),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_publish_qos0_case3(_) ->
    Dup = 0,
    QoS = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId = ?MAX_PRED_TOPIC_ID + 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    Topic = <<"/a/b/c">>,
    send_subscribe_msg_normal_topic(Socket, QoS, Topic, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),

    MsgId1 = 3,
    Payload1 = <<20, 21, 22, 23>>,
    send_publish_msg_predefined_topic(Socket, QoS, MsgId1, TopicId, Payload1),
    timer:sleep(100),

    Eexp =
        <<11, ?SN_PUBLISH, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId:16, (mid(0)):16, <<20, 21, 22, 23>>/binary>>,
    What = receive_response(Socket),
    ?assertEqual(Eexp, What),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_publish_qos0_case04(_) ->
    Dup = 0,
    QoS = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId0 = 0,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    send_subscribe_msg_normal_topic(Socket, QoS, <<"#">>, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId0:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),

    MsgId1 = 2,
    Payload1 = <<20, 21, 22, 23>>,
    Topic = <<"TR">>,
    send_publish_msg_short_topic(Socket, QoS, MsgId1, Topic, Payload1),
    timer:sleep(100),

    Eexp =
        <<11, ?SN_PUBLISH, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_SHORT_TOPIC:2,
            Topic/binary, (mid(0)):16, <<20, 21, 22, 23>>/binary>>,
    What = receive_response(Socket),
    ?assertEqual(Eexp, What),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_publish_qos0_case05(_) ->
    Dup = 0,
    QoS = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId0 = 0,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_subscribe_msg_short_topic(Socket, QoS, <<"/#">>, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId0:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),
    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    gen_udp:close(Socket).

t_publish_qos0_case06(_) ->
    Dup = 0,
    QoS = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId1 = ?MAX_PRED_TOPIC_ID + 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    Topic = <<"abc">>,
    send_subscribe_msg_normal_topic(Socket, QoS, Topic, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId1:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),

    MsgId1 = 3,
    Payload1 = <<20, 21, 22, 23>>,
    send_publish_msg_normal_topic(Socket, QoS, MsgId1, TopicId1, Payload1),
    timer:sleep(100),

    Eexp =
        <<11, ?SN_PUBLISH, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId1:16, (mid(0)):16, <<20, 21, 22, 23>>/binary>>,
    What = receive_response(Socket),
    ?assertEqual(Eexp, What),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_publish_qos1_case01(_) ->
    Dup = 0,
    QoS = 1,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId1 = ?MAX_PRED_TOPIC_ID + 1,
    Topic = <<"abc">>,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_subscribe_msg_normal_topic(Socket, QoS, Topic, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId1:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),

    Payload1 = <<20, 21, 22, 23>>,
    send_publish_msg_normal_topic(Socket, QoS, MsgId, TopicId1, Payload1),
    ?assertEqual(
        <<7, ?SN_PUBACK, TopicId1:16, MsgId:16, ?SN_RC_ACCEPTED>>, receive_response(Socket)
    ),
    timer:sleep(100),

    ?assertEqual(
        <<11, ?SN_PUBLISH, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId1:16, MsgId:16, <<20, 21, 22, 23>>/binary>>,
        receive_response(Socket)
    ),

    send_disconnect_msg(Socket, undefined),
    gen_udp:close(Socket).

t_publish_qos1_case02(_) ->
    Dup = 0,
    QoS = 1,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    PredefTopicId = ?PREDEF_TOPIC_ID1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    send_subscribe_msg_predefined_topic(Socket, QoS, PredefTopicId, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            PredefTopicId:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),

    Payload1 = <<20, 21, 22, 23>>,
    send_publish_msg_predefined_topic(Socket, QoS, MsgId, PredefTopicId, Payload1),
    ?assertEqual(
        <<7, ?SN_PUBACK, PredefTopicId:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),
    timer:sleep(100),

    send_disconnect_msg(Socket, undefined),
    gen_udp:close(Socket).

t_publish_qos1_case03(_) ->
    QoS = 1,
    MsgId = 1,
    TopicId5 = 5,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_publish_msg_predefined_topic(Socket, QoS, MsgId, tid(5), <<20, 21, 22, 23>>),
    ?assertEqual(
        <<7, ?SN_PUBACK, TopicId5:16, MsgId:16, ?SN_RC_INVALID_TOPIC_ID>>,
        receive_response(Socket)
    ),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_publish_qos1_case04(_) ->
    Dup = 0,
    QoS = 1,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 7,
    TopicId0 = 0,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_subscribe_msg_short_topic(Socket, QoS, <<"ab">>, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId0:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),

    Topic = <<"ab">>,
    Payload1 = <<20, 21, 22, 23>>,
    send_publish_msg_short_topic(Socket, QoS, MsgId, Topic, Payload1),
    <<TopicIdShort:16>> = Topic,
    ?assertEqual(
        <<7, ?SN_PUBACK, TopicIdShort:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),
    timer:sleep(100),

    send_disconnect_msg(Socket, undefined),
    gen_udp:close(Socket).

t_publish_qos1_case05(_) ->
    Dup = 0,
    QoS = 1,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 7,
    TopicId1 = ?MAX_PRED_TOPIC_ID + 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    send_subscribe_msg_normal_topic(Socket, QoS, <<"ab">>, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId1:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),

    send_publish_msg_short_topic(Socket, QoS, MsgId, <<"/#">>, <<20, 21, 22, 23>>),
    <<TopicIdShort:16>> = <<"/#">>,
    ?assertEqual(
        <<7, ?SN_PUBACK, TopicIdShort:16, MsgId:16, ?SN_RC_NOT_SUPPORTED>>,
        receive_response(Socket)
    ),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_publish_qos1_case06(_) ->
    Dup = 0,
    QoS = 1,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 7,
    TopicId1 = ?MAX_PRED_TOPIC_ID + 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    send_subscribe_msg_normal_topic(Socket, QoS, <<"ab">>, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId1:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),

    send_publish_msg_short_topic(Socket, QoS, MsgId, <<"/+">>, <<20, 21, 22, 23>>),
    <<TopicIdShort:16>> = <<"/+">>,
    ?assertEqual(
        <<7, ?SN_PUBACK, TopicIdShort:16, MsgId:16, ?SN_RC_NOT_SUPPORTED>>, receive_response(Socket)
    ),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_publish_qos2_case01(_) ->
    Dup = 0,
    QoS = 2,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 7,
    TopicId1 = ?MAX_PRED_TOPIC_ID + 1,
    Topic = <<"/abc">>,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_subscribe_msg_normal_topic(Socket, QoS, Topic, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, ?FNU:1, QoS:2, ?FNU:5, TopicId1:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),
    Payload1 = <<20, 21, 22, 23>>,

    send_publish_msg_normal_topic(Socket, QoS, MsgId, TopicId1, Payload1),
    ?assertEqual(<<4, ?SN_PUBREC, MsgId:16>>, receive_response(Socket)),
    send_pubrel_msg(Socket, MsgId),
    ?assertEqual(
        <<11, ?SN_PUBLISH, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId1:16, 1:16, <<20, 21, 22, 23>>/binary>>,
        receive_response(Socket)
    ),
    ?assertEqual(<<4, ?SN_PUBCOMP, MsgId:16>>, receive_response(Socket)),
    timer:sleep(100),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_publish_qos2_case02(_) ->
    Dup = 0,
    QoS = 2,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 7,
    PredefTopicId = ?PREDEF_TOPIC_ID2,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    send_subscribe_msg_predefined_topic(Socket, QoS, PredefTopicId, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, ?FNU:1, QoS:2, ?FNU:5, PredefTopicId:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),

    Payload1 = <<20, 21, 22, 23>>,
    send_publish_msg_predefined_topic(Socket, QoS, MsgId, PredefTopicId, Payload1),
    ?assertEqual(<<4, ?SN_PUBREC, MsgId:16>>, receive_response(Socket)),
    send_pubrel_msg(Socket, MsgId),

    ?assertEqual(
        <<11, ?SN_PUBLISH, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_PREDEFINED_TOPIC:2,
            PredefTopicId:16, 1:16, <<20, 21, 22, 23>>/binary>>,
        receive_response(Socket)
    ),
    ?assertEqual(<<4, ?SN_PUBCOMP, MsgId:16>>, receive_response(Socket)),

    timer:sleep(100),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_publish_qos2_case03(_) ->
    Dup = 0,
    QoS = 2,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 7,
    TopicId0 = 0,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    send_subscribe_msg_normal_topic(Socket, QoS, <<"/#">>, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, ?FNU:1, QoS:2, ?FNU:5, TopicId0:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),

    Payload1 = <<20, 21, 22, 23>>,
    send_publish_msg_short_topic(Socket, QoS, MsgId, <<"/a">>, Payload1),
    ?assertEqual(<<4, ?SN_PUBREC, MsgId:16>>, receive_response(Socket)),
    send_pubrel_msg(Socket, MsgId),

    ?assertEqual(
        <<11, ?SN_PUBLISH, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_SHORT_TOPIC:2,
            <<"/a">>/binary, 1:16, <<20, 21, 22, 23>>/binary>>,
        receive_response(Socket)
    ),
    ?assertEqual(<<4, ?SN_PUBCOMP, MsgId:16>>, receive_response(Socket)),
    timer:sleep(100),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_publish_mountpoint(_) ->
    update_mqttsn_with_mountpoint(<<"mp/">>),
    Dup = 0,
    QoS = 1,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId1 = ?MAX_PRED_TOPIC_ID + 1,
    Topic = <<"abc">>,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_subscribe_msg_normal_topic(Socket, QoS, Topic, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId1:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),

    Payload1 = <<20, 21, 22, 23>>,
    send_publish_msg_normal_topic(Socket, QoS, MsgId, TopicId1, Payload1),
    ?assertEqual(
        <<7, ?SN_PUBACK, TopicId1:16, MsgId:16, ?SN_RC_ACCEPTED>>, receive_response(Socket)
    ),
    timer:sleep(100),

    ?assertEqual(
        <<11, ?SN_PUBLISH, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId1:16, MsgId:16, <<20, 21, 22, 23>>/binary>>,
        receive_response(Socket)
    ),

    send_disconnect_msg(Socket, undefined),
    update_mqttsn_with_mountpoint(<<>>),
    gen_udp:close(Socket).

t_delivery_qos1_register_invalid_topic_id(_) ->
    Dup = 0,
    QoS = 1,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId = ?MAX_PRED_TOPIC_ID + 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    send_subscribe_msg_normal_topic(Socket, QoS, <<"ab">>, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId:16, MsgId:16, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),

    Payload = <<"test-registration-inconsistent">>,
    _ = emqx:publish(emqx_message:make(test, ?QOS_1, <<"ab">>, Payload)),

    ?assertEqual(
        <<
            (7 + byte_size(Payload)),
            ?SN_PUBLISH,
            Dup:1,
            QoS:2,
            Retain:1,
            Will:1,
            CleanSession:1,
            ?SN_NORMAL_TOPIC:2,
            TopicId:16,
            MsgId:16,
            Payload/binary
        >>,
        receive_response(Socket)
    ),
    %% acked with ?SN_RC_INVALID_TOPIC_ID to
    send_puback_msg(Socket, TopicId, MsgId, ?SN_RC_INVALID_TOPIC_ID),

    ?assertMatch(
        {TopicId, _},
        check_register_msg_on_udp(<<"ab">>, receive_response(Socket))
    ),
    send_regack_msg(Socket, TopicId, MsgId + 1),

    %% receive the replay message
    ?assertEqual(
        <<
            (7 + byte_size(Payload)),
            ?SN_PUBLISH,
            Dup:1,
            QoS:2,
            Retain:1,
            Will:1,
            CleanSession:1,
            ?SN_NORMAL_TOPIC:2,
            TopicId:16,
            (MsgId):16,
            Payload/binary
        >>,
        receive_response(Socket)
    ),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_will_case01(_) ->
    QoS = 1,
    Duration = 1,
    WillMsg = <<10, 11, 12, 13, 14>>,
    WillTopic = <<"abc">>,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,

    ok = emqx_broker:subscribe(WillTopic),

    send_connect_msg_with_will(Socket, Duration, ClientId),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),

    send_willtopic_msg(Socket, WillTopic, QoS),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),

    send_willmsg_msg(Socket, WillMsg),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    send_pingreq_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),

    % wait udp client keepalive timeout
    timer:sleep(3000),

    receive
        {deliver, WillTopic, #message{payload = WillMsg}} ->
            ok
    after 1000 -> ct:fail(wait_willmsg_timeout)
    end,
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    gen_udp:close(Socket).

t_will_test2(_) ->
    QoS = 2,
    Duration = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),

    ClientId = ?CLIENTID,
    send_connect_msg_with_will(Socket, Duration, ClientId),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_msg(Socket, <<"goodbye">>, QoS),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),
    send_willmsg_msg(Socket, <<10, 11, 12, 13, 14>>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_pingreq_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),

    timer:sleep(4000),

    % ignore PUBACK
    receive_response(Socket),
    % ignore PUBCOMP
    receive_response(Socket),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    gen_udp:close(Socket).

t_will_test3(_) ->
    Duration = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),

    ClientId = ?CLIENTID,
    send_connect_msg_with_will(Socket, Duration, ClientId),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_empty_msg(Socket),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_pingreq_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),

    timer:sleep(4000),

    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    gen_udp:close(Socket).

t_will_test4(_) ->
    QoS = 1,
    Duration = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),

    ClientId = ?CLIENTID,
    send_connect_msg_with_will(Socket, Duration, ClientId),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_msg(Socket, <<"abc">>, QoS),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),
    send_willmsg_msg(Socket, <<10, 11, 12, 13, 14>>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_pingreq_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),
    send_willtopicupd_msg(Socket, <<"/XYZ">>, ?QOS_1),
    ?assertEqual(<<3, ?SN_WILLTOPICRESP, ?SN_RC_ACCEPTED>>, receive_response(Socket)),
    send_willmsgupd_msg(Socket, <<"1A2B3C">>),
    ?assertEqual(<<3, ?SN_WILLMSGRESP, ?SN_RC_ACCEPTED>>, receive_response(Socket)),

    timer:sleep(4000),

    % ignore PUBACK
    receive_response(Socket),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    gen_udp:close(Socket).

t_will_test5(_) ->
    QoS = 1,
    Duration = 5,
    {ok, Socket} = gen_udp:open(0, [binary]),

    ClientId = ?CLIENTID,
    send_connect_msg_with_will(Socket, Duration, ClientId),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_msg(Socket, <<"abc">>, QoS),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),
    send_willmsg_msg(Socket, <<10, 11, 12, 13, 14>>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_pingreq_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),
    send_willtopicupd_empty_msg(Socket),
    ?assertEqual(<<3, ?SN_WILLTOPICRESP, ?SN_RC_ACCEPTED>>, receive_response(Socket)),

    timer:sleep(1000),

    ?assertEqual(udp_receive_timeout, receive_response(Socket)),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    gen_udp:close(Socket).

t_will_case06(_) ->
    QoS = 1,
    Duration = 1,
    WillMsg = <<10, 11, 12, 13, 14>>,
    WillTopic = <<"abc">>,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,

    ok = emqx_broker:subscribe(WillTopic),

    send_connect_msg_with_will1(Socket, Duration, ClientId),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),

    send_willtopic_msg(Socket, WillTopic, QoS),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),

    send_willmsg_msg(Socket, WillMsg),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    send_pingreq_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),

    % wait udp client keepalive timeout
    timer:sleep(3000),

    receive
        {deliver, WillTopic, #message{payload = WillMsg}} -> ok;
        Msg -> ct:print("received --- unex: ~p", [Msg])
    after 1000 -> ct:fail(wait_willmsg_timeout)
    end,
    send_disconnect_msg(Socket, undefined),

    gen_udp:close(Socket).

t_asleep_test01_timeout(_) ->
    QoS = 1,
    Duration = 1,
    WillTopic = <<"dead">>,
    WillPayload = <<10, 11, 12, 13, 14>>,
    {ok, Socket} = gen_udp:open(0, [binary]),

    ClientId = ?CLIENTID,
    send_connect_msg_with_will(Socket, Duration, ClientId),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_msg(Socket, WillTopic, QoS),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),
    send_willmsg_msg(Socket, WillPayload),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    send_disconnect_msg(Socket, 1),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    %% asleep timer should get timeout, and device is lost
    timer:sleep(3000),

    ?assertEqual([], emqx_gateway_cm:lookup_by_clientid(mqttsn, ClientId)),

    gen_udp:close(Socket).

t_asleep_test02_to_awake_and_back(_) ->
    QoS = 1,
    KeepaliveDuration = 1,
    SleepDuration = 5,
    WillTopic = <<"dead">>,
    WillPayload = <<10, 11, 12, 13, 14>>,
    {ok, Socket} = gen_udp:open(0, [binary]),

    ClientId = ?CLIENTID,
    send_connect_msg_with_will(Socket, KeepaliveDuration, ClientId),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_msg(Socket, WillTopic, QoS),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),
    send_willmsg_msg(Socket, WillPayload),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    % goto asleep state
    send_disconnect_msg(Socket, SleepDuration),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    timer:sleep(4500),

    % goto awake state and back
    send_pingreq_msg(Socket, ClientId),
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),

    timer:sleep(4500),

    % goto awake state and back
    send_pingreq_msg(Socket, ClientId),
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),

    %% during above procedure, mqtt keepalive timer should not terminate mqtt-sn process

    %% asleep timer should get timeout, and device should get lost
    timer:sleep(8000),

    ?assertEqual([], emqx_gateway_cm:lookup_by_clientid(mqttsn, ClientId)),

    gen_udp:close(Socket).

t_asleep_test03_to_awake_qos1_dl_msg(_) ->
    QoS = 1,
    Duration = 5,
    WillTopic = <<"dead">>,
    WillPayload = <<10, 11, 12, 13, 14>>,
    MsgId = 1000,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg_with_will(Socket, Duration, ClientId),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_msg(Socket, WillTopic, QoS),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),
    send_willmsg_msg(Socket, WillPayload),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    % subscribe
    TopicName1 = <<"abc">>,
    MsgId1 = 25,
    TopicId1 = ?MAX_PRED_TOPIC_ID + 1,
    WillBit = 0,
    Dup = 0,
    Retain = 0,
    CleanSession = 0,
    ReturnCode = 0,
    Payload1 = <<55, 66, 77, 88, 99>>,

    send_register_msg(Socket, TopicName1, MsgId1),
    ?assertEqual(<<7, ?SN_REGACK, TopicId1:16, MsgId1:16, 0:8>>, receive_response(Socket)),
    send_subscribe_msg_predefined_topic(Socket, QoS, TopicId1, MsgId),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, WillBit:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId1:16, MsgId:16, ReturnCode>>,
        receive_response(Socket)
    ),

    % goto asleep state
    send_disconnect_msg(Socket, 1),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    timer:sleep(300),

    emqx_broker:publish(
        emqx_message:make(<<"ct">>, QoS, TopicName1, Payload1)
    ),

    timer:sleep(50),

    % goto awake state, receive downlink messages, and go back to asleep
    send_pingreq_msg(Socket, ClientId),

    %% the broker should sent dl msgs to the awake client before sending the pingresp
    UdpData = receive_response(Socket),
    MsgId_udp = check_publish_msg_on_udp(
        {Dup, QoS, Retain, WillBit, CleanSession, ?SN_NORMAL_TOPIC, TopicId1, Payload1}, UdpData
    ),
    send_puback_msg(Socket, TopicId1, MsgId_udp),

    %% check the pingresp is received at last
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),

    timer:sleep(5000),

    ?assertEqual([], emqx_gateway_cm:lookup_by_clientid(mqttsn, ClientId)),

    gen_udp:close(Socket).

t_asleep_test04_to_awake_qos1_dl_msg(_) ->
    QoS = 1,
    Duration = 5,
    WillTopic = <<"dead">>,
    WillPayload = <<10, 11, 12, 13, 14>>,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg_with_will(Socket, Duration, ClientId),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_msg(Socket, WillTopic, QoS),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),
    send_willmsg_msg(Socket, WillPayload),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    % subscribe
    TopicName1 = <<"a/+/c">>,
    MsgId1 = 25,
    TopicId0 = 0,
    WillBit = 0,
    Dup = 0,
    Retain = 0,
    CleanSession = 0,
    ReturnCode = 0,
    send_subscribe_msg_normal_topic(Socket, QoS, TopicName1, MsgId1),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, WillBit:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId0:16, MsgId1:16, ReturnCode>>,
        receive_response(Socket)
    ),

    % goto asleep state
    send_disconnect_msg(Socket, 1),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    timer:sleep(300),

    %% send downlink data in asleep state. This message should be send to device once it wake up
    Payload1 = <<55, 66, 77, 88, 99>>,
    Payload2 = <<55, 66, 77, 88, 100>>,

    {ok, C} = emqtt:start_link(),
    {ok, _} = emqtt:connect(C),
    {ok, _} = emqtt:publish(C, <<"a/b/c">>, Payload1, QoS),
    {ok, _} = emqtt:publish(C, <<"a/b/c">>, Payload2, QoS),
    timer:sleep(100),
    ok = emqtt:disconnect(C),

    timer:sleep(300),

    % goto awake state, receive downlink messages, and go back to asleep
    send_pingreq_msg(Socket, ClientId),

    %% 1. get REGISTER first, since this topic has never been registered
    UdpData1 = receive_response(Socket),
    {TopicIdNew, MsgId3} = check_register_msg_on_udp(<<"a/b/c">>, UdpData1),

    %% 2. but before we reply the REGACK, the sn-gateway should not send any PUBLISH
    ?assertError(_, receive_publish(Socket)),

    send_regack_msg(Socket, TopicIdNew, MsgId3),

    UdpData2 = receive_response(Socket),
    MsgId_udp2 = check_publish_msg_on_udp(
        {Dup, QoS, Retain, WillBit, CleanSession, ?SN_NORMAL_TOPIC, TopicIdNew, Payload1}, UdpData2
    ),
    send_puback_msg(Socket, TopicIdNew, MsgId_udp2),

    UdpData3 = receive_response(Socket),
    MsgId_udp3 = check_publish_msg_on_udp(
        {Dup, QoS, Retain, WillBit, CleanSession, ?SN_NORMAL_TOPIC, TopicIdNew, Payload2}, UdpData3
    ),
    send_puback_msg(Socket, TopicIdNew, MsgId_udp3),

    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),

    timer:sleep(8000),

    ?assertEqual([], emqx_gateway_cm:lookup_by_clientid(mqttsn, ClientId)),

    gen_udp:close(Socket).

receive_publish(Socket) ->
    UdpData3 = receive_response(Socket, 1000),
    <<HeaderUdp:5/binary, _:16, _/binary>> = UdpData3,
    <<_:8, ?SN_PUBLISH, _/binary>> = HeaderUdp.

t_asleep_test05_to_awake_qos1_dl_msg(_) ->
    QoS = 1,
    Duration = 5,
    WillTopic = <<"dead">>,
    WillPayload = <<10, 11, 12, 13, 14>>,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg_with_will(Socket, Duration, ClientId),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_msg(Socket, WillTopic, QoS),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),
    send_willmsg_msg(Socket, WillPayload),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    % subscribe
    TopicName1 = <<"u/+/w">>,
    MsgId1 = 25,
    TopicId0 = 0,
    WillBit = 0,
    Dup = 0,
    Retain = 0,
    CleanSession = 0,
    ReturnCode = 0,
    send_subscribe_msg_normal_topic(Socket, QoS, TopicName1, MsgId1),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, WillBit:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId0:16, MsgId1:16, ReturnCode>>,
        receive_response(Socket)
    ),

    % goto asleep state
    SleepDuration = 5,
    send_disconnect_msg(Socket, SleepDuration),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    timer:sleep(300),

    %% send downlink data in asleep state. This message should be send to device once it wake up
    Payload2 = <<55, 66, 77, 88, 99>>,
    Payload3 = <<61, 71, 81>>,
    Payload4 = <<100, 101, 102, 103, 104, 105, 106, 107>>,
    TopicName_test5 = <<"u/v/w">>,
    {ok, C} = emqtt:start_link(),
    {ok, _} = emqtt:connect(C),
    {ok, _} = emqtt:publish(C, TopicName_test5, Payload2, QoS),
    timer:sleep(100),
    {ok, _} = emqtt:publish(C, TopicName_test5, Payload3, QoS),
    timer:sleep(100),
    {ok, _} = emqtt:publish(C, TopicName_test5, Payload4, QoS),
    timer:sleep(200),
    ok = emqtt:disconnect(C),
    timer:sleep(50),

    % goto awake state, receive downlink messages, and go back to asleep
    send_pingreq_msg(Socket, ClientId),

    UdpData_reg = receive_response(Socket),
    {TopicIdNew, MsgId_reg} = check_register_msg_on_udp(TopicName_test5, UdpData_reg),
    send_regack_msg(Socket, TopicIdNew, MsgId_reg),

    UdpData2 = receive_response(Socket),
    MsgId2 = check_publish_msg_on_udp(
        {Dup, QoS, Retain, WillBit, CleanSession, ?SN_NORMAL_TOPIC, TopicIdNew, Payload2}, UdpData2
    ),
    send_puback_msg(Socket, TopicIdNew, MsgId2),
    timer:sleep(50),

    UdpData3 = wrap_receive_response(Socket),
    MsgId3 = check_publish_msg_on_udp(
        {Dup, QoS, Retain, WillBit, CleanSession, ?SN_NORMAL_TOPIC, TopicIdNew, Payload3}, UdpData3
    ),
    send_puback_msg(Socket, TopicIdNew, MsgId3),
    timer:sleep(50),

    case receive_response(Socket) of
        <<2, 23>> ->
            ok;
        UdpData4 ->
            MsgId4 = check_publish_msg_on_udp(
                {Dup, QoS, Retain, WillBit, CleanSession, ?SN_NORMAL_TOPIC, TopicIdNew, Payload4},
                UdpData4
            ),
            send_puback_msg(Socket, TopicIdNew, MsgId4)
    end,
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),

    timer:sleep(8000),
    ?assertEqual([], emqx_gateway_cm:lookup_by_clientid(mqttsn, ClientId)),

    gen_udp:close(Socket).

t_asleep_test06_to_awake_qos2_dl_msg(_) ->
    QoS = 2,
    Duration = 1,
    WillTopic = <<"dead">>,
    WillPayload = <<10, 11, 12, 13, 14>>,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg_with_will(Socket, Duration, ClientId),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_msg(Socket, WillTopic, QoS),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),
    send_willmsg_msg(Socket, WillPayload),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    % subscribe
    TopicName_tom = <<"tom">>,
    MsgId1 = 25,
    WillBit = 0,
    Dup = 0,
    Retain = 0,
    CleanSession = 0,
    ReturnCode = 0,
    send_register_msg(Socket, TopicName_tom, MsgId1),
    timer:sleep(50),
    TopicId_tom = check_regack_msg_on_udp(MsgId1, receive_response(Socket)),
    send_subscribe_msg_predefined_topic(Socket, QoS, TopicId_tom, MsgId1),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, WillBit:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId_tom:16, MsgId1:16, ReturnCode>>,
        receive_response(Socket)
    ),

    % goto asleep state
    SleepDuration = 5,
    send_disconnect_msg(Socket, SleepDuration),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    timer:sleep(100),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% send downlink data in asleep state. This message should be send to device once it wake up
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    Payload1 = <<55, 66, 77, 88, 99>>,
    {ok, C} = emqtt:start_link(),
    {ok, _} = emqtt:connect(C),
    {ok, _} = emqtt:publish(C, TopicName_tom, Payload1, QoS),
    timer:sleep(100),
    ok = emqtt:disconnect(C),
    timer:sleep(300),

    % goto awake state, receive downlink messages, and go back to asleep
    send_pingreq_msg(Socket, ClientId),

    UdpData = wrap_receive_response(Socket),
    MsgId_udp = check_publish_msg_on_udp(
        {Dup, QoS, Retain, WillBit, CleanSession, ?SN_NORMAL_TOPIC, TopicId_tom, Payload1}, UdpData
    ),
    send_pubrec_msg(Socket, MsgId_udp),
    ?assertMatch(<<_:8, ?SN_PUBREL:8, _/binary>>, receive_response(Socket)),
    send_pubcomp_msg(Socket, MsgId_udp),

    %% verify the pingresp is received after receiving all the buffered qos2 msgs
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),

    timer:sleep(8000),
    ?assertEqual([], emqx_gateway_cm:lookup_by_clientid(mqttsn, ClientId)),

    gen_udp:close(Socket).

t_asleep_test07_to_connected(_) ->
    QoS = 1,
    Keepalive_Duration = 10,
    SleepDuration = 1,
    WillTopic = <<"dead">>,
    WillPayload = <<10, 11, 12, 13, 14>>,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg_with_will(Socket, Keepalive_Duration, ClientId),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_msg(Socket, WillTopic, QoS),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),
    send_willmsg_msg(Socket, WillPayload),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    % subscribe
    TopicName_tom = <<"tom">>,
    MsgId1 = 25,
    WillBit = 0,
    Dup = 0,
    Retain = 0,
    CleanSession = 0,
    ReturnCode = 0,
    send_register_msg(Socket, TopicName_tom, MsgId1),
    TopicId_tom = check_regack_msg_on_udp(MsgId1, receive_response(Socket)),
    send_subscribe_msg_predefined_topic(Socket, QoS, TopicId_tom, MsgId1),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, WillBit:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId_tom:16, MsgId1:16, ReturnCode>>,
        receive_response(Socket)
    ),

    % goto asleep state
    send_disconnect_msg(Socket, SleepDuration),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    timer:sleep(100),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% send connect message, and goto connected state
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, ?SN_RC_ACCEPTED>>, receive_response(Socket)),

    timer:sleep(1500),
    % asleep timer should get timeout, without any effect
    ?assertEqual([], emqx_gateway_cm:lookup_by_clientid(mqttsn, ClientId)),
    gen_udp:close(Socket).

t_asleep_test08_to_disconnected(_) ->
    QoS = 1,
    Keepalive_Duration = 3,
    SleepDuration = 1,
    WillTopic = <<"dead">>,
    WillPayload = <<10, 11, 12, 13, 14>>,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg_with_will(Socket, Keepalive_Duration, ClientId),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_msg(Socket, WillTopic, QoS),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),
    send_willmsg_msg(Socket, WillPayload),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    % goto asleep state
    send_disconnect_msg(Socket, SleepDuration),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    timer:sleep(100),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% send disconnect message, and goto disconnected state
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    timer:sleep(8000),
    % it is a normal termination, without will message
    ?assertEqual([], emqx_gateway_cm:lookup_by_clientid(mqttsn, ClientId)),

    gen_udp:close(Socket).

t_asleep_test09_to_awake_again_qos1_dl_msg(_) ->
    QoS = 1,
    Duration = 5,
    WillTopic = <<"dead">>,
    WillPayload = <<10, 11, 12, 13, 14>>,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg_with_will(Socket, Duration, ClientId),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_msg(Socket, WillTopic, QoS),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),
    send_willmsg_msg(Socket, WillPayload),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    % subscribe
    TopicName1 = <<"u/+/k">>,
    MsgId1 = 25,
    TopicId0 = 0,
    WillBit = 0,
    Dup = 0,
    Retain = 0,
    CleanSession = 0,
    ReturnCode = 0,
    send_subscribe_msg_normal_topic(Socket, QoS, TopicName1, MsgId1),
    ?assertEqual(
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, WillBit:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId0:16, MsgId1:16, ReturnCode>>,
        receive_response(Socket)
    ),
    % goto asleep state
    SleepDuration = 5,
    send_disconnect_msg(Socket, SleepDuration),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    timer:sleep(200),

    %% send downlink data in asleep state. This message should be send to device once it wake up
    Payload2 = <<55, 66, 77, 88, 99>>,
    Payload3 = <<61, 71, 81>>,
    Payload4 = <<100, 101, 102, 103, 104, 105, 106, 107>>,
    TopicName_test9 = <<"u/v/k">>,
    {ok, C} = emqtt:start_link(),
    {ok, _} = emqtt:connect(C),
    {ok, _} = emqtt:publish(C, TopicName_test9, Payload2, QoS),
    timer:sleep(100),
    {ok, _} = emqtt:publish(C, TopicName_test9, Payload3, QoS),
    timer:sleep(100),
    {ok, _} = emqtt:publish(C, TopicName_test9, Payload4, QoS),
    timer:sleep(200),
    ok = emqtt:disconnect(C),

    % goto awake state, receive downlink messages, and go back to asleep
    send_pingreq_msg(Socket, ClientId),

    UdpData_reg = receive_response(Socket),
    {TopicIdNew, MsgId_reg} = check_register_msg_on_udp(TopicName_test9, UdpData_reg),
    send_regack_msg(Socket, TopicIdNew, MsgId_reg),

    case wrap_receive_response(Socket) of
        udp_receive_timeout ->
            ok;
        UdpData2 ->
            MsgId2 = check_publish_msg_on_udp(
                {Dup, QoS, Retain, WillBit, CleanSession, ?SN_NORMAL_TOPIC, TopicIdNew, Payload2},
                UdpData2
            ),
            send_puback_msg(Socket, TopicIdNew, MsgId2)
    end,
    timer:sleep(100),

    case wrap_receive_response(Socket) of
        udp_receive_timeout ->
            ok;
        UdpData3 ->
            MsgId3 = check_publish_msg_on_udp(
                {Dup, QoS, Retain, WillBit, CleanSession, ?SN_NORMAL_TOPIC, TopicIdNew, Payload3},
                UdpData3
            ),
            send_puback_msg(Socket, TopicIdNew, MsgId3)
    end,
    timer:sleep(100),

    case wrap_receive_response(Socket) of
        udp_receive_timeout ->
            ok;
        UdpData4 ->
            MsgId4 = check_publish_msg_on_udp(
                {Dup, QoS, Retain, WillBit, CleanSession, ?SN_NORMAL_TOPIC, TopicIdNew, Payload4},
                UdpData4
            ),
            send_puback_msg(Socket, TopicIdNew, MsgId4)
    end,
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),

    %% send PINGREQ again to enter awake state
    send_pingreq_msg(Socket, ClientId),
    %% will not receive any buffered PUBLISH messages buffered before last
    %% awake, only receive PINGRESP here
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),

    timer:sleep(10000),
    ?assertEqual([], emqx_gateway_cm:lookup_by_clientid(mqttsn, ClientId)),

    gen_udp:close(Socket).

t_awake_test01_to_connected(_) ->
    QoS = 1,
    Keepalive_Duration = 3,
    SleepDuration = 1,
    WillTopic = <<"dead">>,
    WillPayload = <<10, 11, 12, 13, 14>>,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg_with_will(Socket, Keepalive_Duration, ClientId),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_msg(Socket, WillTopic, QoS),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),
    send_willmsg_msg(Socket, WillPayload),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    % goto asleep state
    send_disconnect_msg(Socket, SleepDuration),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    timer:sleep(100),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% send connect message, and goto connected state
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, ?SN_RC_ACCEPTED>>, receive_response(Socket)),

    timer:sleep(1500),
    % asleep timer should get timeout
    ?assertEqual([], emqx_gateway_cm:lookup_by_clientid(mqttsn, ClientId)),
    gen_udp:close(Socket).

t_awake_test02_to_disconnected(_) ->
    QoS = 1,
    Keepalive_Duration = 3,
    SleepDuration = 1,
    WillTopic = <<"dead">>,
    WillPayload = <<10, 11, 12, 13, 14>>,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ClientId = ?CLIENTID,
    send_connect_msg_with_will(Socket, Keepalive_Duration, ClientId),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_msg(Socket, WillTopic, QoS),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),
    send_willmsg_msg(Socket, WillPayload),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    % goto asleep state
    send_disconnect_msg(Socket, SleepDuration),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    timer:sleep(100),

    % goto awake state
    send_pingreq_msg(Socket, ClientId),
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% send disconnect message, and goto disconnected state
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    %% asleep timeout
    timer:sleep(8000),
    ?assertEqual([], emqx_gateway_cm:lookup_by_clientid(mqttsn, ClientId)),

    gen_udp:close(Socket).

t_broadcast_test1(_) ->
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_searchgw_msg(Socket),
    ?assertEqual(<<3, ?SN_GWINFO, 1>>, receive_response(Socket)),
    timer:sleep(600),
    gen_udp:close(Socket).

t_register_subs_resume_on(_) ->
    update_mqttsn_with_subs_resume_on(),
    MsgId = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>, 0),
    ?assertMatch(
        <<_, ?SN_CONNACK, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),

    send_subscribe_msg_normal_topic(Socket, ?QOS_2, <<"topic-a">>, MsgId + 1),
    <<_, ?SN_SUBACK, 2#01000000, TopicIdA:16, _:16, ?SN_RC_ACCEPTED>> = receive_response(Socket),

    send_subscribe_msg_normal_topic(Socket, ?QOS_2, <<"topic-b">>, MsgId + 2),
    <<_, ?SN_SUBACK, 2#01000000, TopicIdB:16, _:16, ?SN_RC_ACCEPTED>> = receive_response(Socket),

    _ = emqx:publish(
        emqx_message:make(test, ?QOS_1, <<"topic-a">>, <<"test-a">>)
    ),
    _ = emqx:publish(
        emqx_message:make(test, ?QOS_1, <<"topic-b">>, <<"test-b">>)
    ),

    <<_, ?SN_PUBLISH, 2#00100000, TopicIdA:16, MsgId1:16, "test-a">> = receive_response(Socket),
    send_puback_msg(Socket, TopicIdA, MsgId1, ?SN_RC_ACCEPTED),

    <<_, ?SN_PUBLISH, 2#00100000, TopicIdB:16, MsgId2:16, "test-b">> = receive_response(Socket),
    send_puback_msg(Socket, TopicIdB, MsgId2, ?SN_RC_ACCEPTED),

    send_disconnect_msg(Socket, undefined),
    ?assertMatch(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket),

    %% offline messages will be queued into the MQTT-SN session
    _ = emqx:publish(emqx_message:make(test, ?QOS_0, <<"topic-a">>, <<"m1">>)),
    _ = emqx:publish(emqx_message:make(test, ?QOS_1, <<"topic-a">>, <<"m2">>)),
    _ = emqx:publish(emqx_message:make(test, ?QOS_2, <<"topic-a">>, <<"m3">>)),
    _ = emqx:publish(emqx_message:make(test, ?QOS_0, <<"topic-b">>, <<"m1">>)),
    _ = emqx:publish(emqx_message:make(test, ?QOS_1, <<"topic-b">>, <<"m2">>)),
    _ = emqx:publish(emqx_message:make(test, ?QOS_2, <<"topic-b">>, <<"m3">>)),

    {ok, NSocket} = gen_udp:open(0, [binary]),
    send_connect_msg(NSocket, <<"test">>, 0),
    ?assertMatch(
        <<_, ?SN_CONNACK, ?SN_RC_ACCEPTED>>,
        receive_response(NSocket)
    ),

    %% receive subs register requests
    <<_, ?SN_REGISTER, TopicIdA:16, RegMsgIdA:16, "topic-a">> = receive_response(NSocket),
    send_regack_msg(NSocket, TopicIdA, RegMsgIdA),

    <<_, ?SN_REGISTER, TopicIdB:16, RegMsgIdB:16, "topic-b">> = receive_response(NSocket),
    send_regack_msg(NSocket, TopicIdB, RegMsgIdB),

    %% receive the queued messages

    <<_, ?SN_PUBLISH, 2#00000000, TopicIdA:16, 0:16, "m1">> = receive_response(NSocket),

    <<_, ?SN_PUBLISH, 2#00100000, TopicIdA:16, MsgIdA1:16, "m2">> = receive_response(NSocket),
    send_puback_msg(NSocket, TopicIdA, MsgIdA1, ?SN_RC_ACCEPTED),

    <<_, ?SN_PUBLISH, 2#01000000, TopicIdA:16, MsgIdA2:16, "m3">> = receive_response(NSocket),
    send_pubrec_msg(NSocket, MsgIdA2),
    <<_, ?SN_PUBREL, MsgIdA2:16>> = receive_response(NSocket),
    send_pubcomp_msg(NSocket, MsgIdA2),

    <<_, ?SN_PUBLISH, 2#00000000, TopicIdB:16, 0:16, "m1">> = receive_response(NSocket),

    <<_, ?SN_PUBLISH, 2#00100000, TopicIdB:16, MsgIdB1:16, "m2">> = receive_response(NSocket),
    send_puback_msg(NSocket, TopicIdB, MsgIdB1, ?SN_RC_ACCEPTED),

    <<_, ?SN_PUBLISH, 2#01000000, TopicIdB:16, MsgIdB2:16, "m3">> = receive_response(NSocket),
    send_pubrec_msg(NSocket, MsgIdB2),
    <<_, ?SN_PUBREL, MsgIdB2:16>> = receive_response(NSocket),
    send_pubcomp_msg(NSocket, MsgIdB2),

    %% no more messages
    ?assertEqual(udp_receive_timeout, receive_response(NSocket)),

    gen_udp:close(NSocket),
    {ok, NSocket1} = gen_udp:open(0, [binary]),
    send_connect_msg(NSocket1, <<"test">>),
    ?assertMatch(
        <<_, ?SN_CONNACK, ?SN_RC_ACCEPTED>>,
        receive_response(NSocket1)
    ),
    send_disconnect_msg(NSocket1, undefined),
    ?assertMatch(<<2, ?SN_DISCONNECT>>, receive_response(NSocket1)),
    gen_udp:close(NSocket1),
    update_mqttsn_with_subs_resume_off().

t_register_subs_resume_off(_) ->
    MsgId = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>, 0),
    ?assertMatch(
        <<_, ?SN_CONNACK, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),

    send_subscribe_msg_normal_topic(Socket, ?QOS_1, <<"topic-a">>, MsgId + 1),
    <<_, ?SN_SUBACK, 2#00100000, TopicIdA:16, _:16, ?SN_RC_ACCEPTED>> = receive_response(Socket),

    send_subscribe_msg_normal_topic(Socket, ?QOS_2, <<"topic-b">>, MsgId + 2),
    <<_, ?SN_SUBACK, 2#01000000, TopicIdB:16, _:16, ?SN_RC_ACCEPTED>> = receive_response(Socket),

    _ = emqx:publish(
        emqx_message:make(test, ?QOS_1, <<"topic-a">>, <<"test-a">>)
    ),
    _ = emqx:publish(
        emqx_message:make(test, ?QOS_2, <<"topic-b">>, <<"test-b">>)
    ),

    QoS1Flags = 2#00100000,
    QoS2Flags = 2#01000000,

    %% This is a QoS 1 message
    <<_, ?SN_PUBLISH, QoS1Flags, TopicIdA:16, MsgId1:16, "test-a">> = receive_response(Socket),
    send_puback_msg(Socket, TopicIdA, MsgId1, ?SN_RC_ACCEPTED),

    %% This is a QoS 2 message
    <<_, ?SN_PUBLISH, QoS2Flags, TopicIdB:16, MsgId2:16, "test-b">> = receive_response(Socket),
    send_pubrec_msg(Socket, MsgId2),
    %% discard PUBREL message
    <<_, ?SN_PUBREL, MsgId2:16>> = receive_response(Socket),

    send_disconnect_msg(Socket, undefined),
    ?assertMatch(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket),

    %% offline messages will be queued into the MQTT-SN session
    _ = emqx:publish(emqx_message:make(test, ?QOS_1, <<"topic-a">>, <<"m1">>)),
    _ = emqx:publish(emqx_message:make(test, ?QOS_1, <<"topic-a">>, <<"m2">>)),
    _ = emqx:publish(emqx_message:make(test, ?QOS_1, <<"topic-a">>, <<"m3">>)),
    _ = emqx:publish(emqx_message:make(test, ?QOS_2, <<"topic-b">>, <<"m1">>)),
    _ = emqx:publish(emqx_message:make(test, ?QOS_2, <<"topic-b">>, <<"m2">>)),
    _ = emqx:publish(emqx_message:make(test, ?QOS_2, <<"topic-b">>, <<"m3">>)),

    {ok, NSocket} = gen_udp:open(0, [binary]),
    send_connect_msg(NSocket, <<"test">>, 0),
    ?assertMatch(
        <<_, ?SN_CONNACK, ?SN_RC_ACCEPTED>>,
        receive_response(NSocket)
    ),

    %% qos1

    %% receive PUBREL for `test-b' again
    <<_, ?SN_PUBREL, MsgId2:16>> = receive_response(NSocket),
    send_pubcomp_msg(NSocket, MsgId2),
    %% received the resume messages
    <<_, ?SN_PUBLISH, QoS1Flags, TopicIdA:16, MsgIdA0:16, "m1">> = receive_response(NSocket),
    %% only one qos1/qos2 inflight
    ?assertEqual(udp_receive_timeout, receive_response(NSocket)),
    send_puback_msg(NSocket, TopicIdA, MsgIdA0, ?SN_RC_INVALID_TOPIC_ID),
    %% recv register
    <<_, ?SN_REGISTER, TopicIdA:16, RegMsgIdA:16, "topic-a">> = receive_response(NSocket),
    send_regack_msg(NSocket, TopicIdA, RegMsgIdA),
    %% received the replay messages
    <<_, ?SN_PUBLISH, QoS1Flags, TopicIdA:16, MsgIdA1:16, "m1">> = receive_response(NSocket),
    send_puback_msg(NSocket, TopicIdA, MsgIdA1, ?SN_RC_ACCEPTED),

    <<_, ?SN_PUBLISH, QoS1Flags, TopicIdA:16, MsgIdA2:16, "m2">> = receive_response(NSocket),
    send_puback_msg(NSocket, TopicIdA, MsgIdA2, ?SN_RC_ACCEPTED),

    <<_, ?SN_PUBLISH, QoS1Flags, TopicIdA:16, MsgIdA3:16, "m3">> = receive_response(NSocket),
    send_puback_msg(NSocket, TopicIdA, MsgIdA3, ?SN_RC_ACCEPTED),

    %% qos2
    <<_, ?SN_PUBLISH, QoS2Flags, TopicIdB:16, MsgIdB0:16, "m1">> = receive_response(NSocket),
    %% only one qos1/qos2 inflight
    ?assertEqual(udp_receive_timeout, receive_response(NSocket)),
    send_puback_msg(NSocket, TopicIdB, MsgIdB0, ?SN_RC_INVALID_TOPIC_ID),
    %% recv register
    <<_, ?SN_REGISTER, TopicIdB:16, RegMsgIdB:16, "topic-b">> = receive_response(NSocket),
    send_regack_msg(NSocket, TopicIdB, RegMsgIdB),
    %% received the replay messages
    <<_, ?SN_PUBLISH, QoS2Flags, TopicIdB:16, MsgIdB1:16, "m1">> = receive_response(NSocket),
    send_pubrec_msg(NSocket, MsgIdB1),
    <<_, ?SN_PUBREL, MsgIdB1:16>> = receive_response(NSocket),
    send_pubcomp_msg(NSocket, MsgIdB1),

    <<_, ?SN_PUBLISH, QoS2Flags, TopicIdB:16, MsgIdB2:16, "m2">> = receive_response(NSocket),
    send_pubrec_msg(NSocket, MsgIdB2),
    <<_, ?SN_PUBREL, MsgIdB2:16>> = receive_response(NSocket),
    send_pubcomp_msg(NSocket, MsgIdB2),

    <<_, ?SN_PUBLISH, QoS2Flags, TopicIdB:16, MsgIdB3:16, "m3">> = receive_response(NSocket),
    send_pubrec_msg(NSocket, MsgIdB3),
    <<_, ?SN_PUBREL, MsgIdB3:16>> = receive_response(NSocket),
    send_pubcomp_msg(NSocket, MsgIdB3),

    %% no more messages
    ?assertEqual(udp_receive_timeout, receive_response(NSocket)),

    gen_udp:close(NSocket),
    {ok, NSocket1} = gen_udp:open(0, [binary]),
    send_connect_msg(NSocket1, <<"test">>),
    ?assertMatch(
        <<_, ?SN_CONNACK, ?SN_RC_ACCEPTED>>,
        receive_response(NSocket1)
    ),
    send_disconnect_msg(NSocket1, undefined),
    ?assertMatch(<<2, ?SN_DISCONNECT>>, receive_response(NSocket1)),
    gen_udp:close(NSocket1).

t_register_skip_failure_topic_name_and_reach_max_retry_times(_) ->
    update_mqttsn_with_subs_resume_on(),
    MsgId = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>, 0),
    ?assertMatch(
        <<_, ?SN_CONNACK, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),

    send_subscribe_msg_normal_topic(Socket, ?QOS_2, <<"topic-a">>, MsgId + 1),
    <<_, ?SN_SUBACK, 2#01000000, TopicIdA:16, _:16, ?SN_RC_ACCEPTED>> = receive_response(Socket),

    send_subscribe_msg_normal_topic(Socket, ?QOS_2, <<"topic-b">>, MsgId + 2),
    <<_, ?SN_SUBACK, 2#01000000, TopicIdB:16, _:16, ?SN_RC_ACCEPTED>> = receive_response(Socket),

    send_disconnect_msg(Socket, undefined),
    ?assertMatch(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket),

    {ok, NSocket} = gen_udp:open(0, [binary]),
    send_connect_msg(NSocket, <<"test">>, 0),
    ?assertMatch(
        <<_, ?SN_CONNACK, ?SN_RC_ACCEPTED>>,
        receive_response(NSocket)
    ),

    %% receive subs register requests

    %% registered failured topic-name will be skipped
    <<_, ?SN_REGISTER, TopicIdA:16, RegMsgIdA:16, "topic-a">> = receive_response(NSocket),
    send_regack_msg(NSocket, TopicIdA, RegMsgIdA, ?SN_RC_INVALID_TOPIC_ID),

    %% the gateway try to shutdown this client if it reached max-retry-times
    %%
    %% times-0
    <<_, ?SN_REGISTER, TopicIdB:16, RegMsgIdB:16, "topic-b">> = receive_response(NSocket),
    %% times-1

    %% RETYRY_TIMEOUT
    timer:sleep(5000),
    <<_, ?SN_REGISTER, TopicIdB:16, RegMsgIdB:16, "topic-b">> = receive_response(NSocket),
    %% times-2

    %% RETYRY_TIMEOUT
    timer:sleep(5000),
    <<_, ?SN_REGISTER, TopicIdB:16, RegMsgIdB:16, "topic-b">> = receive_response(NSocket),
    %% just a ping
    send_pingreq_msg(NSocket, <<"test">>),
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(NSocket)),
    %% times-3

    %% RETYRY_TIMEOUT
    timer:sleep(5000),
    <<_, ?SN_REGISTER, TopicIdB:16, RegMsgIdB:16, "topic-b">> = receive_response(NSocket),
    %% shutdown due to reached max retry times

    %% RETYRY_TIMEOUT
    timer:sleep(5000),
    ?assertMatch(<<2, ?SN_DISCONNECT>>, receive_response(NSocket)),
    gen_udp:close(NSocket),
    update_mqttsn_with_subs_resume_off().

t_register_enqueue_delivering_messages(_) ->
    update_mqttsn_with_subs_resume_on(),
    MsgId = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>, 0),
    ?assertMatch(
        <<_, ?SN_CONNACK, ?SN_RC_ACCEPTED>>,
        receive_response(Socket)
    ),

    send_subscribe_msg_normal_topic(Socket, ?QOS_2, <<"topic-a">>, MsgId + 1),
    <<_, ?SN_SUBACK, 2#01000000, TopicIdA:16, _:16, ?SN_RC_ACCEPTED>> = receive_response(Socket),

    send_disconnect_msg(Socket, undefined),
    ?assertMatch(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket),

    {ok, NSocket} = gen_udp:open(0, [binary]),
    send_connect_msg(NSocket, <<"test">>, 0),
    ?assertMatch(
        <<_, ?SN_CONNACK, ?SN_RC_ACCEPTED>>,
        receive_response(NSocket)
    ),

    %% receive subs register requests

    %% registered failured topic-name will be skipped
    <<_, ?SN_REGISTER, TopicIdA:16, RegMsgIdA:16, "topic-a">> = receive_response(NSocket),

    _ = emqx:publish(emqx_message:make(test, ?QOS_0, <<"topic-a">>, <<"m1">>)),
    _ = emqx:publish(emqx_message:make(test, ?QOS_1, <<"topic-a">>, <<"m2">>)),

    send_regack_msg(NSocket, TopicIdA, RegMsgIdA, ?SN_RC_ACCEPTED),

    %% receive the queued messages

    <<_, ?SN_PUBLISH, 2#00000000, TopicIdA:16, 0:16, "m1">> = receive_response(NSocket),

    <<_, ?SN_PUBLISH, 2#00100000, TopicIdA:16, MsgIdA1:16, "m2">> = receive_response(NSocket),
    send_puback_msg(NSocket, TopicIdA, MsgIdA1, ?SN_RC_ACCEPTED),

    %% no more messages
    ?assertEqual(udp_receive_timeout, receive_response(NSocket)),

    gen_udp:close(NSocket),
    {ok, NSocket1} = gen_udp:open(0, [binary]),
    send_connect_msg(NSocket1, <<"test">>),
    ?assertMatch(
        <<_, ?SN_CONNACK, ?SN_RC_ACCEPTED>>,
        receive_response(NSocket1)
    ),
    send_disconnect_msg(NSocket1, undefined),
    ?assertMatch(<<2, ?SN_DISCONNECT>>, receive_response(NSocket1)),
    gen_udp:close(NSocket1),
    update_mqttsn_with_subs_resume_off().

t_socket_passvice(_) ->
    %% TODO: test this gateway enter the passvie event
    ok.

t_clients_api(_) ->
    TsNow = emqx_utils_calendar:now_to_rfc3339(millisecond),
    ClientId = <<"client_id_test1">>,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    %% list
    {200, #{data := [Client1]}} = request(get, "/gateways/mqttsn/clients"),
    #{clientid := ClientId} = Client1,
    %% searching
    {200, #{data := [Client2]}} =
        request(get, "/gateways/mqttsn/clients", [{<<"clientid">>, ClientId}]),
    {200, #{data := [Client3]}} =
        request(
            get,
            "/gateways/mqttsn/clients",
            [
                {<<"like_clientid">>, <<"test1">>},
                {<<"proto_ver">>, <<"1.2">>},
                {<<"ip_address">>, <<"127.0.0.1">>},
                {<<"conn_state">>, <<"connected">>},
                {<<"clean_start">>, <<"true">>},
                {<<"gte_connected_at">>, TsNow}
            ]
        ),
    %% lookup
    {200, Client4} =
        request(get, "/gateways/mqttsn/clients/client_id_test1"),
    %% assert
    Client1 = Client2 = Client3 = Client4,
    %% assert keepalive
    ?assertEqual(10, maps:get(keepalive, Client4)),
    %% kickout
    {204, _} =
        request(delete, "/gateways/mqttsn/clients/client_id_test1"),
    timer:sleep(100),
    {200, #{data := []}} = request(get, "/gateways/mqttsn/clients"),

    send_disconnect_msg(Socket, undefined),
    gen_udp:close(Socket).

t_clients_subscription_api(_) ->
    ClientId = <<"client_id_test1">>,
    Path = "/gateways/mqttsn/clients/client_id_test1/subscriptions",
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    %% list
    {200, []} = request(get, Path),
    %% create
    SubReq = #{
        topic => <<"tx">>,
        qos => 1,
        nl => 0,
        rap => 0,
        rh => 0
    },
    {201, SubsResp} = request(post, Path, SubReq),

    {200, [SubsResp]} = request(get, Path),

    {204, _} = request(delete, Path ++ "/tx"),

    {200, []} = request(get, Path),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

t_clients_api_complex_id(_) ->
    ClientId = <<"!@#$%^&*()_+{}:\"<>?/">>,
    ClientIdUriEncoded = cow_qs:urlencode(ClientId),
    Path = "/gateways/mqttsn/clients/" ++ binary_to_list(ClientIdUriEncoded),
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    %% get
    {200, Client} = request(get, Path),
    ?assertMatch(#{clientid := ClientId}, Client),
    %% subscription list
    {200, []} = request(get, Path ++ "/subscriptions"),
    %% kickout
    {204, _} = request(delete, Path),
    gen_udp:close(Socket).

t_update_info_after_subscribed_via_api(_) ->
    ClientId = <<"client_id_test1">>,
    Path = "/gateways/mqttsn/clients/client_id_test1/subscriptions",
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    %% create
    SubReq = #{
        topic => <<"tx">>,
        qos => 1,
        nl => 0,
        rap => 0,
        rh => 0
    },
    {201, _SubsResp} = request(post, Path, SubReq),
    timer:sleep(500),
    %% assert
    {200, Client} = request(get, "/gateways/mqttsn/clients/client_id_test1"),
    ?assertMatch(#{subscriptions_cnt := 1}, Client),

    send_disconnect_msg(Socket, undefined),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

%%--------------------------------------------------------------------
%% Helper funcs
%%--------------------------------------------------------------------

send_searchgw_msg(Socket) ->
    Length = 3,
    MsgType = ?SN_SEARCHGW,
    Radius = 0,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, <<Length:8, MsgType:8, Radius:8>>).

make_connect_msg(ClientId, CleanSession) when
    CleanSession == 0;
    CleanSession == 1
->
    Length = 6 + byte_size(ClientId),
    MsgType = ?SN_CONNECT,
    Dup = 0,
    QoS = 0,
    Retain = 0,
    Will = 0,
    TopicIdType = 0,
    ProtocolId = 1,
    Duration = 10,
    <<Length:8, MsgType:8, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2,
        ProtocolId:8, Duration:16, ClientId/binary>>.

send_connect_msg(Socket, ClientId) ->
    send_connect_msg(Socket, ClientId, 1).

send_connect_msg(Socket, ClientId, CleanSession) ->
    Packet = make_connect_msg(ClientId, CleanSession),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, Packet).

send_connect_msg_with_will(Socket, Duration, ClientId) ->
    Length = 6 + byte_size(ClientId),
    Will = 1,
    CleanSession = 1,
    ProtocolId = 1,
    ConnectPacket =
        <<Length:8, ?SN_CONNECT:8, ?FNU:4, Will:1, CleanSession:1, ?FNU:2, ProtocolId:8,
            Duration:16, ClientId/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, ConnectPacket).

send_connect_msg_with_will1(Socket, Duration, ClientId) ->
    Length = 6 + byte_size(ClientId),
    Will = 1,
    CleanSession = 0,
    ProtocolId = 1,
    ConnectPacket =
        <<Length:8, ?SN_CONNECT:8, ?FNU:4, Will:1, CleanSession:1, ?FNU:2, ProtocolId:8,
            Duration:16, ClientId/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, ConnectPacket).

send_willtopic_msg(Socket, Topic, QoS) ->
    Length = 3 + byte_size(Topic),
    MsgType = ?SN_WILLTOPIC,
    Retain = 0,
    WillTopicPacket = <<Length:8, MsgType:8, ?FNU:1, QoS:2, Retain:1, ?FNU:4, Topic/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, WillTopicPacket).

send_willtopic_empty_msg(Socket) ->
    Length = 2,
    MsgType = ?SN_WILLTOPIC,
    WillTopicPacket = <<Length:8, MsgType:8>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, WillTopicPacket).

send_willmsg_msg(Socket, Msg) ->
    Length = 2 + byte_size(Msg),
    WillMsgPacket = <<Length:8, ?SN_WILLMSG:8, Msg/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, WillMsgPacket).

send_willtopicupd_msg(Socket, Topic, QoS) ->
    Length = 3 + byte_size(Topic),
    MsgType = ?SN_WILLTOPICUPD,
    Retain = 0,
    WillTopicPacket = <<Length:8, MsgType:8, ?FNU:1, QoS:2, Retain:1, ?FNU:4, Topic/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, WillTopicPacket).

send_willtopicupd_empty_msg(Socket) ->
    Length = 2,
    MsgType = ?SN_WILLTOPICUPD,
    WillTopicPacket = <<Length:8, MsgType:8>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, WillTopicPacket).

send_willmsgupd_msg(Socket, Msg) ->
    Length = 2 + byte_size(Msg),
    MsgType = ?SN_WILLMSGUPD,
    WillTopicPacket = <<Length:8, MsgType:8, Msg/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, WillTopicPacket).

send_register_msg(Socket, TopicName, MsgId) ->
    Length = 6 + byte_size(TopicName),
    MsgType = ?SN_REGISTER,
    TopicId = 0,
    RegisterPacket = <<Length:8, MsgType:8, TopicId:16, MsgId:16, TopicName/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, RegisterPacket).

send_regack_msg(Socket, TopicId, MsgId) ->
    send_regack_msg(Socket, TopicId, MsgId, ?SN_RC_ACCEPTED).

send_regack_msg(Socket, TopicId, MsgId, Rc) ->
    Length = 7,
    MsgType = ?SN_REGACK,
    Packet = <<Length:8, MsgType:8, TopicId:16, MsgId:16, Rc>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, Packet).

send_publish_msg_normal_topic(Socket, QoS, MsgId, TopicId, Data) ->
    Length = 7 + byte_size(Data),
    MsgType = ?SN_PUBLISH,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = ?SN_NORMAL_TOPIC,
    PublishPacket =
        <<Length:8, MsgType:8, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2,
            TopicId:16, MsgId:16, Data/binary>>,
    ?LOG("send_publish_msg_normal_topic TopicId=~p, Data=~p", [TopicId, Data]),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, PublishPacket).

send_publish_msg_predefined_topic(Socket, QoS, MsgId, TopicId, Data) ->
    Length = 7 + byte_size(Data),
    MsgType = ?SN_PUBLISH,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = ?SN_PREDEFINED_TOPIC,
    PublishPacket =
        <<Length:8, MsgType:8, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2,
            TopicId:16, MsgId:16, Data/binary>>,
    ?LOG("send_publish_msg_predefined_topic TopicId=~p, Data=~p", [TopicId, Data]),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, PublishPacket).

send_publish_msg_short_topic(Socket, QoS, MsgId, TopicName, Data) ->
    Length = 7 + byte_size(Data),
    MsgType = ?SN_PUBLISH,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = 2,
    PublishPacket =
        <<Length:8, MsgType:8, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2,
            TopicName/binary, MsgId:16, Data/binary>>,
    ?LOG("send_publish_msg_short_topic TopicName=~p, Data=~p", [TopicName, Data]),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, PublishPacket).

send_puback_msg(Socket, TopicId, MsgId) ->
    send_puback_msg(Socket, TopicId, MsgId, ?SN_RC_ACCEPTED).

send_puback_msg(Socket, TopicId, MsgId, Rc) ->
    Length = 7,
    MsgType = ?SN_PUBACK,
    PubAckPacket = <<Length:8, MsgType:8, TopicId:16, MsgId:16, Rc:8>>,
    ?LOG("send_puback_msg TopicId=~p, MsgId=~p", [TopicId, MsgId]),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, PubAckPacket).

send_pubrec_msg(Socket, MsgId) ->
    Length = 4,
    MsgType = ?SN_PUBREC,
    PubRecPacket = <<Length:8, MsgType:8, MsgId:16>>,
    ?LOG("send_pubrec_msg MsgId=~p", [MsgId]),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, PubRecPacket).

send_pubrel_msg(Socket, MsgId) ->
    Length = 4,
    MsgType = ?SN_PUBREL,
    PubRelPacket = <<Length:8, MsgType:8, MsgId:16>>,
    ?LOG("send_pubrel_msg MsgId=~p", [MsgId]),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, PubRelPacket).

send_pubcomp_msg(Socket, MsgId) ->
    Length = 4,
    MsgType = ?SN_PUBCOMP,
    PubCompPacket = <<Length:8, MsgType:8, MsgId:16>>,
    ?LOG("send_pubcomp_msg MsgId=~p", [MsgId]),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, PubCompPacket).

send_subscribe_msg_normal_topic(Socket, QoS, Topic, MsgId) ->
    MsgType = ?SN_SUBSCRIBE,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = ?SN_NORMAL_TOPIC,
    Length = byte_size(Topic) + 5,
    SubscribePacket =
        <<Length:8, MsgType:8, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2,
            MsgId:16, Topic/binary>>,
    ?LOG("send_subscribe_msg_normal_topic Topic=~p, MsgId=~p", [Topic, MsgId]),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, SubscribePacket).

send_subscribe_msg_predefined_topic(Socket, QoS, TopicId, MsgId) ->
    Length = 7,
    MsgType = ?SN_SUBSCRIBE,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = ?SN_PREDEFINED_TOPIC,
    SubscribePacket =
        <<Length:8, MsgType:8, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2,
            MsgId:16, TopicId:16>>,
    ?LOG("send_subscribe_msg_predefined_topic TopicId=~p, MsgId=~p", [TopicId, MsgId]),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, SubscribePacket).

send_subscribe_msg_short_topic(Socket, QoS, Topic, MsgId) ->
    Length = 7,
    MsgType = ?SN_SUBSCRIBE,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = ?SN_SHORT_TOPIC,
    SubscribePacket =
        <<Length:8, MsgType:8, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2,
            MsgId:16, Topic/binary>>,
    ?LOG("send_subscribe_msg_short_topic Topic=~p, MsgId=~p", [Topic, MsgId]),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, SubscribePacket).

send_subscribe_msg_reserved_topic(Socket, QoS, TopicId, MsgId) ->
    Length = 7,
    MsgType = ?SN_SUBSCRIBE,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = ?SN_RESERVED_TOPIC,
    SubscribePacket =
        <<Length:8, MsgType:8, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2,
            MsgId:16, TopicId:16>>,
    ?LOG("send_subscribe_msg_reserved_topic TopicId=~p, MsgId=~p", [TopicId, MsgId]),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, SubscribePacket).

send_unsubscribe_msg_predefined_topic(Socket, TopicId, MsgId) ->
    Length = 7,
    MsgType = ?SN_UNSUBSCRIBE,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = ?SN_PREDEFINED_TOPIC,
    UnSubscribePacket =
        <<Length:8, MsgType:8, Dup:1, 0:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2,
            MsgId:16, TopicId:16>>,
    ?LOG("send_unsubscribe_msg_predefined_topic TopicId=~p, MsgId=~p", [TopicId, MsgId]),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, UnSubscribePacket).

send_unsubscribe_msg_normal_topic(Socket, TopicName, MsgId) ->
    MsgType = ?SN_UNSUBSCRIBE,
    Dup = 0,
    QoS = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = ?SN_NORMAL_TOPIC,
    Length = 5 + byte_size(TopicName),
    UnSubscribePacket =
        <<Length:8, MsgType:8, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2,
            MsgId:16, TopicName/binary>>,
    ?LOG("send_unsubscribe_msg_normal_topic TopicName=~p, MsgId=~p", [TopicName, MsgId]),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, UnSubscribePacket).

send_unsubscribe_msg_short_topic(Socket, TopicId, MsgId) ->
    Length = 7,
    MsgType = ?SN_UNSUBSCRIBE,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = ?SN_SHORT_TOPIC,
    UnSubscribePacket =
        <<Length:8, MsgType:8, Dup:1, ?QOS_0:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2,
            MsgId:16, TopicId/binary>>,
    ?LOG("send_unsubscribe_msg_short_topic TopicId=~p, MsgId=~p", [TopicId, MsgId]),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, UnSubscribePacket).

send_pingreq_msg(Socket, ClientId) ->
    Length = 2,
    MsgType = ?SN_PINGREQ,
    PingReqPacket =
        case ClientId of
            undefined ->
                <<Length:8, MsgType:8>>;
            Other ->
                Size = byte_size(Other) + 2,
                <<Size:8, MsgType:8, Other/binary>>
        end,
    ?LOG("send_pingreq_msg ClientId=~p", [ClientId]),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, PingReqPacket).

make_disconnect_msg(Duration) ->
    Length = 2,
    Length2 = 4,
    MsgType = ?SN_DISCONNECT,
    case Duration of
        undefined -> <<Length:8, MsgType:8>>;
        Other -> <<Length2:8, MsgType:8, Other:16>>
    end.

send_disconnect_msg(Socket, Duration) ->
    DisConnectPacket = make_disconnect_msg(Duration),
    ?LOG("send_disconnect_msg Duration=~p", [Duration]),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, DisConnectPacket).

mid(Id) -> Id.
tid(Id) -> Id.

%% filter <<2, 23>> pingresp packet
wrap_receive_response(Socket) ->
    case receive_response(Socket) of
        <<2, 23>> ->
            ct:log("PingResp"),
            wrap_receive_response(Socket);
        Other ->
            ct:log("Other: ~p", [Other]),
            Other
    end.
receive_response(Socket) ->
    receive_response(Socket, 2000).
receive_response(Socket, Timeout) ->
    receive
        {udp, Socket, _, _, Bin} ->
            ?LOG("receive_response Bin=~p~n", [Bin]),
            Bin;
        {mqttc, From, Data2} ->
            ?LOG("receive_response() ignore mqttc From=~p, Data2=~p~n", [From, Data2]),
            receive_response(Socket)
    after Timeout ->
        udp_receive_timeout
    end.

check_dispatched_message(Dup, QoS, Retain, TopicIdType, TopicId, Payload, Socket) ->
    PubMsg = receive_response(Socket),
    Length = 7 + byte_size(Payload),
    ?LOG("check_dispatched_message ~p~n", [PubMsg]),
    ?LOG(
        "expected ~p xx ~p~n",
        [
            <<Length, ?SN_PUBLISH, Dup:1, QoS:2, Retain:1, ?FNU:2, TopicIdType:2, TopicId:16>>,
            Payload
        ]
    ),
    <<Length, ?SN_PUBLISH, Dup:1, QoS:2, Retain:1, ?FNU:2, TopicIdType:2, TopicId:16, MsgId:16,
        Payload/binary>> = PubMsg,
    case QoS of
        0 ->
            ok;
        1 ->
            send_puback_msg(Socket, TopicId, MsgId);
        2 ->
            send_pubrel_msg(Socket, MsgId),
            ?assertEqual(<<4, ?SN_PUBCOMP, MsgId:16>>, receive_response(Socket))
    end,
    ok.

get_udp_broadcast_address() ->
    "255.255.255.255".

check_publish_msg_on_udp(
    {Dup, QoS, Retain, WillBit, CleanSession, TopicType, TopicId, Payload}, UdpData
) ->
    <<HeaderUdp:5/binary, MsgId:16, PayloadIn/binary>> = UdpData,
    ct:pal("UdpData: ~p, Payload: ~p, PayloadIn: ~p", [UdpData, Payload, PayloadIn]),
    Size9 = byte_size(Payload) + 7,
    Eexp =
        <<Size9:8, ?SN_PUBLISH, Dup:1, QoS:2, Retain:1, WillBit:1, CleanSession:1, TopicType:2,
            TopicId:16>>,
    % mqtt-sn header should be same
    ?assertEqual(Eexp, HeaderUdp),
    % payload should be same
    ?assertEqual(Payload, PayloadIn),
    MsgId.

check_register_msg_on_udp(TopicName, UdpData) ->
    ct:log("UdpData: ~p~n", [UdpData]),
    <<HeaderUdp:2/binary, TopicId:16, MsgId:16, PayloadIn/binary>> = UdpData,
    Size = byte_size(TopicName) + 6,
    ?assertEqual(<<Size:8, ?SN_REGISTER>>, HeaderUdp),
    ?assertEqual(TopicName, PayloadIn),
    {TopicId, MsgId}.

check_regack_msg_on_udp(MsgId, UdpData) ->
    <<7, ?SN_REGACK, TopicId:16, MsgId:16, 0:8>> = UdpData,
    TopicId.

flush() ->
    flush([]).
flush(Msgs) ->
    receive
        M -> flush([M | Msgs])
    after 0 -> lists:reverse(Msgs)
    end.

ensure_connected_client(ClientId) ->
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, ClientId),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    Socket.
