%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_iot_e2e_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include("emqx_iot.hrl").

-define(PORT, 1883).
-define(PRODUCT, <<"P1">>).
-define(USER, <<"P1-e2e">>).
-define(PAYLOAD, <<"e2e_test_payload">>).

all() -> emqx_common_test_helpers:all(?MODULE).

%%====================================================================
%% Setup
%%====================================================================

-define(EMQX_CONF, #{
    <<"listeners">> => #{
        <<"tcp">> => #{
            <<"default">> => #{<<"acceptors">> => 4}
        }
    },
    <<"mqtt">> => #{
        <<"allow_anonymous">> => true,
        <<"client_attrs_init">> => [
            #{<<"expression">> => <<"nth(1,tokens(username,'-'))">>, <<"set_as_attr">> => <<"tns">>}
        ]
    },
    <<"multi_tenancy">> => #{<<"allow_only_managed_namespaces">> => false},
    <<"authorization">> => #{<<"no_match">> => <<"allow">>}
}).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, ?EMQX_CONF}, mria],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], [
        #{expression => <<"nth(1,tokens(username,'-'))">>, set_as_attr => <<"tns">>}
    ]),
    ok = emqx_iot:init_tables(),
    init_test_config(),
    ok = emqx_iot:hook(),
    _ =
        try
            ets:new(iot_mq_counters, [named_table, public, set, {write_concurrency, true}])
        catch
            _:_ -> ok
        end,
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_iot:unhook(),
    emqx_cth_suite:stop(?config(apps, Config)).

init_test_config() ->
    persistent_term:put({emqx_iot, config}, #{
        msg_ttl => 15 * 86400,
        cleanup_interval => 60,
        max_device_count => 10000,
        max_message_size_batch => 10240,
        max_message_size_broadcast => 65536,
        broadcast_topic => <<"/sys/broadcast/${productKey}">>,
        batch_topic => <<"/${productKey}/${deviceName}/user/get">>
    }).

init_per_testcase(_Case, Config) -> Config.
end_per_testcase(_Case, _Config) -> ok.

%%====================================================================
%% Helpers
%%====================================================================

connect_client(ClientId) ->
    Topic = <<"/P1/", ClientId/binary, "/user/get">>,
    {_, Port} = emqx_config:get([listeners, tcp, default, bind]),
    Self = self(),
    {ok, C} = emqtt:start_link([
        {host, "127.0.0.1"},
        {port, Port},
        {clientid, ClientId},
        {username, ?USER},
        {proto_ver, v5},
        {clean_start, true},
        {msg_handler, #{
            publish => fun(Msg) -> Self ! {pub, ClientId, Msg} end
        }}
    ]),
    {ok, _} = emqtt:connect(C),
    {ok, _, [1]} = emqtt:subscribe(C, Topic, ?QOS_1),
    C.

disconnect_client(C) ->
    emqtt:disconnect(C),
    emqtt:stop(C).

api_call(Body) ->
    emqx_iot_api:handle(post, [<<"pub">>], #{body => Body}).

b64(S) -> base64:encode(S).

receive_pubs(Count, Timeout) ->
    receive_pubs(Count, [], Timeout).

receive_pubs(0, Acc, _Timeout) ->
    {ok, length(Acc), Acc};
receive_pubs(Count, Acc, Timeout) ->
    receive
        {pub, ClientId, #{payload := Payload, topic := Topic}} ->
            receive_pubs(Count - 1, [{ClientId, Topic, Payload} | Acc], Timeout)
    after Timeout ->
        {timeout, length(Acc), Acc}
    end.

%%====================================================================
%% Tests
%%====================================================================

t_batch_pub_qos0_e2e(_Config) ->
    C1 = connect_client(<<"e2e_q0_1">>),
    C2 = connect_client(<<"e2e_q0_2">>),
    timer:sleep(500),

    B64 = b64(?PAYLOAD),
    {ok, 200, _, Resp} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => ?PRODUCT,
        <<"DeviceName">> => [<<"e2e_q0_1">>, <<"e2e_q0_2">>],
        <<"MessageContent">> => B64,
        <<"Qos">> => 0
    }),
    ?assert(maps:get(<<"Success">>, Resp)),

    {ok, N, Pubs} = receive_pubs(2, 5000),
    ?assertEqual(2, N, #{got => Pubs}),
    lists:foreach(
        fun({_, _, P}) -> ?assertEqual(?PAYLOAD, P) end,
        Pubs
    ),

    disconnect_client(C1),
    disconnect_client(C2).

t_batch_pub_qos1_e2e(_Config) ->
    C1 = connect_client(<<"e2e_q1_1">>),
    C2 = connect_client(<<"e2e_q1_2">>),
    timer:sleep(500),

    B64 = b64(?PAYLOAD),
    {ok, 200, _, Resp} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => ?PRODUCT,
        <<"DeviceName">> => [<<"e2e_q1_1">>, <<"e2e_q1_2">>],
        <<"MessageContent">> => B64,
        <<"Qos">> => 1
    }),
    ?assert(maps:get(<<"Success">>, Resp)),

    {ok, N, Pubs} = receive_pubs(2, 5000),
    ?assertEqual(2, N, #{got => Pubs}),
    lists:foreach(
        fun({_, _, P}) -> ?assertEqual(?PAYLOAD, P) end,
        Pubs
    ),

    disconnect_client(C1),
    disconnect_client(C2).

t_batch_pub_messageid_reuse_e2e(_Config) ->
    B64 = b64(<<"reuse_payload">>),
    {ok, 200, _, RegResp} = api_call(#{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => B64
    }),
    MsgId = maps:get(<<"MessageId">>, RegResp),

    C1 = connect_client(<<"e2e_reuse_1">>),
    timer:sleep(500),

    {ok, 200, _, Resp} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => ?PRODUCT,
        <<"DeviceName">> => [<<"e2e_reuse_1">>],
        <<"MessageId">> => MsgId,
        <<"Qos">> => 1
    }),
    ?assert(maps:get(<<"Success">>, Resp)),
    ?assertEqual(MsgId, maps:get(<<"MessageId">>, Resp)),

    {ok, 1, [{<<"e2e_reuse_1">>, _, <<"reuse_payload">>}]} = receive_pubs(1, 5000),

    disconnect_client(C1).

t_pub_broadcast_e2e(_Config) ->
    C1 = connect_client(<<"e2e_bc_1">>),
    C2 = connect_client(<<"e2e_bc_2">>),
    C3 = connect_client(<<"e2e_bc_3">>),
    timer:sleep(500),

    B64 = b64(?PAYLOAD),
    {ok, 200, _, Resp} = api_call(#{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => ?PRODUCT,
        <<"MessageContent">> => B64
    }),
    ?assert(maps:get(<<"Success">>, Resp)),

    {ok, N, Pubs} = receive_pubs(3, 5000),
    ?assertEqual(3, N, #{got => Pubs}),

    disconnect_client(C1),
    disconnect_client(C2),
    disconnect_client(C3).

t_batch_pub_partial_online_e2e(_Config) ->
    %% Only C1 online, C2 offline
    C1 = connect_client(<<"e2e_part_1">>),
    timer:sleep(500),

    B64 = b64(?PAYLOAD),
    {ok, 200, _, Resp} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => ?PRODUCT,
        <<"DeviceName">> => [<<"e2e_part_1">>, <<"e2e_part_2">>],
        <<"MessageContent">> => B64,
        <<"Qos">> => 1
    }),
    ?assert(maps:get(<<"Success">>, Resp)),

    %% C1 receives immediately
    {ok, 1, [{<<"e2e_part_1">>, _, ?PAYLOAD}]} = receive_pubs(1, 5000),

    %% C2 connects later, should receive via replay
    C2 = connect_client(<<"e2e_part_2">>),
    timer:sleep(2000),

    {ok, 1, [{<<"e2e_part_2">>, _, ?PAYLOAD}]} = receive_pubs(1, 5000),

    disconnect_client(C1),
    disconnect_client(C2).

t_batch_pub_topic_template_e2e(_Config) ->
    CustomTopic = <<"/custom/${deviceName}/topic">>,
    ExpectedTopic = <<"/custom/e2e_tpl_1/topic">>,

    C1 = connect_client(<<"e2e_tpl_1">>),
    emqtt:subscribe(C1, ExpectedTopic, ?QOS_1),
    timer:sleep(500),

    B64 = b64(?PAYLOAD),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => ?PRODUCT,
        <<"DeviceName">> => [<<"e2e_tpl_1">>],
        <<"MessageContent">> => B64,
        <<"Qos">> => 0,
        <<"TopicTemplateName">> => CustomTopic
    }),

    {ok, 1, [{_, ReceivedTopic, ?PAYLOAD}]} = receive_pubs(1, 5000),
    ?assertEqual(ExpectedTopic, ReceivedTopic),

    disconnect_client(C1).

t_register_message_e2e(_Config) ->
    B64 = b64(<<"reg_message">>),
    {ok, 200, _, R1} = api_call(#{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => B64
    }),
    Mid1 = maps:get(<<"MessageId">>, R1),

    %% Same content → same MessageId
    {ok, 200, _, R2} = api_call(#{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => B64
    }),
    ?assertEqual(Mid1, maps:get(<<"MessageId">>, R2)).
