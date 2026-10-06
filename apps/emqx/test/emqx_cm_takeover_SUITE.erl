%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cm_takeover_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, #{override_env => [{boot_modules, [broker]}]}}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

end_per_testcase(_, _) ->
    emqx_common_test_helpers:call_janitor().

-doc "Decode a 6.3.0 priority queue, preserving priority and mixed-QoS lane order.".
t_decode_630_priorities(_Config) ->
    #{session := Session, exported := Expected} = fixture_630_priorities(),
    ?assertEqual(Expected, emqx_cm_takeover:from_legacy_session(Session, 2)).

t_decode_631(_Config) ->
    #{session := Session, exported := Expected} = fixture(),
    ?assertEqual(Expected, emqx_cm_takeover:from_legacy_session(Session, 2)).

t_encode_631(_Config) ->
    #{session := Expected, exported := Exported, conf := Conf} = fixture(),
    Session = emqx_cm_takeover:to_legacy_session(
        <<"fixture">>, #{clientinfo => clientinfo()}, Exported, Conf, 2
    ),
    %% Keep the tagged session layout, but flatten its queue to delivery order.
    %% The three payloads total 16 bytes: "first", "second", "third".
    Queue =
        {mqueue, true, 1000, 0, 16, {queue, [], maps:get(mqueue, Exported), 3}, disabled,
            undefined},
    ?assertEqual(setelement(8, Expected, Queue), Session),
    ?assertEqual(Exported, emqx_cm_takeover:from_legacy_session(Session, 2)),
    %% The pre-6.3 codec remains available for MQTT BPAPI v3 and old gateways.
    Old = emqx_cm_takeover:to_legacy_session(
        <<"fixture">>, #{clientinfo => clientinfo()}, Exported, Conf, 1
    ),
    ?assertMatch(
        {session, <<"fixture">>, _, _, _, _, _, {inflight, 1, _},
            {mqueue, _, _, _, _, _, _, _, _, _, _}, _, _, _, _, _, _},
        Old
    ),
    ?assertEqual(Exported, emqx_cm_takeover:from_legacy_session(Old, 1)).

%% Generated with emqx_session_mem, emqx_mqueue and emqx_pqueue from tag 6.3.0.
%% Commit: 021c5ef13bf8c767058626ad9b760052c273736f.
%% emqx_mqueue:in/2 enqueued low-1, high-0a, default-2, high-1, low-0, high-0b.
%% Priorities: high = 10, low = 1, default = lowest; store_qos0 = true.
%% The high and low priorities contain mixed QoS0/non-QoS0 class queues.
%% Expected state comes from that release's emqx_session_mem:export/1.
fixture_630_priorities() ->
    #{
        session =>
            {session, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 118>>, true,
                #{<<"#">> => #{qos => 2}}, 23, true, {inflight, 1, {0, nil}},
                {mqueue, true, 1000, 0, 39,
                    {pqueue, [
                        {
                            -10,
                            {qos0, qos0,
                                [
                                    '$switch',
                                    {message, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4>>, 1,
                                        fixture, #{}, #{}, <<"high">>, <<"high-1">>, 1000, #{
                                            mqueue_insert_ts => 1791306814643797046
                                        }}
                                ],
                                [],
                                [
                                    {message, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6>>, 0,
                                        fixture, #{}, #{}, <<"high">>, <<"high-0b">>, 1000, #{
                                            mqueue_insert_ts => 1791306814643808487
                                        }},
                                    '$switch'
                                ],
                                [
                                    {message, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2>>, 0,
                                        fixture, #{}, #{}, <<"high">>, <<"high-0a">>, 1000, #{
                                            mqueue_insert_ts => 1791306814643795373
                                        }}
                                ],
                                3}
                        },
                        {
                            -1,
                            {default, qos0,
                                [
                                    '$switch',
                                    {message, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1>>, 1,
                                        fixture, #{}, #{}, <<"low">>, <<"low-1">>, 1000, #{
                                            mqueue_insert_ts => 1791306814643791045
                                        }}
                                ],
                                [],
                                [
                                    {message, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5>>, 0,
                                        fixture, #{}, #{}, <<"low">>, <<"low-0">>, 1000, #{
                                            mqueue_insert_ts => 1791306814643807645
                                        }}
                                ],
                                [], 2}
                        },
                        {0,
                            {queue,
                                [
                                    {message, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3>>, 2,
                                        fixture, #{}, #{}, <<"other">>, <<"default-2">>, 1000, #{
                                            mqueue_insert_ts => 1791306814643796435
                                        }}
                                ],
                                [], 1}}
                    ]},
                    {prios, #{<<"high">> => 10, <<"low">> => 1}, 0, 10, 0}, undefined},
                false, 43, 250, #{42 => 101}, 12, 5000, 1000},
        exported =>
            #{
                id => <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 118>>,
                mqueue =>
                    [
                        {message, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2>>, 0, fixture,
                            #{}, #{}, <<"high">>, <<"high-0a">>, 1000, #{}},
                        {message, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4>>, 1, fixture,
                            #{}, #{}, <<"high">>, <<"high-1">>, 1000, #{}},
                        {message, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6>>, 0, fixture,
                            #{}, #{}, <<"high">>, <<"high-0b">>, 1000, #{}},
                        {message, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1>>, 1, fixture,
                            #{}, #{}, <<"low">>, <<"low-1">>, 1000, #{}},
                        {message, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5>>, 0, fixture,
                            #{}, #{}, <<"low">>, <<"low-0">>, 1000, #{}},
                        {message, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3>>, 2, fixture,
                            #{}, #{}, <<"other">>, <<"default-2">>, 1000, #{}}
                    ],
                is_persistent => true,
                subscriptions => #{<<"#">> => #{qos => 2}},
                inflight => [],
                next_pkt_id => 43,
                awaiting_rel => #{42 => 101},
                created_at => 1000
            }
    }.

%% Generated using session_mem, mqueue and pqueue from tag 6.3.1.
%% Commit: b0604926195f9372b0068b61f88d409d639a2401.
%% * Quota disabled, one wait_comp entry and one awaiting_rel entry.
%% * `enqueue/3` added QoS0, QoS1, QoS0 messages, exercising both queue lanes.
%% * Expected exported map was produced by that release `export/1`.
fixture() ->
    #{
        session =>
            {session, <<0, 6, 93, 45, 211, 204, 142, 91, 212, 68, 0, 0, 179, 91, 0, 0>>, true,
                #{<<"out">> => #{qos => 2}}, 23, true,
                {inflight, 1,
                    {1,
                        {41,
                            {inflight_data, wait_comp,
                                {message,
                                    <<0, 6, 93, 45, 211, 204, 142, 120, 212, 68, 0, 0, 179, 91, 0,
                                        1>>,
                                    2, fixture, #{}, #{}, <<"out">>, <<"inflight">>, 1791301268573,
                                    #{}},
                                100},
                            nil, nil}}},
                {mqueue, true, 1000, 0, 16,
                    {qos0, qos0,
                        [
                            '$switch',
                            {message,
                                <<0, 6, 93, 45, 211, 204, 142, 121, 212, 68, 0, 0, 179, 91, 0, 3>>,
                                1, fixture, #{}, #{}, <<"out">>, <<"second">>, 1791301268573, #{
                                    mqueue_insert_ts => 1791301268573824212
                                }}
                        ],
                        [],
                        [
                            {message,
                                <<0, 6, 93, 45, 211, 204, 142, 121, 212, 68, 0, 0, 179, 91, 0, 4>>,
                                0, fixture, #{}, #{}, <<"out">>, <<"third">>, 1791301268573, #{
                                    mqueue_insert_ts => 1791301268573824733
                                }},
                            '$switch'
                        ],
                        [
                            {message,
                                <<0, 6, 93, 45, 211, 204, 142, 121, 212, 68, 0, 0, 179, 91, 0, 2>>,
                                0, fixture, #{}, #{}, <<"out">>, <<"first">>, 1791301268573, #{
                                    mqueue_insert_ts => 1791301268573820816
                                }}
                        ],
                        3},
                    disabled, undefined},
                false, 43, 250, #{42 => 101}, 12, 5000, 1791301268573},
        exported =>
            #{
                id => <<0, 6, 93, 45, 211, 204, 142, 91, 212, 68, 0, 0, 179, 91, 0, 0>>,
                mqueue =>
                    [
                        {message,
                            <<0, 6, 93, 45, 211, 204, 142, 121, 212, 68, 0, 0, 179, 91, 0, 2>>, 0,
                            fixture, #{}, #{}, <<"out">>, <<"first">>, 1791301268573, #{}},
                        {message,
                            <<0, 6, 93, 45, 211, 204, 142, 121, 212, 68, 0, 0, 179, 91, 0, 3>>, 1,
                            fixture, #{}, #{}, <<"out">>, <<"second">>, 1791301268573, #{}},
                        {message,
                            <<0, 6, 93, 45, 211, 204, 142, 121, 212, 68, 0, 0, 179, 91, 0, 4>>, 0,
                            fixture, #{}, #{}, <<"out">>, <<"third">>, 1791301268573, #{}}
                    ],
                inflight =>
                    [
                        #{
                            message =>
                                {message,
                                    <<0, 6, 93, 45, 211, 204, 142, 120, 212, 68, 0, 0, 179, 91, 0,
                                        1>>,
                                    2, fixture, #{}, #{}, <<"out">>, <<"inflight">>, 1791301268573,
                                    #{}},
                            timestamp => 100,
                            phase => wait_comp,
                            packet_id => 41
                        }
                    ],
                created_at => 1791301268573,
                subscriptions => #{<<"out">> => #{qos => 2}},
                is_persistent => true,
                next_pkt_id => 43,
                awaiting_rel => #{42 => 101}
            },
        conf =>
            #{
                retry_interval => 250,
                max_subscriptions => 23,
                upgrade_qos => true,
                receive_maximum => 1,
                max_awaiting_rel => 12,
                await_rel_timeout => 5000,
                enable_quota => false
            }
    }.

clientinfo() ->
    #{
        zone => default,
        listener => 'udp:default',
        clientid => <<"fixture">>,
        protocol => mqttsn
    }.
