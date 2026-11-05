%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cm_takeover_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

%% Frozen session records provide field indexes; fixtures retain their wire tags.

%% Copied from emqx_session_mem.hrl @ 6.2.2.
-record(session_62x, {
    clientid :: emqx_types:clientid(),
    id :: emqx_session:session_id(),
    is_persistent :: boolean(),
    subscriptions :: map(),
    max_subscriptions :: non_neg_integer() | infinity,
    upgrade_qos = false :: boolean(),
    inflight :: emqx_inflight:inflight(),
    mqueue :: emqx_mqueue:mqueue(),
    next_pkt_id = 1 :: emqx_types:packet_id(),
    retry_interval :: timeout(),
    awaiting_rel :: map(),
    max_awaiting_rel :: non_neg_integer() | infinity,
    await_rel_timeout :: timeout(),
    created_at :: pos_integer()
}).

%% Copied from emqx_session_mem.hrl @ 6.3.0.
-record(session_630, {
    id :: emqx_session:session_id(),
    is_persistent :: boolean(),
    subscriptions :: emqx_session_mem:subscriptions(),
    max_subscriptions :: non_neg_integer() | infinity,
    upgrade_qos = false :: boolean(),
    inflight :: emqx_inflight:inflight(),
    mqueue :: emqx_mqueue:mqueue(),
    quota :: emqx_limiter_client_container:t() | false,
    next_pkt_id = 1 :: emqx_types:packet_id(),
    retry_interval :: timeout(),
    awaiting_rel :: emqx_session_mem:awaiting_rel(),
    max_awaiting_rel :: non_neg_integer() | infinity,
    await_rel_timeout :: timeout(),
    created_at :: pos_integer()
}).

%% Copied from emqx_session_mem.hrl @ 6.3.1.
-record(session_631, {
    id :: emqx_session:session_id(),
    is_persistent :: boolean(),
    subscriptions :: emqx_session_mem:subscriptions(),
    max_subscriptions :: non_neg_integer() | infinity,
    upgrade_qos = false :: boolean(),
    inflight :: emqx_inflight:inflight(),
    mqueue :: emqx_mqueue:mqueue(),
    quota :: emqx_limiter_client_container:t() | false | {lazy, emqx_limiter:listener_id()},
    next_pkt_id = 1 :: emqx_types:packet_id(),
    retry_interval :: timeout(),
    awaiting_rel :: emqx_session_mem:awaiting_rel(),
    max_awaiting_rel :: non_neg_integer() | infinity,
    await_rel_timeout :: timeout(),
    created_at :: pos_integer()
}).

%% Legacy mqueue record layouts

-define(MQUEUE_62X(STOREQOS0, MAXLEN, LEN, DROPPED, PTAB, DEFPRIO, Q, SHIFTOPTS, LASTPRIO, PCRED),
    {mqueue, STOREQOS0, MAXLEN, LEN, DROPPED, PTAB, DEFPRIO, Q, SHIFTOPTS, LASTPRIO, PCRED}
).

-define(MQUEUE_630(STOREQOS0, MAXLEN, DROPPED, PAYLOAD_BYTES, Q, PRIOS, PCRED),
    {mqueue, STOREQOS0, MAXLEN, DROPPED, PAYLOAD_BYTES, Q, PRIOS, PCRED}
).

-define(MQUEUE_631(STOREQOS0, MAXLEN, DROPPED, PAYLOAD_BYTES, Q, PRIOS, PCRED),
    ?MQUEUE_630(STOREQOS0, MAXLEN, DROPPED, PAYLOAD_BYTES, Q, PRIOS, PCRED)
).

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

-doc "Session codecs convert legacy timestamps and leave the current protocol unchanged.".
t_session_map_timestamps(_) ->
    #{v1 := V1, current := Current, upgraded := Upgraded} = fixture_session_timestamps(),
    ?assertEqual(Current, emqx_cm_takeover:to_session(Current, #{vsn => 2})),
    ?assertEqual(Current, emqx_cm_takeover:from_session(Current, #{vsn => 2})),
    ?assertEqual(V1, emqx_cm_takeover:to_session(Current, #{vsn => 1})),
    ?assertEqual(Upgraded, emqx_cm_takeover:from_session(V1, #{vsn => 1})).

-doc "Exported (vsn 1) messages without delivery timestamps use the inflight entry timestamp.".
t_session_map_timestamp_fallback(_) ->
    #{v1 := V1, upgraded := Upgraded} = fixture_session_timestamp_fallback(),
    ?assertEqual(Upgraded, emqx_cm_takeover:from_session(V1, #{vsn => 1})).

-doc "Local takeover keeps the current session format while RPC replies use requester protocol.".
t_begin_session_format(_) ->
    #{exported := Session, {exported, 1} := SessionV1} = fixture(),
    ok = meck:new(emqx_cm, [passthrough, no_link]),
    try
        ok = meck:expect(emqx_cm, do_get_chann_conn_mod, fun(_, _) -> emqx_connection end),
        ok = meck:expect(emqx_cm, request_stepdown, fun(_, _, _, _) -> {ok, Session} end),
        ?assertMatch({ok, _, Session}, emqx_cm_takeover:begin_local(<<"fixture">>, self())),
        ?assertMatch(
            {ok, _, Session},
            emqx_cm_takeover:begin_rpc(<<"fixture">>, self(), #{vsn => 2})
        ),
        ?assertMatch(
            {ok, _, SessionV1},
            emqx_cm_takeover:begin_rpc(<<"fixture">>, self(), #{vsn => 1})
        )
    after
        meck:unload(emqx_cm)
    end.

-doc "Decode a 6.3.0 priority queue, preserving timestamps and restoring insertion order.".
t_decode_630_priorities(_Config) ->
    #{session := Session, exported := Expected} = fixture_630_priorities(),
    ?assertEqual(Expected, emqx_cm_takeover:from_legacy_session(Session, v630)).

t_decode_631(_Config) ->
    #{session := Session, exported := Expected} = fixture(),
    ?assertEqual(Expected, emqx_cm_takeover:from_legacy_session(Session, v630)).

-doc "Import a version 1 exported map with the 6.3.0 priority queue messages.".
t_decode_exported_630_priorities(_Config) ->
    #{{exported, 1} := Exported, exported := Expected} = fixture_630_priorities(),
    ?assertEqual(Expected, emqx_cm_takeover:from_session(Exported, #{vsn => 1})).

-doc "Import a version 1 exported map with 6.3.1 inflight delivery metadata.".
t_decode_exported_631(_Config) ->
    #{{exported, 1} := Exported, exported := Expected} = fixture(),
    ?assertEqual(Expected, emqx_cm_takeover:from_session(Exported, #{vsn => 1})).

t_encode_631(_Config) ->
    ChanInfo = #{clientinfo => clientinfo(), conninfo => #{receive_maximum => 1}},
    #{session := Expected, exported := Exported, conf := Conf} = fixture(),
    Session = emqx_cm_takeover:to_legacy_session(<<"fixture">>, ChanInfo, Exported, Conf, v630),
    %% Keep the tagged session layout, but flatten its queue to delivery order.
    %% The three payloads total 16 bytes: "first", "second", "third".
    Messages = legacy_mqueue_messages(Session, v630),
    Queue = ?MQUEUE_631(true, 1000, 0, 16, {queue, [], Messages, 3}, disabled, undefined),
    ?assertEqual(setelement(#session_631.mqueue, Expected, Queue), Session),
    ?assertEqual(Exported, emqx_cm_takeover:from_legacy_session(Session, v630)),
    %% The pre-6.3 codec remains available for MQTT BPAPI v3 and old gateways.
    Old = emqx_cm_takeover:to_legacy_session(<<"fixture">>, ChanInfo, Exported, Conf, v5),
    ?assertEqual(session, element(1, Old)),
    ?assertMatch(
        #session_62x{
            clientid = <<"fixture">>,
            inflight = {inflight, 1, _},
            mqueue = ?MQUEUE_62X(_, _, _, _, _, _, _, _, _, _)
        },
        setelement(1, Old, session_62x)
    ),
    ?assertEqual(Exported, emqx_cm_takeover:from_legacy_session(Old, v5)).

-doc "6.2.x queues carry pagination timestamps without changing message metadata.".
t_encode_mqueue_timestamps_62x(_Config) ->
    assert_encode_mqueue_timestamps(v5).

-doc "6.3.0/6.3.1 queues carry pagination timestamps without changing message metadata.".
t_encode_mqueue_timestamps_630(_Config) ->
    assert_encode_mqueue_timestamps(v630).

assert_encode_mqueue_timestamps(Version) ->
    ChanInfo = #{clientinfo => clientinfo()},
    #{exported := Exported0 = #{mqueue := Messages0}, conf := Conf} = fixture(),
    Messages = [
        Msg#message{extra = Extra#{other_metadata => preserved}}
     || Msg = #message{extra = Extra} <- Messages0
    ],
    %% Flattening must restore timestamp order, even when priorities changed traversal order.
    Exported = Exported0#{mqueue := lists:reverse(Messages)},
    Session = emqx_cm_takeover:to_legacy_session(<<"fixture">>, ChanInfo, Exported, Conf, Version),
    Stamped = legacy_mqueue_messages(Session, Version),
    Timestamps = [Ts || #message{extra = #{mqueue_insert_ts := Ts}} <- Stamped],
    ?assertEqual(length(Messages), length(Timestamps)),
    ?assert(lists:all(fun is_integer/1, Timestamps)),
    ?assertEqual(Timestamps, lists:sort(Timestamps)),
    ?assertEqual(Messages, Stamped),
    ?assertEqual(
        Exported0#{mqueue := Messages},
        emqx_cm_takeover:from_legacy_session(Session, Version)
    ).

legacy_mqueue_messages(Session, v5) ->
    ?MQUEUE_62X(_, _, _, _, _, _, {queue, [], Messages, _}, _, _, _) = element(
        #session_62x.mqueue,
        Session
    ),
    Messages;
legacy_mqueue_messages(Session, v630) ->
    ?MQUEUE_630(_, _, _, _, {queue, [], Messages, _}, _, _) = element(
        #session_630.mqueue,
        Session
    ),
    Messages.

%% Fixtures

%% Synthetic map fixtures cover sub-millisecond delivery times and both extra representations.
fixture_session_timestamps() ->
    Message = #message{
        id = <<1:128>>,
        qos = 1,
        from = fixture,
        topic = <<"out">>,
        payload = <<"payload">>,
        timestamp = 1791301268573
    },
    V1 = Message#message{headers = #{deliver_begin_at => 1791301268573}},
    Current = Message#message{headers = #{deliver_begin_at_us => 1791301268573123}},
    Upgraded = Message#message{headers = #{deliver_begin_at_us => 1791301268573000}},
    Entry1 = #{packet_id => 41, phase => wait_ack, timestamp => 1791301269000},
    Entry2 = #{packet_id => 42, phase => wait_ack, timestamp => 1791301269000},
    #{
        v1 => #{
            inflight => [
                Entry1#{
                    message => V1#message{
                        extra = #{other => metadata, inflight_insert_ts => 1791301268573123000}
                    }
                },
                Entry2#{
                    message => V1#message{
                        extra = #{inflight_insert_ts => 1791301268573123000}
                    }
                }
            ]
        },
        current => #{
            inflight => [
                Entry1#{message => Current#message{extra = #{other => metadata}}},
                Entry2#{message => Current#message{extra = []}}
            ]
        },
        upgraded => #{
            inflight => [
                Entry1#{message => Upgraded#message{extra = #{other => metadata}}},
                Entry2#{message => Upgraded#message{extra = #{}}}
            ]
        }
    }.

fixture_session_timestamp_fallback() ->
    Message = #message{
        id = <<2:128>>,
        qos = 2,
        from = fixture,
        topic = <<"out">>,
        payload = <<"payload">>,
        timestamp = 40,
        extra = []
    },
    Entry = #{packet_id => 41, phase => wait_comp, timestamp => 42},
    #{
        v1 => #{
            inflight => [
                Entry#{message => Message}
            ]
        },
        upgraded => #{
            inflight => [
                Entry#{message => Message#message{headers = #{deliver_begin_at_us => 42000}}}
            ]
        }
    }.

%% Generated with emqx_session_mem, emqx_mqueue and emqx_pqueue from tag 6.3.0.
%% Commit: 021c5ef13bf8c767058626ad9b760052c273736f.
%% emqx_mqueue:in/2 enqueued low-1, high-0a, default-2, high-1, low-0, high-0b.
%% Priorities: high = 10, low = 1, default = lowest; store_qos0 = true.
%% The high and low priorities contain mixed QoS0/non-QoS0 class queues.
%% Expected state retains the raw queue timestamps and orders messages by insertion time.
fixture_630_priorities() ->
    Session =
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
    Exported =
        #{
            id => <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 118>>,
            mqueue =>
                [
                    {message, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1>>, 1, fixture, #{},
                        #{}, <<"low">>, <<"low-1">>, 1000, #{
                            mqueue_insert_ts => 1791306814643791045
                        }},
                    {message, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2>>, 0, fixture, #{},
                        #{}, <<"high">>, <<"high-0a">>, 1000, #{
                            mqueue_insert_ts => 1791306814643795373
                        }},
                    {message, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3>>, 2, fixture, #{},
                        #{}, <<"other">>, <<"default-2">>, 1000, #{
                            mqueue_insert_ts => 1791306814643796435
                        }},
                    {message, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4>>, 1, fixture, #{},
                        #{}, <<"high">>, <<"high-1">>, 1000, #{
                            mqueue_insert_ts => 1791306814643797046
                        }},
                    {message, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5>>, 0, fixture, #{},
                        #{}, <<"low">>, <<"low-0">>, 1000, #{
                            mqueue_insert_ts => 1791306814643807645
                        }},
                    {message, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6>>, 0, fixture, #{},
                        #{}, <<"high">>, <<"high-0b">>, 1000, #{
                            mqueue_insert_ts => 1791306814643808487
                        }}
                ],
            is_persistent => true,
            subscriptions => #{<<"#">> => #{qos => 2}},
            inflight => [],
            next_pkt_id => 43,
            awaiting_rel => #{42 => 101},
            created_at => 1000
        },
    ExportedV1 = Exported,
    #{session => Session, exported => Exported, {exported, 1} => ExportedV1}.

%% Generated using session_mem, mqueue and pqueue from tag 6.3.1.
%% Commit: b0604926195f9372b0068b61f88d409d639a2401.
%% * Quota disabled, one wait_comp entry and one awaiting_rel entry.
%% * Synthetic inflight entry uses delivery metadata, with timestamps fixed to publication time.
%% * `enqueue/3` added QoS0, QoS1, QoS0 messages, exercising both queue lanes.
%% * Expected exported map retains the raw queue insertion timestamps.
fixture() ->
    Session =
        {session, <<0, 6, 93, 45, 211, 204, 142, 91, 212, 68, 0, 0, 179, 91, 0, 0>>, true,
            #{<<"out">> => #{qos => 2}}, 23, true,
            {inflight, 1,
                {1,
                    {41,
                        {inflight_data, wait_comp,
                            {message,
                                <<0, 6, 93, 45, 211, 204, 142, 120, 212, 68, 0, 0, 179, 91, 0, 1>>,
                                2, fixture, #{}, #{deliver_begin_at => 1791301268573}, <<"out">>,
                                <<"inflight">>, 1791301268573, #{
                                    inflight_insert_ts => 1791301268573000000
                                }},
                            1791301268573},
                        nil, nil}}},
            {mqueue, true, 1000, 0, 16,
                {qos0, qos0,
                    [
                        '$switch',
                        {message,
                            <<0, 6, 93, 45, 211, 204, 142, 121, 212, 68, 0, 0, 179, 91, 0, 3>>, 1,
                            fixture, #{}, #{}, <<"out">>, <<"second">>, 1791301268573, #{
                                mqueue_insert_ts => 1791301268573824212
                            }}
                    ],
                    [],
                    [
                        {message,
                            <<0, 6, 93, 45, 211, 204, 142, 121, 212, 68, 0, 0, 179, 91, 0, 4>>, 0,
                            fixture, #{}, #{}, <<"out">>, <<"third">>, 1791301268573, #{
                                mqueue_insert_ts => 1791301268573824733
                            }},
                        '$switch'
                    ],
                    [
                        {message,
                            <<0, 6, 93, 45, 211, 204, 142, 121, 212, 68, 0, 0, 179, 91, 0, 2>>, 0,
                            fixture, #{}, #{}, <<"out">>, <<"first">>, 1791301268573, #{
                                mqueue_insert_ts => 1791301268573820816
                            }}
                    ],
                    3},
                disabled, undefined},
            false, 43, 250, #{42 => 101}, 12, 5000, 1791301268573},
    Exported =
        #{
            id => <<0, 6, 93, 45, 211, 204, 142, 91, 212, 68, 0, 0, 179, 91, 0, 0>>,
            mqueue =>
                [
                    {message, <<0, 6, 93, 45, 211, 204, 142, 121, 212, 68, 0, 0, 179, 91, 0, 2>>, 0,
                        fixture, #{}, #{}, <<"out">>, <<"first">>, 1791301268573, #{
                            mqueue_insert_ts => 1791301268573820816
                        }},
                    {message, <<0, 6, 93, 45, 211, 204, 142, 121, 212, 68, 0, 0, 179, 91, 0, 3>>, 1,
                        fixture, #{}, #{}, <<"out">>, <<"second">>, 1791301268573, #{
                            mqueue_insert_ts => 1791301268573824212
                        }},
                    {message, <<0, 6, 93, 45, 211, 204, 142, 121, 212, 68, 0, 0, 179, 91, 0, 4>>, 0,
                        fixture, #{}, #{}, <<"out">>, <<"third">>, 1791301268573, #{
                            mqueue_insert_ts => 1791301268573824733
                        }}
                ],
            inflight =>
                [
                    #{
                        message =>
                            {message,
                                <<0, 6, 93, 45, 211, 204, 142, 120, 212, 68, 0, 0, 179, 91, 0, 1>>,
                                2, fixture, #{}, #{deliver_begin_at_us => 1791301268573000},
                                <<"out">>, <<"inflight">>, 1791301268573, #{}},
                        timestamp => 1791301268573,
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
    ExportedV1 =
        Exported#{
            inflight := [
                #{
                    message =>
                        {message,
                            <<0, 6, 93, 45, 211, 204, 142, 120, 212, 68, 0, 0, 179, 91, 0, 1>>, 2,
                            fixture, #{}, #{deliver_begin_at => 1791301268573}, <<"out">>,
                            <<"inflight">>, 1791301268573, #{
                                inflight_insert_ts => 1791301268573000000
                            }},
                    timestamp => 1791301268573,
                    phase => wait_comp,
                    packet_id => 41
                }
            ]
        },
    Conf =
        #{
            retry_interval => 250,
            max_subscriptions => 23,
            upgrade_qos => true,
            max_awaiting_rel => 12,
            await_rel_timeout => 5000
        },
    #{session => Session, exported => Exported, {exported, 1} => ExportedV1, conf => Conf}.

clientinfo() ->
    #{
        zone => default,
        listener => 'udp:default',
        clientid => <<"fixture">>,
        protocol => mqttsn
    }.
