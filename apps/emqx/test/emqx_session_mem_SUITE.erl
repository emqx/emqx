%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_session_mem_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

-type inflight_data_phase() :: wait_ack | wait_comp.

-record(inflight_data, {
    phase :: inflight_data_phase(),
    message :: emqx_types:message(),
    timestamp :: non_neg_integer()
}).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    ok = meck:new(
        [emqx_broker, emqx_hooks, emqx_session],
        [passthrough, no_history, no_link]
    ),
    ok = meck:expect(emqx_hooks, run, fun(_Hook, _Args) -> ok end),
    Apps = emqx_cth_suite:start(
        [
            {emqx, #{
                override_env => [{boot_modules, [broker]}]
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)),
    meck:unload([emqx_broker, emqx_hooks]).

%%--------------------------------------------------------------------
%% Test cases for session init
%%--------------------------------------------------------------------

t_session_init(_) ->
    ClientInfo = #{zone => default, clientid => <<"fake-test">>},
    ConnInfo = #{receive_maximum => 64, expiry_interval => 0},
    Session = emqx_session_mem:create(
        ClientInfo,
        ConnInfo,
        _WillMsg = undefined,
        emqx_session:get_session_conf(ClientInfo)
    ),
    ?assertEqual(#{}, emqx_session_mem:info(subscriptions, Session)),
    ?assertEqual(0, emqx_session_mem:info(subscriptions_cnt, Session)),
    ?assertEqual(infinity, emqx_session_mem:info(subscriptions_max, Session)),
    ?assertEqual(false, emqx_session_mem:info(upgrade_qos, Session)),
    ?assertEqual(0, emqx_session_mem:info(inflight_cnt, Session)),
    ?assertEqual(64, emqx_session_mem:info(inflight_max, Session)),
    ?assertEqual(1, emqx_session_mem:info(next_pkt_id, Session)),
    ?assertEqual(infinity, emqx_session_mem:info(retry_interval, Session)),
    ?assertEqual(0, emqx_mqueue:len(emqx_session_mem:info(mqueue, Session))),
    ?assertEqual(0, emqx_session_mem:info(awaiting_rel_cnt, Session)),
    ?assertEqual(100, emqx_session_mem:info(awaiting_rel_max, Session)),
    ?assertEqual(300000, emqx_session_mem:info(await_rel_timeout, Session)),
    ?assert(is_integer(emqx_session_mem:info(created_at, Session))).

%%--------------------------------------------------------------------
%% Test cases for session info/stats
%%--------------------------------------------------------------------

t_session_info(_) ->
    Keys = [subscriptions, upgrade_qos, retry_interval, await_rel_timeout],
    ?assertMatch(
        #{
            subscriptions := #{},
            upgrade_qos := false,
            retry_interval := infinity,
            await_rel_timeout := 300000
        },
        maps:from_list(emqx_session_mem:info(Keys, session()))
    ).

t_session_stats(_) ->
    Stats = emqx_session_mem:stats(session()),
    ?assertMatch(
        #{
            subscriptions_max := infinity,
            inflight_max := 0,
            mqueue_len := 0,
            mqueue_max := 1000,
            mqueue_dropped := 0,
            next_pkt_id := 1,
            awaiting_rel_cnt := 0,
            awaiting_rel_max := 100
        },
        maps:from_list(Stats)
    ).

t_session_inflight_query(_) ->
    EmptyInflight = emqx_inflight:new(500),
    Session = session(#{inflight => EmptyInflight}),
    EmptyQueryResMeta = {[], #{position => none, start => none}},
    ?assertEqual(EmptyQueryResMeta, inflight_query(Session, none, 10)),
    ?assertEqual(EmptyQueryResMeta, inflight_query(Session, none, 10)),
    RandPos = erlang:system_time(nanosecond),
    ?assertEqual({[], #{position => RandPos, start => none}}, inflight_query(Session, RandPos, 10)),
    Inflight = lists:foldl(
        fun(Seq, Acc) ->
            Msg = emqx_message:make(clientid, ?QOS_2, <<"t">>, integer_to_binary(Seq)),
            emqx_inflight:insert(Seq, emqx_session_mem:with_ts(Msg), Acc)
        end,
        EmptyInflight,
        lists:seq(1, 114)
    ),

    Session1 = session(#{inflight => Inflight}),

    {LastPos, LastStart} = lists:foldl(
        fun(PageSeq, {Pos, PrevStart}) ->
            Limit = 10,
            {Page, #{position := NextPos, start := Start}} = inflight_query(Session1, Pos, Limit),
            ?assertEqual(10, length(Page)),
            ExpFirst = PageSeq * Limit - Limit + 1,
            ExpLast = PageSeq * Limit,
            FirstMsg = lists:nth(1, Page),
            LastMsg = lists:nth(10, Page),
            ?assertEqual(integer_to_binary(ExpFirst), emqx_message:payload(FirstMsg)),
            ?assertEqual(integer_to_binary(ExpLast), emqx_message:payload(LastMsg)),
            %% start value must not change as Inflight is not modified during traversal
            NextStart =
                case PageSeq of
                    1 ->
                        ?assertEqual(inflight_ts(FirstMsg), Start),
                        Start;
                    _ ->
                        ?assertEqual(PrevStart, Start),
                        PrevStart
                end,
            ?assertEqual(inflight_ts(LastMsg), NextPos),
            {NextPos, NextStart}
        end,
        {none, none},
        lists:seq(1, 11)
    ),
    {LastPartialPage, #{position := FinalPos} = LastMeta} = inflight_query(
        Session1, LastPos, 10
    ),
    LastMsg = lists:nth(4, LastPartialPage),
    ?assertEqual(4, length(LastPartialPage)),
    ?assertEqual(<<"111">>, emqx_message:payload(lists:nth(1, LastPartialPage))),
    ?assertEqual(<<"114">>, emqx_message:payload(LastMsg)),
    ?assertEqual(#{position => inflight_ts(LastMsg), start => LastStart}, LastMeta),
    ?assertEqual(
        {[], #{start => LastStart, position => FinalPos}},
        inflight_query(Session1, FinalPos, 10)
    ),

    {LargePage, LargeMeta} = inflight_query(Session1, none, 1000),
    ?assertEqual(114, length(LargePage)),
    ?assertEqual(<<"1">>, emqx_message:payload(hd(LargePage))),
    ?assertEqual(<<"114">>, emqx_message:payload(lists:last(LargePage))),
    ?assertEqual(#{start => LastStart, position => FinalPos}, LargeMeta),

    {FullPage, FullMeta} = inflight_query(Session1, none, 114),
    ?assertEqual(LargePage, FullPage),
    ?assertEqual(LargeMeta, FullMeta),

    Session2 = session(#{inflight => emqx_inflight:delete(1, Inflight)}),
    {PageAfterRemove, #{start := StartAfterRemove}} = inflight_query(Session2, none, 10),
    ?assertEqual(<<"2">>, emqx_message:payload(hd(PageAfterRemove))),
    ?assertEqual(StartAfterRemove, inflight_ts(hd(PageAfterRemove))).

%%--------------------------------------------------------------------
%% Test cases for sub/unsub
%%--------------------------------------------------------------------

t_subscribe(_) ->
    ok = meck:expect(emqx_broker, subscribe, fun(_, _, _) -> ok end),
    {ok, Session} = emqx_session_mem:subscribe(<<"#">>, subopts(), session()),
    ?assertEqual(1, emqx_session_mem:info(subscriptions_cnt, Session)).

t_is_subscriptions_full_false(_) ->
    Session = session(#{max_subscriptions => infinity}),
    ?assertNot(emqx_session_mem:is_subscriptions_full(Session)).

t_is_subscriptions_full_true(_) ->
    ok = meck:expect(emqx_broker, subscribe, fun(_, _, _) -> ok end),
    Session = session(#{max_subscriptions => 1}),
    ?assertNot(emqx_session_mem:is_subscriptions_full(Session)),
    {ok, Session1} = emqx_session_mem:subscribe(
        <<"t1">>, subopts(), Session
    ),
    ?assert(emqx_session_mem:is_subscriptions_full(Session1)),
    {error, ?RC_QUOTA_EXCEEDED} = emqx_session_mem:subscribe(
        <<"t2">>, subopts(), Session1
    ).

t_unsubscribe(_) ->
    ok = meck:expect(emqx_broker, unsubscribe, fun(_) -> ok end),
    SubOpts = subopts(),
    Session = session(#{subscriptions => #{<<"#">> => SubOpts}}),
    {ok, Session1, SubOpts} = emqx_session_mem:unsubscribe(<<"#">>, Session),
    {error, ?RC_NO_SUBSCRIPTION_EXISTED} = emqx_session_mem:unsubscribe(<<"#">>, Session1).

t_publish_qos0(_) ->
    ok = meck:expect(emqx_broker, publish, fun(_) -> [] end),
    Msg = emqx_message:make(clientid, ?QOS_0, <<"t">>, <<"payload">>),
    {ok, [], [], Session} = emqx_session_mem:publish(1, Msg, Session = session()),
    {ok, [], [], Session} = emqx_session_mem:publish(undefined, Msg, Session).

t_publish_qos1(_) ->
    ok = meck:expect(emqx_broker, publish, fun(_) -> [] end),
    Msg = emqx_message:make(clientid, ?QOS_1, <<"t">>, <<"payload">>),
    {ok, [], [], Session} = emqx_session_mem:publish(1, Msg, Session = session()),
    {ok, [], [], Session} = emqx_session_mem:publish(2, Msg, Session).

t_publish_qos2(_) ->
    ok = meck:expect(emqx_broker, publish, fun(_) -> [] end),
    Msg = emqx_message:make(clientid, ?QOS_2, <<"t">>, <<"payload">>),
    {ok, [], Session} = emqx_session_mem:publish(1, Msg, session()),
    ?assertEqual(1, emqx_session_mem:info(awaiting_rel_cnt, Session)),
    {ok, Session1} = emqx_session_mem:pubrel(1, Session),
    ?assertEqual(0, emqx_session_mem:info(awaiting_rel_cnt, Session1)),
    {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} = emqx_session_mem:pubrel(1, Session1).

t_publish_qos2_with_error_return(_) ->
    ok = meck:expect(emqx_broker, publish, fun(_) -> [] end),
    ok = meck:expect(emqx_hooks, run, fun
        ('message.dropped', [Msg, _By, ReasonName]) ->
            self() ! {'message.dropped', ReasonName, Msg},
            ok;
        (_Hook, _Arg) ->
            ok
    end),

    PacketId1 = 1,
    Session = session(#{max_awaiting_rel => 2, awaiting_rel => #{PacketId1 => ts(millisecond)}}),
    Msg1 = emqx_message:make(clientid, ?QOS_2, <<"t">>, <<"payload1">>),
    {error, RC1 = ?RC_PACKET_IDENTIFIER_IN_USE} = emqx_session:publish(
        clientinfo(), PacketId1, Msg1, Session
    ),
    receive
        {'message.dropped', Reason1, RecMsg1} ->
            ?assertEqual(Reason1, emqx_reason_codes:name(RC1)),
            ?assertEqual(RecMsg1, Msg1)
    after 1000 ->
        ct:fail(?FUNCTION_NAME)
    end,

    Msg2 = emqx_message:make(clientid, ?QOS_2, <<"t">>, <<"payload2">>),
    {ok, [], Session1} = emqx_session:publish(
        clientinfo(), _PacketId2 = 2, Msg2, Session
    ),
    ?assertEqual(2, emqx_session_mem:info(awaiting_rel_cnt, Session1)),
    {error, RC2 = ?RC_RECEIVE_MAXIMUM_EXCEEDED} = emqx_session:publish(
        clientinfo(), _PacketId3 = 3, Msg2, Session1
    ),
    receive
        {'message.dropped', Reason2, RecMsg2} ->
            ?assertEqual(Reason2, emqx_reason_codes:name(RC2)),
            ?assertEqual(RecMsg2, Msg2)
    after 1000 ->
        ct:fail(?FUNCTION_NAME)
    end,

    ok = meck:expect(emqx_hooks, run, fun(_Hook, _Args) -> ok end).

t_is_awaiting_full_false(_) ->
    Session = session(#{max_awaiting_rel => infinity}),
    ?assertNot(emqx_session_mem:is_awaiting_full(Session)).

t_is_awaiting_full_true(_) ->
    Session = session(#{
        max_awaiting_rel => 1,
        awaiting_rel => #{1 => ts(millisecond)}
    }),
    ?assert(emqx_session_mem:is_awaiting_full(Session)).

t_puback(_) ->
    Msg = emqx_message:make(test, ?QOS_1, <<"t">>, <<>>),
    Inflight = emqx_inflight:insert(1, with_ts(wait_ack, Msg), emqx_inflight:new()),
    Session = session(#{inflight => Inflight, mqueue => mqueue()}),
    {ok, Msg, [], Session1} = emqx_session_mem:puback(clientinfo(), 1, Session),
    ?assertEqual(0, emqx_session_mem:info(inflight_cnt, Session1)).

t_puback_with_dequeue(_) ->
    Msg1 = emqx_message:make(clientid, ?QOS_1, <<"t1">>, <<"payload1">>),
    Inflight = emqx_inflight:insert(1, with_ts(wait_ack, Msg1), emqx_inflight:new()),
    Msg2 = emqx_message:make(clientid, ?QOS_1, <<"t2">>, <<"payload2">>),
    {_, Q} = emqx_mqueue:in(Msg2, mqueue(#{max_len => 10})),
    Session = session(#{inflight => Inflight, mqueue => Q}),
    {ok, Msg1, [{_, Msg3}], Session1} = emqx_session_mem:puback(clientinfo(), 1, Session),
    ?assertEqual(1, emqx_session_mem:info(inflight_cnt, Session1)),
    ?assertEqual(0, emqx_session_mem:info(mqueue_len, Session1)),
    ?assertEqual(<<"t2">>, emqx_message:topic(Msg3)).

t_puback_error_packet_id_in_use(_) ->
    Inflight = emqx_inflight:insert(1, with_ts(wait_comp, undefined), emqx_inflight:new()),
    {error, ?RC_PACKET_IDENTIFIER_IN_USE} =
        emqx_session_mem:puback(clientinfo(), 1, session(#{inflight => Inflight})).

t_puback_error_packet_id_not_found(_) ->
    {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} = emqx_session_mem:puback(clientinfo(), 1, session()).

t_pubrec(_) ->
    Msg = emqx_message:make(test, ?QOS_2, <<"t">>, <<>>),
    Inflight = emqx_inflight:insert(2, with_ts(wait_ack, Msg), emqx_inflight:new()),
    Session = session(#{inflight => Inflight}),
    {ok, Msg, Session1} = emqx_session_mem:pubrec(2, Session),
    ?assertMatch(
        [#inflight_data{phase = wait_comp}],
        emqx_inflight:values(emqx_session_mem:info(inflight, Session1))
    ).

t_pubrec_packet_id_in_use_error(_) ->
    Inflight = emqx_inflight:insert(1, with_ts(wait_comp, undefined), emqx_inflight:new()),
    Session = session(#{inflight => Inflight}),
    {error, ?RC_PACKET_IDENTIFIER_IN_USE} = emqx_session_mem:pubrec(1, Session).

t_pubrec_packet_id_not_found_error(_) ->
    {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} = emqx_session_mem:pubrec(1, session()).

t_pubrel(_) ->
    Session = session(#{awaiting_rel => #{1 => ts(millisecond)}}),
    {ok, Session1} = emqx_session_mem:pubrel(1, Session),
    ?assertEqual(#{}, emqx_session_mem:info(awaiting_rel, Session1)).

t_pubrel_error_packetid_not_found(_) ->
    {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} = emqx_session_mem:pubrel(1, session()).

t_pubcomp(_) ->
    Msg = emqx_message:make(test, ?QOS_2, <<"t">>, <<>>),
    Inflight = emqx_inflight:insert(1, with_ts(wait_comp, Msg), emqx_inflight:new()),
    Session = session(#{inflight => Inflight}),
    {ok, Msg, [], Session1} = emqx_session_mem:pubcomp(clientinfo(), 1, Session),
    ?assertEqual(0, emqx_session_mem:info(inflight_cnt, Session1)).

t_pubcomp_error_packetid_in_use(_) ->
    Msg = emqx_message:make(test, ?QOS_2, <<"t">>, <<>>),
    Inflight = emqx_inflight:insert(1, {Msg, ts(millisecond)}, emqx_inflight:new()),
    Session = session(#{inflight => Inflight}),
    {error, ?RC_PACKET_IDENTIFIER_IN_USE} = emqx_session_mem:pubcomp(clientinfo(), 1, Session).

t_pubcomp_error_packetid_not_found(_) ->
    {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} = emqx_session_mem:pubcomp(clientinfo(), 1, session()).

%%--------------------------------------------------------------------
%% Test cases for deliver/retry
%%--------------------------------------------------------------------

t_dequeue(_) ->
    Q = mqueue(#{store_qos0 => true}),
    {ok, [], Session} = emqx_session_mem:dequeue(clientinfo(), session(#{mqueue => Q})),
    Msgs = [
        emqx_message:make(clientid, ?QOS_0, <<"t0">>, <<"payload">>),
        emqx_message:make(clientid, ?QOS_1, <<"t1">>, <<"payload">>),
        emqx_message:make(clientid, ?QOS_2, <<"t2">>, <<"payload">>)
    ],
    Session1 = emqx_session_mem:enqueue(clientinfo(), Msgs, Session),
    {ok, [{undefined, Msg0}, {1, Msg1}, {2, Msg2}], Session2} =
        emqx_session_mem:dequeue(clientinfo(), Session1),
    ?assertEqual(0, emqx_session_mem:info(mqueue_len, Session2)),
    ?assertEqual(2, emqx_session_mem:info(inflight_cnt, Session2)),
    ?assertEqual(<<"t0">>, emqx_message:topic(Msg0)),
    ?assertEqual(<<"t1">>, emqx_message:topic(Msg1)),
    ?assertEqual(<<"t2">>, emqx_message:topic(Msg2)).

t_deliver_qos0(_) ->
    ok = meck:expect(emqx_broker, subscribe, fun(_, _, _) -> ok end),
    {ok, Session} = emqx_session_mem:subscribe(<<"t0">>, subopts(), session()),
    {ok, Session1} = emqx_session_mem:subscribe(<<"t1">>, subopts(), Session),
    Deliveries = enrich([delivery(?QOS_0, T) || T <- [<<"t0">>, <<"t1">>]], Session1),
    {ok, [{undefined, Msg1}, {undefined, Msg2}], Session1} =
        emqx_session_mem:deliver(clientinfo(), Deliveries, Session1),
    ?assertEqual(<<"t0">>, emqx_message:topic(Msg1)),
    ?assertEqual(<<"t1">>, emqx_message:topic(Msg2)).

t_deliver_qos1(_) ->
    ok = meck:expect(emqx_broker, subscribe, fun(_, _, _) -> ok end),
    {ok, Session} = emqx_session_mem:subscribe(
        <<"t1">>, subopts(#{qos => ?QOS_1}), session()
    ),
    Delivers = enrich([delivery(?QOS_1, T) || T <- [<<"t1">>, <<"t2">>]], Session),
    {ok, [{1, Msg1}, {2, Msg2}], Session1} =
        emqx_session_mem:deliver(clientinfo(), Delivers, Session),
    ?assertEqual(2, emqx_session_mem:info(inflight_cnt, Session1)),
    ?assertEqual(<<"t1">>, emqx_message:topic(Msg1)),
    ?assertEqual(<<"t2">>, emqx_message:topic(Msg2)),
    {ok, Msg1T, [], Session2} = emqx_session_mem:puback(clientinfo(), 1, Session1),
    ?assertEqual(Msg1, remove_deliver_flag(Msg1T)),
    ?assertEqual(1, emqx_session_mem:info(inflight_cnt, Session2)),
    {ok, Msg2T, [], Session3} = emqx_session_mem:puback(clientinfo(), 2, Session2),
    ?assertEqual(Msg2, remove_deliver_flag(Msg2T)),
    ?assertEqual(0, emqx_session_mem:info(inflight_cnt, Session3)).

t_deliver_qos2(_) ->
    ok = meck:expect(emqx_broker, subscribe, fun(_, _, _) -> ok end),
    Session = session(),
    Delivers = enrich([delivery(?QOS_2, <<"t0">>), delivery(?QOS_2, <<"t1">>)], Session),
    {ok, [{1, Msg1}, {2, Msg2}], Session1} =
        emqx_session_mem:deliver(clientinfo(), Delivers, Session),
    ?assertEqual(2, emqx_session_mem:info(inflight_cnt, Session1)),
    ?assertEqual(<<"t0">>, emqx_message:topic(Msg1)),
    ?assertEqual(<<"t1">>, emqx_message:topic(Msg2)).

t_deliver_one_msg(_) ->
    Session = session(),
    {ok, [{1, Msg}], Session1} = emqx_session_mem:deliver(
        clientinfo(),
        enrich(delivery(?QOS_1, <<"t1">>), Session),
        Session
    ),
    ?assertEqual(1, emqx_session_mem:info(inflight_cnt, Session1)),
    ?assertEqual(<<"t1">>, emqx_message:topic(Msg)).

t_deliver_when_inflight_is_full(_) ->
    Session = session(#{inflight => emqx_inflight:new(1)}),
    Delivers = enrich([delivery(?QOS_1, <<"t1">>), delivery(?QOS_2, <<"t2">>)], Session),
    {ok, Publishes, Session1} =
        emqx_session_mem:deliver(clientinfo(), Delivers, Session),
    ?assertEqual(1, length(Publishes)),
    ?assertEqual(1, emqx_session_mem:info(inflight_cnt, Session1)),
    ?assertEqual(1, emqx_session_mem:info(mqueue_len, Session1)),
    {ok, Msg1, [{2, Msg2}], Session2} =
        emqx_session_mem:puback(clientinfo(), 1, Session1),
    ?assertEqual(1, emqx_session_mem:info(inflight_cnt, Session2)),
    ?assertEqual(0, emqx_session_mem:info(mqueue_len, Session2)),
    ?assertEqual(<<"t1">>, emqx_message:topic(Msg1)),
    ?assertEqual(<<"t2">>, emqx_message:topic(Msg2)).

t_enqueue(_) ->
    Session = session(#{mqueue => mqueue(#{max_len => 3, store_qos0 => true})}),
    Session1 = emqx_session_mem:enqueue(
        clientinfo(),
        emqx_session:enrich_delivers(
            clientinfo(),
            [
                delivery(?QOS_0, <<"t0">>),
                delivery(?QOS_1, <<"t1">>),
                delivery(?QOS_2, <<"t2">>)
            ],
            Session
        ),
        Session
    ),
    ?assertEqual(3, emqx_session_mem:info(mqueue_len, Session1)),
    Session2 = emqx_session_mem:enqueue(
        clientinfo(),
        emqx_session:enrich_delivers(clientinfo(), [delivery(?QOS_1, <<"drop">>)], Session1),
        Session1
    ),
    ?assertEqual(3, emqx_session_mem:info(mqueue_len, Session2)).

t_enqueue_qos0(_) ->
    Session = session(#{mqueue => mqueue(#{store_qos0 => false})}),
    Session1 = emqx_session_mem:enqueue(
        clientinfo(),
        emqx_session:enrich_delivers(
            clientinfo(),
            [
                delivery(?QOS_0, <<"t0">>),
                delivery(?QOS_1, <<"t1">>),
                delivery(?QOS_2, <<"t2">>)
            ],
            Session
        ),
        Session
    ),
    ?assertEqual(2, emqx_session_mem:info(mqueue_len, Session1)).

t_retry(_) ->
    RetryIntervalMs = 1000,
    Session = session(#{retry_interval => RetryIntervalMs}),
    Delivers = enrich(
        [
            delivery(?QOS_1, <<"t1">>, <<"expiressoon">>, _Expiry = 1),
            delivery(?QOS_2, <<"t2">>),
            delivery(?QOS_0, <<"t3">>),
            delivery(?QOS_1, <<"t4">>)
        ],
        Session
    ),
    {ok, Pubs, Session1} = emqx_session_mem:deliver(clientinfo(), Delivers, Session),
    [_Pub1, Pub2, _Pub3, Pub4] = Pubs,
    {ok, _Msg, Session2} = emqx_session_mem:pubrec(get_packet_id(Pub2), Session1),
    ElapseMs = 1500,
    ok = timer:sleep(ElapseMs),
    {ok, PubsRetry, RetryIntervalMs, Session3} = emqx_session_mem:handle_timeout(
        clientinfo(), retry_delivery, Session2
    ),
    ?assertEqual(
        [
            % Pub1 is expired
            {pubrel, get_packet_id(Pub2)},
            % Pub3 is QoS0
            set_duplicate_pub(Pub4)
        ],
        remove_deliver_flag(PubsRetry)
    ),
    ?assertEqual(
        2,
        emqx_session_mem:info(inflight_cnt, Session3)
    ).

%%--------------------------------------------------------------------
%% Test cases for takeover/resume
%%--------------------------------------------------------------------

t_takeover(_) ->
    ok = meck:expect(emqx_broker, unsubscribe, fun(_) -> ok end),
    Session = session(#{subscriptions => #{<<"t">> => ?DEFAULT_SUBOPTS}}),
    ok = emqx_session_mem:takeover(Session).

t_resume(_) ->
    ok = meck:expect(emqx_broker, subscribe, fun(_, _, _) -> ok end),
    Session = session(#{subscriptions => #{<<"t">> => ?DEFAULT_SUBOPTS}}),
    _ = emqx_session_mem:resume(#{clientid => <<"clientid">>}, Session).

t_replay(_) ->
    Session = session(),
    Messages = enrich([delivery(?QOS_1, <<"t1">>), delivery(?QOS_2, <<"t2">>)], Session),
    {ok, Pubs, Session1} = emqx_session_mem:deliver(clientinfo(), Messages, Session),
    Msg = emqx_message:make(clientid, ?QOS_1, <<"t1">>, <<"payload">>),
    Session2 = emqx_session_mem:enqueue(clientinfo(), [Msg], Session1),
    Pubs1 = [{I, emqx_message:set_flag(dup, M)} || {I, M} <- Pubs],
    Pendings =
        [Msg4, Msg5] = enrich(
            [_D4 = delivery(?QOS_1, <<"t4">>), D5 = delivery(?QOS_2, <<"t5">>)],
            Session1
        ),
    _ = self() ! D5,
    _ = self() ! D6 = delivery(?QOS_1, <<"t6">>),
    [Msg6] = enrich([D6], Session1),
    {ok, ReplayPubs, Session3} = emqx_session_mem:replay(clientinfo(), Pendings, Session2),
    ?assertEqual(
        Pubs1 ++ [{3, Msg}, {4, Msg4}, {5, Msg5}, {6, Msg6}],
        remove_deliver_flag(ReplayPubs)
    ),
    ?assertEqual(6, emqx_session_mem:info(inflight_cnt, Session3)).

t_expire_awaiting_rel(_) ->
    Now = ts(millisecond),
    AwaitRelTimeout = 10000,
    Session = session(#{await_rel_timeout => AwaitRelTimeout}),
    Ts1 = Now - 1000,
    Ts2 = Now - 20000,
    {ok, [], Session1} = emqx_session_mem:expire(clientinfo(), Session),
    Session2 = emqx_session_mem:set_field(awaiting_rel, #{1 => Ts1, 2 => Ts2}, Session1),
    {ok, [], Timeout, Session3} = emqx_session_mem:expire(clientinfo(), Session2),
    ?assertEqual(#{1 => Ts1}, emqx_session_mem:info(awaiting_rel, Session3)),
    ?assert(Timeout =< AwaitRelTimeout).

t_expire_awaiting_rel_all(_) ->
    Session = session(#{awaiting_rel => #{1 => 1, 2 => 2}}),
    {ok, [], Session1} = emqx_session_mem:expire(clientinfo(), Session),
    ?assertEqual(#{}, emqx_session_mem:info(awaiting_rel, Session1)).

%%--------------------------------------------------------------------
%% CT for utility functions
%%--------------------------------------------------------------------

t_next_pakt_id(_) ->
    Session = session(#{next_pkt_id => 16#FFFF}),
    Session1 = emqx_session_mem:next_pkt_id(Session),
    ?assertEqual(1, emqx_session_mem:info(next_pkt_id, Session1)),
    Session2 = emqx_session_mem:next_pkt_id(Session1),
    ?assertEqual(2, emqx_session_mem:info(next_pkt_id, Session2)).

t_obtain_next_pkt_id(_) ->
    Session = session(#{next_pkt_id => 16#FFFF}),
    {16#FFFF, Session1} = emqx_session_mem:obtain_next_pkt_id(Session),
    ?assertEqual(1, emqx_session_mem:info(next_pkt_id, Session1)),
    {1, Session2} = emqx_session_mem:obtain_next_pkt_id(Session1),
    ?assertEqual(2, emqx_session_mem:info(next_pkt_id, Session2)).

%% Helper functions
%%--------------------------------------------------------------------

mqueue() -> mqueue(#{}).
mqueue(Opts) ->
    emqx_mqueue:init(maps:merge(#{max_len => 0, store_qos0 => false}, Opts)).

session() -> session(#{}).
session(InitFields) when is_map(InitFields) ->
    ClientInfo = #{zone => default, clientid => <<"fake-test">>},
    ConnInfo = #{receive_maximum => 0, expiry_interval => 0},
    Session = emqx_session_mem:create(
        ClientInfo,
        ConnInfo,
        _WillMsg = undefined,
        emqx_session:get_session_conf(ClientInfo)
    ),
    maps:fold(
        fun(Field, Value, SessionAcc) ->
            emqx_session_mem:set_field(Field, Value, SessionAcc)
        end,
        Session,
        InitFields
    ).

clientinfo() -> clientinfo(#{}).
clientinfo(Init) ->
    maps:merge(
        #{
            zone => ?MODULE,
            clientid => <<"clientid">>,
            username => <<"username">>
        },
        Init
    ).

subopts() -> subopts(#{}).
subopts(Init) ->
    maps:merge(?DEFAULT_SUBOPTS, Init).

delivery(QoS, Topic) ->
    Payload = emqx_guid:to_hexstr(emqx_guid:gen()),
    {deliver, Topic, emqx_message:make(test, QoS, Topic, Payload)}.

delivery(QoS, Topic, Payload, ExpiryInterval) ->
    Headers = #{properties => #{'Message-Expiry-Interval' => ExpiryInterval}},
    {deliver, Topic, emqx_message:make(test, QoS, Topic, Payload, #{}, Headers)}.

enrich(Delivers, Session) when is_list(Delivers) ->
    emqx_session:enrich_delivers(clientinfo(), Delivers, Session);
enrich(Delivery, Session) when is_tuple(Delivery) ->
    enrich([Delivery], Session).

ts(second) ->
    erlang:system_time(second);
ts(millisecond) ->
    erlang:system_time(millisecond).

with_ts(Phase, Msg) ->
    with_ts(Phase, Msg, erlang:system_time(millisecond)).

with_ts(Phase, Msg, Ts) ->
    #inflight_data{
        phase = Phase,
        message = Msg,
        timestamp = Ts
    }.

remove_deliver_flag({pubrel, Id}) ->
    {pubrel, Id};
remove_deliver_flag({Id, Data}) ->
    {Id, remove_deliver_flag(Data)};
remove_deliver_flag(List) when is_list(List) ->
    lists:map(fun remove_deliver_flag/1, List);
remove_deliver_flag(Msg) ->
    emqx_message:remove_header(deliver_begin_at, Msg).

set_duplicate_pub({Id, Msg}) ->
    {Id, emqx_message:set_flag(dup, Msg)}.

get_packet_id({Id, _}) ->
    Id.

inflight_query(Session, Pos, Limit) ->
    emqx_session_mem:info({inflight_msgs, #{position => Pos, limit => Limit}}, Session).

inflight_ts(#message{extra = #{inflight_insert_ts := Ts}}) -> Ts.
