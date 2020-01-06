%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_session_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    ok = meck:new([emqx_hooks, emqx_metrics, emqx_broker],
                  [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_metrics, inc, fun(_) -> ok end),
    ok = meck:expect(emqx_metrics, inc, fun(_K, _V) -> ok end),
    ok = meck:expect(emqx_hooks, run, fun(_Hook, _Args) -> ok end),
    Config.

end_per_suite(_Config) ->
    meck:unload([emqx_broker, emqx_hooks, emqx_metrics]).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Test cases for session init
%%--------------------------------------------------------------------

t_session_init(_) ->
    Session = emqx_session:init(#{zone => zone}, #{receive_maximum => 64}),
    ?assertEqual(#{}, emqx_session:info(subscriptions, Session)),
    ?assertEqual(0, emqx_session:info(subscriptions_cnt, Session)),
    ?assertEqual(0, emqx_session:info(subscriptions_max, Session)),
    ?assertEqual(false, emqx_session:info(upgrade_qos, Session)),
    ?assertEqual(0, emqx_session:info(inflight_cnt, Session)),
    ?assertEqual(64, emqx_session:info(inflight_max, Session)),
    ?assertEqual(1, emqx_session:info(next_pkt_id, Session)),
    ?assertEqual(0, emqx_session:info(retry_interval, Session)),
    ?assertEqual(0, emqx_mqueue:len(emqx_session:info(mqueue, Session))),
    ?assertEqual(0, emqx_session:info(awaiting_rel_cnt, Session)),
    ?assertEqual(100, emqx_session:info(awaiting_rel_max, Session)),
    ?assertEqual(300, emqx_session:info(await_rel_timeout, Session)),
    ?assert(is_integer(emqx_session:info(created_at, Session))).

%%--------------------------------------------------------------------
%% Test cases for session info/stats
%%--------------------------------------------------------------------

t_session_info(_) ->
    ?assertMatch(#{subscriptions  := #{},
                   upgrade_qos    := false,
                   retry_interval := 0,
                   await_rel_timeout := 300
                  }, emqx_session:info(session())).

t_session_stats(_) ->
    Stats = emqx_session:stats(session()),
    ?assertMatch(#{subscriptions_max := 0,
                   inflight_max      := 0,
                   mqueue_len        := 0,
                   mqueue_max        := 1000,
                   mqueue_dropped    := 0,
                   next_pkt_id       := 1,
                   awaiting_rel_cnt  := 0,
                   awaiting_rel_max  := 100
                  }, maps:from_list(Stats)).

%%--------------------------------------------------------------------
%% Test cases for sub/unsub
%%--------------------------------------------------------------------

t_subscribe(_) ->
    ok = meck:expect(emqx_broker, subscribe, fun(_, _, _) -> ok end),
    {ok, Session} = emqx_session:subscribe(
                      clientinfo(), <<"#">>, subopts(), session()),
    ?assertEqual(1, emqx_session:info(subscriptions_cnt, Session)).

t_is_subscriptions_full_false(_) ->
    Session = session(#{max_subscriptions => 0}),
    ?assertNot(emqx_session:is_subscriptions_full(Session)).

t_is_subscriptions_full_true(_) ->
    ok = meck:expect(emqx_broker, subscribe, fun(_, _, _) -> ok end),
    Session = session(#{max_subscriptions => 1}),
    ?assertNot(emqx_session:is_subscriptions_full(Session)),
    {ok, Session1} = emqx_session:subscribe(
                       clientinfo(), <<"t1">>, subopts(), Session),
    ?assert(emqx_session:is_subscriptions_full(Session1)),
    {error, ?RC_QUOTA_EXCEEDED} =
        emqx_session:subscribe(clientinfo(), <<"t2">>, subopts(), Session1).

t_unsubscribe(_) ->
    ok = meck:expect(emqx_broker, unsubscribe, fun(_) -> ok end),
    Session = session(#{subscriptions => #{<<"#">> => subopts()}}),
    {ok, Session1} = emqx_session:unsubscribe(clientinfo(), <<"#">>, Session),
    {error, ?RC_NO_SUBSCRIPTION_EXISTED} =
        emqx_session:unsubscribe(clientinfo(), <<"#">>, Session1).

t_publish_qos0(_) ->
    ok = meck:expect(emqx_broker, publish, fun(_) -> [] end),
    Msg = emqx_message:make(clientid, ?QOS_0, <<"t">>, <<"payload">>),
    {ok, [], Session} = emqx_session:publish(1, Msg, Session = session()),
    {ok, [], Session} = emqx_session:publish(undefined, Msg, Session).

t_publish_qos1(_) ->
    ok = meck:expect(emqx_broker, publish, fun(_) -> [] end),
    Msg = emqx_message:make(clientid, ?QOS_1, <<"t">>, <<"payload">>),
    {ok, [], Session} = emqx_session:publish(1, Msg, Session = session()),
    {ok, [], Session} = emqx_session:publish(2, Msg, Session).

t_publish_qos2(_) ->
    ok = meck:expect(emqx_broker, publish, fun(_) -> [] end),
    Msg = emqx_message:make(clientid, ?QOS_2, <<"t">>, <<"payload">>),
    {ok, [], Session} = emqx_session:publish(1, Msg, session()),
    ?assertEqual(1, emqx_session:info(awaiting_rel_cnt, Session)),
    {ok, Session1} = emqx_session:pubrel(1, Session),
    ?assertEqual(0, emqx_session:info(awaiting_rel_cnt, Session1)),
    {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} = emqx_session:pubrel(1, Session1).

t_publish_qos2_with_error_return(_) ->
    ok = meck:expect(emqx_broker, publish, fun(_) -> [] end),
    Session = session(#{max_awaiting_rel => 2,
                        awaiting_rel => #{1 => ts(millisecond)}
                       }),
    Msg = emqx_message:make(clientid, ?QOS_2, <<"t">>, <<"payload">>),
    {error, ?RC_PACKET_IDENTIFIER_IN_USE} = emqx_session:publish(1, Msg, Session),
    {ok, [], Session1} = emqx_session:publish(2, Msg, Session),
    ?assertEqual(2, emqx_session:info(awaiting_rel_cnt, Session1)),
    {error, ?RC_RECEIVE_MAXIMUM_EXCEEDED} = emqx_session:publish(3, Msg, Session1).

t_is_awaiting_full_false(_) ->
    Session = session(#{max_awaiting_rel => 0}),
    ?assertNot(emqx_session:is_awaiting_full(Session)).

t_is_awaiting_full_true(_) ->
    Session = session(#{max_awaiting_rel => 1,
                        awaiting_rel => #{1 => ts(millisecond)}
                       }),
    ?assert(emqx_session:is_awaiting_full(Session)).

t_puback(_) ->
    Msg = emqx_message:make(test, ?QOS_1, <<"t">>, <<>>),
    Inflight = emqx_inflight:insert(1, {Msg, ts(millisecond)}, emqx_inflight:new()),
    Session = session(#{inflight => Inflight, mqueue => mqueue()}),
    {ok, Msg, Session1} = emqx_session:puback(1, Session),
    ?assertEqual(0, emqx_session:info(inflight_cnt, Session1)).

t_puback_with_dequeue(_) ->
    Msg1 = emqx_message:make(clientid, ?QOS_1, <<"t1">>, <<"payload1">>),
    Inflight = emqx_inflight:insert(1, {Msg1, ts(millisecond)}, emqx_inflight:new()),
    Msg2 = emqx_message:make(clientid, ?QOS_1, <<"t2">>, <<"payload2">>),
    {_, Q} = emqx_mqueue:in(Msg2, mqueue(#{max_len => 10})),
    Session = session(#{inflight => Inflight, mqueue => Q}),
    {ok, Msg1, [{_, Msg3}], Session1} = emqx_session:puback(1, Session),
    ?assertEqual(1, emqx_session:info(inflight_cnt, Session1)),
    ?assertEqual(0, emqx_session:info(mqueue_len, Session1)),
    ?assertEqual(<<"t2">>, emqx_message:topic(Msg3)).

t_puback_error_packet_id_in_use(_) ->
    Inflight = emqx_inflight:insert(1, {pubrel, ts(millisecond)}, emqx_inflight:new()),
    {error, ?RC_PACKET_IDENTIFIER_IN_USE} =
        emqx_session:puback(1, session(#{inflight => Inflight})).

t_puback_error_packet_id_not_found(_) ->
    {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} = emqx_session:puback(1, session()).

t_pubrec(_) ->
    Msg = emqx_message:make(test, ?QOS_2, <<"t">>, <<>>),
    Inflight = emqx_inflight:insert(2, {Msg, ts(millisecond)}, emqx_inflight:new()),
    Session = session(#{inflight => Inflight}),
    {ok, Msg, Session1} = emqx_session:pubrec(2, Session),
    ?assertMatch([{pubrel, _}], emqx_inflight:values(emqx_session:info(inflight, Session1))).

t_pubrec_packet_id_in_use_error(_) ->
    Inflight = emqx_inflight:insert(1, {pubrel, ts(millisecond)}, emqx_inflight:new()),
    {error, ?RC_PACKET_IDENTIFIER_IN_USE} =
        emqx_session:pubrec(1, session(#{inflight => Inflight})).

t_pubrec_packet_id_not_found_error(_) ->
    {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} = emqx_session:pubrec(1, session()).

t_pubrel(_) ->
    Session = session(#{awaiting_rel => #{1 => ts(millisecond)}}),
    {ok, Session1} = emqx_session:pubrel(1, Session),
    ?assertEqual(#{}, emqx_session:info(awaiting_rel, Session1)).

t_pubrel_error_packetid_not_found(_) ->
    {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} = emqx_session:pubrel(1, session()).

t_pubcomp(_) ->
    Inflight = emqx_inflight:insert(1, {pubrel, ts(millisecond)}, emqx_inflight:new()),
    Session = session(#{inflight => Inflight}),
    {ok, Session1} = emqx_session:pubcomp(1, Session),
    ?assertEqual(0, emqx_session:info(inflight_cnt, Session1)).

t_pubcomp_error_packetid_in_use(_) ->
    Msg = emqx_message:make(test, ?QOS_2, <<"t">>, <<>>),
    Inflight = emqx_inflight:insert(1, {Msg, ts(millisecond)}, emqx_inflight:new()),
    Session = session(#{inflight => Inflight}),
    {error, ?RC_PACKET_IDENTIFIER_IN_USE} = emqx_session:pubcomp(1, Session).

t_pubcomp_error_packetid_not_found(_) ->
    {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} = emqx_session:pubcomp(1, session()).

%%--------------------------------------------------------------------
%% Test cases for deliver/retry
%%--------------------------------------------------------------------

t_dequeue(_) ->
    Q = mqueue(#{store_qos0 => true}),
    {ok, Session} = emqx_session:dequeue(session(#{mqueue => Q})),
    Msgs = [emqx_message:make(clientid, ?QOS_0, <<"t0">>, <<"payload">>),
            emqx_message:make(clientid, ?QOS_1, <<"t1">>, <<"payload">>),
            emqx_message:make(clientid, ?QOS_2, <<"t2">>, <<"payload">>)
           ],
    Session1 = lists:foldl(fun emqx_session:enqueue/2, Session, Msgs),
    {ok, [{undefined, Msg0}, {1, Msg1}, {2, Msg2}], Session2} =
        emqx_session:dequeue(Session1),
    ?assertEqual(0, emqx_session:info(mqueue_len, Session2)),
    ?assertEqual(2, emqx_session:info(inflight_cnt, Session2)),
    ?assertEqual(<<"t0">>, emqx_message:topic(Msg0)),
    ?assertEqual(<<"t1">>, emqx_message:topic(Msg1)),
    ?assertEqual(<<"t2">>, emqx_message:topic(Msg2)).

t_deliver_qos0(_) ->
    ok = meck:expect(emqx_broker, subscribe, fun(_, _, _) -> ok end),
    {ok, Session} = emqx_session:subscribe(
                      clientinfo(), <<"t0">>, subopts(), session()),
    {ok, Session1} = emqx_session:subscribe(
                       clientinfo(), <<"t1">>, subopts(), Session),
    Deliveries = [delivery(?QOS_0, T) || T <- [<<"t0">>, <<"t1">>]],
    {ok, [{undefined, Msg1}, {undefined, Msg2}], Session1} =
        emqx_session:deliver(Deliveries, Session1),
    ?assertEqual(<<"t0">>, emqx_message:topic(Msg1)),
    ?assertEqual(<<"t1">>, emqx_message:topic(Msg2)).

t_deliver_qos1(_) ->
    ok = meck:expect(emqx_broker, subscribe, fun(_, _, _) -> ok end),
    {ok, Session} = emqx_session:subscribe(
                       clientinfo(), <<"t1">>, subopts(#{qos => ?QOS_1}), session()),
    Delivers = [delivery(?QOS_1, T) || T <- [<<"t1">>, <<"t2">>]],
    {ok, [{1, Msg1}, {2, Msg2}], Session1} = emqx_session:deliver(Delivers, Session),
    ?assertEqual(2, emqx_session:info(inflight_cnt, Session1)),
    ?assertEqual(<<"t1">>, emqx_message:topic(Msg1)),
    ?assertEqual(<<"t2">>, emqx_message:topic(Msg2)),
    {ok, Msg1, Session2} = emqx_session:puback(1, Session1),
    ?assertEqual(1, emqx_session:info(inflight_cnt, Session2)),
    {ok, Msg2, Session3} = emqx_session:puback(2, Session2),
    ?assertEqual(0, emqx_session:info(inflight_cnt, Session3)).

t_deliver_qos2(_) ->
    ok = meck:expect(emqx_broker, subscribe, fun(_, _, _) -> ok end),
    Delivers = [delivery(?QOS_2, <<"t0">>), delivery(?QOS_2, <<"t1">>)],
    {ok, [{1, Msg1}, {2, Msg2}], Session} =
        emqx_session:deliver(Delivers, session()),
    ?assertEqual(2, emqx_session:info(inflight_cnt, Session)),
    ?assertEqual(<<"t0">>, emqx_message:topic(Msg1)),
    ?assertEqual(<<"t1">>, emqx_message:topic(Msg2)).

t_deliver_one_msg(_) ->
    {ok, [{1, Msg}], Session} =
        emqx_session:deliver([delivery(?QOS_1, <<"t1">>)], session()),
    ?assertEqual(1, emqx_session:info(inflight_cnt, Session)),
    ?assertEqual(<<"t1">>, emqx_message:topic(Msg)).

t_deliver_when_inflight_is_full(_) ->
    Delivers = [delivery(?QOS_1, <<"t1">>), delivery(?QOS_2, <<"t2">>)],
    Session = session(#{inflight => emqx_inflight:new(1)}),
    {ok, Publishes, Session1} = emqx_session:deliver(Delivers, Session),
    ?assertEqual(1, length(Publishes)),
    ?assertEqual(1, emqx_session:info(inflight_cnt, Session1)),
    ?assertEqual(1, emqx_session:info(mqueue_len, Session1)),
    {ok, Msg1, [{2, Msg2}], Session2} = emqx_session:puback(1, Session1),
    ?assertEqual(1, emqx_session:info(inflight_cnt, Session2)),
    ?assertEqual(0, emqx_session:info(mqueue_len, Session2)),
    ?assertEqual(<<"t1">>, emqx_message:topic(Msg1)),
    ?assertEqual(<<"t2">>, emqx_message:topic(Msg2)).

t_enqueue(_) ->
    %% store_qos0 = true
    Session = emqx_session:enqueue([delivery(?QOS_0, <<"t0">>)], session()),
    Session1 = emqx_session:enqueue([delivery(?QOS_1, <<"t1">>),
                                     delivery(?QOS_2, <<"t2">>)], Session),
    ?assertEqual(3, emqx_session:info(mqueue_len, Session1)).

t_retry(_) ->
    Delivers = [delivery(?QOS_1, <<"t1">>), delivery(?QOS_2, <<"t2">>)],
    Session = session(#{retry_interval => 100}),
    {ok, Pubs, Session1} = emqx_session:deliver(Delivers, Session),
    ok = timer:sleep(200),
    Msgs1 = [{I, emqx_message:set_flag(dup, Msg)} || {I, Msg} <- Pubs],
    {ok, Msgs1, 100, Session2} = emqx_session:retry(Session1),
    ?assertEqual(2, emqx_session:info(inflight_cnt, Session2)).

%%--------------------------------------------------------------------
%% Test cases for takeover/resume
%%--------------------------------------------------------------------

t_takeover(_) ->
    ok = meck:expect(emqx_broker, unsubscribe, fun(_) -> ok end),
    Session = session(#{subscriptions => #{<<"t">> => ?DEFAULT_SUBOPTS}}),
    ok = emqx_session:takeover(Session).

t_resume(_) ->
    ok = meck:expect(emqx_broker, subscribe, fun(_, _, _) -> ok end),
    Session = session(#{subscriptions => #{<<"t">> => ?DEFAULT_SUBOPTS}}),
    ok = emqx_session:resume(#{clientid => <<"clientid">>}, Session).

t_replay(_) ->
    Delivers = [delivery(?QOS_1, <<"t1">>), delivery(?QOS_2, <<"t2">>)],
    {ok, Pubs, Session1} = emqx_session:deliver(Delivers, session()),
    Msg = emqx_message:make(clientid, ?QOS_1, <<"t1">>, <<"payload">>),
    Session2 = emqx_session:enqueue(Msg, Session1),
    Pubs1 = [{I, emqx_message:set_flag(dup, M)} || {I, M} <- Pubs],
    {ok, ReplayPubs, Session3} = emqx_session:replay(Session2),
    ?assertEqual(Pubs1 ++ [{3, Msg}], ReplayPubs),
    ?assertEqual(3, emqx_session:info(inflight_cnt, Session3)).

t_expire_awaiting_rel(_) ->
    {ok, Session} = emqx_session:expire(awaiting_rel, session()),
    Timeout = emqx_session:info(await_rel_timeout, Session) * 1000,
    Session1 = emqx_session:set_field(awaiting_rel, #{1 => Ts = ts(millisecond)}, Session),
    {ok, Timeout, Session2} = emqx_session:expire(awaiting_rel, Session1),
    ?assertEqual(#{1 => Ts}, emqx_session:info(awaiting_rel, Session2)).

t_expire_awaiting_rel_all(_) ->
    Session = session(#{awaiting_rel => #{1 => 1, 2 => 2}}),
    {ok, Session1} = emqx_session:expire(awaiting_rel, Session),
    ?assertEqual(#{}, emqx_session:info(awaiting_rel, Session1)).

%%--------------------------------------------------------------------
%% CT for utility functions
%%--------------------------------------------------------------------

t_next_pakt_id(_) ->
    Session = session(#{next_pkt_id => 16#FFFF}),
    Session1 = emqx_session:next_pkt_id(Session),
    ?assertEqual(1, emqx_session:info(next_pkt_id, Session1)),
    Session2 = emqx_session:next_pkt_id(Session1),
    ?assertEqual(2, emqx_session:info(next_pkt_id, Session2)).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

mqueue() -> mqueue(#{}).
mqueue(Opts) ->
    emqx_mqueue:init(maps:merge(#{max_len => 0, store_qos0 => false}, Opts)).

session() -> session(#{}).
session(InitFields) when is_map(InitFields) ->
    maps:fold(fun(Field, Value, Session) ->
                      emqx_session:set_field(Field, Value, Session)
              end,
              emqx_session:init(#{zone => channel}, #{receive_maximum => 0}),
              InitFields).


clientinfo() -> clientinfo(#{}).
clientinfo(Init) ->
    maps:merge(#{clientid => <<"clientid">>,
                 username => <<"username">>
                }, Init).

subopts() -> subopts(#{}).
subopts(Init) ->
    maps:merge(?DEFAULT_SUBOPTS, Init).

delivery(QoS, Topic) ->
    {deliver, Topic, emqx_message:make(test, QoS, Topic, <<"payload">>)}.

ts(second) ->
    erlang:system_time(second);
ts(millisecond) ->
    erlang:system_time(millisecond).

