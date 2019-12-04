%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-import(emqx_session, [set_field/3]).

all() -> emqx_ct:all(?MODULE).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

init_per_testcase(_TestCase, Config) ->
    %% Meck Broker
    ok = meck:new(emqx_broker, [passthrough, no_history]),
    ok = meck:new(emqx_hooks, [passthrough, no_history]),
    ok = meck:expect(emqx_hooks, run, fun(_Hook, _Args) -> ok end),
    Config.

end_per_testcase(_TestCase, Config) ->
    ok = meck:unload(emqx_broker),
    ok = meck:unload(emqx_hooks),
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
    ?assertEqual(0, emqx_session:info(awaiting_rel_cnt, Session)),
    ?assertEqual(100, emqx_session:info(awaiting_rel_max, Session)),
    ?assertEqual(3600000, emqx_session:info(awaiting_rel_timeout, Session)),
    ?assert(is_integer(emqx_session:info(created_at, Session))).

%%--------------------------------------------------------------------
%% Test cases for session info/stats
%%--------------------------------------------------------------------

t_session_info(_) ->
    Info = emqx_session:info(session()),
    ?assertMatch(#{subscriptions := #{},
                   subscriptions_max := 0,
                   upgrade_qos := false,
                   inflight_max := 0,
                   retry_interval := 0,
                   mqueue_len := 0,
                   mqueue_max := 1000,
                   mqueue_dropped := 0,
                   next_pkt_id := 1,
                   awaiting_rel := #{},
                   awaiting_rel_max := 100,
                   awaiting_rel_timeout := 3600000
                  }, Info).

t_session_attrs(_) ->
    Attrs = emqx_session:attrs(session()),
    io:format("~p~n", [Attrs]).

t_session_stats(_) ->
    Stats = emqx_session:stats(session()),
    io:format("~p~n", [Stats]).

%%--------------------------------------------------------------------
%% Test cases for pub/sub
%%--------------------------------------------------------------------

t_subscribe(_) ->
    ok = meck:expect(emqx_broker, subscribe, fun(_, _, _) -> ok end),
    ok = meck:expect(emqx_broker, set_subopts, fun(_, _) -> ok end),
    {ok, Session} = emqx_session:subscribe(
                      clientinfo(), <<"#">>, subopts(), session()),
    ?assertEqual(1, emqx_session:info(subscriptions_cnt, Session)).

t_is_subscriptions_full_false(_) ->
    Session = session(#{max_subscriptions => 0}),
    ?assertNot(emqx_session:is_subscriptions_full(Session)).

t_is_subscriptions_full_true(_) ->
    Session = session(#{max_subscriptions => 1}),
    ?assertNot(emqx_session:is_subscriptions_full(Session)),
    Subs = #{<<"t1">> => subopts(), <<"t2">> => subopts()},
    NSession = set_field(subscriptions, Subs, Session),
    ?assert(emqx_session:is_subscriptions_full(NSession)).

t_unsubscribe(_) ->
    ok = meck:expect(emqx_broker, unsubscribe, fun(_) -> ok end),
    Session = session(#{subscriptions => #{<<"#">> => subopts()}}),
    {ok, NSession} = emqx_session:unsubscribe(clientinfo(), <<"#">>, Session),
    Error = emqx_session:unsubscribe(clientinfo(), <<"#">>, NSession),
    ?assertEqual({error, ?RC_NO_SUBSCRIPTION_EXISTED}, Error).

t_publish_qos2(_) ->
    ok = meck:expect(emqx_broker, publish, fun(_) -> [] end),
    Msg = emqx_message:make(test, ?QOS_2, <<"t">>, <<"payload">>),
    {ok, [], Session} = emqx_session:publish(1, Msg, session()),
    ?assertEqual(1, emqx_session:info(awaiting_rel_cnt, Session)).

t_publish_qos1(_) ->
    ok = meck:expect(emqx_broker, publish, fun(_) -> [] end),
    Msg = emqx_message:make(test, ?QOS_1, <<"t">>, <<"payload">>),
    {ok, [], _Session} = emqx_session:publish(1, Msg, session()).

t_publish_qos0(_) ->
    ok = meck:expect(emqx_broker, publish, fun(_) -> [] end),
    Msg = emqx_message:make(test, ?QOS_1, <<"t">>, <<"payload">>),
    {ok, [], _Session} = emqx_session:publish(0, Msg, session()).

t_is_awaiting_full_false(_) ->
    ?assertNot(emqx_session:is_awaiting_full(session(#{max_awaiting_rel => 0}))).

t_is_awaiting_full_true(_) ->
    Session = session(#{max_awaiting_rel => 1,
                        awaiting_rel => #{1 => 1}
                       }),
    ?assert(emqx_session:is_awaiting_full(Session)).

t_puback(_) ->
    Msg = emqx_message:make(test, ?QOS_1, <<"t">>, <<>>),
    Inflight = emqx_inflight:insert(1, {Msg, os:timestamp()}, emqx_inflight:new()),
    Session = set_field(inflight, Inflight, session()),
    {ok, Msg, NSession} = emqx_session:puback(1, Session),
    ?assertEqual(0, emqx_session:info(inflight_cnt, NSession)).

t_puback_error_packet_id_in_use(_) ->
    Inflight = emqx_inflight:insert(1, {pubrel, os:timestamp()}, emqx_inflight:new()),
    Session = set_field(inflight, Inflight, session()),
    {error, ?RC_PACKET_IDENTIFIER_IN_USE} = emqx_session:puback(1, Session).

t_puback_error_packet_id_not_found(_) ->
    {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} = emqx_session:puback(1, session()).

t_pubrec(_) ->
    Msg = emqx_message:make(test, ?QOS_2, <<"t">>, <<>>),
    Inflight = emqx_inflight:insert(2, {Msg, os:timestamp()}, emqx_inflight:new()),
    Session = set_field(inflight, Inflight, session()),
    {ok, Msg, NSession} = emqx_session:pubrec(2, Session),
    ?assertMatch([{pubrel, _}], emqx_inflight:values(emqx_session:info(inflight, NSession))).

t_pubrec_packet_id_in_use_error(_) ->
    Inflight = emqx_inflight:insert(1, {pubrel, ts()}, emqx_inflight:new()),
    Session = set_field(inflight, Inflight, session()),
    {error, ?RC_PACKET_IDENTIFIER_IN_USE} = emqx_session:puback(1, Session).

t_pubrec_packet_id_not_found_error(_) ->
    {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} = emqx_session:pubrec(1, session()).

t_pubrel(_) ->
    Session = set_field(awaiting_rel, #{1 => os:timestamp()}, session()),
    {ok, NSession} = emqx_session:pubrel(1, Session),
    ?assertEqual(#{}, emqx_session:info(awaiting_rel, NSession)).

t_pubrel_id_not_found(_) ->
    {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} = emqx_session:pubrel(1, session()).

t_pubcomp(_) ->
    Inflight = emqx_inflight:insert(2, {pubrel, os:timestamp()}, emqx_inflight:new()),
    Session = emqx_session:set_field(inflight, Inflight, session()),
    {ok, NSession} = emqx_session:pubcomp(2, Session),
    ?assertEqual(0, emqx_session:info(inflight_cnt, NSession)).

t_pubcomp_id_not_found(_) ->
    {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} = emqx_session:pubcomp(2, session()).

%%--------------------------------------------------------------------
%% Test cases for deliver/retry
%%--------------------------------------------------------------------

t_dequeue(_) ->
    {ok, _Session} = emqx_session:dequeue(session()).

t_deliver(_) ->
    Delivers = [delivery(?QOS_1, <<"t1">>), delivery(?QOS_2, <<"t2">>)],
    {ok, Publishes, _Session} = emqx_session:deliver(Delivers, session()),
    ?assertEqual(2, length(Publishes)).

t_enqueue(_) ->
    Delivers = [delivery(?QOS_1, <<"t1">>), delivery(?QOS_2, <<"t2">>)],
    Session = emqx_session:enqueue(Delivers, session()),
    ?assertEqual(2, emqx_session:info(mqueue_len, Session)).

t_retry(_) ->
    {ok, _Session} = emqx_session:retry(session()).

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
    ok = emqx_session:resume(<<"clientid">>, Session).

t_redeliver(_) ->
    {ok, [], _Session} = emqx_session:redeliver(session()).

t_expire(_) ->
    {ok, _Session} = emqx_session:expire(awaiting_rel, session()).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

session() -> session(#{}).
session(InitFields) when is_map(InitFields) ->
    maps:fold(fun(Field, Value, Session) ->
                      emqx_session:set_field(Field, Value, Session)
              end,
              emqx_session:init(#{zone => zone}, #{receive_maximum => 0}),
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

ts() -> erlang:system_time(second).

