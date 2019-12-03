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

-module(emqx_channel_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_CONNINFO,
        #{peername => {{127,0,0,1}, 3456},
          sockname => {{127,0,0,1}, 1883},
          conn_mod => emqx_connection,
          proto_name => <<"MQTT">>,
          proto_ver => ?MQTT_PROTO_V5,
          clean_start => true,
          keepalive => 30,
          clientid => <<"clientid">>,
          username => <<"username">>,
          conn_props => #{},
          receive_maximum => 100,
          expiry_interval => 0
         }).

all() -> emqx_ct:all(?MODULE).

%%--------------------------------------------------------------------
%% CT Callbacks
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% CM Meck
    ok = meck:new(emqx_cm, [passthrough, no_history]),
    %% Access Control Meck
    ok = meck:new(emqx_access_control, [passthrough, no_history]),
    ok = meck:expect(emqx_access_control, authenticate,
                     fun(_) -> {ok, #{auth_result => success}} end),
    ok = meck:expect(emqx_access_control, check_acl, fun(_, _, _) -> allow end),
    %% Broker Meck
    ok = meck:new(emqx_broker, [passthrough, no_history]),
    %% Hooks Meck
    ok = meck:new(emqx_hooks, [passthrough, no_history]),
    ok = meck:expect(emqx_hooks, run, fun(_Hook, _Args) -> ok end),
    ok = meck:expect(emqx_hooks, run_fold, fun(_Hook, _Args, Acc) -> Acc end),
    %% Session Meck
    ok = meck:new(emqx_session, [passthrough, no_history]),
    %% Metrics
    ok = meck:new(emqx_metrics, [passthrough, no_history]),
    ok = meck:expect(emqx_metrics, inc, fun(_) -> ok end),
    ok = meck:expect(emqx_metrics, inc, fun(_, _) -> ok end),
    Config.

end_per_testcase(_TestCase, Config) ->
    ok = meck:unload(emqx_access_control),
    ok = meck:unload(emqx_metrics),
    ok = meck:unload(emqx_session),
    ok = meck:unload(emqx_broker),
    ok = meck:unload(emqx_hooks),
    ok = meck:unload(emqx_cm),
    Config.

%%--------------------------------------------------------------------
%% Test cases for channel info/stats/caps
%%--------------------------------------------------------------------

t_chan_info(_) ->
    #{conn_state := connected,
      clientinfo := ClientInfo
     } = emqx_channel:info(channel()),
    ?assertEqual(clientinfo(), ClientInfo).

t_chan_attrs(_) ->
    #{conn_state := connected} = emqx_channel:attrs(channel()).

t_chan_caps(_) ->
    _Caps = emqx_channel:caps(channel()).

%%--------------------------------------------------------------------
%% Test cases for channel init
%%--------------------------------------------------------------------

%% TODO:
t_chan_init(_) ->
    _Channel = channel().

%%--------------------------------------------------------------------
%% Test cases for channel handle_in
%%--------------------------------------------------------------------

t_handle_in_connect_packet_sucess(_) ->
    ok = meck:expect(emqx_cm, open_session,
                     fun(true, _ClientInfo, _ConnInfo) ->
                             {ok, #{session => session(), present => false}}
                     end),
    {ok, [{connack, ?CONNACK_PACKET(?RC_SUCCESS, 0)}], Channel}
        = emqx_channel:handle_in(?CONNECT_PACKET(connpkt()), channel(#{conn_state => idle})),
    ClientInfo = emqx_channel:info(clientinfo, Channel),
    ?assertMatch(#{clientid := <<"clientid">>,
                   username := <<"username">>
                  }, ClientInfo),
    ?assertEqual(connected, emqx_channel:info(conn_state, Channel)).

t_handle_in_unexpected_connect_packet(_) ->
    Channel = emqx_channel:set_field(conn_state, connected, channel()),
    {shutdown, protocol_error, ?DISCONNECT_PACKET(?RC_PROTOCOL_ERROR), Channel}
        = emqx_channel:handle_in(?CONNECT_PACKET(connpkt()), Channel).

t_handle_in_qos0_publish(_) ->
    ok = meck:expect(emqx_broker, publish, fun(_) -> ok end),
    Channel = channel(#{conn_state => connected}),
    Publish = ?PUBLISH_PACKET(?QOS_0, <<"topic">>, undefined, <<"payload">>),
    {ok, _NChannel} = emqx_channel:handle_in(Publish, Channel).
    % ?assertEqual(#{publish_in => 1}, emqx_channel:info(pub_stats, NChannel)).

t_handle_in_qos1_publish(_) ->
    ok = meck:expect(emqx_broker, publish, fun(_) -> ok end),
    Channel = channel(#{conn_state => connected}),
    Publish = ?PUBLISH_PACKET(?QOS_1, <<"topic">>, 1, <<"payload">>),
    {ok, ?PUBACK_PACKET(1, RC), _NChannel} = emqx_channel:handle_in(Publish, Channel),
    ?assert((RC == ?RC_SUCCESS) orelse (RC == ?RC_NO_MATCHING_SUBSCRIBERS)).
    % ?assertEqual(#{publish_in => 1, puback_out => 1}, emqx_channel:info(pub_stats, NChannel)).

t_handle_in_qos2_publish(_) ->
    ok = meck:expect(emqx_session, publish, fun(_, _Msg, Session) -> {ok, [], Session} end),
    ok = meck:expect(emqx_session, info, fun(awaiting_rel_timeout, _Session) -> 300000 end),
    Channel = channel(#{conn_state => connected}),
    Publish = ?PUBLISH_PACKET(?QOS_2, <<"topic">>, 1, <<"payload">>),
    {ok, ?PUBREC_PACKET(1, RC), _NChannel} = emqx_channel:handle_in(Publish, Channel),
    ?assert((RC == ?RC_SUCCESS) orelse (RC == ?RC_NO_MATCHING_SUBSCRIBERS)).
    % ?assertEqual(#{publish_in => 1, pubrec_out => 1}, emqx_channel:info(pub_stats, NChannel)).

t_handle_in_puback_ok(_) ->
    Msg = emqx_message:make(<<"t">>, <<"payload">>),
    ok = meck:expect(emqx_session, puback,
                     fun(_PacketId, Session) -> {ok, Msg, Session} end),
    Channel = channel(#{conn_state => connected}),
    {ok, _NChannel} = emqx_channel:handle_in(?PUBACK_PACKET(1, ?RC_SUCCESS), Channel).
    % ?assertEqual(#{puback_in => 1}, emqx_channel:info(pub_stats, NChannel)).

t_handle_in_puback_id_in_use(_) ->
    ok = meck:expect(emqx_session, puback,
                     fun(_, _Session) ->
                             {error, ?RC_PACKET_IDENTIFIER_IN_USE}
                     end),
    {ok, _Channel} = emqx_channel:handle_in(?PUBACK_PACKET(1, ?RC_SUCCESS), channel()).
    % ?assertEqual(#{puback_in => 1}, emqx_channel:info(pub_stats, Channel)).

t_handle_in_puback_id_not_found(_) ->
    ok = meck:expect(emqx_session, puback,
                     fun(_, _Session) ->
                             {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
                     end),
    {ok, _Channel} = emqx_channel:handle_in(?PUBACK_PACKET(1, ?RC_SUCCESS), channel()).
    % ?assertEqual(#{puback_in => 1}, emqx_channel:info(pub_stats, Channel)).

t_handle_in_pubrec_ok(_) ->
    Msg = emqx_message:make(test,?QOS_2, <<"t">>, <<"payload">>),
    ok = meck:expect(emqx_session, pubrec, fun(_, Session) -> {ok, Msg, Session} end),
    Channel = channel(#{conn_state => connected}),
    {ok, ?PUBREL_PACKET(1, ?RC_SUCCESS), _Channel1}
        = emqx_channel:handle_in(?PUBREC_PACKET(1, ?RC_SUCCESS), Channel).
    % ?assertEqual(#{pubrec_in => 1, pubrel_out => 1},
    %              emqx_channel:info(pub_stats, Channel1)).

t_handle_in_pubrec_id_in_use(_) ->
    ok = meck:expect(emqx_session, pubrec,
                     fun(_, _Session) ->
                             {error, ?RC_PACKET_IDENTIFIER_IN_USE}
                     end),
    {ok, ?PUBREL_PACKET(1, ?RC_PACKET_IDENTIFIER_IN_USE), _Channel}
        = emqx_channel:handle_in(?PUBREC_PACKET(1, ?RC_SUCCESS), channel()).
    % ?assertEqual(#{pubrec_in => 1, pubrel_out => 1},
    %              emqx_channel:info(pub_stats, Channel)).

t_handle_in_pubrec_id_not_found(_) ->
    ok = meck:expect(emqx_session, pubrec,
                     fun(_, _Session) ->
                             {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
                     end),
    {ok, ?PUBREL_PACKET(1, ?RC_PACKET_IDENTIFIER_NOT_FOUND), _Channel}
        = emqx_channel:handle_in(?PUBREC_PACKET(1, ?RC_SUCCESS), channel()).
    % ?assertEqual(#{pubrec_in => 1, pubrel_out => 1},
    %              emqx_channel:info(pub_stats, Channel)).

t_handle_in_pubrel_ok(_) ->
    ok = meck:expect(emqx_session, pubrel, fun(_, Session) -> {ok, Session} end),
    Channel = channel(#{conn_state => connected}),
    {ok, ?PUBCOMP_PACKET(1, ?RC_SUCCESS), _Channel1}
        = emqx_channel:handle_in(?PUBREL_PACKET(1, ?RC_SUCCESS), Channel).
    % ?assertEqual(#{pubrel_in => 1, pubcomp_out => 1},
    %              emqx_channel:info(pub_stats, Channel1)).

t_handle_in_pubrel_not_found_error(_) ->
    ok = meck:expect(emqx_session, pubrel,
                     fun(_PacketId, _Session) ->
                             {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
                     end),
    {ok, ?PUBCOMP_PACKET(1, ?RC_PACKET_IDENTIFIER_NOT_FOUND), _Channel}
        = emqx_channel:handle_in(?PUBREL_PACKET(1, ?RC_SUCCESS), channel()).

t_handle_in_pubcomp_ok(_) ->
    ok = meck:expect(emqx_session, pubcomp, fun(_, Session) -> {ok, Session} end),
    {ok, _Channel} = emqx_channel:handle_in(?PUBCOMP_PACKET(1, ?RC_SUCCESS), channel()).
    % ?assertEqual(#{pubcomp_in => 1}, emqx_channel:info(pub_stats, Channel)).

t_handle_in_pubcomp_not_found_error(_) ->
    ok = meck:expect(emqx_session, pubcomp,
                     fun(_PacketId, _Session) ->
                             {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
                     end),
    Channel = channel(#{conn_state => connected}),
    {ok, _Channel1} = emqx_channel:handle_in(?PUBCOMP_PACKET(1, ?RC_SUCCESS), Channel).
    % ?assertEqual(#{pubcomp_in => 1}, emqx_channel:info(pub_stats, Channel1)).

t_handle_in_subscribe(_) ->
    ok = meck:expect(emqx_session, subscribe,
                     fun(_, _, _, Session) ->
                             {ok, Session}
                     end),
    Channel = channel(#{conn_state => connected}),
    TopicFilters = [{<<"+">>, ?DEFAULT_SUBOPTS}],
    Subscribe = ?SUBSCRIBE_PACKET(1, #{}, TopicFilters),
    {ok, ?SUBACK_PACKET(1, [?QOS_0]), _} = emqx_channel:handle_in(Subscribe, Channel).

t_handle_in_unsubscribe(_) ->
    ok = meck:expect(emqx_session, unsubscribe,
                     fun(_, _, Session) ->
                             {ok, Session}
                     end),
    Channel = channel(#{conn_state => connected}),
    UnsubPkt = ?UNSUBSCRIBE_PACKET(1, #{}, [<<"+">>]),
    {ok, ?UNSUBACK_PACKET(1), _} = emqx_channel:handle_in(UnsubPkt, Channel).

t_handle_in_pingreq(_) ->
    {ok, ?PACKET(?PINGRESP), _Channel}
        = emqx_channel:handle_in(?PACKET(?PINGREQ), channel()).

t_handle_in_disconnect(_) ->
    Channel = channel(#{conn_state => connected}),
    {shutdown, normal, Channel1} = emqx_channel:handle_in(?DISCONNECT_PACKET(?RC_SUCCESS), Channel),
    ?assertEqual(undefined, emqx_channel:info(will_msg, Channel1)).

t_handle_in_auth(_) ->
    Channel = channel(#{conn_state => connected}),
    Packet = ?DISCONNECT_PACKET(?RC_IMPLEMENTATION_SPECIFIC_ERROR),
    {shutdown, implementation_specific_error, Packet, Channel}
        = emqx_channel:handle_in(?AUTH_PACKET(), Channel).

t_handle_in_frame_error(_) ->
    IdleChannel = channel(#{conn_state => idle}),
    {shutdown, frame_too_large, _}
        = emqx_channel:handle_in({frame_error, frame_too_large}, IdleChannel),
    ConnectingChan =  channel(#{conn_state => connecting}),
    {shutdown, frame_too_large, ?CONNACK_PACKET(?RC_MALFORMED_PACKET), _}
        = emqx_channel:handle_in({frame_error, frame_too_large}, ConnectingChan),
    ConnectedChan = channel(#{conn_state => connected}),
    {shutdown, malformed_Packet, ?DISCONNECT_PACKET(?RC_MALFORMED_PACKET), _}
        = emqx_channel:handle_in({frame_error, frame_too_large}, ConnectedChan),
    DisconnectedChan = channel(#{conn_state => disconnected}),
    {ok, DisconnectedChan}
        = emqx_channel:handle_in({frame_error, frame_too_large}, DisconnectedChan).

%% TODO:
t_handle_in_expected_packet(_) ->
    {shutdown, protocol_error, ?DISCONNECT_PACKET(?RC_PROTOCOL_ERROR), _Chan}
        = emqx_channel:handle_in(packet, channel()).

t_process_connect(_) ->
    ok = meck:expect(emqx_cm, open_session,
                     fun(true, _ClientInfo, _ConnInfo) ->
                             {ok, #{session => session(), present => false}}
                     end),
    {ok, [{connack, ?CONNACK_PACKET(?RC_SUCCESS)}], _Channel}
        = emqx_channel:process_connect(connpkt(), channel(#{conn_state => idle})).

t_handle_publish_qos0(_) ->
    ok = meck:expect(emqx_broker, publish, fun(_) -> [] end),
    Publish = ?PUBLISH_PACKET(?QOS_0, <<"t">>, 1, <<"payload">>),
    {ok, _Channel} = emqx_channel:handle_publish(Publish, channel()).

t_process_publish_qos1(_) ->
    ok = meck:expect(emqx_broker, publish, fun(_) -> [] end),
    Msg = emqx_message:make(test, ?QOS_1, <<"t">>, <<"payload">>),
    {ok, ?PUBACK_PACKET(1, ?RC_NO_MATCHING_SUBSCRIBERS), _Channel}
        = emqx_channel:process_publish(1, Msg, channel()).

t_process_subscribe(_) ->
    ok = meck:expect(emqx_session, subscribe, fun(_, _, _, Session) -> {ok, Session} end),
    TopicFilters = [{<<"+">>, ?DEFAULT_SUBOPTS}],
    {[?RC_SUCCESS], _Channel} = emqx_channel:process_subscribe(TopicFilters, channel()).

t_process_unsubscribe(_) ->
    ok = meck:expect(emqx_session, unsubscribe, fun(_, _, Session) -> {ok, Session} end),
    TopicFilters = [{<<"+">>, ?DEFAULT_SUBOPTS}],
    {[?RC_SUCCESS], _Channel} = emqx_channel:process_unsubscribe(TopicFilters, channel()).

%%--------------------------------------------------------------------
%% Test cases for handle_out
%%--------------------------------------------------------------------

t_handle_out_delivers(_) ->
    WithPacketId = fun(Msgs) ->
                           lists:zip(lists:seq(1, length(Msgs)), Msgs)
                   end,
    ok = meck:expect(emqx_session, deliver,
                     fun(Delivers, Session) ->
                             Msgs = [Msg || {deliver, _, Msg} <- Delivers],
                             Publishes = [{publish, PacketId, Msg}
                                          || {PacketId, Msg} <- WithPacketId(Msgs)],
                             {ok, Publishes, Session}
                     end),
    ok = meck:expect(emqx_session, info, fun(retry_interval, _Session) -> 20000 end),
    Msg0 = emqx_message:make(test, ?QOS_1, <<"t1">>, <<"qos1">>),
    Msg1 = emqx_message:make(test, ?QOS_2, <<"t2">>, <<"qos2">>),
    Delivers = [{deliver, <<"+">>, Msg0}, {deliver, <<"+">>, Msg1}],
    {ok, {outgoing, Packets}, _Ch} = emqx_channel:handle_out(Delivers, channel()),
    ?assertEqual([?QOS_1, ?QOS_2], [emqx_packet:qos(Pkt)|| Pkt <- Packets]).

t_handle_out_publishes(_) ->
    Channel = channel(#{conn_state => connected}),
    Pub0 = {publish, undefined, emqx_message:make(<<"t">>, <<"qos0">>)},
    Pub1 = {publish, 1, emqx_message:make(<<"c">>, ?QOS_1, <<"t">>, <<"qos1">>)},
    {ok, {outgoing, Packets}, _NChannel}
        = emqx_channel:handle_out({publish, [Pub0, Pub1]}, Channel),
    ?assertEqual(2, length(Packets)).
    % ?assertEqual(#{publish_out => 2}, emqx_channel:info(pub_stats, NChannel)).

t_handle_out_publish(_) ->
    Msg = emqx_message:make(<<"clientid">>, ?QOS_1, <<"t">>, <<"payload">>),
    {ok, ?PUBLISH_PACKET(?QOS_1, <<"t">>, 1, <<"payload">>), _Chan}
        = emqx_channel:handle_out({publish, 1, Msg}, channel()).

t_handle_out_publish_nl(_) ->
    ClientInfo = clientinfo(#{clientid => <<"clientid">>}),
    Channel = channel(#{clientinfo => ClientInfo}),
    Msg = emqx_message:make(<<"clientid">>, ?QOS_1, <<"t1">>, <<"qos1">>),
    Publish = {publish, 1, emqx_message:set_flag(nl, Msg)},
    {ok, Channel} = emqx_channel:handle_out(Publish, Channel).

t_handle_out_connack_sucess(_) ->
    {ok, [{connack, ?CONNACK_PACKET(?RC_SUCCESS, _SP, _)}], _Chan}
        = emqx_channel:handle_out(connack, {?RC_SUCCESS, 0, connpkt()}, channel()).

t_handle_out_connack_failure(_) ->
    {shutdown, not_authorized, ?CONNACK_PACKET(?RC_NOT_AUTHORIZED), _Chan}
        = emqx_channel:handle_out(connack, {?RC_NOT_AUTHORIZED, connpkt()}, channel()).

t_handle_out_puback(_) ->
    Channel = channel(#{conn_state => connected}),
    {ok, ?PUBACK_PACKET(1, ?RC_SUCCESS), _NChannel}
        = emqx_channel:handle_out(puback, {1, ?RC_SUCCESS}, Channel).
    % ?assertEqual(#{puback_out => 1}, emqx_channel:info(pub_stats, NChannel)).

t_handle_out_pubrec(_) ->
    Channel = channel(#{conn_state => connected}),
    {ok, ?PUBREC_PACKET(1, ?RC_SUCCESS), _NChannel}
        = emqx_channel:handle_out(pubrec, {1, ?RC_SUCCESS}, Channel).
    % ?assertEqual(#{pubrec_out => 1}, emqx_channel:info(pub_stats, NChannel)).

t_handle_out_pubrel(_) ->
    Channel = channel(#{conn_state => connected}),
    {ok, ?PUBREL_PACKET(1), Channel1}
        = emqx_channel:handle_out(pubrel, {1, ?RC_SUCCESS}, Channel),
    {ok, ?PUBREL_PACKET(2, ?RC_SUCCESS), _Channel2}
        = emqx_channel:handle_out(pubrel, {2, ?RC_SUCCESS}, Channel1).
    % ?assertEqual(#{pubrel_out => 2}, emqx_channel:info(pub_stats, Channel2)).

t_handle_out_pubcomp(_) ->
    {ok, ?PUBCOMP_PACKET(1, ?RC_SUCCESS), _Channel}
        = emqx_channel:handle_out(pubcomp, {1, ?RC_SUCCESS}, channel()).
    % ?assertEqual(#{pubcomp_out => 1}, emqx_channel:info(pub_stats, Channel)).

t_handle_out_suback(_) ->
    {ok, ?SUBACK_PACKET(1, [?QOS_2]), _Channel}
        = emqx_channel:handle_out(suback, {1, [?QOS_2]}, channel()).

t_handle_out_unsuback(_) ->
    {ok, ?UNSUBACK_PACKET(1, [?RC_SUCCESS]), _Channel}
        = emqx_channel:handle_out(unsuback, {1, [?RC_SUCCESS]}, channel()).

t_handle_out_disconnect(_) ->
    {shutdown, normal, ?DISCONNECT_PACKET(?RC_SUCCESS), _Chan}
        = emqx_channel:handle_out(disconnect, ?RC_SUCCESS, channel()).

t_handle_out_unexpected(_) ->
    {ok, _Channel} = emqx_channel:handle_out(unexpected, <<"data">>, channel()).

%%--------------------------------------------------------------------
%% Test cases for handle_call
%%--------------------------------------------------------------------

t_handle_call_kick(_) ->
    {shutdown, kicked, ok, _Chan} = emqx_channel:handle_call(kick, channel()).

t_handle_call_discard(_) ->
    Packet = ?DISCONNECT_PACKET(?RC_SESSION_TAKEN_OVER),
    {shutdown, discarded, ok, Packet, _Channel}
        = emqx_channel:handle_call(discard, channel()).

t_handle_call_takeover_begin(_) ->
    {reply, undefined, _Channel}
        = emqx_channel:handle_call({takeover, 'begin'}, channel()).

t_handle_call_takeover_end(_) ->
    ok = meck:expect(emqx_session, takeover, fun(_) -> ok end),
    {shutdown, takeovered, [], _Channel}
        = emqx_channel:handle_call({takeover, 'end'}, channel()).

t_handle_call_unexpected(_) ->
    {reply, ignored, _Channel} = emqx_channel:handle_call(unexpected_req, channel()).

%%--------------------------------------------------------------------
%% Test cases for handle_info
%%--------------------------------------------------------------------

t_handle_info_subscribe(_) ->
    ok = meck:expect(emqx_session, subscribe, fun(_, _, _, Session) -> {ok, Session} end),
    {ok, _Chan} = emqx_channel:handle_info({subscribe, topic_filters()}, channel()).

t_handle_info_unsubscribe(_) ->
    ok = meck:expect(emqx_session, unsubscribe, fun(_, _, Session) -> {ok, Session} end),
    {ok, _Chan} = emqx_channel:handle_info({unsubscribe, topic_filters()}, channel()).

t_handle_info_sock_closed(_) ->
    {ok, _Chan} = emqx_channel:handle_out({sock_closed, reason},
                                          channel(#{conn_state => disconnected})).

%%--------------------------------------------------------------------
%% Test cases for handle_timeout
%%--------------------------------------------------------------------

t_handle_timeout_emit_stats(_) ->
    ok = meck:expect(emqx_cm, set_chan_stats, fun(_, _) -> ok end),
    TRef = make_ref(),
    Channel = emqx_channel:set_field(timers, #{stats_timer => TRef}, channel()),
    {ok, _Chan} = emqx_channel:handle_timeout(TRef, {emit_stats, []}, Channel).

t_handle_timeout_keepalive(_) ->
    TRef = make_ref(),
    _Channel = emqx_channel:set_field(timers, #{alive_timer => TRef}, channel()),
    {ok, _Chan} = emqx_channel:handle_timeout(make_ref(), {keepalive, 10}, channel()).

t_handle_timeout_retry_delivery(_) ->
    ok = meck:expect(emqx_session, retry, fun(Session) -> {ok, Session} end),
    TRef = make_ref(),
    _Channel = emqx_channel:set_field(timers, #{retry_timer => TRef}, channel()),
    {ok, _Chan} = emqx_channel:handle_timeout(TRef, retry_delivery, channel()).

t_handle_timeout_expire_awaiting_rel(_) ->
    ok = meck:expect(emqx_session, expire, fun(_, Session) -> {ok, Session} end),
    TRef = make_ref(),
    Channel = emqx_channel:set_field(timers, #{await_timer => TRef}, channel()),
    {ok, _Chan} = emqx_channel:handle_timeout(TRef, expire_awaiting_rel, Channel).

t_handle_timeout_expire_session(_) ->
    TRef = make_ref(),
    Channel = emqx_channel:set_field(timers, #{expire_timer => TRef}, channel()),
    {shutdown, expired, _Chan} = emqx_channel:handle_timeout(TRef, expire_session, Channel).

t_handle_timeout_will_message(_) ->
    {ok, _Chan} = emqx_channel:handle_timeout(make_ref(), will_message, channel()).

%%--------------------------------------------------------------------
%% Test cases for internal functions
%%--------------------------------------------------------------------

t_enrich_conninfo(_) ->
    {ok, _Chan} = emqx_channel:enrich_conninfo(connpkt(), channel()).

t_enrich_client(_) ->
    {ok, _ConnPkt, _Chan} = emqx_channel:enrich_client(connpkt(), channel()).

t_check_banned(_) ->
    ok = emqx_channel:check_banned(connpkt(), channel()).

t_auth_connect(_) ->
    {ok, _Chan} = emqx_channel:auth_connect(connpkt(), channel()).

t_process_alias(_) ->
    Publish = #mqtt_packet_publish{topic_name = <<>>, properties = #{'Topic-Alias' => 1}},
    Channel = emqx_channel:set_field(topic_aliases, #{1 => <<"t">>}, channel()),
    {ok, #mqtt_packet{variable = #mqtt_packet_publish{topic_name = <<"t">>}}, _Chan}
        = emqx_channel:process_alias(#mqtt_packet{variable = Publish}, Channel).

t_check_pub_acl(_) ->
    ok = meck:new(emqx_zone, [passthrough, no_history]),
    ok = meck:expect(emqx_zone, enable_acl, fun(_) -> true end),
    Publish = ?PUBLISH_PACKET(?QOS_0, <<"t">>, 1, <<"payload">>),
    ok = emqx_channel:check_pub_acl(Publish, channel()),
    ok = meck:unload(emqx_zone).

t_check_pub_alias(_) ->
    Publish = #mqtt_packet_publish{topic_name = <<>>, properties = #{'Topic-Alias' => 1}},
    Channel = emqx_channel:set_field(alias_maximum, #{inbound => 10}, channel()),
    ok = emqx_channel:check_pub_alias(#mqtt_packet{variable = Publish}, Channel).

t_check_subscribe(_) ->
    ok = meck:new(emqx_zone, [passthrough, no_history]),
    ok = meck:expect(emqx_zone, enable_acl, fun(_) -> true end),
    ok = emqx_channel:check_subscribe(<<"t">>, ?DEFAULT_SUBOPTS, channel()),
    ok = meck:unload(emqx_zone).

t_enrich_caps(_) ->
    ok = meck:new(emqx_mqtt_caps, [passthrough, no_history]),
    ok = meck:expect(emqx_mqtt_caps, get_caps,
                     fun(_Zone) ->
                        #{max_packet_size => 1024,
                          max_qos_allowed => ?QOS_2,
                          retain_available => true,
                          max_topic_alias => 10,
                          shared_subscription => true,
                          wildcard_subscription => true
                         }
                     end),
    AckProps = emqx_channel:enrich_caps(#{}, channel()),
    ?assertMatch(#{'Retain-Available' := 1,
                   'Maximum-Packet-Size' := 1024,
                   'Topic-Alias-Maximum' := 10,
                   'Wildcard-Subscription-Available' := 1,
                   'Subscription-Identifier-Available' := 1,
                   'Shared-Subscription-Available' := 1,
                   'Maximum-QoS' := ?QOS_2
                  }, AckProps),
    ok = meck:unload(emqx_mqtt_caps).

%%--------------------------------------------------------------------
%% Test cases for terminate
%%--------------------------------------------------------------------

t_terminate(_) ->
    ok = emqx_channel:terminate(normal, channel()),
    ok = emqx_channel:terminate(sock_error, channel(#{conn_state => connected})),
    ok = emqx_channel:terminate({shutdown, kicked}, channel(#{conn_state => connected})).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

channel() -> channel(#{}).
channel(InitFields) ->
    maps:fold(fun(Field, Value, Channel) ->
                      emqx_channel:set_field(Field, Value, Channel)
              end, default_channel(), InitFields).

default_channel() ->
    Channel = emqx_channel:init(?DEFAULT_CONNINFO, [{zone, zone}]),
    Channel1 = emqx_channel:set_field(conn_state, connected, Channel),
    emqx_channel:set_field(clientinfo, clientinfo(), Channel1).

clientinfo() -> clientinfo(#{}).
clientinfo(InitProps) ->
    maps:merge(#{zone       => zone,
                 protocol   => mqtt,
                 peerhost   => {127,0,0,1},
                 clientid   => <<"clientid">>,
                 username   => <<"username">>,
                 is_superuser => false,
                 peercert   => undefined,
                 mountpoint => undefined
                }, InitProps).

topic_filters() ->
    [{<<"+">>, ?DEFAULT_SUBOPTS}, {<<"#">>, ?DEFAULT_SUBOPTS}].

connpkt() ->
    #mqtt_packet_connect{
       proto_name  = <<"MQTT">>,
       proto_ver   = ?MQTT_PROTO_V4,
       is_bridge   = false,
       clean_start = true,
       keepalive   = 30,
       properties  = undefined,
       clientid    = <<"clientid">>,
       username    = <<"username">>,
       password    = <<"passwd">>
      }.

session() -> session(#{}).
session(InitFields) when is_map(InitFields) ->
    maps:fold(fun(Field, Value, Session) ->
                      emqx_session:set_field(Field, Value, Session)
              end,
              emqx_session:init(#{zone => zone}, #{receive_maximum => 0}),
              InitFields).

