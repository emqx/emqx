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

-module(emqx_protocol_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-import(emqx_protocol,
        [ handle_in/2
        , handle_out/2
        ]).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

%%--------------------------------------------------------------------
%% Test cases for handle_in
%%--------------------------------------------------------------------

t_handle_connect(_) ->
    ConnPkt = #mqtt_packet_connect{
                 proto_name  = <<"MQTT">>,
                 proto_ver   = ?MQTT_PROTO_V4,
                 is_bridge   = false,
                 clean_start = true,
                 keepalive   = 30,
                 properties  = #{},
                 client_id   = <<"clientid">>,
                 username    = <<"username">>,
                 password    = <<"passwd">>
                },
    with_proto(
      fun(PState) ->
              {ok, ?CONNACK_PACKET(?RC_SUCCESS), PState1}
                = handle_in(?CONNECT_PACKET(ConnPkt), PState),
              Client = emqx_protocol:info(client, PState1),
              ?assertEqual(<<"clientid">>, maps:get(client_id, Client)),
              ?assertEqual(<<"username">>, maps:get(username, Client))
      end).

t_handle_publish_qos0(_) ->
    with_proto(
      fun(PState) ->
              Publish = ?PUBLISH_PACKET(?QOS_0, <<"topic">>, undefined, <<"payload">>),
              {ok, PState} = handle_in(Publish, PState)
      end).

t_handle_publish_qos1(_) ->
    with_proto(
      fun(PState) ->
              Publish = ?PUBLISH_PACKET(?QOS_1, <<"topic">>, 1, <<"payload">>),
              {ok, ?PUBACK_PACKET(1, RC), _} = handle_in(Publish, PState),
              ?assert((RC == ?RC_SUCCESS) orelse (RC == ?RC_NO_MATCHING_SUBSCRIBERS))
      end).

t_handle_publish_qos2(_) ->
    with_proto(
      fun(PState) ->
              Publish1 = ?PUBLISH_PACKET(?QOS_2, <<"topic">>, 1, <<"payload">>),
              {ok, ?PUBREC_PACKET(1, RC), PState1} = handle_in(Publish1, PState),
              Publish2 = ?PUBLISH_PACKET(?QOS_2, <<"topic">>, 2, <<"payload">>),
              {ok, ?PUBREC_PACKET(2, RC), PState2} = handle_in(Publish2, PState1),
              ?assert((RC == ?RC_SUCCESS) orelse (RC == ?RC_NO_MATCHING_SUBSCRIBERS)),
              Session = emqx_protocol:info(session, PState2),
              ?assertEqual(2, emqx_session:info(awaiting_rel, Session))
      end).

t_handle_puback(_) ->
    with_proto(
      fun(PState) ->
              {ok, PState} = handle_in(?PUBACK_PACKET(1, ?RC_SUCCESS), PState)
      end).

t_handle_pubrec(_) ->
    with_proto(
      fun(PState) ->
              {ok, ?PUBREL_PACKET(1, ?RC_PACKET_IDENTIFIER_NOT_FOUND), PState}
                = handle_in(?PUBREC_PACKET(1, ?RC_SUCCESS), PState)
      end).

t_handle_pubrel(_) ->
    with_proto(
      fun(PState) ->
              {ok, ?PUBCOMP_PACKET(1, ?RC_PACKET_IDENTIFIER_NOT_FOUND), PState}
              = handle_in(?PUBREL_PACKET(1, ?RC_SUCCESS), PState)
      end).

t_handle_pubcomp(_) ->
    with_proto(
      fun(PState) ->
              {ok, PState} = handle_in(?PUBCOMP_PACKET(1, ?RC_SUCCESS), PState)
      end).

t_handle_subscribe(_) ->
    with_proto(
      fun(PState) ->
              TopicFilters = [{<<"+">>, ?DEFAULT_SUBOPTS}],
              {ok, ?SUBACK_PACKET(10, [?QOS_0]), PState1}
                = handle_in(?SUBSCRIBE_PACKET(10, #{}, TopicFilters), PState),
              Session = emqx_protocol:info(session, PState1),
              ?assertEqual(maps:from_list(TopicFilters),
                           emqx_session:info(subscriptions, Session))

      end).

t_handle_unsubscribe(_) ->
    with_proto(
      fun(PState) ->
              {ok, ?UNSUBACK_PACKET(11), PState}
                = handle_in(?UNSUBSCRIBE_PACKET(11, #{}, [<<"+">>]), PState)
      end).

t_handle_pingreq(_) ->
    with_proto(
      fun(PState) ->
          {ok, ?PACKET(?PINGRESP), PState} = handle_in(?PACKET(?PINGREQ), PState)
      end).

t_handle_disconnect(_) ->
    with_proto(
      fun(PState) ->
              {stop, normal, PState1} = handle_in(?DISCONNECT_PACKET(?RC_SUCCESS), PState),
              ?assertEqual(undefined, emqx_protocol:info(will_msg, PState1))
      end).

t_handle_auth(_) ->
    with_proto(
      fun(PState) ->
              {ok, PState} = handle_in(?AUTH_PACKET(), PState)
      end).

%%--------------------------------------------------------------------
%% Test cases for handle_deliver
%%--------------------------------------------------------------------

t_handle_deliver(_) ->
    with_proto(
      fun(PState) ->
              TopicFilters = [{<<"+">>, ?DEFAULT_SUBOPTS#{qos => ?QOS_2}}],
              {ok, ?SUBACK_PACKET(1, [?QOS_2]), PState1}
                = handle_in(?SUBSCRIBE_PACKET(1, #{}, TopicFilters), PState),
              Msg0 = emqx_message:make(<<"clientx">>, ?QOS_0, <<"t0">>, <<"qos0">>),
              Msg1 = emqx_message:make(<<"clientx">>, ?QOS_1, <<"t1">>, <<"qos1">>),
              Delivers = [{deliver, <<"+">>, Msg0},
                          {deliver, <<"+">>, Msg1}],
              {ok, Packets, _PState2} = emqx_protocol:handle_deliver(Delivers, PState1),
              ?assertMatch([?PUBLISH_PACKET(?QOS_0, <<"t0">>, undefined, <<"qos0">>),
                            ?PUBLISH_PACKET(?QOS_1, <<"t1">>, 1, <<"qos1">>)
                           ], Packets)
      end).

%%--------------------------------------------------------------------
%% Test cases for handle_out
%%--------------------------------------------------------------------

t_handle_conack(_) ->
    with_proto(
      fun(PState) ->
              {ok, ?CONNACK_PACKET(?RC_SUCCESS, SP, _), _}
                = handle_out({connack, ?RC_SUCCESS, 0}, PState),
              {error, unauthorized_client, ?CONNACK_PACKET(5), _}
                = handle_out({connack, ?RC_NOT_AUTHORIZED}, PState)
      end).

t_handle_out_publish(_) ->
    with_proto(
      fun(PState) ->
              Pub0 = {publish, undefined, emqx_message:make(<<"t">>, <<"qos0">>)},
              Pub1 = {publish, 1, emqx_message:make(<<"c">>, ?QOS_1, <<"t">>, <<"qos1">>)},
              {ok, ?PUBLISH_PACKET(?QOS_0), PState} = handle_out(Pub0, PState),
              {ok, ?PUBLISH_PACKET(?QOS_1), PState} = handle_out(Pub1, PState),
              {ok, Packets, PState} = handle_out({publish, [Pub0, Pub1]}, PState),
              ?assertEqual(2, length(Packets))
      end).

t_handle_out_puback(_) ->
    with_proto(
      fun(PState) ->
              {ok, PState} = handle_out({puberr, ?RC_NOT_AUTHORIZED}, PState),
              {ok, ?PUBACK_PACKET(1, ?RC_SUCCESS), PState}
                = handle_out({puback, 1, ?RC_SUCCESS}, PState)
      end).

t_handle_out_pubrec(_) ->
    with_proto(
      fun(PState) ->
              {ok, ?PUBREC_PACKET(4, ?RC_SUCCESS), PState}
                = handle_out({pubrec, 4, ?RC_SUCCESS}, PState)
      end).

t_handle_out_pubrel(_) ->
    with_proto(
      fun(PState) ->
              {ok, ?PUBREL_PACKET(2), PState} = handle_out({pubrel, 2}, PState),
              {ok, ?PUBREL_PACKET(3, ?RC_SUCCESS), PState}
                = handle_out({pubrel, 3, ?RC_SUCCESS}, PState)
      end).

t_handle_out_pubcomp(_) ->
    with_proto(
      fun(PState) ->
              {ok, ?PUBCOMP_PACKET(5, ?RC_SUCCESS), PState}
                = handle_out({pubcomp, 5, ?RC_SUCCESS}, PState)
      end).

t_handle_out_suback(_) ->
    with_proto(
      fun(PState) ->
              {ok, ?SUBACK_PACKET(1, [?QOS_2]), PState}
                 = handle_out({suback, 1, [?QOS_2]}, PState)
      end).

t_handle_out_unsuback(_) ->
    with_proto(
      fun(PState) ->
              {ok, ?UNSUBACK_PACKET(1), PState} = handle_out({unsuback, 1, [?RC_SUCCESS]}, PState)
      end).

t_handle_out_disconnect(_) ->
    with_proto(
      fun(PState) ->
              handle_out({disconnect, 0}, PState)
      end).

%%--------------------------------------------------------------------
%% Test cases for handle_timeout
%%--------------------------------------------------------------------

t_handle_timeout(_) ->
    with_proto(
      fun(PState) ->
        'TODO'
      end).

%%--------------------------------------------------------------------
%% Test cases for terminate
%%--------------------------------------------------------------------

t_terminate(_) ->
    with_proto(
      fun(PState) ->
        'TODO'
      end).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

with_proto(Fun) ->
    ConnInfo = #{peername => {{127,0,0,1}, 3456},
                 sockname => {{127,0,0,1}, 1883},
                 client_id => <<"clientid">>,
                 username => <<"username">>
                },
    Options = [{zone, testing}],
    PState = emqx_protocol:init(ConnInfo, Options),
    Session = emqx_session:init(false, #{zone => testing},
                                #{max_inflight => 100,
                                  expiry_interval => 0
                                 }),
    Fun(emqx_protocol:set(session, Session, PState)).

