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

-module(emqx_metrics_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

t_new(_) ->
    with_metrics_server(
      fun() ->
          ok = emqx_metrics:new('metrics.test'),
          ok = emqx_metrics:new('metrics.test'),
          0 = emqx_metrics:val('metrics.test'),
          ok = emqx_metrics:inc('metrics.test'),
          1 = emqx_metrics:val('metrics.test'),
          ok = emqx_metrics:new(counter, 'metrics.test.cnt'),
          0 = emqx_metrics:val('metrics.test.cnt'),
          ok = emqx_metrics:inc('metrics.test.cnt'),
          1 = emqx_metrics:val('metrics.test.cnt'),
          ok = emqx_metrics:new(gauge, 'metrics.test.total'),
          0 = emqx_metrics:val('metrics.test.total'),
          ok = emqx_metrics:inc('metrics.test.total'),
          1 = emqx_metrics:val('metrics.test.total')
      end).

t_all(_) ->
    with_metrics_server(
      fun() ->
          Metrics = emqx_metrics:all(),
          ?assert(length(Metrics) > 50)
      end).

t_inc_dec(_) ->
    with_metrics_server(
      fun() ->
        ?assertEqual(0, emqx_metrics:val('bytes.received')),
        ?assertEqual(0, emqx_metrics:val('messages.retained')),
        ok = emqx_metrics:inc('bytes.received'),
        ok = emqx_metrics:inc('bytes.received', 2),
        ok = emqx_metrics:inc('bytes.received', 2),
        ?assertEqual(5, emqx_metrics:val('bytes.received')),
        ok = emqx_metrics:inc('messages.retained', 2),
        ok = emqx_metrics:inc('messages.retained', 2),
        ?assertEqual(4, emqx_metrics:val('messages.retained')),
        ok = emqx_metrics:dec('messages.retained'),
        ok = emqx_metrics:dec('messages.retained', 1),
        ?assertEqual(2, emqx_metrics:val('messages.retained')),
        ok = emqx_metrics:set('messages.retained', 3),
        ?assertEqual(3, emqx_metrics:val('messages.retained'))
      end).

t_inc_recv(_) ->
    with_metrics_server(
      fun() ->
        ok = emqx_metrics:inc_recv(?PACKET(?CONNECT)),
        ok = emqx_metrics:inc_recv(?PUBLISH_PACKET(0, 0)),
        ok = emqx_metrics:inc_recv(?PUBLISH_PACKET(1, 0)),
        ok = emqx_metrics:inc_recv(?PUBLISH_PACKET(2, 0)),
        ok = emqx_metrics:inc_recv(?PUBLISH_PACKET(3, 0)),
        ok = emqx_metrics:inc_recv(?PACKET(?PUBACK)),
        ok = emqx_metrics:inc_recv(?PACKET(?PUBREC)),
        ok = emqx_metrics:inc_recv(?PACKET(?PUBREL)),
        ok = emqx_metrics:inc_recv(?PACKET(?PUBCOMP)),
        ok = emqx_metrics:inc_recv(?PACKET(?SUBSCRIBE)),
        ok = emqx_metrics:inc_recv(?PACKET(?UNSUBSCRIBE)),
        ok = emqx_metrics:inc_recv(?PACKET(?PINGREQ)),
        ok = emqx_metrics:inc_recv(?PACKET(?DISCONNECT)),
        ok = emqx_metrics:inc_recv(?PACKET(?AUTH)),
        ok = emqx_metrics:inc_recv(?PACKET(?RESERVED)),
        ?assertEqual(15, emqx_metrics:val('packets.received')),
        ?assertEqual(1, emqx_metrics:val('packets.connect.received')),
        ?assertEqual(4, emqx_metrics:val('messages.received')),
        ?assertEqual(1, emqx_metrics:val('messages.qos0.received')),
        ?assertEqual(1, emqx_metrics:val('messages.qos1.received')),
        ?assertEqual(1, emqx_metrics:val('messages.qos2.received')),
        ?assertEqual(4, emqx_metrics:val('packets.publish.received')),
        ?assertEqual(1, emqx_metrics:val('packets.puback.received')),
        ?assertEqual(1, emqx_metrics:val('packets.pubrec.received')),
        ?assertEqual(1, emqx_metrics:val('packets.pubrel.received')),
        ?assertEqual(1, emqx_metrics:val('packets.pubcomp.received')),
        ?assertEqual(1, emqx_metrics:val('packets.subscribe.received')),
        ?assertEqual(1, emqx_metrics:val('packets.unsubscribe.received')),
        ?assertEqual(1, emqx_metrics:val('packets.pingreq.received')),
        ?assertEqual(1, emqx_metrics:val('packets.disconnect.received')),
        ?assertEqual(1, emqx_metrics:val('packets.auth.received'))
      end).

t_inc_sent(_) ->
    with_metrics_server(
      fun() ->
        ok = emqx_metrics:inc_sent(?CONNACK_PACKET(0)),
        ok = emqx_metrics:inc_sent(?PUBLISH_PACKET(0, 0)),
        ok = emqx_metrics:inc_sent(?PUBLISH_PACKET(1, 0)),
        ok = emqx_metrics:inc_sent(?PUBLISH_PACKET(2, 0)),
        ok = emqx_metrics:inc_sent(?PUBACK_PACKET(0, 0)),
        ok = emqx_metrics:inc_sent(?PUBREC_PACKET(3, 0)),
        ok = emqx_metrics:inc_sent(?PACKET(?PUBREL)),
        ok = emqx_metrics:inc_sent(?PACKET(?PUBCOMP)),
        ok = emqx_metrics:inc_sent(?PACKET(?SUBACK)),
        ok = emqx_metrics:inc_sent(?PACKET(?UNSUBACK)),
        ok = emqx_metrics:inc_sent(?PACKET(?PINGRESP)),
        ok = emqx_metrics:inc_sent(?PACKET(?DISCONNECT)),
        ok = emqx_metrics:inc_sent(?PACKET(?AUTH)),
        ?assertEqual(13, emqx_metrics:val('packets.sent')),
        ?assertEqual(1, emqx_metrics:val('packets.connack.sent')),
        ?assertEqual(3, emqx_metrics:val('messages.sent')),
        ?assertEqual(1, emqx_metrics:val('messages.qos0.sent')),
        ?assertEqual(1, emqx_metrics:val('messages.qos1.sent')),
        ?assertEqual(1, emqx_metrics:val('messages.qos2.sent')),
        ?assertEqual(3, emqx_metrics:val('packets.publish.sent')),
        ?assertEqual(1, emqx_metrics:val('packets.puback.sent')),
        ?assertEqual(1, emqx_metrics:val('packets.pubrec.sent')),
        ?assertEqual(1, emqx_metrics:val('packets.pubrel.sent')),
        ?assertEqual(1, emqx_metrics:val('packets.pubcomp.sent')),
        ?assertEqual(1, emqx_metrics:val('packets.suback.sent')),
        ?assertEqual(1, emqx_metrics:val('packets.unsuback.sent')),
        ?assertEqual(1, emqx_metrics:val('packets.pingresp.sent')),
        ?assertEqual(1, emqx_metrics:val('packets.disconnect.sent')),
        ?assertEqual(1, emqx_metrics:val('packets.auth.sent'))
      end).

t_trans(_) ->
    with_metrics_server(
      fun() ->
        ok = emqx_metrics:trans(inc, 'bytes.received'),
        ok = emqx_metrics:trans(inc, 'bytes.received', 2),
        ?assertEqual(0, emqx_metrics:val('bytes.received')),
        ok = emqx_metrics:trans(inc, 'messages.retained', 2),
        ok = emqx_metrics:trans(inc, 'messages.retained', 2),
        ?assertEqual(0, emqx_metrics:val('messages.retained')),
        ok = emqx_metrics:commit(),
        ?assertEqual(3, emqx_metrics:val('bytes.received')),
        ?assertEqual(4, emqx_metrics:val('messages.retained')),
        ok = emqx_metrics:trans(dec, 'messages.retained'),
        ok = emqx_metrics:trans(dec, 'messages.retained', 1),
        ?assertEqual(4, emqx_metrics:val('messages.retained')),
        ok = emqx_metrics:commit(),
        ?assertEqual(2, emqx_metrics:val('messages.retained'))
      end).

with_metrics_server(Fun) ->
    {ok, _} = emqx_metrics:start_link(),
    _ = Fun(),
    ok = emqx_metrics:stop().

