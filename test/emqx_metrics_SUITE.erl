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

-module(emqx_metrics_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

t_val(_) ->
    error('TODO').

t_dec(_) ->
    error('TODO').

t_set(_) ->
    error('TODO').

t_commit(_) ->
    error('TODO').

t_inc(_) ->
    error('TODO').

t_new(_) ->
    with_metrics_server(
      fun() ->
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
        ?assertEqual(1, emqx_metrics:val('packets.received')),
        ?assertEqual(1, emqx_metrics:val('packets.connect.received'))
      end).

t_inc_sent(_) ->
    with_metrics_server(
      fun() ->
        ok = emqx_metrics:inc_sent(?CONNACK_PACKET(0)),
        ?assertEqual(1, emqx_metrics:val('packets.sent')),
        ?assertEqual(1, emqx_metrics:val('packets.connack.sent'))
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

