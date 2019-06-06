%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_metrics_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> [t_inc_dec, t_inc_recv, t_inc_sent, t_trans].

t_inc_dec(_) ->
    {ok, _} = emqx_metrics:start_link(),
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
    ?assertEqual(3, emqx_metrics:val('messages.retained')),
    ok = emqx_metrics:stop().

t_inc_recv(_) ->
    {ok, _} = emqx_metrics:start_link(),
    ok = emqx_metrics:inc_recv(?PACKET(?CONNECT)),
    ?assertEqual(1, emqx_metrics:val('packets.received')),
    ?assertEqual(1, emqx_metrics:val('packets.connect.received')),
    ok = emqx_metrics:stop().

t_inc_sent(_) ->
    {ok, _} = emqx_metrics:start_link(),
    ok = emqx_metrics:inc_sent(?CONNACK_PACKET(0)),
    ?assertEqual(1, emqx_metrics:val('packets.sent')),
    ?assertEqual(1, emqx_metrics:val('packets.connack.sent')),
    ok = emqx_metrics:stop().

t_trans(_) ->
    {ok, _} = emqx_metrics:start_link(),
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
    ?assertEqual(2, emqx_metrics:val('messages.retained')),
    ok = emqx_metrics:stop().

