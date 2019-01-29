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

all() -> [t_inc_dec_metrics, t_trans].

t_inc_dec_metrics(_) ->
    {ok, _} = emqx_metrics:start_link(),
    {0, 0} = {emqx_metrics:val('bytes/received'), emqx_metrics:val('messages/retained')},
    emqx_metrics:inc('bytes/received'),
    emqx_metrics:inc({counter, 'bytes/received'}, 2),
    emqx_metrics:inc(counter, 'bytes/received', 1),
    emqx_metrics:inc('bytes/received', 1),
    emqx_metrics:inc({gauge, 'messages/retained'}, 2),
    emqx_metrics:inc(gauge, 'messages/retained', 2),
    {5, 4} = {emqx_metrics:val('bytes/received'), emqx_metrics:val('messages/retained')},
    emqx_metrics:dec(gauge, 'messages/retained'),
    emqx_metrics:dec(gauge, 'messages/retained', 1),
    2 = emqx_metrics:val('messages/retained'),
    emqx_metrics:set('messages/retained', 3),
    3 = emqx_metrics:val('messages/retained'),
    emqx_metrics:received(#mqtt_packet{header = #mqtt_packet_header{type = ?CONNECT}}),
    {1, 1} = {emqx_metrics:val('packets/received'), emqx_metrics:val('packets/connect')},
    emqx_metrics:sent(#mqtt_packet{header = #mqtt_packet_header{type = ?CONNACK}}),
    {1, 1} = {emqx_metrics:val('packets/sent'), emqx_metrics:val('packets/connack')}.

t_trans(_) ->
    {ok, _} = emqx_metrics:start_link(),
    emqx_metrics:trans(inc, 'bytes/received'),
    emqx_metrics:trans(inc, {counter, 'bytes/received'}, 2),
    emqx_metrics:trans(inc, counter, 'bytes/received', 2),
    emqx_metrics:trans(inc, {gauge, 'messages/retained'}, 2),
    emqx_metrics:trans(inc, gauge, 'messages/retained', 2),
    {0, 0} = {emqx_metrics:val('bytes/received'), emqx_metrics:val('messages/retained')},
    emqx_metrics:commit(),
    {5, 4} = {emqx_metrics:val('bytes/received'), emqx_metrics:val('messages/retained')},
    emqx_metrics:trans(dec, gauge, 'messages/retained'),
    emqx_metrics:trans(dec, gauge, 'messages/retained', 1),
    4 = emqx_metrics:val('messages/retained'),
    emqx_metrics:commit(),
    2 = emqx_metrics:val('messages/retained').
