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

-module(emqx_mqtt_props_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").

all() -> [t_mqtt_properties_all].

t_mqtt_properties_all(_) ->
    Props = emqx_mqtt_props:filter(?CONNECT, #{'Session-Expiry-Interval' => 1, 'Maximum-Packet-Size' => 255}),
    ok = emqx_mqtt_props:validate(Props),
    #{} = emqx_mqtt_props:filter(?CONNECT, #{'Maximum-QoS' => ?QOS_2}).

