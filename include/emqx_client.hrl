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


-ifndef(EMQX_CLIENT_HRL).
-define(EMQX_CLIENT_HRL, true).
-include("emqx_mqtt.hrl").
-record(mqtt_msg, {qos = ?QOS_0, retain = false, dup = false,
                   packet_id, topic, props, payload}).
-endif.
